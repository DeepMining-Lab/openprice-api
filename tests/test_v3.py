"""API V3 tests: Parquet sync, store lookups, truncation fixes, and V2 parity.

All tests use small synthetic CSV datasets and a temporary Parquet root: neither the
real datasets nor ``~/openprice/parquet`` are touched.
"""

from __future__ import annotations

import csv
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.config as config_module
from app.config import V3Config
from app.main import app  # imported first: it calls load_config() at import time
from app.v3 import store as store_mod
from app.v3 import service as service_mod
from app.v3 import sync as sync_mod

UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S+00:00")


def _write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _append(path: Path, rows: list[list]) -> None:
    with path.open("a", newline="") as f:
        csv.writer(f).writerows(rows)


POOL_HEADER = ["timestamp", "price_usdc_per_link", "volume_usdc", "pool_tvl_at_block", "slip_1k",
               "block_number", "transaction_hash", "log_index"]
POOL_REL = "link/link_usdc_uniswap_v3_03.csv"
CL_HEADER = ["round_updated_at_utc", "answer_normalized"]


@pytest.fixture
def v3cfg(cfg, tmp_path, monkeypatch):
    """Config clone with an empty dataset root + a private Parquet root."""
    root = tmp_path / "datasets"
    root.mkdir()
    c = cfg.model_copy(deep=True)
    c.paths.datasets_root = str(root)
    c.v3 = V3Config(parquet_root=str(tmp_path / "parquet"), cache_size=0, manifest_poll_seconds=0)
    monkeypatch.setattr(config_module, "_config", c)
    store_mod.reset_store()
    service_mod.reset_service()
    yield c
    store_mod.reset_store()
    service_mod.reset_service()


def _pool_rows(n: int, start: datetime = T0, step_s: int = 60, base: float = 14.0) -> list[list]:
    return [[_iso(start + timedelta(seconds=i * step_s)), base + (i % 7) * 0.01, 1000.0,
             2_000_000.0, 0.001, 1000 + i, f"0x{i:064x}", 0] for i in range(n)]


def _build(cfg) -> store_mod.Store:
    sync_mod.run_sync(cfg)
    return store_mod.Store(cfg)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

class TestSync:
    def test_full_build_matches_csv_and_keeps_csv_untouched(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(50))
        before = p.read_bytes()
        res = sync_mod.run_sync(v3cfg)
        assert [r.action for r in res if r.rel == POOL_REL] == ["rebuild"]
        assert p.read_bytes() == before
        st = store_mod.Store(v3cfg)
        assert st.dataset(POOL_REL).n_rows == 50

    def test_duplicates_removed_keep_first(self, v3cfg):
        rows = _pool_rows(5)
        rows.insert(3, list(rows[2][:6]) + [rows[2][6], rows[2][7]])  # exact duplicate event
        rows[3][1] = 99.0  # a later re-extraction could differ in metadata only; price kept from the first
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, rows)
        res = [r for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL][0]
        assert res.dups_removed == 1 and res.new_rows == 5
        st = store_mod.Store(v3cfg)
        assert 99.0 not in st.prices(POOL_REL, "px_usd", T0 - timedelta(days=1), T0 + timedelta(days=1))

    def test_incremental_append_and_boundary_overlap(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        rows = _pool_rows(10)
        _write(p, POOL_HEADER, rows)
        sync_mod.run_sync(v3cfg)
        # next daily extraction re-emits the last event and adds 5 new ones
        new = _pool_rows(6, start=T0 + timedelta(seconds=9 * 60))
        for i, r in enumerate(new):
            r[5], r[6] = 1009 + i, f"0x{9 + i:064x}"
        _append(p, new)
        res = [r for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL][0]
        assert res.action == "append" and res.dups_removed == 1 and res.new_rows == 5
        assert store_mod.Store(v3cfg).dataset(POOL_REL).n_rows == 15
        assert [r.action for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL] == ["unchanged"]

    def test_partial_last_line_is_not_consumed(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(5))
        sync_mod.run_sync(v3cfg)
        with p.open("a") as f:  # writer caught in the middle of a line
            f.write("2024-01-02 00:00:00+00:00,15.0,10")
        assert [r.action for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL] == ["unchanged"]
        with p.open("a") as f:
            f.write("00.0,2000000.0,0.001,2000,0xabc,1\n")
        res = [r for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL][0]
        assert res.action == "append" and res.new_rows == 1
        st = store_mod.Store(v3cfg)
        assert st.as_of(POOL_REL, datetime(2024, 1, 2, tzinfo=UTC), ["px_usd"])["px_usd"] == 15.0

    def test_rewritten_prefix_triggers_full_rebuild(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(10))
        sync_mod.run_sync(v3cfg)
        _write(p, POOL_HEADER, _pool_rows(12, base=20.0))  # same header, different history
        res = [r for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL][0]
        assert res.action == "rebuild"
        assert store_mod.Store(v3cfg).dataset(POOL_REL).n_rows == 12

    def test_changed_header_triggers_full_rebuild(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(3))
        sync_mod.run_sync(v3cfg)
        _write(p, POOL_HEADER + ["extra"], [r + ["x"] for r in _pool_rows(4)])
        assert [r.action for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL] == ["rebuild"]


# ---------------------------------------------------------------------------
# Store semantics
# ---------------------------------------------------------------------------

class TestStore:
    def test_as_of_ties_return_first_csv_row(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        same = _iso(T0 + timedelta(hours=1))
        rows = [[_iso(T0), 10.0, 1.0, 2e6, 0.001, 1, "0xa", 0],
                [same, 11.0, 1.0, 2e6, 0.001, 2, "0xb", 0],
                [same, 12.0, 1.0, 2e6, 0.001, 2, "0xc", 1],
                [same, 13.0, 1.0, 2e6, 0.001, 2, "0xd", 2]]
        _write(p, POOL_HEADER, rows)
        st = _build(v3cfg)
        assert st.as_of(POOL_REL, T0 + timedelta(hours=2), ["px_usd"])["px_usd"] == 11.0

    def test_as_of_before_first_row_and_far_lookback(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, [[_iso(T0), 10.0, 1.0, 2e6, 0.001, 1, "0xa", 0]])
        st = _build(v3cfg)
        assert st.as_of(POOL_REL, T0 - timedelta(seconds=1), ["px_usd"]) is None
        assert st.as_of(POOL_REL, T0 + timedelta(days=400), ["px_usd"])["px_usd"] == 10.0  # beyond the 1d/30d ladder

    def test_window_is_left_closed_right_open_and_sum_inclusive(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(5, step_s=3600))
        st = _build(v3cfg)
        rows = st.window(POOL_REL, T0, T0 + timedelta(hours=2), ["px_usd"])
        assert len(rows) == 2
        assert st.sum_between(POOL_REL, "vol_usd", T0, T0 + timedelta(hours=2)) == 3000.0

    def test_price_stats_matches_python_upper_median_and_mad(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        prices = [5.0, 1.0, 4.0, 2.0, 3.0, 9.0]  # even n -> sorted[n//2] = 4.0 (upper median)
        _write(p, POOL_HEADER, [[_iso(T0 + timedelta(minutes=i)), v, 1.0, 2e6, 0.001, i, f"0x{i}", 0]
                                for i, v in enumerate(prices)])
        st = _build(v3cfg)
        n, med, mad = st.price_stats(POOL_REL, "px_usd", T0, T0 + timedelta(days=1))
        s = sorted(prices)
        m = s[len(s) // 2]
        expected_mad = sorted(abs(x - m) for x in prices)[len(prices) // 2]
        assert (n, med, mad) == (6, m, expected_mad)


# ---------------------------------------------------------------------------
# The two truncation fixes (and their legacy switch)
# ---------------------------------------------------------------------------

def _big_pool(cfg, n: int):
    """A LINK/USDC pool with more rows than api.max_limit inside the 7 days before T."""
    root = Path(cfg.paths.datasets_root)
    T = T0 + timedelta(days=8)
    rows = []
    for i in range(n):  # old part of the window is flat at 10, recent part at 20
        t = T - timedelta(days=7) + timedelta(seconds=i * (7 * 86400 - 1) / n)
        price = 10.0 if i < n // 2 else 20.0
        rows.append([_iso(t), price, 100000.0, 2e6, 0.001, i, f"0x{i:064x}", 0])
    _write(root / POOL_REL, POOL_HEADER, rows)
    _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER,
           [[_iso(T - timedelta(hours=1)), 20.0]])
    return T


class TestTruncationFixes:
    def test_s_stat_window_is_complete_by_default_and_truncated_in_legacy(self, v3cfg):
        n = 3 * v3cfg.api.max_limit  # api.max_limit is 1000 in the test config
        T = _big_pool(v3cfg, n)
        st = _build(v3cfg)
        # full window: half the prices are 10, half 20 -> median = 20 (upper), MAD = 0 -> S_stat = 1
        v3cfg.v3.legacy_truncation = False
        fixed, _ = service_mod.compute_s_stat(st, POOL_REL, T, 20.0, v3cfg)
        # legacy: only the oldest 1000 rows (all 10.0) -> median 10, MAD 0 -> S_stat = 1 as well,
        # so use a price that separates them: with MAD > 0 the score depends on the window content.
        assert fixed == 1.0
        n_full, med_full, _ = st.price_stats(POOL_REL, "px_usd", T - timedelta(days=7), T, None)
        n_leg, med_leg, _ = st.price_stats(POOL_REL, "px_usd", T - timedelta(days=7), T, v3cfg.api.max_limit)
        assert n_full == n and n_leg == v3cfg.api.max_limit
        assert med_full == 20.0 and med_leg == 10.0  # legacy window only covers the OLDEST rows

    def test_windowed_vwmp_reads_all_swaps_in_window(self, v3cfg):
        root = Path(v3cfg.paths.datasets_root)
        T = T0 + timedelta(days=3, hours=12)
        n = 3 * v3cfg.api.max_limit
        rows = []
        for i in range(n):  # 24 h day-window centred on T: first third at 10, rest at 20
            t = T - timedelta(hours=12) + timedelta(seconds=i * 86399 / n)
            rows.append([_iso(t), 10.0 if i < n // 3 else 20.0, 1000.0, 2e6, 0.001, i, f"0x{i:064x}", 0])
        _write(root / POOL_REL, POOL_HEADER, rows)
        _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T - timedelta(hours=1)), 20.0]])
        _build(v3cfg)
        svc = service_mod.Service(v3cfg)
        v3cfg.v3.legacy_truncation = False
        fixed, _ = svc.price_at("LINK", T, granularity="day", include_confidence=False)
        assert fixed.swap_count == n and fixed.price_usd == 20.0
        v3cfg.v3.legacy_truncation = True
        legacy, _ = svc.price_at("LINK", T, granularity="day", include_confidence=False)
        assert legacy.swap_count == v3cfg.api.max_limit and legacy.price_usd == 10.0


# ---------------------------------------------------------------------------
# Parity with V2 on the synthetic dataset (legacy mode == V2 exactly)
# ---------------------------------------------------------------------------

class TestParityWithV2:
    @pytest.mark.parametrize("asset,ts,gran", [
        ("LINK", "2024-01-01T12:00:00Z", "raw"),
        ("LINK", "2024-01-01T06:20:00Z", "hour"),
        ("LINK", "2024-01-02T00:00:00Z", "day"),
        ("LINK", "2023-06-01T00:00:00Z", "raw"),     # before any observation
        ("ETH", "2024-01-01T12:00:00Z", "raw"),
        ("ETH", "2024-01-01T12:00:00Z", "day"),
    ])
    def test_v3_legacy_equals_v2(self, cfg, tmp_dataset_root, tmp_path, monkeypatch, asset, ts, gran):
        from app.routers import prices_v2
        c = cfg.model_copy(deep=True)
        c.v3 = V3Config(parquet_root=str(tmp_path / "pq"), legacy_truncation=True, cache_size=0, manifest_poll_seconds=0)
        monkeypatch.setattr(config_module, "_config", c)
        store_mod.reset_store(); service_mod.reset_service()
        sync_mod.run_sync(c)
        t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        v2 = prices_v2.price_at_v2(asset=asset, timestamp=t, source="auto", branch="auto", granularity=gran,
                                   include_confidence=True, include_provenance=True).model_dump(mode="json")
        v3, _ = service_mod.Service(c).price_at(asset, t, granularity=gran)
        got = v3.model_dump(mode="json")
        assert _approx_equal(v2, got), (v2, got)
        store_mod.reset_store(); service_mod.reset_service()


def _approx_equal(a, b) -> bool:
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_approx_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_approx_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, float) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)
    return a == b


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

class TestEndpoints:
    def test_v3_503_when_store_empty(self, cfg, tmp_path, monkeypatch):
        from fastapi.testclient import TestClient
        c = cfg.model_copy(deep=True)
        c.v3 = V3Config(parquet_root=str(tmp_path / "nothing"))
        monkeypatch.setattr(config_module, "_config", c)
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            r = client.get("/v3/prices/LINK/at", params={"timestamp": "2024-01-01T12:00:00Z"})
            assert r.status_code == 503
            assert client.get("/v3/ready").status_code == 503
        store_mod.reset_store(); service_mod.reset_service()

    def test_v3_price_endpoint_headers_and_cache(self, v3cfg):
        from fastapi.testclient import TestClient
        v3cfg.v3.cache_size = 16
        root = Path(v3cfg.paths.datasets_root)
        _write(root / POOL_REL, POOL_HEADER, _pool_rows(20))
        _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T0), 14.0]])
        sync_mod.run_sync(v3cfg)
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            q = {"timestamp": "2024-01-01T00:10:00Z"}
            r1 = client.get("/v3/prices/LINK/at", params=q)
            r2 = client.get("/v3/prices/LINK/at", params=q)
            assert r1.status_code == 200, r1.text
            assert r1.headers["X-Cache"] == "MISS" and r2.headers["X-Cache"] == "HIT"
            assert r1.json() == r2.json()
            assert "Server-Timing" in r1.headers and r1.headers["X-Dataset-Version"]
            assert client.get("/v3/prices/DOGE/at", params=q).status_code == 404
            assert client.get("/v3/prices/LINK/at", params={**q, "granularity": "week"}).status_code == 422
            assert client.get("/v3/ready").json()["status"] == "ok"
