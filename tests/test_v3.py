"""API V3 tests: Parquet sync, store lookups, truncation fixes, and V2 parity.

All tests use small synthetic CSV datasets and a temporary Parquet root: neither the
real datasets nor ``~/openprice/parquet`` are touched.
"""

from __future__ import annotations

import csv
import json
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
        got = service_mod.strip_v3_diagnostics(v3.model_dump(mode="json"))
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


# ---------------------------------------------------------------------------
# V3 diagnostics: future timestamps, data coverage, explained fallbacks
# ---------------------------------------------------------------------------

ETH_REF_REL = "eth/eth_usdc_uniswap_v3_005.csv"
ETH_REF_HEADER = ["timestamp", "price_usdc_per_eth", "volume_usdc", "pool_tvl_at_block", "slip_1k"]
TOKEN_LEG_REL = "link/link_weth_uniswap_v3_03.csv"
TOKEN_LEG_HEADER = ["timestamp", "price_weth_per_link", "volume_weth", "pool_tvl_at_block", "slip_1k"]


def _simple_link(cfg, n: int = 20) -> None:
    root = Path(cfg.paths.datasets_root)
    _write(root / POOL_REL, POOL_HEADER, _pool_rows(n))
    _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T0), 14.0]])
    sync_mod.run_sync(cfg)


class TestDiagnostics:
    def test_future_timestamp_is_an_explicit_null_and_never_cached(self, v3cfg):
        v3cfg.v3.cache_size = 16
        _simple_link(v3cfg)
        svc = service_mod.Service(v3cfg)
        future = datetime.now(UTC) + timedelta(days=1)
        r1, c1 = svc.price_at("LINK", future)
        _, c2 = svc.price_at("LINK", future)
        assert (r1.price_usd, r1.branch_level, r1.data_status) == (None, "4", "unavailable")
        assert r1.unavailable_reason == "future_timestamp" and r1.confidence is None
        assert [w.code for w in r1.warnings] == ["future_timestamp"]
        assert not c1 and not c2

    def test_beyond_data_coverage_flags_provisional_prices_only(self, v3cfg):
        _simple_link(v3cfg)  # last synced observation of link/ = T0 + 19 min
        svc = service_mod.Service(v3cfg)

        def codes(resp):
            return [w.code for w in resp.warnings]

        inside, _ = svc.price_at("LINK", T0 + timedelta(minutes=10))
        after, _ = svc.price_at("LINK", T0 + timedelta(minutes=30))
        window, _ = svc.price_at("LINK", T0 + timedelta(minutes=10), granularity="hour")  # window ends at +40 min
        assert "beyond_data_coverage" not in codes(inside)
        assert after.branch_level == "0a" and after.price_usd is not None  # still answered (as-of)
        assert "beyond_data_coverage" in codes(after)
        assert "beyond_data_coverage" in [w.code for w in after.confidence.warnings]
        assert "beyond_data_coverage" in codes(window)

    def test_fallback_explains_each_rejected_candidate(self, v3cfg):
        root = Path(v3cfg.paths.datasets_root)
        T = T0 + timedelta(hours=3)
        # 0a LINK/USDC: active but TVL 500 USD (< 1 000 000 in the test config)
        _write(root / POOL_REL, POOL_HEADER,
               [[_iso(T0 + timedelta(minutes=10 * i)), 14.0, 50000.0, 500.0, 0.001, i, f"0x{i:064x}", 0]
                for i in range(19)])
        # ETH/USD leg observed every minute up to T; LINK/WETH leg last traded 2 h before T
        _write(root / ETH_REF_REL, ETH_REF_HEADER,
               [[_iso(T0 + timedelta(minutes=i)), 2200.0, 100000.0, 5e6, 0.0005] for i in range(181)])
        _write(root / TOKEN_LEG_REL, TOKEN_LEG_HEADER, [[_iso(T0 + timedelta(hours=1)), 0.0064, 10.0, 1.5e6, 0.001]])
        _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T - timedelta(minutes=5)), 14.1]])
        sync_mod.run_sync(v3cfg)
        resp, _ = service_mod.Service(v3cfg).price_at("LINK", T)

        assert (resp.branch_level, resp.price_usd) == ("3", 14.1)
        rejected = [(r.level, r.file, r.rule) for r in resp.provenance.rejected_candidates]
        assert rejected == [
            ("0a", POOL_REL, "zombie_tvl"),
            ("0a", "link/link_usdt_uniswap_v3_03.csv", "dataset_missing"),
            ("0b", TOKEN_LEG_REL, "cross_rate_lag"),
            ("1", "link/link_weth_uniswap_v2_03.csv", "dataset_missing"),
            ("2", "link/link_eth_sushiswap_v2_03.csv", "dataset_missing"),
            ("2", "link/link_eth_sushiswap_v3_03.csv", "dataset_missing"),
        ]
        lag = resp.provenance.rejected_candidates[2]
        assert (lag.value, lag.threshold) == (7200.0, 3600.0)
        assert resp.provenance.rejected_candidates[0].value == 500.0
        fallback = [w for w in resp.warnings if w.code == "fallback_explained"]
        assert len(fallback) == 1 and "level 3 (chainlink_fallback)" in fallback[0].message
        assert "fallback_explained" in [w.code for w in resp.confidence.warnings]  # explains C = null
        # the diagnostics are the only difference from a V2-shaped response
        stripped = service_mod.strip_v3_diagnostics(resp.model_dump(mode="json"))
        assert "rejected_candidates" not in stripped["provenance"]
        assert all(w["code"] not in service_mod.V3_DIAGNOSTIC_CODES for w in stripped["warnings"])

    def test_no_fallback_warning_when_the_first_level_answers(self, v3cfg):
        _simple_link(v3cfg)
        resp, _ = service_mod.Service(v3cfg).price_at("LINK", T0 + timedelta(minutes=10))
        assert resp.branch_level == "0a"
        assert "fallback_explained" not in [w.code for w in resp.warnings]
        # the missing sibling 0a pool is still listed for audit
        assert [(r.level, r.rule) for r in resp.provenance.rejected_candidates] == [("0a", "dataset_missing")]


# ---------------------------------------------------------------------------
# Range endpoints: one point per timestamp, truncation and pagination
# ---------------------------------------------------------------------------

class TestRanges:
    def _dup_pool(self, cfg) -> list[datetime]:
        """7 swaps on 5 distinct timestamps (two blocks with two swaps each)."""
        root = Path(cfg.paths.datasets_root)
        offsets = [0, 0, 60, 60, 120, 180, 240]
        rows = [[_iso(T0 + timedelta(seconds=o)), 14.0 + i * 0.01, 10000.0, 2e6, 0.001, 1000 + o, f"0x{i:064x}", i]
                for i, o in enumerate(offsets)]
        _write(root / POOL_REL, POOL_HEADER, rows)
        _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T0), 14.0]])
        sync_mod.run_sync(cfg)
        return [T0 + timedelta(seconds=o) for o in sorted(set(offsets))]

    def test_raw_series_has_one_point_per_timestamp_and_paginates_exactly(self, v3cfg):
        distinct = self._dup_pool(v3cfg)
        svc = service_mod.Service(v3cfg)
        end = T0 + timedelta(minutes=10)
        p1 = svc.price_range("LINK", T0, end, 3)
        assert [p.timestamp for p in p1.items] == distinct[:3]
        assert p1.next_start == distinct[3]
        p2 = svc.price_range("LINK", p1.next_start, end, 3)
        assert [p.timestamp for p in p2.items] == distinct[3:] and p2.next_start is None

    def test_grid_series_reports_the_next_grid_point(self, v3cfg):
        self._dup_pool(v3cfg)
        svc = service_mod.Service(v3cfg)
        end = T0 + timedelta(hours=5)
        p1 = svc.price_range("LINK", T0, end, 2, granularity="hour")
        assert [p.timestamp for p in p1.items] == [T0, T0 + timedelta(hours=1)]
        assert p1.next_start == T0 + timedelta(hours=2)
        p2 = svc.price_range("LINK", p1.next_start, end, 10, granularity="hour")
        assert len(p2.items) == 4 and p2.next_start is None

    def test_compare_pages_never_split_a_timestamp(self, v3cfg):
        self._dup_pool(v3cfg)
        root = Path(v3cfg.paths.datasets_root)
        rounds = [0, 60, 60, 120, 180]  # two rounds share T0 + 60 s
        _write(root / "link" / "chainlink_link_usd.csv", CL_HEADER,
               [[_iso(T0 + timedelta(seconds=o)), 14.0 + i] for i, o in enumerate(rounds)])
        sync_mod.run_sync(v3cfg)
        svc = service_mod.Service(v3cfg)
        end = T0 + timedelta(minutes=10)
        p1 = svc.compare("LINK", T0, end, 2)
        assert [p.timestamp for p in p1.items] == [T0] and p1.next_start == T0 + timedelta(seconds=60)
        p2 = svc.compare("LINK", p1.next_start, end, 2)
        assert [p.chainlink_price_usd for p in p2.items] == [15.0, 16.0]
        assert p2.next_start == T0 + timedelta(seconds=120)

    def test_range_confidence_and_compare_headers(self, v3cfg):
        from fastapi.testclient import TestClient
        v3cfg.v3.cache_size = 64
        self._dup_pool(v3cfg)
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            q = {"start": "2024-01-01T00:00:00Z", "end": "2024-01-01T00:10:00Z", "limit": 3}
            r1 = client.get("/v3/prices/LINK", params=q)
            r2 = client.get("/v3/prices/LINK", params=q)
            assert r1.status_code == 200, r1.text
            assert r1.headers["X-Truncated"] == "true"
            assert r1.headers["X-Next-Start"] == "2024-01-01T00:03:00Z"
            link = r1.headers["Link"]  # relative to the request URL (stays valid behind a gateway)
            assert link.startswith("<?") and "start=2024-01-01T00%3A03%3A00Z" in link and link.endswith('rel="next"')
            assert (r1.headers["X-Cache"], r2.headers["X-Cache"]) == ("MISS", "HIT")
            last = client.get("/v3/prices/LINK", params={**q, "start": r1.headers["X-Next-Start"]})
            assert last.headers["X-Truncated"] == "false" and "X-Next-Start" not in last.headers
            assert len(r1.json()) + len(last.json()) == 5

            ts = {"timestamp": "2024-01-01T00:02:00Z"}
            assert client.get("/v3/prices/LINK/at", params=ts).headers["X-Cache"] == "MISS"
            c = client.get("/v3/confidence/LINK/at", params=ts)  # same cache entry as the default /at
            assert c.status_code == 200 and c.headers["X-Cache"] == "HIT"

            cmp = client.get("/v3/compare/LINK", params={"start": q["start"], "end": q["end"]})
            assert cmp.status_code == 200 and cmp.headers["X-Truncated"] == "false"
            assert "X-Cache" not in cmp.headers and cmp.headers["X-Dataset-Version"]

            future = {"timestamp": "2999-01-01T00:00:00Z"}
            fut = client.get("/v3/prices/LINK/at", params=future)
            assert fut.status_code == 200 and fut.json()["unavailable_reason"] == "future_timestamp"
            nf = client.get("/v3/confidence/LINK/at", params=future)
            assert nf.status_code == 404 and "future_timestamp" in nf.json()["detail"]
        store_mod.reset_store(); service_mod.reset_service()


# ---------------------------------------------------------------------------
# Validity fixes of 2026-09-24: quality counters, explicit ties, versions, empty files, Chainlink phases
# ---------------------------------------------------------------------------

def _raw_csv(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def _pool_line(i: int, price: str = None, tvl: str = "2000000.0") -> str:
    t = _iso(T0 + timedelta(minutes=i))
    return f"{t},{price or 14.0 + i * 0.01},1000.0,{tvl},0.001,{1000 + i},0x{i:064x},0"


class TestQuality:
    def test_every_line_is_accounted_for(self, v3cfg):
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        lines = [",".join(POOL_HEADER)] + [_pool_line(i) for i in range(10)]
        lines += [",".join(POOL_HEADER),                          # header repeated in the middle
                  _pool_line(10, price="abc"),                    # unreadable price: row dropped (as V1/V2)
                  _pool_line(11, tvl="n/a"),                      # unreadable TVL: row kept, TVL NULL
                  _pool_line(12) + ",EXTRA",                      # one column too many: kept, truncated
                  _pool_line(13) + ",0x" + "f" * 110_000,         # longer than max_line_size: dropped
                  _pool_line(14)]
        _raw_csv(p, lines)
        res = [r for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL][0]
        q = sync_mod.load_manifest(v3cfg.v3.parquet_path)["datasets"][POOL_REL]["quality"]
        assert q["physical_lines"] == 16 and q["rows_read"] == 15 and q["unaccounted_lines"] == 0
        assert q["reader_dropped"] == {"LINE SIZE OVER MAXIMUM": 1}
        assert q["strict_anomalies"] == {"TOO MANY COLUMNS": 2, "LINE SIZE OVER MAXIMUM": 1}  # line 16 has both
        assert (q["repeated_headers"], q["unreadable_timestamp_rows"], q["unreadable_price_rows"]) == (1, 1, 1)
        assert q["cast_failures"] == {"px_usd": 1, "tvl": 1}
        assert {s["line"] for s in q["samples"] if s.get("line")} == {15, 16}  # 1-based lines of the CSV file
        assert res.issues > 0
        st = store_mod.Store(v3cfg)
        assert st.dataset(POOL_REL).n_rows == 13  # 10 + TVL row + truncated row + last row
        # the row with the unreadable price is skipped: the as-of answer is the previous swap
        assert st.as_of(POOL_REL, T0 + timedelta(minutes=10, seconds=30), ["px_usd"])["ts"] == T0 + timedelta(minutes=9)

    def test_clean_file_has_no_issue(self, v3cfg):
        _simple_link(v3cfg)
        d = sync_mod.load_manifest(v3cfg.v3.parquet_path)["datasets"][POOL_REL]
        assert sync_mod.quality_issues(d["quality"]) == 0
        assert d["quality"]["physical_lines"] == d["quality"]["rows_read"] == 20


class TestTieBreak:
    def _reversed_block(self, cfg) -> datetime:
        p = Path(cfg.paths.datasets_root) / POOL_REL
        same = T0 + timedelta(hours=1)
        rows = [[_iso(T0), 10.0, 1e5, 2e6, 0.001, 1, "0xa", 0],
                [_iso(same), 13.0, 1e5, 2e6, 0.001, 2, "0xd", 2],   # written in reverse log order
                [_iso(same), 12.0, 1e5, 2e6, 0.001, 2, "0xc", 1],
                [_iso(same), 11.0, 1e5, 2e6, 0.001, 2, "0xb", 0]]
        _write(p, POOL_HEADER, rows)
        _write(Path(cfg.paths.datasets_root) / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T0), 11.0]])
        sync_mod.run_sync(cfg)
        return same + timedelta(minutes=5)

    def test_ties_follow_on_chain_order_and_legacy_follows_the_csv(self, v3cfg):
        T = self._reversed_block(v3cfg)
        svc = service_mod.Service(v3cfg)
        resp, _ = svc.price_at("LINK", T)
        ev = resp.provenance.source_event
        assert resp.price_usd == 11.0
        assert (ev.tx_hash, ev.log_index, ev.block_number, ev.rows_at_same_timestamp, ev.tie_break_rule) == (
            "0xb", 0, 2, 3, "first_swap_of_block")
        v3cfg.v3.legacy_truncation = True
        legacy, _ = svc.price_at("LINK", T)
        assert legacy.price_usd == 13.0 and legacy.provenance.source_event.tie_break_rule == "first_csv_row"


class TestVersions:
    def test_history_log_and_versions_in_the_response(self, v3cfg):
        from fastapi.testclient import TestClient
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(10))
        _write(Path(v3cfg.paths.datasets_root) / "link" / "chainlink_link_usd.csv", CL_HEADER, [[_iso(T0), 14.0]])
        sync_mod.run_sync(v3cfg)
        v1 = sync_mod.load_manifest(v3cfg.v3.parquet_path)["version"]
        new = _pool_rows(3, start=T0 + timedelta(hours=1))
        for i, r in enumerate(new):
            r[5], r[6] = 2000 + i, f"0x{100 + i:064x}"
        _append(p, new)
        sync_mod.run_sync(v3cfg)
        m = sync_mod.load_manifest(v3cfg.v3.parquet_path)
        assert m["version"] != v1 and sync_mod.load_history(v3cfg.v3.parquet_path, v1)["version"] == v1
        log = [json.loads(line) for line in sync_mod.sync_log_path(v3cfg.v3.parquet_path).read_text().splitlines()]
        pool = [(e["action"], e["reason"], e["version"]) for e in log if e["dataset"] == POOL_REL]
        assert pool == [("rebuild", "first_build", v1), ("append", None, m["version"])]
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            r = client.get("/v3/prices/LINK/at", params={"timestamp": "2024-01-01T00:10:00Z"}).json()
            prov = r["provenance"]
            assert prov["dataset_version"] == m["version"]
            assert prov["dataset_files"][POOL_REL] == m["datasets"][POOL_REL]["file_version"]
            assert "link/chainlink_link_usd.csv" in prov["dataset_files"]
            assert client.get(f"/v3/datasets/versions/{v1}").json()["version"] == v1
            assert client.get("/v3/datasets/versions/nope").status_code == 404
            ds = {d["file"]: d for d in client.get("/v3/datasets").json()["datasets"]}
            assert ds[POOL_REL]["status"] == "ok" and ds[POOL_REL]["rows"] == 13
        store_mod.reset_store(); service_mod.reset_service()

    def test_sampled_fingerprint_detects_a_rewrite_before_the_tail(self, v3cfg, monkeypatch):
        monkeypatch.setattr(sync_mod, "SAMPLE_STRIDE", 1024)
        monkeypatch.setattr(sync_mod, "FINGERPRINT_BYTES", 256)
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(200))
        sync_mod.run_sync(v3cfg)
        b = bytearray(p.read_bytes())
        i = b.index(b"14.03", 3000)
        b[i:i + 5] = b"14.09"  # same length, far from the end of the file
        p.write_bytes(bytes(b))
        _append(p, _pool_rows(2, start=T0 + timedelta(days=1)))
        res = [r for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL][0]
        assert (res.action, res.reason) == ("rebuild", "sampled_fingerprint_mismatch")

    def test_verify_detects_what_the_samples_miss(self, v3cfg, monkeypatch):
        monkeypatch.setattr(sync_mod, "FINGERPRINT_BYTES", 256)
        p = Path(v3cfg.paths.datasets_root) / POOL_REL
        _write(p, POOL_HEADER, _pool_rows(200))
        sync_mod.run_sync(v3cfg)
        b = bytearray(p.read_bytes())
        i = b.index(b"14.03", 5000)
        b[i:i + 5] = b"14.09"  # past the only sample ([0, 4 KB) with the 256 MB stride): incremental runs miss it
        p.write_bytes(bytes(b))
        assert [r.action for r in sync_mod.run_sync(v3cfg) if r.rel == POOL_REL] == ["unchanged"]
        res = [r for r in sync_mod.run_sync(v3cfg, verify=True) if r.rel == POOL_REL][0]
        assert (res.action, res.reason) == ("rebuild", "verify_mismatch")
        again = [r for r in sync_mod.run_sync(v3cfg, verify=True) if r.rel == POOL_REL][0]
        assert again.action == "verified"  # same bytes; only the date of the last verification moves


class TestEmptyDataset:
    def test_header_only_file_is_reported_as_empty(self, v3cfg):
        from fastapi.testclient import TestClient
        root = Path(v3cfg.paths.datasets_root)
        _write(root / "eth" / "crvusd_weth_curve.csv",
               ["timestamp", "price_weth_per_crvusd", "crvusd_amount", "weth_amount", "volume_crvusd"], [])
        _write(root / ETH_REF_REL, ETH_REF_HEADER, [[_iso(T0), 2200.0, 1e5, 5e6, 0.0005]])
        sync_mod.run_sync(v3cfg)
        resp, _ = service_mod.Service(v3cfg).price_at("ETH", T0 + timedelta(minutes=1), branch="2")
        assert resp.branch_level == "4"
        assert [(r.level, r.rule) for r in resp.provenance.rejected_candidates] == [("2", "dataset_empty")]
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            ds = {d["file"]: d for d in client.get("/v3/datasets").json()["datasets"]}
            assert ds["eth/crvusd_weth_curve.csv"]["status"] == "empty"
        store_mod.reset_store(); service_mod.reset_service()


class TestParameters:
    def test_config_and_confidence_expose_every_parameter_used(self, v3cfg):
        from fastapi.testclient import TestClient
        _simple_link(v3cfg)
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            c = client.get("/v3/config").json()
            assert len(c["config_sha256"]) == 64
            assert c["thresholds"]["seuil_TVL_min_usd"] == v3cfg.thresholds.seuil_TVL_min_usd
            assert c["scoring"]["tvl_score_mode"] == v3cfg.scoring.tvl_score_mode
            assert {"confidence_v2", "v3", "api"} <= c.keys()
            r = client.get("/v3/prices/LINK/at", params={"timestamp": "2024-01-01T00:10:00Z"}).json()
            params = r["confidence"]["parameters"]
            assert set(service_mod.V3_PARAMETER_KEYS) <= params.keys()
            assert params["coh_delta_tol_used"] == v3cfg.confidence_v2.coh.delta_tol_by_asset.get(
                "LINK", v3cfg.confidence_v2.coh.default_delta_tol)
            stripped = service_mod.strip_v3_diagnostics(r)
            assert not set(service_mod.V3_PARAMETER_KEYS) & stripped["confidence"]["parameters"].keys()
            assert not set(service_mod.V3_PROVENANCE_FIELDS) & stripped["provenance"].keys()
        store_mod.reset_store(); service_mod.reset_service()


# --- Chainlink phases -------------------------------------------------------

CL_PHASE_HEADER = ["round_updated_at_utc", "answer_normalized", "phase", "aggregator_round", "feed_proxy_address"]
PROXY = "0x00000000000000000000000000000000000000aa"
BLOCK_S = 12


class FakeRpc:
    """Chain where block b is at T0 + 12 s * b and ``phaseId()`` switches at the given first blocks."""

    def __init__(self, first_block: dict[int, int], head: int):
        self.first_block, self.head, self.calls = first_block, head, 0

    def block_number(self) -> int:
        self.calls += 1
        return self.head

    def block_time(self, block: int) -> datetime:
        self.calls += 1
        return T0 + timedelta(seconds=BLOCK_S * block)

    def phase_id(self, proxy: str, block: int) -> int:
        self.calls += 1
        return max([p for p, b in self.first_block.items() if b <= block], default=0)


def _phase_feed(cfg) -> None:
    """LINK/USD rounds: phase 1 every 10 min for 3 h (it keeps publishing after the switch at T0 + 1 h), phase 2
    every 10 min from T0 + 35 min (before the switch). Phase 2 rounds 10 (209.0) and 11 (310.0) share the second
    T0 + 125 min, the earlier round written first."""
    p1 = [(10 * i, 100.0 + i) for i in range(19)]
    p2 = sorted([(35 + 10 * i, 200.0 + i) for i in range(15)] + [(125, 310.0)])
    rows = [[_iso(T0 + timedelta(minutes=m)), v, 1, r + 1, PROXY] for r, (m, v) in enumerate(p1)]
    rows += [[_iso(T0 + timedelta(minutes=m)), v, 2, r + 1, PROXY] for r, (m, v) in enumerate(p2)]
    rows.sort(key=lambda r: r[0])  # stable: within a second, rounds keep their order
    _write(Path(cfg.paths.datasets_root) / "link" / "chainlink_link_usd.csv", CL_PHASE_HEADER, rows)


class TestChainlinkPhases:
    def test_binary_search_finds_each_switch_and_extends_the_table(self):
        from app.v3 import chainlink_phases as cp
        rpc = FakeRpc({1: 10, 2: 300, 3: 777}, head=1000)
        table = cp.refresh(None, PROXY, rpc)
        assert [(s["phase"], s["block"]) for s in table["switches"]] == [(1, 10), (2, 300), (3, 777)]
        assert table["verified_block"] == 1000
        rpc.first_block[4] = 1500
        rpc.head, rpc.calls = 2000, 0
        table = cp.refresh(table, PROXY, rpc)
        assert [(s["phase"], s["block"]) for s in table["switches"]][-1] == (4, 1500)
        assert rpc.calls < 20  # one new binary search, the known switches are not recomputed
        pt = cp.PhaseTable(table)
        assert [pt.active_phase(T0 + timedelta(seconds=BLOCK_S * b)) for b in (5, 10, 299, 300, 1600)] == [0, 1, 1, 2, 4]

    def test_level_3_reads_the_phase_served_at_t(self, v3cfg):
        _phase_feed(v3cfg)
        sync_mod.run_sync(v3cfg, rpc=FakeRpc({1: 0, 2: 300}, head=900))  # switch at T0 + 1 h; checked to T0 + 3 h
        svc = service_mod.Service(v3cfg)

        def level3(minutes: int):
            r, _ = svc.price_at("LINK", T0 + timedelta(minutes=minutes))
            assert r.branch_level == "3"
            return r

        before = level3(47)   # proxy on phase 1; phase 2 already published at +45 min
        assert before.price_usd == 104.0 and before.provenance.source_event.phase == 1
        assert before.provenance.source_event.tie_break_rule == "latest_round_of_active_phase"
        assert "phase 1 only" in before.provenance.calculation_path[-1]
        after = level3(62)    # proxy on phase 2; phase 1 published again at +60 min
        assert after.price_usd == 202.0 and after.provenance.source_event.phase == 2
        tie = level3(126)     # two phase-2 rounds at +125 min: the latest round wins
        assert tie.price_usd == 310.0 and tie.provenance.source_event.aggregator_round == 11
        assert "chainlink_phase_unverified" not in [w.code for w in tie.warnings]
        late = level3(190)    # after the last on-chain check (T0 + 3 h)
        assert "chainlink_phase_unverified" in [w.code for w in late.warnings]

        v3cfg.v3.legacy_truncation = True  # V1/V2: rounds of every phase, first CSV row on ties
        assert level3(47).price_usd == 201.0 and level3(62).price_usd == 106.0 and level3(126).price_usd == 209.0
        v3cfg.v3.legacy_truncation = False
        v3cfg.v3.chainlink_active_phase_only = False
        assert level3(47).price_usd == 201.0

    def test_compare_lists_the_rounds_the_proxy_served(self, v3cfg):
        _phase_feed(v3cfg)
        sync_mod.run_sync(v3cfg, rpc=FakeRpc({1: 0, 2: 300}, head=900))
        page = service_mod.Service(v3cfg).compare("LINK", T0, T0 + timedelta(minutes=90), 100)
        served = [(p.timestamp - T0, p.chainlink_price_usd) for p in page.items]
        assert served == [(timedelta(minutes=m), v) for m, v in
                          [(0, 100.0), (10, 101.0), (20, 102.0), (30, 103.0), (40, 104.0), (50, 105.0),
                           (65, 203.0), (75, 204.0), (85, 205.0)]]

    def test_without_a_phase_table_every_phase_is_read_and_flagged(self, v3cfg, monkeypatch):
        monkeypatch.delenv(v3cfg.v3.rpc_url_env, raising=False)
        _phase_feed(v3cfg)
        res = sync_mod.run_sync(v3cfg)
        assert any("is not set" in n for r in res for n in r.notes)
        cl = sync_mod.load_manifest(v3cfg.v3.parquet_path)["datasets"]["link/chainlink_link_usd.csv"]["chainlink"]
        assert cl["status"] == "rpc_not_configured" and cl["proxy"] == PROXY
        r, _ = service_mod.Service(v3cfg).price_at("LINK", T0 + timedelta(minutes=47))
        assert r.price_usd == 201.0
        w = [w for w in r.warnings if w.code == "chainlink_phase_unverified"]
        assert w and "no phase switch table" in w[0].message

    def test_rounds_are_deduplicated_on_phase_and_round(self, v3cfg):
        _phase_feed(v3cfg)
        p = Path(v3cfg.paths.datasets_root) / "link" / "chainlink_link_usd.csv"
        _append(p, [[_iso(T0 + timedelta(minutes=180)), 118.0, 1, 19, PROXY]])  # round (1, 19) again
        res = [r for r in sync_mod.run_sync(v3cfg, rpc=FakeRpc({1: 0, 2: 300}, head=900))
               if r.rel == "link/chainlink_link_usd.csv"][0]
        assert res.dups_removed == 1
