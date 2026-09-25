"""Permanent guards on the real data.

* API V3 in legacy mode must reproduce the V2 golden master (tests/golden/v2_golden.jsonl, captured from the
  unchanged V2 CSV engine). The only excused differences are the windows containing the 110 duplicated swaps
  that V3 removes on purpose. Regenerate it only after a deliberate V2 change.
* Without the Chainlink phase filter and the source hierarchy corrections of 2026-09-25, V3 must reproduce its own
  capture of 2026-09-24, taken before the validity fixes of that day (tests/golden/v3_golden_base.jsonl): quality
  counters, explicit ties, versions, the batched reads, the in-DuckDB VWMP and the other additions change no number.
* As configured, V3 must reproduce tests/golden/v3_golden.jsonl (capture_v3.py). Regenerate it only after a
  deliberate change of a V3 number, and say which in the commit.

The tests replay the golden requests on the real Parquet store (``OPENPRICE_CONFIG`` selects another config), so
they are skipped when the store has not been built (`python -m app.v3.sync`).
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

import app.config as config_module
from app.config import load_config
from app.v3 import service as service_mod
from app.v3 import store as store_mod

ROOT = Path(__file__).resolve().parent.parent
GOLDEN = ROOT / "tests" / "golden" / "v2_golden.jsonl"
V3_BASE = ROOT / "tests" / "golden" / "v3_golden_base.jsonl"
V3_GOLDEN = ROOT / "tests" / "golden" / "v3_golden.jsonl"


def _load_compare():
    spec = importlib.util.spec_from_file_location("compare_v3", ROOT / "tests" / "golden" / "compare_v3.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def real_v3(monkeypatch):
    cfg = load_config(os.environ.get("OPENPRICE_CONFIG", ROOT / "config" / "openprice.yaml"))
    if not (cfg.v3.parquet_path / "manifest.json").exists() or not GOLDEN.exists():
        config_module._config = None
        pytest.skip("V3 Parquet store or golden master not available")
    monkeypatch.setattr(config_module, "_config", cfg)
    store_mod.reset_store()
    service_mod.reset_service()
    yield cfg
    store_mod.reset_store()
    service_mod.reset_service()
    config_module._config = None


def test_v3_legacy_mode_reproduces_v2_golden_master(real_v3):
    counts, _, bad = _load_compare().compare(legacy=True)
    assert counts["total"] >= 200
    assert not bad, f"{len(bad)} record(s) differ from V2, first: {bad[0][0]} {bad[0][1][:3]}"
    assert counts["identical"] + counts["dup_zone"] == counts["total"] - counts["skipped"]


def test_v3_default_mode_only_changes_eth_confidence_and_windowed_prices(real_v3):
    """The two truncation fixes must not touch any other asset (Chainlink phase filter and hierarchy corrections off)."""
    counts, _, bad = _load_compare().compare(legacy=False, phase_filter=False, hierarchy_fixes=False)
    assert {rid.split("|")[0] for rid, _ in bad} <= {"ETH"}


def test_v3_additions_change_no_number(real_v3):
    """Without the phase filter and the hierarchy corrections, V3 answers exactly as it did before the validity fixes
    of 2026-09-24."""
    if not V3_BASE.exists():
        pytest.skip("V3 base capture not available")
    counts, _, bad = _load_compare().compare(legacy=False, golden=str(V3_BASE), v3_golden=True, phase_filter=False,
                                             hierarchy_fixes=False)
    assert counts["total"] >= 200
    assert not bad, f"{len(bad)} record(s) differ from the base capture, first: {bad[0][0]} {bad[0][1][:3]}"


def test_v3_matches_its_golden_capture(real_v3):
    if not V3_GOLDEN.exists():
        pytest.skip("V3 golden capture not available")
    counts, _, bad = _load_compare().compare(legacy=False, golden=str(V3_GOLDEN), v3_golden=True,
                                             phase_filter=real_v3.v3.chainlink_active_phase_only)
    assert counts["total"] >= 200
    assert not bad, f"{len(bad)} record(s) differ from v3_golden.jsonl, first: {bad[0][0]} {bad[0][1][:3]}"


def test_sql_vwmp_equals_the_python_formulas_on_real_windows(real_v3):
    """The in-DuckDB MAD filter + VWMP (used on large windows) returns what the V1/V2 Python code computes, on
    hour and day windows of the largest pools (normal and legacy truncation)."""
    import random
    from datetime import datetime, timedelta, timezone

    svc = service_mod.get_service()
    store, eng = svc.store, svc.engine
    rng = random.Random(20260925)
    checked = 0
    for legacy in (False, True):
        real_v3.v3.legacy_truncation = legacy
        for rel in ("eth/eth_usdc_uniswap_v3_005.csv", "eth/weth_usdt_uniswap_v2_03.csv",
                    "uni/uni_weth_uniswap_v2_03.csv"):
            ds = store.dataset(rel)
            if ds is None or not ds.n_rows:
                continue
            price = ds.col("price_usd") or ds.col("price_token_eth")
            vol = ds.col("volume_usd") or ds.col("volume_token")
            lo, hi = ds.min_ts.timestamp(), ds.max_ts.timestamp()
            for _ in range(8):
                t = datetime.fromtimestamp(rng.uniform(lo, hi), timezone.utc).replace(microsecond=0)
                half = timedelta(seconds=rng.choice([1800, 3600, 43200]))
                sql = store.window_vwmp(rel, price, vol, t - half, t + half, real_v3.thresholds.sigma_mad, eng._limit())
                py = eng.window_stats_python(ds, price, vol, t - half, t + half)
                assert sql is None or sql == py, (rel, t, half, sql, py)
                checked += 1
    assert checked >= 24


def test_bulk_ranges_equal_point_by_point_on_real_data(real_v3):
    """Ranges and /compare computed with bulk lookups (app.v3.batch) return the responses of one query per point."""
    from datetime import datetime, timezone

    svc = service_mod.get_service()
    real_v3.v3.cache_size = 0
    svc.cache.size = 0
    u = timezone.utc
    calls = [
        lambda: svc.compare("COMP", datetime(2024, 3, 1, tzinfo=u), datetime(2024, 3, 3, tzinfo=u), 1000),
        lambda: svc.price_range("AAVE", datetime(2022, 5, 10, tzinfo=u), datetime(2022, 5, 12, tzinfo=u), 1000,
                                granularity="hour", include_confidence=True, include_provenance=True),
        lambda: svc.price_range("LINK", datetime(2024, 3, 1, tzinfo=u), datetime(2024, 3, 1, 6, tzinfo=u), 1000,
                                include_confidence=True, include_provenance=True),
    ]
    for call in calls:
        real_v3.v3.batch_ranges = False
        one_by_one = [p.model_dump(mode="json") for p in call().items]
        real_v3.v3.batch_ranges = True
        bulk = [p.model_dump(mode="json") for p in call().items]
        assert len(bulk) >= 8 and bulk == one_by_one
