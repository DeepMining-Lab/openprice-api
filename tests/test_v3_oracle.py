"""Independent check of the S_stat and day-VWMP values of API V3 on the real data.

The validity audit of 2026-09-21/22 found that V1/V2 truncate the S_stat 7-day window and the windowed VWMP read
to their 10 000 oldest rows. This test recomputes both quantities in plain Python, from the rows of the Parquet
store read directly with DuckDB (none of the V3 store, engine or V1/V2 helpers is used), on the dates the audit
cites, and compares them with what V3 returns. It also pins the S_stat values the audit expected from the complete
window. Skipped when the real Parquet store has not been built (``python -m app.v3.sync``).
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

import app.config as config_module
from app.config import load_config
from app.v3 import service as service_mod
from app.v3 import store as store_mod

ROOT = Path(__file__).resolve().parent.parent
ETH_POOL = "eth/eth_usdc_uniswap_v3_005.csv"
SIGMA_MAD = 3.5

# Complete-window S_stat expected by the audit (V1/V2 returned 0.000, 0.000 and 0.837 on these dates).
AUDIT_S_STAT = {"2022-05-12": 0.840, "2024-08-05": 0.872, "2025-06-01": 0.944}
DAY_DATES = ["2022-05-12", "2023-03-11", "2024-08-05", "2025-06-01", "2026-05-20"]


@pytest.fixture(scope="module")
def real():
    cfg = load_config(os.environ.get("OPENPRICE_CONFIG", ROOT / "config" / "openprice.yaml"))
    manifest = cfg.v3.parquet_path / "manifest.json"
    if not manifest.exists():
        config_module._config = None
        pytest.skip("V3 Parquet store not available")
    d = json.loads(manifest.read_text())["datasets"].get(ETH_POOL)
    if not d or not d["segments"]:
        config_module._config = None
        pytest.skip("ETH/USDC pool not in the V3 store")
    folder = cfg.v3.parquet_path / ETH_POOL.replace("/", "__").removesuffix(".csv")
    files = [str(folder / s["file"]) for s in d["segments"]]
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(f"CREATE VIEW pool AS SELECT * FROM read_parquet({files!r})")
    old = config_module._config
    config_module._config = cfg
    store_mod.reset_store()
    service_mod.reset_service()
    yield cfg, con, service_mod.get_service()
    store_mod.reset_store()
    service_mod.reset_service()
    config_module._config = old


def _t(day: str) -> datetime:
    return datetime.fromisoformat(day).replace(hour=12, tzinfo=timezone.utc)


def _upper_median(values: list[float]) -> float:
    s = sorted(values)
    return s[len(s) // 2]


def _s_stat(window: list[float], price: float) -> float:
    med = _upper_median(window)
    mad = _upper_median([abs(p - med) for p in window])
    if mad == 0:
        return 1.0
    z = 0.6745 * abs(price - med) / mad
    return math.exp(-(z ** 2) / (2 * SIGMA_MAD ** 2))


def _vwmp_after_mad(prices: list[float], volumes: list[float]) -> tuple[float, int]:
    """MAD filter (modified z-score <= sigma, unfiltered fallback) then volume-weighted median price."""
    kept = list(zip(prices, volumes))
    if len(prices) >= 3:
        med = _upper_median(prices)
        mad = _upper_median([abs(p - med) for p in prices])
        if mad != 0:
            kept = [(p, v) for p, v in zip(prices, volumes) if 0.6745 * abs(p - med) / mad <= SIGMA_MAD] or kept
    if len(kept) == 1:
        return kept[0][0], 1
    total = sum(v for _, v in kept)
    if total <= 0:
        return _upper_median([p for p, _ in kept]), len(kept)
    cum = 0.0
    for p, v in sorted(kept, key=lambda x: x[0]):
        cum += v
        if cum >= total / 2:
            return p, len(kept)
    return max(p for p, _ in kept), len(kept)


@pytest.mark.parametrize("day", sorted(AUDIT_S_STAT))
def test_s_stat_of_a_raw_eth_price_uses_the_whole_7_day_window(real, day):
    cfg, con, svc = real
    T = _t(day)
    window = [r[0] for r in con.execute(
        "SELECT px_usd FROM pool WHERE ts >= ? AND ts < ? AND px_usd IS NOT NULL", [T - timedelta(days=7), T]).fetchall()]
    assert len(window) > cfg.api.max_limit  # the window V1/V2 truncated
    price = con.execute("SELECT px_usd FROM pool WHERE ts <= ? ORDER BY ts DESC, block_number, log_index LIMIT 1",
                        [T]).fetchone()[0]
    expected = _s_stat(window, price)

    resp, _ = svc.price_at("ETH", T)
    assert (resp.branch_level, resp.price_raw_in_quote) == ("0a", price)
    assert math.isclose(resp.confidence.subscores["S_stat"], expected, rel_tol=1e-9)
    assert abs(expected - AUDIT_S_STAT[day]) < 5e-4


@pytest.mark.parametrize("day", DAY_DATES)
def test_day_vwmp_reads_every_swap_of_the_24_hour_window(real, day):
    cfg, con, svc = real
    T = _t(day)
    rows = con.execute("SELECT px_usd, vol_usd FROM pool WHERE ts >= ? AND ts < ? AND px_usd IS NOT NULL",
                       [T - timedelta(hours=12), T + timedelta(hours=12)]).fetchall()
    price, n_clean = _vwmp_after_mad([r[0] for r in rows], [r[1] if r[1] is not None else 1.0 for r in rows])

    resp, _ = svc.price_at("ETH", T, granularity="day")
    assert resp.branch_level == "0a"
    assert resp.price_raw_in_quote == price
    assert resp.swap_count == n_clean
