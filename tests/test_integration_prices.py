"""Integration tests against real CSV datasets.

Requires the real datasets to be present at ~/openprice/datasets.
Each test was manually verified against the live API before being written:
all expected values, branch levels, swap counts and window sizes were
confirmed to be correct. A test failure therefore indicates a regression.

Run only these tests:
    pytest tests/test_integration_prices.py -v

Skip if datasets are absent (CI without data):
    Tests are auto-skipped when ~/openprice/datasets/uni/uni_usdt_uniswap_v3_03.csv
    is not found.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Guard — skip the entire module when real datasets are not present
# ---------------------------------------------------------------------------

_DATASETS_ROOT = Path.home() / "openprice" / "datasets"
_DATASETS_AVAILABLE = (_DATASETS_ROOT / "uni" / "uni_usdt_uniswap_v3_03.csv").exists()

pytestmark = pytest.mark.skipif(
    not _DATASETS_AVAILABLE,
    reason="Real datasets not found at ~/openprice/datasets — integration tests skipped.",
)


# ---------------------------------------------------------------------------
# Fixture — TestClient using the real config and real datasets
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def real_client():
    """FastAPI TestClient pointing at the real CSV datasets.

    Sets app.config._config to the project config so all get_config() calls
    return the real config without monkeypatching individual references.
    """
    import app.config as cfg_mod

    config_path = Path(__file__).parent.parent / "config" / "openprice.yaml"
    original_config = cfg_mod._config
    cfg_mod.load_config(config_path)

    from app.main import app
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        yield c

    cfg_mod._config = original_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(real_client, asset: str, ts: str, *, gran: str = "raw", **kw):
    params = {
        "timestamp": ts,
        "granularity": gran,
        "include_confidence": "false",
        "include_provenance": "false",
        **kw,
    }
    r = real_client.get(f"/v1/prices/{asset}/at", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _get_with_provenance(real_client, asset: str, ts: str, *, gran: str = "raw"):
    params = {
        "timestamp": ts,
        "granularity": gran,
        "include_confidence": "false",
        "include_provenance": "true",
    }
    r = real_client.get(f"/v1/prices/{asset}/at", params=params)
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Raw mode — branch level and exact price
#
# Verified manually on 2026-05-18:
#   UNI  2025-01-15 → 0a  (UNI/USDT TVL=$4.28M, vol24h=$648k — not zombie)
#   UNI  2022-06-01 → 0a  (UNI/USDC TVL=$1.6M — not zombie)
#   LINK 2024-06-01 → 0a  (LINK/USDC TVL=$1.21M — not zombie)
#   LINK 2023-06-01 → 3   (LINK/USDC TVL=$272k < 1M → zombie, 0b impossible: ETH ref empty)
#   AAVE 2025-01-15 → 3   (AAVE/USDC TVL=$52k → zombie, AAVE/USDT TVL too low)
#   COMP 2025-01-15 → 3   (COMP/USDC TVL=$159 → zombie)
#   UNI  2020-01-01 → 4   (before UNI genesis; first swap 2021-05)
#   LINK 2020-09-01 → 4   (before Chainlink LINK feed; first round 2021-03-12)
# ---------------------------------------------------------------------------

class TestRawBranchSelection:

    def test_uni_jan2025_is_0a(self, real_client):
        d = _get(real_client, "UNI", "2025-01-15T12:00:00Z")
        assert d["branch_level"] == "0a"
        assert d["branch_label"] == "direct_stable"
        assert d["price_usd"] == pytest.approx(13.030295016275565, rel=1e-4)
        assert d["granularity"] == "raw"
        assert d["swap_count"] is None  # raw = point read, no VWMP aggregation

    def test_uni_jun2022_is_0a(self, real_client):
        # UNI/USDT is zombie at this date; pipeline falls to UNI/USDC (TVL=$1.6M)
        d = _get(real_client, "UNI", "2022-06-01T12:00:00Z")
        assert d["branch_level"] == "0a"
        assert d["price_usd"] == pytest.approx(5.61734529072465, rel=1e-4)

    def test_link_jun2024_is_0a(self, real_client):
        d = _get(real_client, "LINK", "2024-06-01T12:00:00Z")
        assert d["branch_level"] == "0a"
        assert d["price_usd"] == pytest.approx(18.567710623625604, rel=1e-4)

    def test_link_jun2023_is_3_zombie(self, real_client):
        # LINK/USDC TVL=$272k at this date → zombie; ETH ref files are empty → 0b fails
        d = _get(real_client, "LINK", "2023-06-01T12:00:00Z")
        assert d["branch_level"] == "3"
        assert d["branch_label"] == "chainlink_fallback"
        assert d["data_status"] == "oracle_fallback"
        assert d["price_usd"] == pytest.approx(6.409, rel=1e-4)

    def test_aave_jan2025_is_3_zombie(self, real_client):
        # AAVE/USDC TVL=$52k, AAVE/USDT also below threshold → all 0a pools zombie
        d = _get(real_client, "AAVE", "2025-01-15T12:00:00Z")
        assert d["branch_level"] == "3"
        assert d["price_usd"] == pytest.approx(287.0119, rel=1e-4)

    def test_comp_jan2025_is_3_zombie(self, real_client):
        # COMP/USDC TVL=$159 → permanently dead pool
        d = _get(real_client, "COMP", "2025-01-15T12:00:00Z")
        assert d["branch_level"] == "3"
        assert d["price_usd"] == pytest.approx(77.61517253, rel=1e-4)

    def test_uni_jan2020_is_4_pre_genesis(self, real_client):
        # Before UNI protocol launch and before Chainlink UNI feed
        d = _get(real_client, "UNI", "2020-01-01T12:00:00Z")
        assert d["branch_level"] == "4"
        assert d["branch_label"] == "unavailable"
        assert d["data_status"] == "unavailable"
        assert d["price_usd"] is None
        assert d["unavailable_reason"] is not None

    def test_link_sep2020_is_4_pre_chainlink(self, real_client):
        # Before Chainlink LINK feed started (2021-03-12); ETH ref files empty → no 0b
        d = _get(real_client, "LINK", "2020-09-01T12:00:00Z")
        assert d["branch_level"] == "4"
        assert d["price_usd"] is None


# ---------------------------------------------------------------------------
# Granularity — UNI 2025-01-15T12:00:00Z
#
# Verified manually on 2026-05-18:
#   minute → 0a, 1 swap,   window=60s    (first window ±30s succeeded)
#   hour   → 0a, 6 swaps,  window=3600s
#   day    → 0a, 246 swaps, window=86400s
# ---------------------------------------------------------------------------

class TestGranularityUNI:

    TS = "2025-01-15T12:00:00Z"

    def test_minute_is_0a_first_window(self, real_client):
        d = _get(real_client, "UNI", self.TS, gran="minute")
        assert d["branch_level"] == "0a"
        assert d["granularity"] == "minute"
        assert d["price_usd"] == pytest.approx(13.040298978493366, rel=1e-4)
        assert d["swap_count"] == 1
        assert d["window_seconds"] == 60.0  # first window succeeded, no expansion

    def test_hour_is_0a(self, real_client):
        d = _get(real_client, "UNI", self.TS, gran="hour")
        assert d["branch_level"] == "0a"
        assert d["granularity"] == "hour"
        assert d["price_usd"] == pytest.approx(13.03482169182639, rel=1e-4)
        assert d["swap_count"] == 6
        assert d["window_seconds"] == 3600.0

    def test_day_is_0a(self, real_client):
        d = _get(real_client, "UNI", self.TS, gran="day")
        assert d["branch_level"] == "0a"
        assert d["granularity"] == "day"
        assert d["price_usd"] == pytest.approx(14.052300005676631, rel=1e-4)
        assert d["swap_count"] == 246
        assert d["window_seconds"] == 86400.0

    def test_day_vwmp_differs_from_raw(self, real_client):
        # VWMP over 246 swaps (day window) differs from single point read (raw).
        # Verifies that aggregation actually changes the value, not a pass-through.
        raw = _get(real_client, "UNI", self.TS, gran="raw")
        day = _get(real_client, "UNI", self.TS, gran="day")
        assert raw["price_usd"] != pytest.approx(day["price_usd"], rel=1e-3)


# ---------------------------------------------------------------------------
# Granularity — LINK 2024-06-01T12:00:00Z
#
# Verified manually on 2026-05-18:
#   minute → 3   (DEX swaps too sparse at noon; max R1 window ±450s finds nothing)
#   hour   → 0a, 4 swaps, window=3600s
#   day    → 0a, 50 swaps, window=86400s
#
# The minute → Chainlink fallback demonstrates that R1 expansion (60→120→300→900s)
# is applied and exhausted before the pipeline falls through to level 3.
# ---------------------------------------------------------------------------

class TestGranularityLINK:

    TS = "2024-06-01T12:00:00Z"

    def test_minute_falls_to_chainlink(self, real_client):
        # No LINK/USDC or LINK/USDT swap within ±450s of noon → R1 exhausted → level 3
        d = _get(real_client, "LINK", self.TS, gran="minute")
        assert d["branch_level"] == "3"
        assert d["granularity"] == "minute"
        assert d["swap_count"] is None  # Chainlink is a point read, never VWMP
        assert d["window_seconds"] is None

    def test_hour_is_0a(self, real_client):
        d = _get(real_client, "LINK", self.TS, gran="hour")
        assert d["branch_level"] == "0a"
        assert d["granularity"] == "hour"
        assert d["price_usd"] == pytest.approx(18.538232923197498, rel=1e-4)
        assert d["swap_count"] == 4
        assert d["window_seconds"] == 3600.0

    def test_day_is_0a(self, real_client):
        d = _get(real_client, "LINK", self.TS, gran="day")
        assert d["branch_level"] == "0a"
        assert d["granularity"] == "day"
        assert d["price_usd"] == pytest.approx(18.499815197075428, rel=1e-4)
        assert d["swap_count"] == 50
        assert d["window_seconds"] == 86400.0

    def test_minute_and_raw_use_different_sources(self, real_client):
        # raw → 0a DEX price; minute → level-3 Chainlink price. They differ.
        raw = _get(real_client, "LINK", self.TS, gran="raw")
        minute = _get(real_client, "LINK", self.TS, gran="minute")
        assert raw["branch_level"] == "0a"
        assert minute["branch_level"] == "3"
        assert raw["price_usd"] != pytest.approx(minute["price_usd"], rel=1e-3)


# ---------------------------------------------------------------------------
# R1 window expansion
#
# Verified manually on 2026-05-18:
#   UNI 2025-01-15T12:05:00Z minute:
#     ±30s window (60s total) → 0 swaps
#     ±60s window (120s) → 0 swaps
#     ±150s window (300s) → 0 swaps
#     ±450s window (900s) → 2 swaps found → VWMP returned
#   window_seconds=900 confirms that R1 expanded to the maximum minute step.
# ---------------------------------------------------------------------------

class TestR1WindowExpansion:

    def test_uni_minute_r1_reaches_max_step(self, real_client):
        # T=12:05:00 — no swap within ±30s; R1 expands through 60/120/300 → 900s succeeds
        d = _get(real_client, "UNI", "2025-01-15T12:05:00Z", gran="minute")
        assert d["branch_level"] == "0a"
        assert d["swap_count"] == 2
        assert d["window_seconds"] == 900.0  # max minute expansion step

    def test_uni_minute_r1_expansion_visible_in_provenance(self, real_client):
        d = _get_with_provenance(real_client, "UNI", "2025-01-15T12:05:00Z", gran="minute")
        prov = d["provenance"]
        assert prov["swap_count"] == 2
        assert prov["window_seconds"] == 900.0
        assert prov["excluded_swaps"] == 0

    def test_uni_minute_at_exact_swap_time_uses_first_window(self, real_client):
        # T=12:00:00 has a swap directly in the ±30s window → no expansion needed
        d = _get(real_client, "UNI", "2025-01-15T12:00:00Z", gran="minute")
        assert d["branch_level"] == "0a"
        assert d["window_seconds"] == 60.0  # first window; not expanded


# ---------------------------------------------------------------------------
# Zombie persistence across granularities
#
# When all 0a pools are zombie and 0b is impossible (empty ETH ref files),
# every granularity falls to level 3. VWMP windows cannot revive a zombie pool.
# Verified manually on 2026-05-18 for LINK 2023-06-01, AAVE 2025-01-15.
# ---------------------------------------------------------------------------

class TestZombiePersistsAcrossGranularities:

    @pytest.mark.parametrize("gran", ["raw", "minute", "hour", "day"])
    def test_link_jun2023_always_chainlink(self, real_client, gran):
        d = _get(real_client, "LINK", "2023-06-01T12:00:00Z", gran=gran)
        assert d["branch_level"] == "3", f"gran={gran}: expected 3, got {d['branch_level']}"

    @pytest.mark.parametrize("gran", ["raw", "minute", "hour", "day"])
    def test_aave_jan2025_always_chainlink(self, real_client, gran):
        d = _get(real_client, "AAVE", "2025-01-15T12:00:00Z", gran=gran)
        assert d["branch_level"] == "3", f"gran={gran}: expected 3, got {d['branch_level']}"


# ---------------------------------------------------------------------------
# Level 4 persistence across granularities
#
# When there is no data at all (pre-genesis timestamps), every granularity
# returns level 4 with price_usd=null.
# Verified manually on 2026-05-18.
# ---------------------------------------------------------------------------

class TestLevel4PersistsAcrossGranularities:

    @pytest.mark.parametrize("gran", ["raw", "minute", "hour", "day"])
    def test_uni_pre_genesis_always_4(self, real_client, gran):
        d = _get(real_client, "UNI", "2020-01-01T12:00:00Z", gran=gran)
        assert d["branch_level"] == "4", f"gran={gran}: expected 4, got {d['branch_level']}"
        assert d["price_usd"] is None


# ---------------------------------------------------------------------------
# Response shape — windowed granularities must expose VWMP metadata
# ---------------------------------------------------------------------------

class TestWindowedResponseShape:

    def test_minute_exposes_swap_count_and_window(self, real_client):
        d = _get(real_client, "UNI", "2025-01-15T12:00:00Z", gran="minute")
        assert d["swap_count"] is not None
        assert d["window_seconds"] is not None
        assert d["granularity"] == "minute"

    def test_hour_exposes_swap_count_and_window(self, real_client):
        d = _get(real_client, "UNI", "2025-01-15T12:00:00Z", gran="hour")
        assert d["swap_count"] is not None
        assert d["window_seconds"] is not None
        assert d["granularity"] == "hour"

    def test_day_exposes_swap_count_and_window(self, real_client):
        d = _get(real_client, "UNI", "2025-01-15T12:00:00Z", gran="day")
        assert d["swap_count"] is not None
        assert d["window_seconds"] is not None
        assert d["granularity"] == "day"

    def test_raw_has_null_swap_count_and_window(self, real_client):
        d = _get(real_client, "UNI", "2025-01-15T12:00:00Z", gran="raw")
        assert d["swap_count"] is None
        assert d["window_seconds"] is None
        assert d["granularity"] == "raw"

    def test_chainlink_fallback_has_null_swap_count(self, real_client):
        # Level-3 Chainlink is always a point read; it never has VWMP fields
        for gran in ("raw", "minute", "hour", "day"):
            d = _get(real_client, "LINK", "2023-06-01T12:00:00Z", gran=gran)
            assert d["swap_count"] is None, f"gran={gran}: Chainlink should not have swap_count"
            assert d["window_seconds"] is None, f"gran={gran}: Chainlink should not have window_seconds"
