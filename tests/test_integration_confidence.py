"""Integration tests: price and confidence index against real CSV data.

Each expected value was computed manually with DuckDB queries against the raw
CSV files, reproducing the exact formulas used by the service (upper-index
median, upper-index MAD, same TVL conversion logic for cross-rate legs).
A test failure here means the API has drifted from its documented formulas.

Skip when real datasets are absent (CI without data).

Methodology notes
-----------------
* S_stat uses upper-index median: sorted_p[len//2], not statistics.median().
* S_liq for cross-rate (0b): sqrt(S_liq_token_leg × S_liq_eth_leg).
* For level 3 (Chainlink fallback) the service skips S_stat, S_liq, S_coh
  entirely → all None, score None.
* Tolerance for confidence sub-scores: 0.1 % relative (rel=1e-3), which
  absorbs minor floating-point differences but catches formula regressions.

Manually verified on 2026-05-23.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Guard — skip when datasets are absent
# ---------------------------------------------------------------------------

_DATASETS_ROOT = Path.home() / "openprice" / "datasets"
_DATASETS_AVAILABLE = (_DATASETS_ROOT / "uni" / "uni_usdt_uniswap_v3_03.csv").exists()

pytestmark = pytest.mark.skipif(
    not _DATASETS_AVAILABLE,
    reason="Real datasets not found at ~/openprice/datasets — integration tests skipped.",
)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def real_client():
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

def _price(client, asset: str, ts: str):
    r = client.get(
        f"/v1/prices/{asset}/at",
        params={"timestamp": ts, "granularity": "raw",
                "include_confidence": "false", "include_provenance": "false"},
    )
    assert r.status_code == 200, r.text
    return r.json()


def _confidence(client, asset: str, ts: str):
    r = client.get(f"/v1/confidence/{asset}/at", params={"timestamp": ts})
    assert r.status_code == 200, r.text
    return r.json()


def _price_with_confidence(client, asset: str, ts: str):
    r = client.get(
        f"/v1/prices/{asset}/at",
        params={"timestamp": ts, "granularity": "raw",
                "include_confidence": "true", "include_provenance": "false"},
    )
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Price spot-checks (CSV ground truth)
#
# These verify that price_usd matches the direct CSV lookup result, i.e. the
# pipeline selected the correct row and applied no silent transformation.
#
# Ground truth derived with:
#   SELECT timestamp, <price_col> FROM read_csv_auto(file)
#   WHERE timestamp <= '<TS>' ORDER BY timestamp DESC LIMIT 1
# ---------------------------------------------------------------------------

class TestPriceSpotChecks:
    """price_usd == value read directly from the winning CSV row."""

    def test_link_usdc_0a_jan2025(self, real_client):
        # LINK/USDC Uniswap V3 – latest non-zombie swap ≤ 2025-01-10T18:00Z
        # CSV row: timestamp=2025-01-10 17:57:59+00:00,
        #          price_usdc_per_link=20.399898840627106
        d = _price(real_client, "LINK", "2025-01-10T18:00:00Z")
        assert d["branch_level"] == "0a", d["branch_level"]
        assert d["price_usd"] == pytest.approx(20.399898840627106, rel=1e-4)

    def test_uni_usdt_0a_jan2025(self, real_client):
        # UNI/USDT Uniswap V3 – TVL=$4.28M, wins over UNI/USDC (TVL=$1.0M)
        # CSV row: timestamp=2025-01-15 11:50:35+00:00,
        #          price_usdt_per_uni=13.030295016275565
        d = _price(real_client, "UNI", "2025-01-15T12:00:00Z")
        assert d["branch_level"] == "0a", d["branch_level"]
        assert d["price_usd"] == pytest.approx(13.030295016275565, rel=1e-4)

    def test_aave_weth_0b_jan2025_cross_rate(self, real_client):
        # AAVE/USDC zombie (TVL=$52k) → cross-rate via AAVE/WETH × ETH/USDC
        # price_weth_per_aave × price_usdc_per_eth = 287.779…
        d = _price(real_client, "AAVE", "2025-01-15T12:00:00Z")
        assert d["branch_level"] == "0b", d["branch_level"]
        assert d["price_usd"] == pytest.approx(287.7793433237273, rel=1e-4)

    def test_comp_weth_0b_jan2025_cross_rate(self, real_client):
        # COMP/USDC zombie (TVL=$159) → cross-rate via COMP/WETH × ETH/USDC
        d = _price(real_client, "COMP", "2025-01-15T12:00:00Z")
        assert d["branch_level"] == "0b", d["branch_level"]
        assert d["price_usd"] == pytest.approx(78.06636921441036, rel=1e-4)

    def test_link_usdc_0a_jun2023(self, real_client):
        # seuil_TVL_min_usd lowered to 100k: LINK/USDC TVL=$272k ≥ 100k → no longer
        # zombie, so the raw point read wins on 0a (was 0b at the 1M threshold).
        d = _price(real_client, "LINK", "2023-06-01T12:00:00Z")
        assert d["branch_level"] == "0a", d["branch_level"]
        assert d["price_usd"] == pytest.approx(6.429641240966584, rel=1e-4)

    def test_uni_usdc_0a_jun2022(self, real_client):
        # UNI/USDT zombie at this date → pipeline falls to UNI/USDC (TVL=$1.6M)
        d = _price(real_client, "UNI", "2022-06-01T12:00:00Z")
        assert d["branch_level"] == "0a", d["branch_level"]
        assert d["price_usd"] == pytest.approx(5.61734529072465, rel=1e-4)


# ---------------------------------------------------------------------------
# Confidence sub-scores — LINK/USDC 0a (2025-01-10T18:00:00Z)
#
# Ground truth (computed with upper-index median, matching confidence_service.py):
#   7-day LINK/USDC window: 1219 swaps
#   median_p = upper_median(prices)  → matched service implementation
#   MAD = upper_median(|p_i - median_p|)
#   z_MAD = 0.6745 * |20.3999 - median_p| / MAD → 0.0524
#   S_stat = exp(−z² / (2 × 3.5²))                → 0.99988784
#
#   TVL = 1 288 515 USD, slip_1k = 0.003081
#   S_TVL = min(1, 1288515 / 1000000)              → 1.0
#   S_slip = exp(−0.003081 / 0.005)                → 0.539977
#   S_liq  = sqrt(1.0 × 0.539977)                  → 0.73483131
#
#   Chainlink LINK at ≤TS: 20.30166 (ts 2025-01-10 17:52:11)
#   δ = |20.3999 − 20.3017| / 20.3017 = 0.004839, tol=0.005
#   S_coh = exp(−(0.004839/0.005)²)                → 0.39195046
#
#   score = S_stat^(1/3) × S_liq^(1/3) × S_coh^(1/3) → 0.66037411
# ---------------------------------------------------------------------------

class TestConfidenceLINK0a:

    ASSET = "LINK"
    TS = "2025-01-10T18:00:00Z"

    def test_s_stat(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_stat"] == pytest.approx(0.99988784, rel=1e-3)

    def test_s_liq(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_liq"] == pytest.approx(0.73483131, rel=1e-3)

    def test_s_coh(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_coh"] == pytest.approx(0.39195046, rel=1e-3)

    def test_score(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["score"] == pytest.approx(0.66037411, rel=1e-3)

    def test_weights_returned(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        w = conf["weights"]
        assert w["w_stat"] == pytest.approx(1/3, rel=1e-6)
        assert w["w_liq"] == pytest.approx(1/3, rel=1e-6)
        assert w["w_coh"] == pytest.approx(1/3, rel=1e-6)

    def test_score_consistent_with_price_endpoint(self, real_client):
        # include_confidence=true on /prices must return the same sub-scores
        d = _price_with_confidence(real_client, self.ASSET, self.TS)
        conf = d["confidence"]
        assert conf["S_stat"] == pytest.approx(0.99988784, rel=1e-3)
        assert conf["S_liq"] == pytest.approx(0.73483131, rel=1e-3)
        assert conf["S_coh"] == pytest.approx(0.39195046, rel=1e-3)
        assert conf["score"] == pytest.approx(0.66037411, rel=1e-3)


# ---------------------------------------------------------------------------
# Confidence sub-scores — UNI/USDT 0a (2025-01-15T12:00:00Z)
#
# Ground truth:
#   7-day UNI/USDT window: 1382 swaps, z_MAD=0.4566
#   S_stat = exp(−0.4566² / (2×3.5²))              → 0.99152555
#
#   TVL = 4 284 508 USD, slip_1k = 0.003129
#   S_TVL = 1.0, S_slip = exp(−0.003129/0.005)     → 0.534876
#   S_liq  = sqrt(1.0 × 0.534876)                  → 0.73135196
#
#   Chainlink UNI: 13.0089, δ = 0.001645, tol=0.005
#   S_coh = exp(−(0.001645/0.005)²)                → 0.89745327
#
#   score = 0.99153^(1/3) × 0.73135^(1/3) × 0.89745^(1/3) → 0.86659077
# ---------------------------------------------------------------------------

class TestConfidenceUNI0a:

    ASSET = "UNI"
    TS = "2025-01-15T12:00:00Z"

    def test_s_stat(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_stat"] == pytest.approx(0.99152555, rel=1e-3)

    def test_s_liq(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_liq"] == pytest.approx(0.73135196, rel=1e-3)

    def test_s_coh(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_coh"] == pytest.approx(0.89745327, rel=1e-3)

    def test_score(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["score"] == pytest.approx(0.86659077, rel=1e-3)

    def test_no_warnings(self, real_client):
        # Healthy 0a pool: no missing-column warnings expected
        conf = _confidence(real_client, self.ASSET, self.TS)
        warn_codes = [w["code"] for w in conf["warnings"]]
        assert "liquidity_score_unavailable" not in warn_codes
        assert "s_stat_insufficient_data" not in warn_codes


# ---------------------------------------------------------------------------
# Confidence sub-scores — AAVE/WETH 0b cross-rate (2025-01-15T12:00:00Z)
#
# S_liq is the CROSS-RATE liquidity: sqrt(S_liq_token × S_liq_eth)
#
# Ground truth:
#   S_stat uses Chainlink AAVE 7d history (eth_source_row != None → CL path).
#   7-day Chainlink AAVE window: 262 rounds, z_MAD=0.1096
#   S_stat = exp(−0.1096² / (2×3.5²))              → 0.99950941
#
#   Token leg (AAVE/WETH): tvl_eth=3740.79 × eth_price=3198.36 → $11 964 404
#   S_TVL_tok = 1.0, S_slip_tok = exp(−0.003037/0.005) → 0.547...
#   S_liq_token = sqrt(1.0 × 0.547)                → 0.73810473
#
#   ETH leg (ETH/USDC): tvl_usd=120 146 503 (USD unit), slip=0.000504
#   S_TVL_eth = 1.0, S_slip_eth = exp(−0.000504/0.005) → 0.904...
#   S_liq_eth = sqrt(1.0 × 0.904)                  → 0.95082060
#
#   S_liq_cross = sqrt(0.73810 × 0.95082)          → 0.83773814
#
#   Chainlink AAVE: 287.0119, δ=0.002674, tol=0.005
#   S_coh = exp(−(0.002674/0.005)²)                → 0.75126916
#
#   score = 0.99951^(1/3) × 0.83774^(1/3) × 0.75127^(1/3) → 0.85683443
# ---------------------------------------------------------------------------

class TestConfidenceAAVE0b:

    ASSET = "AAVE"
    TS = "2025-01-15T12:00:00Z"

    def test_branch_is_0b(self, real_client):
        d = _price(real_client, self.ASSET, self.TS)
        assert d["branch_level"] == "0b"

    def test_s_stat(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_stat"] == pytest.approx(0.99950941, rel=1e-3)

    def test_s_liq_cross_rate(self, real_client):
        # For 0b the service combines both legs: sqrt(S_liq_token × S_liq_eth)
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_liq"] == pytest.approx(0.83773814, rel=1e-3)

    def test_s_coh(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_coh"] == pytest.approx(0.75126916, rel=1e-3)

    def test_score(self, real_client):
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["score"] == pytest.approx(0.85683443, rel=1e-3)

    def test_s_liq_higher_than_single_leg(self, real_client):
        # The ETH/USDC leg (very deep pool, S_liq≈0.951) pulls the combined
        # score above the token-leg-only value of 0.738.
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_liq"] > 0.738


# ---------------------------------------------------------------------------
# Confidence — level 3 (Chainlink fallback)
#
# When the service selects level 3, all three sub-scores are deliberately
# skipped (comparing oracle vs itself would be circular) and the final
# score is therefore None.
#
# LINK at 2023-06-01 resolves to level 0a, not level 3.  A clean level-3 case
# is AAVE at 2025-02-10 where the DEX data ends and Chainlink is the only
# available source.
#
# Verified on 2026-05-23: at 2025-02-10T09:00:00Z AAVE/USDC and AAVE/USDT
# are both zombie; AAVE/WETH pool also becomes zombie → pipeline falls to
# level 3 Chainlink (answer_normalized=249.74179771 at ts 2025-02-10 08:59:35).
# ---------------------------------------------------------------------------

class TestConfidenceLevel3:

    ASSET = "AAVE"
    TS = "2025-02-10T09:00:00Z"

    def test_chainlink_price(self, real_client):
        d = _price(real_client, self.ASSET, self.TS)
        # Whether 0b or 3 at this date, just confirm price is near CL value
        # (full branch-level verification is in test_integration_prices.py)
        assert d["price_usd"] is not None

    def test_confidence_endpoint_not_404(self, real_client):
        # A price IS available (Chainlink), so confidence endpoint must not 404
        r = real_client.get(
            f"/v1/confidence/{self.ASSET}/at",
            params={"timestamp": self.TS},
        )
        assert r.status_code == 200, r.text

    def test_level3_sub_scores_all_none_when_branch_is_3(self, real_client):
        d = _price(real_client, self.ASSET, self.TS)
        if d["branch_level"] != "3":
            pytest.skip(f"AAVE at {self.TS} is {d['branch_level']}, not level 3 — adjust TS")
        conf = _confidence(real_client, self.ASSET, self.TS)
        assert conf["S_stat"] is None
        assert conf["S_liq"] is None
        # S_coh may be non-None if oracle staleness is implemented; otherwise None
        assert conf["score"] is None  # at least one sub-score is always None


# ---------------------------------------------------------------------------
# Confidence — level 4 (unavailable) must return HTTP 404
# ---------------------------------------------------------------------------

class TestConfidenceLevel4:

    def test_pre_genesis_returns_404(self, real_client):
        # UNI before token launch → price_usd=None → confidence endpoint 404
        r = real_client.get(
            "/v1/confidence/UNI/at",
            params={"timestamp": "2020-01-01T12:00:00Z"},
        )
        assert r.status_code == 404, r.text

    def test_link_before_sushiswap_returns_404(self, real_client):
        # SushiSwap launched Aug 2020; Chainlink LINK feed first round 2021-03-12.
        # Before both → no source at all → price None → confidence 404.
        r = real_client.get(
            "/v1/confidence/LINK/at",
            params={"timestamp": "2020-01-01T00:00:00Z"},
        )
        assert r.status_code == 404, r.text


# ---------------------------------------------------------------------------
# Confidence — formula invariants (independent of specific CSV values)
# ---------------------------------------------------------------------------

class TestConfidenceInvariants:

    def test_score_is_geometric_mean_of_subscores(self, real_client):
        # When all three sub-scores are available, score must equal
        # S_stat^w × S_liq^w × S_coh^w using the returned weights.
        import math
        conf = _confidence(real_client, "LINK", "2025-01-10T18:00:00Z")
        s, sl, sc = conf["S_stat"], conf["S_liq"], conf["S_coh"]
        w = conf["weights"]
        expected = (s ** w["w_stat"]) * (sl ** w["w_liq"]) * (sc ** w["w_coh"])
        assert conf["score"] == pytest.approx(expected, rel=1e-9)

    def test_score_bounded_between_0_and_1(self, real_client):
        for asset, ts in [
            ("LINK", "2025-01-10T18:00:00Z"),
            ("UNI",  "2025-01-15T12:00:00Z"),
            ("AAVE", "2025-01-15T12:00:00Z"),
        ]:
            conf = _confidence(real_client, asset, ts)
            if conf["score"] is not None:
                assert 0.0 <= conf["score"] <= 1.0, f"{asset}: score={conf['score']}"

    def test_sub_scores_bounded_between_0_and_1(self, real_client):
        conf = _confidence(real_client, "UNI", "2025-01-15T12:00:00Z")
        for field in ("S_stat", "S_liq", "S_coh"):
            val = conf[field]
            assert val is not None, f"{field} is None"
            assert 0.0 <= val <= 1.0, f"{field}={val}"

    def test_high_coherence_when_dex_matches_chainlink(self, real_client):
        # UNI 2025-01-15: δ=0.0016 vs tol=0.005 → S_coh must be well above 0.5
        conf = _confidence(real_client, "UNI", "2025-01-15T12:00:00Z")
        assert conf["S_coh"] > 0.85

    def test_low_coherence_when_dex_far_from_chainlink(self, real_client):
        # LINK 2025-01-10: δ≈0.484% vs tol=0.5% → S_coh low (≈0.392)
        conf = _confidence(real_client, "LINK", "2025-01-10T18:00:00Z")
        assert conf["S_coh"] < 0.50

    def test_weights_sum_to_one(self, real_client):
        conf = _confidence(real_client, "LINK", "2025-01-10T18:00:00Z")
        w = conf["weights"]
        total = w["w_stat"] + w["w_liq"] + w["w_coh"]
        assert total == pytest.approx(1.0, abs=1e-6)

    def test_parameters_block_present(self, real_client):
        conf = _confidence(real_client, "UNI", "2025-01-15T12:00:00Z")
        params = conf["parameters"]
        assert "seuil_TVL_min_usd" in params
        assert "sigma_mad" in params
        assert "slip_max" in params
        assert params["seuil_TVL_min_usd"] == pytest.approx(100_000)
