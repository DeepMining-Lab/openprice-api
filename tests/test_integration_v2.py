"""Integration tests for the API V2 confidence model against real CSV data.

Centerpiece: the USDC depeg of 2023-03-11 (SVB collapse), which is the
validation case from the expert review (CLAUDE.md §22 §1). In V1 the AAVE DEX
price (quoted in USDC) diverges ~9 % from the Chainlink AAVE/USD feed because
USDC itself is worth ~$0.91, so S_coh collapses to ~0 — a false alarm. In V2
the price is neutralized by the USDC/USD peg before S_coh, so coherence is
preserved and the depeg is carried by the separate S_peg sub-score.

Ground truth read directly from the CSVs on 2026-06-01:
  USDC/USD peg ≤ T : 0.90965689   (2023-03-11 11:33:59)
  Chainlink AAVE   : 65.23103673  (2023-03-11 11:37:35)
  AAVE branch      : 0b cross-rate (AAVE/USDC and AAVE/USDT are zombie)
  raw price (USDC) : 71.6691…
  neutralized USD  : 71.6691 × 0.90965689 ≈ 65.194  ≈ Chainlink

Skipped when the real datasets (incl. the stablecoins peg feeds) are absent.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_DATASETS_ROOT = Path.home() / "openprice" / "datasets"
_DATASETS_AVAILABLE = (
    (_DATASETS_ROOT / "aave" / "aave_weth_uniswap_v3_03.csv").exists()
    and (_DATASETS_ROOT / "stablecoins" / "chainlink_usdc_usd.csv").exists()
)

pytestmark = pytest.mark.skipif(
    not _DATASETS_AVAILABLE,
    reason="Real datasets / peg feeds not found — V2 integration tests skipped.",
)


@pytest.fixture(scope="module")
def real_client():
    import app.config as cfg_mod

    config_path = Path(__file__).parent.parent / "config" / "openprice.yaml"
    original = cfg_mod._config
    cfg_mod.load_config(config_path)

    from app.main import app
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        yield c

    cfg_mod._config = original


def _v2_price(client, asset, ts, **params):
    p = {"timestamp": ts, "include_confidence": "true", "include_provenance": "true"}
    p.update(params)
    r = client.get(f"/v2/prices/{asset}/at", params=p)
    assert r.status_code == 200, r.text
    return r.json()


def _v1_confidence(client, asset, ts):
    r = client.get(f"/v1/confidence/{asset}/at", params={"timestamp": ts})
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------------------
# USDC depeg — 2023-03-11 (SVB)
# ---------------------------------------------------------------------------

class TestUSDCDepeg:
    ASSET = "AAVE"
    TS = "2023-03-11T12:00:00Z"

    def test_branch_is_cross_rate(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert d["branch_level"] == "0b"

    def test_quote_currency_and_peg(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert d["quote_currency"] == "USDC"
        assert d["quote_currency_peg"] == pytest.approx(0.90965689, rel=1e-4)

    def test_neutralized_price_matches_chainlink(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        # headline price_usd is the neutralized price; ~Chainlink AAVE 65.23.
        assert d["price_usd"] == pytest.approx(65.19, rel=2e-3)
        assert d["price_neutralized_usd"] == pytest.approx(
            d["price_raw_in_quote"] * d["quote_currency_peg"], rel=1e-9
        )

    def test_s_coh_does_not_collapse(self, real_client):
        # V2: neutralized price is coherent with Chainlink → S_coh stays high.
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert d["confidence"]["subscores"]["S_coh"] > 0.9

    def test_s_peg_carries_the_depeg(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        # ~9 % depeg vs 0.25 % tol → S_peg ≈ 0.
        assert d["confidence"]["S_peg"] < 1e-6

    def test_v2_recovers_where_v1_collapses(self, real_client):
        # The whole point of recommendation 2: V1 S_coh collapses, V2 does not.
        v1 = _v1_confidence(real_client, self.ASSET, self.TS)
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert v1["S_coh"] < 1e-6                      # V1 false alarm
        assert d["confidence"]["subscores"]["S_coh"] > 0.9   # V2 preserved
        assert d["confidence"]["C"] > 0.5

    def test_warnings_expose_neutralization(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        codes = [w["code"] for w in d["warnings"]]
        assert "coh_neutralized_peg" in codes


# ---------------------------------------------------------------------------
# Healthy peg — V2 stays close to V1 when USDC ≈ $1
# ---------------------------------------------------------------------------

class TestHealthyPeg:
    ASSET = "UNI"
    TS = "2025-01-15T12:00:00Z"

    def test_peg_near_one(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert d["quote_currency"] in ("USDC", "USDT")
        assert d["quote_currency_peg"] == pytest.approx(1.0, abs=0.01)

    def test_neutralized_close_to_raw(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert d["price_usd"] == pytest.approx(d["price_raw_in_quote"], rel=0.01)

    def test_s_peg_high(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        assert d["confidence"]["S_peg"] > 0.5


# ---------------------------------------------------------------------------
# Level 3 fallback — S_peg not applicable
# ---------------------------------------------------------------------------

class TestLevel3PegNotApplicable:
    ASSET = "AAVE"
    TS = "2025-02-10T09:00:00Z"

    def test_s_peg_not_applicable_on_chainlink_fallback(self, real_client):
        d = _v2_price(real_client, self.ASSET, self.TS)
        if d["branch_level"] != "3":
            pytest.skip(f"AAVE at {self.TS} is {d['branch_level']}, not level 3")
        assert d["quote_currency"] is None
        assert d["confidence"]["S_peg"] is None
        codes = [w["code"] for w in d["warnings"]]
        assert "s_peg_not_applicable" in codes
