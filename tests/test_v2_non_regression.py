"""V1 non-regression + V2 endpoint smoke tests (CLAUDE.md §22 §0/§7.6).

The V2 work must leave every /v1/... route byte-shape identical: the V1
confidence schema (score, S_stat, S_liq, S_coh) carries no V2 fields, and
/v1/config never exposes the confidence_v2 block. These tests use the synthetic
fixture client, so they run without the real dataset.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# V1 confidence schema is unchanged (no V2 fields leak in)
# ---------------------------------------------------------------------------

_V2_ONLY_FIELDS = {"C", "composition_mode", "fragility_flag", "subscores", "S_peg"}


class TestV1NonRegression:
    TS = "2024-01-01T12:00:00Z"

    def test_v1_confidence_shape_unchanged(self, client):
        r = client.get(f"/v1/confidence/LINK/at", params={"timestamp": self.TS})
        assert r.status_code == 200, r.text
        body = r.json()
        # V1 keys present
        for key in ("score", "S_stat", "S_liq", "S_coh", "weights", "parameters"):
            assert key in body, f"missing V1 key {key}"
        # No V2-only keys
        assert _V2_ONLY_FIELDS.isdisjoint(body.keys()), body.keys()

    def test_v1_price_confidence_shape_unchanged(self, client):
        r = client.get(
            f"/v1/prices/LINK/at",
            params={"timestamp": self.TS, "include_confidence": "true"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # No V2 price fields on the V1 response
        for f in ("price_raw_in_quote", "price_neutralized_usd", "quote_currency",
                  "quote_currency_peg"):
            assert f not in body, f"V1 price response leaked V2 field {f}"
        conf = body.get("confidence")
        if conf is not None:
            assert _V2_ONLY_FIELDS.isdisjoint(conf.keys())

    def test_v1_config_has_no_v2_block(self, client):
        r = client.get("/v1/config")
        assert r.status_code == 200, r.text
        assert "confidence_v2" not in r.json()


# ---------------------------------------------------------------------------
# V2 endpoints — schema smoke tests
# ---------------------------------------------------------------------------

class TestV2Endpoints:
    TS = "2024-01-01T12:00:00Z"

    def test_v2_price_schema(self, client):
        r = client.get(
            f"/v2/prices/LINK/at",
            params={"timestamp": self.TS, "include_confidence": "true",
                    "include_provenance": "true"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        for f in ("price_usd", "price_raw_in_quote", "price_neutralized_usd",
                  "quote_currency", "quote_currency_peg", "confidence"):
            assert f in body, f"missing V2 field {f}"
        conf = body["confidence"]
        assert "C" in conf and "subscores" in conf and "S_peg" in conf
        assert conf["composition_mode"] == "3sub"
        # S_peg sits OUTSIDE subscores (3sub: separate signal).
        assert "S_peg" not in conf["subscores"]

    def test_v2_confidence_endpoint(self, client):
        r = client.get(f"/v2/confidence/LINK/at", params={"timestamp": self.TS})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["composition_mode"] == "3sub"
        assert set(body["subscores"].keys()) == {"S_stat", "S_liq", "S_coh"}

    def test_v2_fragility_null_when_uncalibrated(self, client):
        # Default config: c_threshold is null → fragility_flag null + warning.
        r = client.get(f"/v2/confidence/LINK/at", params={"timestamp": self.TS})
        body = r.json()
        assert body["fragility_flag"] is None
        codes = [w["code"] for w in body["warnings"]]
        assert "fragility_threshold_uncalibrated" in codes

    def test_v2_config_exposes_v2_block(self, client):
        r = client.get("/v2/config")
        assert r.status_code == 200, r.text
        body = r.json()
        assert "confidence_v2" in body
        assert body["confidence_v2"]["composite"]["mode"] == "3sub"
        assert "peg_feeds" in body

    def test_v2_unknown_asset_404(self, client):
        r = client.get("/v2/prices/DOGE/at", params={"timestamp": self.TS})
        assert r.status_code == 404
