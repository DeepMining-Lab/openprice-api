"""Tests for price service and endpoints."""

from datetime import datetime, timezone
import pytest


TS = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_assets_endpoint(client):
    r = client.get("/v1/assets")
    assert r.status_code == 200
    assets = [a["asset"] for a in r.json()["assets"]]
    assert set(assets) == {"ETH", "LINK", "UNI", "AAVE", "COMP"}


def test_unknown_asset_returns_404(client):
    r = client.get("/v1/prices/DOGE/at?timestamp=2024-01-01T00:00:00Z")
    assert r.status_code == 404


def test_invalid_branch_returns_422(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&branch=99")
    assert r.status_code == 422


def test_link_price_at_returns_value(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z")
    assert r.status_code == 200
    body = r.json()
    assert body["asset"] == "LINK"
    assert body["price_usd"] is not None


def test_link_price_uses_level_0a_when_tvl_ok(client):
    """With TVL=2M in fixture, level 0a should be chosen."""
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T06:00:00Z&branch=0a")
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "0a"
    assert body["price_usd"] == pytest.approx(14.2, rel=0.01)


def test_price_response_has_provenance(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&include_provenance=true")
    assert r.status_code == 200
    body = r.json()
    assert "provenance" in body
    assert body["provenance"]["files_used"]


def test_price_response_has_confidence(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&include_confidence=true")
    assert r.status_code == 200
    body = r.json()
    assert "confidence" in body
    conf = body["confidence"]
    assert "score" in conf


def test_no_persistent_db_created(tmp_path):
    """DuckDB must not create .duckdb files."""
    import os, glob
    duckdb_files = glob.glob(str(tmp_path / "*.duckdb"))
    assert len(duckdb_files) == 0


def test_datasets_endpoint(client):
    r = client.get("/v1/datasets")
    assert r.status_code == 200
    body = r.json()
    assert "files" in body
    assert len(body["files"]) > 0


def test_datasets_schema_endpoint(client):
    r = client.get("/v1/datasets/schema?asset=LINK")
    assert r.status_code == 200
    body = r.json()
    assert body["asset"] == "LINK"
    assert len(body["files"]) > 0


def test_datasets_schema_unknown_asset(client):
    r = client.get("/v1/datasets/schema?asset=DOGE")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Granularity — unit tests for VWMP and MAD
# ---------------------------------------------------------------------------

def test_vwmp_equal_weights():
    from app.services.price_service import _compute_vwmp
    # 3 equal-weight prices → median
    assert _compute_vwmp([10.0, 12.0, 14.0], [1.0, 1.0, 1.0]) == pytest.approx(12.0)


def test_vwmp_volume_weighted_low():
    from app.services.price_service import _compute_vwmp
    # 90 % volume at 10.0 → VWMP = 10.0
    assert _compute_vwmp([10.0, 20.0], [9.0, 1.0]) == pytest.approx(10.0)


def test_vwmp_volume_weighted_high():
    from app.services.price_service import _compute_vwmp
    # 60 % volume at 20.0 → VWMP = 20.0
    assert _compute_vwmp([10.0, 20.0], [4.0, 6.0]) == pytest.approx(20.0)


def test_vwmp_single_swap():
    from app.services.price_service import _compute_vwmp
    assert _compute_vwmp([15.5], [100.0]) == pytest.approx(15.5)


def test_vwmp_empty():
    from app.services.price_service import _compute_vwmp
    assert _compute_vwmp([], []) is None


def test_vwmp_zero_volume_falls_back_to_median():
    from app.services.price_service import _compute_vwmp
    # All zero volumes → simple median
    result = _compute_vwmp([10.0, 20.0, 30.0], [0.0, 0.0, 0.0])
    assert result == pytest.approx(20.0)


def test_mad_filter_removes_outlier():
    from app.services.price_service import _filter_mad_outliers
    prices = [10.0, 10.1, 10.2, 100.0]
    volumes = [1.0, 1.0, 1.0, 1.0]
    kept_p, kept_v, excluded = _filter_mad_outliers(prices, volumes, 3.5)
    assert excluded == 1
    assert 100.0 not in kept_p
    assert len(kept_p) == 3


def test_mad_filter_no_outlier():
    from app.services.price_service import _filter_mad_outliers
    prices = [10.0, 10.1, 10.2]
    volumes = [1.0, 1.0, 1.0]
    kept_p, kept_v, excluded = _filter_mad_outliers(prices, volumes, 3.5)
    assert excluded == 0
    assert kept_p == prices


def test_mad_filter_insufficient_data():
    from app.services.price_service import _filter_mad_outliers
    # < 3 prices → no filtering applied
    prices = [10.0, 20.0]
    volumes = [1.0, 1.0]
    kept_p, kept_v, excluded = _filter_mad_outliers(prices, volumes, 3.5)
    assert excluded == 0
    assert kept_p == prices


# ---------------------------------------------------------------------------
# Granularity — endpoint tests
# ---------------------------------------------------------------------------

def test_invalid_granularity_returns_422(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&granularity=second")
    assert r.status_code == 422


def test_price_at_raw_granularity_unchanged(client):
    # raw (default) must behave identically to the pre-granularity behaviour
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T06:00:00Z&branch=0a&granularity=raw")
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "0a"
    assert body["price_usd"] == pytest.approx(14.2, rel=0.01)
    assert body["granularity"] == "raw"
    assert body["swap_count"] is None  # point read — no swap aggregation


def test_price_at_day_granularity_returns_vwmp(client):
    # Day window ±12 h centred on noon captures rows at 00:00, 06:00, and 02 Jan 00:00
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T12:00:00Z"
        "&granularity=day&branch=0a&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["price_usd"] is not None
    assert body["granularity"] == "day"
    assert body["swap_count"] is not None
    assert body["swap_count"] >= 1
    assert body["window_seconds"] == 86400


def test_price_at_hour_granularity_returns_value(client):
    # Hour window ±30 min around 00:30 captures the row at 00:00
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T00:30:00Z"
        "&granularity=hour&branch=0a&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["price_usd"] is not None
    assert body["granularity"] == "hour"
    assert body["swap_count"] is not None


def test_price_at_minute_granularity_returns_value(client):
    # Minute window ±30 s around 00:00:00 captures the row exactly at 00:00:00
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z"
        "&granularity=minute&branch=0a&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["price_usd"] is not None
    assert body["granularity"] == "minute"


def test_price_at_day_granularity_provenance_has_swap_count(client):
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T12:00:00Z"
        "&granularity=day&branch=0a&include_provenance=true&include_confidence=false"
    )
    assert r.status_code == 200
    body = r.json()
    prov = body["provenance"]
    assert prov["swap_count"] is not None
    assert prov["window_seconds"] == 86400


def test_price_range_day_granularity(client):
    r = client.get(
        "/v1/prices/LINK?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z"
        "&granularity=day&branch=0a&limit=10"
    )
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list)
    assert len(body) >= 1
    for point in body:
        assert point["granularity"] == "day"


def test_price_range_hour_granularity_count(client):
    # 6-hour range → should produce 7 points (00:00 to 06:00 inclusive)
    r = client.get(
        "/v1/prices/LINK?start=2024-01-01T00:00:00Z&end=2024-01-01T06:00:00Z"
        "&granularity=hour&branch=0a&limit=100"
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 7  # 00, 01, 02, 03, 04, 05, 06


def test_window_expansion_produces_result(client):
    # Request at a time with no swap within 30s; R1 should expand and find data
    # Row at 00:00:00; requesting at 00:05:00 → first minute window ±30s misses,
    # expands to ±60s (misses), ±150s (misses), ±450s (±7.5 min, catches 00:00:00)
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T00:05:00Z"
        "&granularity=minute&branch=0a&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["price_usd"] is not None
    assert body["window_seconds"] > 60  # window was expanded
