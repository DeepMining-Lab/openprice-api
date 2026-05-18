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


# ---------------------------------------------------------------------------
# Level 0b windowed — cross-rate with VWMP
# ---------------------------------------------------------------------------

def test_0b_windowed_computes_cross_rate(client):
    """Force branch=0b, hour granularity at T=00:30 — VWMP cross-rate ≈ 14.0.

    Fixture: LINK/WETH row at 00:00 (inside ±30 min window around T=00:30) with
    price_weth_per_link=0.00636363636, and ETH/USDC at 00:00 (point read, 2200 USD/ETH).
    Expected: VWMP(0.00636363636) × 2200 ≈ 14.0 USD/LINK.
    """
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T00:30:00Z"
        "&granularity=hour&branch=0b&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "0b"
    assert body["price_usd"] == pytest.approx(14.0, rel=0.01)
    assert body["swap_count"] == 1
    assert body["window_seconds"] == 3600


# ---------------------------------------------------------------------------
# Hierarchy fallthrough — DEX levels → Chainlink
# ---------------------------------------------------------------------------

def test_minute_hierarchy_falls_to_chainlink(client):
    """At T=03:00, all minute R1 windows miss DEX data → pipeline reaches Chainlink.

    Max minute window ±450 s spans [02:52:30, 03:07:30].
    Fixture LINK/USDC rows are at 00:00 and 06:00; LINK/WETH row is at 00:00.
    None fall in that window → 0a and 0b both fail → level 3.
    """
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T03:00:00Z"
        "&granularity=minute&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "3"


def test_hour_hierarchy_falls_to_chainlink(client):
    """At T=12:00, max hour window ±4 h = [08:00, 16:00] misses all DEX data → Chainlink.

    Fixture LINK/USDC rows are at 00:00 and 06:00; LINK/WETH row is at 00:00.
    All fall outside [08:00, 16:00] → 0a and 0b both fail → level 3.
    """
    r = client.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T12:00:00Z"
        "&granularity=hour&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "3"


# ---------------------------------------------------------------------------
# Hierarchy fallthrough — 0a → 0b and multi-pool R1 (custom fixtures)
# ---------------------------------------------------------------------------

def _write_csv_local(path, rows):
    import csv as _csv_mod
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        w = _csv_mod.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _make_custom_client(tmp_path, monkeypatch, csv_files: dict):
    """Write CSV fixtures under tmp_path/datasets and return a patched TestClient."""
    from app.config import (
        AppConfig, ApiConfig, PathsConfig, ThresholdsConfig,
        ConfidenceWeightsConfig, ChainlinkConfig, ScoringConfig,
    )
    import app.config as _cfg_mod
    import app.registry as _reg_mod
    from fastapi.testclient import TestClient
    from app.main import app

    root = tmp_path / "datasets"
    for rel, rows in csv_files.items():
        _write_csv_local(root / rel, rows)

    custom_cfg = AppConfig(
        api=ApiConfig(name="TestAPI", version="0.0.1", default_limit=100, max_limit=1000),
        paths=PathsConfig(datasets_root=str(root)),
        thresholds=ThresholdsConfig(
            seuil_TVL_min_usd=1000000,
            seuil_vol_min_usd_24h=10000,
            fenetre_inactivite_jours=30,
            min_swaps_for_stat_score=3,
            s_stat_floor=0.2,
            slip_reference_order_usd=1000,
            slip_max=0.005,
            sigma_mad=3.5,
            cross_rate_max_lag_seconds=3600,
            chainlink_default_heartbeat_seconds=86400,
        ),
        confidence_weights=ConfidenceWeightsConfig(
            w_stat=0.3333333333,
            w_liq=0.3333333333,
            w_coh=0.3333333334,
        ),
        chainlink=ChainlinkConfig(
            default_deviation_threshold=0.005,
            deviation_threshold_by_asset={"ETH": 0.005, "LINK": 0.005,
                                          "UNI": 0.005, "AAVE": 0.005, "COMP": 0.01},
            heartbeat_seconds_by_asset={"ETH": 86400, "LINK": 86400,
                                        "UNI": 86400, "AAVE": 86400, "COMP": 86400},
        ),
        scoring=ScoringConfig(tvl_score_mode="linear_to_threshold"),
    )

    # Each module that does `from app.config import get_config` holds its own
    # reference to the function; we must patch every such reference explicitly.
    import app.routers.prices as _prices_mod
    monkeypatch.setattr(_prices_mod, "get_config", lambda: custom_cfg)
    monkeypatch.setattr(_reg_mod, "get_config", lambda: custom_cfg)
    return TestClient(app)


def test_0a_falls_to_0b_windowed(tmp_path, monkeypatch):
    """When 0a has no swap in any minute window the pipeline falls through to 0b.

    LINK/USDC and LINK/USDT data are 60 minutes before T (outside max ±450 s window).
    LINK/WETH data is exactly at T (inside the first ±30 s window).
    After 0a exhausts all R1 steps for every viable pool, 0b picks up the
    cross-rate: VWMP(0.006363636 WETH/LINK) × 2200 USD/ETH ≈ 14.0 USD/LINK.
    """
    tc = _make_custom_client(tmp_path, monkeypatch, {
        "link/link_usdc_uniswap_v3_03.csv": [
            {"timestamp": "2024-01-01 00:00:00+00:00", "price_usdc_per_link": 14.0,
             "volume_usdc": 50000.0, "pool_tvl_at_block": 2000000.0, "slip_1k": 0.001},
        ],
        "link/link_usdt_uniswap_v3_03.csv": [
            {"timestamp": "2024-01-01 00:00:00+00:00", "price_usdt_per_link": 14.0,
             "volume_usdt": 45000.0, "pool_tvl_at_block": 1800000.0, "slip_1k": 0.001},
        ],
        "link/link_weth_uniswap_v3_03.csv": [
            {"timestamp": "2024-01-01 01:00:00+00:00", "price_weth_per_link": 0.006363636,
             "volume_weth": 10.0, "pool_tvl_at_block": 1500000.0, "slip_1k": 0.001},
        ],
        "eth/eth_usdc_uniswap_v3_005.csv": [
            {"timestamp": "2024-01-01 01:00:00+00:00", "price_usdc_per_eth": 2200.0,
             "volume_usdc": 100000.0, "pool_tvl_at_block": 5000000.0, "slip_1k": 0.0005},
        ],
        "link/chainlink_link_usd.csv": [
            {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 14.0},
        ],
        "eth/chainlink_eth_usd.csv": [
            {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 2200.0},
        ],
    })

    r = tc.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T01:00:00Z"
        "&granularity=minute&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "0b", f"Expected 0b, got {body['branch_level']}"
    assert body["price_usd"] == pytest.approx(14.0, rel=0.01)


def test_0a_second_pool_used_when_best_has_no_window_data(tmp_path, monkeypatch):
    """Within 0a, the best-TVL pool (USDC 3M) has no data in any minute window.

    After R1 exhaustion on the USDC pool, _try_level_0a_windowed tries the
    next viable pool (USDT 1.5M) which has a swap at T → returns its VWMP.
    """
    tc = _make_custom_client(tmp_path, monkeypatch, {
        "link/link_usdc_uniswap_v3_03.csv": [
            {"timestamp": "2024-01-01 00:00:00+00:00", "price_usdc_per_link": 14.0,
             "volume_usdc": 50000.0, "pool_tvl_at_block": 3000000.0, "slip_1k": 0.001},
        ],
        "link/link_usdt_uniswap_v3_03.csv": [
            {"timestamp": "2024-01-01 01:00:00+00:00", "price_usdt_per_link": 14.1,
             "volume_usdt": 45000.0, "pool_tvl_at_block": 1500000.0, "slip_1k": 0.001},
        ],
        "link/chainlink_link_usd.csv": [
            {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 14.0},
        ],
        "eth/chainlink_eth_usd.csv": [
            {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 2200.0},
        ],
    })

    r = tc.get(
        "/v1/prices/LINK/at?timestamp=2024-01-01T01:00:00Z"
        "&granularity=minute&branch=0a&include_confidence=false&include_provenance=false"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "0a", f"Expected 0a, got {body['branch_level']}"
    assert body["price_usd"] == pytest.approx(14.1, rel=0.01)
