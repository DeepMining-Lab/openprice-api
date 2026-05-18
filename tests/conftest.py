"""Test fixtures — small synthetic CSVs, no real dataset required."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path

import pytest

from app.config import AppConfig, _config as _cfg_module
import app.config as config_module


SAMPLE_YAML = """
api:
  name: "TestAPI"
  version: "0.0.1"
  default_limit: 100
  max_limit: 1000

paths:
  datasets_root: "{root}"

thresholds:
  seuil_TVL_min_usd: 1000000
  seuil_vol_min_usd_24h: 10000
  fenetre_inactivite_jours: 30
  min_swaps_for_stat_score: 3
  s_stat_floor: 0.2
  slip_reference_order_usd: 1000
  slip_max: 0.005
  sigma_mad: 3.5
  cross_rate_max_lag_seconds: 3600
  chainlink_default_heartbeat_seconds: 86400

confidence_weights:
  w_stat: 0.3333333333
  w_liq: 0.3333333333
  w_coh: 0.3333333334

chainlink:
  default_deviation_threshold: 0.005
  deviation_threshold_by_asset:
    ETH: 0.005
    LINK: 0.005
    UNI: 0.005
    AAVE: 0.005
    COMP: 0.01
  heartbeat_seconds_by_asset:
    ETH: 86400
    LINK: 86400
    UNI: 86400
    AAVE: 86400
    COMP: 86400

scoring:
  tvl_score_mode: "linear_to_threshold"
"""


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="session")
def tmp_dataset_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("datasets")

    # Chainlink LINK/USD
    _write_csv(root / "link" / "chainlink_link_usd.csv", [
        {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 14.0},
        {"round_updated_at_utc": "2024-01-02 00:00:00+00:00", "answer_normalized": 15.0},
    ])

    # Chainlink ETH/USD (for S_coh reference)
    _write_csv(root / "eth" / "chainlink_eth_usd.csv", [
        {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 2200.0},
        {"round_updated_at_utc": "2024-01-02 00:00:00+00:00", "answer_normalized": 2250.0},
    ])

    # LINK/USDC direct stable (high TVL so not zombie)
    _write_csv(root / "link" / "link_usdc_uniswap_v3_03.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00",
         "price_usdc_per_link": 14.0, "volume_usdc": 50000.0,
         "pool_tvl_at_block": 2000000.0, "slip_1k": 0.001},
        {"timestamp": "2024-01-01 06:00:00+00:00",
         "price_usdc_per_link": 14.2, "volume_usdc": 60000.0,
         "pool_tvl_at_block": 2100000.0, "slip_1k": 0.001},
        {"timestamp": "2024-01-02 00:00:00+00:00",
         "price_usdc_per_link": 15.0, "volume_usdc": 55000.0,
         "pool_tvl_at_block": 2000000.0, "slip_1k": 0.001},
    ])

    # ETH/USD reference (WETH/USDC)
    _write_csv(root / "eth" / "eth_usdc_uniswap_v3_005.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00",
         "price_usdc_per_eth": 2200.0, "volume_usdc": 100000.0,
         "pool_tvl_at_block": 5000000.0, "slip_1k": 0.0005},
        {"timestamp": "2024-01-02 00:00:00+00:00",
         "price_usdc_per_eth": 2250.0, "volume_usdc": 110000.0,
         "pool_tvl_at_block": 5000000.0, "slip_1k": 0.0005},
    ])

    # LINK/WETH cross-rate leg
    _write_csv(root / "link" / "link_weth_uniswap_v3_03.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00",
         "price_weth_per_link": 0.00636363636, "volume_weth": 10.0,
         "pool_tvl_at_block": 1500000.0, "slip_1k": 0.001},
    ])

    return root


@pytest.fixture(scope="session")
def cfg(tmp_dataset_root, tmp_path_factory):
    import yaml
    yaml_path = tmp_path_factory.mktemp("config") / "openprice.yaml"
    yaml_path.write_text(SAMPLE_YAML.format(root=str(tmp_dataset_root)))
    loaded = config_module.load_config(yaml_path)
    yield loaded
    # Reset global config so other tests can load fresh
    config_module._config = None


@pytest.fixture
def client(cfg, tmp_dataset_root, monkeypatch):
    """FastAPI test client with patched config and registry paths."""
    from fastapi.testclient import TestClient

    # Import app.main FIRST so all routers bind get_config to the original function,
    # not to a monkeypatched lambda.  If we patched config_module.get_config before
    # this import, prices.py (and other routers) would permanently capture the lambda.
    from app.main import app

    # Patch _config directly.  All get_config() calls use the original function which
    # reads _config, so this single patch is sufficient for every router and service.
    monkeypatch.setattr(config_module, "_config", cfg)

    with TestClient(app) as c:
        yield c
