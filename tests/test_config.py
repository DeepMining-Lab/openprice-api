"""Tests for config loading and validation."""

import pytest
from pydantic import ValidationError

from app.config import AppConfig, load_config


def test_config_loads(cfg):
    assert cfg.api.name == "TestAPI"
    assert cfg.api.version == "0.0.1"


def test_confidence_weights_sum_to_one(cfg):
    w = cfg.confidence_weights
    total = w.w_stat + w.w_liq + w.w_coh
    assert abs(total - 1.0) < 1e-6


def test_invalid_weights_rejected():
    from app.config import ConfidenceWeightsConfig
    with pytest.raises(ValidationError):
        ConfidenceWeightsConfig(w_stat=0.5, w_liq=0.5, w_coh=0.5)


def test_negative_tvl_rejected():
    from app.config import ThresholdsConfig
    with pytest.raises(ValidationError):
        ThresholdsConfig(
            seuil_TVL_min_usd=-1,
            seuil_vol_min_usd_24h=0,
            fenetre_inactivite_jours=30,
            min_swaps_for_stat_score=3,
            s_stat_floor=0.2,
            slip_reference_order_usd=1000,
            slip_max=0.005,
            sigma_mad=3.5,
            cross_rate_max_lag_seconds=3600,
            chainlink_default_heartbeat_seconds=86400,
        )


def test_threshold_values_changeable(cfg, tmp_path):
    """Thresholds can be overridden by writing a new config file."""
    import yaml
    new_cfg_data = {
        "api": cfg.api.model_dump(),
        "paths": {"datasets_root": str(cfg.paths.datasets_root)},
        "thresholds": {**cfg.thresholds.model_dump(), "seuil_TVL_min_usd": 500000},
        "confidence_weights": cfg.confidence_weights.model_dump(),
        "chainlink": cfg.chainlink.model_dump(),
        "scoring": cfg.scoring.model_dump(),
    }
    p = tmp_path / "alt.yaml"
    p.write_text(yaml.dump(new_cfg_data))
    alt = load_config(p)
    assert alt.thresholds.seuil_TVL_min_usd == 500000
