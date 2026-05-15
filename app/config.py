"""Config loader — reads openprice.yaml and validates all parameters."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, field_validator, model_validator


class ApiConfig(BaseModel):
    name: str
    version: str
    default_limit: int
    max_limit: int


class PathsConfig(BaseModel):
    datasets_root: str

    @property
    def datasets_path(self) -> Path:
        return Path(self.datasets_root).expanduser().resolve()


class ThresholdsConfig(BaseModel):
    seuil_TVL_min_usd: float
    seuil_vol_min_usd_24h: float
    fenetre_inactivite_jours: int
    min_swaps_for_stat_score: int
    s_stat_floor: float
    slip_reference_order_usd: float
    slip_max: float
    sigma_mad: float
    cross_rate_max_lag_seconds: int
    chainlink_default_heartbeat_seconds: int

    @field_validator("seuil_TVL_min_usd")
    @classmethod
    def tvl_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("seuil_TVL_min_usd must be > 0")
        return v

    @field_validator("seuil_vol_min_usd_24h")
    @classmethod
    def vol_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("seuil_vol_min_usd_24h must be >= 0")
        return v

    @field_validator("fenetre_inactivite_jours")
    @classmethod
    def window_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("fenetre_inactivite_jours must be > 0")
        return v

    @field_validator("sigma_mad")
    @classmethod
    def sigma_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("sigma_mad must be > 0")
        return v

    @field_validator("slip_max")
    @classmethod
    def slip_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("slip_max must be > 0")
        return v

    @field_validator("cross_rate_max_lag_seconds")
    @classmethod
    def lag_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("cross_rate_max_lag_seconds must be >= 0")
        return v


class ConfidenceWeightsConfig(BaseModel):
    w_stat: float
    w_liq: float
    w_coh: float

    @field_validator("w_stat", "w_liq", "w_coh")
    @classmethod
    def weight_in_range(cls, v: float) -> float:
        if not (0 <= v <= 1):
            raise ValueError("weights must be between 0 and 1")
        return v

    @model_validator(mode="after")
    def weights_sum_to_one(self) -> "ConfidenceWeightsConfig":
        total = self.w_stat + self.w_liq + self.w_coh
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"w_stat + w_liq + w_coh must equal 1.0 (got {total})")
        return self


class ChainlinkConfig(BaseModel):
    default_deviation_threshold: float
    deviation_threshold_by_asset: dict[str, float]
    heartbeat_seconds_by_asset: dict[str, int]


class ScoringConfig(BaseModel):
    tvl_score_mode: Literal["linear_to_threshold", "binary_threshold", "log_memoire"]


class AppConfig(BaseModel):
    api: ApiConfig
    paths: PathsConfig
    thresholds: ThresholdsConfig
    confidence_weights: ConfidenceWeightsConfig
    chainlink: ChainlinkConfig
    scoring: ScoringConfig


_config: AppConfig | None = None


def load_config(path: str | Path | None = None) -> AppConfig:
    global _config
    if path is None:
        path = os.environ.get("OPENPRICE_CONFIG", "config/openprice.yaml")
    config_path = Path(path).expanduser().resolve()
    with config_path.open() as f:
        raw = yaml.safe_load(f)
    _config = AppConfig(**raw)
    return _config


def get_config() -> AppConfig:
    global _config
    if _config is None:
        _config = load_config()
    return _config
