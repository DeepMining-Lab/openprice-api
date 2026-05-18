"""Pydantic response schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class Warning(BaseModel):
    code: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"


class ConfidenceDetail(BaseModel):
    score: float | None
    S_stat: float | None = None
    S_liq: float | None = None
    S_coh: float | None = None
    coherence_mode: str | None = None
    weights: dict[str, float] | None = None
    parameters: dict[str, Any] | None = None
    warnings: list[Warning] = []


class Provenance(BaseModel):
    files_used: list[str]
    branch_level: str
    branch_label: str
    calculation_path: list[str] = []
    token_leg_timestamp: datetime | None = None
    eth_usd_leg_timestamp: datetime | None = None
    cross_rate_lag_seconds: float | None = None
    n_raw: int | None = None
    swap_count: int | None = None
    window_seconds: float | None = None
    excluded_swaps: int | None = None
    parameters: dict[str, Any] = {}
    detected_columns: dict[str, list[str]] = {}
    warnings: list[Warning] = []


class PriceResponse(BaseModel):
    asset: str
    timestamp_requested: datetime
    timestamp_observed: datetime | None
    price_usd: float | None
    branch_level: str
    branch_label: str
    data_status: str
    granularity: str = "raw"
    n_raw: int | None = None
    swap_count: int | None = None
    window_seconds: float | None = None
    unavailable_reason: str | None = None
    confidence: ConfidenceDetail | None = None
    provenance: Provenance | None = None
    warnings: list[Warning] = []


class DatasetFile(BaseModel):
    asset: str
    path: str
    exists: bool
    role: str


class DatasetsResponse(BaseModel):
    datasets_root: str
    files: list[DatasetFile]


class ColumnMapping(BaseModel):
    file: str
    raw_columns: list[str]
    canonical_mapping: dict[str, str]
    warnings: list[Warning] = []


class SchemaResponse(BaseModel):
    asset: str
    files: list[ColumnMapping]


class ComparePoint(BaseModel):
    timestamp: datetime
    dex_price_usd: float | None
    chainlink_price_usd: float | None
    deviation: float | None
    dex_branch: str | None = None
    warnings: list[Warning] = []
