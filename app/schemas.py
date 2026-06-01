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


# ---------------------------------------------------------------------------
# API V2 schemas (CLAUDE.md §22). Additive — V1 schemas above are unchanged.
# ---------------------------------------------------------------------------

class ConfidenceV2Detail(BaseModel):
    C: float | None
    composition_mode: Literal["3sub", "4sub"]
    fragility_flag: bool | None = None
    # S_stat / S_liq / S_coh — S_peg is published separately (3sub: signal apart).
    subscores: dict[str, float | None]
    S_peg: float | None = None
    coherence_mode: str | None = None
    qualitative_level: str | None = None
    weights: dict[str, float] | None = None
    parameters: dict[str, Any] | None = None
    warnings: list[Warning] = []


class PriceV2Response(BaseModel):
    asset: str
    timestamp: datetime
    timestamp_observed: datetime | None
    granularity: str = "raw"
    price_usd: float | None            # headline price = peg-neutralized USD price
    price_raw_in_quote: float | None = None
    price_neutralized_usd: float | None = None
    quote_currency: str | None = None
    quote_currency_peg: float | None = None
    branch_level: str
    branch_label: str
    data_status: str
    swap_count: int | None = None
    window_seconds: float | None = None
    unavailable_reason: str | None = None
    confidence: ConfidenceV2Detail | None = None
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
