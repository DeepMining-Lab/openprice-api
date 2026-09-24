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
    # Temporal provenance fields (mémoire §6.8.1). Additive — populated for
    # windowed reads; null for raw point reads (no window).
    initial_window_seconds: float | None = None
    window_start_utc: datetime | None = None
    window_end_utc: datetime | None = None
    window_bound_policy: str | None = None
    expansion_step: int | None = None
    # Reference block b_ref(T) (mémoire §6.2.6). Populated from the winning
    # source row when a block_number column exists (DEX); null otherwise.
    reference_block_number: int | None = None
    reference_block_timestamp: datetime | None = None
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


# ---------------------------------------------------------------------------
# API V3 schemas. Additive — V1/V2 schemas above are unchanged, so V3 can
# explain its source selection without altering the V1/V2 JSON.
# ---------------------------------------------------------------------------

class RejectedCandidate(BaseModel):
    """A source file that the hierarchy evaluated at T and did not use, and why."""
    level: str
    file: str
    rule: str                      # zombie_tvl | zombie_volume_24h | inactive | cross_rate_lag | ...
    message: str
    value: float | None = None     # measured value that failed the rule (USD, seconds, ...)
    threshold: float | None = None
    last_observation_utc: datetime | None = None


class SourceEventV3(BaseModel):
    """The on-chain event behind a point read: a swap, or a Chainlink round."""
    file: str
    timestamp: datetime
    tx_hash: str | None = None
    log_index: int | None = None
    block_number: int | None = None
    phase: int | None = None                  # Chainlink aggregator phase
    aggregator_round: int | None = None       # Chainlink round within that phase
    rows_at_same_timestamp: int               # rows of the file sharing this timestamp
    tie_break_rule: str                       # first_swap_of_block | latest_round_of_active_phase | first_csv_row


class ProvenanceV3(Provenance):
    rejected_candidates: list[RejectedCandidate] = []
    source_event: SourceEventV3 | None = None          # raw point reads only (a VWMP aggregates many swaps)
    eth_usd_leg_event: SourceEventV3 | None = None     # cross-rates: the ETH/USD point read
    dataset_version: str | None = None                 # version of the Parquet store that answered
    dataset_files: dict[str, str] = {}                 # version of each file read for this answer


class PriceV3Response(PriceV2Response):
    provenance: ProvenanceV3 | None = None


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
