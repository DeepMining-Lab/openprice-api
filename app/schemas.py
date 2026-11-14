"""Pydantic response schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


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


# --- Inputs of the confidence index (provenance.confidence_inputs) ----------------------------------------------
# Everything needed to recompute S_stat, S_liq, S_coh, S_peg, C and fragility_flag from the record alone, with the
# parameters each score used. Column names are those of the source CSV. Values are the ones the API used, at full
# precision.

class RowRefV3(BaseModel):
    """A row of a pool CSV, identified on-chain: (block_number, log_index) is unique on Ethereum."""
    file: str
    timestamp: datetime
    timestamp_column: str | None = None
    block_number: int | None = None
    log_index: int | None = None
    tie_break_rule: str = Field(description="Row chosen among rows sharing its timestamp: first_swap_of_block "
                                            "(on-chain order) or first_csv_row (legacy mode).")


class ChainlinkRoundV3(BaseModel):
    """A Chainlink round. ``global_round_id`` = phase << 64 | aggregator_round, the round id the feed's proxy
    returns: ``getRoundData(global_round_id)`` on ``proxy_address`` gives the round on-chain."""
    file: str
    proxy_address: str | None = None
    phase: int | None = None
    aggregator_round: int | None = None
    global_round_id: str | None = Field(None, description="Decimal string: an 80-bit integer exceeds JSON number "
                                                          "precision in JavaScript.")
    timestamp: datetime
    answer: float
    answer_column: str | None = None
    timestamp_column: str | None = None
    phase_rule: str = Field("active_phase_at_t", description="active_phase_at_t: latest round at or before T of the "
                                                             "phase the proxy served at T (the latest round wins a "
                                                             "tie); all_phases: latest row of the file at or before T "
                                                             "(the first CSV row wins a tie).")


class ServedPhaseV3(BaseModel):
    """Part of the S_stat window in which ``phase`` was the aggregator phase the feed's proxy served."""
    phase: int
    from_utc: datetime
    to_utc: datetime


class SStatInputsV3(BaseModel):
    rule: str = Field(description="mad_z_score (formula applied), insufficient_data_floor (fewer than "
                                  "min_observations: value = floor), mad_zero (value = 1), missing_columns, "
                                  "nonpositive_price.")
    method: str = Field(description="mad: z = 0.6745 |price_tested - median| / mad. log_mad / log_realized_vol "
                                    "(confidence_v2.s_stat.normalize_by_volatility): z = |ln(price_tested) - "
                                    "median_log| / local_volatility. Then value = exp(-z^2 / (2 sigma_mad^2)).")
    reference_file: str | None = None
    reference_kind: str | None = Field(None, description="source_pool (the pool that priced, levels 0a/1/2 direct) "
                                                         "or chainlink_feed (cross-rates).")
    reference_timestamp_column: str | None = None
    reference_price_column: str | None = None
    window_start_utc: datetime
    window_end_utc: datetime
    window_bound_policy: str = "left_closed_right_open"
    window_row_limit: int | None = Field(None, description="Legacy mode only: the window is cut to its first rows.")
    served_phases: list[ServedPhaseV3] | None = Field(
        None, description="Chainlink reference read with the phase filter: only rounds of the phase the proxy served "
                          "at their own time count. Null: every row of the window counts.")
    n_observations: int = 0
    median: float | None = Field(None, description="sorted(prices)[n // 2] (upper median for an even n).")
    mad: float | None = Field(None, description="sorted(|p - median|)[n // 2].")
    median_log: float | None = None
    local_volatility: float | None = None
    price_tested: float | None = None
    price_tested_field: str = Field("price_raw_in_quote", description="Response field holding price_tested.")
    z_score: float | None = None
    sigma_mad: float
    min_observations: int
    floor: float
    value: float | None


class SLiqLegV3(BaseModel):
    role: str = Field(description="pool (a direct price), token_leg or eth_usd_leg (a cross-rate).")
    file: str
    row: RowRefV3 | None = Field(None, description="Row whose TVL and slippage are scored: the as-of row at T "
                                                   "(latest swap at or before T), for windowed prices too.")
    tvl_column: str | None = None
    tvl_raw: float | None = None
    tvl_unit: str = Field("usd", description="Unit the API assumes for the TVL column: usd, or eth (converted with "
                                             "eth_usd_price_for_tvl when there is one).")
    eth_usd_price_for_tvl: float | None = Field(None, description="Price of the ETH/USD leg's as-of row, which "
                                                                 "converts a TVL in ETH.")
    price_column: str | None = Field(None, description="eth_usd_leg only: the ETH/USD price column of the file.")
    price: float | None = Field(None, description="eth_usd_leg only: the row's ETH/USD price (it converts the token "
                                                  "leg's TVL when that TVL is in ETH).")
    tvl_usd: float | None = Field(None, description="TVL that S_TVL scores.")
    slippage_column: str | None = None
    slippage: float | None = None
    s_tvl: float | None = None
    s_slip: float | None = None
    value: float | None = Field(None, description="sqrt(s_tvl * s_slip), or the one that exists.")


class SLiqInputsV3(BaseModel):
    rule: str = Field(description="single_leg, geometric_mean_of_legs (sqrt of the two leg values), token_leg_only, "
                                  "eth_usd_leg_only, or unavailable (no TVL or slippage in any leg).")
    legs: list[SLiqLegV3]
    tvl_score_mode: str = Field(description="log_memoire: s_tvl = clamp(log10(tvl/min) / log10(ref/min), 0, 1); "
                                            "linear_to_threshold: min(1, tvl / seuil_TVL_min_usd); "
                                            "binary_threshold: 1 if tvl >= seuil_TVL_min_usd else 0.")
    tvl_log_min_usd: float
    tvl_log_ref_usd: float
    seuil_TVL_min_usd: float
    slip_max: float = Field(description="s_slip = exp(-|slippage| / slip_max).")
    value: float | None


class SCohInputsV3(BaseModel):
    rule: str = Field(description="deviation (value = exp(-(delta / delta_tol)^2)), no_chainlink_observation, "
                                  "chainlink_zero_price or missing_columns.")
    chainlink_round: ChainlinkRoundV3 | None = Field(None, description="Latest round at or before T.")
    phase_rule: str = Field(description="active_phase_at_t (rounds of the phase the proxy served at T) or "
                                        "all_phases.")
    price_compared: float | None = None
    price_compared_field: str = Field(description="Response field holding price_compared.")
    delta: float | None = Field(None, description="|price_compared - answer| / answer.")
    delta_tol: float
    value: float | None


class SPegInputsV3(BaseModel):
    rule: str = Field(description="peg_deviation (value = exp(-(|peg - 1| / peg_tol)^2)) or not_applicable.")
    quote_currency: str | None = None
    peg_round: ChainlinkRoundV3 | None = Field(None, description="Latest round at or before T of the quote "
                                                                 "currency's USD feed; its answer is the peg.")
    peg: float | None = None
    peg_tol: float
    value: float | None


class CompositionV3(BaseModel):
    mode: str = Field(description="3sub: C = S_stat^w_stat * S_liq^w_liq * S_coh^w_coh. 4sub: the product of the "
                                  "four scores (S_peg included) to the power w_i / sum(w).")
    weights: dict[str, float]
    C: float | None
    fragility_c_threshold: float | None = None
    fragility_flag: bool | None = Field(None, description="C < fragility_c_threshold.")


class ConfidenceInputsV3(BaseModel):
    s_stat: SStatInputsV3 | None = None
    s_liq: SLiqInputsV3 | None = None
    s_coh: SCohInputsV3 | None = None
    s_peg: SPegInputsV3 | None = None
    composition: CompositionV3


class ProvenanceV3(Provenance):
    rejected_candidates: list[RejectedCandidate] = []
    source_event: SourceEventV3 | None = None          # raw point reads only (a VWMP aggregates many swaps)
    eth_usd_leg_event: SourceEventV3 | None = None     # cross-rates: the ETH/USD point read
    dataset_version: str | None = None                 # version of the Parquet store that answered
    dataset_files: dict[str, str] = {}                 # version of each file read for this answer
    confidence_inputs: ConfidenceInputsV3 | None = Field(
        None, description="Inputs of every confidence score, enough to recompute the index from this record alone. "
                          "Null when there is no confidence index (level 3 or 4, or include_confidence=false).")
    config_sha256: str | None = Field(None, description="SHA-256 of every parameter V3 read (as GET /v3/config).")
    code_commit: str | None = Field(None, description="Git commit of the API code checked out when the service "
                                                      "started.")
    code_dirty: bool | None = Field(None, description="True when app/ or config/ had uncommitted changes when the "
                                                      "service started: code_commit then does not describe the "
                                                      "running code exactly.")


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


class ComparePointV3(ComparePoint):
    """A /v3/compare row. ``dex_price_usd`` is the price /v3/prices returns: peg-neutralized when the quote is a
    stablecoin with a peg feed, the price S_coh compares with Chainlink. The raw price in the quote currency and the
    peg used are given next to it."""
    dex_price_raw_in_quote: float | None = None
    quote_currency: str | None = None
    quote_currency_peg: float | None = None
