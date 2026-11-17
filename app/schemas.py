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
#
# The descriptions and examples below are the OpenAPI documentation of the V3 provenance record (GET /openapi.json,
# /docs). They document; they change no type, default or served value. Conventions: file paths are relative to the
# datasets root; timestamps are UTC (ISO 8601); durations are seconds; ratios, tolerances and scores are plain
# numbers (0.0125 = 1.25 %), scores lie in [0, 1]; column names are those of the source CSV file.
# ---------------------------------------------------------------------------

class RejectedCandidate(BaseModel):
    """A source file that the hierarchy evaluated at T and did not use, and why."""
    level: str = Field(description="Source level of the candidate: 0a, 0b, 1, 2 or 3.", examples=["0a"])
    file: str = Field(description="CSV file of the candidate.", examples=["link/link_usdt_uniswap_v3_03.csv"])
    rule: str = Field(
        description="Rule that rejected it: zombie_tvl, zombie_volume_24h, inactive, stale_observation, "
                    "cross_rate_lag, eth_leg_unavailable, no_observation, no_swaps_in_window, missing_columns, "
                    "dataset_missing or dataset_empty.",
        examples=["zombie_tvl"])
    message: str = Field(description="The rejection in words, with the value measured.")
    value: float | None = Field(
        None, description="Value measured that failed the rule: USD for zombie_tvl and zombie_volume_24h, seconds "
                          "for inactive, stale_observation and cross_rate_lag. Null for a rule without a measure.",
        examples=[3791.0])
    threshold: float | None = Field(None, description="Limit the value was compared with, in the same unit.",
                                    examples=[100000.0])
    last_observation_utc: datetime | None = Field(
        None, description="Latest observation of the file at or before T, for the rules that read one.")


class SourceEventV3(BaseModel):
    """The on-chain event behind a point read: a swap, or a Chainlink round."""
    file: str = Field(description="CSV file holding the event.", examples=["eth/eth_usdc_uniswap_v3_005.csv"])
    timestamp: datetime = Field(description="Block time of the swap, or publication time of the round (UTC).")
    tx_hash: str | None = Field(None, description="Transaction hash of the swap (0x + 64 hex). Null for a round.")
    log_index: int | None = Field(None, description="Index of the Swap log in its block. With block_number it "
                                                    "identifies the event on Ethereum. Null for a round.")
    block_number: int | None = Field(None, description="Ethereum block of the swap. Null for a round.")
    phase: int | None = Field(None, description="Chainlink aggregator phase of the round. Null for a swap.")
    aggregator_round: int | None = Field(None, description="Chainlink round within that phase. Null for a swap.")
    rows_at_same_timestamp: int = Field(description="Number of rows of the file sharing this timestamp (>= 1).")
    tie_break_rule: str = Field(
        description="Row chosen among those rows: first_swap_of_block (lowest block_number, log_index), "
                    "latest_round_of_active_phase (Chainlink), or first_csv_row (legacy mode, or a file without "
                    "block number and log index).",
        examples=["first_swap_of_block"])


# --- Inputs of the confidence index (provenance.confidence_inputs) ----------------------------------------------
# Everything needed to recompute S_stat, S_liq, S_coh, S_peg, C and fragility_flag from the record alone, with the
# parameters each score used. Column names are those of the source CSV. Values are the ones the API used, at full
# precision.

class RowRefV3(BaseModel):
    """A row of a pool CSV, identified on-chain: (block_number, log_index) is unique on Ethereum."""
    file: str = Field(description="CSV file of the row.", examples=["aave/aave_weth_uniswap_v3_03.csv"])
    timestamp: datetime = Field(description="Block time of the swap (UTC).", examples=["2023-03-11T11:58:35Z"])
    timestamp_column: str | None = Field(None, description="CSV column holding that timestamp.",
                                         examples=["timestamp"])
    block_number: int | None = Field(None, description="Ethereum block of the swap. Null when the file has no "
                                                       "block number column.", examples=[16804694])
    log_index: int | None = Field(None, description="Index of the Swap log in its block. Null when the file has no "
                                                    "log index column.", examples=[192])
    tie_break_rule: str = Field(description="Row chosen among rows sharing its timestamp: first_swap_of_block "
                                            "(on-chain order) or first_csv_row (legacy mode).",
                                examples=["first_swap_of_block"])


class ChainlinkRoundV3(BaseModel):
    """A Chainlink round. ``global_round_id`` = phase << 64 | aggregator_round, the round id the feed's proxy
    returns: ``getRoundData(global_round_id)`` on ``proxy_address`` gives the round on-chain."""
    file: str = Field(description="CSV file of the feed.", examples=["aave/chainlink_aave_usd.csv"])
    proxy_address: str | None = Field(None, description="Ethereum address of the feed's proxy contract "
                                                        "(0x + 40 hex).",
                                      examples=["0x547a514d5e3769680ce22b2361c10ea13619e8a9"])
    phase: int | None = Field(None, description="Aggregator phase of the round (phaseId of the proxy, >= 1).",
                              examples=[3])
    aggregator_round: int | None = Field(None, description="Round number within that phase.", examples=[20806])
    global_round_id: str | None = Field(None, description="Decimal string: an 80-bit integer exceeds JSON number "
                                                          "precision in JavaScript.",
                                        examples=["55340232221128675654"])
    timestamp: datetime = Field(description="Publication time of the round (updatedAt, UTC).",
                                examples=["2023-03-11T11:37:35Z"])
    answer: float = Field(description="Answer of the round, scaled to USD per unit (the on-chain integer divided by "
                                      "10^decimals of the feed).", examples=[65.23103673])
    answer_column: str | None = Field(None, description="CSV column holding the answer.",
                                      examples=["answer_normalized"])
    timestamp_column: str | None = Field(None, description="CSV column holding the publication time.",
                                         examples=["round_updated_at_utc"])
    phase_rule: str = Field("active_phase_at_t", description="active_phase_at_t: latest round at or before T of the "
                                                             "phase the proxy served at T (the latest round wins a "
                                                             "tie); all_phases: latest row of the file at or before T "
                                                             "(the first CSV row wins a tie).")


class ServedPhaseV3(BaseModel):
    """Part of the S_stat window in which ``phase`` was the aggregator phase the feed's proxy served."""
    phase: int = Field(description="Aggregator phase served by the proxy over this interval.", examples=[3])
    from_utc: datetime = Field(description="Start of the interval (included).")
    to_utc: datetime = Field(description="End of the interval (excluded).")


class SStatInputsV3(BaseModel):
    """Inputs of S_stat: the median and MAD of the reference prices of the window before T, and the price tested."""
    rule: str = Field(description="mad_z_score (formula applied), insufficient_data_floor (fewer than "
                                  "min_observations: value = floor), mad_zero (value = 1), missing_columns, "
                                  "nonpositive_price.")
    method: str = Field(description="mad: z = 0.6745 |price_tested - median| / mad. log_mad / log_realized_vol "
                                    "(confidence_v2.s_stat.normalize_by_volatility): z = |ln(price_tested) - "
                                    "median_log| / local_volatility. Then value = exp(-z^2 / (2 sigma_mad^2)).")
    reference_file: str | None = Field(None, description="CSV file whose prices form the reference window.",
                                       examples=["aave/chainlink_aave_usd.csv"])
    reference_kind: str | None = Field(None, description="source_pool (the pool that priced, levels 0a/1/2 direct) "
                                                         "or chainlink_feed (cross-rates).")
    reference_timestamp_column: str | None = Field(None, description="CSV column of the reference timestamps.",
                                                   examples=["round_updated_at_utc"])
    reference_price_column: str | None = Field(None, description="CSV column of the reference prices.",
                                               examples=["answer_normalized"])
    window_start_utc: datetime = Field(description="Start of the reference window (included): T minus "
                                                   "confidence_v2.s_stat.window_seconds (7 days).",
                                       examples=["2023-03-04T12:00:00Z"])
    window_end_utc: datetime = Field(description="End of the reference window (excluded): T.",
                                     examples=["2023-03-11T12:00:00Z"])
    window_bound_policy: str = Field("left_closed_right_open",
                                     description="Bounds of the window: window_start_utc <= t < window_end_utc.")
    window_row_limit: int | None = Field(None, description="Legacy mode only: the window is cut to its first rows.")
    served_phases: list[ServedPhaseV3] | None = Field(
        None, description="Chainlink reference read with the phase filter: only rounds of the phase the proxy served "
                          "at their own time count. Null: every row of the window counts.")
    n_observations: int = Field(0, description="Number of reference prices in the window.", examples=[199])
    median: float | None = Field(None, description="sorted(prices)[n // 2] (upper median for an even n).",
                                 examples=[74.1053])
    mad: float | None = Field(None, description="sorted(|p - median|)[n // 2].", examples=[2.349908880000001])
    median_log: float | None = Field(None, description="Methods log_mad and log_realized_vol only: upper median of "
                                                       "ln(price).")
    local_volatility: float | None = Field(None, description="Methods log_mad and log_realized_vol only: MAD of "
                                                             "ln(price) / 0.6745, or the standard deviation of the "
                                                             "log returns.")
    price_tested: float | None = Field(None, description="Price compared with the median, in the unit of "
                                                         "price_tested_field.", examples=[71.99640839241866])
    price_tested_field: str = Field("price_raw_in_quote", description="Response field holding price_tested.")
    z_score: float | None = Field(None, description="Modified z-score of price_tested (rule mad_z_score); null for "
                                                    "the other rules.", examples=[0.6053202323800804])
    sigma_mad: float = Field(description="Parameter thresholds.sigma_mad.", examples=[3.5])
    min_observations: int = Field(description="Parameter thresholds.min_swaps_for_stat_score.", examples=[3])
    floor: float = Field(description="Parameter thresholds.s_stat_floor: the value served below min_observations.",
                         examples=[0.2])
    value: float | None = Field(description="S_stat, in [0, 1]. Equals confidence.subscores.S_stat.",
                                examples=[0.9851556641143034])


class SLiqLegV3(BaseModel):
    """One leg of S_liq: the row scored, its TVL and slippage, and the leg value."""
    role: str = Field(description="pool (a direct price), token_leg or eth_usd_leg (a cross-rate).")
    file: str = Field(description="CSV file of the pool.", examples=["aave/aave_weth_uniswap_v3_03.csv"])
    row: RowRefV3 | None = Field(None, description="Row whose TVL and slippage are scored: the as-of row at T "
                                                   "(latest swap at or before T), for windowed prices too.")
    tvl_column: str | None = Field(None, description="CSV column holding the TVL.", examples=["pool_tvl_at_block"])
    tvl_raw: float | None = Field(None, description="TVL as read in the row, in the unit tvl_unit.",
                                  examples=[2459.725415049526])
    tvl_unit: str = Field("usd", description="Unit the API assumes for the TVL column: usd, or eth (converted with "
                                             "eth_usd_price_for_tvl when there is one).")
    eth_usd_price_for_tvl: float | None = Field(None, description="Price of the ETH/USD leg's as-of row, which "
                                                                 "converts a TVL in ETH.")
    price_column: str | None = Field(None, description="eth_usd_leg only: the ETH/USD price column of the file.")
    price: float | None = Field(None, description="eth_usd_leg only: the row's ETH/USD price (it converts the token "
                                                  "leg's TVL when that TVL is in ETH).")
    tvl_usd: float | None = Field(None, description="TVL that S_TVL scores.")
    slippage_column: str | None = Field(None, description="CSV column holding the slippage.", examples=["slip_1k"])
    slippage: float | None = Field(None, description="Relative cost of the extraction's reference order (1 000 USDC "
                                                     "or USDT on a pool quoted in a stablecoin, 1 WETH on a TOKEN/WETH "
                                                     "pool), pool fee included, as read in the row (0.0032 = 0.32 %): "
                                                     "1 - received / expected at the block's reference price. Null "
                                                     "when the column is empty: the leg value is then s_tvl alone.",
                                   examples=[0.0031904093500543])
    s_tvl: float | None = Field(None, description="TVL score in [0, 1], computed from tvl_usd with tvl_score_mode.",
                                examples=[1.0])
    s_slip: float | None = Field(None, description="Slippage score in [0, 1]: exp(-|slippage| / slip_max).",
                                 examples=[0.5283048100882693])
    value: float | None = Field(None, description="sqrt(s_tvl * s_slip), or the one that exists.")


class SLiqInputsV3(BaseModel):
    """Inputs of S_liq: one entry per leg, the parameters, and how the legs combine."""
    rule: str = Field(description="single_leg, geometric_mean_of_legs (sqrt of the two leg values), token_leg_only, "
                                  "eth_usd_leg_only, or unavailable (no TVL or slippage in any leg).")
    legs: list[SLiqLegV3] = Field(description="One leg for a direct price (role pool); two for a cross-rate "
                                              "(token_leg, then eth_usd_leg).")
    tvl_score_mode: str = Field(description="log_memoire: s_tvl = clamp(log10(tvl/min) / log10(ref/min), 0, 1); "
                                            "linear_to_threshold: min(1, tvl / seuil_TVL_min_usd); "
                                            "binary_threshold: 1 if tvl >= seuil_TVL_min_usd else 0.")
    tvl_log_min_usd: float = Field(description="Parameter scoring.tvl_log_min_usd (USD): TVL scored 0 in "
                                               "log_memoire mode.", examples=[10000.0])
    tvl_log_ref_usd: float = Field(description="Parameter scoring.tvl_log_ref_usd (USD): TVL scored 1 in "
                                               "log_memoire mode.", examples=[1000000.0])
    seuil_TVL_min_usd: float = Field(description="Parameter thresholds.seuil_TVL_min_usd (USD), used by the "
                                                 "linear_to_threshold and binary_threshold modes.",
                                     examples=[100000.0])
    slip_max: float = Field(description="s_slip = exp(-|slippage| / slip_max).")
    value: float | None = Field(description="S_liq, in [0, 1]. Equals confidence.subscores.S_liq.",
                                examples=[0.8314683866366447])


class SCohInputsV3(BaseModel):
    """Inputs of S_coh: the Chainlink round compared, the price compared and the deviation."""
    rule: str = Field(description="deviation (value = exp(-(delta / delta_tol)^2)), no_chainlink_observation, "
                                  "chainlink_zero_price or missing_columns.")
    chainlink_round: ChainlinkRoundV3 | None = Field(None, description="Latest round at or before T.")
    phase_rule: str = Field(description="active_phase_at_t (rounds of the phase the proxy served at T) or "
                                        "all_phases.")
    price_compared: float | None = Field(None, description="On-chain price compared with the round's answer (USD).",
                                         examples=[65.49202894941746])
    price_compared_field: str = Field(description="Response field holding price_compared.")
    delta: float | None = Field(None, description="|price_compared - answer| / answer.")
    delta_tol: float = Field(description="Tolerance used for the asset (v3.coh_delta_tol_by_asset, else "
                                         "confidence_v2.coh): 0.0184 = 1.84 %.", examples=[0.0184])
    value: float | None = Field(description="S_coh, in [0, 1]. Equals confidence.subscores.S_coh.",
                                examples=[0.9538168174605365])


class SPegInputsV3(BaseModel):
    """Inputs of S_peg: the round of the quote currency's USD feed."""
    rule: str = Field(description="peg_deviation (value = exp(-(|peg - 1| / peg_tol)^2)) or not_applicable.")
    quote_currency: str | None = Field(None, description="Quote currency of the source pool, or of the ETH/USD leg "
                                                         "of a cross-rate: USDC or USDT. Another quote currency "
                                                         "has no peg feed (rule not_applicable).",
                                       examples=["USDC"])
    peg_round: ChainlinkRoundV3 | None = Field(None, description="Latest round at or before T of the quote "
                                                                 "currency's USD feed; its answer is the peg.")
    peg: float | None = Field(None, description="USD value of one unit of the quote currency: the round's answer. "
                                                "The served price is price_raw_in_quote x peg.",
                              examples=[0.90965689])
    peg_tol: float = Field(description="Parameter confidence_v2.peg.tol: 0.0025 = 0.25 %.", examples=[0.0025])
    value: float | None = Field(description="S_peg, in [0, 1]. Equals confidence.S_peg.", examples=[0.0])


class CompositionV3(BaseModel):
    """How the sub-scores combine into C, and the fragility flag."""
    mode: str = Field(description="3sub: C = S_stat^w_stat * S_liq^w_liq * S_coh^w_coh. 4sub: the product of the "
                                  "four scores (S_peg included) to the power w_i / sum(w).")
    weights: dict[str, float] = Field(description="Weights w_stat, w_liq, w_coh (and w_peg in 4sub mode).",
                                      examples=[{"w_stat": 0.3333333333, "w_liq": 0.3333333333,
                                                 "w_coh": 0.3333333334}])
    C: float | None = Field(description="Confidence index, in [0, 1]. Null when a sub-score is missing. Equals "
                                        "confidence.C.", examples=[0.9210259328287568])
    fragility_c_threshold: float | None = Field(None, description="Threshold of the granularity "
                                                                  "(v3.fragility_c_threshold_by_granularity, else "
                                                                  "confidence_v2.fragility.c_threshold).",
                                                examples=[0.57])
    fragility_flag: bool | None = Field(None, description="C < fragility_c_threshold.")


class ConfidenceInputsV3(BaseModel):
    """The inputs of every confidence score. ``tools/verify_confidence.py`` recomputes the index from this block."""
    s_stat: SStatInputsV3 | None = Field(None, description="Inputs of S_stat: reference file and window, number of "
                                                           "observations, median, MAD, price tested.")
    s_liq: SLiqInputsV3 | None = Field(None, description="Inputs of S_liq: TVL and slippage of the row scored in "
                                                         "each leg, identified by block number and log index.")
    s_coh: SCohInputsV3 | None = Field(None, description="Inputs of S_coh: the Chainlink round (phase, round, "
                                                         "global id, publication time, answer) and the deviation.")
    s_peg: SPegInputsV3 | None = Field(None, description="Inputs of S_peg: the round of the peg feed.")
    composition: CompositionV3 = Field(description="Composition mode, weights, C, threshold and fragility flag.")


class ProvenanceV3(Provenance):
    """Provenance record of a V3 response. The fields before ``rejected_candidates`` are those of V1/V2, redeclared
    here with the same types and defaults so that the V3 OpenAPI schema documents them."""
    files_used: list[str] = Field(description="Files that priced the answer: the pool (or the Chainlink feed at "
                                              "level 3), then the ETH/USD reference of a cross-rate.",
                                  examples=[["aave/aave_weth_uniswap_v3_03.csv", "eth/eth_usdc_uniswap_v3_005.csv"]])
    branch_level: str = Field(description="Source level that answered: 0a, 0b, 1, 2, 3 (Chainlink fallback) or 4 "
                                          "(typed NULL).", examples=["0b"])
    branch_label: str = Field(description="direct_stable, cross_rate, alternative_pool, alternative_amm, "
                                          "chainlink_fallback or unavailable.", examples=["cross_rate"])
    calculation_path: list[str] = Field([], description="The computation in words, one step per item.")
    token_leg_timestamp: datetime | None = Field(None, description="Cross-rate: time of the token leg (T for a "
                                                                   "windowed price).")
    eth_usd_leg_timestamp: datetime | None = Field(None, description="Cross-rate: time of the ETH/USD leg.")
    cross_rate_lag_seconds: float | None = Field(None, description="Cross-rate: lag compared with "
                                                                   "cross_rate_max_lag_seconds (seconds): between "
                                                                   "the two legs for a raw read, between the "
                                                                   "ETH/USD leg and T for a windowed price.")
    n_raw: int | None = Field(None, description="Windowed price: swaps of the window before the MAD filter.",
                              examples=[88])
    swap_count: int | None = Field(None, description="Windowed price: swaps kept by the MAD filter, those of the "
                                                     "VWMP. Null for a raw read.", examples=[87])
    window_seconds: float | None = Field(None, description="Width of the window that priced (seconds), after the "
                                                           "expansion of rule R1. Null for a raw read.",
                                         examples=[86400.0])
    excluded_swaps: int | None = Field(None, description="Windowed price: swaps removed by the MAD filter.",
                                       examples=[1])
    # Temporal provenance fields (mémoire §6.8.1). Additive — populated for
    # windowed reads; null for raw point reads (no window).
    initial_window_seconds: float | None = Field(None, description="Width of the window of the granularity before "
                                                                   "any expansion: 60, 3600 or 86400 seconds.")
    window_start_utc: datetime | None = Field(None, description="Start of the window that priced (included).")
    window_end_utc: datetime | None = Field(None, description="End of the window that priced (excluded).")
    window_bound_policy: str | None = Field(None, description="left_closed_right_open: window_start_utc <= t < "
                                                              "window_end_utc.")
    expansion_step: int | None = Field(None, description="Step of rule R1 that priced: 0 = the initial window, "
                                                         "1 to 3 = an expanded window (data_status reconstructed).")
    # Reference block b_ref(T) (mémoire §6.2.6). Populated from the winning
    # source row when a block_number column exists (DEX); null otherwise.
    reference_block_number: int | None = Field(None, description="Raw read: Ethereum block of the swap that "
                                                                 "priced. Null for a windowed price (a VWMP "
                                                                 "aggregates several blocks) and for Chainlink.")
    reference_block_timestamp: datetime | None = Field(None, description="Time of that block (UTC).")
    parameters: dict[str, Any] = Field({}, description="Viability thresholds applied (seuil_TVL_min_usd and "
                                                       "seuil_vol_min_usd_24h in USD, fenetre_inactivite_jours in "
                                                       "days), the quote currency and the file and time of the "
                                                       "peg round used.")
    detected_columns: dict[str, list[str]] = Field({}, description="Columns found in the header of the source file.")
    warnings: list[Warning] = Field([], description="Diagnostics of the price computation.")
    rejected_candidates: list[RejectedCandidate] = Field(
        [], description="Sources the hierarchy evaluated before the one that answered and rejected, in evaluation "
                        "order, each with the rule and the value measured.")
    source_event: SourceEventV3 | None = Field(
        None, description="Raw point reads only: the on-chain event behind the price (a windowed price aggregates "
                          "many swaps and has none).")
    eth_usd_leg_event: SourceEventV3 | None = Field(
        None, description="Cross-rates whose ETH/USD leg is a point read: the swap of that leg.")
    dataset_version: str | None = Field(
        None, description="Version of the data store that answered (16 hex), as the X-Dataset-Version header. "
                          "GET /v3/datasets/versions/{version} returns its manifest.",
        examples=["a87252328cd8246d"])
    dataset_files: dict[str, str] = Field(
        {}, description="Version of each file read for this answer (price sources, Chainlink feed of S_coh, peg "
                        "feed): file -> version.")
    confidence_inputs: ConfidenceInputsV3 | None = Field(
        None, description="Inputs of every confidence score, enough to recompute the index from this record alone. "
                          "Null when there is no confidence index (level 3 or 4, or include_confidence=false).")
    config_sha256: str | None = Field(None, description="SHA-256 of every parameter V3 read (as GET /v3/config).",
                                      examples=["90ba8f5a63db1b45ecf8559400b9f4b08db01906d05866e9eb68271534864b28"])
    code_commit: str | None = Field(None, description="Git commit of the API code checked out when the service "
                                                      "started.",
                                    examples=["27e9d6679185d6a4312475c0846887b5e7d667ed"])
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
