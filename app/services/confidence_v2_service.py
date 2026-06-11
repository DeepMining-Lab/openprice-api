"""Confidence index — API V2 (CLAUDE.md §22).

V2 keeps the V1 sub-scores S_stat and S_liq unchanged (imported from
`confidence_service`) and adds three changes driven by the expert review:

1. Peg neutralization upstream of S_coh (§1): the DEX price is multiplied by
   the effective quote-currency peg (USDC/USD or USDT/USD, read as-of T from a
   Chainlink stablecoin feed) before measuring coherence against the asset feed.
2. A new separate sub-score S_peg (§2): exp(−(|peg−1| / peg_tol)²).
3. An optional volatility-normalized S_stat variant (§3), off by default.

Composition is weighted (exponents = configured weights), matching V1's form:
    3sub (default):  C = S_stat^w_stat · S_liq^w_liq · S_coh^w_coh
    4sub (optional): C = ∏ S_i^(w_i / Σw)  including S_peg

S_peg is published separately from C (it never enters the 3sub product).
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path

from app import csv_adapter, duckdb_client, registry
from app.config import AppConfig
from app.schemas import Warning

# Raw column names that carry the quote-token symbol across the dataset.
_QUOTE_SYMBOL_COLUMNS = ("quote_token_symbol", "quote_asset", "quote_currency")


# ---------------------------------------------------------------------------
# Quote currency detection + peg lookup
# ---------------------------------------------------------------------------

def read_quote_currency(schema: csv_adapter.SchemaInfo) -> str | None:
    """Return the pool's quote-token symbol (e.g. "USDC"), or None if absent.

    The symbol is constant per file, so the earliest row is sufficient. Returns
    None for files without a quote-symbol column (Curve crvUSD, Chainlink feeds),
    which signals that S_peg is not applicable.
    """
    quote_col = next((c for c in _QUOTE_SYMBOL_COLUMNS if c in schema.raw_columns), None)
    if quote_col is None:
        return None
    ts_col = schema.mapping.get("timestamp")
    if not ts_col:
        return None
    # Read the most recent value at or before "now"; the symbol never changes.
    row = duckdb_client.latest_at_or_before(
        schema.path, datetime.now().astimezone(), [ts_col, quote_col], ts_col
    )
    if row is None or row.get(quote_col) is None:
        return None
    return str(row[quote_col]).strip().upper()


def get_peg_at(
    quote_currency: str,
    timestamp: datetime,
    cfg: AppConfig,
) -> tuple[float | None, datetime | None, str | None, list[Warning]]:
    """Read peg(T) as-of T from the Chainlink stablecoin feed.

    Returns (peg_value, observed_timestamp, relative_path, warnings).
    peg_value is None when no peg feed is registered for the quote currency or
    no observation exists at or before T.
    """
    warnings: list[Warning] = []
    peg_path = registry.get_peg_feed_path(quote_currency)
    if peg_path is None or not peg_path.exists():
        return None, None, None, warnings

    schema = csv_adapter.inspect(peg_path)
    ts_col = schema.mapping.get("timestamp")
    price_col = schema.mapping.get("price_usd")
    if not ts_col or not price_col:
        return None, None, None, warnings

    row = duckdb_client.latest_at_or_before(peg_path, timestamp, [ts_col, price_col], ts_col)
    if row is None or row.get(price_col) is None:
        return None, None, None, warnings

    rel = str(peg_path.relative_to(cfg.paths.datasets_path))
    return float(row[price_col]), row[ts_col], rel, warnings


# ---------------------------------------------------------------------------
# S_peg — quote-currency peg stability (§2)
# ---------------------------------------------------------------------------

def compute_s_peg(
    peg_value: float | None,
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """S_peg = exp(−(|peg − 1| / peg_tol)²).

    Returns (None, [s_peg_not_applicable]) when no peg value is available
    (non-stablecoin quote or missing feed).
    """
    if peg_value is None:
        return None, [Warning(
            code="s_peg_not_applicable",
            message=(
                "Source has no stablecoin quotation asset (level 3 / Curve) "
                "or the peg feed is unavailable; S_peg = null."
            ),
            severity="info",
        )]
    tol = cfg.confidence_v2.peg.tol
    s_peg = math.exp(-((abs(peg_value - 1.0) / tol) ** 2))
    return s_peg, []


# ---------------------------------------------------------------------------
# S_coh (V2) — on the peg-neutralized DEX price (§1)
# ---------------------------------------------------------------------------

def compute_s_coh_v2(
    asset: str,
    dex_price_neutralized: float,
    chainlink_path: Path,
    timestamp: datetime,
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """DEX-vs-Chainlink coherence on the peg-neutralized price.

    δ = |P_DEX_neutralized − P_CL| / P_CL ; S_coh = exp(−(δ / δ_tol)²).
    δ_tol comes from confidence_v2.coh (decoupled from the feed's native
    deviation threshold), per recommendation 2.
    """
    warnings: list[Warning] = [Warning(
        code="coh_neutralized_peg",
        message="S_coh computed on the peg-neutralized DEX price (V2 behaviour).",
        severity="info",
    )]
    schema = csv_adapter.inspect(chainlink_path)
    ts_col = schema.mapping.get("timestamp")
    price_col = schema.mapping.get("price_usd")
    if not ts_col or not price_col:
        warnings.append(Warning(code="s_coh_chainlink_missing_columns",
                                message="Cannot compute S_coh: Chainlink file missing columns."))
        return None, warnings

    row = duckdb_client.latest_at_or_before(chainlink_path, timestamp, [ts_col, price_col], ts_col)
    if row is None or row.get(price_col) is None:
        warnings.append(Warning(code="s_coh_no_chainlink_observation",
                                message="No Chainlink observation found at or before timestamp."))
        return None, warnings

    cl_price = float(row[price_col])
    if cl_price == 0:
        warnings.append(Warning(code="s_coh_chainlink_zero_price",
                                message="Chainlink price is zero; S_coh cannot be computed."))
        return None, warnings

    tol = cfg.confidence_v2.coh.delta_tol_by_asset.get(
        asset, cfg.confidence_v2.coh.default_delta_tol
    )
    delta = abs(dex_price_neutralized - cl_price) / cl_price
    s_coh = math.exp(-((delta / tol) ** 2))
    return s_coh, warnings


# ---------------------------------------------------------------------------
# S_stat (V2) — configurable window + optional volatility normalization (§3)
# ---------------------------------------------------------------------------

def compute_s_stat_v2(
    price_path: Path,
    timestamp: datetime,
    price_at_t: float,
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """Local-anomaly score over a configurable window.

    With normalize_by_volatility=false (default) this reproduces the V1 MAD
    z-score on raw prices, but over confidence_v2.s_stat.window_seconds.
    With normalize_by_volatility=true it works in log space and normalizes by a
    local volatility estimator (MAD of log-prices or realized volatility of
    log-returns). It is a *local anomaly* score, not a direct volatility measure.
    """
    warnings: list[Warning] = []
    s_cfg = cfg.confidence_v2.s_stat
    schema = csv_adapter.inspect(price_path)
    ts_col = schema.mapping.get("timestamp")
    price_col = schema.mapping.get("price_usd")
    if not ts_col or not price_col:
        warnings.append(Warning(
            code="s_stat_missing_columns",
            message="Cannot compute S_stat: missing timestamp or price_usd column.",
        ))
        return cfg.thresholds.s_stat_floor, warnings

    window_start = timestamp - timedelta(seconds=s_cfg.window_seconds)
    rows = duckdb_client.range_query(
        price_path, window_start, timestamp,
        [ts_col, price_col], cfg.api.max_limit, ts_col,
    )
    prices = [float(r[price_col]) for r in rows if r[price_col] is not None]

    min_n = cfg.thresholds.min_swaps_for_stat_score
    if len(prices) < min_n:
        warnings.append(Warning(
            code="s_stat_insufficient_data",
            message=f"Only {len(prices)} observations in window (min {min_n}); using floor.",
        ))
        return cfg.thresholds.s_stat_floor, warnings

    sigma = cfg.thresholds.sigma_mad

    if not s_cfg.normalize_by_volatility:
        # V1-equivalent raw-price MAD z-score (configurable window only).
        sorted_p = sorted(prices)
        median_p = sorted_p[len(sorted_p) // 2]
        abs_devs = sorted([abs(p - median_p) for p in prices])
        mad = abs_devs[len(abs_devs) // 2]
        if mad == 0:
            return 1.0, warnings
        z = 0.6745 * abs(price_at_t - median_p) / mad
        return math.exp(-(z ** 2) / (2 * sigma ** 2)), warnings

    # Volatility-normalized variant (experimental, off by default).
    if price_at_t <= 0 or any(p <= 0 for p in prices):
        warnings.append(Warning(
            code="s_stat_nonpositive_price",
            message="Non-positive price encountered; log-space S_stat cannot be computed.",
        ))
        return cfg.thresholds.s_stat_floor, warnings

    logs = [math.log(p) for p in prices]
    sorted_logs = sorted(logs)
    median_log = sorted_logs[len(sorted_logs) // 2]

    if s_cfg.volatility_estimator == "realized_vol":
        returns = [logs[i] - logs[i - 1] for i in range(1, len(logs))]
        mean_r = sum(returns) / len(returns) if returns else 0.0
        var = sum((r - mean_r) ** 2 for r in returns) / len(returns) if returns else 0.0
        local_vol = math.sqrt(var)
    else:  # MAD of log-prices, scaled to a standard-deviation equivalent
        abs_devs = sorted([abs(lp - median_log) for lp in logs])
        mad_log = abs_devs[len(abs_devs) // 2]
        local_vol = mad_log / 0.6745

    if local_vol == 0:
        return 1.0, warnings

    z = abs(math.log(price_at_t) - median_log) / local_vol
    return math.exp(-(z ** 2) / (2 * sigma ** 2)), warnings


# ---------------------------------------------------------------------------
# Composition + fragility
# ---------------------------------------------------------------------------

def compose_v2(
    s_stat: float | None,
    s_liq: float | None,
    s_coh: float | None,
    s_peg: float | None,
    cfg: AppConfig,
) -> tuple[float | None, str]:
    """Weighted geometric composition. Returns (C, composition_mode).

    3sub: C = S_stat^w_stat · S_liq^w_liq · S_coh^w_coh (S_peg excluded).
    4sub: includes S_peg^w_peg with the four weights renormalized to sum to 1.
    Returns C = None if any required sub-score is None.
    """
    mode = cfg.confidence_v2.composite.mode
    w = cfg.confidence_v2.weights

    if mode == "4sub":
        if None in (s_stat, s_liq, s_coh, s_peg):
            return None, mode
        weights = [w.w_stat, w.w_liq, w.w_coh, w.w_peg]
        scores = [s_stat, s_liq, s_coh, s_peg]
        total = sum(weights)
        c = math.prod(s ** (wi / total) for s, wi in zip(scores, weights))
        return c, mode

    # 3sub (default)
    if None in (s_stat, s_liq, s_coh):
        return None, mode
    c = (s_stat ** w.w_stat) * (s_liq ** w.w_liq) * (s_coh ** w.w_coh)
    return c, mode


def fragility_flag(
    score: float | None,
    cfg: AppConfig,
) -> tuple[bool | None, list[Warning]]:
    """fragility_flag = (C < c_threshold).

    Returns (None, warning) when c_threshold is not calibrated (null) so the
    API imposes no qualitative decision until the threshold is set empirically.
    """
    threshold = cfg.confidence_v2.fragility.c_threshold
    if threshold is None:
        return None, [Warning(
            code="fragility_threshold_uncalibrated",
            message="confidence_v2.fragility.c_threshold is not set; fragility_flag = null.",
            severity="info",
        )]
    if score is None:
        return None, []
    return score < threshold, []


