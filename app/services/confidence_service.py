"""Confidence index — S_stat, S_liq, S_coh, final weighted geometric mean."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app import csv_adapter, duckdb_client
from app.config import AppConfig
from app.schemas import Warning


@dataclass
class ConfidenceResult:
    score: float | None
    s_stat: float | None
    s_liq: float | None
    s_coh: float | None
    coherence_mode: str | None = None
    warnings: list[Warning] = field(default_factory=list)


# ---------------------------------------------------------------------------
# S_stat
# ---------------------------------------------------------------------------

def compute_s_stat(
    price_path: Path,
    timestamp: datetime,
    price_at_t: float,
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """MAD-based statistical hygiene score."""
    warnings: list[Warning] = []
    schema = csv_adapter.inspect(price_path)
    ts_col = schema.mapping.get("timestamp")
    price_col = schema.mapping.get("price_usd")
    if not ts_col or not price_col:
        warnings.append(Warning(
            code="s_stat_missing_columns",
            message="Cannot compute S_stat: missing timestamp or price_usd column.",
        ))
        return cfg.thresholds.s_stat_floor, warnings

    window_start = timestamp - timedelta(days=7)
    rows = duckdb_client.range_query(
        price_path, window_start, timestamp,
        [ts_col, price_col], cfg.api.max_limit, ts_col,
    )
    prices = [r[price_col] for r in rows if r[price_col] is not None]

    min_n = cfg.thresholds.min_swaps_for_stat_score
    if len(prices) < min_n:
        warnings.append(Warning(
            code="s_stat_insufficient_data",
            message=f"Only {len(prices)} observations in 7-day window (min {min_n}); using floor.",
        ))
        return cfg.thresholds.s_stat_floor, warnings

    sorted_p = sorted(prices)
    median_p = sorted_p[len(sorted_p) // 2]
    abs_devs = sorted([abs(p - median_p) for p in prices])
    mad = abs_devs[len(abs_devs) // 2]

    if mad == 0:
        return 1.0, warnings

    z_mad = 0.6745 * abs(price_at_t - median_p) / mad
    sigma = cfg.thresholds.sigma_mad
    s_stat = math.exp(-(z_mad ** 2) / (2 * sigma ** 2))
    return s_stat, warnings


# ---------------------------------------------------------------------------
# S_liq
# ---------------------------------------------------------------------------

def compute_s_liq(
    schema: csv_adapter.SchemaInfo,
    row: dict[str, Any],
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """Liquidity score from TVL and slippage."""
    warnings: list[Warning] = []
    tvl_col = schema.mapping.get("tvl_usd")
    slip_col = schema.mapping.get("slippage")

    tvl_val: float | None = None
    slip_val: float | None = None

    if tvl_col and row.get(tvl_col) is not None:
        try:
            tvl_val = float(row[tvl_col])
        except (ValueError, TypeError):
            warnings.append(Warning(code="tvl_parse_error",
                                    message=f"TVL value could not be parsed ({row[tvl_col]!r}); skipped."))
    if slip_col and row.get(slip_col) is not None:
        try:
            slip_val = float(row[slip_col])
        except (ValueError, TypeError):
            warnings.append(Warning(code="slippage_parse_error",
                                    message=f"Slippage value could not be parsed ({row[slip_col]!r}); skipped."))

    threshold = cfg.thresholds.seuil_TVL_min_usd
    mode = cfg.scoring.tvl_score_mode

    s_tvl: float | None = None
    if tvl_val is not None:
        if mode == "linear_to_threshold":
            s_tvl = min(1.0, tvl_val / threshold)
        elif mode == "binary_threshold":
            s_tvl = 1.0 if tvl_val >= threshold else 0.0
        elif mode == "log_memoire":
            tvl_min = cfg.scoring.tvl_log_min_usd
            tvl_ref = cfg.scoring.tvl_log_ref_usd
            if tvl_val <= 0 or tvl_min <= 0 or tvl_ref <= tvl_min:
                s_tvl = 0.0
            else:
                s_tvl = max(0.0, min(1.0,
                    math.log10(tvl_val / tvl_min) / math.log10(tvl_ref / tvl_min)
                ))
    else:
        warnings.append(Warning(code="missing_tvl_column",
                                message="TVL viability check could not be evaluated."))

    s_slip: float | None = None
    if slip_val is not None:
        s_slip = math.exp(-abs(slip_val) / cfg.thresholds.slip_max)
    else:
        warnings.append(Warning(code="missing_slippage_column",
                                message="Slippage column not found; slippage score unavailable."))

    if s_tvl is not None and s_slip is not None:
        return math.sqrt(s_tvl * s_slip), warnings
    if s_tvl is not None:
        return s_tvl, warnings
    if s_slip is not None:
        return s_slip, warnings

    warnings.append(Warning(code="liquidity_score_unavailable",
                            message="TVL and slippage columns were not found for the selected source."))
    return None, warnings


# ---------------------------------------------------------------------------
# S_coh
# ---------------------------------------------------------------------------

def compute_s_coh_dex_vs_chainlink(
    asset: str,
    dex_price: float,
    chainlink_path: Path,
    timestamp: datetime,
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """DEX vs Chainlink coherence score."""
    warnings: list[Warning] = []
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

    tol = cfg.chainlink.deviation_threshold_by_asset.get(
        asset, cfg.chainlink.default_deviation_threshold
    )
    delta = abs(dex_price - cl_price) / cl_price
    s_coh = math.exp(-((delta / tol) ** 2))
    return s_coh, warnings


def compute_s_coh_oracle_staleness(
    asset: str,
    chainlink_path: Path,
    timestamp: datetime,
    cfg: AppConfig,
) -> tuple[float | None, list[Warning]]:
    """Oracle-only staleness coherence for level-3 fallback."""
    warnings: list[Warning] = []
    schema = csv_adapter.inspect(chainlink_path)
    ts_col = schema.mapping.get("timestamp")
    price_col = schema.mapping.get("price_usd")
    if not ts_col or not price_col:
        warnings.append(Warning(code="s_coh_oracle_missing_columns",
                                message="Chainlink file missing columns for staleness check."))
        return None, warnings

    row = duckdb_client.latest_at_or_before(chainlink_path, timestamp, [ts_col, price_col], ts_col)
    if row is None:
        warnings.append(Warning(code="s_coh_no_oracle_observation",
                                message="No Chainlink observation for staleness computation."))
        return None, warnings

    obs_ts = row[ts_col]
    if hasattr(obs_ts, "timestamp"):
        staleness = (timestamp - obs_ts).total_seconds()
    else:
        staleness = cfg.chainlink.heartbeat_seconds_by_asset.get(asset, cfg.thresholds.chainlink_default_heartbeat_seconds)

    heartbeat = cfg.chainlink.heartbeat_seconds_by_asset.get(
        asset, cfg.thresholds.chainlink_default_heartbeat_seconds
    )
    s_coh = math.exp(-staleness / heartbeat)
    return s_coh, warnings


# ---------------------------------------------------------------------------
# Final composition
# ---------------------------------------------------------------------------

def compose(
    s_stat: float | None,
    s_liq: float | None,
    s_coh: float | None,
    cfg: AppConfig,
) -> float | None:
    """Weighted geometric mean: C = S_stat^w_stat * S_liq^w_liq * S_coh^w_coh."""
    if s_stat is None or s_liq is None or s_coh is None:
        return None
    w = cfg.confidence_weights
    return (s_stat ** w.w_stat) * (s_liq ** w.w_liq) * (s_coh ** w.w_coh)
