"""Price service — implements the source hierarchy 0a → 0b → 1 → 2 → 3 → 4."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app import csv_adapter, duckdb_client, registry
from app.config import AppConfig
from app.schemas import Warning

# Window sizes in seconds for each expansion step (R1 rule).
# Day has no expansion: falls directly to the next source level.
_WINDOW_STEPS: dict[str, list[int]] = {
    "minute": [60, 120, 300, 900],
    "hour":   [3600, 7200, 14400, 28800],
    "day":    [86400],
}


@dataclass
class PriceResult:
    price_usd: float | None
    timestamp_observed: datetime | None
    branch_level: str
    branch_label: str
    data_status: str
    files_used: list[str] = field(default_factory=list)
    calculation_path: list[str] = field(default_factory=list)
    token_leg_timestamp: datetime | None = None
    eth_usd_leg_timestamp: datetime | None = None
    cross_rate_lag_seconds: float | None = None
    detected_columns: dict[str, list[str]] = field(default_factory=dict)
    warnings: list[Warning] = field(default_factory=list)
    unavailable_reason: str | None = None
    source_row: dict[str, Any] | None = None
    source_schema: csv_adapter.SchemaInfo | None = None
    eth_source_row: dict[str, Any] | None = None
    eth_source_schema: csv_adapter.SchemaInfo | None = None
    granularity: str = "raw"
    n_raw: int | None = None
    swap_count: int | None = None
    window_seconds: float | None = None
    excluded_swaps: int | None = None


# ---------------------------------------------------------------------------
# Zombie pool check
# ---------------------------------------------------------------------------

def _is_zombie(
    schema: csv_adapter.SchemaInfo,
    row: dict[str, Any],
    timestamp: datetime,
    cfg: AppConfig,
) -> tuple[bool, list[Warning]]:
    warnings: list[Warning] = []
    is_zombie = False

    tvl_col = schema.mapping.get("tvl_usd")
    vol_col = schema.mapping.get("volume_usd")
    ts_col = schema.mapping.get("timestamp")

    if tvl_col:
        tvl = row.get(tvl_col)
        try:
            if tvl is not None and float(tvl) < cfg.thresholds.seuil_TVL_min_usd:
                is_zombie = True
        except (ValueError, TypeError):
            warnings.append(Warning(code="tvl_parse_error",
                                    message=f"TVL value could not be parsed as a number (got {tvl!r}); viability check skipped."))
    else:
        warnings.append(Warning(code="missing_tvl_column",
                                message="TVL viability check could not be evaluated for this source file."))

    if vol_col and ts_col:
        # volume_usdc/volume_usdt are per-swap amounts, not 24h aggregates.
        # Sum all swaps in the 24h window ending at T to get the true daily volume.
        window_start = timestamp - timedelta(hours=24)
        vol_24h = duckdb_client.sum_column_in_range(
            schema.path, vol_col, window_start, timestamp, ts_col
        )
        if vol_24h is not None and vol_24h < cfg.thresholds.seuil_vol_min_usd_24h:
            is_zombie = True
        elif vol_24h is None:
            warnings.append(Warning(code="volume_sum_empty",
                                    message="No swaps found in the 24h window; volume viability check skipped."))
    elif schema.mapping.get("volume_token"):
        # Pool uses ETH/WETH-denominated swap volumes — cannot compare to USD threshold.
        warnings.append(Warning(code="volume_not_usd",
                                message="Volume column is token-denominated (ETH/WETH/crvUSD); USD volume check skipped."))
    else:
        warnings.append(Warning(code="missing_volume_column",
                                message="Volume viability check could not be evaluated."))

    return is_zombie, warnings


def _is_recently_active(
    path: Path,
    ts_col: str,
    timestamp: datetime,
    cfg: AppConfig,
) -> bool:
    window_start = timestamp - timedelta(days=cfg.thresholds.fenetre_inactivite_jours)
    count = duckdb_client.count_rows_in_range(path, window_start, timestamp, ts_col)
    return count > 0


# ---------------------------------------------------------------------------
# VWMP and MAD utilities (windowed granularities)
# ---------------------------------------------------------------------------

def _compute_vwmp(prices: list[float], volumes: list[float]) -> float | None:
    """Volume-Weighted Median Price.

    Sort swaps by price; return the price where cumulative volume first reaches
    >= 50 % of total volume.  This is the operational definition of VWMP used
    in the research methodology (mémoire, 2026).
    """
    if not prices:
        return None
    if len(prices) == 1:
        return prices[0]

    total = sum(volumes)
    if total <= 0:
        sorted_prices = sorted(prices)
        return sorted_prices[len(sorted_prices) // 2]

    pairs = sorted(zip(prices, volumes), key=lambda x: x[0])
    cumulative = 0.0
    half = total / 2.0
    for price, volume in pairs:
        cumulative += volume
        if cumulative >= half:
            return price
    return pairs[-1][0]


def _filter_mad_outliers(
    prices: list[float],
    volumes: list[float],
    sigma_mad: float,
) -> tuple[list[float], list[float], int]:
    """Remove price outliers via the modified Z-score (MAD) method.

    Returns (kept_prices, kept_volumes, n_excluded).
    Also filters MEV-like outliers because extreme sandwich prices have
    z_MAD >> sigma_mad and are caught by the same criterion.
    """
    if len(prices) < 3:
        return prices, volumes, 0

    sorted_p = sorted(prices)
    n = len(sorted_p)
    median = sorted_p[n // 2]
    abs_devs = [abs(p - median) for p in prices]
    sorted_devs = sorted(abs_devs)
    mad = sorted_devs[n // 2]

    if mad == 0:
        return prices, volumes, 0

    kept_p: list[float] = []
    kept_v: list[float] = []
    excluded = 0
    for p, v, dev in zip(prices, volumes, abs_devs):
        z = 0.6745 * dev / mad
        if z <= sigma_mad:
            kept_p.append(p)
            kept_v.append(v)
        else:
            excluded += 1

    return kept_p, kept_v, excluded


# ---------------------------------------------------------------------------
# ETH/USD reference (shared by all cross-rate levels)
# ---------------------------------------------------------------------------

def _get_eth_usd_at(
    timestamp: datetime, cfg: AppConfig, asset: str = "ETH"
) -> tuple[float | None, datetime | None, str | None, dict | None, "csv_adapter.SchemaInfo | None"]:
    """Return (eth_price, obs_timestamp, relative_path, row, schema) from best ETH/USD reference."""
    for path in registry.get_eth_usd_reference_paths(asset):
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_usd")
        if not ts_col or not price_col:
            continue
        cols = [ts_col, price_col]
        for c in ("tvl_usd", "slippage"):
            mapped = schema.mapping.get(c)
            if mapped:
                cols.append(mapped)
        row = duckdb_client.latest_at_or_before(path, timestamp, cols, ts_col)
        if row and row.get(price_col) is not None:
            rel = str(path.relative_to(cfg.paths.datasets_path))
            return float(row[price_col]), row[ts_col], rel, row, schema
    return None, None, None, None, None


# ---------------------------------------------------------------------------
# Core pool-selection helpers (parameterized by branch level)
# ---------------------------------------------------------------------------

def _try_direct_stable_pools(
    asset: str,
    timestamp: datetime,
    cfg: AppConfig,
    candidate_paths: list[Path],
    branch_level: str,
    branch_label: str,
) -> PriceResult | None:
    """Point-read from direct TOKEN/USDC or TOKEN/USDT pools.

    Selects the highest-TVL non-zombie, recently-active pool.
    Used by levels 0a and 1 (direct stable).
    """
    best: PriceResult | None = None
    best_tvl: float = -1.0

    for path in candidate_paths:
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_usd")
        if not ts_col or not price_col:
            continue

        cols = [ts_col, price_col]
        for c in ["tvl_usd", "volume_usd", "slippage"]:
            mapped = schema.mapping.get(c)
            if mapped:
                cols.append(mapped)

        row = duckdb_client.latest_at_or_before(path, timestamp, cols, ts_col)
        if row is None or row.get(price_col) is None:
            continue

        zombie, z_warns = _is_zombie(schema, row, timestamp, cfg)
        if zombie:
            continue
        if not _is_recently_active(path, ts_col, timestamp, cfg):
            continue

        tvl_col = schema.mapping.get("tvl_usd")
        try:
            tvl = float(row[tvl_col]) if tvl_col and row.get(tvl_col) else 0.0
        except (ValueError, TypeError):
            tvl = 0.0

        if tvl > best_tvl or best is None:
            best_tvl = tvl
            best = PriceResult(
                price_usd=float(row[price_col]),
                timestamp_observed=row[ts_col],
                branch_level=branch_level,
                branch_label=branch_label,
                data_status="observed",
                files_used=[str(path.relative_to(cfg.paths.datasets_path))],
                calculation_path=["Direct stablecoin price from pool"],
                detected_columns={path.name: schema.raw_columns},
                source_row=row,
                source_schema=schema,
                warnings=z_warns,
            )

    return best


def _try_cross_rate_pools(
    asset: str,
    timestamp: datetime,
    cfg: AppConfig,
    token_paths: list[Path],
    branch_level: str,
    branch_label: str,
) -> PriceResult | None:
    """Point-read cross-rate: TOKEN/ETH × ETH/USD.

    Used by levels 0b (Uniswap V3), 1 (Uniswap V2), and 2 (SushiSwap).
    Returns the first viable pool that passes zombie and lag checks.
    """
    eth_price, eth_ts, eth_file, eth_row, eth_schema = _get_eth_usd_at(timestamp, cfg, asset)
    if eth_price is None:
        return None

    for path in token_paths:
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_token_eth")
        if not ts_col or not price_col:
            continue

        cols = [ts_col, price_col]
        for c in ["tvl_usd", "volume_usd", "slippage"]:
            mapped = schema.mapping.get(c)
            if mapped:
                cols.append(mapped)

        row = duckdb_client.latest_at_or_before(path, timestamp, cols, ts_col)
        if row is None or row.get(price_col) is None:
            continue

        token_ts = row[ts_col]
        if eth_ts is not None and hasattr(token_ts, "timestamp") and hasattr(eth_ts, "timestamp"):
            lag = abs((token_ts - eth_ts).total_seconds())
        else:
            lag = 0.0

        if lag > cfg.thresholds.cross_rate_max_lag_seconds:
            continue

        zombie, z_warns = _is_zombie(schema, row, timestamp, cfg)
        if zombie:
            continue

        token_eth_price = float(row[price_col])
        price_usd = token_eth_price * eth_price

        rel_token = str(path.relative_to(cfg.paths.datasets_path))
        files = [rel_token]
        if eth_file:
            files.append(eth_file)

        return PriceResult(
            price_usd=price_usd,
            timestamp_observed=token_ts,
            branch_level=branch_level,
            branch_label=branch_label,
            data_status="observed",
            files_used=files,
            calculation_path=[
                f"{asset}/ETH or WETH leg: {rel_token}",
                f"ETH/USD leg: {eth_file}",
                f"{asset}/USD = {asset}/ETH × ETH/USD",
            ],
            token_leg_timestamp=token_ts,
            eth_usd_leg_timestamp=eth_ts,
            cross_rate_lag_seconds=lag,
            detected_columns={path.name: schema.raw_columns},
            source_row=row,
            source_schema=schema,
            eth_source_row=eth_row,
            eth_source_schema=eth_schema,
            warnings=z_warns,
        )
    return None


def _try_direct_stable_pools_windowed(
    asset: str,
    timestamp: datetime,
    cfg: AppConfig,
    granularity: str,
    candidate_paths: list[Path],
    branch_level: str,
    branch_label: str,
) -> PriceResult | None:
    """VWMP over a symmetric window for direct stablecoin pools.

    All viable pools are sorted by TVL descending. For each pool, R1 window
    expansion is attempted in order. The first pool that yields at least one
    swap in some window wins.
    """
    viable: list[tuple[float, Path, csv_adapter.SchemaInfo, dict[str, Any], list[Warning]]] = []

    for path in candidate_paths:
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_usd")
        if not ts_col or not price_col:
            continue

        cols: list[str] = [ts_col, price_col]
        for c in ("tvl_usd", "volume_usd", "slippage"):
            mapped = schema.mapping.get(c)
            if mapped:
                cols.append(mapped)

        viability_row = duckdb_client.latest_at_or_before(path, timestamp, cols, ts_col)
        if viability_row is None:
            continue

        zombie, z_warns = _is_zombie(schema, viability_row, timestamp, cfg)
        if zombie:
            continue
        if not _is_recently_active(path, ts_col, timestamp, cfg):
            continue

        tvl_col = schema.mapping.get("tvl_usd")
        try:
            tvl = float(viability_row[tvl_col]) if tvl_col and viability_row.get(tvl_col) else 0.0
        except (ValueError, TypeError):
            tvl = 0.0

        viable.append((tvl, path, schema, viability_row, z_warns))

    viable.sort(key=lambda x: x[0], reverse=True)

    for tvl, best_path, schema, best_viability_row, pool_warnings in viable:
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_usd")
        vol_col = schema.mapping.get("volume_usd")
        slip_col = schema.mapping.get("slippage")
        tvl_col = schema.mapping.get("tvl_usd")

        cols = [ts_col, price_col]
        for c in (vol_col, slip_col, tvl_col):
            if c:
                cols.append(c)

        for window_s in _WINDOW_STEPS[granularity]:
            half = window_s / 2.0
            w_start = timestamp - timedelta(seconds=half)
            w_end = timestamp + timedelta(seconds=half)

            rows = duckdb_client.range_query(best_path, w_start, w_end, cols, cfg.api.max_limit, ts_col)
            if not rows:
                continue

            n_raw = len(rows)
            prices = [float(r[price_col]) for r in rows if r.get(price_col) is not None]
            volumes = (
                [float(r[vol_col]) if r.get(vol_col) is not None else 1.0
                 for r in rows if r.get(price_col) is not None]
                if vol_col else [1.0] * len(prices)
            )
            if not prices:
                continue

            prices_clean, volumes_clean, excluded = _filter_mad_outliers(
                prices, volumes, cfg.thresholds.sigma_mad
            )
            warns: list[Warning] = list(pool_warnings)

            if excluded > 0:
                warns.append(Warning(
                    code="mad_outliers_excluded",
                    message=f"{excluded} swap(s) excluded by MAD filter (sigma_mad={cfg.thresholds.sigma_mad}).",
                ))
            if not prices_clean:
                prices_clean, volumes_clean = prices, volumes
                warns.append(Warning(
                    code="mad_filter_fallback",
                    message="All swaps flagged by MAD filter; using unfiltered data.",
                ))
            if len(prices_clean) < cfg.thresholds.min_swaps_for_stat_score:
                warns.append(Warning(
                    code="low_swap_count",
                    message=(
                        f"Only {len(prices_clean)} clean swap(s) in window "
                        f"(recommended min: {cfg.thresholds.min_swaps_for_stat_score})."
                    ),
                    severity="info",
                ))

            price_vwmp = _compute_vwmp(prices_clean, volumes_clean)
            if price_vwmp is None:
                continue

            rel = str(best_path.relative_to(cfg.paths.datasets_path))
            return PriceResult(
                price_usd=price_vwmp,
                timestamp_observed=timestamp,
                branch_level=branch_level,
                branch_label=branch_label,
                data_status="observed",
                files_used=[rel],
                calculation_path=[
                    f"VWMP({len(prices_clean)} swaps, window ±{half:.0f}s)",
                    "Direct stablecoin VWMP",
                ],
                detected_columns={best_path.name: schema.raw_columns},
                source_row=best_viability_row,
                source_schema=schema,
                warnings=warns,
                granularity=granularity,
                n_raw=n_raw,
                swap_count=len(prices_clean),
                window_seconds=float(window_s),
                excluded_swaps=excluded,
            )

    return None


def _try_cross_rate_pools_windowed(
    asset: str,
    timestamp: datetime,
    cfg: AppConfig,
    granularity: str,
    token_paths: list[Path],
    branch_level: str,
    branch_label: str,
) -> PriceResult | None:
    """VWMP on TOKEN/ETH leg × ETH/USD point read.

    Window expansion R1 applies to the TOKEN/ETH leg only.
    The ETH/USD reference is always a point read.
    Used by levels 0b (Uniswap V3), 1 (Uniswap V2), and 2 (SushiSwap).
    """
    eth_price, eth_ts, eth_file, eth_row, eth_schema = _get_eth_usd_at(timestamp, cfg, asset)
    if eth_price is None:
        return None

    for path in token_paths:
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_token_eth")
        if not ts_col or not price_col:
            continue

        cols_check: list[str] = [ts_col, price_col]
        for c in ("tvl_usd", "volume_usd", "volume_token", "slippage"):
            mapped = schema.mapping.get(c)
            if mapped:
                cols_check.append(mapped)

        viability_row = duckdb_client.latest_at_or_before(path, timestamp, cols_check, ts_col)
        if viability_row is None:
            continue

        zombie, z_warns = _is_zombie(schema, viability_row, timestamp, cfg)
        if zombie:
            continue

        if (
            eth_ts is not None
            and viability_row.get(ts_col) is not None
            and hasattr(viability_row[ts_col], "timestamp")
        ):
            lag = abs((viability_row[ts_col] - eth_ts).total_seconds())
            if lag > cfg.thresholds.cross_rate_max_lag_seconds:
                continue

        vol_col = schema.mapping.get("volume_usd") or schema.mapping.get("volume_token")
        cols = [ts_col, price_col]
        if vol_col:
            cols.append(vol_col)

        for window_s in _WINDOW_STEPS[granularity]:
            half = window_s / 2.0
            w_start = timestamp - timedelta(seconds=half)
            w_end = timestamp + timedelta(seconds=half)

            rows = duckdb_client.range_query(path, w_start, w_end, cols, cfg.api.max_limit, ts_col)
            if not rows:
                continue

            n_raw = len(rows)
            prices_eth = [float(r[price_col]) for r in rows if r.get(price_col) is not None]
            volumes = (
                [float(r[vol_col]) if r.get(vol_col) is not None else 1.0
                 for r in rows if r.get(price_col) is not None]
                if vol_col else [1.0] * len(prices_eth)
            )
            if not prices_eth:
                continue

            prices_clean, volumes_clean, excluded = _filter_mad_outliers(
                prices_eth, volumes, cfg.thresholds.sigma_mad
            )
            warns: list[Warning] = list(z_warns)

            if excluded > 0:
                warns.append(Warning(
                    code="mad_outliers_excluded",
                    message=f"{excluded} swap(s) excluded by MAD filter.",
                ))
            if not prices_clean:
                prices_clean, volumes_clean = prices_eth, volumes
                warns.append(Warning(
                    code="mad_filter_fallback",
                    message="All swaps flagged by MAD filter; using unfiltered data.",
                ))
            if len(prices_clean) < cfg.thresholds.min_swaps_for_stat_score:
                warns.append(Warning(
                    code="low_swap_count",
                    message=f"Only {len(prices_clean)} clean swap(s) in window.",
                    severity="info",
                ))

            token_eth_vwmp = _compute_vwmp(prices_clean, volumes_clean)
            if token_eth_vwmp is None:
                continue

            price_usd = token_eth_vwmp * eth_price
            eth_lag = (
                abs((timestamp - eth_ts).total_seconds()) if eth_ts is not None else None
            )

            rel_token = str(path.relative_to(cfg.paths.datasets_path))
            files = [rel_token]
            if eth_file:
                files.append(eth_file)

            return PriceResult(
                price_usd=price_usd,
                timestamp_observed=timestamp,
                branch_level=branch_level,
                branch_label=branch_label,
                data_status="observed",
                files_used=files,
                calculation_path=[
                    f"{asset}/ETH VWMP({len(prices_clean)} swaps, ±{half:.0f}s): {rel_token}",
                    f"ETH/USD point read: {eth_file}",
                    f"{asset}/USD = {asset}/ETH VWMP × ETH/USD",
                ],
                token_leg_timestamp=timestamp,
                eth_usd_leg_timestamp=eth_ts,
                cross_rate_lag_seconds=eth_lag,
                detected_columns={path.name: schema.raw_columns},
                source_row=viability_row,
                source_schema=schema,
                eth_source_row=eth_row,
                eth_source_schema=eth_schema,
                warnings=warns,
                granularity=granularity,
                n_raw=n_raw,
                swap_count=len(prices_clean),
                window_seconds=float(window_s),
                excluded_swaps=excluded,
            )

    return None


# ---------------------------------------------------------------------------
# Level entry points — raw (point read)
# ---------------------------------------------------------------------------

def _try_level_0a(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    return _try_direct_stable_pools(
        asset, timestamp, cfg,
        registry.get_level_0a_paths(asset), "0a", "direct_stable",
    )


def _try_level_0b(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    return _try_cross_rate_pools(
        asset, timestamp, cfg,
        registry.get_level_0b_token_paths(asset), "0b", "cross_rate",
    )


def _try_level_1(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    """Level 1 — Uniswap V2 fallback on the same pair.

    ETH: direct stable (WETH/USDC or WETH/USDT V2).
    Non-ETH: cross-rate TOKEN/WETH V2 × ETH/USD.
    """
    if asset == "ETH":
        result = _try_direct_stable_pools(
            asset, timestamp, cfg,
            registry.get_level_1_direct_paths(asset), "1", "alternative_pool",
        )
        if result:
            return result
    xrate_paths = registry.get_level_1_cross_rate_token_paths(asset)
    if xrate_paths:
        return _try_cross_rate_pools(
            asset, timestamp, cfg,
            xrate_paths, "1", "alternative_pool",
        )
    return None


def _try_level_2_eth_curve(timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    """ETH price from Curve crvUSD/WETH pool.

    The file stores price_weth_per_crvusd (WETH per crvUSD ≈ 1/ETH_price).
    ETH/USD = 1 / price_weth_per_crvusd.
    """
    paths = registry.REGISTRY.get("ETH", {}).get("level_2_amm", [])
    for rel in paths:
        path = registry.resolve_path(rel)
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        inv_col = schema.mapping.get("price_inverse_eth")
        if not ts_col or not inv_col:
            continue

        cols = [ts_col, inv_col]
        for c in ["tvl_usd", "slippage"]:
            mapped = schema.mapping.get(c)
            if mapped:
                cols.append(mapped)

        row = duckdb_client.latest_at_or_before(path, timestamp, cols, ts_col)
        if row is None or row.get(inv_col) is None or float(row[inv_col]) == 0:
            continue

        eth_usd = 1.0 / float(row[inv_col])
        zombie, z_warns = _is_zombie(schema, row, timestamp, cfg)
        if zombie:
            continue

        return PriceResult(
            price_usd=eth_usd,
            timestamp_observed=row[ts_col],
            branch_level="2",
            branch_label="alternative_amm",
            data_status="observed",
            files_used=[rel],
            calculation_path=[
                f"Curve crvUSD/WETH: price_weth_per_crvusd = {float(row[inv_col]):.8f}",
                "ETH/USD = 1 / price_weth_per_crvusd",
            ],
            detected_columns={path.name: schema.raw_columns},
            source_row=row,
            source_schema=schema,
            warnings=z_warns,
        )
    return None


def _try_level_2_sushiswap(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    """Level 2 for non-ETH assets — SushiSwap TOKEN/ETH cross-rate."""
    return _try_cross_rate_pools(
        asset, timestamp, cfg,
        registry.get_level_2_amm_token_paths(asset), "2", "alternative_amm",
    )


# ---------------------------------------------------------------------------
# Level entry points — windowed (VWMP)
# ---------------------------------------------------------------------------

def _try_level_0a_windowed(
    asset: str, timestamp: datetime, cfg: AppConfig, granularity: str,
) -> PriceResult | None:
    return _try_direct_stable_pools_windowed(
        asset, timestamp, cfg, granularity,
        registry.get_level_0a_paths(asset), "0a", "direct_stable",
    )


def _try_level_0b_windowed(
    asset: str, timestamp: datetime, cfg: AppConfig, granularity: str,
) -> PriceResult | None:
    return _try_cross_rate_pools_windowed(
        asset, timestamp, cfg, granularity,
        registry.get_level_0b_token_paths(asset), "0b", "cross_rate",
    )


def _try_level_1_windowed(
    asset: str, timestamp: datetime, cfg: AppConfig, granularity: str,
) -> PriceResult | None:
    """Level 1 windowed — Uniswap V2 VWMP fallback on the same pair."""
    if asset == "ETH":
        result = _try_direct_stable_pools_windowed(
            asset, timestamp, cfg, granularity,
            registry.get_level_1_direct_paths(asset), "1", "alternative_pool",
        )
        if result:
            return result
    xrate_paths = registry.get_level_1_cross_rate_token_paths(asset)
    if xrate_paths:
        return _try_cross_rate_pools_windowed(
            asset, timestamp, cfg, granularity,
            xrate_paths, "1", "alternative_pool",
        )
    return None


def _try_level_2_eth_curve_windowed(
    timestamp: datetime, cfg: AppConfig, granularity: str,
) -> PriceResult | None:
    """Level 2 ETH — Curve crvUSD/WETH pool with VWMP.

    ETH/USD = 1 / VWMP(price_weth_per_crvusd swaps in window).
    """
    paths = registry.REGISTRY.get("ETH", {}).get("level_2_amm", [])
    for rel in paths:
        path = registry.resolve_path(rel)
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        inv_col = schema.mapping.get("price_inverse_eth")
        if not ts_col or not inv_col:
            continue

        cols_check = [ts_col, inv_col]
        for c in ("tvl_usd", "slippage"):
            mapped = schema.mapping.get(c)
            if mapped:
                cols_check.append(mapped)

        viability_row = duckdb_client.latest_at_or_before(path, timestamp, cols_check, ts_col)
        if viability_row is None:
            continue
        zombie, z_warns = _is_zombie(schema, viability_row, timestamp, cfg)
        if zombie:
            continue

        vol_col = schema.mapping.get("volume_usd") or schema.mapping.get("volume_token")
        cols = [ts_col, inv_col]
        if vol_col:
            cols.append(vol_col)

        for window_s in _WINDOW_STEPS[granularity]:
            half = window_s / 2.0
            w_start = timestamp - timedelta(seconds=half)
            w_end = timestamp + timedelta(seconds=half)

            rows = duckdb_client.range_query(path, w_start, w_end, cols, cfg.api.max_limit, ts_col)
            n_raw = len(rows)
            valid = [r for r in rows if r.get(inv_col) is not None and float(r[inv_col]) != 0]
            if not valid:
                continue

            prices = [1.0 / float(r[inv_col]) for r in valid]
            volumes = (
                [float(r[vol_col]) if r.get(vol_col) is not None else 1.0 for r in valid]
                if vol_col else [1.0] * len(prices)
            )

            prices_clean, volumes_clean, excluded = _filter_mad_outliers(
                prices, volumes, cfg.thresholds.sigma_mad
            )
            warns: list[Warning] = list(z_warns)
            if excluded > 0:
                warns.append(Warning(
                    code="mad_outliers_excluded",
                    message=f"{excluded} swap(s) excluded by MAD filter.",
                ))
            if not prices_clean:
                prices_clean, volumes_clean = prices, volumes
                warns.append(Warning(
                    code="mad_filter_fallback",
                    message="All swaps flagged by MAD filter; using unfiltered data.",
                ))

            price_vwmp = _compute_vwmp(prices_clean, volumes_clean)
            if price_vwmp is None:
                continue

            return PriceResult(
                price_usd=price_vwmp,
                timestamp_observed=timestamp,
                branch_level="2",
                branch_label="alternative_amm",
                data_status="observed",
                files_used=[rel],
                calculation_path=[
                    f"Curve VWMP({len(prices_clean)} swaps, ±{half:.0f}s)",
                    "ETH/USD = 1 / VWMP(price_weth_per_crvusd)",
                ],
                detected_columns={path.name: schema.raw_columns},
                source_row=viability_row,
                source_schema=schema,
                warnings=warns,
                granularity=granularity,
                n_raw=n_raw,
                swap_count=len(prices_clean),
                window_seconds=float(window_s),
                excluded_swaps=excluded,
            )

    return None


def _try_level_2_sushiswap_windowed(
    asset: str, timestamp: datetime, cfg: AppConfig, granularity: str,
) -> PriceResult | None:
    """Level 2 windowed for non-ETH assets — SushiSwap TOKEN/ETH VWMP cross-rate."""
    return _try_cross_rate_pools_windowed(
        asset, timestamp, cfg, granularity,
        registry.get_level_2_amm_token_paths(asset), "2", "alternative_amm",
    )


# ---------------------------------------------------------------------------
# Level 3 — Chainlink fallback
# ---------------------------------------------------------------------------

def _try_level_3(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    for path in registry.get_chainlink_paths(asset):
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_usd")
        if not ts_col or not price_col:
            continue

        row = duckdb_client.latest_at_or_before(path, timestamp, [ts_col, price_col], ts_col)
        if row is None or row.get(price_col) is None:
            continue

        rel = str(path.relative_to(cfg.paths.datasets_path))
        return PriceResult(
            price_usd=float(row[price_col]),
            timestamp_observed=row[ts_col],
            branch_level="3",
            branch_label="chainlink_fallback",
            data_status="oracle_fallback",
            files_used=[rel],
            calculation_path=["Chainlink oracle — latest observation at or before T"],
            detected_columns={path.name: schema.raw_columns},
            source_row=row,
            source_schema=schema,
        )
    return None


# ---------------------------------------------------------------------------
# Level 4 — explicit NULL
# ---------------------------------------------------------------------------

def _level_4(reason: str, granularity: str = "raw") -> PriceResult:
    return PriceResult(
        price_usd=None,
        timestamp_observed=None,
        branch_level="4",
        branch_label="unavailable",
        data_status="unavailable",
        unavailable_reason=reason,
        granularity=granularity,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def get_price_at(
    asset: str,
    timestamp: datetime,
    cfg: AppConfig,
    branch: str = "auto",
    source: str = "auto",
    granularity: str = "raw",
) -> PriceResult:
    """Run the source hierarchy and return the best available price.

    Hierarchy: 0a → 0b → 1 → 2 → 3 → 4

    granularity="raw"    — point read (latest observation at or before T).
    granularity="minute" — VWMP over T ± 30 s, expanding per R1 up to ± 7.5 min.
    granularity="hour"   — VWMP over T ± 30 min, expanding per R1 up to ± 4 h.
    granularity="day"    — VWMP over T ± 12 h (no expansion; falls to next level).

    Level 2 is asset-specific:
      ETH    → Curve crvUSD/WETH (inverted price)
      others → SushiSwap TOKEN/ETH cross-rate
    """
    if granularity == "raw":
        if branch in ("auto", "0a") and source in ("auto", "dex"):
            result = _try_level_0a(asset, timestamp, cfg)
            if result:
                return result
            if branch == "0a":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "0b") and source in ("auto", "dex"):
            result = _try_level_0b(asset, timestamp, cfg)
            if result:
                return result
            if branch == "0b":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "1") and source in ("auto", "dex"):
            result = _try_level_1(asset, timestamp, cfg)
            if result:
                return result
            if branch == "1":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "2") and source in ("auto", "dex"):
            if asset == "ETH":
                result = _try_level_2_eth_curve(timestamp, cfg)
            else:
                result = _try_level_2_sushiswap(asset, timestamp, cfg)
            if result:
                return result
            if branch == "2":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "3") or source == "chainlink":
            result = _try_level_3(asset, timestamp, cfg)
            if result:
                return result
            if branch == "3":
                return _level_4("missing_source", granularity)

        return _level_4("missing_source", granularity)

    else:
        # Windowed VWMP path (minute / hour / day)
        if branch in ("auto", "0a") and source in ("auto", "dex"):
            result = _try_level_0a_windowed(asset, timestamp, cfg, granularity)
            if result:
                return result
            if branch == "0a":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "0b") and source in ("auto", "dex"):
            result = _try_level_0b_windowed(asset, timestamp, cfg, granularity)
            if result:
                return result
            if branch == "0b":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "1") and source in ("auto", "dex"):
            result = _try_level_1_windowed(asset, timestamp, cfg, granularity)
            if result:
                return result
            if branch == "1":
                return _level_4("no_observation_in_window", granularity)

        if branch in ("auto", "2") and source in ("auto", "dex"):
            if asset == "ETH":
                result = _try_level_2_eth_curve_windowed(timestamp, cfg, granularity)
            else:
                result = _try_level_2_sushiswap_windowed(asset, timestamp, cfg, granularity)
            if result:
                return result
            if branch == "2":
                return _level_4("no_observation_in_window", granularity)

        # Level 3 (Chainlink) stays as a point read regardless of granularity:
        # Chainlink rounds are not individual swaps and cannot be VWMP-aggregated.
        if branch in ("auto", "3") or source == "chainlink":
            result = _try_level_3(asset, timestamp, cfg)
            if result:
                return result
            if branch == "3":
                return _level_4("missing_source", granularity)

        return _level_4("missing_source", granularity)
