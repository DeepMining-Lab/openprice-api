"""Price service — implements the source hierarchy 0a → 0b → 3 → 4."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app import csv_adapter, duckdb_client, registry
from app.config import AppConfig
from app.schemas import Warning


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

    if vol_col:
        vol = row.get(vol_col)
        try:
            if vol is not None and float(vol) < cfg.thresholds.seuil_vol_min_usd_24h:
                is_zombie = True
        except (ValueError, TypeError):
            warnings.append(Warning(code="volume_parse_error",
                                    message=f"Volume value could not be parsed as a number (got {vol!r}); viability check skipped."))
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
# Level 0a — direct stablecoin
# ---------------------------------------------------------------------------

def _try_level_0a(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    candidates = registry.get_level_0a_paths(asset)
    best: PriceResult | None = None
    best_tvl: float = -1.0

    for path in candidates:
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
            obs_ts = row[ts_col]
            best = PriceResult(
                price_usd=float(row[price_col]),
                timestamp_observed=obs_ts,
                branch_level="0a",
                branch_label="direct_stable",
                data_status="observed",
                files_used=[str(path.relative_to(cfg.paths.datasets_path))],
                calculation_path=["Direct stablecoin price from pool"],
                detected_columns={path.name: schema.raw_columns},
                source_row=row,
                source_schema=schema,
                warnings=z_warns,
            )

    return best


# ---------------------------------------------------------------------------
# Level 0b — cross-rate TOKEN/ETH × ETH/USD
# ---------------------------------------------------------------------------

def _get_eth_usd_at(
    timestamp: datetime, cfg: AppConfig, asset: str = "ETH"
) -> tuple[float | None, datetime | None, str | None]:
    """Return (eth_price, obs_timestamp, relative_path) from best ETH/USD reference."""
    for path in registry.get_eth_usd_reference_paths(asset):
        if not path.exists():
            continue
        schema = csv_adapter.inspect(path)
        ts_col = schema.mapping.get("timestamp")
        price_col = schema.mapping.get("price_usd")
        if not ts_col or not price_col:
            continue
        row = duckdb_client.latest_at_or_before(path, timestamp, [ts_col, price_col], ts_col)
        if row and row.get(price_col) is not None:
            rel = str(path.relative_to(cfg.paths.datasets_path))
            return float(row[price_col]), row[ts_col], rel
    return None, None, None


def _try_level_0b(asset: str, timestamp: datetime, cfg: AppConfig) -> PriceResult | None:
    token_paths = registry.get_level_0b_token_paths(asset)
    eth_price, eth_ts, eth_file = _get_eth_usd_at(timestamp, cfg, asset)
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
            branch_level="0b",
            branch_label="cross_rate",
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
            warnings=z_warns,
        )
    return None


# ---------------------------------------------------------------------------
# Level 2 — Alternative AMMs (SushiSwap, Curve)
# ---------------------------------------------------------------------------

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
        inv_col = schema.mapping.get("price_inverse_eth")  # price_weth_per_crvusd
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

def _level_4(reason: str) -> PriceResult:
    return PriceResult(
        price_usd=None,
        timestamp_observed=None,
        branch_level="4",
        branch_label="unavailable",
        data_status="unavailable",
        unavailable_reason=reason,
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
) -> PriceResult:
    """Run the source hierarchy and return the best available price."""

    if branch in ("auto", "0a") and source in ("auto", "dex"):
        result = _try_level_0a(asset, timestamp, cfg)
        if result:
            return result
        if branch == "0a":
            return _level_4("no_observation_in_window")

    if branch in ("auto", "0b") and source in ("auto", "dex"):
        result = _try_level_0b(asset, timestamp, cfg)
        if result:
            return result
        if branch == "0b":
            return _level_4("no_observation_in_window")

    if branch in ("auto", "2") and source in ("auto", "dex") and asset == "ETH":
        result = _try_level_2_eth_curve(timestamp, cfg)
        if result:
            return result
        if branch == "2":
            return _level_4("no_observation_in_window")

    if branch in ("auto", "3") or source == "chainlink":
        result = _try_level_3(asset, timestamp, cfg)
        if result:
            return result
        if branch == "3":
            return _level_4("missing_source")

    return _level_4("missing_source")
