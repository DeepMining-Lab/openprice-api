"""Source hierarchy 0a → 0b → 1 → 2 → 3 → 4 on the Parquet store (API V3).

A faithful port of ``app.services.price_service`` (V1/V2, untouched): same formulas,
same branch order, same warnings and provenance. Only the data access changed:

* reads go through :class:`app.v3.store.Store` (indexed Parquet lookups) instead of
  scanning CSV files with a fresh DuckDB connection per query;
* the "recently active" check is an O(1) comparison on the as-of row (a row exists in
  ``[T - N days, T]`` iff the latest row at or before T is at or after ``T - N days``);
* the ETH/USD reference leg is resolved once per request and shared by the levels;
* the 10 000-row truncation of the windowed VWMP read (``api.max_limit`` used as a SQL
  LIMIT, V1/V2 behaviour) is removed. ``v3.legacy_truncation: true`` restores it so
  the engine can be proven identical to V2.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app import registry
from app.config import AppConfig
from app.schemas import Warning
from app.services.price_service import (
    PriceResult,
    _WINDOW_BOUND_POLICY,
    _WINDOW_STEPS,
    _coerce_dt,
    _compute_vwmp,
    _filter_mad_outliers,
    _level_4,
    _windowed_status,
)
from app.v3.store import DatasetInfo, Store

_BLOCK_TS_CANDIDATES = ("block_timestamp_utc", "block_timestamp", "block_time")
_ROW_CANON = (
    "price_usd", "price_token_eth", "price_inverse_eth",
    "volume_usd", "volume_token", "tvl_usd", "slippage", "block_number",
)


def _row_cols(ds: DatasetInfo) -> list[str]:
    """Parquet columns fetched with an as-of row (superset of what V1/V2 selected)."""
    cols = [c for c in (ds.col(k) for k in _ROW_CANON) if c]
    if any(b in ds.raw_columns for b in _BLOCK_TS_CANDIDATES):
        cols.append("block_ts")
    return cols


class _Ctx:
    """Per-request memo (the ETH/USD leg is identical across levels for one T)."""

    def __init__(self) -> None:
        self.eth: dict[tuple[str, datetime], tuple] = {}


class Engine:
    def __init__(self, cfg: AppConfig, store: Store):
        self.cfg = cfg
        self.store = store

    # ------------------------------------------------------------------ helpers
    def _rel(self, path: Path) -> str:
        return str(path.relative_to(self.cfg.paths.datasets_path))

    def _ds(self, path: Path) -> DatasetInfo | None:
        return self.store.dataset(self._rel(path))

    def _limit(self) -> int | None:
        return self.cfg.api.max_limit if self.cfg.v3.legacy_truncation else None

    def _extract_block_ref(self, ds: DatasetInfo, row: dict[str, Any]) -> tuple[int | None, datetime | None, list[Warning]]:
        bn_col = ds.col("block_number")
        has_bts = any(c in ds.raw_columns for c in _BLOCK_TS_CANDIDATES)
        block_number: int | None = None
        if bn_col and row.get(bn_col) is not None:
            try:
                block_number = int(row[bn_col])
            except (ValueError, TypeError):
                block_number = None
        block_ts = _coerce_dt(row.get("block_ts")) if has_bts else None
        warns: list[Warning] = []
        if block_number is None:
            warns.append(Warning(
                code="block_metadata_unavailable",
                message="Source has no block_number column; reference block fields are null.",
                severity="info",
            ))
        return block_number, block_ts, warns

    # ------------------------------------------------------------- zombie check
    def _is_zombie(
        self, ds: DatasetInfo, row: dict[str, Any], timestamp: datetime, eth_usd_price: float | None = None,
    ) -> tuple[bool, list[Warning]]:
        cfg = self.cfg
        warnings: list[Warning] = []
        is_zombie = False
        schema = ds.schema
        tvl_col = schema.mapping.get("tvl_usd")
        vol_col = schema.mapping.get("volume_usd")
        ts_col = schema.mapping.get("timestamp")

        if tvl_col:
            tvl = row.get(tvl_col)
            try:
                if tvl is not None:
                    tvl_float = float(tvl)
                    if schema.tvl_unit == "eth" and eth_usd_price is not None:
                        tvl_float *= eth_usd_price
                    if tvl_float < cfg.thresholds.seuil_TVL_min_usd:
                        is_zombie = True
            except (ValueError, TypeError):
                warnings.append(Warning(code="tvl_parse_error",
                                        message=f"TVL value could not be parsed as a number (got {tvl!r}); viability check skipped."))
        else:
            warnings.append(Warning(code="missing_tvl_column",
                                    message="TVL viability check could not be evaluated for this source file."))

        if vol_col and ts_col:
            window_start = timestamp - timedelta(hours=24)
            vol_24h = self.store.sum_between(ds.rel, vol_col, window_start, timestamp)
            if vol_24h is not None and vol_24h < cfg.thresholds.seuil_vol_min_usd_24h:
                is_zombie = True
            elif vol_24h is None:
                warnings.append(Warning(code="volume_sum_empty",
                                        message="No swaps found in the 24h window; volume viability check skipped."))
        elif schema.mapping.get("volume_token"):
            warnings.append(Warning(code="volume_not_usd",
                                    message="Volume column is token-denominated (ETH/WETH/crvUSD); USD volume check skipped."))
        else:
            warnings.append(Warning(code="missing_volume_column",
                                    message="Volume viability check could not be evaluated."))
        return is_zombie, warnings

    def _recently_active(self, row: dict[str, Any], timestamp: datetime) -> bool:
        """True iff a row exists in [T - N days, T]; ``row`` is the as-of row at T (ts <= T)."""
        window_start = timestamp - timedelta(days=self.cfg.thresholds.fenetre_inactivite_jours)
        return row["ts"] >= window_start

    # -------------------------------------------------------- ETH/USD reference
    def _eth_usd_at(self, timestamp: datetime, asset: str, ctx: _Ctx):
        key = (asset, timestamp)
        if key in ctx.eth:
            return ctx.eth[key]
        out: tuple = (None, None, None, None, None)
        for path in registry.get_eth_usd_reference_paths(asset):
            ds = self._ds(path)
            if ds is None:
                continue
            ts_col, price_col = ds.col("timestamp"), ds.col("price_usd")
            if not ts_col or not price_col:
                continue
            row = self.store.as_of(ds.rel, timestamp, _row_cols(ds))
            if row and row.get(price_col) is not None:
                out = (float(row[price_col]), row[ts_col], ds.rel, row, ds.schema)
                break
        ctx.eth[key] = out
        return out

    # ------------------------------------------------------------- raw builders
    def _direct_stable_raw(self, asset, timestamp, paths, level, label) -> PriceResult | None:
        best: PriceResult | None = None
        best_tvl = -1.0
        for path in paths:
            ds = self._ds(path)
            if ds is None:
                continue
            ts_col, price_col = ds.col("timestamp"), ds.col("price_usd")
            if not ts_col or not price_col:
                continue
            row = self.store.as_of(ds.rel, timestamp, _row_cols(ds))
            if row is None or row.get(price_col) is None:
                continue
            zombie, z_warns = self._is_zombie(ds, row, timestamp)
            if zombie:
                continue
            if not self._recently_active(row, timestamp):
                continue
            tvl_col = ds.col("tvl_usd")
            try:
                tvl = float(row[tvl_col]) if tvl_col and row.get(tvl_col) else 0.0
            except (ValueError, TypeError):
                tvl = 0.0
            if tvl > best_tvl or best is None:
                best_tvl = tvl
                bn, bts, bwarns = self._extract_block_ref(ds, row)
                best = PriceResult(
                    price_usd=float(row[price_col]),
                    timestamp_observed=row[ts_col],
                    branch_level=level,
                    branch_label=label,
                    data_status="observed",
                    files_used=[ds.rel],
                    calculation_path=["Direct stablecoin price from pool"],
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=row,
                    source_schema=ds.schema,
                    warnings=z_warns + bwarns,
                    expansion_step=0,
                    reference_block_number=bn,
                    reference_block_timestamp=bts,
                )
        return best

    def _cross_rate_raw(self, asset, timestamp, token_paths, level, label, ctx) -> PriceResult | None:
        if not token_paths:
            return None  # V1/V2 looked the ETH/USD leg up and then looped over nothing
        eth_price, eth_ts, eth_file, eth_row, eth_schema = self._eth_usd_at(timestamp, asset, ctx)
        if eth_price is None:
            return None
        for path in token_paths:
            ds = self._ds(path)
            if ds is None:
                continue
            ts_col, price_col = ds.col("timestamp"), ds.col("price_token_eth")
            if not ts_col or not price_col:
                continue
            row = self.store.as_of(ds.rel, timestamp, _row_cols(ds))
            if row is None or row.get(price_col) is None:
                continue
            token_ts = row[ts_col]
            if eth_ts is not None and hasattr(token_ts, "timestamp") and hasattr(eth_ts, "timestamp"):
                lag = abs((token_ts - eth_ts).total_seconds())
            else:
                lag = 0.0
            if lag > self.cfg.thresholds.cross_rate_max_lag_seconds:
                continue
            zombie, z_warns = self._is_zombie(ds, row, timestamp, eth_usd_price=eth_price)
            if zombie:
                continue
            token_eth_price = float(row[price_col])
            price_usd = token_eth_price * eth_price
            files = [ds.rel]
            if eth_file:
                files.append(eth_file)
            bn, bts, bwarns = self._extract_block_ref(ds, row)
            return PriceResult(
                price_usd=price_usd,
                timestamp_observed=token_ts,
                branch_level=level,
                branch_label=label,
                data_status="observed",
                files_used=files,
                calculation_path=[
                    f"{asset}/ETH or WETH leg: {ds.rel}",
                    f"ETH/USD leg: {eth_file}",
                    f"{asset}/USD = {asset}/ETH × ETH/USD",
                ],
                token_leg_timestamp=token_ts,
                eth_usd_leg_timestamp=eth_ts,
                cross_rate_lag_seconds=lag,
                detected_columns={ds.csv_name: ds.raw_columns},
                source_row=row,
                source_schema=ds.schema,
                eth_source_row=eth_row,
                eth_source_schema=eth_schema,
                warnings=z_warns + bwarns,
                expansion_step=0,
                reference_block_number=bn,
                reference_block_timestamp=bts,
            )
        return None

    # -------------------------------------------------------- windowed builders
    def _window_vwmp(self, ds, price_col, vol_col, timestamp, granularity, transform=None):
        """Yield (step_idx, window_s, half, w_start, w_end, rows) for each R1 step with data."""
        cols = [price_col] + ([vol_col] if vol_col else [])
        for step_idx, window_s in enumerate(_WINDOW_STEPS[granularity]):
            half = window_s / 2.0
            w_start = timestamp - timedelta(seconds=half)
            w_end = timestamp + timedelta(seconds=half)
            rows = self.store.window(ds.rel, w_start, w_end, cols, self._limit())
            yield step_idx, window_s, half, w_start, w_end, rows

    def _clean(self, prices, volumes, warns_list, message_full: bool, sigma: float):
        """MAD filter + fallback exactly as V1/V2. Returns (prices, volumes, excluded, mad_fallback)."""
        prices_clean, volumes_clean, excluded = _filter_mad_outliers(prices, volumes, sigma)
        mad_fallback = False
        if excluded > 0:
            msg = (f"{excluded} swap(s) excluded by MAD filter (sigma_mad={sigma})." if message_full
                   else f"{excluded} swap(s) excluded by MAD filter.")
            warns_list.append(Warning(code="mad_outliers_excluded", message=msg))
        if not prices_clean:
            mad_fallback = True
            prices_clean, volumes_clean = prices, volumes
            warns_list.append(Warning(code="mad_filter_fallback",
                                      message="All swaps flagged by MAD filter; using unfiltered data."))
        return prices_clean, volumes_clean, excluded, mad_fallback

    def _direct_stable_windowed(self, asset, timestamp, granularity, paths, level, label) -> PriceResult | None:
        cfg = self.cfg
        viable: list[tuple[float, DatasetInfo, dict[str, Any], list[Warning]]] = []
        for path in paths:
            ds = self._ds(path)
            if ds is None:
                continue
            ts_col, price_col = ds.col("timestamp"), ds.col("price_usd")
            if not ts_col or not price_col:
                continue
            row = self.store.as_of(ds.rel, timestamp, _row_cols(ds))
            if row is None:
                continue
            zombie, z_warns = self._is_zombie(ds, row, timestamp)
            if zombie:
                continue
            if not self._recently_active(row, timestamp):
                continue
            tvl_col = ds.col("tvl_usd")
            try:
                tvl = float(row[tvl_col]) if tvl_col and row.get(tvl_col) else 0.0
            except (ValueError, TypeError):
                tvl = 0.0
            viable.append((tvl, ds, row, z_warns))
        viable.sort(key=lambda x: x[0], reverse=True)

        for tvl, ds, viability_row, pool_warnings in viable:
            price_col, vol_col = ds.col("price_usd"), ds.col("volume_usd")
            for step_idx, window_s, half, w_start, w_end, rows in self._window_vwmp(ds, price_col, vol_col, timestamp, granularity):
                if not rows:
                    continue
                n_raw = len(rows)
                prices = [float(r[price_col]) for r in rows if r.get(price_col) is not None]
                volumes = (
                    [float(r[vol_col]) if r.get(vol_col) is not None else 1.0 for r in rows if r.get(price_col) is not None]
                    if vol_col else [1.0] * len(prices)
                )
                if not prices:
                    continue
                warns: list[Warning] = list(pool_warnings)
                pc, vc, excluded, mad_fallback = self._clean(prices, volumes, warns, True, cfg.thresholds.sigma_mad)
                if len(pc) < cfg.thresholds.min_swaps_for_stat_score:
                    warns.append(Warning(
                        code="low_swap_count",
                        message=(f"Only {len(pc)} clean swap(s) in window "
                                 f"(recommended min: {cfg.thresholds.min_swaps_for_stat_score})."),
                        severity="info",
                    ))
                price_vwmp = _compute_vwmp(pc, vc)
                if price_vwmp is None:
                    continue
                warns.append(Warning(code="block_metadata_aggregated",
                                     message="VWMP aggregates multiple swaps/blocks; no single reference block.",
                                     severity="info"))
                return PriceResult(
                    price_usd=price_vwmp,
                    timestamp_observed=timestamp,
                    branch_level=level,
                    branch_label=label,
                    data_status=_windowed_status(step_idx, mad_fallback),
                    files_used=[ds.rel],
                    calculation_path=[f"VWMP({len(pc)} swaps, window ±{half:.0f}s)", "Direct stablecoin VWMP"],
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=viability_row,
                    source_schema=ds.schema,
                    warnings=warns,
                    granularity=granularity,
                    n_raw=n_raw,
                    swap_count=len(pc),
                    window_seconds=float(window_s),
                    excluded_swaps=excluded,
                    initial_window_seconds=float(_WINDOW_STEPS[granularity][0]),
                    window_start_utc=w_start,
                    window_end_utc=w_end,
                    window_bound_policy=_WINDOW_BOUND_POLICY,
                    expansion_step=step_idx,
                )
        return None

    def _cross_rate_windowed(self, asset, timestamp, granularity, token_paths, level, label, ctx) -> PriceResult | None:
        cfg = self.cfg
        if not token_paths:
            return None
        eth_price, eth_ts, eth_file, eth_row, eth_schema = self._eth_usd_at(timestamp, asset, ctx)
        if eth_price is None:
            return None
        for path in token_paths:
            ds = self._ds(path)
            if ds is None:
                continue
            ts_col, price_col = ds.col("timestamp"), ds.col("price_token_eth")
            if not ts_col or not price_col:
                continue
            viability_row = self.store.as_of(ds.rel, timestamp, _row_cols(ds))
            if viability_row is None:
                continue
            zombie, z_warns = self._is_zombie(ds, viability_row, timestamp, eth_usd_price=eth_price)
            if zombie:
                continue
            if (eth_ts is not None and viability_row.get(ts_col) is not None
                    and hasattr(viability_row[ts_col], "timestamp")):
                lag = abs((viability_row[ts_col] - eth_ts).total_seconds())
                if lag > cfg.thresholds.cross_rate_max_lag_seconds:
                    continue
            vol_col = ds.col("volume_usd") or ds.col("volume_token")
            for step_idx, window_s, half, w_start, w_end, rows in self._window_vwmp(ds, price_col, vol_col, timestamp, granularity):
                if not rows:
                    continue
                n_raw = len(rows)
                prices_eth = [float(r[price_col]) for r in rows if r.get(price_col) is not None]
                volumes = (
                    [float(r[vol_col]) if r.get(vol_col) is not None else 1.0 for r in rows if r.get(price_col) is not None]
                    if vol_col else [1.0] * len(prices_eth)
                )
                if not prices_eth:
                    continue
                warns: list[Warning] = list(z_warns)
                pc, vc, excluded, mad_fallback = self._clean(prices_eth, volumes, warns, False, cfg.thresholds.sigma_mad)
                if len(pc) < cfg.thresholds.min_swaps_for_stat_score:
                    warns.append(Warning(code="low_swap_count",
                                         message=f"Only {len(pc)} clean swap(s) in window.", severity="info"))
                token_eth_vwmp = _compute_vwmp(pc, vc)
                if token_eth_vwmp is None:
                    continue
                price_usd = token_eth_vwmp * eth_price
                eth_lag = abs((timestamp - eth_ts).total_seconds()) if eth_ts is not None else None
                files = [ds.rel]
                if eth_file:
                    files.append(eth_file)
                warns.append(Warning(code="block_metadata_aggregated",
                                     message="VWMP aggregates multiple swaps/blocks; no single reference block.",
                                     severity="info"))
                return PriceResult(
                    price_usd=price_usd,
                    timestamp_observed=timestamp,
                    branch_level=level,
                    branch_label=label,
                    data_status=_windowed_status(step_idx, mad_fallback),
                    files_used=files,
                    calculation_path=[
                        f"{asset}/ETH VWMP({len(pc)} swaps, ±{half:.0f}s): {ds.rel}",
                        f"ETH/USD point read: {eth_file}",
                        f"{asset}/USD = {asset}/ETH VWMP × ETH/USD",
                    ],
                    token_leg_timestamp=timestamp,
                    eth_usd_leg_timestamp=eth_ts,
                    cross_rate_lag_seconds=eth_lag,
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=viability_row,
                    source_schema=ds.schema,
                    eth_source_row=eth_row,
                    eth_source_schema=eth_schema,
                    warnings=warns,
                    granularity=granularity,
                    n_raw=n_raw,
                    swap_count=len(pc),
                    window_seconds=float(window_s),
                    excluded_swaps=excluded,
                    initial_window_seconds=float(_WINDOW_STEPS[granularity][0]),
                    window_start_utc=w_start,
                    window_end_utc=w_end,
                    window_bound_policy=_WINDOW_BOUND_POLICY,
                    expansion_step=step_idx,
                )
        return None

    # -------------------------------------------------------------- level 2 ETH
    def _curve_raw(self, timestamp) -> PriceResult | None:
        for rel in registry.REGISTRY.get("ETH", {}).get("level_2_amm", []):
            ds = self.store.dataset(rel)
            if ds is None:
                continue
            ts_col, inv_col = ds.col("timestamp"), ds.col("price_inverse_eth")
            if not ts_col or not inv_col:
                continue
            row = self.store.as_of(rel, timestamp, _row_cols(ds))
            if row is None or row.get(inv_col) is None or float(row[inv_col]) == 0:
                continue
            eth_usd = 1.0 / float(row[inv_col])
            zombie, z_warns = self._is_zombie(ds, row, timestamp, eth_usd_price=eth_usd)
            if zombie:
                continue
            bn, bts, bwarns = self._extract_block_ref(ds, row)
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
                detected_columns={ds.csv_name: ds.raw_columns},
                source_row=row,
                source_schema=ds.schema,
                warnings=z_warns + bwarns,
                expansion_step=0,
                reference_block_number=bn,
                reference_block_timestamp=bts,
            )
        return None

    def _curve_windowed(self, timestamp, granularity) -> PriceResult | None:
        cfg = self.cfg
        for rel in registry.REGISTRY.get("ETH", {}).get("level_2_amm", []):
            ds = self.store.dataset(rel)
            if ds is None:
                continue
            ts_col, inv_col = ds.col("timestamp"), ds.col("price_inverse_eth")
            if not ts_col or not inv_col:
                continue
            viability_row = self.store.as_of(rel, timestamp, _row_cols(ds))
            if viability_row is None:
                continue
            eth_usd_viability = (
                1.0 / float(viability_row[inv_col])
                if viability_row.get(inv_col) and float(viability_row[inv_col]) != 0 else None
            )
            zombie, z_warns = self._is_zombie(ds, viability_row, timestamp, eth_usd_price=eth_usd_viability)
            if zombie:
                continue
            vol_col = ds.col("volume_usd") or ds.col("volume_token")
            for step_idx, window_s, half, w_start, w_end, rows in self._window_vwmp(ds, inv_col, vol_col, timestamp, granularity):
                n_raw = len(rows)
                valid = [r for r in rows if r.get(inv_col) is not None and float(r[inv_col]) != 0]
                if not valid:
                    continue
                prices = [1.0 / float(r[inv_col]) for r in valid]
                volumes = (
                    [float(r[vol_col]) if r.get(vol_col) is not None else 1.0 for r in valid]
                    if vol_col else [1.0] * len(prices)
                )
                warns: list[Warning] = list(z_warns)
                pc, vc, excluded, mad_fallback = self._clean(prices, volumes, warns, False, cfg.thresholds.sigma_mad)
                price_vwmp = _compute_vwmp(pc, vc)
                if price_vwmp is None:
                    continue
                warns.append(Warning(code="block_metadata_aggregated",
                                     message="VWMP aggregates multiple swaps/blocks; no single reference block.",
                                     severity="info"))
                return PriceResult(
                    price_usd=price_vwmp,
                    timestamp_observed=timestamp,
                    branch_level="2",
                    branch_label="alternative_amm",
                    data_status=_windowed_status(step_idx, mad_fallback),
                    files_used=[rel],
                    calculation_path=[
                        f"Curve VWMP({len(pc)} swaps, ±{half:.0f}s)",
                        "ETH/USD = 1 / VWMP(price_weth_per_crvusd)",
                    ],
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=viability_row,
                    source_schema=ds.schema,
                    warnings=warns,
                    granularity=granularity,
                    n_raw=n_raw,
                    swap_count=len(pc),
                    window_seconds=float(window_s),
                    excluded_swaps=excluded,
                    initial_window_seconds=float(_WINDOW_STEPS[granularity][0]),
                    window_start_utc=w_start,
                    window_end_utc=w_end,
                    window_bound_policy=_WINDOW_BOUND_POLICY,
                    expansion_step=step_idx,
                )
        return None

    # ----------------------------------------------------------------- level 3
    def _chainlink(self, asset, timestamp) -> PriceResult | None:
        for path in registry.get_chainlink_paths(asset):
            ds = self._ds(path)
            if ds is None:
                continue
            ts_col, price_col = ds.col("timestamp"), ds.col("price_usd")
            if not ts_col or not price_col:
                continue
            row = self.store.as_of(ds.rel, timestamp, _row_cols(ds))
            if row is None or row.get(price_col) is None:
                continue
            bn, bts, bwarns = self._extract_block_ref(ds, row)
            return PriceResult(
                price_usd=float(row[price_col]),
                timestamp_observed=row[ts_col],
                branch_level="3",
                branch_label="chainlink_fallback",
                data_status="oracle_fallback",
                files_used=[ds.rel],
                calculation_path=["Chainlink oracle — latest observation at or before T"],
                detected_columns={ds.csv_name: ds.raw_columns},
                source_row=row,
                source_schema=ds.schema,
                warnings=bwarns,
                expansion_step=0,
                reference_block_number=bn,
                reference_block_timestamp=bts,
            )
        return None

    # ------------------------------------------------------------- entry point
    def get_price_at(
        self, asset: str, timestamp: datetime, branch: str = "auto", source: str = "auto", granularity: str = "raw",
    ) -> PriceResult:
        """Same hierarchy and short-circuit rules as ``price_service.get_price_at``."""
        ctx = _Ctx()
        dex = source in ("auto", "dex")
        raw = granularity == "raw"

        def level(name: str, fn):
            if branch in ("auto", name) and dex:
                res = fn()
                if res:
                    return res
                if branch == name:
                    return _level_4("no_observation_in_window", granularity)
            return None

        p0a = registry.get_level_0a_paths(asset)
        p0b = registry.get_level_0b_token_paths(asset)
        p1x = registry.get_level_1_cross_rate_token_paths(asset)

        if raw:
            steps = [
                ("0a", lambda: self._direct_stable_raw(asset, timestamp, p0a, "0a", "direct_stable")),
                ("0b", lambda: self._cross_rate_raw(asset, timestamp, p0b, "0b", "cross_rate", ctx)),
                ("1", lambda: self._level1_raw(asset, timestamp, ctx)),
                ("2", lambda: self._curve_raw(timestamp) if asset == "ETH" else
                 self._cross_rate_raw(asset, timestamp, registry.get_level_2_amm_token_paths(asset), "2", "alternative_amm", ctx)),
            ]
        else:
            steps = [
                ("0a", lambda: self._direct_stable_windowed(asset, timestamp, granularity, p0a, "0a", "direct_stable")),
                ("0b", lambda: self._cross_rate_windowed(asset, timestamp, granularity, p0b, "0b", "cross_rate", ctx)),
                ("1", lambda: self._level1_windowed(asset, timestamp, granularity, p1x, ctx)),
                ("2", lambda: self._curve_windowed(timestamp, granularity) if asset == "ETH" else
                 self._cross_rate_windowed(asset, timestamp, granularity, registry.get_level_2_amm_token_paths(asset), "2", "alternative_amm", ctx)),
            ]
        for name, fn in steps:
            res = level(name, fn)
            if res is not None:
                return res

        # Level 3 (Chainlink) stays a point read whatever the granularity.
        if branch in ("auto", "3") or source == "chainlink":
            res = self._chainlink(asset, timestamp)
            if res:
                return res
            if branch == "3":
                return _level_4("missing_source", granularity)
        return _level_4("missing_source", granularity)

    def _level1_raw(self, asset, timestamp, ctx) -> PriceResult | None:
        if asset == "ETH":
            res = self._direct_stable_raw(asset, timestamp, registry.get_level_1_direct_paths(asset), "1", "alternative_pool")
            if res:
                return res
        xrate = registry.get_level_1_cross_rate_token_paths(asset)
        if xrate:
            return self._cross_rate_raw(asset, timestamp, xrate, "1", "alternative_pool", ctx)
        return None

    def _level1_windowed(self, asset, timestamp, granularity, xrate, ctx) -> PriceResult | None:
        if asset == "ETH":
            res = self._direct_stable_windowed(asset, timestamp, granularity,
                                               registry.get_level_1_direct_paths(asset), "1", "alternative_pool")
            if res:
                return res
        if xrate:
            return self._cross_rate_windowed(asset, timestamp, granularity, xrate, "1", "alternative_pool", ctx)
        return None
