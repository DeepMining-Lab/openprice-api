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
  the engine can be proven identical to V2;
* a Chainlink fallback (level 3) reads only the rounds of the aggregator phase the proxy
  served at T (``v3.chainlink_active_phase_only``; not in legacy mode);
* an hour/day cross-rate multiplies by the ETH/USD VWMP over the token leg's window, not by a single
  swap (``windowed_eth_leg``; not in legacy mode);
* four corrections of the V1/V2 hierarchy (2026-09-25; each has its ``v3`` key, none applies in
  legacy mode): ``source=dex`` never answers from Chainlink (``strict_source_filter``); an hour/day
  cross-rate bounds the lag between the ETH/USD point read and T instead of the lag between that read
  and the token pool's last swap before T (``windowed_lag_check``); a raw DEX price is never built
  from an observation older than ``raw_max_age_seconds`` before T; no swap in the 24 h before T is a
  24 h volume of 0 for the zombie rule (``no_swap_is_zero_volume``).

Data access is batched: one query per candidate gives its as-of row, the rows sharing that
timestamp and its 24 h volume (``Store.probe``), and the MAD filter + VWMP of a window run inside
DuckDB (``Store.window_vwmp``, the Python formulas of V1/V2 being the fallback). Both return exactly
what the V1/V2 code computes.

Every candidate file that the hierarchy evaluates and does not use is recorded with the
rule that rejected it (``get_price_at(..., trace=[...])``). This is bookkeeping only: the
selection logic, its order and its thresholds are exactly those of V1/V2.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app import registry
from app.config import AppConfig
from app.schemas import RejectedCandidate, Warning
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
from app.v3.store import DatasetInfo, Probe, Store, WindowStats

_BLOCK_TS_CANDIDATES = ("block_timestamp_utc", "block_timestamp", "block_time")
_ROW_CANON = (
    "price_usd", "price_token_eth", "price_inverse_eth",
    "volume_usd", "volume_token", "tvl_usd", "slippage", "block_number",
)


def _row_cols(ds: DatasetInfo) -> list[str]:
    """Parquet columns fetched with an as-of row (superset of what V1/V2 selected), plus the on-chain identity
    of the row for the provenance (log index of a swap, phase and round of a Chainlink answer) and its row number.
    The transaction hash is not read here: a text column costs a third of the lookup, and only the row that
    answers needs it (``Store.tx_hash_of``, when the provenance is built)."""
    cols = [c for c in (ds.col(k) for k in _ROW_CANON) if c]
    if any(b in ds.raw_columns for b in _BLOCK_TS_CANDIDATES):
        cols.append("block_ts")
    cols += [c for c in ("log_index", "phase", "agg_round", "rn") if c in ds.columns]
    return cols


class _Ctx:
    """Per-request memo (the ETH/USD leg is identical across levels for one T) and trace of
    the candidates rejected along the hierarchy, in evaluation order."""

    def __init__(self) -> None:
        self.eth: dict[tuple[str, datetime], tuple] = {}
        self.rejected: list[RejectedCandidate] = []

    def reject(self, level: str, file: str, rule: str, message: str, value: float | None = None,
               threshold: float | None = None, last: datetime | None = None) -> None:
        self.rejected.append(RejectedCandidate(level=level, file=file, rule=rule, message=message, value=value,
                                               threshold=threshold, last_observation_utc=last))


def _hms(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _hours(seconds: float) -> str:
    return f"{seconds / 3600:,.1f} h"


# Key of an as-of row read by Store.probe that carries the number of rows sharing its timestamp.
N_SAME_TS = "_n_same_ts"
# Key set on the ETH/USD row of an hour/day cross-rate whose ETH/USD leg is a VWMP (the row still gives its S_liq).
ETH_LEG_AGGREGATED = "_eth_leg_aggregated"
# Expected swaps in a window above which the MAD filter + VWMP run inside DuckDB (measured break-even ~1 000-2 000).
_SQL_VWMP_MIN_ROWS = 2000


def phase_note(ds: DatasetInfo, phase: int) -> str:
    """Calculation-path line of a Chainlink read restricted to the phase the proxy served at T."""
    start = ds.phases.start_of(phase) if ds.phases else None
    since = f" since {_hms(start)}" if start else ""
    return f"Rounds of aggregator phase {phase} only: the one the proxy served at T{since}"


def no_round_message(ds: DatasetInfo, phase: int | None) -> str:
    if phase is None:
        return "no Chainlink round at or before T"
    if phase == 0:
        first = ds.phases.starts[0] if ds.phases and ds.phases.starts else None
        return "the Chainlink proxy did not exist yet at T" + (f" (deployed {_hms(first)})" if first else "")
    return (f"no round of aggregator phase {phase} (the one the proxy served at T) at or before T; "
            "rounds of the other phases are not used")


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

    # ------------------------------------------------- corrections of 2026-09-25
    # Each one is off in legacy mode, which replays V1/V2 exactly.
    def strict_source(self) -> bool:
        return self.cfg.v3.strict_source_filter and not self.cfg.v3.legacy_truncation

    def _windowed_lag_check(self) -> str:
        return "last_swap" if self.cfg.v3.legacy_truncation else self.cfg.v3.windowed_lag_check

    def max_age(self) -> float | None:
        return None if self.cfg.v3.legacy_truncation else self.cfg.v3.raw_max_age_seconds

    def _no_swap_is_zero_volume(self) -> bool:
        return self.cfg.v3.no_swap_is_zero_volume and not self.cfg.v3.legacy_truncation

    def _windowed_eth_leg(self) -> str:
        return "point" if self.cfg.v3.legacy_truncation else self.cfg.v3.windowed_eth_leg

    def _eth_leg_vwmp(self, eth_file: str | None, start: datetime, end: datetime) -> WindowStats | None:
        """ETH/USD leg of an hour/day cross-rate as a VWMP over the token leg's window (``windowed_eth_leg: vwmp``):
        the MAD filter and VWMP of the ETH/USD reference file that gave the point read. None: keep the point read
        (V1/V2 rule, or no ETH/USD swap in the window)."""
        if self._windowed_eth_leg() != "vwmp" or not eth_file:
            return None
        ds = self.store.dataset(eth_file)
        price_col = ds.col("price_usd") if ds else None
        if not price_col:
            return None
        st = self._window_stats(ds, price_col, ds.col("volume_usd"), start, end)
        return st if st.n_valid and st.vwmp is not None else None

    def _stale(self, ctx: _Ctx, level: str, file: str, observed: datetime | None, timestamp: datetime,
               what: str = "last observation") -> bool:
        """Record and reject an observation older than ``raw_max_age_seconds`` before T (raw DEX reads)."""
        limit = self.max_age()
        if limit is None or observed is None:
            return False
        age = (timestamp - observed).total_seconds()
        if age <= limit:
            return False
        ctx.reject(level, file, "stale_observation", f"{what} {_hours(age)} before T (limit {_hours(limit)})",
                   age, float(limit), observed)
        return True

    # ------------------------------------------------------------ batched reads
    def _probe(self, ds: DatasetInfo, timestamp: datetime) -> Probe:
        """As-of row of a candidate plus what its zombie check needs, in one query."""
        pr = self.store.probe(ds.rel, timestamp, _row_cols(ds), ds.col("volume_usd"))
        if pr.row is not None and pr.n_same_ts is not None:
            pr.row[N_SAME_TS] = pr.n_same_ts
        return pr

    def _window_stats(self, ds: DatasetInfo, price_col: str, vol_col: str | None, start: datetime, end: datetime,
                      inverse: bool = False) -> WindowStats:
        """MAD filter + VWMP of one window. Both implementations return the same numbers; DuckDB is faster only on
        large windows (it has a fixed cost of ~8 ms), so the expected row count of the window, from the dataset's
        average density, picks one. The Python functions of V1/V2 also serve when the SQL result could depend on the
        order of the float additions (``Store.window_vwmp`` returns None)."""
        span = (ds.max_ts - ds.min_ts).total_seconds() if ds.min_ts and ds.max_ts else 0.0
        expected = ds.n_rows * (end - start).total_seconds() / span if span > 0 else 0.0
        if expected >= _SQL_VWMP_MIN_ROWS:
            sigma = self.cfg.thresholds.sigma_mad
            st = self.store.window_vwmp(ds.rel, price_col, vol_col, start, end, sigma, self._limit(), inverse)
            if st is not None:
                return st
        return self.window_stats_python(ds, price_col, vol_col, start, end, inverse)

    def window_stats_python(self, ds: DatasetInfo, price_col: str, vol_col: str | None, start: datetime,
                            end: datetime, inverse: bool = False) -> WindowStats:
        """The V1/V2 computation on the materialised swaps (reference implementation of ``Store.window_vwmp``)."""
        cols = [price_col] + ([vol_col] if vol_col else [])
        rows = self.store.window(ds.rel, start, end, cols, self._limit())
        if inverse:
            valid = [r for r in rows if r.get(price_col) is not None and float(r[price_col]) != 0]
            prices = [1.0 / float(r[price_col]) for r in valid]
        else:
            valid = [r for r in rows if r.get(price_col) is not None]
            prices = [float(r[price_col]) for r in valid]
        volumes = ([float(r[vol_col]) if r.get(vol_col) is not None else 1.0 for r in valid]
                   if vol_col else [1.0] * len(prices))
        if not prices:
            return WindowStats(len(rows), 0, 0, 0, None)
        pc, vc, excluded = _filter_mad_outliers(prices, volumes, self.cfg.thresholds.sigma_mad)
        fallback = not pc
        if fallback:
            pc, vc = prices, volumes
        return WindowStats(len(rows), len(prices), len(pc), excluded, _compute_vwmp(pc, vc), fallback)

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
        probe: Probe | None = None,
    ) -> tuple[bool, list[Warning], list[tuple[str, str, float, float]]]:
        """(is_zombie, warnings, failed rules as (rule, message, value, threshold)). ``probe`` carries the 24 h
        volume already read with the as-of row."""
        cfg = self.cfg
        warnings: list[Warning] = []
        reasons: list[tuple[str, str, float, float]] = []
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
                        unconverted = schema.tvl_unit == "eth" and eth_usd_price is None
                        unit = "ETH (no ETH/USD price to convert it)" if unconverted else "USD"
                        thr = float(cfg.thresholds.seuil_TVL_min_usd)
                        reasons.append(("zombie_tvl", f"TVL {tvl_float:,.0f} {unit} < {thr:,.0f} USD", tvl_float, thr))
            except (ValueError, TypeError):
                warnings.append(Warning(code="tvl_parse_error",
                                        message=f"TVL value could not be parsed as a number (got {tvl!r}); viability check skipped."))
        else:
            warnings.append(Warning(code="missing_tvl_column",
                                    message="TVL viability check could not be evaluated for this source file."))

        if vol_col and ts_col:
            if probe is not None:
                vol_24h, n_24h = probe.vol_24h, probe.n_24h
            else:
                window_start = timestamp - timedelta(hours=24)
                vol_24h, n_24h = self.store.sum_between(ds.rel, vol_col, window_start, timestamp), None
            thr = float(cfg.thresholds.seuil_vol_min_usd_24h)
            if vol_24h is None and n_24h == 0 and self._no_swap_is_zero_volume():
                is_zombie = True
                reasons.append(("zombie_volume_24h", f"no swap in the 24 h before T (0 USD < {thr:,.0f} USD)", 0.0, thr))
            elif vol_24h is not None and vol_24h < cfg.thresholds.seuil_vol_min_usd_24h:
                is_zombie = True
                reasons.append(("zombie_volume_24h", f"24 h volume {vol_24h:,.0f} USD < {thr:,.0f} USD", vol_24h, thr))
            elif vol_24h is None:
                warnings.append(Warning(code="volume_sum_empty",
                                        message="No swaps found in the 24h window; volume viability check skipped."))
        elif schema.mapping.get("volume_token"):
            warnings.append(Warning(code="volume_not_usd",
                                    message="Volume column is token-denominated (ETH/WETH/crvUSD); USD volume check skipped."))
        else:
            warnings.append(Warning(code="missing_volume_column",
                                    message="Volume viability check could not be evaluated."))
        return is_zombie, warnings, reasons

    def _recently_active(self, row: dict[str, Any], timestamp: datetime) -> bool:
        """True iff a row exists in [T - N days, T]; ``row`` is the as-of row at T (ts <= T)."""
        window_start = timestamp - timedelta(days=self.cfg.thresholds.fenetre_inactivite_jours)
        return row["ts"] >= window_start

    # ------------------------------------------------------------ rejection trace
    def _reject_row(self, ctx: _Ctx, level: str, ds: DatasetInfo, row: dict[str, Any], timestamp: datetime,
                    reasons: list[tuple[str, str, float, float]], inactive: bool = False) -> None:
        """Record why a candidate with an as-of row was not used (zombie rules, inactivity)."""
        last = row.get("ts")
        for rule, message, value, threshold in reasons:
            ctx.reject(level, ds.rel, rule, message, value, threshold, last)
        if inactive:
            days = self.cfg.thresholds.fenetre_inactivite_jours
            age = (timestamp - last).total_seconds()
            ctx.reject(level, ds.rel, "inactive", f"last observation {age / 86400:.1f} d before T (limit {days} d)",
                       age, days * 86400.0, last)

    def _reject_eth_leg(self, ctx: _Ctx, level: str, asset: str) -> None:
        refs = [self._rel(p) for p in registry.get_eth_usd_reference_paths(asset)]
        ctx.reject(level, refs[0] if refs else "eth_usd_reference", "eth_leg_unavailable",
                   f"no ETH/USD observation at or before T in the {len(refs)} reference file(s)")

    def _reject_unreadable(self, ctx: _Ctx, level: str, rel: str, ds: DatasetInfo | None) -> None:
        if ds is None:
            ctx.reject(level, rel, "dataset_missing", "file is not in the Parquet store")
        else:
            ctx.reject(level, rel, "missing_columns", "no timestamp or price column for this level")

    def _empty(self, ctx: _Ctx, level: str, ds: DatasetInfo) -> bool:
        """Record and skip a candidate whose CSV has a header but no data row (it can never answer)."""
        if ds.files and ds.n_rows:
            return False
        ctx.reject(level, ds.rel, "dataset_empty", "the CSV file has a header but no data row")
        return True

    def _reject_lag(self, ctx: _Ctx, level: str, ds: DatasetInfo, token_ts: datetime, eth_ts: datetime,
                    lag: float) -> None:
        limit = self.cfg.thresholds.cross_rate_max_lag_seconds
        ctx.reject(level, ds.rel, "cross_rate_lag",
                   f"token leg {_hms(token_ts)} is {lag:,.0f} s from the ETH/USD leg {_hms(eth_ts)} (limit {limit:,} s)",
                   lag, float(limit), token_ts)

    def _reject_window(self, ctx: _Ctx, level: str, ds: DatasetInfo, granularity: str,
                       viability_row: dict[str, Any]) -> None:
        half = _WINDOW_STEPS[granularity][-1] / 2.0
        ctx.reject(level, ds.rel, "no_swaps_in_window",
                   f"no usable swap within ±{half:.0f} s of T (window expansion exhausted)",
                   last=viability_row.get("ts"))

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
    def _direct_stable_raw(self, asset, timestamp, paths, level, label, ctx) -> PriceResult | None:
        best: PriceResult | None = None
        best_tvl = -1.0
        for path in paths:
            ds = self._ds(path)
            ts_col, price_col = (ds.col("timestamp"), ds.col("price_usd")) if ds else (None, None)
            if not ts_col or not price_col:
                self._reject_unreadable(ctx, level, self._rel(path), ds)
                continue
            if self._empty(ctx, level, ds):
                continue
            pr = self._probe(ds, timestamp)
            row = pr.row
            if row is None or row.get(price_col) is None:
                ctx.reject(level, ds.rel, "no_observation", "no observation at or before T")
                continue
            zombie, z_warns, z_why = self._is_zombie(ds, row, timestamp, probe=pr)
            active = self._recently_active(row, timestamp)
            if zombie or not active:
                self._reject_row(ctx, level, ds, row, timestamp, z_why, inactive=not active)
                continue
            if self._stale(ctx, level, ds.rel, row["ts"], timestamp):
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
            self._reject_eth_leg(ctx, level, asset)
            return None
        if self._stale(ctx, level, eth_file, eth_ts, timestamp, "ETH/USD leg observed"):
            return None
        for path in token_paths:
            ds = self._ds(path)
            ts_col, price_col = (ds.col("timestamp"), ds.col("price_token_eth")) if ds else (None, None)
            if not ts_col or not price_col:
                self._reject_unreadable(ctx, level, self._rel(path), ds)
                continue
            if self._empty(ctx, level, ds):
                continue
            pr = self._probe(ds, timestamp)
            row = pr.row
            if row is None or row.get(price_col) is None:
                ctx.reject(level, ds.rel, "no_observation", "no observation at or before T")
                continue
            token_ts = row[ts_col]
            if eth_ts is not None and hasattr(token_ts, "timestamp") and hasattr(eth_ts, "timestamp"):
                lag = abs((token_ts - eth_ts).total_seconds())
            else:
                lag = 0.0
            if lag > self.cfg.thresholds.cross_rate_max_lag_seconds:
                self._reject_lag(ctx, level, ds, token_ts, eth_ts, lag)
                continue
            zombie, z_warns, z_why = self._is_zombie(ds, row, timestamp, eth_usd_price=eth_price, probe=pr)
            if zombie:
                self._reject_row(ctx, level, ds, row, timestamp, z_why)
                continue
            if self._stale(ctx, level, ds.rel, token_ts, timestamp, "token leg observed"):
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
    def _window_vwmp(self, ds, price_col, vol_col, timestamp, granularity, inverse=False):
        """Yield (step_idx, window_s, half, w_start, w_end, stats) for each R1 step."""
        for step_idx, window_s in enumerate(_WINDOW_STEPS[granularity]):
            half = window_s / 2.0
            w_start = timestamp - timedelta(seconds=half)
            w_end = timestamp + timedelta(seconds=half)
            yield step_idx, window_s, half, w_start, w_end, self._window_stats(ds, price_col, vol_col, w_start,
                                                                               w_end, inverse)

    def _clean(self, st: WindowStats, warns_list, message_full: bool, sigma: float) -> None:
        """The MAD-filter warnings of V1/V2 for a window already filtered (``st``)."""
        if st.excluded > 0:
            msg = (f"{st.excluded} swap(s) excluded by MAD filter (sigma_mad={sigma})." if message_full
                   else f"{st.excluded} swap(s) excluded by MAD filter.")
            warns_list.append(Warning(code="mad_outliers_excluded", message=msg))
        if st.mad_fallback:
            warns_list.append(Warning(code="mad_filter_fallback",
                                      message="All swaps flagged by MAD filter; using unfiltered data."))

    def _direct_stable_windowed(self, asset, timestamp, granularity, paths, level, label, ctx) -> PriceResult | None:
        cfg = self.cfg
        viable: list[tuple[float, DatasetInfo, dict[str, Any], list[Warning]]] = []
        for path in paths:
            ds = self._ds(path)
            ts_col, price_col = (ds.col("timestamp"), ds.col("price_usd")) if ds else (None, None)
            if not ts_col or not price_col:
                self._reject_unreadable(ctx, level, self._rel(path), ds)
                continue
            if self._empty(ctx, level, ds):
                continue
            pr = self._probe(ds, timestamp)
            row = pr.row
            if row is None:
                ctx.reject(level, ds.rel, "no_observation", "no observation at or before T")
                continue
            zombie, z_warns, z_why = self._is_zombie(ds, row, timestamp, probe=pr)
            active = self._recently_active(row, timestamp)
            if zombie or not active:
                self._reject_row(ctx, level, ds, row, timestamp, z_why, inactive=not active)
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
            for step_idx, window_s, half, w_start, w_end, st in self._window_vwmp(ds, price_col, vol_col, timestamp, granularity):
                if not st.n_valid:
                    continue
                warns: list[Warning] = list(pool_warnings)
                self._clean(st, warns, True, cfg.thresholds.sigma_mad)
                if st.kept < cfg.thresholds.min_swaps_for_stat_score:
                    warns.append(Warning(
                        code="low_swap_count",
                        message=(f"Only {st.kept} clean swap(s) in window "
                                 f"(recommended min: {cfg.thresholds.min_swaps_for_stat_score})."),
                        severity="info",
                    ))
                price_vwmp = st.vwmp
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
                    data_status=_windowed_status(step_idx, st.mad_fallback),
                    files_used=[ds.rel],
                    calculation_path=[f"VWMP({st.kept} swaps, window ±{half:.0f}s)", "Direct stablecoin VWMP"],
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=viability_row,
                    source_schema=ds.schema,
                    warnings=warns,
                    granularity=granularity,
                    n_raw=st.n_raw,
                    swap_count=st.kept,
                    window_seconds=float(window_s),
                    excluded_swaps=st.excluded,
                    initial_window_seconds=float(_WINDOW_STEPS[granularity][0]),
                    window_start_utc=w_start,
                    window_end_utc=w_end,
                    window_bound_policy=_WINDOW_BOUND_POLICY,
                    expansion_step=step_idx,
                )
            self._reject_window(ctx, level, ds, granularity, viability_row)
        return None

    def _cross_rate_windowed(self, asset, timestamp, granularity, token_paths, level, label, ctx) -> PriceResult | None:
        cfg = self.cfg
        if not token_paths:
            return None
        eth_price, eth_ts, eth_file, eth_row, eth_schema = self._eth_usd_at(timestamp, asset, ctx)
        if eth_price is None:
            self._reject_eth_leg(ctx, level, asset)
            return None
        last_swap_rule = self._windowed_lag_check() == "last_swap"
        if not last_swap_rule and eth_ts is not None:
            # The token leg is a VWMP centred on T: the lag that matters is the ETH/USD point read's distance to T.
            eth_lag = abs((timestamp - eth_ts).total_seconds())
            limit = cfg.thresholds.cross_rate_max_lag_seconds
            if eth_lag > limit:
                ctx.reject(level, eth_file, "cross_rate_lag",
                           f"ETH/USD leg {_hms(eth_ts)} is {eth_lag:,.0f} s from T (limit {limit:,} s)",
                           eth_lag, float(limit), eth_ts)
                return None
        for path in token_paths:
            ds = self._ds(path)
            ts_col, price_col = (ds.col("timestamp"), ds.col("price_token_eth")) if ds else (None, None)
            if not ts_col or not price_col:
                self._reject_unreadable(ctx, level, self._rel(path), ds)
                continue
            if self._empty(ctx, level, ds):
                continue
            pr = self._probe(ds, timestamp)
            viability_row = pr.row
            if viability_row is None:
                ctx.reject(level, ds.rel, "no_observation", "no observation at or before T")
                continue
            zombie, z_warns, z_why = self._is_zombie(ds, viability_row, timestamp, eth_usd_price=eth_price, probe=pr)
            if zombie:
                self._reject_row(ctx, level, ds, viability_row, timestamp, z_why)
                continue
            if (last_swap_rule and eth_ts is not None and viability_row.get(ts_col) is not None
                    and hasattr(viability_row[ts_col], "timestamp")):
                lag = abs((viability_row[ts_col] - eth_ts).total_seconds())
                if lag > cfg.thresholds.cross_rate_max_lag_seconds:
                    self._reject_lag(ctx, level, ds, viability_row[ts_col], eth_ts, lag)
                    continue
            vol_col = ds.col("volume_usd") or ds.col("volume_token")
            for step_idx, window_s, half, w_start, w_end, st in self._window_vwmp(ds, price_col, vol_col, timestamp, granularity):
                if not st.n_valid:
                    continue
                warns: list[Warning] = list(z_warns)
                self._clean(st, warns, False, cfg.thresholds.sigma_mad)
                if st.kept < cfg.thresholds.min_swaps_for_stat_score:
                    warns.append(Warning(code="low_swap_count",
                                         message=f"Only {st.kept} clean swap(s) in window.", severity="info"))
                token_eth_vwmp = st.vwmp
                if token_eth_vwmp is None:
                    continue
                eth_st = self._eth_leg_vwmp(eth_file, w_start, w_end)
                if eth_st is not None:
                    # Both legs aggregate the same window: the ETH/USD leg is timed at T like the token leg.
                    eth_leg, eth_leg_ts, eth_lag = eth_st.vwmp, timestamp, 0.0
                    eth_line = (f"ETH/USD VWMP({eth_st.kept} swaps, ±{half:.0f}s"
                                + (f", {eth_st.excluded} excluded by MAD" if eth_st.excluded else "") + f"): {eth_file}")
                    leg_row = {**eth_row, ETH_LEG_AGGREGATED: True}  # no single ETH/USD event behind the price
                else:
                    eth_leg, eth_leg_ts, leg_row = eth_price, eth_ts, eth_row
                    eth_lag = abs((timestamp - eth_ts).total_seconds()) if eth_ts is not None else None
                    eth_line = f"ETH/USD point read: {eth_file}"
                price_usd = token_eth_vwmp * eth_leg
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
                    data_status=_windowed_status(step_idx, st.mad_fallback),
                    files_used=files,
                    calculation_path=[
                        f"{asset}/ETH VWMP({st.kept} swaps, ±{half:.0f}s): {ds.rel}",
                        eth_line,
                        f"{asset}/USD = {asset}/ETH VWMP × ETH/USD" + (" VWMP" if eth_st is not None else ""),
                    ],
                    token_leg_timestamp=timestamp,
                    eth_usd_leg_timestamp=eth_leg_ts,
                    cross_rate_lag_seconds=eth_lag,
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=viability_row,
                    source_schema=ds.schema,
                    eth_source_row=leg_row,
                    eth_source_schema=eth_schema,
                    warnings=warns,
                    granularity=granularity,
                    n_raw=st.n_raw,
                    swap_count=st.kept,
                    window_seconds=float(window_s),
                    excluded_swaps=st.excluded,
                    initial_window_seconds=float(_WINDOW_STEPS[granularity][0]),
                    window_start_utc=w_start,
                    window_end_utc=w_end,
                    window_bound_policy=_WINDOW_BOUND_POLICY,
                    expansion_step=step_idx,
                )
            self._reject_window(ctx, level, ds, granularity, viability_row)
        return None

    # -------------------------------------------------------------- level 2 ETH
    def _curve_raw(self, timestamp, ctx) -> PriceResult | None:
        for rel in registry.REGISTRY.get("ETH", {}).get("level_2_amm", []):
            ds = self.store.dataset(rel)
            ts_col, inv_col = (ds.col("timestamp"), ds.col("price_inverse_eth")) if ds else (None, None)
            if not ts_col or not inv_col:
                self._reject_unreadable(ctx, "2", rel, ds)
                continue
            if self._empty(ctx, "2", ds):
                continue
            pr = self._probe(ds, timestamp)
            row = pr.row
            if row is None or row.get(inv_col) is None or float(row[inv_col]) == 0:
                ctx.reject("2", rel, "no_observation", "no observation at or before T")
                continue
            eth_usd = 1.0 / float(row[inv_col])
            zombie, z_warns, z_why = self._is_zombie(ds, row, timestamp, eth_usd_price=eth_usd, probe=pr)
            if zombie:
                self._reject_row(ctx, "2", ds, row, timestamp, z_why)
                continue
            if self._stale(ctx, "2", rel, row["ts"], timestamp):
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

    def _curve_windowed(self, timestamp, granularity, ctx) -> PriceResult | None:
        cfg = self.cfg
        for rel in registry.REGISTRY.get("ETH", {}).get("level_2_amm", []):
            ds = self.store.dataset(rel)
            ts_col, inv_col = (ds.col("timestamp"), ds.col("price_inverse_eth")) if ds else (None, None)
            if not ts_col or not inv_col:
                self._reject_unreadable(ctx, "2", rel, ds)
                continue
            if self._empty(ctx, "2", ds):
                continue
            pr = self._probe(ds, timestamp)
            viability_row = pr.row
            if viability_row is None:
                ctx.reject("2", rel, "no_observation", "no observation at or before T")
                continue
            eth_usd_viability = (
                1.0 / float(viability_row[inv_col])
                if viability_row.get(inv_col) and float(viability_row[inv_col]) != 0 else None
            )
            zombie, z_warns, z_why = self._is_zombie(ds, viability_row, timestamp, eth_usd_price=eth_usd_viability,
                                                     probe=pr)
            if zombie:
                self._reject_row(ctx, "2", ds, viability_row, timestamp, z_why)
                continue
            vol_col = ds.col("volume_usd") or ds.col("volume_token")
            for step_idx, window_s, half, w_start, w_end, st in self._window_vwmp(ds, inv_col, vol_col, timestamp,
                                                                                 granularity, inverse=True):
                if not st.n_valid:
                    continue
                warns: list[Warning] = list(z_warns)
                self._clean(st, warns, False, cfg.thresholds.sigma_mad)
                price_vwmp = st.vwmp
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
                    data_status=_windowed_status(step_idx, st.mad_fallback),
                    files_used=[rel],
                    calculation_path=[
                        f"Curve VWMP({st.kept} swaps, ±{half:.0f}s)",
                        "ETH/USD = 1 / VWMP(price_weth_per_crvusd)",
                    ],
                    detected_columns={ds.csv_name: ds.raw_columns},
                    source_row=viability_row,
                    source_schema=ds.schema,
                    warnings=warns,
                    granularity=granularity,
                    n_raw=st.n_raw,
                    swap_count=st.kept,
                    window_seconds=float(window_s),
                    excluded_swaps=st.excluded,
                    initial_window_seconds=float(_WINDOW_STEPS[granularity][0]),
                    window_start_utc=w_start,
                    window_end_utc=w_end,
                    window_bound_policy=_WINDOW_BOUND_POLICY,
                    expansion_step=step_idx,
                )
            self._reject_window(ctx, "2", ds, granularity, viability_row)
        return None

    # ----------------------------------------------------------------- level 3
    def _chainlink(self, asset, timestamp, ctx) -> PriceResult | None:
        for path in registry.get_chainlink_paths(asset):
            ds = self._ds(path)
            ts_col, price_col = (ds.col("timestamp"), ds.col("price_usd")) if ds else (None, None)
            if not ts_col or not price_col:
                self._reject_unreadable(ctx, "3", self._rel(path), ds)
                continue
            if self._empty(ctx, "3", ds):
                continue
            phase = self.store.active_phase(ds.rel, timestamp)
            row = self.store.as_of(ds.rel, timestamp, _row_cols(ds), phase=phase)
            if row is None or row.get(price_col) is None:
                ctx.reject("3", ds.rel, "no_observation", no_round_message(ds, phase))
                continue
            bn, bts, bwarns = self._extract_block_ref(ds, row)
            path_ = ["Chainlink oracle — latest observation at or before T"]
            if phase is not None:
                path_.append(phase_note(ds, phase))
            return PriceResult(
                price_usd=float(row[price_col]),
                timestamp_observed=row[ts_col],
                branch_level="3",
                branch_label="chainlink_fallback",
                data_status="oracle_fallback",
                files_used=[ds.rel],
                calculation_path=path_,
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
        trace: list[RejectedCandidate] | None = None,
    ) -> PriceResult:
        """Same hierarchy and short-circuit rules as ``price_service.get_price_at``.

        When ``trace`` is given, every candidate evaluated and not used is appended to it.
        """
        ctx = _Ctx()
        try:
            return self._hierarchy(asset, timestamp, branch, source, granularity, ctx)
        finally:
            if trace is not None:
                trace.extend(ctx.rejected)

    def _hierarchy(self, asset: str, timestamp: datetime, branch: str, source: str, granularity: str,
                   ctx: _Ctx) -> PriceResult:
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
                ("0a", lambda: self._direct_stable_raw(asset, timestamp, p0a, "0a", "direct_stable", ctx)),
                ("0b", lambda: self._cross_rate_raw(asset, timestamp, p0b, "0b", "cross_rate", ctx)),
                ("1", lambda: self._level1_raw(asset, timestamp, ctx)),
                ("2", lambda: self._curve_raw(timestamp, ctx) if asset == "ETH" else
                 self._cross_rate_raw(asset, timestamp, registry.get_level_2_amm_token_paths(asset), "2", "alternative_amm", ctx)),
            ]
        else:
            steps = [
                ("0a", lambda: self._direct_stable_windowed(asset, timestamp, granularity, p0a, "0a", "direct_stable", ctx)),
                ("0b", lambda: self._cross_rate_windowed(asset, timestamp, granularity, p0b, "0b", "cross_rate", ctx)),
                ("1", lambda: self._level1_windowed(asset, timestamp, granularity, p1x, ctx)),
                ("2", lambda: self._curve_windowed(timestamp, granularity, ctx) if asset == "ETH" else
                 self._cross_rate_windowed(asset, timestamp, granularity, registry.get_level_2_amm_token_paths(asset), "2", "alternative_amm", ctx)),
            ]
        for name, fn in steps:
            res = level(name, fn)
            if res is not None:
                return res

        # Level 3 (Chainlink) stays a point read whatever the granularity. V1/V2 also reached it with source=dex (and
        # with source=chainlink whatever the branch); the strict filter keeps source=dex on the DEX levels.
        if self.strict_source():
            chainlink = source in ("auto", "chainlink") and branch in ("auto", "3")
        else:
            chainlink = branch in ("auto", "3") or source == "chainlink"
        if chainlink:
            res = self._chainlink(asset, timestamp, ctx)
            if res:
                return res
            if branch == "3":
                return _level_4("missing_source", granularity)
        return _level_4("missing_source", granularity)

    def _level1_raw(self, asset, timestamp, ctx) -> PriceResult | None:
        if asset == "ETH":
            res = self._direct_stable_raw(asset, timestamp, registry.get_level_1_direct_paths(asset), "1", "alternative_pool", ctx)
            if res:
                return res
        xrate = registry.get_level_1_cross_rate_token_paths(asset)
        if xrate:
            return self._cross_rate_raw(asset, timestamp, xrate, "1", "alternative_pool", ctx)
        return None

    def _level1_windowed(self, asset, timestamp, granularity, xrate, ctx) -> PriceResult | None:
        if asset == "ETH":
            res = self._direct_stable_windowed(asset, timestamp, granularity,
                                               registry.get_level_1_direct_paths(asset), "1", "alternative_pool", ctx)
            if res:
                return res
        if xrate:
            return self._cross_rate_windowed(asset, timestamp, granularity, xrate, "1", "alternative_pool", ctx)
        return None
