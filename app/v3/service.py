"""API V3 service layer: peg neutralization, V2 confidence model, response assembly, cache.

Response schema and methodology are those of API V2 (peg-neutralized price, S_peg,
3sub/4sub composition, fragility flag). What differs from V2 is only the engine
underneath (Parquet store instead of CSV scans) and two corrections that remove the
V1/V2 truncation of a window to ``api.max_limit`` rows:

* the S_stat reference window ``[T - window, T)`` is now read in full;
* the windowed VWMP read is no longer cut to its oldest 10 000 swaps.

``v3.legacy_truncation: true`` restores both, to prove parity with V2.

On top of the V2 response, V3 adds diagnostics that never change a number (additive, listed in
``V3_DIAGNOSTIC_CODES``): a timestamp in the future is answered as an explicit NULL, a price that
depends on data not yet synced is flagged as provisional, a fallback to a lower source level says
which candidates were rejected and why (``provenance.rejected_candidates``). Range endpoints report
truncation and the start of the next page; ``raw`` series have one point per distinct timestamp.
"""

from __future__ import annotations

import copy
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app import registry
from app.config import AppConfig, get_config
from app.routers.prices_v2 import _compute_s_liq
from app.schemas import ComparePoint, ConfidenceV2Detail, PriceV3Response, ProvenanceV3, RejectedCandidate, Warning
from app.services import confidence_v2_service as v2
from app.services.price_service import PriceResult, _level_4
from app.services.provenance_service import build_provenance
from app.v3.engine import Engine
from app.v3.store import Store, get_store


# ---------------------------------------------------------------------------
# Sub-scores on the Parquet store
# ---------------------------------------------------------------------------

def compute_s_stat(store: Store, rel: str, timestamp: datetime, price_at_t: float, cfg: AppConfig):
    """S_stat over ``confidence_v2.s_stat.window_seconds`` (V2 formula, complete window)."""
    warnings: list[Warning] = []
    s_cfg = cfg.confidence_v2.s_stat
    ds = store.dataset(rel)
    ts_col = ds.col("timestamp") if ds else None
    price_col = ds.col("price_usd") if ds else None
    if not ts_col or not price_col:
        warnings.append(Warning(code="s_stat_missing_columns",
                                message="Cannot compute S_stat: missing timestamp or price_usd column."))
        return cfg.thresholds.s_stat_floor, warnings

    window_start = timestamp - timedelta(seconds=s_cfg.window_seconds)
    limit = cfg.api.max_limit if cfg.v3.legacy_truncation else None
    min_n = cfg.thresholds.min_swaps_for_stat_score
    sigma = cfg.thresholds.sigma_mad

    if not s_cfg.normalize_by_volatility:
        n, median_p, mad = store.price_stats(rel, price_col, window_start, timestamp, limit)
        if n < min_n:
            warnings.append(Warning(code="s_stat_insufficient_data",
                                    message=f"Only {n} observations in window (min {min_n}); using floor."))
            return cfg.thresholds.s_stat_floor, warnings
        if mad == 0:
            return 1.0, warnings
        z = 0.6745 * abs(price_at_t - median_p) / mad
        return math.exp(-(z ** 2) / (2 * sigma ** 2)), warnings

    # Volatility-normalized variant (off by default): needs the raw series.
    prices = store.prices(rel, price_col, window_start, timestamp, limit)
    if len(prices) < min_n:
        warnings.append(Warning(code="s_stat_insufficient_data",
                                message=f"Only {len(prices)} observations in window (min {min_n}); using floor."))
        return cfg.thresholds.s_stat_floor, warnings
    if price_at_t <= 0 or any(p <= 0 for p in prices):
        warnings.append(Warning(code="s_stat_nonpositive_price",
                                message="Non-positive price encountered; log-space S_stat cannot be computed."))
        return cfg.thresholds.s_stat_floor, warnings
    logs = [math.log(p) for p in prices]
    median_log = sorted(logs)[len(logs) // 2]
    if s_cfg.volatility_estimator == "realized_vol":
        returns = [logs[i] - logs[i - 1] for i in range(1, len(logs))]
        mean_r = sum(returns) / len(returns) if returns else 0.0
        var = sum((r - mean_r) ** 2 for r in returns) / len(returns) if returns else 0.0
        local_vol = math.sqrt(var)
    else:
        abs_devs = sorted(abs(lp - median_log) for lp in logs)
        local_vol = abs_devs[len(abs_devs) // 2] / 0.6745
    if local_vol == 0:
        return 1.0, warnings
    z = abs(math.log(price_at_t) - median_log) / local_vol
    return math.exp(-(z ** 2) / (2 * sigma ** 2)), warnings


def compute_s_coh(store: Store, asset: str, dex_price_neutralized: float, cl_rel: str,
                  timestamp: datetime, cfg: AppConfig):
    warnings: list[Warning] = [Warning(
        code="coh_neutralized_peg",
        message="S_coh computed on the peg-neutralized DEX price (V2 behaviour).",
        severity="info",
    )]
    ds = store.dataset(cl_rel)
    ts_col = ds.col("timestamp") if ds else None
    price_col = ds.col("price_usd") if ds else None
    if not ts_col or not price_col:
        warnings.append(Warning(code="s_coh_chainlink_missing_columns",
                                message="Cannot compute S_coh: Chainlink file missing columns."))
        return None, warnings
    row = store.as_of(cl_rel, timestamp, [price_col])
    if row is None or row.get(price_col) is None:
        warnings.append(Warning(code="s_coh_no_chainlink_observation",
                                message="No Chainlink observation found at or before timestamp."))
        return None, warnings
    cl_price = float(row[price_col])
    if cl_price == 0:
        warnings.append(Warning(code="s_coh_chainlink_zero_price",
                                message="Chainlink price is zero; S_coh cannot be computed."))
        return None, warnings
    tol = cfg.confidence_v2.coh.delta_tol_by_asset.get(asset, cfg.confidence_v2.coh.default_delta_tol)
    delta = abs(dex_price_neutralized - cl_price) / cl_price
    return math.exp(-((delta / tol) ** 2)), warnings


def get_peg_at(store: Store, quote_currency: str, timestamp: datetime, cfg: AppConfig):
    """(peg_value, observed_ts, relative_path) read as-of T from the Chainlink stablecoin feed."""
    peg_path = registry.get_peg_feed_path(quote_currency)
    if peg_path is None:
        return None, None, None
    rel = str(peg_path.relative_to(cfg.paths.datasets_path))
    ds = store.dataset(rel)
    if ds is None:
        return None, None, None
    ts_col, price_col = ds.col("timestamp"), ds.col("price_usd")
    if not ts_col or not price_col:
        return None, None, None
    row = store.as_of(rel, timestamp, [price_col])
    if row is None or row.get(price_col) is None:
        return None, None, None
    return float(row[price_col]), row[ts_col], rel


# ---------------------------------------------------------------------------
# V3 diagnostics (additive: they never change a price or a score)
# ---------------------------------------------------------------------------

V3_DIAGNOSTIC_CODES = frozenset({"future_timestamp", "beyond_data_coverage", "fallback_explained"})

_LEVEL_RANK = {"0a": 0, "0b": 1, "1": 2, "2": 3, "3": 4, "4": 5}


def _iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def fallback_warning(result: PriceResult, rejected: list[RejectedCandidate]) -> Warning | None:
    """Summarise the candidates of higher-priority levels that were rejected before ``result``."""
    rank = _LEVEL_RANK.get(result.branch_level, 5)
    by_file: dict[tuple[str, str], list[str]] = {}
    for r in rejected:
        if _LEVEL_RANK.get(r.level, 5) < rank:
            by_file.setdefault((r.level, r.file), []).append(r.message)
    if not by_file:
        return None
    parts = [f"{lvl} {Path(f).stem}: {', '.join(msgs)}" for (lvl, f), msgs in by_file.items()]
    shown, more = parts[:6], len(parts) - 6
    lead = ("No source could answer at T: " if result.branch_level == "4" else
            f"Answered from level {result.branch_level} ({result.branch_label}) because higher-priority "
            "sources were rejected: ")
    return Warning(
        code="fallback_explained",
        severity="info",
        message=(lead + "; ".join(shown) + (f"; and {more} more" if more > 0 else "")
                 + ". Details in provenance.rejected_candidates."),
    )


def strip_v3_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    """V2-comparable copy of a V3 point response (JSON dict) for the parity tools: drops the
    warnings whose code is in ``V3_DIAGNOSTIC_CODES`` and ``provenance.rejected_candidates``."""
    out = copy.deepcopy(payload)

    def keep(ws: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [w for w in ws if w.get("code") not in V3_DIAGNOSTIC_CODES]

    out["warnings"] = keep(out.get("warnings") or [])
    for block in ("confidence", "provenance"):
        if out.get(block):
            out[block]["warnings"] = keep(out[block].get("warnings") or [])
    if out.get("provenance"):
        out["provenance"].pop("rejected_candidates", None)
    return out


@dataclass
class Page:
    """One page of a range endpoint. ``next_start`` is set when the result was cut at ``limit``."""
    items: list[Any]
    next_start: datetime | None = None
    cache_hits: int = 0


# ---------------------------------------------------------------------------
# Small thread-safe LRU
# ---------------------------------------------------------------------------

class _LRU:
    def __init__(self, size: int):
        self.size = size
        self._d: OrderedDict[Any, Any] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
                self.hits += 1
                return self._d[key]
            self.misses += 1
            return None

    def put(self, key, value) -> None:
        if self.size <= 0:
            return
        with self._lock:
            self._d[key] = value
            self._d.move_to_end(key)
            while len(self._d) > self.size:
                self._d.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._d.clear()


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class Service:
    def __init__(self, cfg: AppConfig, store: Store | None = None):
        self.cfg = cfg
        self.store = store or get_store(cfg)
        self.engine = Engine(cfg, self.store)
        self.cache = _LRU(cfg.v3.cache_size)

    # ---------------------------------------------------------- neutralization
    def _neutralize(self, result: PriceResult, timestamp: datetime):
        schema = result.eth_source_schema if result.eth_source_schema is not None else result.source_schema
        if schema is None or result.branch_level in ("3", "4"):
            return None, None, None, None
        quote_currency = self.store.quote_symbol(str(schema.path))
        if quote_currency is None:
            return None, None, None, None
        peg_value, peg_ts, peg_rel = get_peg_at(self.store, quote_currency, timestamp, self.cfg)
        return quote_currency, peg_value, peg_ts, peg_rel

    # -------------------------------------------------------------- confidence
    def _confidence(self, result: PriceResult, asset: str, timestamp: datetime,
                    peg_value: float | None, price_for_coh: float | None) -> ConfidenceV2Detail:
        cfg = self.cfg
        warnings: list[Warning] = []
        cl_paths = registry.get_chainlink_paths(asset)
        cl_rel = str(cl_paths[0].relative_to(cfg.paths.datasets_path)) if cl_paths else None
        cl_exists = cl_rel is not None and self.store.dataset(cl_rel) is not None

        s_stat: float | None = None
        if result.price_usd is not None and result.branch_level != "3":
            if result.eth_source_row is not None and cl_exists:
                ref_rel = cl_rel
            else:
                ref_rel = result.files_used[0] if result.files_used else None
            if ref_rel is not None and self.store.dataset(ref_rel) is not None:
                s_stat, w = compute_s_stat(self.store, ref_rel, timestamp, result.price_usd, cfg)
                warnings.extend(w)

        s_liq, liq_warns = _compute_s_liq(result, cfg)
        warnings.extend(liq_warns)

        s_coh: float | None = None
        if result.branch_level != "3" and price_for_coh is not None and cl_exists:
            s_coh, coh_warns = compute_s_coh(self.store, asset, price_for_coh, cl_rel, timestamp, cfg)
            warnings.extend(coh_warns)

        s_peg, peg_warns = v2.compute_s_peg(peg_value, cfg)
        warnings.extend(peg_warns)

        c, mode = v2.compose_v2(s_stat, s_liq, s_coh, s_peg, cfg)
        flag, frag_warns = v2.fragility_flag(c, cfg)
        warnings.extend(frag_warns)

        w = cfg.confidence_v2.weights
        weights = {"w_stat": w.w_stat, "w_liq": w.w_liq, "w_coh": w.w_coh}
        if mode == "4sub":
            weights["w_peg"] = w.w_peg
        t = cfg.thresholds
        return ConfidenceV2Detail(
            C=c,
            composition_mode=mode,
            fragility_flag=flag,
            subscores={"S_stat": s_stat, "S_liq": s_liq, "S_coh": s_coh},
            S_peg=s_peg,
            coherence_mode="oracle_only_staleness" if result.branch_level == "3" else None,
            weights=weights,
            parameters={
                "peg_tol": cfg.confidence_v2.peg.tol,
                "coh_default_delta_tol": cfg.confidence_v2.coh.default_delta_tol,
                "s_stat_window_seconds": cfg.confidence_v2.s_stat.window_seconds,
                "sigma_mad": t.sigma_mad,
                "slip_max": t.slip_max,
                "fragility_c_threshold": cfg.confidence_v2.fragility.c_threshold,
            },
            warnings=warnings,
        )

    # ------------------------------------------------------------- diagnostics
    def _coverage_warning(self, timestamp: datetime, result: PriceResult, peg_rel: str | None) -> Warning | None:
        """Flag a price that depends on data after the last sync of one of its sources' folders."""
        if result.price_usd is None:
            return None
        horizon = result.window_end_utc or timestamp
        late: dict[str, datetime] = {}
        for rel in [*result.files_used, *([peg_rel] if peg_rel else [])]:
            cov = self.store.coverage(rel)
            if cov is not None and horizon > cov:
                late[rel.split("/")[0]] = cov
        if not late:
            return None
        what = "window end" if result.window_end_utc else "requested time"
        synced = ", ".join(f"{folder}/ up to {_iso(cov)}" for folder, cov in sorted(late.items()))
        return Warning(
            code="beyond_data_coverage",
            message=(f"The {what} {_iso(horizon)} is after the last synced data ({synced}); this price is "
                     "provisional and may change after the next data update."),
        )

    def _future_response(self, asset: str, timestamp: datetime, granularity: str, include_provenance: bool,
                         now: datetime) -> PriceV3Response:
        server_time = _iso(now.replace(microsecond=0))
        warning = Warning(code="future_timestamp",
                          message=f"Requested timestamp {_iso(timestamp)} is after the server time {server_time}; "
                                  "no price can exist yet.")
        return self._build_response(asset, timestamp, _level_4("future_timestamp", granularity), False,
                                    include_provenance, granularity, extra=[warning])

    # ---------------------------------------------------------------- response
    def _build_response(self, asset, timestamp, result: PriceResult, include_confidence: bool,
                        include_provenance: bool, granularity: str,
                        rejected: list[RejectedCandidate] | None = None,
                        extra: list[Warning] | None = None) -> PriceV3Response:
        cfg = self.cfg
        rejected = rejected or []
        quote_currency, peg_value, peg_ts, peg_rel = self._neutralize(result, timestamp)

        price_raw_in_quote = result.price_usd
        price_neutralized_usd: float | None = None
        headline_price = result.price_usd
        if price_raw_in_quote is not None and peg_value is not None:
            price_neutralized_usd = price_raw_in_quote * peg_value
            headline_price = price_neutralized_usd
        price_for_coh = price_neutralized_usd if price_neutralized_usd is not None else price_raw_in_quote

        # V3 diagnostics: appended after the V2 warnings, and to the confidence block when there
        # is one (they qualify the scores too: provisional data, why C is null after a fallback).
        diagnostics = [w for w in (*(extra or []), self._coverage_warning(timestamp, result, peg_rel),
                                   fallback_warning(result, rejected)) if w is not None]

        confidence = None
        if include_confidence and result.price_usd is not None:
            confidence = self._confidence(result, asset, timestamp, peg_value, price_for_coh)
            confidence.warnings.extend(diagnostics)

        provenance: ProvenanceV3 | None = None
        if include_provenance:
            prov = ProvenanceV3(**dict(build_provenance(result)), rejected_candidates=rejected)
            prov.parameters = {
                "seuil_TVL_min_usd": cfg.thresholds.seuil_TVL_min_usd,
                "seuil_vol_min_usd_24h": cfg.thresholds.seuil_vol_min_usd_24h,
                "fenetre_inactivite_jours": cfg.thresholds.fenetre_inactivite_jours,
                "quote_currency": quote_currency,
                "quote_currency_peg_file": peg_rel,
                "quote_currency_peg_timestamp": peg_ts.isoformat() if hasattr(peg_ts, "isoformat") else None,
            }
            provenance = prov

        seen: set[str] = set()
        top: list[Warning] = []
        all_src = list(result.warnings)
        if provenance:
            all_src.extend(provenance.warnings)
        if confidence:
            all_src.extend(confidence.warnings)
        all_src.extend(diagnostics)
        for w in all_src:
            if w.code not in seen:
                seen.add(w.code)
                top.append(w)

        return PriceV3Response(
            asset=asset,
            timestamp=timestamp,
            timestamp_observed=result.timestamp_observed,
            granularity=granularity,
            price_usd=headline_price,
            price_raw_in_quote=price_raw_in_quote,
            price_neutralized_usd=price_neutralized_usd,
            quote_currency=quote_currency,
            quote_currency_peg=peg_value,
            branch_level=result.branch_level,
            branch_label=result.branch_label,
            data_status=result.data_status,
            swap_count=result.swap_count,
            window_seconds=result.window_seconds,
            unavailable_reason=result.unavailable_reason,
            confidence=confidence,
            provenance=provenance,
            warnings=top,
        )

    # ------------------------------------------------------------------ public
    def price_at(self, asset: str, timestamp: datetime, branch: str = "auto", source: str = "auto",
                 granularity: str = "raw", include_confidence: bool = True,
                 include_provenance: bool = True) -> tuple[PriceV3Response, bool]:
        """Returns (response, served_from_cache). A timestamp in the future is an explicit NULL
        (level 4, ``future_timestamp``) and is never cached: the answer changes once T has passed."""
        self.store.maybe_reload()
        now = datetime.now(timezone.utc)
        if timestamp > now:
            return self._future_response(asset, timestamp, granularity, include_provenance, now), False
        key = (self.store.version, self.cfg.v3.legacy_truncation, asset, timestamp, branch, source,
               granularity, include_confidence, include_provenance)
        hit = self.cache.get(key)
        if hit is not None:
            return hit, True
        rejected: list[RejectedCandidate] = []
        result = self.engine.get_price_at(asset, timestamp, branch=branch, source=source, granularity=granularity,
                                          trace=rejected)
        resp = self._build_response(asset, timestamp, result, include_confidence, include_provenance, granularity,
                                    rejected=rejected)
        self.cache.put(key, resp)
        return resp, False

    def confidence_at(self, asset: str, timestamp: datetime) -> tuple[ConfidenceV2Detail | None, bool, PriceV3Response]:
        """(confidence, served_from_cache, point response). The confidence block of the default
        ``/v3/prices/{asset}/at`` response, so both endpoints share the same cache entry."""
        resp, cached = self.price_at(asset, timestamp)
        return resp.confidence, cached, resp

    # ------------------------------------------------------------------ ranges
    def _map(self, fn, items):
        """Ordered parallel map (DuckDB releases the GIL; cursors are per-thread)."""
        items = list(items)
        if len(items) < 8:
            return [fn(i) for i in items]
        with ThreadPoolExecutor(max(1, self.cfg.v3.range_workers)) as ex:
            return list(ex.map(fn, items))

    def price_range(self, asset: str, start: datetime, end: datetime, limit: int, branch: str = "auto",
                    source: str = "auto", granularity: str = "raw", include_confidence: bool = False,
                    include_provenance: bool = False) -> Page:
        """Time series between ``start`` and ``end`` (V1 ``/prices/{asset}`` semantics, except that a
        ``raw`` series has one point per distinct timestamp: every row sharing a timestamp gets the
        same as-of answer, the first swap of the block)."""
        self.store.maybe_reload()
        limit = min(limit, self.cfg.api.max_limit)
        next_start: datetime | None = None
        if granularity != "raw":
            step = timedelta(seconds={"minute": 60, "hour": 3600, "day": 86400}[granularity])
            stamps: list[datetime] = []
            t = start
            while t <= end and len(stamps) < limit:
                stamps.append(t)
                t += step
            if t <= end:
                next_start = t
        else:
            # The winning source is probed at `end`, but never in the future (see price_at).
            probe = self.engine.get_price_at(asset, min(end, datetime.now(timezone.utc)), branch=branch, source=source)
            if probe.branch_level == "4" or not probe.files_used:
                return Page([])
            stamps = self.store.distinct_ts(probe.files_used[0], start, end, limit + 1)
            if len(stamps) > limit:
                next_start, stamps = stamps[limit], stamps[:limit]
        results = self._map(
            lambda ts: self.price_at(asset, ts, branch, source, granularity, include_confidence, include_provenance),
            stamps,
        )
        return Page([r for r, _ in results], next_start, sum(1 for _, cached in results if cached))

    def compare(self, asset: str, start: datetime, end: datetime, limit: int) -> Page:
        """DEX price vs Chainlink at every Chainlink round in [start, end) (V1 ``/compare`` semantics)."""
        self.store.maybe_reload()
        limit = min(limit, self.cfg.api.max_limit)
        cl_paths = registry.get_chainlink_paths(asset)
        if not cl_paths:
            return Page([])
        ds = self.store.dataset(str(cl_paths[0].relative_to(self.cfg.paths.datasets_path)))
        if ds is None or not ds.col("timestamp") or not ds.col("price_usd"):
            return Page([])
        price_col = ds.col("price_usd")
        rows = self.store.window(ds.rel, start, end, [price_col], limit + 1)
        next_start: datetime | None = None
        if len(rows) > limit:
            next_start, rows = rows[limit]["ts"], rows[:limit]
            # Never split rounds sharing a timestamp across two pages: the next page restarts at it.
            cut = len(rows)
            while cut > 0 and rows[cut - 1]["ts"] == next_start:
                cut -= 1
            rows = rows[:cut] or rows

        def one(row: dict[str, Any]) -> ComparePoint:
            ts = row["ts"]
            cl_price = float(row[price_col]) if row.get(price_col) is not None else None
            dex = self.engine.get_price_at(asset, ts, source="dex")
            dex_price = dex.price_usd
            deviation = None
            if dex_price is not None and cl_price is not None and cl_price != 0:
                deviation = abs(dex_price - cl_price) / cl_price
            return ComparePoint(timestamp=ts, dex_price_usd=dex_price, chainlink_price_usd=cl_price,
                                deviation=deviation, dex_branch=dex.branch_level if dex_price is not None else None,
                                warnings=dex.warnings)

        return Page(self._map(one, rows), next_start)


_service: Service | None = None
_service_lock = threading.Lock()


def get_service() -> Service:
    global _service
    if _service is None:
        with _service_lock:
            if _service is None:
                _service = Service(get_config())
    return _service


def reset_service() -> None:
    global _service
    _service = None
