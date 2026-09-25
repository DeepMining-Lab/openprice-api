"""API V3 service layer: peg neutralization, V2 confidence model, response assembly, cache.

Response schema and methodology are those of API V2 (peg-neutralized price, S_peg,
3sub/4sub composition, fragility flag). What differs from V2 is only the engine
underneath (Parquet store instead of CSV scans) and two corrections that remove the
V1/V2 truncation of a window to ``api.max_limit`` rows:

* the S_stat reference window ``[T - window, T)`` is now read in full;
* the windowed VWMP read is no longer cut to its oldest 10 000 swaps.

``v3.legacy_truncation: true`` restores both, to prove parity with V2.

A third correction reads Chainlink (S_coh, the level-3 fallback, the peg feeds, the S_stat
reference of a cross-rate) only from the aggregator phase the proxy served at T
(``v3.chainlink_active_phase_only``, off in legacy mode). V2 read the rounds of every phase.

On top of the V2 response, V3 adds diagnostics that never change a number (additive, listed in
``V3_DIAGNOSTIC_CODES``): a timestamp in the future is answered as an explicit NULL, a price that
depends on data not yet synced is flagged as provisional, a fallback to a lower source level says
which candidates were rejected and why (``provenance.rejected_candidates``), a Chainlink read whose
phase table was not checked up to T is flagged, and so is a Chainlink price (level 3, which has no confidence
index) whose round is older than the feed's heartbeat allows (``oracle_stale``). The provenance also names the on-chain event behind
a point price (``source_event``, ``eth_usd_leg_event``) and the data version that answered
(``dataset_version``, ``dataset_files``); ``confidence.parameters`` lists every parameter used.
Range endpoints report truncation and the start of the next page; ``raw`` series have one point per
distinct timestamp.
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
from app.schemas import (ComparePointV3, ConfidenceV2Detail, PriceV3Response, ProvenanceV3, RejectedCandidate,
                         SourceEventV3, Warning)
from app.services import confidence_v2_service as v2
from app.services.price_service import PriceResult, _level_4
from app.services.provenance_service import build_provenance
from app.v3.batch import BatchStore
from app.v3.engine import ETH_LEG_AGGREGATED, N_SAME_TS, Engine, no_round_message
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
    served = store.phase_filtering(rel)  # a Chainlink reference: rounds of the phase served at their time

    if not s_cfg.normalize_by_volatility:
        n, median_p, mad = store.price_stats(rel, price_col, window_start, timestamp, limit, served=served)
        if n < min_n:
            warnings.append(Warning(code="s_stat_insufficient_data",
                                    message=f"Only {n} observations in window (min {min_n}); using floor."))
            return cfg.thresholds.s_stat_floor, warnings
        if mad == 0:
            return 1.0, warnings
        z = 0.6745 * abs(price_at_t - median_p) / mad
        return math.exp(-(z ** 2) / (2 * sigma ** 2)), warnings

    # Volatility-normalized variant (off by default): needs the raw series.
    prices = store.prices(rel, price_col, window_start, timestamp, limit, served=served)
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
    phase = store.active_phase(cl_rel, timestamp)
    row = store.as_of(cl_rel, timestamp, [price_col], phase=phase)
    if row is None or row.get(price_col) is None:
        message = "No Chainlink observation found at or before timestamp."
        if phase is not None:
            message = f"No Chainlink observation found: {no_round_message(ds, phase)}."
        warnings.append(Warning(code="s_coh_no_chainlink_observation", message=message))
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
    row = store.as_of(rel, timestamp, [price_col], phase=store.active_phase(rel, timestamp))
    if row is None or row.get(price_col) is None:
        return None, None, None
    return float(row[price_col]), row[ts_col], rel


# ---------------------------------------------------------------------------
# V3 diagnostics (additive: they never change a price or a score)
# ---------------------------------------------------------------------------

V3_DIAGNOSTIC_CODES = frozenset({"future_timestamp", "beyond_data_coverage", "fallback_explained",
                                 "chainlink_phase_unverified", "oracle_stale"})
# Additive V3 fields: provenance blocks and confidence parameters that V2 does not have.
V3_PROVENANCE_FIELDS = ("rejected_candidates", "source_event", "eth_usd_leg_event", "dataset_version", "dataset_files")
V3_PARAMETER_KEYS = ("coh_delta_tol_used", "seuil_TVL_min_usd", "tvl_score_mode", "tvl_log_min_usd", "tvl_log_ref_usd",
                     "min_swaps_for_stat_score", "s_stat_floor")

_LEVEL_RANK = {"0a": 0, "0b": 1, "1": 2, "2": 3, "3": 4, "4": 5}
_BATCH_MIN_POINTS = 8  # below this, one query per point costs no more than a bulk query


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
    warnings whose code is in ``V3_DIAGNOSTIC_CODES``, the provenance fields of ``V3_PROVENANCE_FIELDS``
    and the confidence parameters of ``V3_PARAMETER_KEYS``."""
    out = copy.deepcopy(payload)

    def keep(ws: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [w for w in ws if w.get("code") not in V3_DIAGNOSTIC_CODES]

    out["warnings"] = keep(out.get("warnings") or [])
    for block in ("confidence", "provenance"):
        if out.get(block):
            out[block]["warnings"] = keep(out[block].get("warnings") or [])
    if out.get("provenance"):
        for k in V3_PROVENANCE_FIELDS:
            out["provenance"].pop(k, None)
    if out.get("confidence") and out["confidence"].get("parameters"):
        for k in V3_PARAMETER_KEYS:
            out["confidence"]["parameters"].pop(k, None)
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
            # V2 labels level 3 although it computes no S_coh there (no confidence index for an oracle price).
            coherence_mode="oracle_only_staleness" if result.branch_level == "3" and (
                cfg.v3.v2_oracle_coherence_label or cfg.v3.legacy_truncation) else None,
            weights=weights,
            parameters={
                "peg_tol": cfg.confidence_v2.peg.tol,
                "coh_default_delta_tol": cfg.confidence_v2.coh.default_delta_tol,
                "s_stat_window_seconds": cfg.confidence_v2.s_stat.window_seconds,
                "sigma_mad": t.sigma_mad,
                "slip_max": t.slip_max,
                "fragility_c_threshold": cfg.confidence_v2.fragility.c_threshold,
                # V3: the remaining parameters of the scores, as used (V3_PARAMETER_KEYS)
                "coh_delta_tol_used": cfg.confidence_v2.coh.delta_tol_by_asset.get(
                    asset, cfg.confidence_v2.coh.default_delta_tol),
                "seuil_TVL_min_usd": t.seuil_TVL_min_usd,
                "tvl_score_mode": cfg.scoring.tvl_score_mode,
                "tvl_log_min_usd": cfg.scoring.tvl_log_min_usd,
                "tvl_log_ref_usd": cfg.scoring.tvl_log_ref_usd,
                "min_swaps_for_stat_score": t.min_swaps_for_stat_score,
                "s_stat_floor": t.s_stat_floor,
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

    def _oracle_warning(self, asset: str, timestamp: datetime, result: PriceResult) -> Warning | None:
        """Flag a level-3 (Chainlink) price whose round is older than heartbeat x (1 + v3.oracle_stale_tolerance)
        before T: the feed publishes at least once per heartbeat, so a newer round should exist."""
        observed = result.timestamp_observed
        heartbeat = self.cfg.v3.oracle_heartbeat_seconds.get(asset)
        if result.branch_level != "3" or observed is None or not heartbeat:
            return None
        limit = heartbeat * (1 + self.cfg.v3.oracle_stale_tolerance)
        age = (timestamp - observed).total_seconds()
        if age <= limit:
            return None
        return Warning(
            code="oracle_stale",
            message=(f"The Chainlink round used was published at {_iso(observed)}, {age / 3600:,.1f} h before T. The "
                     f"{asset}/USD feed publishes at least every {heartbeat:,.0f} s (heartbeat), so this oracle price is "
                     f"stale (limit {limit:,.0f} s: heartbeat + {self.cfg.v3.oracle_stale_tolerance:.0%})."),
        )

    def _phase_warning(self, timestamp: datetime, rels: list[str]) -> Warning | None:
        """Flag a Chainlink read at T whose phase switches were not checked on-chain up to T."""
        late, missing = [], []
        for rel in dict.fromkeys(rels):
            status = self.store.phase_status(rel, timestamp)
            if status == "unverified":
                late.append(rel)
            elif status == "no_table":
                missing.append(rel)
        if not late and not missing:
            return None
        parts = []
        if late:
            checked = {rel: self.store.dataset(rel).phases.verified for rel in late}
            parts.append("the aggregator phase switches of " + ", ".join(
                f"{rel} were last checked on-chain at {_iso(t)}" if t else f"{rel} were never checked on-chain"
                for rel, t in checked.items())
                + ", before T: a switch after that time is not taken into account")
        if missing:
            parts.append("no phase switch table for " + ", ".join(missing)
                         + " (not read from the chain yet): rounds of every aggregator phase were used, as V2 does")
        return Warning(code="chainlink_phase_unverified", message="Chainlink: " + "; ".join(parts) + ".")

    def _event(self, rel: str | None, row: dict[str, Any] | None) -> SourceEventV3 | None:
        """The on-chain event behind a point read: a swap (transaction, log index) or a Chainlink round."""
        ds = self.store.dataset(rel) if rel else None
        if ds is None or row is None or row.get("ts") is None:
            return None
        ts = row["ts"]
        phase = row.get("phase") if ds.has_phases else None
        if phase is not None and self.store.phase_filtering(rel):
            rule, n = "latest_round_of_active_phase", self.store.count_at(rel, ts, phase)
        else:
            n = row[N_SAME_TS] if row.get(N_SAME_TS) is not None else self.store.count_at(rel, ts)
            rule = ("first_swap_of_block" if row.get("log_index") is not None and not self.store.legacy
                    else "first_csv_row")
        tx_hash = row.get("tx_hash")
        if tx_hash is None and row.get("rn") is not None and "transaction_hash" in ds.raw_columns:
            tx_hash = self.store.tx_hash_of(rel, ts, row["rn"])
        return SourceEventV3(
            file=rel, timestamp=ts, tx_hash=tx_hash, log_index=row.get("log_index"),
            block_number=row.get("block_number"), phase=phase, aggregator_round=row.get("agg_round"),
            rows_at_same_timestamp=n, tie_break_rule=rule,
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

        confidence = None
        cl_rel: str | None = None
        if include_confidence and result.price_usd is not None:
            confidence = self._confidence(result, asset, timestamp, peg_value, price_for_coh)
            cl_paths = registry.get_chainlink_paths(asset)
            cl_rel = str(cl_paths[0].relative_to(cfg.paths.datasets_path)) if cl_paths else None

        # V3 diagnostics: appended after the V2 warnings, and to the confidence block when there
        # is one (they qualify the scores too: provisional data, why C is null after a fallback).
        chainlink_read = [r for r in [*result.files_used, cl_rel, peg_rel] if r]
        diagnostics = [w for w in (*(extra or []), self._coverage_warning(timestamp, result, peg_rel),
                                   self._oracle_warning(asset, timestamp, result),
                                   fallback_warning(result, rejected),
                                   self._phase_warning(timestamp, chainlink_read) if result.price_usd is not None
                                   else None) if w is not None]
        if confidence is not None:
            confidence.warnings.extend(diagnostics)

        provenance: ProvenanceV3 | None = None
        if include_provenance:
            files = [r for r in dict.fromkeys([*result.files_used, cl_rel, peg_rel]) if r and self.store.dataset(r)]
            prov = ProvenanceV3(
                **dict(build_provenance(result)), rejected_candidates=rejected,
                source_event=self._event(result.files_used[0] if result.files_used else None,
                                         result.source_row) if result.granularity == "raw" else None,
                eth_usd_leg_event=self._event(result.files_used[1], result.eth_source_row)
                if result.eth_source_row is not None and len(result.files_used) > 1
                and not result.eth_source_row.get(ETH_LEG_AGGREGATED) else None,
                dataset_version=self.store.version,
                dataset_files={r: self.store.dataset(r).file_version for r in files},
            )
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
        key = (self.store.version, self.cfg.v3.legacy_truncation, self.cfg.v3.chainlink_active_phase_only, asset,
               timestamp, branch, source, granularity, include_confidence, include_provenance)
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
    def _for_points(self, stamps: list[datetime]) -> "Service":
        """This service with a store that answers the per-point lookups of ``stamps`` in bulk (``BatchStore``), same
        config and same response cache. Itself for a few points, in legacy mode or with ``v3.batch_ranges: false``."""
        if len(stamps) < _BATCH_MIN_POINTS or not self.cfg.v3.batch_ranges or self.cfg.v3.legacy_truncation:
            return self
        view = copy.copy(self)
        view.store = BatchStore(self.store, stamps)
        view.engine = Engine(self.cfg, view.store)
        return view

    def _map(self, fn, items):
        """Ordered parallel map (DuckDB releases the GIL; cursors are per-thread). With bulk lookups most of the work
        per point is Python, which threads do not run in parallel: fewer threads (``v3.batch_workers``)."""
        items = list(items)
        if len(items) < 8:
            return [fn(i) for i in items]
        workers = self.cfg.v3.batch_workers if isinstance(self.store, BatchStore) else self.cfg.v3.range_workers
        with ThreadPoolExecutor(max(1, workers)) as ex:
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
            # The winning source is probed at `end`, but never in the future (see price_at) nor after the asset's last
            # synced data (a raw read then rejects every pool as older than v3.raw_max_age_seconds).
            at = min(end, datetime.now(timezone.utc))
            covered = self.store.coverage(f"{asset.lower()}/")
            if covered is not None and self.engine.max_age() is not None and at > covered:
                at = max(start, covered)
            probe = self.engine.get_price_at(asset, at, branch=branch, source=source)
            if probe.branch_level == "4" or not probe.files_used:
                return Page([])
            stamps = self.store.distinct_ts(probe.files_used[0], start, end, limit + 1)
            if len(stamps) > limit:
                next_start, stamps = stamps[limit], stamps[:limit]
        svc = self._for_points(stamps)
        results = svc._map(
            lambda ts: svc.price_at(asset, ts, branch, source, granularity, include_confidence, include_provenance),
            stamps,
        )
        return Page([r for r, _ in results], next_start, sum(1 for _, cached in results if cached))

    def compare(self, asset: str, start: datetime, end: datetime, limit: int) -> Page:
        """DEX price vs Chainlink at every Chainlink round in [start, end) (V1 ``/compare`` semantics); with the
        phase filter, only the rounds of the phase the proxy served when they were published. The DEX price is the
        one /v3/prices returns, peg-neutralized like the price S_coh compares (V1/V2 and legacy mode: raw price in
        the quote currency)."""
        self.store.maybe_reload()
        limit = min(limit, self.cfg.api.max_limit)
        cl_paths = registry.get_chainlink_paths(asset)
        if not cl_paths:
            return Page([])
        ds = self.store.dataset(str(cl_paths[0].relative_to(self.cfg.paths.datasets_path)))
        if ds is None or not ds.col("timestamp") or not ds.col("price_usd"):
            return Page([])
        price_col = ds.col("price_usd")
        rows = self.store.window(ds.rel, start, end, [price_col], limit + 1, served=True)
        next_start: datetime | None = None
        if len(rows) > limit:
            next_start, rows = rows[limit]["ts"], rows[:limit]
            # Never split rounds sharing a timestamp across two pages: the next page restarts at it.
            cut = len(rows)
            while cut > 0 and rows[cut - 1]["ts"] == next_start:
                cut -= 1
            rows = rows[:cut] or rows

        svc = self._for_points([r["ts"] for r in rows])

        def one(row: dict[str, Any]) -> ComparePointV3:
            ts = row["ts"]
            cl_price = float(row[price_col]) if row.get(price_col) is not None else None
            dex = svc.engine.get_price_at(asset, ts, source="dex")
            raw = dex.price_usd
            quote, peg = None, None
            if raw is not None and not self.cfg.v3.legacy_truncation:
                quote, peg, _, _ = svc._neutralize(dex, ts)
            dex_price = raw * peg if raw is not None and peg is not None else raw
            deviation = None
            if dex_price is not None and cl_price is not None and cl_price != 0:
                deviation = abs(dex_price - cl_price) / cl_price
            return ComparePointV3(timestamp=ts, dex_price_usd=dex_price, chainlink_price_usd=cl_price,
                                  deviation=deviation, dex_branch=dex.branch_level if dex_price is not None else None,
                                  warnings=dex.warnings, dex_price_raw_in_quote=raw, quote_currency=quote,
                                  quote_currency_peg=peg)

        return Page(svc._map(one, rows), next_start)


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
