"""Price endpoints — /v1/prices/{asset}/at and /v1/prices/{asset}."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app import registry
from app.config import get_config
from app.schemas import (
    ConfidenceDetail,
    PriceResponse,
    Provenance,
    Warning,
)
from app.services import confidence_service, price_service
from app.services.provenance_service import build_provenance

router = APIRouter(prefix="/v1")

VALID_BRANCHES = {"auto", "0a", "0b", "1", "2", "3", "4"}
VALID_SOURCES = {"auto", "dex", "chainlink"}
VALID_GRANULARITIES = {"raw", "minute", "hour", "day"}


def _validate_asset(asset: str) -> str:
    a = asset.upper()
    if a not in registry.SUPPORTED_ASSETS:
        raise HTTPException(status_code=404, detail=f"Asset '{a}' not supported.")
    return a


def _validate_branch(branch: str) -> str:
    if branch not in VALID_BRANCHES:
        raise HTTPException(status_code=422, detail=f"Invalid branch '{branch}'. Valid: {sorted(VALID_BRANCHES)}")
    return branch


def _validate_source(source: str) -> str:
    if source not in VALID_SOURCES:
        raise HTTPException(status_code=422, detail=f"Invalid source '{source}'. Valid: {sorted(VALID_SOURCES)}")
    return source


def _validate_granularity(granularity: str) -> str:
    if granularity not in VALID_GRANULARITIES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid granularity '{granularity}'. Valid: {sorted(VALID_GRANULARITIES)}",
        )
    return granularity


def _build_confidence(
    result: price_service.PriceResult,
    asset: str,
    timestamp: datetime,
    cfg,
) -> ConfidenceDetail:
    all_warnings: list[Warning] = []

    # S_stat
    s_stat: float | None = None
    if result.price_usd is not None and result.files_used:
        first_path = registry.resolve_path(result.files_used[0])
        if first_path.exists():
            s_stat, stat_warns = confidence_service.compute_s_stat(
                first_path, timestamp, result.price_usd, cfg
            )
            all_warnings.extend(stat_warns)

    # S_liq
    s_liq: float | None = None
    if result.source_schema and result.source_row:
        s_liq, liq_warns = confidence_service.compute_s_liq(
            result.source_schema, result.source_row, cfg
        )
        all_warnings.extend(liq_warns)

    # S_coh
    s_coh: float | None = None
    coherence_mode: str | None = None
    cl_paths = registry.get_chainlink_paths(asset)

    if result.branch_level == "3":
        # Oracle-only staleness
        if cl_paths and cl_paths[0].exists():
            s_coh, coh_warns = confidence_service.compute_s_coh_oracle_staleness(
                asset, cl_paths[0], timestamp, cfg
            )
            coherence_mode = "oracle_only_staleness"
            all_warnings.extend(coh_warns)
    elif result.price_usd is not None and cl_paths and cl_paths[0].exists():
        s_coh, coh_warns = confidence_service.compute_s_coh_dex_vs_chainlink(
            asset, result.price_usd, cl_paths[0], timestamp, cfg
        )
        all_warnings.extend(coh_warns)

    score = confidence_service.compose(s_stat, s_liq, s_coh, cfg)
    w = cfg.confidence_weights
    t = cfg.thresholds

    return ConfidenceDetail(
        score=score,
        S_stat=s_stat,
        S_liq=s_liq,
        S_coh=s_coh,
        coherence_mode=coherence_mode,
        weights={"w_stat": w.w_stat, "w_liq": w.w_liq, "w_coh": w.w_coh},
        parameters={
            "seuil_TVL_min_usd": t.seuil_TVL_min_usd,
            "seuil_vol_min_usd_24h": t.seuil_vol_min_usd_24h,
            "fenetre_inactivite_jours": t.fenetre_inactivite_jours,
            "sigma_mad": t.sigma_mad,
            "slip_max": t.slip_max,
        },
        warnings=all_warnings,
    )


def _to_response(
    asset: str,
    timestamp: datetime,
    result: price_service.PriceResult,
    include_confidence: bool,
    include_provenance: bool,
    cfg,
    granularity: str = "raw",
) -> PriceResponse:
    confidence = None
    if include_confidence and result.price_usd is not None:
        confidence = _build_confidence(result, asset, timestamp, cfg)

    provenance = None
    if include_provenance:
        prov = build_provenance(result)
        prov.parameters = {
            "seuil_TVL_min_usd": cfg.thresholds.seuil_TVL_min_usd,
            "seuil_vol_min_usd_24h": cfg.thresholds.seuil_vol_min_usd_24h,
            "fenetre_inactivite_jours": cfg.thresholds.fenetre_inactivite_jours,
        }
        provenance = prov

    return PriceResponse(
        asset=asset,
        timestamp_requested=timestamp,
        timestamp_observed=result.timestamp_observed,
        price_usd=result.price_usd,
        branch_level=result.branch_level,
        branch_label=result.branch_label,
        data_status=result.data_status,
        granularity=granularity,
        n_raw=result.n_raw,
        swap_count=result.swap_count,
        window_seconds=result.window_seconds,
        unavailable_reason=result.unavailable_reason,
        confidence=confidence,
        provenance=provenance,
        warnings=result.warnings,
    )


@router.get(
    "/prices/{asset}/at",
    response_model=PriceResponse,
    summary="Price at a specific timestamp",
    description=(
        "Returns the best available price for the given asset at or before `timestamp`, "
        "following the source hierarchy `0a → 0b → 2 → 3 → 4`.\n\n"
        "**Granularity:**\n"
        "- `raw` (default) — latest single observation at or before T\n"
        "- `minute` — VWMP over T ± 30 s; expands to ± 1 min, 2.5 min, 7.5 min if empty\n"
        "- `hour` — VWMP over T ± 30 min; expands to ± 1 h, 2 h, 4 h if empty\n"
        "- `day` — VWMP over T ± 12 h; no expansion (falls to next source level)\n\n"
        "**Source hierarchy:**\n"
        "- `0a` — Direct TOKEN/USDC or TOKEN/USDT pool (Uniswap V3)\n"
        "- `0b` — Cross-rate TOKEN/WETH VWMP × WETH/USD point read\n"
        "- `2` — Alternative AMM (Curve, SushiSwap)\n"
        "- `3` — Chainlink oracle fallback (always a point read)\n"
        "- `4` — Explicit NULL (no reliable source)\n\n"
        "When no reliable source is available, returns `price_usd: null` and "
        "`data_status: unavailable` with an `unavailable_reason`."
    ),
    tags=["Prices"],
)
def price_at(
    asset: str,
    timestamp: datetime = Query(..., description="ISO 8601 timestamp (e.g. 2024-01-01T00:00:00Z)"),
    source: str = Query("auto", description="Source filter: auto | dex | chainlink"),
    branch: str = Query("auto", description="Force source level: auto | 0a | 0b | 2 | 3 | 4"),
    granularity: str = Query("raw", description="Price granularity: raw | minute | hour | day"),
    include_confidence: bool = Query(True, description="Include S_stat, S_liq, S_coh confidence sub-scores"),
    include_provenance: bool = Query(True, description="Include files used, calculation path, and lags"),
):
    asset = _validate_asset(asset)
    branch = _validate_branch(branch)
    source = _validate_source(source)
    granularity = _validate_granularity(granularity)
    cfg = get_config()

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    result = price_service.get_price_at(asset, timestamp, cfg, branch=branch, source=source, granularity=granularity)
    return _to_response(asset, timestamp, result, include_confidence, include_provenance, cfg, granularity)


@router.get(
    "/prices/{asset}",
    response_model=list[PriceResponse],
    summary="Price time series over a date range",
    description=(
        "Returns a list of price observations for the given asset between `start` and `end`.\n\n"
        "**Granularity behaviour:**\n"
        "- `raw` (default) — timestamps from the winning source CSV; no resampling\n"
        "- `minute` — one VWMP point per minute from `start` to `end`\n"
        "- `hour` — one VWMP point per hour from `start` to `end`\n"
        "- `day` — one VWMP point per day from `start` to `end`\n\n"
        "Results are capped at `limit` (max 10 000). "
        "Confidence and provenance are disabled by default for performance."
    ),
    tags=["Prices"],
)
def price_range(
    asset: str,
    start: datetime = Query(..., description="Start of the time range (ISO 8601)"),
    end: datetime = Query(..., description="End of the time range (ISO 8601)"),
    limit: int = Query(1000, description="Maximum number of rows to return (hard cap: 10 000)"),
    source: str = Query("auto", description="Source filter: auto | dex | chainlink"),
    branch: str = Query("auto", description="Force source level: auto | 0a | 0b | 2 | 3 | 4"),
    granularity: str = Query("raw", description="Granularity: raw | minute | hour | day"),
    include_confidence: bool = Query(False, description="Include confidence sub-scores (slower)"),
    include_provenance: bool = Query(False, description="Include full provenance for each point"),
):
    asset = _validate_asset(asset)
    branch = _validate_branch(branch)
    source = _validate_source(source)
    granularity = _validate_granularity(granularity)
    cfg = get_config()

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    limit = min(limit, cfg.api.max_limit)

    if granularity != "raw":
        # Generate evenly-spaced timestamps and compute VWMP at each point
        step_map = {"minute": 60, "hour": 3600, "day": 86400}
        step = timedelta(seconds=step_map[granularity])
        ts = start
        results = []
        while ts <= end and len(results) < limit:
            r = price_service.get_price_at(asset, ts, cfg, branch=branch, source=source, granularity=granularity)
            results.append(_to_response(asset, ts, r, include_confidence, include_provenance, cfg, granularity))
            ts += step
        return results

    # raw: enumerate timestamps from the winning source CSV
    probe = price_service.get_price_at(asset, end, cfg, branch=branch, source=source)
    if probe.branch_level == "4" or not probe.files_used:
        return []

    from app import duckdb_client as dc
    first_path = registry.resolve_path(probe.files_used[0])
    if not first_path.exists():
        return []

    from app.csv_adapter import inspect as csv_inspect
    schema = csv_inspect(first_path)
    ts_col = schema.mapping.get("timestamp")
    if not ts_col:
        return []

    ts_rows = dc.range_query(first_path, start, end, [ts_col], limit, ts_col)
    results = []
    for row in ts_rows:
        ts = row[ts_col]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        r = price_service.get_price_at(asset, ts, cfg, branch=branch, source=source)
        results.append(_to_response(asset, ts, r, include_confidence, include_provenance, cfg, granularity))
    return results
