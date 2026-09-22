"""API V3 endpoints — /v3/... (Parquet engine; V2 methodology and response schema).

V1 and V2 are untouched and keep reading the CSV files. V3 answers from the derived
Parquet store built by ``python -m app.v3.sync`` and is expected to be one to three
orders of magnitude faster.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Response

from app.config import get_config
from app.routers.prices import _validate_asset, _validate_branch, _validate_granularity, _validate_source
from app.schemas import ComparePoint, ConfidenceV2Detail, PriceV2Response
from app.v3.service import Service, get_service

router = APIRouter(prefix="/v3")


def _service() -> Service:
    svc = get_service()
    svc.store.maybe_reload()
    if not svc.store.datasets():
        raise HTTPException(status_code=503, detail="V3 store is empty: run `python -m app.v3.sync` first.")
    return svc


def _headers(response: Response, svc: Service, t0: float, cached: bool | None = None) -> None:
    response.headers["X-Dataset-Version"] = str(svc.store.version)
    if cached is not None:
        response.headers["X-Cache"] = "HIT" if cached else "MISS"
    response.headers["Server-Timing"] = f"total;dur={(time.perf_counter() - t0) * 1000:.1f}"


@router.get(
    "/prices/{asset}/at",
    response_model=PriceV2Response,
    summary="Price at a timestamp (V3 — fast engine, V2 methodology)",
    description=(
        "Same source hierarchy, VWMP, peg neutralization and confidence model as `/v2`, "
        "served from an indexed Parquet store. Differences from V2: the S_stat 7-day window "
        "and the windowed VWMP read are no longer truncated to 10 000 rows, and 110 duplicated "
        "swap events of the ETH/USDC pool are ignored."
    ),
    tags=["Prices V3"],
)
def price_at_v3(
    response: Response,
    asset: str,
    timestamp: datetime = Query(..., description="ISO 8601 timestamp (e.g. 2024-01-01T00:00:00Z)"),
    source: str = Query("auto", description="Source filter: auto | dex | chainlink"),
    branch: str = Query("auto", description="Force source level: auto | 0a | 0b | 1 | 2 | 3 | 4"),
    granularity: str = Query("raw", description="Price granularity: raw | minute | hour | day"),
    include_confidence: bool = Query(True, description="Include V2 confidence (C, subscores, S_peg)"),
    include_provenance: bool = Query(True, description="Include files used, calculation path, peg source"),
):
    t0 = time.perf_counter()
    asset = _validate_asset(asset)
    branch = _validate_branch(branch)
    source = _validate_source(source)
    granularity = _validate_granularity(granularity)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    svc = _service()
    resp, cached = svc.price_at(asset, timestamp, branch, source, granularity, include_confidence, include_provenance)
    _headers(response, svc, t0, cached)
    return resp


@router.get(
    "/confidence/{asset}/at",
    response_model=ConfidenceV2Detail,
    summary="Confidence index at a timestamp (V3)",
    tags=["Confidence & Provenance V3"],
)
def confidence_at_v3(
    response: Response,
    asset: str,
    timestamp: datetime = Query(..., description="ISO 8601 timestamp (e.g. 2024-01-01T00:00:00Z)"),
):
    t0 = time.perf_counter()
    asset = _validate_asset(asset)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    svc = _service()
    conf = svc.confidence_at(asset, timestamp)
    if conf is None:
        raise HTTPException(status_code=404, detail="No price available; confidence cannot be computed.")
    _headers(response, svc, t0)
    return conf


@router.get("/config", summary="Effective V3 configuration", tags=["System"])
def effective_config_v3():
    cfg = get_config()
    return {"v3": cfg.v3.model_dump(), "confidence_v2": cfg.confidence_v2.model_dump()}


@router.get("/ready", summary="V3 readiness: Parquet store loaded", tags=["System"])
def ready():
    svc = get_service()
    svc.store.maybe_reload()
    ds = svc.store.datasets()
    if not ds:
        raise HTTPException(status_code=503, detail="V3 store is empty: run `python -m app.v3.sync` first.")
    return {
        "status": "ok",
        "dataset_version": svc.store.version,
        "datasets": len(ds),
        "rows": sum(d.n_rows for d in ds.values()),
        "latest_data": max((d.max_ts for d in ds.values() if d.max_ts), default=None),
        "cache": {"hits": svc.cache.hits, "misses": svc.cache.misses},
    }


@router.get(
    "/prices/{asset}",
    response_model=list[PriceV2Response],
    summary="Price time series over a date range (V3)",
    description=(
        "`granularity=raw` enumerates the swap timestamps of the winning source; `minute|hour|day` "
        "compute one VWMP point per step. Confidence and provenance are off by default. "
        "Points are computed in parallel on the fast engine; capped at `limit` (max 10 000)."
    ),
    tags=["Prices V3"],
)
def price_range_v3(
    response: Response,
    asset: str,
    start: datetime = Query(..., description="Start of the time range (ISO 8601)"),
    end: datetime = Query(..., description="End of the time range (ISO 8601)"),
    limit: int = Query(1000, ge=1, description="Maximum number of points (hard cap: 10 000)"),
    source: str = Query("auto"),
    branch: str = Query("auto"),
    granularity: str = Query("raw"),
    include_confidence: bool = Query(False),
    include_provenance: bool = Query(False),
):
    t0 = time.perf_counter()
    asset = _validate_asset(asset)
    branch = _validate_branch(branch)
    source = _validate_source(source)
    granularity = _validate_granularity(granularity)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    svc = _service()
    out = svc.price_range(asset, start, end, limit, branch, source, granularity, include_confidence, include_provenance)
    _headers(response, svc, t0)
    return out


@router.get(
    "/compare/{asset}",
    response_model=list[ComparePoint],
    summary="Compare DEX price vs Chainlink oracle over a date range (V3)",
    tags=["Confidence & Provenance V3"],
)
def compare_v3(
    response: Response,
    asset: str,
    start: datetime = Query(..., description="Start of the time range (ISO 8601)"),
    end: datetime = Query(..., description="End of the time range (ISO 8601)"),
    limit: int = Query(1000, ge=1, description="Maximum number of rows (hard cap: 10 000)"),
):
    t0 = time.perf_counter()
    asset = _validate_asset(asset)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    svc = _service()
    out = svc.compare(asset, start, end, limit)
    _headers(response, svc, t0)
    return out
