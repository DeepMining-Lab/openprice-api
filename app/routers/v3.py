"""API V3 endpoints — /v3/... (Parquet engine; V2 methodology and response schema).

V1 and V2 are untouched and keep reading the CSV files. V3 answers from the derived
Parquet store built by ``python -m app.v3.sync`` and is expected to be one to three
orders of magnitude faster.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.config import get_config
from app.routers.prices import _validate_asset, _validate_branch, _validate_granularity, _validate_source
from app.schemas import ComparePoint, ConfidenceV2Detail, PriceV3Response
from app.v3 import sync as sync_mod
from app.v3.service import Service, get_service

router = APIRouter(prefix="/v3")


def _service() -> Service:
    svc = get_service()
    svc.store.maybe_reload()
    if not svc.store.datasets():
        raise HTTPException(status_code=503, detail="V3 store is empty: run `python -m app.v3.sync` first.")
    return svc


def _headers(response: Response, svc: Service, t0: float, cache: str | None = None) -> None:
    response.headers["X-Dataset-Version"] = str(svc.store.version)
    if cache is not None:
        response.headers["X-Cache"] = cache
    response.headers["Server-Timing"] = f"total;dur={(time.perf_counter() - t0) * 1000:.1f}"


def _page_headers(request: Request, response: Response, next_start: datetime | None) -> None:
    """Truncation signal of a range endpoint; the next page is the same query from ``X-Next-Start``.

    ``Link`` is a query-only reference, resolved by the client against the URL it called: an absolute
    URL built here would name the internal host and miss the prefix of a gateway in front of the API.
    """
    response.headers["X-Truncated"] = "true" if next_start is not None else "false"
    if next_start is not None:
        nxt = next_start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        response.headers["X-Next-Start"] = nxt
        response.headers["Link"] = f'<?{request.url.include_query_params(start=nxt).query}>; rel="next"'


def _hdr(description: str) -> dict:
    return {"description": description, "schema": {"type": "string"}}


_H_COMMON = {
    "X-Dataset-Version": _hdr("Version of the Parquet store that answered; changes after every sync that adds data."),
    "Server-Timing": _hdr("Server-side duration: `total;dur=<milliseconds>`."),
}
_H_CACHE = _hdr("`HIT` or `MISS`: served from the in-process LRU cache of point responses or computed. "
                "The cache is emptied whenever the dataset version changes.")
_H_RANGE_CACHE = _hdr("`HIT` (every point from the cache), `MISS` (none) or `PARTIAL`.")
_H_PAGE = {
    "X-Truncated": _hdr("`true` when the result was cut at `limit`; request the next page from `X-Next-Start`."),
    "X-Next-Start": _hdr("Only when truncated: the `start` value of the next page (ISO 8601, UTC)."),
    "Link": _hdr('Only when truncated: `<?query of the next page>; rel="next"` (RFC 8288), relative to the '
                 'request URL.'),
}


@router.get(
    "/prices/{asset}/at",
    response_model=PriceV3Response,
    summary="Price at a timestamp (V3 — fast engine, V2 methodology)",
    description=(
        "Same source hierarchy, VWMP, peg neutralization and confidence model as `/v2`, "
        "served from an indexed Parquet store. Differences from V2: the S_stat 7-day window "
        "and the windowed VWMP read are no longer truncated to 10 000 rows, 110 duplicated "
        "swap events of the ETH/USDC pool are ignored, and Chainlink is read only from the aggregator "
        "phase its proxy served at T.\n\n"
        "Additive diagnostics (never change a number):\n"
        "- a `timestamp` after the server time returns level 4 with `unavailable_reason: future_timestamp`;\n"
        "- `beyond_data_coverage` warns that the price depends on data after the last sync (provisional);\n"
        "- `fallback_explained` says why higher-priority sources were rejected; the full list is in "
        "`provenance.rejected_candidates`;\n"
        "- `chainlink_phase_unverified` warns that a Chainlink phase switch after the last on-chain check "
        "could be missing.\n\n"
        "The provenance names the on-chain event of a point read (`source_event`, `eth_usd_leg_event`: "
        "transaction and log index of a swap, or phase and round of a Chainlink answer) and the data version "
        "that answered (`dataset_version`, `dataset_files`)."
    ),
    tags=["Prices V3"],
    responses={200: {"headers": {**_H_COMMON, "X-Cache": _H_CACHE}}},
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
    _headers(response, svc, t0, "HIT" if cached else "MISS")
    return resp


@router.get(
    "/confidence/{asset}/at",
    response_model=ConfidenceV2Detail,
    summary="Confidence index at a timestamp (V3)",
    description="The confidence block of the default `/v3/prices/{asset}/at` response (same cache entry).",
    tags=["Confidence & Provenance V3"],
    responses={200: {"headers": {**_H_COMMON, "X-Cache": _H_CACHE}}},
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
    conf, cached, resp = svc.confidence_at(asset, timestamp)
    if conf is None:
        reason = f" ({resp.unavailable_reason})" if resp.unavailable_reason else ""
        raise HTTPException(status_code=404, detail=f"No price available{reason}; confidence cannot be computed.")
    _headers(response, svc, t0, "HIT" if cached else "MISS")
    return conf


@router.get("/config", summary="Effective V3 configuration", tags=["System"])
def effective_config_v3():
    """Every parameter V3 reads, as loaded. ``config_sha256`` identifies this set of values (canonical JSON)."""
    cfg = get_config()
    effective = {
        "api": {"default_limit": cfg.api.default_limit, "max_limit": cfg.api.max_limit},
        "thresholds": cfg.thresholds.model_dump(),
        "scoring": cfg.scoring.model_dump(),
        "confidence_v2": cfg.confidence_v2.model_dump(),
        "v3": cfg.v3.model_dump(),
    }
    digest = hashlib.sha256(json.dumps(effective, sort_keys=True).encode()).hexdigest()
    return {"config_sha256": digest, **effective}


@router.get("/datasets", summary="V3 datasets: rows, quality counters, Chainlink phase switches", tags=["System"])
def datasets_v3():
    """One entry per CSV file of the store. ``quality`` accounts for every line read from the CSV (lines dropped
    or anomalous, cells that could not be converted, duplicates removed); ``status`` is ``empty`` (header only),
    ``anomalies`` (some line or cell was not stored as it is in the CSV) or ``ok``."""
    svc = _service()
    manifest = sync_mod.load_manifest(svc.store.root)
    out = []
    for rel, d in sorted(manifest["datasets"].items()):
        q = d.get("quality") or {}
        cl = d.get("chainlink")
        out.append({
            "file": rel,
            "status": "empty" if not d["n_rows"] else ("anomalies" if sync_mod.quality_issues(q) else "ok"),
            "rows": d["n_rows"],
            "first_observation_utc": d["min_ts"],
            "last_observation_utc": d["max_ts"],
            "file_version": d.get("file_version"),
            "csv_bytes_read": d["csv_offset"],
            "csv_lines_read": d.get("csv_lines"),
            "csv_sha256": d.get("csv_sha256"),
            "last_change": d.get("last_change"),
            "quality": q,
            "chainlink": {k: cl.get(k) for k in ("proxy", "switches", "verified_block", "verified_ts", "status")}
            if cl else None,
        })
    return {"dataset_version": manifest.get("version"), "generated_at": manifest.get("generated_at"),
            "schema_version": manifest.get("schema_version"), "datasets": out}


@router.get("/datasets/versions/{version}", summary="A past manifest of the V3 store", tags=["System"])
def dataset_version_v3(version: str):
    """The manifest published under ``version`` (the ``X-Dataset-Version`` / ``provenance.dataset_version`` of a
    response): per file, the CSV bytes and rows it contained. Every published version is kept."""
    svc = _service()
    m = sync_mod.load_history(svc.store.root, version)
    if m is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset version {version!r}.")
    return m


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
    response_model=list[PriceV3Response],
    summary="Price time series over a date range (V3)",
    description=(
        "`granularity=raw` returns one point per distinct swap timestamp of the winning source "
        "(rows sharing a timestamp have the same as-of answer); `minute|hour|day` compute one VWMP "
        "point per step from `start`. Confidence and provenance are off by default. Points are "
        "computed in parallel on the fast engine; at most `limit` points (max 10 000). When the "
        "result is cut at `limit`, `X-Truncated: true` and `X-Next-Start` give the `start` of the "
        "next page (also as a `Link: rel=\"next\"` header)."
    ),
    tags=["Prices V3"],
    responses={200: {"headers": {**_H_COMMON, "X-Cache": _H_RANGE_CACHE, **_H_PAGE}}},
)
def price_range_v3(
    request: Request,
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
    page = svc.price_range(asset, start, end, limit, branch, source, granularity, include_confidence,
                           include_provenance)
    n = len(page.items)
    cache = "HIT" if n and page.cache_hits == n else ("MISS" if page.cache_hits == 0 else "PARTIAL")
    _page_headers(request, response, page.next_start)
    _headers(response, svc, t0, cache)
    return page.items


@router.get(
    "/compare/{asset}",
    response_model=list[ComparePoint],
    summary="Compare DEX price vs Chainlink oracle over a date range (V3)",
    description=(
        "One row per Chainlink round in [start, end), as `/v1/compare`. Not cached (no `X-Cache`). "
        "When the result is cut at `limit`, `X-Truncated: true` and `X-Next-Start` give the `start` "
        "of the next page."
    ),
    tags=["Confidence & Provenance V3"],
    responses={200: {"headers": {**_H_COMMON, **_H_PAGE}}},
)
def compare_v3(
    request: Request,
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
    page = svc.compare(asset, start, end, limit)
    _page_headers(request, response, page.next_start)
    _headers(response, svc, t0)
    return page.items
