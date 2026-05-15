from datetime import datetime, timezone
from fastapi import APIRouter, Query
from app import duckdb_client, registry
from app.config import get_config
from app.csv_adapter import inspect as csv_inspect
from app.schemas import ComparePoint, Warning
from app.services import price_service
from app.routers.prices import _validate_asset

router = APIRouter(prefix="/v1")


@router.get(
    "/compare/{asset}",
    response_model=list[ComparePoint],
    summary="Compare DEX price vs Chainlink oracle over a date range",
    description=(
        "For each Chainlink observation between `start` and `end`, returns the "
        "corresponding DEX price (best available branch), the Chainlink oracle price, "
        "and the relative deviation `|DEX − CL| / CL`.\n\n"
        "Use this endpoint to audit DEX/oracle divergence and validate the **S_coh** "
        "coherence score. Each row includes:\n\n"
        "- **timestamp** — Chainlink observation timestamp used as the time axis\n"
        "- **dex_price_usd** — best DEX price at or before that timestamp "
        "(levels `0a → 0b → 2`; `null` if no DEX source is available)\n"
        "- **chainlink_price_usd** — Chainlink oracle price at that timestamp\n"
        "- **deviation** — `|dex − cl| / cl`; `null` when either price is missing\n"
        "- **dex_branch** — source level used for the DEX price (`0a`, `0b`, `2`, or `null`)\n"
        "- **warnings** — any issues encountered while computing the DEX price\n\n"
        "Results are capped at `limit` (max 10 000)."
    ),
    tags=["Confidence & Provenance"],
)
def compare(
    asset: str,
    start: datetime = Query(..., description="Start of the time range (ISO 8601)"),
    end: datetime = Query(..., description="End of the time range (ISO 8601)"),
    limit: int = Query(1000, description="Maximum number of rows to return (hard cap: 10 000)"),
):
    asset = _validate_asset(asset)
    cfg = get_config()
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    limit = min(limit, cfg.api.max_limit)

    # Get Chainlink timestamps as the axis
    cl_paths = registry.get_chainlink_paths(asset)
    if not cl_paths or not cl_paths[0].exists():
        return []

    cl_path = cl_paths[0]
    cl_schema = csv_inspect(cl_path)
    ts_col = cl_schema.mapping.get("timestamp")
    price_col = cl_schema.mapping.get("price_usd")
    if not ts_col or not price_col:
        return []

    cl_rows = duckdb_client.range_query(cl_path, start, end, [ts_col, price_col], limit, ts_col)

    results = []
    for row in cl_rows:
        ts = row[ts_col]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        cl_price = float(row[price_col]) if row.get(price_col) is not None else None

        dex_result = price_service.get_price_at(asset, ts, cfg, source="dex")
        dex_price = dex_result.price_usd

        deviation: float | None = None
        if dex_price is not None and cl_price is not None and cl_price != 0:
            deviation = abs(dex_price - cl_price) / cl_price

        results.append(ComparePoint(
            timestamp=ts,
            dex_price_usd=dex_price,
            chainlink_price_usd=cl_price,
            deviation=deviation,
            dex_branch=dex_result.branch_level if dex_price is not None else None,
            warnings=dex_result.warnings,
        ))
    return results
