"""API V2 confidence endpoint — /v2/confidence/{asset}/at (CLAUDE.md §22)."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from app.config import get_config
from app.schemas import ConfidenceV2Detail
from app.services import price_service
from app.routers.prices import _validate_asset
from app.routers.prices_v2 import build_confidence_v2, _neutralize

router = APIRouter(prefix="/v2")


@router.get(
    "/confidence/{asset}/at",
    response_model=ConfidenceV2Detail,
    summary="V2 confidence index at a timestamp",
    description=(
        "Returns the V2 confidence breakdown for the best available price:\n\n"
        "- **subscores** — S_stat, S_liq, S_coh (S_coh on the peg-neutralized price).\n"
        "- **S_peg** — separate quote-currency peg stability score.\n"
        "- **C** — weighted geometric mean (`3sub` default, `4sub` optional).\n"
        "- **fragility_flag** — `C < c_threshold` (null until calibrated).\n\n"
        "Returns 404 if no price is available."
    ),
    tags=["Confidence & Provenance V2"],
)
def confidence_at_v2(
    asset: str,
    timestamp: datetime = Query(..., description="ISO 8601 timestamp (e.g. 2024-01-01T00:00:00Z)"),
):
    asset = _validate_asset(asset)
    cfg = get_config()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    result = price_service.get_price_at(asset, timestamp, cfg)
    if result.price_usd is None:
        raise HTTPException(status_code=404, detail="No price available; confidence cannot be computed.")

    _, peg_value, _, _, _ = _neutralize(result, timestamp, cfg)
    price_for_coh = (
        result.price_usd * peg_value if peg_value is not None else result.price_usd
    )
    return build_confidence_v2(result, asset, timestamp, cfg, peg_value, price_for_coh)
