from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query
from app import registry
from app.config import get_config
from app.schemas import ConfidenceDetail
from app.services import price_service
from app.routers.prices import _validate_asset, _build_confidence

router = APIRouter(prefix="/v1")


@router.get(
    "/confidence/{asset}/at",
    response_model=ConfidenceDetail,
    summary="Confidence index at a specific timestamp",
    description=(
        "Returns the full confidence breakdown for the best available price at `timestamp`:\n\n"
        "- **S_stat** — Statistical hygiene: modified Z-score (MAD method) over the prior 7-day "
        "window. Formula: `exp(−z_MAD² / (2·σ_MAD²))`. Falls back to `s_stat_floor` when fewer "
        "than `min_swaps_for_stat_score` observations exist.\n"
        "- **S_liq** — Liquidity depth: `sqrt(S_TVL × S_slip)` where "
        "`S_TVL = min(1, TVL / seuil_TVL_min_usd)` and `S_slip = exp(−slip_1k / slip_max)`. "
        "Degrades gracefully when TVL or slippage columns are absent.\n"
        "- **S_coh** — Coherence: `exp(−(δ / δ_tol)²)` where δ is the relative deviation "
        "between the DEX price and the Chainlink oracle. For Chainlink fallback (level 3), uses "
        "oracle staleness instead: `exp(−staleness / heartbeat)`.\n"
        "- **score** — Final weighted geometric mean: `S_stat^w_stat × S_liq^w_liq × S_coh^w_coh`.\n\n"
        "Returns 404 if no price is available for the requested timestamp."
    ),
    tags=["Confidence & Provenance"],
)
def confidence_at(
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

    return _build_confidence(result, asset, timestamp, cfg)
