from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query
from app.config import get_config
from app.schemas import Provenance
from app.services import price_service
from app.services.provenance_service import build_provenance
from app.routers.prices import _validate_asset

router = APIRouter(prefix="/v1")


@router.get(
    "/provenance/{asset}/at",
    response_model=Provenance,
    summary="Full provenance at a specific timestamp",
    description=(
        "Returns the complete data lineage for the price computed at `timestamp`, including:\n\n"
        "- **files_used** — CSV files that contributed to the price\n"
        "- **branch_level / branch_label** — which level of the source hierarchy was selected\n"
        "- **calculation_path** — step-by-step description of how the price was derived "
        "(e.g. `LINK/WETH × WETH/USD` for a cross-rate)\n"
        "- **token_leg_timestamp / eth_usd_leg_timestamp** — individual observation timestamps "
        "for each leg of a cross-rate (level 0b)\n"
        "- **cross_rate_lag_seconds** — time difference between the two legs\n"
        "- **parameters** — threshold values active at query time\n"
        "- **warnings** — any viability issues encountered (missing columns, parse errors, etc.)"
    ),
    tags=["Confidence & Provenance"],
)
def provenance_at(
    asset: str,
    timestamp: datetime = Query(..., description="ISO 8601 timestamp (e.g. 2024-01-01T00:00:00Z)"),
):
    asset = _validate_asset(asset)
    cfg = get_config()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    result = price_service.get_price_at(asset, timestamp, cfg)
    prov = build_provenance(result)
    prov.parameters = {
        "seuil_TVL_min_usd": cfg.thresholds.seuil_TVL_min_usd,
        "seuil_vol_min_usd_24h": cfg.thresholds.seuil_vol_min_usd_24h,
        "fenetre_inactivite_jours": cfg.thresholds.fenetre_inactivite_jours,
    }
    return prov
