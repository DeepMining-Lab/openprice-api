from fastapi import APIRouter
from app.registry import SUPPORTED_ASSETS, REGISTRY

router = APIRouter(prefix="/v1")


@router.get(
    "/assets",
    summary="List supported assets",
    description=(
        "Returns the five supported crypto-assets (`ETH`, `LINK`, `UNI`, `AAVE`, `COMP`) "
        "along with the source roles available for each one (e.g. `chainlink`, "
        "`level_0a_direct_stable`, `level_0b_cross_rate`)."
    ),
    tags=["Assets & Datasets"],
)
def list_assets():
    result = []
    for asset in SUPPORTED_ASSETS:
        entry = REGISTRY.get(asset, {})
        roles = list(entry.keys())
        result.append({"asset": asset, "roles": roles})
    return {"assets": result}
