from fastapi import APIRouter
from app.config import get_config

router = APIRouter(prefix="/v1")


@router.get(
    "/config",
    summary="Effective configuration",
    description=(
        "Returns all active thresholds, confidence weights, dataset paths, and scoring parameters "
        "as loaded from `config/openprice.yaml`. "
        "Use this endpoint to verify which values are currently in use — especially useful when "
        "evaluating the impact of threshold or weight changes without restarting the API."
    ),
    tags=["System"],
)
def effective_config():
    cfg = get_config()
    return {
        "api": cfg.api.model_dump(),
        "paths": {"datasets_root": cfg.paths.datasets_root,
                  "datasets_path_resolved": str(cfg.paths.datasets_path)},
        "thresholds": cfg.thresholds.model_dump(),
        "confidence_weights": cfg.confidence_weights.model_dump(),
        "chainlink": cfg.chainlink.model_dump(),
        "scoring": cfg.scoring.model_dump(),
    }
