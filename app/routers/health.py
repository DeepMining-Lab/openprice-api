from fastapi import APIRouter
from app.config import get_config

router = APIRouter()


@router.get(
    "/health",
    summary="Health check",
    description="Returns `{status: ok}` when the API is running. No authentication required. Use this endpoint for liveness probes.",
    tags=["System"],
)
def health():
    cfg = get_config()
    return {"status": "ok", "api": cfg.api.name}
