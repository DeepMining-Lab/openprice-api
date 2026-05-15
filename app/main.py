"""OpenPrice API — FastAPI application entry point."""

from fastapi import FastAPI

from app.config import load_config
from app.routers import assets, compare, confidence, datasets, health, provenance, prices
from app.routers import config_router

cfg = load_config()

app = FastAPI(title=cfg.api.name, version=cfg.api.version)

app.include_router(health.router)
app.include_router(config_router.router)
app.include_router(assets.router)
app.include_router(datasets.router)
app.include_router(prices.router)
app.include_router(confidence.router)
app.include_router(provenance.router)
app.include_router(compare.router)
