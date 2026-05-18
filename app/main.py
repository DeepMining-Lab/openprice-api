"""OpenPrice API — FastAPI application entry point."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from app.config import load_config
from app.routers import assets, compare, confidence, datasets, health, provenance, prices
from app.routers import config_router

cfg = load_config()

app = FastAPI(title=cfg.api.name, version=cfg.api.version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_UI_PATH = Path(__file__).parent.parent.parent / "interface-api" / "index.html"


@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui():
    return _UI_PATH.read_text(encoding="utf-8")

app.include_router(health.router)
app.include_router(config_router.router)
app.include_router(assets.router)
app.include_router(datasets.router)
app.include_router(prices.router)
app.include_router(confidence.router)
app.include_router(provenance.router)
app.include_router(compare.router)
