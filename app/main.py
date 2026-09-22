"""OpenPrice API — FastAPI application entry point."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

from app.config import load_config
from app.routers import assets, compare, confidence, datasets, health, provenance, prices
from app.routers import config_router
from app.routers import prices_v2, confidence_v2
from app.routers import v3

cfg = load_config()

app = FastAPI(title=cfg.api.name, version=cfg.api.version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_UI_PATH = Path(__file__).parent.parent / "interface-api" / "index.html"
_FAVICON_PATH = Path(__file__).parent.parent / "interface-api" / "favicon.svg"


@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui():
    return _UI_PATH.read_text(encoding="utf-8")


@app.get("/favicon.svg", include_in_schema=False)
@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    # Served at both paths: /favicon.svg is what <link rel="icon"> in /ui points to;
    # /favicon.ico is the implicit path most browsers probe on their own. SVG content
    # at the .ico path is intentional — every current browser accepts it via the
    # response's real Content-Type instead of trusting the extension.
    return FileResponse(_FAVICON_PATH, media_type="image/svg+xml")

app.include_router(health.router)
app.include_router(config_router.router)
app.include_router(assets.router)
app.include_router(datasets.router)
app.include_router(prices.router)
app.include_router(confidence.router)
app.include_router(provenance.router)
app.include_router(compare.router)
app.include_router(prices_v2.router)
app.include_router(confidence_v2.router)
app.include_router(v3.router)
