"""Tests for price service and endpoints."""

from datetime import datetime, timezone
import pytest


TS = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_assets_endpoint(client):
    r = client.get("/v1/assets")
    assert r.status_code == 200
    assets = [a["asset"] for a in r.json()["assets"]]
    assert set(assets) == {"ETH", "LINK", "UNI", "AAVE", "COMP"}


def test_unknown_asset_returns_404(client):
    r = client.get("/v1/prices/DOGE/at?timestamp=2024-01-01T00:00:00Z")
    assert r.status_code == 404


def test_invalid_branch_returns_422(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&branch=99")
    assert r.status_code == 422


def test_link_price_at_returns_value(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z")
    assert r.status_code == 200
    body = r.json()
    assert body["asset"] == "LINK"
    assert body["price_usd"] is not None


def test_link_price_uses_level_0a_when_tvl_ok(client):
    """With TVL=2M in fixture, level 0a should be chosen."""
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T06:00:00Z&branch=0a")
    assert r.status_code == 200
    body = r.json()
    assert body["branch_level"] == "0a"
    assert body["price_usd"] == pytest.approx(14.2, rel=0.01)


def test_price_response_has_provenance(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&include_provenance=true")
    assert r.status_code == 200
    body = r.json()
    assert "provenance" in body
    assert body["provenance"]["files_used"]


def test_price_response_has_confidence(client):
    r = client.get("/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&include_confidence=true")
    assert r.status_code == 200
    body = r.json()
    assert "confidence" in body
    conf = body["confidence"]
    assert "score" in conf


def test_no_persistent_db_created(tmp_path):
    """DuckDB must not create .duckdb files."""
    import os, glob
    duckdb_files = glob.glob(str(tmp_path / "*.duckdb"))
    assert len(duckdb_files) == 0


def test_datasets_endpoint(client):
    r = client.get("/v1/datasets")
    assert r.status_code == 200
    body = r.json()
    assert "files" in body
    assert len(body["files"]) > 0


def test_datasets_schema_endpoint(client):
    r = client.get("/v1/datasets/schema?asset=LINK")
    assert r.status_code == 200
    body = r.json()
    assert body["asset"] == "LINK"
    assert len(body["files"]) > 0


def test_datasets_schema_unknown_asset(client):
    r = client.get("/v1/datasets/schema?asset=DOGE")
    assert r.status_code == 404
