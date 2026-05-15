"""Tests for the confidence index."""

import csv
import math
import pytest
from datetime import datetime, timezone
from pathlib import Path

from app.config import AppConfig
from app.services.confidence_service import compose, compute_s_liq
from app.csv_adapter import inspect as csv_inspect


def _make_csv(tmp_path, filename, rows):
    p = tmp_path / filename
    with p.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return p


def test_confidence_weights_sum(cfg):
    w = cfg.confidence_weights
    assert abs(w.w_stat + w.w_liq + w.w_coh - 1.0) < 1e-6


def test_compose_all_scores(cfg):
    score = compose(0.9, 0.8, 0.85, cfg)
    assert score is not None
    assert 0 < score <= 1


def test_compose_missing_score_returns_none(cfg):
    assert compose(0.9, None, 0.85, cfg) is None
    assert compose(None, 0.8, 0.85, cfg) is None
    assert compose(0.9, 0.8, None, cfg) is None


def test_compose_uses_weights(cfg):
    """Verify weighted geometric mean formula."""
    s_stat, s_liq, s_coh = 0.9, 0.8, 0.7
    w = cfg.confidence_weights
    expected = (s_stat ** w.w_stat) * (s_liq ** w.w_liq) * (s_coh ** w.w_coh)
    result = compose(s_stat, s_liq, s_coh, cfg)
    assert result == pytest.approx(expected, rel=1e-9)


def test_s_liq_with_tvl_and_slip(cfg, tmp_path):
    p = _make_csv(tmp_path, "pool.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00",
         "price_usdc_per_link": 14.0,
         "pool_tvl_at_block": 2000000.0,
         "slip_1k": 0.001}
    ])
    schema = csv_inspect(p)
    row = {
        "pool_tvl_at_block": 2000000.0,
        "slip_1k": 0.001,
    }
    s_liq, warns = compute_s_liq(schema, row, cfg)
    assert s_liq is not None
    assert 0 < s_liq <= 1
    # tvl=2M > threshold=1M → s_tvl=1.0; slip=0.001/0.005 → s_slip=exp(-0.2)≈0.819
    s_tvl = 1.0
    s_slip = math.exp(-0.001 / 0.005)
    expected = math.sqrt(s_tvl * s_slip)
    assert s_liq == pytest.approx(expected, rel=1e-6)


def test_s_liq_missing_tvl_uses_slip(cfg, tmp_path):
    p = _make_csv(tmp_path, "pool.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00",
         "price_usdc_per_link": 14.0,
         "slip_1k": 0.002}
    ])
    schema = csv_inspect(p)
    row = {"slip_1k": 0.002}
    s_liq, warns = compute_s_liq(schema, row, cfg)
    assert s_liq is not None
    codes = [w.code for w in warns]
    assert "missing_tvl_column" in codes


def test_s_liq_missing_both_returns_none(cfg, tmp_path):
    p = _make_csv(tmp_path, "pool.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "price_usdc_per_link": 14.0}
    ])
    schema = csv_inspect(p)
    row = {}
    s_liq, warns = compute_s_liq(schema, row, cfg)
    assert s_liq is None
    codes = [w.code for w in warns]
    assert "liquidity_score_unavailable" in codes


def test_cross_rate_price_formula():
    """Cross-rate: TOKEN/USD = TOKEN/ETH × ETH/USD."""
    token_eth = 0.00636363636
    eth_usd = 2200.0
    expected = token_eth * eth_usd
    assert expected == pytest.approx(14.0, rel=0.001)


def test_confidence_endpoint(client):
    r = client.get("/v1/confidence/LINK/at?timestamp=2024-01-01T00:00:00Z")
    assert r.status_code == 200
    body = r.json()
    assert "score" in body
    assert "S_stat" in body
    assert "S_liq" in body
    assert "S_coh" in body
    assert "weights" in body
    assert "parameters" in body
