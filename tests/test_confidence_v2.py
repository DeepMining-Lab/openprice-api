"""Unit tests for the API V2 confidence model (CLAUDE.md §22).

Pure-function and small-fixture tests — no real dataset required. They cover:
- S_peg formula and the not-applicable case;
- weighted composition in 3sub (default) and 4sub modes;
- fragility_flag behaviour when c_threshold is null vs calibrated;
- S_stat V2 default path reproducing V1 (non-regression of the score);
- quote-currency detection and as-of peg lookup.
"""

from __future__ import annotations

import csv
import math
from datetime import datetime, timezone

import pytest

from app import csv_adapter
from app.config import ConfidenceV2Config
from app.services import confidence_service as v1
from app.services import confidence_v2_service as v2


@pytest.fixture(autouse=True)
def _pin_test_config(cfg, monkeypatch):
    """Pin the global config to the test fixture so registry.resolve_path()
    resolves under tmp_dataset_root regardless of cross-test pollution of the
    module-level _config (other tests' client fixtures may leave it pointing at
    the real dataset root). monkeypatch auto-restores after each test."""
    import app.config as config_module
    monkeypatch.setattr(config_module, "_config", cfg)


# ---------------------------------------------------------------------------
# S_peg
# ---------------------------------------------------------------------------

class TestSPeg:
    def test_perfect_peg_is_one(self, cfg):
        s, warns = v2.compute_s_peg(1.0, cfg)
        assert s == pytest.approx(1.0)
        assert warns == []

    def test_formula(self, cfg):
        peg = 1.001
        s, _ = v2.compute_s_peg(peg, cfg)
        tol = cfg.confidence_v2.peg.tol
        assert s == pytest.approx(math.exp(-((abs(peg - 1.0) / tol) ** 2)))

    def test_depeg_collapses_score(self, cfg):
        # 5 % depeg vs tol 0.25 % → essentially zero.
        s, _ = v2.compute_s_peg(0.95, cfg)
        assert 0.0 <= s < 1e-6

    def test_none_is_not_applicable(self, cfg):
        s, warns = v2.compute_s_peg(None, cfg)
        assert s is None
        assert warns[0].code == "s_peg_not_applicable"


# ---------------------------------------------------------------------------
# Composition (weighted, like V1)
# ---------------------------------------------------------------------------

class TestComposition:
    def test_3sub_default_excludes_peg(self, cfg):
        c, mode = v2.compose_v2(0.9, 0.8, 0.7, 0.5, cfg)
        assert mode == "3sub"
        w = cfg.confidence_v2.weights
        expected = (0.9 ** w.w_stat) * (0.8 ** w.w_liq) * (0.7 ** w.w_coh)
        assert c == pytest.approx(expected)

    def test_3sub_peg_none_does_not_block(self, cfg):
        c, _ = v2.compose_v2(0.9, 0.8, 0.7, None, cfg)
        assert c is not None

    def test_3sub_missing_subscore_is_none(self, cfg):
        c, _ = v2.compose_v2(None, 0.8, 0.7, 0.9, cfg)
        assert c is None

    def test_4sub_renormalizes_weights(self, cfg):
        c2 = cfg.model_copy(deep=True)
        c2.confidence_v2.composite.mode = "4sub"
        score, mode = v2.compose_v2(0.9, 0.8, 0.7, 0.95, c2)
        assert mode == "4sub"
        w = c2.confidence_v2.weights
        total = w.w_stat + w.w_liq + w.w_coh + w.w_peg
        expected = (
            0.9 ** (w.w_stat / total)
            * 0.8 ** (w.w_liq / total)
            * 0.7 ** (w.w_coh / total)
            * 0.95 ** (w.w_peg / total)
        )
        assert score == pytest.approx(expected)

    def test_4sub_peg_none_blocks(self, cfg):
        c2 = cfg.model_copy(deep=True)
        c2.confidence_v2.composite.mode = "4sub"
        score, _ = v2.compose_v2(0.9, 0.8, 0.7, None, c2)
        assert score is None

    def test_score_bounded(self, cfg):
        c, _ = v2.compose_v2(0.99, 0.5, 0.3, 0.9, cfg)
        assert 0.0 <= c <= 1.0


# ---------------------------------------------------------------------------
# Fragility flag
# ---------------------------------------------------------------------------

class TestFragility:
    def test_null_threshold_returns_none_plus_warning(self, cfg):
        flag, warns = v2.fragility_flag(0.5, cfg)  # default c_threshold = None
        assert flag is None
        assert warns[0].code == "fragility_threshold_uncalibrated"

    def test_true_below_threshold(self, cfg):
        c2 = cfg.model_copy(deep=True)
        c2.confidence_v2.fragility.c_threshold = 0.6
        flag, warns = v2.fragility_flag(0.5, c2)
        assert flag is True
        assert warns == []

    def test_false_at_or_above_threshold(self, cfg):
        c2 = cfg.model_copy(deep=True)
        c2.confidence_v2.fragility.c_threshold = 0.6
        flag, _ = v2.fragility_flag(0.7, c2)
        assert flag is False

    def test_none_score_returns_none(self, cfg):
        c2 = cfg.model_copy(deep=True)
        c2.confidence_v2.fragility.c_threshold = 0.6
        flag, _ = v2.fragility_flag(None, c2)
        assert flag is None


# ---------------------------------------------------------------------------
# S_stat V2 — default path reproduces V1
# ---------------------------------------------------------------------------

class TestSStatV2:
    def test_default_matches_v1(self, cfg, tmp_dataset_root):
        from app import registry
        path = registry.resolve_path("link/link_usdc_uniswap_v3_03.csv")
        t = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)
        price = 15.0
        s1, _ = v1.compute_s_stat(path, t, price, cfg)
        s2, _ = v2.compute_s_stat_v2(path, t, price, cfg)
        assert s2 == pytest.approx(s1)

    def test_normalized_variant_is_bounded(self, cfg, tmp_dataset_root):
        from app import registry
        c2 = cfg.model_copy(deep=True)
        c2.confidence_v2.s_stat.normalize_by_volatility = True
        path = registry.resolve_path("link/link_usdc_uniswap_v3_03.csv")
        t = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)
        s, _ = v2.compute_s_stat_v2(path, t, 15.0, c2)
        assert s is None or 0.0 <= s <= 1.0


# ---------------------------------------------------------------------------
# Quote-currency detection + peg lookup
# ---------------------------------------------------------------------------

class TestQuoteAndPeg:
    def _write(self, path, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    def test_read_quote_currency(self, cfg, tmp_dataset_root):
        pool = tmp_dataset_root / "link" / "v2_quote_pool.csv"
        self._write(pool, [
            {"timestamp": "2024-01-01 00:00:00+00:00",
             "price_usdc_per_link": 14.0, "quote_token_symbol": "USDC"},
        ])
        schema = csv_adapter.inspect(pool)
        assert v2.read_quote_currency(schema) == "USDC"

    def test_read_quote_currency_absent(self, cfg, tmp_dataset_root):
        from app import registry
        # The standard LINK/USDC fixture has no quote_token_symbol column.
        path = registry.resolve_path("link/link_usdc_uniswap_v3_03.csv")
        schema = csv_adapter.inspect(path)
        assert v2.read_quote_currency(schema) is None

    def test_get_peg_at(self, cfg, tmp_dataset_root):
        feed = tmp_dataset_root / "stablecoins" / "chainlink_usdc_usd.csv"
        self._write(feed, [
            {"round_updated_at_utc": "2024-01-01 00:00:00+00:00", "answer_normalized": 0.999},
            {"round_updated_at_utc": "2024-01-02 00:00:00+00:00", "answer_normalized": 1.0001},
        ])
        t = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        val, ts, rel, warns = v2.get_peg_at("USDC", t, cfg)
        assert val == pytest.approx(0.999)
        assert rel == "stablecoins/chainlink_usdc_usd.csv"

    def test_get_peg_at_unknown_currency(self, cfg, tmp_dataset_root):
        t = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        val, ts, rel, warns = v2.get_peg_at("WETH", t, cfg)
        assert val is None


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

class TestV2ConfigDefaults:
    def test_defaults_load_without_yaml_block(self):
        # A ConfidenceV2Config built with no args must be valid (so a YAML
        # without a confidence_v2 block still loads — V1 non-regression).
        c = ConfidenceV2Config()
        assert c.composite.mode == "3sub"
        assert c.peg.tol == pytest.approx(0.0025)
        assert c.fragility.c_threshold is None
        assert abs(c.weights.w_stat + c.weights.w_liq + c.weights.w_coh - 1.0) < 1e-6

    def test_cfg_fixture_has_v2_defaults(self, cfg):
        # The test config YAML omits confidence_v2 → defaults must be present.
        assert cfg.confidence_v2.composite.mode == "3sub"
        assert cfg.confidence_v2.peg.tol == pytest.approx(0.0025)
