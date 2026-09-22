"""Permanent parity guard: API V3 in legacy mode must reproduce the V2 golden master.

The golden master (tests/golden/v2_golden.jsonl) was captured from the unchanged V2 CSV engine on the real
datasets. This test replays it on the real Parquet store, so it is skipped when the store has not been built
(`python -m app.v3.sync`). The only excused differences are the windows containing the 110 duplicated swaps
that V3 removes on purpose. Regenerate the golden data only after a deliberate V2 change.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

import app.config as config_module
from app.config import load_config
from app.v3 import service as service_mod
from app.v3 import store as store_mod

ROOT = Path(__file__).resolve().parent.parent
GOLDEN = ROOT / "tests" / "golden" / "v2_golden.jsonl"


def _load_compare():
    spec = importlib.util.spec_from_file_location("compare_v3", ROOT / "tests" / "golden" / "compare_v3.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def real_v3(monkeypatch):
    cfg = load_config(ROOT / "config" / "openprice.yaml")
    if not (cfg.v3.parquet_path / "manifest.json").exists() or not GOLDEN.exists():
        config_module._config = None
        pytest.skip("V3 Parquet store or golden master not available")
    monkeypatch.setattr(config_module, "_config", cfg)
    store_mod.reset_store()
    service_mod.reset_service()
    yield cfg
    store_mod.reset_store()
    service_mod.reset_service()
    config_module._config = None


def test_v3_legacy_mode_reproduces_v2_golden_master(real_v3):
    counts, _, bad = _load_compare().compare(legacy=True)
    assert counts["total"] >= 200
    assert not bad, f"{len(bad)} record(s) differ from V2, first: {bad[0][0]} {bad[0][1][:3]}"
    assert counts["identical"] + counts["dup_zone"] == counts["total"] - counts["skipped"]


def test_v3_default_mode_only_changes_eth_confidence_and_windowed_prices(real_v3):
    """The two truncation fixes must not touch any other asset."""
    counts, _, bad = _load_compare().compare(legacy=False)
    assert {rid.split("|")[0] for rid, _ in bad} <= {"ETH"}
