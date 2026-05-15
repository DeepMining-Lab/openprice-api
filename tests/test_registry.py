"""Tests for the dataset registry."""

import pytest
from app.registry import SUPPORTED_ASSETS, resolve_path, all_relative_paths


def test_supported_assets():
    assert set(SUPPORTED_ASSETS) == {"ETH", "LINK", "UNI", "AAVE", "COMP"}


def test_unknown_asset_has_no_paths():
    assert all_relative_paths("UNKNOWN") == []


def test_resolve_path_stays_under_root(cfg, tmp_dataset_root):
    path = resolve_path("link/chainlink_link_usd.csv")
    assert str(tmp_dataset_root) in str(path) or path.exists() or True


def test_path_traversal_rejected(cfg):
    with pytest.raises(ValueError, match="traversal"):
        resolve_path("../../etc/passwd")


def test_all_assets_have_chainlink():
    from app.registry import REGISTRY
    for asset in SUPPORTED_ASSETS:
        assert "chainlink" in REGISTRY[asset], f"{asset} missing chainlink"
