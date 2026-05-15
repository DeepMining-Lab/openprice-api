"""Dataset registry — maps assets to their CSV source files."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from app.config import get_config

SUPPORTED_ASSETS: list[str] = ["ETH", "LINK", "UNI", "AAVE", "COMP"]

# Registry: asset → role → list of relative paths under datasets_root
REGISTRY: dict[str, dict[str, object]] = {
    "ETH": {
        "chainlink": ["eth/chainlink_eth_usd.csv"],
        "level_0a_direct_stable": ["eth/eth_usdc_uniswap_v3_005.csv"],
        "eth_usd_reference": [
            "eth/eth_usdc_uniswap_v3_005.csv",
            "eth/weth_usdc_uniswap_v2_03.csv",
            "eth/weth_usdt_uniswap_v2_03.csv",
        ],
        "level_2_amm": ["eth/crvusd_weth_curve.csv"],
    },
    "LINK": {
        "chainlink": ["link/chainlink_link_usd.csv"],
        "level_0a_direct_stable": [
            "link/link_usdc_uniswap_v3_03.csv",
            "link/link_usdt_uniswap_v3_03.csv",
        ],
        "level_0b_cross_rate": {
            "token_eth_or_weth": [
                "link/link_weth_uniswap_v3_03.csv",
                "link/link_weth_uniswap_v2_03.csv",
                "link/link_eth_sushiswap_v2_03.csv",
                "link/link_eth_sushiswap_v3_03.csv",
            ],
            "eth_usd_reference": [
                "eth/eth_usdc_uniswap_v3_005.csv",
                "eth/weth_usdc_uniswap_v2_03.csv",
                "eth/weth_usdt_uniswap_v2_03.csv",
            ],
        },
    },
    "UNI": {
        "chainlink": ["uni/chainlink_uni_usd.csv"],
        "level_0a_direct_stable": [
            "uni/uni_usdc_uniswap_v3_03.csv",
            "uni/uni_usdt_uniswap_v3_03.csv",
        ],
        "level_0b_cross_rate": {
            "token_eth_or_weth": [
                "uni/uni_weth_uniswap_v3_03.csv",
                "uni/uni_weth_uniswap_v2_03.csv",
                "uni/uni_eth_sushiswap_v3_03.csv",
            ],
            "eth_usd_reference": [
                "eth/eth_usdc_uniswap_v3_005.csv",
                "eth/weth_usdc_uniswap_v2_03.csv",
                "eth/weth_usdt_uniswap_v2_03.csv",
            ],
        },
    },
    "AAVE": {
        "chainlink": ["aave/chainlink_aave_usd.csv"],
        "level_0a_direct_stable": [
            "aave/aave_usdc_uniswap_v3_03.csv",
            "aave/aave_usdt_uniswap_v3_03.csv",
        ],
        "level_0b_cross_rate": {
            "token_eth_or_weth": [
                "aave/aave_weth_uniswap_v3_03.csv",
                "aave/aave_eth_sushiswap_v2_03.csv",
            ],
            "eth_usd_reference": [
                "eth/eth_usdc_uniswap_v3_005.csv",
                "eth/weth_usdc_uniswap_v2_03.csv",
                "eth/weth_usdt_uniswap_v2_03.csv",
            ],
        },
    },
    "COMP": {
        "chainlink": ["comp/chainlink_comp_usd.csv"],
        "level_0a_direct_stable": [
            "comp/comp_usdc_uniswap_v3_03.csv",
            "comp/comp_usdt_uniswap_v3_03.csv",
        ],
        "level_0b_cross_rate": {
            "token_eth_or_weth": [
                "comp/comp_weth_uniswap_v3_03.csv",
                "comp/comp_weth_uniswap_v2_03.csv",
                "comp/comp_eth_sushiswap_v2_03.csv",
            ],
            "eth_usd_reference": [
                "eth/eth_usdc_uniswap_v3_005.csv",
                "eth/weth_usdc_uniswap_v2_03.csv",
                "eth/weth_usdt_uniswap_v2_03.csv",
            ],
        },
    },
}


def resolve_path(relative: str) -> Path:
    """Resolve a registry-relative path to an absolute path, rejecting traversal."""
    root = get_config().paths.datasets_path
    resolved = (root / relative).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"Path traversal rejected: {relative!r}")
    return resolved


def all_relative_paths(asset: str) -> list[tuple[str, str]]:
    """Return all (role, relative_path) pairs for an asset."""
    entry = REGISTRY.get(asset, {})
    result: list[tuple[str, str]] = []
    for role, value in entry.items():
        if isinstance(value, list):
            for p in value:
                result.append((role, p))
        elif isinstance(value, dict):
            for sub_role, paths in value.items():
                for p in paths:
                    result.append((f"{role}.{sub_role}", p))
    return result


def get_chainlink_paths(asset: str) -> list[Path]:
    paths = REGISTRY.get(asset, {}).get("chainlink", [])
    return [resolve_path(p) for p in paths]  # type: ignore[arg-type]


def get_level_0a_paths(asset: str) -> list[Path]:
    paths = REGISTRY.get(asset, {}).get("level_0a_direct_stable", [])
    return [resolve_path(p) for p in paths]  # type: ignore[arg-type]


def get_level_0b_token_paths(asset: str) -> list[Path]:
    cross = REGISTRY.get(asset, {}).get("level_0b_cross_rate", {})
    if not isinstance(cross, dict):
        return []
    return [resolve_path(p) for p in cross.get("token_eth_or_weth", [])]


def get_eth_usd_reference_paths(asset: str = "ETH") -> list[Path]:
    cross = REGISTRY.get(asset, {}).get("level_0b_cross_rate", {})
    if isinstance(cross, dict):
        paths = cross.get("eth_usd_reference", [])
    else:
        paths = REGISTRY.get("ETH", {}).get("eth_usd_reference", [])
    if not paths:
        paths = REGISTRY.get("ETH", {}).get("eth_usd_reference", [])
    return [resolve_path(p) for p in paths]  # type: ignore[arg-type]
