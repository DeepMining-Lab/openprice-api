"""CSV schema adapter — maps file columns to canonical names.

Column names are verified against the extraction scripts in Open_Price_ETH_Infra.
Each script family produces a predictable schema; the mapping below must cover all
variants across Uniswap V2/V3, SushiSwap V2/V3, Curve, and Chainlink feeds.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from app import duckdb_client

# ---------------------------------------------------------------------------
# Exact alias table  (canonical → list of known raw column names)
# ---------------------------------------------------------------------------
# Verified against scripts in Open_Price_ETH_Infra:
#   Chainlink:   round_updated_at_utc, answer_normalized
#   Uniswap V3:  timestamp, price_usdc_per_*, volume_usdc, pool_tvl_at_block,
#                slip_1k, slip_10k, block_timestamp_utc, dex_protocol
#   Uniswap V2:  same + reserve0/reserve1 (not used)
#   SushiSwap:   price_eth_per_*, volume_eth
#   Curve:       price_weth_per_crvusd (WETH/crvUSD ≈ 1/ETH_price — inverted)
ALIASES: dict[str, list[str]] = {
    "timestamp": [
        "timestamp", "block_timestamp", "block_time", "block_timestamp_utc",
        "datetime", "date", "time",
        "round_updated_at_utc",  # Chainlink
    ],
    "block_number": ["block_number", "block", "blockNumber"],
    "price_usd": [
        "price_usd", "usd_price", "price", "answer",
        "normalized_price_usd", "vwmp_usd", "close",
        "answer_normalized",    # Chainlink feeds
    ],
    "price_token_eth": [
        # Explicit aliases for known ETH/WETH price columns
        "price_eth", "price_weth",
        "token_eth_price", "token_weth_price",
        "price_weth_per_link", "price_eth_per_link",
        "price_weth_per_uni",  "price_eth_per_uni",
        "price_weth_per_aave", "price_eth_per_aave",
        "price_weth_per_comp", "price_eth_per_comp",
    ],
    # price_weth_per_crvusd is intentionally EXCLUDED from price_token_eth:
    # it expresses WETH/crvUSD ≈ 1/ETH_price and requires inversion.
    # It is captured separately via the "price_inverse_eth" canonical below
    # so calling code can detect and handle the inversion explicitly.
    "price_inverse_eth": [
        "price_weth_per_crvusd",  # Curve crvUSD/WETH pool (ETH level_2_amm)
    ],
    # volume_usdc / volume_usdt are stable-denominated → volume_usd.
    # volume_weth, volume_eth, volume_crvusd are ETH/token-denominated:
    # they must NOT be compared against seuil_vol_min_usd_24h which is in USD.
    # They are mapped to a separate canonical so calling code can warn/skip.
    "volume_usd": [
        "volume_usd", "amount_usd", "swap_volume_usd",
        "volume_24h_usd", "volume24h_usd",
        "volume_usdc", "volume_usdt",
    ],
    "volume_token": [
        # ETH/WETH-denominated swap volumes — cannot compare to USD threshold
        "volume_weth", "volume_eth",
        # crvUSD ≈ 1 USD but represents single-swap volume, not 24h aggregate
        "volume_crvusd",
    ],
    "tvl_usd": [
        "tvl_usd", "liquidity_usd", "pool_tvl_usd",
        "reserve_usd", "pool_tvl_at_block",
    ],
    "slippage": [
        "slippage", "slip_1k", "slip_1k_usd", "price_impact_1k",
        "slip_10k",   # 10k-order slippage — fallback when slip_1k absent
    ],
    "tx_hash": ["tx_hash", "transaction_hash", "hash"],
    "pool_address": ["pool_address", "pair_address", "contract_address"],
    "dex": ["dex", "exchange", "protocol", "dex_protocol"],
    "source": ["source", "source_type"],
}

# Reverse map: raw column name → canonical (first match wins)
_ALIAS_TO_CANONICAL: dict[str, str] = {
    alias: canonical
    for canonical, aliases in ALIASES.items()
    for alias in aliases
}

# Pattern-based fallbacks applied after the exact alias lookup.
# Order matters: first matching pattern wins.
# slip_1k and slip_10k already handled by exact aliases above.
_PATTERN_RULES: list[tuple[re.Pattern[str], str]] = [
    # price_usdc_per_X / price_usdt_per_X → direct USD price
    (re.compile(r"^price_us(dc|dt)_per_\w+$", re.I), "price_usd"),
    # price_weth_per_X / price_eth_per_X (except crvusd which is excluded above)
    # → token/ETH price for cross-rate computation
    (re.compile(r"^price_w?eth_per_(?!crvusd)\w+$", re.I), "price_token_eth"),
]


# ---------------------------------------------------------------------------
# Schema descriptor
# ---------------------------------------------------------------------------

@dataclass
class SchemaInfo:
    path: Path
    raw_columns: list[str]
    mapping: dict[str, str]   # canonical → actual column name in file
    warnings: list[dict[str, str]] = field(default_factory=list)

    def has(self, canonical: str) -> bool:
        return canonical in self.mapping

    def col(self, canonical: str) -> str:
        """Return the raw column name for a canonical name."""
        return self.mapping[canonical]

    def available_canonicals(self) -> list[str]:
        return list(self.mapping.keys())


def inspect(path: Path) -> SchemaInfo:
    """Inspect a CSV file and return its schema with canonical column mapping."""
    raw_cols = [d["name"] for d in duckdb_client.describe_csv(path)]
    mapping: dict[str, str] = {}
    for col in raw_cols:
        # 1. Exact alias match
        canonical = _ALIAS_TO_CANONICAL.get(col)
        if canonical and canonical not in mapping:
            mapping[canonical] = col
            continue
        # 2. Pattern-based fallback
        for pattern, canon in _PATTERN_RULES:
            if pattern.match(col) and canon not in mapping:
                mapping[canon] = col
                break
    return SchemaInfo(path=path, raw_columns=raw_cols, mapping=mapping)


def warn_missing(schema: SchemaInfo, canonical: str, context: str = "") -> dict[str, str]:
    code = f"missing_{canonical}_column"
    msg = f"Column '{canonical}' not found in {schema.path.name}"
    if context:
        msg += f" ({context})"
    return {"code": code, "message": msg, "severity": "warning"}
