"""Tests for CSV schema adapter — alias and pattern mapping."""

from pathlib import Path
import csv
import pytest

from app.csv_adapter import inspect, warn_missing


def _make_csv(tmp_path, filename, rows):
    p = tmp_path / filename
    with p.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return p


def test_timestamp_alias_mapping(tmp_path):
    p = _make_csv(tmp_path, "t.csv", [
        {"block_timestamp": "2024-01-01 00:00:00+00:00", "price_usd": "1.0"}
    ])
    schema = inspect(p)
    assert schema.has("timestamp")
    assert schema.col("timestamp") == "block_timestamp"


def test_price_alias_mapping(tmp_path):
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "answer_normalized": "1.5"}
    ])
    schema = inspect(p)
    assert schema.has("price_usd")
    assert schema.col("price_usd") == "answer_normalized"


def test_pattern_usdc_per_asset(tmp_path):
    """price_usdc_per_link pattern → price_usd."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "price_usdc_per_link": "14.0"}
    ])
    schema = inspect(p)
    assert schema.has("price_usd")
    assert schema.col("price_usd") == "price_usdc_per_link"


def test_pattern_weth_per_asset(tmp_path):
    """price_weth_per_link pattern → price_token_eth."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "price_weth_per_link": "0.006"}
    ])
    schema = inspect(p)
    assert schema.has("price_token_eth")
    assert schema.col("price_token_eth") == "price_weth_per_link"


def test_warn_missing_returns_warning(tmp_path):
    p = _make_csv(tmp_path, "t.csv", [{"timestamp": "2024-01-01 00:00:00+00:00"}])
    schema = inspect(p)
    w = warn_missing(schema, "price_usd", "test context")
    assert w["code"] == "missing_price_usd_column"
    assert "price_usd" in w["message"]


def test_missing_columns_no_crash(tmp_path):
    p = _make_csv(tmp_path, "t.csv", [{"timestamp": "2024-01-01 00:00:00+00:00"}])
    schema = inspect(p)
    assert not schema.has("price_usd")
    assert not schema.has("tvl_usd")


def test_slip_10k_maps_to_slippage(tmp_path):
    """slip_10k is an alias for slippage (fallback to slip_1k when absent)."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "slip_10k": "0.003"}
    ])
    schema = inspect(p)
    assert schema.has("slippage")
    assert schema.col("slippage") == "slip_10k"


def test_slip_1k_takes_priority_over_slip_10k(tmp_path):
    """slip_1k is preferred over slip_10k when both are present."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "slip_1k": "0.001", "slip_10k": "0.003"}
    ])
    schema = inspect(p)
    assert schema.col("slippage") == "slip_1k"


def test_block_timestamp_utc_maps_to_timestamp(tmp_path):
    """block_timestamp_utc (new scripts) maps to timestamp canonical."""
    p = _make_csv(tmp_path, "t.csv", [
        {"block_timestamp_utc": "2024-01-01 00:00:00+00:00", "price_usdc_per_link": "14.0"}
    ])
    schema = inspect(p)
    assert schema.has("timestamp")
    assert schema.col("timestamp") == "block_timestamp_utc"


def test_dex_protocol_maps_to_dex(tmp_path):
    """dex_protocol (new scripts) maps to dex canonical."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "dex_protocol": "uniswap_v3"}
    ])
    schema = inspect(p)
    assert schema.has("dex")
    assert schema.col("dex") == "dex_protocol"


def test_volume_weth_maps_to_volume_token_not_volume_usd(tmp_path):
    """volume_weth must NOT map to volume_usd (wrong units for zombie check)."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "volume_weth": "5.0"}
    ])
    schema = inspect(p)
    assert not schema.has("volume_usd"), "volume_weth must not be treated as USD volume"
    assert schema.has("volume_token"), "volume_weth should map to volume_token canonical"


def test_curve_price_maps_to_price_inverse_eth(tmp_path):
    """price_weth_per_crvusd (Curve) → price_inverse_eth, NOT price_token_eth."""
    p = _make_csv(tmp_path, "t.csv", [
        {"timestamp": "2024-01-01 00:00:00+00:00", "price_weth_per_crvusd": "0.00045"}
    ])
    schema = inspect(p)
    assert schema.has("price_inverse_eth"), "Curve price should map to price_inverse_eth"
    assert not schema.has("price_token_eth"), "Curve price must not map to price_token_eth (would invert price)"
    assert schema.col("price_inverse_eth") == "price_weth_per_crvusd"


def test_new_script_full_schema(tmp_path):
    """Full column set from new Uniswap V3 extraction scripts is handled correctly."""
    p = _make_csv(tmp_path, "link_usdc.csv", [{
        "timestamp": "2024-01-01 00:00:00+00:00",
        "price_usdc_per_link": "14.5",
        "usdc_amount": "145.0",
        "link_amount": "10.0",
        "volume_usdc": "145.0",
        "block_number": "19000000",
        "transaction_hash": "0xabc",
        "log_index": "5",
        "pool_address": "0xpool",
        "pool_fee_tier": "3000",
        "chain_id": "1",
        "sqrt_price_x96": "12345",
        "liquidity": "999999",
        "tick": "-12000",
        "extraction_run_id": "uuid",
        "schema_version": "1",
        "extraction_timestamp_utc": "2026-05-15 00:00:00+00:00",
        "block_timestamp_utc": "2024-01-01 00:00:00+00:00",
        "dex_protocol": "uniswap_v3",
        "pool_tvl_at_block": "2000000.0",
        "slip_1k": "0.001",
        "slip_10k": "0.003",
        "quality_flags": "[]",
    }])
    schema = inspect(p)
    assert schema.has("timestamp") and schema.col("timestamp") == "timestamp"
    assert schema.has("price_usd") and schema.col("price_usd") == "price_usdc_per_link"
    assert schema.has("volume_usd") and schema.col("volume_usd") == "volume_usdc"
    assert schema.has("tvl_usd") and schema.col("tvl_usd") == "pool_tvl_at_block"
    assert schema.has("slippage") and schema.col("slippage") == "slip_1k"
    assert schema.has("dex") and schema.col("dex") == "dex_protocol"
