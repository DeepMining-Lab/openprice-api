"""Provenance builder — assembles the provenance block from a PriceResult."""

from __future__ import annotations

from app.schemas import Provenance, Warning
from app.services.price_service import PriceResult


def build_provenance(result: PriceResult, extra_warnings: list[Warning] | None = None) -> Provenance:
    all_warnings = list(result.warnings)
    if extra_warnings:
        all_warnings.extend(extra_warnings)
    return Provenance(
        files_used=result.files_used,
        branch_level=result.branch_level,
        branch_label=result.branch_label,
        calculation_path=result.calculation_path,
        token_leg_timestamp=result.token_leg_timestamp,
        eth_usd_leg_timestamp=result.eth_usd_leg_timestamp,
        cross_rate_lag_seconds=result.cross_rate_lag_seconds,
        swap_count=result.swap_count,
        window_seconds=result.window_seconds,
        excluded_swaps=result.excluded_swaps,
        detected_columns=result.detected_columns,
        warnings=all_warnings,
    )
