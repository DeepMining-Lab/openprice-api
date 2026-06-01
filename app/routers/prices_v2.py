"""API V2 price endpoint — /v2/prices/{asset}/at (CLAUDE.md §22).

Reuses the V1 source hierarchy, DuckDB reads and VWMP unchanged. Only the
confidence computation and response schema differ: peg neutralization upstream
of S_coh, a separate S_peg sub-score, weighted 3sub/4sub composition, and a
fragility flag. The V1 routes are not touched.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

from fastapi import APIRouter, Query

from app import registry
from app.config import get_config
from app.schemas import ConfidenceV2Detail, PriceV2Response, Provenance, Warning
from app.services import confidence_service, confidence_v2_service as v2, price_service
from app.services.provenance_service import build_provenance
from app.routers.prices import (
    _validate_asset,
    _validate_branch,
    _validate_granularity,
    _validate_source,
)

router = APIRouter(prefix="/v2")


# ---------------------------------------------------------------------------
# Peg neutralization
# ---------------------------------------------------------------------------

def _resolve_quote_schema(result: price_service.PriceResult):
    """Pick the schema whose quote token drives the USD conversion.

    Cross-rate branches (0b/1-cross/2-sushi) convert via the ETH/USD reference
    pool, so the relevant peg is that leg's stablecoin quote. Direct branches
    (0a) use the source pool's own quote.
    """
    if result.eth_source_schema is not None:
        return result.eth_source_schema
    return result.source_schema


def _neutralize(
    result: price_service.PriceResult,
    timestamp: datetime,
    cfg,
) -> tuple[str | None, float | None, datetime | None, str | None, list[Warning]]:
    """Return (quote_currency, peg_value, peg_ts, peg_rel_path, warnings)."""
    schema = _resolve_quote_schema(result)
    if schema is None or result.branch_level in ("3", "4"):
        return None, None, None, None, []
    quote_currency = v2.read_quote_currency(schema)
    if quote_currency is None:
        return None, None, None, None, []
    peg_value, peg_ts, peg_rel, warns = v2.get_peg_at(quote_currency, timestamp, cfg)
    return quote_currency, peg_value, peg_ts, peg_rel, warns


# ---------------------------------------------------------------------------
# S_liq (reuses V1 compute_s_liq, including the cross-rate geometric mean)
# ---------------------------------------------------------------------------

def _compute_s_liq(result: price_service.PriceResult, cfg) -> tuple[float | None, list[Warning]]:
    warns: list[Warning] = []
    if result.branch_level == "3" or not (result.source_schema and result.source_row):
        return None, warns

    eth_usd_for_liq: float | None = None
    if result.eth_source_schema and result.eth_source_row:
        col = result.eth_source_schema.mapping.get("price_usd")
        if col and result.eth_source_row.get(col) is not None:
            eth_usd_for_liq = float(result.eth_source_row[col])

    s_liq_token, token_warns = confidence_service.compute_s_liq(
        result.source_schema, result.source_row, cfg, eth_usd_price=eth_usd_for_liq
    )

    if result.eth_source_schema and result.eth_source_row:
        s_liq_eth, eth_warns = confidence_service.compute_s_liq(
            result.eth_source_schema, result.eth_source_row, cfg
        )
        if s_liq_token is not None and s_liq_eth is not None:
            warns.extend(token_warns)
            warns.extend(eth_warns)
            return math.sqrt(s_liq_token * s_liq_eth), warns
        if s_liq_token is not None:
            warns.extend(token_warns)
            warns.extend(eth_warns)
            return s_liq_token, warns
        if s_liq_eth is not None:
            warns.extend(
                w for w in token_warns
                if w.code not in ("liquidity_score_unavailable",
                                  "missing_tvl_column", "missing_slippage_column")
            )
            warns.extend(eth_warns)
            warns.append(Warning(
                code="s_liq_cross_rate_token_leg_missing",
                message=("TOKEN/WETH leg has no TVL or slippage data; "
                         "S_liq estimated from ETH/USD leg only."),
                severity="info",
            ))
            return s_liq_eth, warns
        warns.extend(token_warns)
        warns.extend(eth_warns)
        return None, warns

    warns.extend(token_warns)
    return s_liq_token, warns


# ---------------------------------------------------------------------------
# V2 confidence builder
# ---------------------------------------------------------------------------

def build_confidence_v2(
    result: price_service.PriceResult,
    asset: str,
    timestamp: datetime,
    cfg,
    peg_value: float | None,
    price_for_coh: float | None,
) -> ConfidenceV2Detail:
    warnings: list[Warning] = []

    # S_stat — not applicable for Chainlink fallback (level 3).
    s_stat: float | None = None
    if result.price_usd is not None and result.branch_level != "3":
        cl_paths = registry.get_chainlink_paths(asset)
        stat_ref_path = (
            cl_paths[0]
            if (result.eth_source_row is not None and cl_paths and cl_paths[0].exists())
            else (registry.resolve_path(result.files_used[0]) if result.files_used else None)
        )
        if stat_ref_path is not None and stat_ref_path.exists():
            s_stat, w = v2.compute_s_stat_v2(stat_ref_path, timestamp, result.price_usd, cfg)
            warnings.extend(w)

    # S_liq — reused from V1 (level 3 excluded inside the helper).
    s_liq, liq_warns = _compute_s_liq(result, cfg)
    warnings.extend(liq_warns)

    # S_coh (V2) — on the peg-neutralized price, level 3 excluded.
    s_coh: float | None = None
    cl_paths = registry.get_chainlink_paths(asset)
    if result.branch_level != "3" and price_for_coh is not None and cl_paths and cl_paths[0].exists():
        s_coh, coh_warns = v2.compute_s_coh_v2(asset, price_for_coh, cl_paths[0], timestamp, cfg)
        warnings.extend(coh_warns)

    # S_peg — separate signal.
    s_peg, peg_warns = v2.compute_s_peg(peg_value, cfg)
    warnings.extend(peg_warns)

    c, mode = v2.compose_v2(s_stat, s_liq, s_coh, s_peg, cfg)
    flag, frag_warns = v2.fragility_flag(c, cfg)
    warnings.extend(frag_warns)

    w = cfg.confidence_v2.weights
    weights = {"w_stat": w.w_stat, "w_liq": w.w_liq, "w_coh": w.w_coh}
    if mode == "4sub":
        weights["w_peg"] = w.w_peg
    t = cfg.thresholds

    return ConfidenceV2Detail(
        C=c,
        composition_mode=mode,
        fragility_flag=flag,
        subscores={"S_stat": s_stat, "S_liq": s_liq, "S_coh": s_coh},
        S_peg=s_peg,
        coherence_mode="oracle_only_staleness" if result.branch_level == "3" else None,
        qualitative_level=v2.qualitative_level(c),
        weights=weights,
        parameters={
            "peg_tol": cfg.confidence_v2.peg.tol,
            "coh_default_delta_tol": cfg.confidence_v2.coh.default_delta_tol,
            "s_stat_window_seconds": cfg.confidence_v2.s_stat.window_seconds,
            "sigma_mad": t.sigma_mad,
            "slip_max": t.slip_max,
            "fragility_c_threshold": cfg.confidence_v2.fragility.c_threshold,
        },
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Response assembly
# ---------------------------------------------------------------------------

def build_v2_response(
    asset: str,
    timestamp: datetime,
    result: price_service.PriceResult,
    include_confidence: bool,
    include_provenance: bool,
    cfg,
    granularity: str = "raw",
) -> PriceV2Response:
    quote_currency, peg_value, peg_ts, peg_rel, neutralize_warns = _neutralize(result, timestamp, cfg)

    price_raw_in_quote = result.price_usd
    price_neutralized_usd: float | None = None
    headline_price = result.price_usd
    if price_raw_in_quote is not None and peg_value is not None:
        price_neutralized_usd = price_raw_in_quote * peg_value
        headline_price = price_neutralized_usd

    price_for_coh = price_neutralized_usd if price_neutralized_usd is not None else price_raw_in_quote

    confidence = None
    if include_confidence and result.price_usd is not None:
        confidence = build_confidence_v2(result, asset, timestamp, cfg, peg_value, price_for_coh)

    provenance: Provenance | None = None
    if include_provenance:
        prov = build_provenance(result)
        prov.parameters = {
            "seuil_TVL_min_usd": cfg.thresholds.seuil_TVL_min_usd,
            "seuil_vol_min_usd_24h": cfg.thresholds.seuil_vol_min_usd_24h,
            "fenetre_inactivite_jours": cfg.thresholds.fenetre_inactivite_jours,
            "quote_currency": quote_currency,
            "quote_currency_peg_file": peg_rel,
            "quote_currency_peg_timestamp": peg_ts.isoformat() if hasattr(peg_ts, "isoformat") else None,
        }
        provenance = prov

    # Deduplicated top-level warnings by code.
    seen: set[str] = set()
    top: list[Warning] = []
    all_src = list(result.warnings) + list(neutralize_warns)
    if provenance:
        all_src.extend(provenance.warnings)
    if confidence:
        all_src.extend(confidence.warnings)
    for w in all_src:
        if w.code not in seen:
            seen.add(w.code)
            top.append(w)

    return PriceV2Response(
        asset=asset,
        timestamp=timestamp,
        timestamp_observed=result.timestamp_observed,
        granularity=granularity,
        price_usd=headline_price,
        price_raw_in_quote=price_raw_in_quote,
        price_neutralized_usd=price_neutralized_usd,
        quote_currency=quote_currency,
        quote_currency_peg=peg_value,
        branch_level=result.branch_level,
        branch_label=result.branch_label,
        data_status=result.data_status,
        swap_count=result.swap_count,
        window_seconds=result.window_seconds,
        unavailable_reason=result.unavailable_reason,
        confidence=confidence,
        provenance=provenance,
        warnings=top,
    )


@router.get(
    "/prices/{asset}/at",
    response_model=PriceV2Response,
    summary="Price at a timestamp (V2 — peg-neutralized + S_peg)",
    description=(
        "Same source hierarchy and VWMP as V1, with the V2 confidence model "
        "(CLAUDE.md §22):\n\n"
        "- **Peg neutralization**: the DEX price is multiplied by the effective "
        "quote-currency peg (USDC/USD or USDT/USD, read as-of T) before S_coh. "
        "`price_usd` is the neutralized USD price; `price_raw_in_quote` and "
        "`quote_currency_peg` are exposed for audit.\n"
        "- **S_peg** — separate sub-score `exp(−(|peg−1|/peg_tol)²)`, published "
        "outside `subscores` (mode 3sub).\n"
        "- **fragility_flag** — `C < c_threshold`; `null` until the threshold is "
        "calibrated.\n"
    ),
    tags=["Prices V2"],
)
def price_at_v2(
    asset: str,
    timestamp: datetime = Query(..., description="ISO 8601 timestamp (e.g. 2024-01-01T00:00:00Z)"),
    source: str = Query("auto", description="Source filter: auto | dex | chainlink"),
    branch: str = Query("auto", description="Force source level: auto | 0a | 0b | 1 | 2 | 3 | 4"),
    granularity: str = Query("raw", description="Price granularity: raw | minute | hour | day"),
    include_confidence: bool = Query(True, description="Include V2 confidence (C, subscores, S_peg)"),
    include_provenance: bool = Query(True, description="Include files used, calculation path, peg source"),
):
    asset = _validate_asset(asset)
    branch = _validate_branch(branch)
    source = _validate_source(source)
    granularity = _validate_granularity(granularity)
    cfg = get_config()

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    result = price_service.get_price_at(
        asset, timestamp, cfg, branch=branch, source=source, granularity=granularity
    )
    return build_v2_response(asset, timestamp, result, include_confidence, include_provenance, cfg, granularity)


@router.get(
    "/config",
    summary="Effective V2 configuration",
    description="Returns the active `confidence_v2` block. The V1 `/v1/config` is unchanged.",
    tags=["System"],
)
def effective_config_v2():
    cfg = get_config()
    return {
        "api": cfg.api.model_dump(),
        "confidence_v2": cfg.confidence_v2.model_dump(),
        "peg_feeds": registry.PEG_FEEDS,
    }
