"""What a V3 response records so that a third party can audit it.

* ``provenance.confidence_inputs``: the inputs of every confidence score, with the parameters each score used,
  enough to recompute S_stat, S_liq, S_coh, S_peg, C and ``fragility_flag`` from the record alone
  (``tools/verify_confidence.py`` does it, and checks the inputs against the CSV files with ``--data``);
* ``provenance.config_sha256``: the parameters, as ``GET /v3/config`` publishes them;
* ``provenance.code_commit`` / ``code_dirty``: the code, as checked out when the service started.

Nothing here computes a served number. S_stat, S_coh and S_peg are computed in ``app.v3.service``, which records
their inputs as it computes them. S_liq is computed by the V2 helper ``app.routers.prices_v2._compute_s_liq``
(shared with V2, unchanged): ``s_liq_inputs`` reads the same rows and columns and applies the same formulas, and the
tests check that it gives the served S_liq.
"""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import AppConfig
from app.schemas import ChainlinkRoundV3, RowRefV3, ServedPhaseV3, SLiqInputsV3, SLiqLegV3
from app.services.price_service import PriceResult
from app.v3.store import DatasetInfo, Store

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Parameters and code
# ---------------------------------------------------------------------------

def effective_config(cfg: AppConfig) -> dict[str, Any]:
    """Every parameter V3 reads, as ``GET /v3/config`` publishes it."""
    return {
        "api": {"default_limit": cfg.api.default_limit, "max_limit": cfg.api.max_limit},
        "thresholds": cfg.thresholds.model_dump(),
        "scoring": cfg.scoring.model_dump(),
        "confidence_v2": cfg.confidence_v2.model_dump(),
        "v3": cfg.v3.model_dump(),
    }


def config_sha256(cfg: AppConfig, effective: dict[str, Any] | None = None) -> str:
    """SHA-256 of the effective parameters as canonical JSON (sorted keys)."""
    effective = effective if effective is not None else effective_config(cfg)
    return hashlib.sha256(json.dumps(effective, sort_keys=True).encode()).hexdigest()


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout if out.returncode == 0 else None


def read_code_identity() -> tuple[str | None, bool | None]:
    """(commit checked out, uncommitted changes under app/ or config/), or (None, None) without git."""
    head = _git("rev-parse", "HEAD")
    if not head or not head.strip():
        return None, None
    status = _git("status", "--porcelain", "--untracked-files=all", "--", "app", "config")
    return head.strip(), (bool(status.strip()) if status is not None else None)


# Read once, when the service imports this module (at start-up): the running code is the code of that moment.
CODE_COMMIT, CODE_DIRTY = read_code_identity()


# ---------------------------------------------------------------------------
# Rows and rounds
# ---------------------------------------------------------------------------

def tie_break_rule(store: Store, ds: DatasetInfo) -> str:
    """The rule ``Store._tie_order`` applies among rows sharing a timestamp. Without a block number and a log index
    in the CSV, the on-chain order is empty and the CSV order decides."""
    on_chain = "block_number" in ds.raw_mapping and "log_index" in ds.raw_columns
    if store.legacy or not on_chain or not {"block_number", "log_index"} <= ds.columns:
        return "first_csv_row"
    return "first_swap_of_block"


def row_ref(store: Store, rel: str, row: dict[str, Any] | None) -> RowRefV3 | None:
    ds = store.dataset(rel)
    if ds is None or row is None or row.get("ts") is None:
        return None
    return RowRefV3(file=rel, timestamp=row["ts"], timestamp_column=ds.raw_mapping.get("timestamp"),
                    block_number=row.get("block_number"), log_index=row.get("log_index"),
                    tie_break_rule=tie_break_rule(store, ds))


def round_cols(ds: DatasetInfo, price_col: str) -> list[str]:
    """Columns of a Chainlink as-of read: the answer, and the phase and round that identify it."""
    return [price_col, *(c for c in ("phase", "agg_round") if c in ds.columns)]


def round_ref(ds: DatasetInfo, row: dict[str, Any], price_col: str, active_phase: bool) -> ChainlinkRoundV3:
    """The round of an as-of read; ``active_phase``: the read was restricted to the phase served at T."""
    phase, rnd = row.get("phase"), row.get("agg_round")
    gid = str((int(phase) << 64) | int(rnd)) if phase is not None and rnd is not None else None
    return ChainlinkRoundV3(
        file=ds.rel, proxy_address=ds.chainlink.get("proxy"), phase=phase, aggregator_round=rnd, global_round_id=gid,
        timestamp=row["ts"], answer=float(row[price_col]), answer_column=ds.raw_mapping.get("price_usd"),
        timestamp_column=ds.raw_mapping.get("timestamp"),
        phase_rule="active_phase_at_t" if active_phase else "all_phases",
    )


def served_phases(ds: DatasetInfo, start: datetime, end: datetime) -> list[ServedPhaseV3]:
    """The phase the proxy served over each part of ``[start, end)``: a round counts when its phase is the one served
    at its own time (``PhaseTable.served_sql``)."""
    table = ds.phases
    out: list[ServedPhaseV3] = []
    current, since = table.active_phase(start), start
    for switch, phase in sorted(zip(table.starts, table.phases)):
        if start < switch < end:
            out.append(ServedPhaseV3(phase=current, from_utc=since, to_utc=switch))
            current, since = phase, switch
    out.append(ServedPhaseV3(phase=current, from_utc=since, to_utc=end))
    return out


# ---------------------------------------------------------------------------
# S_liq (computed by app.routers.prices_v2._compute_s_liq; recorded here)
# ---------------------------------------------------------------------------

def _tvl_score(tvl: float, cfg: AppConfig) -> float | None:
    """S_TVL as ``confidence_service.compute_s_liq`` computes it."""
    mode = cfg.scoring.tvl_score_mode
    threshold = cfg.thresholds.seuil_TVL_min_usd
    if mode == "linear_to_threshold":
        return min(1.0, tvl / threshold)
    if mode == "binary_threshold":
        return 1.0 if tvl >= threshold else 0.0
    if mode == "log_memoire":
        tvl_min, tvl_ref = cfg.scoring.tvl_log_min_usd, cfg.scoring.tvl_log_ref_usd
        if tvl <= 0 or tvl_min <= 0 or tvl_ref <= tvl_min:
            return 0.0
        return max(0.0, min(1.0, math.log10(tvl / tvl_min) / math.log10(tvl_ref / tvl_min)))
    return None


def _number(row: dict[str, Any], col: str | None) -> float | None:
    if not col or row.get(col) is None:
        return None
    try:
        return float(row[col])
    except (ValueError, TypeError):
        return None


def _leg(store: Store, role: str, schema, row: dict[str, Any], cfg: AppConfig,
         eth_usd_price: float | None = None) -> SLiqLegV3:
    rel = str(schema.path)
    ds = store.dataset(rel)
    raw = ds.raw_mapping if ds is not None else {}
    tvl_raw = _number(row, schema.mapping.get("tvl_usd"))
    slippage = _number(row, schema.mapping.get("slippage"))
    converted = schema.tvl_unit == "eth" and eth_usd_price is not None
    tvl_usd = None if tvl_raw is None else (tvl_raw * eth_usd_price if converted else tvl_raw)
    s_tvl = _tvl_score(tvl_usd, cfg) if tvl_usd is not None else None
    s_slip = math.exp(-abs(slippage) / cfg.thresholds.slip_max) if slippage is not None else None
    if s_tvl is not None and s_slip is not None:
        value = math.sqrt(s_tvl * s_slip)
    else:
        value = s_tvl if s_tvl is not None else s_slip
    eth_leg = role == "eth_usd_leg"
    return SLiqLegV3(
        role=role, file=rel, row=row_ref(store, rel, row),
        tvl_column=raw.get("tvl_usd"), tvl_raw=tvl_raw, tvl_unit=schema.tvl_unit,
        eth_usd_price_for_tvl=eth_usd_price if schema.tvl_unit == "eth" else None, tvl_usd=tvl_usd,
        price_column=raw.get("price_usd") if eth_leg else None,
        price=_number(row, schema.mapping.get("price_usd")) if eth_leg else None,
        slippage_column=raw.get("slippage"), slippage=slippage, s_tvl=s_tvl, s_slip=s_slip, value=value,
    )


def s_liq_inputs(store: Store, result: PriceResult, cfg: AppConfig) -> SLiqInputsV3 | None:
    """The legs ``_compute_s_liq`` scores and how it combines them; None when it computes no S_liq (level 3, no
    source row)."""
    if result.branch_level == "3" or not (result.source_schema and result.source_row):
        return None
    eth_usd_price: float | None = None
    cross = bool(result.eth_source_schema and result.eth_source_row)
    if cross:
        eth_usd_price = _number(result.eth_source_row, result.eth_source_schema.mapping.get("price_usd"))
    token = _leg(store, "token_leg" if cross else "pool", result.source_schema, result.source_row, cfg,
                 eth_usd_price=eth_usd_price)
    legs = [token]
    if cross:
        eth = _leg(store, "eth_usd_leg", result.eth_source_schema, result.eth_source_row, cfg)
        legs.append(eth)
        if token.value is not None and eth.value is not None:
            rule, value = "geometric_mean_of_legs", math.sqrt(token.value * eth.value)
        elif token.value is not None:
            rule, value = "token_leg_only", token.value
        elif eth.value is not None:
            rule, value = "eth_usd_leg_only", eth.value
        else:
            rule, value = "unavailable", None
    else:
        rule, value = ("single_leg", token.value) if token.value is not None else ("unavailable", None)
    return SLiqInputsV3(
        rule=rule, legs=legs, tvl_score_mode=cfg.scoring.tvl_score_mode, tvl_log_min_usd=cfg.scoring.tvl_log_min_usd,
        tvl_log_ref_usd=cfg.scoring.tvl_log_ref_usd, seuil_TVL_min_usd=cfg.thresholds.seuil_TVL_min_usd,
        slip_max=cfg.thresholds.slip_max, value=value,
    )
