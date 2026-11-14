"""``provenance.confidence_inputs``: what a V3 response records so that a third party can recompute its confidence index
from the record alone (``tools/verify_confidence.py``), and the parameters and code that produced it
(``config_sha256``, ``code_commit``, ``code_dirty``). All on small synthetic CSV files."""

from __future__ import annotations

import copy
import importlib.util
import json
import re
from datetime import timedelta
from pathlib import Path

import pytest

from app.main import app  # imported first: it calls load_config() at import time
from app.v3 import record
from app.v3 import service as service_mod
from app.v3 import store as store_mod
from app.v3 import sync as sync_mod
from tests import test_v3 as base
from tests.test_v3 import v3cfg  # noqa: F401  (fixture)

ROOT = Path(__file__).resolve().parent.parent
T0 = base.T0


def _verifier():
    spec = importlib.util.spec_from_file_location("verify_confidence", ROOT / "tools" / "verify_confidence.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _verify(resp: dict, data: str | None = None):
    """Run the third-party checks on one response (JSON dict); returns the report."""
    v = _verifier()
    rep = v.Report("test")
    v.verify_record(resp, rep)
    if data:
        csv_data = v.CsvData(data)
        v.plan_data_checks(resp, csv_data)
        csv_data.load()
        v.check_data(resp, csv_data, rep)
    return rep


def _failed(rep) -> list:
    return [c for c in rep.checks if not c[1]]


def _pool(cfg, rows: list[list], header: list[str] = base.POOL_HEADER) -> None:
    base._write(Path(cfg.paths.datasets_root) / base.POOL_REL, header, rows)


class TestRecordedInputs:
    def test_a_direct_price_records_every_input_and_recomputes_exactly(self, v3cfg):
        base._simple_link(v3cfg)
        T = T0 + timedelta(minutes=10)
        r, _ = service_mod.Service(v3cfg).price_at("LINK", T)
        d = r.model_dump(mode="json")
        ci = r.provenance.confidence_inputs
        assert r.branch_level == "0a" and r.confidence.C is not None

        st = ci.s_stat
        assert (st.reference_file, st.reference_kind, st.rule, st.method) == (base.POOL_REL, "source_pool",
                                                                              "mad_z_score", "mad")
        assert (st.reference_timestamp_column, st.reference_price_column) == ("timestamp", "price_usdc_per_link")
        assert (st.window_start_utc, st.window_end_utc, st.served_phases) == (T - timedelta(days=7), T, None)
        # The window statistics, recomputed from the CSV rows: [T - 7 d, T) holds rows 0..9 (row 10 is at T).
        prices = sorted(row[1] for row in base._pool_rows(20)[:10])
        med = prices[len(prices) // 2]
        assert (st.n_observations, st.median, st.mad) == (10, med, sorted(abs(p - med) for p in prices)[5])
        assert st.price_tested == r.price_raw_in_quote and st.price_tested_field == "price_raw_in_quote"

        leg, = ci.s_liq.legs
        assert (leg.role, leg.file, leg.tvl_column, leg.slippage_column) == ("pool", base.POOL_REL,
                                                                             "pool_tvl_at_block", "slip_1k")
        assert (leg.tvl_raw, leg.tvl_unit, leg.tvl_usd, leg.slippage) == (2_000_000.0, "usd", 2_000_000.0, 0.001)
        # The row scored is the as-of row: for a raw price, the swap the price comes from.
        ev = r.provenance.source_event
        assert (leg.row.block_number, leg.row.log_index, leg.row.timestamp) == (ev.block_number, ev.log_index,
                                                                                ev.timestamp)
        assert leg.row.tie_break_rule == "first_swap_of_block" and ci.s_liq.rule == "single_leg"

        coh = ci.s_coh
        assert (coh.rule, coh.chainlink_round.answer, coh.chainlink_round.timestamp) == ("deviation", 14.0, T0)
        assert coh.chainlink_round.global_round_id is None  # no phase/round columns in this feed
        assert coh.price_compared == d["price_usd"] and coh.price_compared_field == "price_raw_in_quote"
        assert ci.s_peg.rule == "not_applicable" and ci.s_peg.value is None  # no peg feed here

        comp = ci.composition
        assert (comp.mode, comp.C, comp.weights) == ("3sub", r.confidence.C, r.confidence.weights)

        rep = _verify(d)
        assert rep.checks and not _failed(rep)
        # Python recomputes the very same bits.
        v = _verifier()
        scores = {"S_stat": v.s_stat(d["provenance"]["confidence_inputs"]["s_stat"], v.Report("x")),
                  "S_liq": v.s_liq(d["provenance"]["confidence_inputs"]["s_liq"], v.Report("x")),
                  "S_coh": v.s_coh(d["provenance"]["confidence_inputs"]["s_coh"], v.Report("x")), "S_peg": None}
        assert scores == {**d["confidence"]["subscores"], "S_peg": None}
        assert v.compose(d["provenance"]["confidence_inputs"]["composition"], scores) == d["confidence"]["C"]

    def test_a_windowed_cross_rate_records_both_liquidity_legs(self, v3cfg):
        root = Path(v3cfg.paths.datasets_root)
        T = T0 + timedelta(hours=3)
        base._eth_leg(v3cfg, range(240))
        rows = [[base._iso(T - timedelta(hours=2)), 0.0060, 10.0, 1.5e6, 0.001]]
        rows += [[base._iso(T + timedelta(minutes=m)), 0.0065, 10.0, 1.5e6, 0.001] for m in (5, 10, 15, 20)]
        base._write(root / base.TOKEN_LEG_REL, base.TOKEN_LEG_HEADER, rows)
        base._link_chainlink(v3cfg, price=14.1, at=T - timedelta(minutes=5))
        sync_mod.run_sync(v3cfg)
        r, _ = service_mod.Service(v3cfg).price_at("LINK", T, granularity="hour")
        assert (r.branch_level, r.swap_count) == ("0b", 4)
        ci = r.provenance.confidence_inputs

        # S_stat of a cross-rate is measured against the Chainlink feed: one round in 7 days, so the floor.
        st = ci.s_stat
        assert (st.reference_kind, st.reference_file, st.rule) == ("chainlink_feed", "link/chainlink_link_usd.csv",
                                                                   "insufficient_data_floor")
        assert (st.n_observations, st.value) == (1, v3cfg.thresholds.s_stat_floor)

        token, eth = ci.s_liq.legs
        assert (token.role, eth.role, ci.s_liq.rule) == ("token_leg", "eth_usd_leg", "geometric_mean_of_legs")
        # The scored rows are the as-of rows at T, not the window: the token pool's last swap before T (2 h old)
        # and the ETH/USD row at T (whose price is not the VWMP of the leg but converts the TVL in ETH).
        assert token.row.timestamp == T - timedelta(hours=2) and eth.row.timestamp == T
        assert (token.tvl_unit, token.tvl_raw, token.eth_usd_price_for_tvl) == ("eth", 1.5e6, 2200.0)
        assert token.tvl_usd == 1.5e6 * 2200.0 and (eth.price_column, eth.price) == ("price_usdc_per_eth", 2200.0)
        assert eth.row.tie_break_rule == "first_csv_row"  # no block or log index in this file
        assert r.provenance.eth_usd_leg_event is None  # the price used the ETH/USD VWMP: no single event

        assert ci.s_coh.chainlink_round.answer == 14.1
        rep = _verify(r.model_dump(mode="json"))
        assert rep.checks and not _failed(rep)

    def test_zero_dispersion_and_missing_liquidity_are_recorded(self, v3cfg):
        header = ["timestamp", "price_usdc_per_link", "volume_usdc", "block_number", "transaction_hash", "log_index"]
        _pool(v3cfg, [[base._iso(T0 + timedelta(minutes=i)), 14.0, 5000.0, 1000 + i, f"0x{i:064x}", 0]
                      for i in range(20)], header)
        base._link_chainlink(v3cfg, price=14.0)
        sync_mod.run_sync(v3cfg)
        r, _ = service_mod.Service(v3cfg).price_at("LINK", T0 + timedelta(minutes=10))
        ci = r.provenance.confidence_inputs
        assert (ci.s_stat.rule, ci.s_stat.mad, ci.s_stat.value) == ("mad_zero", 0.0, 1.0)
        leg, = ci.s_liq.legs
        assert (leg.tvl_column, leg.slippage_column, leg.value) == (None, None, None)
        assert (ci.s_liq.rule, ci.s_liq.value, r.confidence.subscores["S_liq"], ci.composition.C) == (
            "unavailable", None, None, None)
        rep = _verify(r.model_dump(mode="json"))
        assert rep.checks and not _failed(rep)

    def test_no_record_without_an_index(self, v3cfg):
        base._link_chainlink(v3cfg)  # Chainlink only: level 3
        sync_mod.run_sync(v3cfg)
        svc = service_mod.Service(v3cfg)
        r, _ = svc.price_at("LINK", T0 + timedelta(minutes=5))
        assert r.branch_level == "3" and r.provenance.confidence_inputs is None
        assert r.provenance.config_sha256 is not None  # the identity of the answer is still recorded
        base._simple_link(v3cfg)
        svc = service_mod.Service(v3cfg)
        r, _ = svc.price_at("LINK", T0 + timedelta(minutes=10), include_confidence=False)
        assert r.branch_level == "0a" and r.provenance.confidence_inputs is None
        r, _ = svc.price_at("LINK", T0 + timedelta(minutes=10), include_provenance=False)
        assert r.provenance is None and r.confidence.C is not None

    def test_the_verifier_reports_a_record_that_does_not_add_up(self, v3cfg):
        base._simple_link(v3cfg)
        r, _ = service_mod.Service(v3cfg).price_at("LINK", T0 + timedelta(minutes=10))
        d = r.model_dump(mode="json")
        for path, value in [(("s_stat", "median"), 15.0), (("s_coh", "chainlink_round", "answer"), 14.5),
                            (("s_liq", "legs", 0, "tvl_raw"), 20_000.0), (("s_stat", "n_observations"), 2)]:
            bad = copy.deepcopy(d)
            node = bad["provenance"]["confidence_inputs"]
            for key in path[:-1]:
                node = node[key]
            node[path[-1]] = value
            assert _failed(_verify(bad)), path

    def test_data_mode_checks_the_inputs_against_the_csv_files(self, v3cfg):
        base._simple_link(v3cfg)
        T = T0 + timedelta(minutes=10)
        d = service_mod.Service(v3cfg).price_at("LINK", T)[0].model_dump(mode="json")
        rep = _verify(d, data=v3cfg.paths.datasets_root)
        names = [n for n, _, _ in rep.checks if n.startswith("data:")]
        assert len(names) >= 8 and not _failed(rep)
        # A row that is in the file but is not the latest swap at or before T is reported.
        bad = copy.deepcopy(d)
        row = bad["provenance"]["confidence_inputs"]["s_liq"]["legs"][0]["row"]
        row["block_number"], row["timestamp"] = 1005, base._iso(T0 + timedelta(minutes=5)).replace(" ", "T")
        failed = [n for n, _, _ in _failed(_verify(bad, data=v3cfg.paths.datasets_root))]
        assert "data: S_liq pool row is the latest swap at or before T" in failed


class TestIdentity:
    def test_parameters_and_code_are_named_in_every_provenance(self, v3cfg):
        from fastapi.testclient import TestClient
        base._simple_link(v3cfg)
        store_mod.reset_store(); service_mod.reset_service()
        with TestClient(app) as client:
            cfg = client.get("/v3/config").json()
            ready = client.get("/v3/ready").json()
            r = client.get("/v3/prices/LINK/at", params={"timestamp": "2024-01-01T00:10:00Z"}).json()
        store_mod.reset_store(); service_mod.reset_service()
        prov = r["provenance"]
        assert prov["config_sha256"] == cfg["config_sha256"] == record.config_sha256(v3cfg)
        assert (prov["code_commit"], prov["code_dirty"]) == (ready["code_commit"], ready["code_dirty"])
        assert prov["code_commit"] is None or re.fullmatch(r"[0-9a-f]{40}", prov["code_commit"])
        stripped = service_mod.strip_v3_diagnostics(r)
        assert not {"confidence_inputs", "config_sha256", "code_commit", "code_dirty"} & stripped["provenance"].keys()

    def test_a_parameter_change_changes_the_fingerprint(self, v3cfg):
        before = record.config_sha256(v3cfg)
        v3cfg.thresholds.sigma_mad = 3.0
        assert record.config_sha256(v3cfg) != before

    def test_code_identity_reads_the_commit_and_uncommitted_changes(self, monkeypatch):
        calls = []

        def fake_git(*args):
            calls.append(args)
            return {"rev-parse": "a" * 40 + "\n", "status": " M app/v3/service.py\n"}[args[0]]

        monkeypatch.setattr(record, "_git", fake_git)
        assert record.read_code_identity() == ("a" * 40, True)
        assert calls[1][-3:] == ("--", "app", "config")  # only the code and the configuration count
        monkeypatch.setattr(record, "_git", lambda *a: {"rev-parse": "b" * 40, "status": ""}[a[0]])
        assert record.read_code_identity() == ("b" * 40, False)
        monkeypatch.setattr(record, "_git", lambda *a: None)  # no git, or not a repository
        assert record.read_code_identity() == (None, None)

    def test_a_round_id_is_the_proxy_round_id(self):
        # phase 6, aggregator round 20 846 of the ETH/USD feed: the id its proxy returns for that round
        assert str((6 << 64) | 20846) == "110680464442257330542"
        assert json.loads(json.dumps({"id": "110680464442257330542"}))["id"] == "110680464442257330542"
