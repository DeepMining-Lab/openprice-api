#!/usr/bin/env python3
"""Recompute the confidence index of OpenPrice API V3 responses from their provenance record.

    python tools/verify_confidence.py RESPONSE.json [...]
    curl -s "<api>/v3/prices/ETH/at?timestamp=2024-08-05T12:00:00Z&granularity=hour" | python tools/verify_confidence.py -
    python tools/verify_confidence.py --data ~/openprice/datasets RESPONSE.json

A file holds one response (JSON object), a series (JSON array) or one response per line (JSON lines). A response
must have been requested with confidence and provenance (the defaults of /v3/prices/{asset}/at).

Without --data, the tool reads nothing but the responses and uses only the Python standard library. From
``provenance.confidence_inputs`` it recomputes S_stat, S_liq, S_coh, S_peg, C and fragility_flag with the formulas of
the API, checks that each score applies the rule the record states (for example that the floor is used only when the
window has fewer observations than the minimum), and compares the results with the values the record states, with the
confidence block of the response, and with the prices and parameters the response lists.

With --data DIR (the dataset CSV files; needs the ``duckdb`` and ``pytz`` packages), it also checks the inputs against
the data, without the API's code or its derived store: the observation count, median and MAD of the S_stat window; each
Chainlink round (answer, time, and that it is the latest round at or before T of the phase the record names); and
each row whose TVL and slippage were scored (values, and that it is the latest swap at or before T, ties broken by the
rule the record names). The CSV files are read as the API reads them: rows with an unreadable timestamp are ignored
and duplicated events are dropped (same transaction hash and log index for a swap, same phase and round for a
Chainlink answer; the first CSV row is kept).

Exit status: 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

REL_TOL = 1e-12  # another language's exp/log/pow may differ from Python's in the last bits


def same(a: Any, b: Any) -> bool:
    numbers = (int, float)
    if isinstance(a, bool) or isinstance(b, bool) or not (isinstance(a, numbers) and isinstance(b, numbers)):
        return a == b
    return a == b or abs(a - b) <= REL_TOL * max(abs(a), abs(b))


def parse_ts(v: str) -> datetime:
    return datetime.fromisoformat(v.replace("Z", "+00:00"))


def fmt(v: Any) -> str:
    return "null" if v is None else (f"{v:.12g}" if isinstance(v, float) else str(v))


class Report:
    def __init__(self, label: str):
        self.label = label
        self.checks: list[tuple[str, bool, str]] = []
        self.note = ""

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.checks.append((name, bool(ok), detail))
        return bool(ok)

    def equal(self, name: str, got: Any, expected: Any) -> bool:
        return self.check(name, same(got, expected), f"{fmt(got)} vs {fmt(expected)}")

    @property
    def ok(self) -> bool:
        return all(ok for _, ok, _ in self.checks)


# ---------------------------------------------------------------------------
# The formulas of the API
# ---------------------------------------------------------------------------

def s_stat(i: dict, rep: Report) -> float | None:
    rule, n, min_n = i["rule"], i["n_observations"], i["min_observations"]
    if rule in ("missing_columns", "nonpositive_price"):
        return i["floor"]
    if rule == "insufficient_data_floor":
        rep.check("S_stat: floor because n < min_observations", n < min_n, f"n = {n}, min {min_n}")
        return i["floor"]
    rep.check("S_stat: n >= min_observations", n >= min_n, f"n = {n}, min {min_n}")
    spread = i["mad"] if i["method"] == "mad" else i["local_volatility"]
    if rule == "mad_zero":
        rep.check("S_stat: value 1 because the dispersion is 0", spread == 0, fmt(spread))
        return 1.0
    rep.check("S_stat: dispersion > 0", spread is not None and spread > 0, fmt(spread))
    if i["method"] == "mad":
        z = 0.6745 * abs(i["price_tested"] - i["median"]) / i["mad"]
    else:
        z = abs(math.log(i["price_tested"]) - i["median_log"]) / i["local_volatility"]
    rep.equal("S_stat: z", z, i["z_score"])
    return math.exp(-(z ** 2) / (2 * i["sigma_mad"] ** 2))


def tvl_score(tvl: float, i: dict) -> float | None:
    mode = i["tvl_score_mode"]
    if mode == "linear_to_threshold":
        return min(1.0, tvl / i["seuil_TVL_min_usd"])
    if mode == "binary_threshold":
        return 1.0 if tvl >= i["seuil_TVL_min_usd"] else 0.0
    if mode == "log_memoire":
        lo, ref = i["tvl_log_min_usd"], i["tvl_log_ref_usd"]
        if tvl <= 0 or lo <= 0 or ref <= lo:
            return 0.0
        return max(0.0, min(1.0, math.log10(tvl / lo) / math.log10(ref / lo)))
    return None


def s_liq(i: dict, rep: Report) -> float | None:
    legs = i["legs"]
    eth = next((leg for leg in legs if leg["role"] == "eth_usd_leg"), None)
    values = []
    for k, leg in enumerate(legs):
        tvl = leg["tvl_raw"]
        if k == 0 and eth is not None and leg["tvl_unit"] == "eth":
            rep.equal("S_liq: the TVL in ETH is converted with the ETH/USD leg's price", leg["eth_usd_price_for_tvl"],
                      eth["price"])
            if tvl is not None and eth["price"] is not None:
                tvl = tvl * eth["price"]
        rep.equal(f"S_liq {leg['role']}: TVL scored", tvl, leg["tvl_usd"])
        s_tvl = tvl_score(tvl, i) if tvl is not None else None
        s_slip = math.exp(-abs(leg["slippage"]) / i["slip_max"]) if leg["slippage"] is not None else None
        value = math.sqrt(s_tvl * s_slip) if s_tvl is not None and s_slip is not None else (
            s_tvl if s_tvl is not None else s_slip)
        rep.equal(f"S_liq {leg['role']}: value", value, leg["value"])
        values.append(value)
    if eth is None:
        expected, value = ("single_leg", values[0]) if values[0] is not None else ("unavailable", None)
    else:
        token, eth_value = values
        if token is not None and eth_value is not None:
            expected, value = "geometric_mean_of_legs", math.sqrt(token * eth_value)
        elif token is not None:
            expected, value = "token_leg_only", token
        elif eth_value is not None:
            expected, value = "eth_usd_leg_only", eth_value
        else:
            expected, value = "unavailable", None
    rep.check("S_liq: combination rule", i["rule"] == expected, f"{i['rule']} (expected {expected})")
    return value


def s_coh(i: dict, rep: Report) -> float | None:
    if i["rule"] != "deviation":
        return None
    answer = i["chainlink_round"]["answer"]
    delta = abs(i["price_compared"] - answer) / answer
    rep.equal("S_coh: delta", delta, i["delta"])
    return math.exp(-((delta / i["delta_tol"]) ** 2))


def s_peg(i: dict, rep: Report) -> float | None:
    if i["rule"] != "peg_deviation":
        return None
    if i.get("peg_round"):
        rep.equal("S_peg: the peg is the round's answer", i["peg"], i["peg_round"]["answer"])
    return math.exp(-((abs(i["peg"] - 1.0) / i["peg_tol"]) ** 2))


def compose(comp: dict, scores: dict[str, float | None]) -> float | None:
    w = comp["weights"]
    if comp["mode"] == "4sub":
        vals = [scores["S_stat"], scores["S_liq"], scores["S_coh"], scores["S_peg"]]
        ws = [w["w_stat"], w["w_liq"], w["w_coh"], w["w_peg"]]
        if None in vals:
            return None
        total = sum(ws)
        return math.prod(s ** (wi / total) for s, wi in zip(vals, ws))
    if None in (scores["S_stat"], scores["S_liq"], scores["S_coh"]):
        return None
    return (scores["S_stat"] ** w["w_stat"]) * (scores["S_liq"] ** w["w_liq"]) * (scores["S_coh"] ** w["w_coh"])


def verify_record(resp: dict, rep: Report) -> None:
    """Recompute every score from ``provenance.confidence_inputs`` and compare."""
    inputs = (resp.get("provenance") or {}).get("confidence_inputs")
    rec = {
        "S_stat": s_stat(inputs["s_stat"], rep) if inputs.get("s_stat") else None,
        "S_liq": s_liq(inputs["s_liq"], rep) if inputs.get("s_liq") else None,
        "S_coh": s_coh(inputs["s_coh"], rep) if inputs.get("s_coh") else None,
        "S_peg": s_peg(inputs["s_peg"], rep) if inputs.get("s_peg") else None,
    }
    for name, key in (("S_stat", "s_stat"), ("S_liq", "s_liq"), ("S_coh", "s_coh"), ("S_peg", "s_peg")):
        if inputs.get(key):
            rep.equal(f"{name} recomputed = value in the record", rec[name], inputs[key]["value"])
    comp = inputs["composition"]
    c = compose(comp, rec)
    rep.equal("C recomputed = C in the record", c, comp["C"])
    threshold = comp.get("fragility_c_threshold")
    flag = None if c is None or threshold is None else c < threshold
    rep.equal("fragility_flag recomputed = flag in the record", flag, comp["fragility_flag"])

    # The record against the rest of the response: served scores, prices and parameters.
    conf = resp.get("confidence")
    if not conf:
        rep.note = "no confidence block in the file: compared with the record only"
        return
    for name in ("S_stat", "S_liq", "S_coh"):
        rep.equal(f"{name} recomputed = served", rec[name], conf["subscores"].get(name))
    rep.equal("S_peg recomputed = served", rec["S_peg"], conf.get("S_peg"))
    rep.equal("C recomputed = served", c, conf.get("C"))
    rep.equal("fragility_flag recomputed = served", flag, conf.get("fragility_flag"))
    if inputs.get("s_stat") and inputs["s_stat"].get("price_tested") is not None:
        field = inputs["s_stat"]["price_tested_field"]
        rep.equal(f"S_stat tests the response's {field}", inputs["s_stat"]["price_tested"], resp.get(field))
    if inputs.get("s_coh"):
        field = inputs["s_coh"]["price_compared_field"]
        rep.equal(f"S_coh compares the response's {field}", inputs["s_coh"]["price_compared"], resp.get(field))
    if inputs.get("s_peg") and inputs["s_peg"]["rule"] == "peg_deviation":
        rep.equal("S_peg: the peg is the response's quote_currency_peg", inputs["s_peg"]["peg"],
                  resp.get("quote_currency_peg"))
    params = conf.get("parameters") or {}
    pairs = [("composition", "weights", None, conf.get("weights")),
             ("composition", "fragility_c_threshold", None, params.get("fragility_c_threshold")),
             ("s_coh", "delta_tol", None, params.get("coh_delta_tol_used")),
             ("s_stat", "sigma_mad", None, params.get("sigma_mad")),
             ("s_stat", "min_observations", None, params.get("min_swaps_for_stat_score")),
             ("s_stat", "floor", None, params.get("s_stat_floor")),
             ("s_liq", "slip_max", None, params.get("slip_max")),
             ("s_liq", "tvl_score_mode", None, params.get("tvl_score_mode")),
             ("s_liq", "tvl_log_min_usd", None, params.get("tvl_log_min_usd")),
             ("s_liq", "tvl_log_ref_usd", None, params.get("tvl_log_ref_usd")),
             ("s_peg", "peg_tol", None, params.get("peg_tol"))]
    for block, key, _, served in pairs:
        if inputs.get(block) and served is not None:
            got = inputs[block][key]
            ok = got == served if isinstance(got, dict) else same(got, served)
            rep.check(f"{block}.{key} = confidence parameter", ok, f"{fmt(got)} vs {fmt(served)}")


# ---------------------------------------------------------------------------
# --data: the inputs against the CSV files
# ---------------------------------------------------------------------------

_READ_OPTS = ("delim=',', quote='\"', escape='\"', header=true, auto_detect=false, max_line_size=102400, "
              "strict_mode=false, null_padding=true, ignore_errors=true")
_EVENT_KEYS = (("transaction_hash", "log_index"), ("phase", "aggregator_round"))


def _q(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


class CsvData:
    """The rows of the CSV files the checks need, read once per file (DuckDB)."""

    def __init__(self, root: str):
        import duckdb  # only --data needs it

        self.root = Path(root).expanduser().resolve()
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.needs: dict[str, dict[str, Any]] = defaultdict(lambda: {"ts_col": None, "cols": set(), "ranges": []})
        self.tables: dict[str, tuple[str, set[str]]] = {}

    def need(self, file: str, ts_col: str | None, cols: list[str | None], start: datetime, end: datetime) -> None:
        n = self.needs[file]
        n["ts_col"] = n["ts_col"] or ts_col
        n["cols"].update(c for c in cols if c)
        n["ranges"].append((start, end))

    def load(self) -> None:
        for k, (file, n) in enumerate(sorted(self.needs.items())):
            path = (self.root / file).resolve()
            if self.root not in path.parents:
                raise ValueError(f"{file} is outside {self.root}")
            with path.open(newline="") as f:
                header = next(csv.reader(f))
            ts_col = n["ts_col"] or "timestamp"
            identity = {c for key in _EVENT_KEYS for c in key} | {"block_number"}
            cols = sorted(n["cols"] | (identity & set(header)))
            missing = [c for c in [ts_col, *cols] if c not in header]
            if missing:
                raise ValueError(f"{file}: no column {missing}")
            columns = "{" + ", ".join(f"'{c.replace(chr(39), chr(39) * 2)}': 'VARCHAR'" for c in header) + "}"
            ranges = " OR ".join("(ts >= ? AND ts <= ?)" for _ in n["ranges"])
            key = next((key for key in _EVENT_KEYS if set(key) <= set(header)), None)
            dedup = (f"QUALIFY CASE WHEN {_q(key[0])} IS NULL OR {_q(key[1])} IS NULL THEN TRUE ELSE "
                     f"row_number() OVER (PARTITION BY {_q(key[0])}, {_q(key[1])} ORDER BY rn) = 1 END") if key else ""
            table = f"t{k}"
            src = str(path).replace("'", "''")
            self.con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM ("
                f" SELECT row_number() OVER () AS rn, TRY_CAST({_q(ts_col)} AS TIMESTAMPTZ) AS ts,"
                f" {', '.join(_q(c) for c in cols)} FROM read_csv('{src}', {_READ_OPTS}, columns={columns})"
                f") WHERE ts IS NOT NULL AND ({ranges}) {dedup}",
                [v for r in n["ranges"] for v in r],
            )
            self.tables[file] = (table, set(header))

    def rows(self, file: str, where: str, args: list, cols: list[str], order: str = "ts, rn") -> list[dict]:
        table, _ = self.tables[file]
        sel = ", ".join(["rn", "ts", *(_q(c) for c in cols)])
        out = self.con.execute(f"SELECT {sel} FROM {table} WHERE {where} ORDER BY {order}", args).fetchall()
        return [dict(zip(["rn", "ts", *cols], r)) for r in out]

    def double(self, v: str | None) -> float | None:
        if v is None or not str(v).strip():
            return None
        return self.con.execute("SELECT TRY_CAST(? AS DOUBLE)", [v]).fetchone()[0]


def plan_data_checks(resp: dict, data: CsvData) -> None:
    inputs = resp["provenance"]["confidence_inputs"]
    t = parse_ts(resp["timestamp"])
    st = inputs.get("s_stat")
    if st and st["rule"] != "missing_columns":
        data.need(st["reference_file"], st["reference_timestamp_column"], [st["reference_price_column"]],
                  parse_ts(st["window_start_utc"]), parse_ts(st["window_end_utc"]))
    for rnd in [(inputs.get("s_coh") or {}).get("chainlink_round"), (inputs.get("s_peg") or {}).get("peg_round")]:
        if rnd:
            data.need(rnd["file"], rnd["timestamp_column"], [rnd["answer_column"]], parse_ts(rnd["timestamp"]), t)
    for leg in (inputs.get("s_liq") or {}).get("legs", []):
        if leg.get("row"):
            data.need(leg["file"], leg["row"]["timestamp_column"],
                      [leg["tvl_column"], leg["slippage_column"], leg.get("price_column")],
                      parse_ts(leg["row"]["timestamp"]), t)


def check_data(resp: dict, data: CsvData, rep: Report) -> None:
    inputs = resp["provenance"]["confidence_inputs"]
    t = parse_ts(resp["timestamp"])

    st = inputs.get("s_stat")
    if st and st["rule"] != "missing_columns":
        start, end = parse_ts(st["window_start_utc"]), parse_ts(st["window_end_utc"])
        col = st["reference_price_column"]
        has_phase = "phase" in data.tables[st["reference_file"]][1]
        rows = data.rows(st["reference_file"], "ts >= ? AND ts < ?", [start, end], [col, "phase"] if has_phase else [col])
        if st.get("served_phases"):
            spans = [(parse_ts(s["from_utc"]), parse_ts(s["to_utc"]), s["phase"]) for s in st["served_phases"]]
            rows = [r for r in rows if any(a <= r["ts"] < b and str(p) == str(r.get("phase")) for a, b, p in spans)]
        if st.get("window_row_limit"):
            rows = rows[: st["window_row_limit"]]  # legacy: the first rows of the window, then empty prices dropped
        prices = sorted(p for p in (data.double(r[col]) for r in rows) if p is not None)
        n = len(prices)
        rep.equal("data: S_stat window, number of observations", n, st["n_observations"])
        if st["method"] == "mad" and n:
            med = prices[n // 2]
            rep.equal("data: S_stat window, median", med, st["median"])
            rep.equal("data: S_stat window, MAD", sorted(abs(p - med) for p in prices)[n // 2], st["mad"])

    for name, rnd in [("S_coh", (inputs.get("s_coh") or {}).get("chainlink_round")),
                      ("S_peg", (inputs.get("s_peg") or {}).get("peg_round"))]:
        if not rnd:
            continue
        rule = rnd.get("phase_rule")
        header = data.tables[rnd["file"]][1]
        rounds = rnd["phase"] is not None and rnd["aggregator_round"] is not None and {"phase", "aggregator_round"} \
            <= header
        if rounds:
            key, rows = (f"{rnd['phase']}/{rnd['aggregator_round']}",
                         data.rows(rnd["file"], "phase = ? AND aggregator_round = ?",
                                   [str(rnd["phase"]), str(rnd["aggregator_round"])], [rnd["answer_column"]]))
        else:  # a feed without phase and round columns: the first CSV row of the round's second
            key, rows = rnd["timestamp"], data.rows(rnd["file"], "ts = ?", [parse_ts(rnd["timestamp"])],
                                                    [rnd["answer_column"]], order="rn")[:1]
        if not rep.check(f"data: {name} round {key} is in the file", len(rows) == 1):
            continue
        rep.equal(f"data: {name} round answer", data.double(rows[0][rnd["answer_column"]]), rnd["answer"])
        rep.equal(f"data: {name} round time", rows[0]["ts"], parse_ts(rnd["timestamp"]))
        # The latest round at or before T: of the round's phase (the phase served at T), or of every phase.
        same_phase = rounds and rule != "all_phases"
        where = "ts <= ?" + (" AND phase = ?" if same_phase else "")
        order = "ts DESC, TRY_CAST(aggregator_round AS BIGINT) DESC, rn" if same_phase else "ts DESC, rn"
        latest = data.rows(rnd["file"], where, [t, *([str(rnd["phase"])] if same_phase else [])], [], order=order)[:1]
        rep.check(f"data: {name} round is the latest at or before T", bool(latest) and latest[0]["rn"] == rows[0]["rn"])

    for leg in (inputs.get("s_liq") or {}).get("legs", []):
        row = leg.get("row")
        if not row:
            continue
        cols = [c for c in (leg["tvl_column"], leg["slippage_column"], leg.get("price_column")) if c]
        header = data.tables[leg["file"]][1]
        on_chain = row["block_number"] is not None and row["log_index"] is not None and \
            {"block_number", "log_index"} <= header
        if on_chain:
            key, found = (f"{row['block_number']}/{row['log_index']}",
                          data.rows(leg["file"],
                                    "TRY_CAST(block_number AS BIGINT) = ? AND TRY_CAST(log_index AS BIGINT) = ?",
                                    [row["block_number"], row["log_index"]], cols))
        else:  # no block or log index in the file: the first CSV row of the row's second
            key, found = row["timestamp"], data.rows(leg["file"], "ts = ?", [parse_ts(row["timestamp"])], cols,
                                                     order="rn")[:1]
        if not rep.check(f"data: S_liq {leg['role']} row {key} is in the file", len(found) == 1):
            continue
        r = found[0]
        rep.equal(f"data: S_liq {leg['role']} row time", r["ts"], parse_ts(row["timestamp"]))
        if leg["tvl_column"]:
            rep.equal(f"data: S_liq {leg['role']} TVL", data.double(r[leg["tvl_column"]]), leg["tvl_raw"])
        if leg["slippage_column"]:
            rep.equal(f"data: S_liq {leg['role']} slippage", data.double(r[leg["slippage_column"]]), leg["slippage"])
        if leg.get("price_column"):
            rep.equal(f"data: S_liq {leg['role']} ETH/USD price", data.double(r[leg["price_column"]]), leg["price"])
        order = ("ts DESC, TRY_CAST(block_number AS BIGINT), TRY_CAST(log_index AS BIGINT), rn"
                 if row["tie_break_rule"] == "first_swap_of_block" and on_chain else "ts DESC, rn")
        latest = data.rows(leg["file"], "ts <= ?", [t], [], order=order)[:1]
        rep.check(f"data: S_liq {leg['role']} row is the latest swap at or before T",
                  bool(latest) and latest[0]["rn"] == r["rn"])


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def load_responses(paths: list[str]) -> list[dict]:
    out: list[dict] = []
    for p in paths:
        text = sys.stdin.read() if p == "-" else Path(p).read_text()
        try:
            docs = [json.loads(text)]
        except json.JSONDecodeError:
            docs = [json.loads(line) for line in text.splitlines() if line.strip()]
        for doc in docs:
            for item in doc if isinstance(doc, list) else [doc]:
                out.append(item.get("response", item) if isinstance(item, dict) else item)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("files", nargs="+", help="response files (JSON, JSON array or JSON lines); '-' reads stdin")
    ap.add_argument("--data", metavar="DIR", help="also check the inputs against the dataset CSV files in DIR")
    ap.add_argument("-v", "--verbose", action="store_true", help="list every check")
    args = ap.parse_args(argv)

    responses = load_responses(args.files)
    reports: list[Report] = []
    with_inputs: list[tuple[dict, Report]] = []
    for resp in responses:
        rep = Report(f"{resp.get('asset')} {resp.get('timestamp')} {resp.get('granularity', 'raw')}")
        reports.append(rep)
        prov = resp.get("provenance")
        if not prov:
            rep.note = "no provenance block (request it with include_provenance=true)"
            continue
        if not prov.get("confidence_inputs"):
            rep.note = (f"no index to recompute (level {resp.get('branch_level')})" if resp.get("confidence")
                        or resp.get("branch_level") in ("3", "4") else
                        "no confidence_inputs (request confidence too, or the API predates them)")
            continue
        verify_record(resp, rep)
        with_inputs.append((resp, rep))

    if args.data and with_inputs:
        data = CsvData(args.data)
        for resp, _ in with_inputs:
            plan_data_checks(resp, data)
        data.load()
        for resp, rep in with_inputs:
            check_data(resp, data, rep)

    failed = 0
    for rep in reports:
        if not rep.checks:
            print(f"-  {rep.label}: {rep.note}")
            continue
        bad = [c for c in rep.checks if not c[1]]
        failed += bool(bad)
        c = next((d for n, _, d in rep.checks if n == "C recomputed = C in the record"), "")
        print(f"{'OK' if not bad else 'FAIL'}  {rep.label}: {len(rep.checks) - len(bad)}/{len(rep.checks)} checks, "
              f"C {c.split(' vs ')[0] if c else '?'}" + (f" ({rep.note})" if rep.note else ""))
        for name, ok, detail in rep.checks if args.verbose else bad:
            print(f"     {'ok ' if ok else 'BAD'} {name}" + (f": {detail}" if detail else ""))
    checked = sum(1 for r in reports if r.checks)
    print(f"{checked} response(s) checked, {failed} with a failed check"
          + (f", {len(reports) - checked} without an index" if len(reports) > checked else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
