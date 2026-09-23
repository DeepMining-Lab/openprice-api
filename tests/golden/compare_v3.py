"""Compare API V3 against the V2 golden master (tests/golden/v2_golden.jsonl).

  python tests/golden/compare_v3.py --legacy   # parity proof: V3 replays V2's 10 000-row truncation
  python tests/golden/compare_v3.py            # V3 as shipped (truncations fixed): lists what changes

Exit code 0 only if every non-excused record is identical (floats: rel 1e-9).
Records whose window can contain one of the 110 duplicated ETH swaps (removed in the
Parquet store, on purpose) are reported separately as "dup-zone". The additive V3 diagnostics
(warning codes in V3_DIAGNOSTIC_CODES, provenance.rejected_candidates) are stripped before the
comparison: they never change a number and V2 has no equivalent.
"""
import argparse, json, math, os, sys
from collections import Counter
from datetime import datetime, timedelta, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT); os.chdir(ROOT)
from app.config import get_config
from app.v3.service import get_service, strip_v3_diagnostics

# Days on which duplicated (tx_hash, log_index) rows exist in eth_usdc_uniswap_v3_005.csv.
DUP_DAYS = ["2023-12-05", "2026-05-20", "2026-05-22", "2026-05-23"]


def in_dup_zone(asset, ts):
    if asset != "ETH":
        return False
    t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    for d in DUP_DAYS:
        day = datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
        if day - timedelta(days=1) <= t <= day + timedelta(days=9):
            return True
    return False


def diff(a, b, path=""):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a: yield (f"{path}.{k}", "<absent>", b[k])
            elif k not in b: yield (f"{path}.{k}", a[k], "<absent>")
            else: yield from diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b): yield (f"{path}[len]", len(a), len(b))
        for i, (x, y) in enumerate(zip(a, b)): yield from diff(x, y, f"{path}[{i}]")
    elif isinstance(a, float) and isinstance(b, (int, float)) and not isinstance(b, bool):
        if not math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12): yield (path, a, b)
    elif a != b:
        yield (path, a, b)


def compare(legacy: bool, limit: int | None = None, golden: str | None = None):
    """Run every golden record through V3. Returns (counts, differing_fields, bad) with
    bad = [(record_id, [(path, v2_value, v3_value), ...])] for non-excused differences."""
    cfg = get_config(); cfg.v3.legacy_truncation = legacy
    svc = get_service()
    golden = golden or os.path.join(os.path.dirname(os.path.abspath(__file__)), "v2_golden.jsonl")
    recs = [json.loads(l) for l in open(golden) if l.strip()]
    if limit: recs = recs[:limit]
    counts = Counter(total=len(recs)); fields = Counter(); bad = []
    for r in recs:
        if "response" not in r: counts["skipped"] += 1; continue
        q = r["req"]
        got, _ = svc.price_at(q["asset"], datetime.fromisoformat(q["timestamp"].replace("Z", "+00:00")),
                              granularity=q["granularity"])
        d = list(diff(r["response"], strip_v3_diagnostics(got.model_dump(mode="json"))))
        if not d: counts["identical"] += 1; continue
        if in_dup_zone(q["asset"], q["timestamp"]): counts["dup_zone"] += 1; continue
        counts["other"] += 1; bad.append((r["id"], d))
        for p, _, _ in d: fields[p.split("[")[0] if "warnings" in p else p] += 1
    return counts, fields, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--legacy", action="store_true")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--show", type=int, default=12)
    args = ap.parse_args()
    counts, fields, bad = compare(args.legacy, args.limit)
    mode = "LEGACY (parity proof)" if args.legacy else "FIXED (as shipped)"
    print(f"mode={mode}: {counts['total']} golden records | identical={counts['identical']} | "
          f"dup-zone diffs={counts['dup_zone']} | other diffs={counts['other']} | skipped={counts['skipped']}")
    if fields:
        print("most frequent differing fields:")
        for p, n in fields.most_common(15): print(f"  {n:4d}  {p}")
    for rid, d in bad[:args.show]:
        print(f"\n{rid}")
        for p, x, y in d[:6]: print(f"   {p}: V2={x!r}  V3={y!r}")
    sys.exit(0 if not bad else 1)


if __name__ == "__main__":
    main()
