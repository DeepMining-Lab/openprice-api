"""Capture API V3 responses on the real Parquet store into a golden file (same requests as the V2 golden master).

  python tests/golden/capture_v3.py --out tests/golden/v3_golden.jsonl            # V3 as configured
  python tests/golden/capture_v3.py --out ... --no-phase-filter                     # Chainlink rounds of every phase

Run from the project root. The store is only read. ``OPENPRICE_CONFIG`` selects another config file.
"""
import argparse, json, os, sys, time
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT); os.chdir(ROOT)
from app.config import get_config
from app.v3.service import get_service

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--no-phase-filter", action="store_true", help="set v3.chainlink_active_phase_only = false")
    args = ap.parse_args()
    cfg = get_config()
    if args.no_phase_filter and hasattr(cfg.v3, "chainlink_active_phase_only"):
        cfg.v3.chainlink_active_phase_only = False
    svc = get_service()
    reqs = json.load(open(os.path.join(HERE, "v2_requests.json")))
    with open(args.out, "w") as f:
        for i, r in enumerate(reqs):
            t0 = time.perf_counter()
            rec = {"id": r["id"], "req": r}
            try:
                resp, _ = svc.price_at(r["asset"], datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")),
                                       granularity=r["granularity"])
                rec["response"] = resp.model_dump(mode="json")
            except Exception as e:  # errors are part of the golden record
                rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
            rec["elapsed_s"] = round(time.perf_counter() - t0, 3)
            f.write(json.dumps(rec) + "\n")
    print(f"{len(reqs)} records -> {args.out} (dataset_version {svc.store.version})")


if __name__ == "__main__":
    main()
