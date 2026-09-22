"""Capture V2 responses (CSV engine, UNCHANGED code) into tests/golden/v2_golden.jsonl. Resumable.
Run from the project root: python tests/golden/capture_v2.py
"""
import json, os, sys, time
from datetime import datetime
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT); os.chdir(ROOT)
from app.routers import prices_v2 as p2

HERE = os.path.dirname(__file__)
OUT = os.path.join(HERE, "v2_golden.jsonl")
reqs = json.load(open(os.path.join(HERE, "v2_requests.json")))
done = set()
if os.path.exists(OUT):
    done = {json.loads(l)["id"] for l in open(OUT) if l.strip()}
with open(OUT, "a") as f:
    for i, r in enumerate(reqs):
        if r["id"] in done: continue
        t0 = time.perf_counter(); rec = {"id": r["id"], "req": r}
        try:
            resp = p2.price_at_v2(asset=r["asset"], timestamp=datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")),
                                  source="auto", branch="auto", granularity=r["granularity"],
                                  include_confidence=True, include_provenance=True)
            rec["response"] = resp.model_dump(mode="json")
        except Exception as e:  # keep errors as part of the golden record
            rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        rec["elapsed_s"] = round(time.perf_counter() - t0, 2)
        f.write(json.dumps(rec) + "\n"); f.flush()
        print(f"[{i+1}/{len(reqs)}] {r['id']} {rec['elapsed_s']}s", flush=True)
