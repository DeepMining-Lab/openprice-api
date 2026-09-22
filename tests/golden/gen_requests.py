"""Generate the FIXED request list used for the V2 golden master (run once; output is committed data).

Uses a scratch Parquet copy only to pick timestamps that coincide with real swaps (tie cases).
Usage: python tests/golden/gen_requests.py <dir with canonical parquet files named <dir>__<file>.parquet>
"""
import json, random, sys
from datetime import datetime, timedelta, timezone
import duckdb

PQ_DIR = sys.argv[1]
ASSETS = ["ETH", "LINK", "UNI", "AAVE", "COMP"]
KEY_DATES = ["2020-06-20", "2021-02-03", "2021-06-01", "2022-05-12", "2023-03-11",
             "2023-12-05", "2024-08-05", "2025-06-01", "2026-05-20", "2026-08-25"]
POOL = {"ETH": "eth__eth_usdc_uniswap_v3_005", "LINK": "link__link_usdc_uniswap_v3_03",
        "UNI": "uni__uni_usdc_uniswap_v3_03", "AAVE": "aave__aave_usdc_uniswap_v3_03",
        "COMP": "comp__comp_weth_uniswap_v3_03"}
rnd = random.Random(20260921)
fmt = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
reqs, seen = [], set()

def add(asset, ts, gran, tag):
    rid = f"{asset}|{ts}|{gran}"
    if rid not in seen:
        seen.add(rid); reqs.append({"id": rid, "asset": asset, "timestamp": ts, "granularity": gran, "tag": tag})

con = duckdb.connect(); con.execute("SET TimeZone='UTC'")
lo, hi = datetime(2021, 5, 10, tzinfo=timezone.utc), datetime(2026, 8, 25, tzinfo=timezone.utc)
for a in ASSETS:
    for d in KEY_DATES:
        for g in ("raw", "hour", "day"):
            add(a, f"{d}T12:00:00Z", g, "key")
    for _ in range(15):
        t = lo + timedelta(seconds=rnd.randrange(int((hi - lo).total_seconds())))
        add(a, fmt(t), rnd.choice(["raw", "raw", "minute", "hour", "day"]), "random")
    n = 6 if a in ("ETH", "LINK") else 3
    rows = con.execute(f"SELECT ts FROM read_parquet('{PQ_DIR}/{POOL[a]}.parquet') WHERE ts BETWEEN ? AND ? USING SAMPLE {n} ROWS (reservoir, {rnd.randrange(10**6)})", [lo, hi]).fetchall()
    for (t,) in rows:
        add(a, fmt(t), "raw", "on_swap")
import os; json.dump(reqs, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "v2_requests.json"), "w"), indent=0)
print(len(reqs), "requests")
