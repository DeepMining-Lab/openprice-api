"""Small load test against a running API: python tests/golden/load_v3.py http://127.0.0.1:8001 [N] [WORKERS] [SEED]
Random timestamps: pass a fresh SEED (default: time-based) or you will mostly measure the LRU cache."""
import random, statistics as st, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import threading
import httpx

_local = threading.local()

def _client():
    # one keep-alive client per thread: a new client per request would make the load generator the bottleneck
    if not hasattr(_local, 'c'): _local.c = httpx.Client(timeout=60)
    return _local.c

base = sys.argv[1]; N = int(sys.argv[2]) if len(sys.argv) > 2 else 400; W = int(sys.argv[3]) if len(sys.argv) > 3 else 8
rnd = random.Random(int(sys.argv[4]) if len(sys.argv) > 4 else time.time_ns())
lo, hi = datetime(2021, 6, 1, tzinfo=timezone.utc), datetime(2026, 8, 25, tzinfo=timezone.utc)
reqs = []
for _ in range(N):
    t = lo + timedelta(seconds=rnd.randrange(int((hi - lo).total_seconds())))
    reqs.append((rnd.choice(["ETH", "ETH", "LINK", "UNI", "AAVE", "COMP"]), t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                 rnd.choice(["raw", "raw", "minute", "hour", "day"])))

def one(r):
    a, t, g = r
    s = time.perf_counter()
    try:
        resp = _client().get(f"{base}/v3/prices/{a}/at", params={"timestamp": t, "granularity": g})
        return resp.status_code, time.perf_counter() - s, a
    except Exception as e:
        return 599, time.perf_counter() - s, a

t0 = time.perf_counter()
with ThreadPoolExecutor(W) as ex: out = list(ex.map(one, reqs))
wall = time.perf_counter() - t0
lat = sorted(x[1] for x in out); codes = {}
for c, _, _ in out: codes[c] = codes.get(c, 0) + 1
q = lambda p: lat[min(len(lat) - 1, int(p * len(lat)))] * 1000
print(f"{N} requests, {W} parallel: wall {wall:.1f}s -> {N/wall:.0f} req/s | status {codes}")
print(f"latency ms: p50={q(.5):.0f} p90={q(.9):.0f} p95={q(.95):.0f} p99={q(.99):.0f} max={lat[-1]*1000:.0f}")
for a in ["ETH", "LINK", "UNI", "AAVE", "COMP"]:
    xs = sorted(x[1] for x in out if x[2] == a); print(f"  {a:5s} p50={st.median(xs)*1000:5.0f}ms p95={xs[int(.95*len(xs))]*1000:5.0f}ms")
