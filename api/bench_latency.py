"""Warm-path latency benchmark for the scorer.

Warmup requests are discarded so the reported percentiles reflect the warm path
rather than a one-off model load, matching how cold start is treated elsewhere.
A single reused connection avoids per-request connection setup, which on windows
localhost adds a fixed multi-second resolution stall unrelated to the service.
"""
import sys
import time

import requests

URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000/score"
CUSTOMER = sys.argv[2] if len(sys.argv) > 2 else "CU00000"
N, WARMUP = 500, 20

payload = {"customer_id": CUSTOMER, "amount": 812.44, "channel": "fednow",
           "txn_ts": "2026-07-02T14:23:00", "transaction_id": "bench"}


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


session = requests.Session()  # reused connection - measures warm service latency not tcp setup
lat = []
for i in range(N + WARMUP):
    t0 = time.perf_counter()
    resp = session.post(URL, json=payload, timeout=5)
    dt = (time.perf_counter() - t0) * 1000
    resp.raise_for_status()
    if i >= WARMUP:
        lat.append(dt)

print(f"n={len(lat)}  p50={pct(lat,50):.2f}ms  p95={pct(lat,95):.2f}ms  "
      f"p99={pct(lat,99):.2f}ms  max={max(lat):.2f}ms")
print("ACCEPTANCE:", "PASS" if pct(lat, 99) < 100 else "FAIL", "(P99 < 100ms)")