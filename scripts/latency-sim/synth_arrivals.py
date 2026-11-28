#!/usr/bin/env python3
"""Deterministic SYNTHETIC arrival trace for the T134 capacity sweep. Contains no production data.

Only aggregate SHAPE parameters are taken from the observed peak hour (2026-11-28, sessions-prod): the
number of arrivals per tenant rank, sessions per tenant rank, a 3,600 s window, and the within-session
gap quantiles (p50 2.7 s, p90 103 s). No identifiers, timestamps or per-session data are used. Tenants and
sessions are synthetic integers/strings. Output: [[arrival_ms, tenant_rank, session_id, arrival_ms], ...]
usage: synth_arrivals.py OUT.json [seed]
"""
import json, math, random, sys

ARRIVALS_PER_TENANT = [4485, 86, 71, 18, 17, 11, 7, 4, 4, 3, 3, 3] + [2] * 15 + [1] * 9
SESSIONS_PER_TENANT = [440, 12, 11, 9, 5, 4, 2, 2, 2, 2, 2, 2, 2] + [1] * 23
WINDOW_MS = 3_600_000
GAP_P50_S, GAP_P90_S = 2.672, 103.175

def main():
    out = sys.argv[1]; seed = int(sys.argv[2]) if len(sys.argv) > 2 else 20261128
    rng = random.Random(seed)
    mu = math.log(GAP_P50_S); sigma = (math.log(GAP_P90_S) - mu) / 1.2816
    rows = []
    for rank, (n, k) in enumerate(zip(ARRIVALS_PER_TENANT, SESSIONS_PER_TENANT), start=1):
        k = max(1, min(k, n))
        # Heavy-tailed split of the tenant's arrivals over its sessions (each session >= 1 arrival).
        w = [rng.paretovariate(1.1) for _ in range(k)]
        share = [1] * k
        for _ in range(n - k):
            share[rng.choices(range(k), weights=w)[0]] += 1
        for j, m in enumerate(share):
            sid = f"synthetic-t{rank}-s{j}"
            t = rng.uniform(0, WINDOW_MS)
            for _ in range(m):
                rows.append([int(t) % WINDOW_MS, rank, sid, int(t) % WINDOW_MS])
                t += rng.lognormvariate(mu, sigma) * 1000
    rows.sort()
    json.dump(rows, open(out, "w"))
    print(f"synthetic arrivals {len(rows)}, tenants {len(ARRIVALS_PER_TENANT)}, sessions {len({r[2] for r in rows})}, seed {seed}")

if __name__ == "__main__":
    main()
