#!/usr/bin/env python3
"""T134 sensitivity sweep (OFFLINE, SCENARIO OUTPUT, NOT A PRODUCTION ESTIMATE).

Same 4,751-arrival anonymised trace and per-session coalescing as replay.py. Adds, per executed pass:
  - a PROOF READ of ASSUMED cost c seconds on the same slot after the search (durable-coverage check);
  - with ASSUMED probability f, durable coverage is ABSENT/FAILED for that pass. Its arrivals are
    then NOT covered by it and need a fallback pass:
      immediate = re-queued at once (T126-style immediate covering search),
      hold      = re-queued 300 s later (T124-style bounded hold, then one inline pass).
Latency is scored to the COVERING pass (a failed pass never counts):
  start miss  = arrival -> covering pass START  > 7 s   (pickup budget)
  finish miss = arrival -> covering pass FINISH > 19 s
Inputs f, c, useful-pass cost profile and slot counts are ASSUMED; nothing here is calibrated
against authorized reads. Usage: sweep.py ../replay/arrivals.json
"""
import heapq, json, math, random, sys
from collections import defaultdict
from replay import POINTER, TODAY, lognorm, mix, pct

HOLD_S = 300.0

def simulate(arrivals, slots, costs, f, proof_s, mode, seed):
    rng = random.Random(seed * 7919 + 13)
    cost_of = {(s, a): c for (a, t, s), c in zip(arrivals, costs)}
    evs = [(a, 0, a, t, s) for a, t, s in arrivals]  # (time, kind 0=arrival/1=requeue, orig, tenant, session)
    heapq.heapify(evs)
    free = slots; ready = []; queued = set(); pending = defaultdict(list); inflight = set(); fins = []
    covered = []; seq = 0; passes = 0; fallbacks = 0
    while evs or fins:
        na = evs[0][0] if evs else math.inf
        nf = fins[0][0] if fins else math.inf
        if na <= nf:
            now, _, orig, t, s = heapq.heappop(evs)
            pending[s].append((orig, t))
            if s not in inflight and s not in queued:
                heapq.heappush(ready, (min(o for o, _ in pending[s]), seq, s)); queued.add(s); seq += 1
        else:
            now, s, batch, start = heapq.heappop(fins)
            free += 1; inflight.discard(s)
            if rng.random() < f:
                fallbacks += 1
                at = now if mode == "immediate" else now + HOLD_S * 1000
                for orig, t in batch:
                    heapq.heappush(evs, (at, 1, orig, t, s))
            else:
                for orig, t in batch:
                    covered.append((t, start - orig, now - orig))
            if pending.get(s) and s not in queued:
                heapq.heappush(ready, (min(o for o, _ in pending[s]), seq, s)); queued.add(s); seq += 1
        while free and ready:
            _, _, s = heapq.heappop(ready); queued.discard(s)
            batch = pending.pop(s, [])
            if not batch: continue
            newest = max(o for o, _ in batch)
            dur = cost_of.get((s, newest)) or min(300.0, POINTER(rng)) * 1000
            dur += proof_s * 1000
            passes += 1; free -= 1; inflight.add(s)
            heapq.heappush(fins, (now + dur, s, batch, now))
    return covered, passes, fallbacks

def main():
    raw = json.load(open(sys.argv[1])); seen = set(); arrivals = []
    for a, t, s, _ in raw:
        if (s, a) in seen: continue
        seen.add((s, a)); arrivals.append((a, t, s))
    arrivals.sort()
    cheap = lognorm(0.035, 0.05); other = sum(w for w, _ in TODAY) - 447
    profiles = {"useful p50 1.0/p95 4.0": mix([(447, lognorm(1.0, 4.0)), (other, cheap)]),
                "useful p50 2.4/p95 8.0": mix([(447, lognorm(2.4, 8.0)), (other, cheap)])}
    seeds = (1, 2, 3, 4, 5)
    tenants_n = defaultdict(int)
    for _, t, _ in arrivals: tenants_n[t] += 1
    eligible = {t for t, n in tenants_n.items() if n >= 3}
    print(f"trace: {len(arrivals)} arrivals, {len(tenants_n)} tenants ({len(eligible)} with >=3 arrivals). ALL INPUTS BELOW ARE ASSUMED.")
    for pname, draw in profiles.items():
        print(f"\n== ASSUMED cost profile: {pname} (+ cheap skips) | coalescing | DelaySeconds 0 | seeds {seeds}")
        print("slots | proof_s | f | mode | start>7s (min-max) | finish>19s (min-max) | passes (min-max) | fallbacks (min-max) | tenants with start p95>7s (min-max of " + str(len(eligible)) + ")")
        for slots in (3, 4, 5):
            for proof_s in (0.05, 0.5, 2.0):
                for f in (0.0, 0.01, 0.05, 0.2):
                    for mode in (("immediate",) if f == 0 else ("immediate", "hold")):
                        sm, fm, ps, fb, bad = [], [], [], [], []
                        for sd in seeds:
                            rng = random.Random(sd)
                            costs = [min(300.0, POINTER(rng) + draw(rng)) * 1000 for _ in arrivals]
                            cov, passes, fallbacks = simulate(arrivals, slots, costs, f, proof_s, mode, sd)
                            sm.append(sum(1 for _, st, _ in cov if st > 7000)); fm.append(sum(1 for _, _, fi in cov if fi > 19000))
                            ps.append(passes); fb.append(fallbacks)
                            per = defaultdict(list)
                            for t, st, _ in cov: per[t].append(st / 1000)
                            bad.append(sum(1 for t in eligible if per.get(t) and pct(per[t], 95) > 7))
                            assert len(cov) == len(arrivals), "every arrival must end covered"
                        r = lambda xs: f"{min(xs)}-{max(xs)}"
                        print(f"{slots:5d} | {proof_s:7.2f} | {f:4.2f} | {mode:9s} | {r(sm):>9s} | {r(fm):>9s} | {r(ps):>11s} | {r(fb):>9s} | {r(bad)}")

if __name__ == "__main__":
    main()
