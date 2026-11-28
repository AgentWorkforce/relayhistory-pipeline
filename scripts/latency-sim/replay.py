#!/usr/bin/env python3
"""Discrete-event replay of observed manifest arrivals against N worker slots.

Input: an arrivals.json of [arrival_ms, tenant_rank, session_hash, first_seen_ms] (anonymised).
Wait = arrival -> start of the pass that covers it (the 7s budget is manifest -> worker start).
Service-time models are lognormals fitted to observed p50/p95 per work class; the
post-fix scenarios are ASSUMPTIONS, labelled as such in the output.
"""
import heapq, json, math, random, sys
from collections import defaultdict

def lognorm(p50, p95):
    mu = math.log(p50); sigma = max(1e-6, (math.log(p95) - mu) / 1.645)
    return lambda rng: rng.lognormvariate(mu, sigma)

POINTER = lognorm(0.36, 1.21)
ARRIVAL_STATE = None
PASS_LOG = None
SUPERSEDE_RULE = False
WITH_SESSION = False
BATCH_CAP = int(__import__('os').environ.get('BATCH_CAP', '1000000'))  # FIFO ESM max batch is 10
# Observed search-stage classes, last hour (share of passes, p50 s, p95 s).
TODAY = [
    (2500, lognorm(4.84, 42.8)),    # ok, no-op
    (113, lognorm(224.5, 237.0)),   # stream_deadline (capped at 300 below)
    (213, lognorm(37.1, 187.0)),    # acceptance_missing full read
    (447, lognorm(2.42, 42.5)),     # useful
    (207, lognorm(11.3, 105.8)),    # directory_event_sources_changed
    (964, lognorm(0.035, 0.05)),    # skipped_before_read (cheap)
]
USEFUL_SHARE = 447 / sum(w for w, _ in TODAY)

def mix(classes):
    total = sum(w for w, _ in classes)
    def draw(rng):
        x = rng.random() * total
        for w, f in classes:
            x -= w
            if x <= 0: return f(rng)
        return classes[-1][1](rng)
    return draw

SCENARIOS = {
    # name: (search service draw, coalesce per session, description)
    "today": (mix(TODAY), False, "observed class mix, no per-session coalescing"),
    "waste_removed": (mix([(447, lognorm(2.42, 42.5)), (sum(w for w, _ in TODAY) - 447, lognorm(0.035, 0.05))]), True,
                      "ASSUMED: no-op/failed passes cost a cheap skip; useful passes as observed; per-session coalescing"),
    "incremental": (mix([(447, lognorm(1.0, 4.0)), (sum(w for w, _ in TODAY) - 447, lognorm(0.035, 0.05))]), True,
                    "ASSUMED: plus incremental indexing, useful p50 1.0s / p95 4.0s; per-session coalescing"),
}

def simulate(arrivals, slots, search_draw, coalesce, delay_s, seed=7, costs=None):
    """Event loop. A 'group' is a session when coalescing (one pass in flight per session; arrivals
    during it merge into its next pass), else each arrival alone. Free slots take ready groups in
    order of their oldest pending arrival. Wait = original arrival -> start of the covering pass."""
    rng = random.Random(seed)
    if costs is None:
        costs = [min(300.0, POINTER(rng) + search_draw(rng)) * 1000 for _ in arrivals]
    cost_of = {(s, a): c for (a, t, s), c in zip(arrivals, costs)}
    evs = sorted((a + delay_s * 1000, a, t, s) for a, t, s in arrivals)
    free = slots
    ready = []          # heap: (oldest pending original arrival, seq, group)
    queued = set()      # groups currently in `ready`
    pending = defaultdict(list)  # group -> [(original arrival, tenant)]
    inflight = set()
    fins = []           # heap: (finish ms, group)
    waits = []
    seq = 0; i = 0
    while i < len(evs) or fins:
        na = evs[i][0] if i < len(evs) else math.inf
        nf = fins[0][0] if fins else math.inf
        if na <= nf:
            now, orig, t, s = evs[i]; i += 1
            g = s if coalesce else (s, orig, seq)
            if ARRIVAL_STATE is not None: ARRIVAL_STATE[(s, orig)] = (g in inflight, free == 0)
            pending[g].append((orig, t))
            if g not in inflight and g not in queued:
                heapq.heappush(ready, (orig, seq, g)); queued.add(g); seq += 1
        else:
            now, g = heapq.heappop(fins)
            free += 1; inflight.discard(g)
            if pending.get(g):
                heapq.heappush(ready, (pending[g][0][0], seq, g)); queued.add(g); seq += 1
        while free and ready:
            _, _, g = heapq.heappop(ready); queued.discard(g)
            queued_items = pending.get(g, [])
            if not queued_items: continue
            batch = queued_items[:BATCH_CAP] if coalesce else queued_items
            rest = queued_items[len(batch):]
            if rest: pending[g] = rest
            else: pending.pop(g, None)
            sess = g if coalesce else g[0]
            dur = cost_of[(sess, max(o for o, _ in batch))]
            if SUPERSEDE_RULE and coalesce and rest:  # fully superseded batch: delivery + pointer only, no search
                dur = POINTER(rng) * 1000
            if PASS_LOG is not None: PASS_LOG.append(dur)
            for orig, t in batch: waits.append((t, now - orig, now + dur - orig, orig, sess) if WITH_SESSION else (t, now - orig, now + dur - orig, orig))
            free -= 1; inflight.add(g)
            heapq.heappush(fins, (now + dur, g))
    return waits

def pct(xs, p):
    if not xs: return float("nan")
    xs = sorted(xs); k = min(len(xs) - 1, max(0, int(math.ceil(p / 100 * len(xs))) - 1))
    return xs[k]

def main():
    raw = json.load(open(sys.argv[1]))
    seen = set(); arrivals = []
    for a, t, s, _ in raw:
        if (s, a) in seen: continue
        seen.add((s, a)); arrivals.append((a, t, s))
    arrivals.sort()
    t0 = arrivals[0][0]; t1 = arrivals[-1][0]
    print(f"arrivals (deduped by session+event time): {len(arrivals)} of {len(raw)} log lines; span {(t1-t0)/1000:.0f}s; "
          f"tenants {len({t for _, t, _ in arrivals})}; sessions {len({s for _, _, s in arrivals})}")
    cheap = lognorm(0.035, 0.05); other = sum(w for w, _ in TODAY) - 447
    scen = [("waste_removed (useful as observed p50 2.4/p95 42.5)", mix([(447, lognorm(2.42, 42.5)), (other, cheap)]))]
    for p50, p95 in ((1.0, 4.0), (2.4, 8.0), (5.0, 15.0)):
        scen.append((f"incremental (ASSUMED useful p50 {p50}/p95 {p95})", mix([(447, lognorm(p50, p95)), (other, cheap)])))
    seeds = (1, 2, 3, 4, 5)
    for name, draw in scen:
        print(f"\n== {name} | coalescing | DelaySeconds=0 | seeds {seeds} (same sampled costs across slot counts within a seed)")
        print("slots | misses>7s / arrivals (min-max over seeds) | all p95 s (min-max) | worst-tenant p95 s (min-max, tenants with >=3 arrivals) | max wait s (max)")
        for slots in (3, 5, 8, 10, 15, 25):
            miss = []; p95 = []; worst = []; mx = []
            for sd in seeds:
                rng = random.Random(sd)
                costs = [min(300.0, POINTER(rng) + draw(rng)) * 1000 for _ in arrivals]
                w = simulate(arrivals, slots, draw, True, 0, seed=sd, costs=costs)
                allw = [x[1] / 1000 for x in w]
                per = defaultdict(list)
                for t, x, *_ in w: per[t].append(x / 1000)
                miss.append(sum(1 for x in allw if x > 7)); p95.append(pct(allw, 95))
                worst.append(max(pct(v, 95) for v in per.values() if len(v) >= 3)); mx.append(max(allw))
            print(f"{slots:5d} | {min(miss)}-{max(miss)} / {len(arrivals)} | {min(p95):.1f}-{max(p95):.1f} | {min(worst):.1f}-{max(worst):.1f} | {max(mx):.1f}")

def tenant_report(path, scen_p50, scen_p95, slots, seeds=(1,2,3,4,5)):
    raw = json.load(open(path)); seen=set(); arrivals=[]
    for a,t,s,_ in raw:
        if (s,a) in seen: continue
        seen.add((s,a)); arrivals.append((a,t,s))
    arrivals.sort()
    cheap = lognorm(0.035, 0.05); other = sum(w for w,_ in TODAY) - 447
    draw = mix([(447, lognorm(scen_p50, scen_p95)), (other, cheap)])
    agg = defaultdict(lambda: {"n":0,"s":[], "f":[]})
    for sd in seeds:
        rng = random.Random(sd)
        costs = [min(300.0, POINTER(rng) + draw(rng)) * 1000 for _ in arrivals]
        for t, st, fi in simulate(arrivals, slots, draw, True, 0, seed=sd, costs=costs):
            k = "#1" if t == 1 else ("#2-#5" if t <= 5 else "#6+")
            agg[k]["s"].append(st/1000); agg[k]["f"].append(fi/1000)
    print(f"useful p50 {scen_p50}/p95 {scen_p95}, {slots} slots, DelaySeconds 0, coalescing, {len(seeds)} seeds pooled")
    print("tenant group | arrivals/seed | START p50/p95/max | FINISH p50/p95/max | start>7s | finish>20s")
    for k in ("#1","#2-#5","#6+"):
        s=agg[k]["s"]; f=agg[k]["f"]; n=len(s)//len(seeds)
        print(f"{k:6s} | {n} | {pct(s,50):.1f}/{pct(s,95):.1f}/{max(s):.1f} | {pct(f,50):.1f}/{pct(f,95):.1f}/{max(f):.1f} | {sum(x>7 for x in s)/len(seeds):.1f} | {sum(x>20 for x in f)/len(seeds):.1f}")

def cutoff_report(path, p50, p95, slots, seeds=(1,2,3,4,5)):
    raw = json.load(open(path)); seen=set(); arrivals=[]
    for a,t,s,_ in raw:
        if (s,a) in seen: continue
        seen.add((s,a)); arrivals.append((a,t,s))
    arrivals.sort(); T = arrivals[-1][0]  # fixed cutoff = end of the arrival window
    cheap = lognorm(0.035, 0.05); other = sum(w for w,_ in TODAY) - 447
    draw = mix([(447, lognorm(p50, p95)), (other, cheap)])
    snaps=[]; per=defaultdict(lambda: {"s":[], "f":[]}); oldest=defaultdict(float)
    for sd in seeds:
        rng = random.Random(sd)
        costs = [min(300.0, POINTER(rng) + draw(rng)) * 1000 for _ in arrivals]
        w = simulate(arrivals, slots, draw, True, 0, seed=sd, costs=costs)
        waiting = [(t, orig) for t, st, fi, orig in w if orig + st > T]
        inserv = [(t, orig) for t, st, fi, orig in w if orig + st <= T < orig + fi]
        snaps.append((len(waiting), len(inserv)))
        for t, orig in waiting: oldest[t] = max(oldest[t], (T - orig) / 1000)
        for t, st, fi, orig in w: per[t]["s"].append(st/1000); per[t]["f"].append(fi/1000)
    print(f"useful p50 {p50}/p95 {p95}, {slots} slots, DelaySeconds 0, coalescing, cap {BATCH_CAP}, {len(seeds)} seeds")
    print(f"  at cutoff (last arrival of the window): waiting min-max {min(s[0] for s in snaps)}-{max(s[0] for s in snaps)}, in service {min(s[1] for s in snaps)}-{max(s[1] for s in snaps)}; "
          f"oldest waiting age at cutoff by tenant (max over seeds): " + (", ".join(f"#{t}={a:.1f}s" for t,a in sorted(oldest.items())) or "none"))
    rows=[]
    for t,v in per.items():
        n=len(v["s"])//len(seeds)
        rows.append((t, n, pct(v["s"],95), max(v["s"]), sum(x>7 for x in v["s"])/len(seeds), pct(v["f"],95), max(v["f"]), sum(x>19 for x in v["f"])/len(seeds)))
    rows.sort(key=lambda r: (-r[7], -r[4], r[0]))
    print("  per tenant (worst first): tenant | arrivals | START p95/max | start>7s | COVERING-PASS FINISH p95/max | finish>19s")
    for r in rows[:8]:
        print(f"  #{r[0]} | {r[1]} | {r[2]:.1f}/{r[3]:.1f} | {r[4]:.1f} | {r[5]:.1f}/{r[6]:.1f} | {r[7]:.1f}")
    print(f"  tenants with any start>7s: {sum(1 for r in rows if r[4]>0)} of {len(rows)}; any finish>19s: {sum(1 for r in rows if r[7]>0)}")

if __name__ == "__main__" and len(sys.argv) > 2 and sys.argv[2] == "cutoff":
    for p50,p95 in ((1.0,4.0),(2.4,8.0),(2.42,42.5)):
        cutoff_report(sys.argv[1], p50, p95, 5); print()
    sys.exit(0)

if __name__ == "__main__" and len(sys.argv) > 2 and sys.argv[2] == "tenants":
    for p50,p95 in ((1.0,4.0),(2.4,8.0),(2.42,42.5)):
        for slots in (5,10):
            tenant_report(sys.argv[1], p50, p95, slots); print()
    sys.exit(0)
if __name__ == "__main__":
    main()
