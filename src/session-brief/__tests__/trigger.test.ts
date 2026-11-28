/**
 * T144 offline fixtures for the session-brief trigger (trigger.ts): a deterministic simulation of the hosted path
 * (a standard queue with delays, visibility timeout and redelivery; a consumer that applies the debounce plan,
 * hands off durably and acknowledges only after the handoff or the run) on a fake clock. No network, no database.
 */
import { describe, expect, test } from "bun:test";
import {
  BRIEF_TRIGGER_CONTRACT, briefTriggerConfig, briefTriggerId, briefWakeHint, claimPlan, finishPlan, handoffPlan,
  parseBriefWakeHint, planWakeup, SQS_MAX_DELAY_SECONDS, type BriefPhysicalIncarnation, type BriefWakeHint,
  type BriefWakeKind, type BriefWakeState,
} from "../trigger.ts";

const T0 = Date.UTC(2026, 10, 28, 12, 0, 0);
const TIMING = { maxDelaySeconds: 8, idleSeconds: 600, maxWaitSeconds: 3_600 };
const THROTTLE_MS = 120_000;
const VISIBILITY_MS = 30_000;
const INC: BriefPhysicalIncarnation = { source: "claude", session: "phys-1", machine: "srv_00000000000000000000000000000001", generation: "1" };
const DIGEST = "a".repeat(64);

const hint = (kind: BriefWakeKind, revision: number, firstUncoveredAt: number, inc: BriefPhysicalIncarnation = INC): BriefWakeHint =>
  briefWakeHint(kind, { org: "org-1", workspace: "ws-1", session: "sess-1", revision, digest: DIGEST, first_uncovered_at: firstUncoveredAt }, inc);

/** The hosted path on a fake clock: queue + durable state + consumer + worker run. */
class Sim {
  now = T0;
  state: BriefWakeState | null = null;
  hasBrief: boolean;
  lastAttempt: number | null = null;
  queue: { hint: BriefWakeHint; visibleAt: number; receives: number }[] = [];
  /** Each run: when it was claimed, the due time it was claimed for, whether the throttle let it through. */
  runs: { claimedAt: number; dueAt: number; generation: number }[] = [];
  throttled = 0;
  failSends = 0;
  inFlight: { generation: number } | null = null;
  /** The highest revision a run has covered (the stored brief's brief_revision). */
  covered: number | undefined;
  constructor(opts: { hasBrief?: boolean } = {}) { this.hasBrief = opts.hasBrief ?? false; }

  send(h: BriefWakeHint, delaySeconds = 0) {
    if (this.failSends > 0) { this.failSends--; throw new Error("sqs_unavailable"); }
    this.queue.push({ hint: h, visibleAt: this.now + delaySeconds * 1_000, receives: 0 });
  }
  /** The producer, right after the authoritative manifest CAS. */
  produce(h: BriefWakeHint) { this.send(h); }

  /** The event source mapping delivers every visible message once; the consumer acks or reports a failure. */
  deliver() {
    const due = this.queue.filter((m) => m.visibleAt <= this.now);
    for (const m of due) {
      m.receives++;
      const ok = this.consume(m.hint);
      if (ok) this.queue.splice(this.queue.indexOf(m), 1);
      else m.visibleAt = this.now + VISIBILITY_MS; // batch item failure: redelivered after the visibility timeout
    }
  }

  /** The hosted consumer: apply the plan, then run or hand off; true = acknowledge the message. */
  consume(h: BriefWakeHint): boolean {
    const next = planWakeup(this.state, h, { nowMs: this.now, hasBrief: this.hasBrief, timing: TIMING, ...(this.covered !== undefined ? { coveredRevision: this.covered } : {}) }) ?? this.state;
    if (!next) return true; // nothing to do (a sweep for no work it could add)
    this.state = next;
    if (next.leased) return true; // a run is in flight: the generation bump keeps the trailing work
    const plan = handoffPlan(next, this.now);
    if (plan.action === "defer") {
      try { this.send(h, plan.delaySeconds); } catch { return false; } // failed handoff: keep the original
      return true;
    }
    this.claim();
    return true;
  }

  /** Claim and (synchronously, unless `hold`) run; the throttle applies to a refresh only. */
  claim(hold = false) {
    const s = this.state!;
    if (this.hasBrief && this.lastAttempt !== null && this.now - this.lastAttempt < THROTTLE_MS) {
      this.throttled++;
      this.state = { ...s, dueAt: this.lastAttempt + THROTTLE_MS };
      this.send(this.continuation(), Math.ceil((this.state.dueAt - this.now) / 1_000));
      return;
    }
    this.runs.push({ claimedAt: this.now, dueAt: s.dueAt, generation: s.generation });
    // The run reads the authoritative pointers: it covers every revision committed so far.
    this.covered = Math.max(this.covered ?? 0, s.revision);
    this.state = claimPlan(s);
    this.inFlight = { generation: s.generation };
    if (!hold) this.finish();
  }
  finish() {
    const g = this.inFlight!.generation;
    this.inFlight = null;
    this.lastAttempt = this.now;
    this.hasBrief = true;
    this.state = finishPlan(this.state!, g, { nowMs: this.now, timing: TIMING });
    if (this.state) this.send(this.continuation(), handoffPlan(this.state, this.now).action === "run" ? 0 : (handoffPlan(this.state, this.now) as { delaySeconds: number }).delaySeconds);
  }
  /** A continuation re-presents the latest hint (same trigger id and revision): it re-evaluates, never re-arms. */
  continuation(): BriefWakeHint { return { ...hint("append", this.state!.revision, T0), trigger_id: this.state!.lastTriggerId! }; }

  advance(ms: number, step = 1_000) {
    const end = this.now + ms;
    while (this.now < end) { this.now = Math.min(end, this.now + step); this.deliver(); }
  }
}

describe("session-brief trigger: hint contract v1", () => {
  test("the contract digest and exact shape", () => {
    expect(BRIEF_TRIGGER_CONTRACT).toBe("f01914a6cdc3db01353cb3a67f3e2cf1631b78da6a019d4582a532c6e7437d37");
    const h = hint("append", 3, T0);
    expect(Object.keys(h).sort()).toEqual(["digest", "first_uncovered_at", "kind", "org", "revision", "session", "trigger_id", "v", "workspace"]);
    expect(parseBriefWakeHint(JSON.parse(JSON.stringify(h)))).toEqual(h);
    for (const bad of [{ ...h, v: 2 }, { ...h, kind: "start" }, { ...h, extra: 1 }, { ...h, org: "../x" }, { ...h, revision: -1 }, { ...h, revision: 1.5 },
      { ...h, digest: "A".repeat(64) }, { ...h, first_uncovered_at: 1_700_000 }, { ...h, trigger_id: "x" }, null, [], "hint"])
      expect(parseBriefWakeHint(bad)).toBeNull();
    const { trigger_id: _t, ...missing } = h;
    expect(parseBriefWakeHint(missing)).toBeNull();
  });

  test("the retry identity is stable for the same commit and differs for any other incarnation, kind or revision", () => {
    const f = { org: "org-1", workspace: "ws-1", session: "sess-1", revision: 3, digest: DIGEST };
    expect(briefTriggerId("append", f, INC)).toBe(briefTriggerId("append", f, { ...INC }));
    for (const other of [{ ...INC, generation: "2" }, { ...INC, machine: "srv_00000000000000000000000000000002" }, { ...INC, session: "phys-2" }, { ...INC, source: "codex" }])
      expect(briefTriggerId("append", f, other)).not.toBe(briefTriggerId("append", f, INC));
    expect(briefTriggerId("end", f, INC)).not.toBe(briefTriggerId("append", f, INC));
    expect(briefTriggerId("append", { ...f, revision: 4 }, INC)).not.toBe(briefTriggerId("append", f, INC));
    expect(briefTriggerId("append", { ...f, digest: "b".repeat(64) }, INC)).not.toBe(briefTriggerId("append", f, INC));
  });

  test("settings: off by default, contract defaults, and a queue URL that is not SQS refuses", () => {
    expect(briefTriggerConfig({})).toEqual({ enabled: false, timing: { maxDelaySeconds: 8, idleSeconds: 600, maxWaitSeconds: 3_600 }, sweepSeconds: 300 });
    expect(briefTriggerConfig({ SESSION_BRIEF_ENABLED: "1" }).enabled).toBe(false);
    expect(briefTriggerConfig({ SESSION_BRIEF_ENABLED: "true" }).enabled).toBe(true);
    // The existing worker sweep setting is untouched: the trigger path has its own.
    expect(briefTriggerConfig({ SESSION_BRIEF_SWEEP_SECONDS: "0" }).sweepSeconds).toBe(300);
    expect(briefTriggerConfig({ SESSION_BRIEF_TRIGGER_SWEEP_SECONDS: "0" }).sweepSeconds).toBe(0);
    expect(briefTriggerConfig({ SESSION_BRIEF_QUEUE_URL: "https://sqs.us-east-1.amazonaws.com/111122223333/brief" }).queueUrl).toContain("sqs.");
    expect(() => briefTriggerConfig({ SESSION_BRIEF_QUEUE_URL: "https://example.com/q" })).toThrow(/SQS queue URL/);
  });
});

describe("session-brief trigger: offline fixtures", () => {
  test("a single early hint with the recovery tick off is handed off durably and claimed at its due time", () => {
    const sim = new Sim();
    sim.produce(hint("append", 1, T0));
    sim.advance(60_000);
    expect(sim.runs).toHaveLength(1);
    const [run] = sim.runs;
    expect(run!.dueAt).toBe(T0 + 8_000); // first brief: max delay 8 s
    expect(run!.claimedAt - run!.dueAt).toBeGreaterThanOrEqual(0);
    expect(run!.claimedAt - run!.dueAt).toBeLessThanOrEqual(1_000); // the measured claim vs due
    expect(sim.queue).toHaveLength(0);
  });

  test("a failed handoff keeps the original, and the work is claimed at its due time once the handoff succeeds", () => {
    const sim = new Sim({ hasBrief: true });
    sim.lastAttempt = T0 - 10 * 60_000;
    sim.produce(hint("append", 1, T0)); // a refresh: due after the 600 s idle window
    sim.failSends = 2; // the first two handoff sends fail
    sim.advance(90_000);
    // The original is still queued (retained, redelivered): two failed handoffs, then the third succeeded.
    expect(sim.failSends).toBe(0);
    expect(sim.runs).toHaveLength(0);
    expect(sim.queue).toHaveLength(1);
    expect(sim.queue[0]!.receives).toBe(0); // the handed-off copy, delayed to the due time
    sim.advance(15 * 60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.runs[0]!.dueAt).toBe(T0 + 1_000 + 600_000);
    expect(sim.runs[0]!.claimedAt - sim.runs[0]!.dueAt).toBeLessThanOrEqual(1_000);
    expect(sim.queue).toHaveLength(0);
  });

  test("duplicates (a producer retry, a redelivery) change nothing and run once", () => {
    const sim = new Sim();
    const h = hint("append", 1, T0);
    sim.produce(h);
    sim.produce(h);
    sim.produce({ ...h });
    sim.advance(60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.state).toBeNull();
    // A late redelivery after the work finished re-arms nothing (its revision is covered).
    sim.produce(h);
    sim.advance(20 * 60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.state).toBeNull();
    // The plan itself: the same trigger id twice is a no-op.
    const s = planWakeup(null, h, { nowMs: T0, hasBrief: false, timing: TIMING })!;
    expect(planWakeup(s, h, { nowMs: T0 + 1_000, hasBrief: false, timing: TIMING })).toBeNull();
  });

  test("continuous arrivals on a briefed session: refreshes are bounded by max-wait, then the idle window", () => {
    const sim = new Sim({ hasBrief: true });
    sim.lastAttempt = T0 - 10 * 60_000;
    // An append every minute for two hours, each a new revision.
    for (let i = 0; i < 120; i++) { sim.produce(hint("append", i + 1, sim.now)); sim.advance(60_000); }
    sim.advance(15 * 60_000);
    expect(sim.runs.length).toBeGreaterThanOrEqual(2);
    // Never more often than the max-wait window while evidence keeps coming.
    for (let i = 1; i < sim.runs.length - 1; i++)
      expect(sim.runs[i]!.claimedAt - sim.runs[i - 1]!.claimedAt).toBeGreaterThanOrEqual(3_600_000 - 60_000);
    // The first refresh is due at the oldest pending evidence plus max-wait (never pushed back by later arrivals).
    expect(sim.runs[0]!.dueAt).toBe(T0 + 3_600_000);
    // After the arrivals stop, the last refresh comes one idle window after the newest evidence.
    const last = sim.runs.at(-1)!;
    expect(last.dueAt).toBeLessThanOrEqual(T0 + 120 * 60_000 + 600_000);
  });

  test("an append while a run is in flight keeps the trailing work and its own oldest time", () => {
    const sim = new Sim({ hasBrief: true });
    sim.lastAttempt = T0 - 10 * 60_000;
    sim.produce(hint("end", 1, T0));
    sim.now += 1;
    // Claim without finishing: the run is in flight.
    sim.state = planWakeup(null, hint("end", 1, T0), { nowMs: sim.now, hasBrief: true, timing: TIMING });
    sim.queue = [];
    sim.claim(true);
    expect(sim.state!.leased).toBe(true);
    const claimedGeneration = sim.inFlight!.generation;
    sim.now += 30_000;
    const trailingAt = sim.now;
    sim.consume(hint("append", 2, trailingAt));
    expect(sim.state!.generation).toBe(claimedGeneration + 1);
    expect(sim.state!.trailingSince).toBe(trailingAt);
    sim.now += 10_000;
    sim.finish(); // the run's finish sees a newer generation: the trailing work stays
    expect(sim.state).not.toBeNull();
    expect(sim.state!.pendingSince).toBe(trailingAt); // from the trailing evidence, not the finish time
    expect(sim.state!.dueAt).toBe(Math.min(sim.state!.evidenceAt + 600_000, trailingAt + 3_600_000));
    sim.advance(20 * 60_000);
    expect(sim.runs).toHaveLength(2);
    expect(sim.state).toBeNull();
  });

  test("a lost trigger is recovered by a sweep hint", () => {
    const sim = new Sim();
    // The producer's hint was lost (never sent). The recovery tick enqueues a sweep for the uncovered revision.
    sim.advance(10 * 60_000);
    expect(sim.runs).toHaveLength(0);
    sim.produce(hint("sweep", 1, T0));
    sim.advance(5_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.runs[0]!.claimedAt).toBe(sim.runs[0]!.dueAt);
  });

  test("a hint at or below the covered revision is stale: it arms nothing", () => {
    const ctx = { nowMs: T0, hasBrief: true, timing: TIMING, coveredRevision: 7 };
    for (const k of ["append", "end", "sweep"] as const) {
      expect(planWakeup(null, hint(k, 7, T0), ctx)).toBeNull();
      expect(planWakeup(null, hint(k, 6, T0), ctx)).toBeNull();
    }
    expect(planWakeup(null, hint("append", 8, T0), ctx)!.revision).toBe(8);
  });

  test("a late, older sweep neither delays a due wakeup nor duplicates work; it only lowers pending_since", () => {
    const ctx = { nowMs: T0 + 60_000, hasBrief: true, timing: TIMING };
    const armed = planWakeup(null, hint("append", 5, T0 + 50_000), ctx)!;
    const older = hint("sweep", 3, T0 + 10_000);
    const after = planWakeup(armed, older, { ...ctx, nowMs: T0 + 70_000 })!;
    expect(after.dueAt).toBe(armed.dueAt);
    expect(after.generation).toBe(armed.generation);
    expect(after.pendingSince).toBe(T0 + 10_000);
    // A sweep with no older evidence changes nothing at all.
    expect(planWakeup(armed, hint("sweep", 4, T0 + 55_000), { ...ctx, nowMs: T0 + 70_000 })).toBeNull();
  });

  test("the oldest pending_since wins across reordering; evidence_at never moves backwards", () => {
    const ctx = { nowMs: T0 + 100_000, hasBrief: true, timing: TIMING };
    const a = planWakeup(null, hint("append", 2, T0 + 90_000), ctx)!;
    const b = planWakeup(a, hint("append", 1, T0 + 20_000), { ...ctx, nowMs: T0 + 110_000 })!; // an older one, late
    expect(b.pendingSince).toBe(T0 + 20_000);
    expect(b.evidenceAt).toBe(T0 + 110_000);
    expect(b.dueAt).toBe(Math.min(T0 + 110_000 + 600_000, T0 + 20_000 + 3_600_000));
    // A first_uncovered_at in the future of the consumer's clock is clamped to now.
    expect(planWakeup(null, hint("append", 3, T0 + 999_000), ctx)!.pendingSince).toBe(T0 + 100_000);
  });

  test("first brief bypasses the throttle; a refresh is throttled", () => {
    const first = new Sim();
    first.lastAttempt = T0 - 1_000; // a recent attempt that produced no brief
    first.produce(hint("append", 1, T0));
    first.advance(30_000);
    expect(first.runs).toHaveLength(1);
    expect(first.throttled).toBe(0);
    const refresh = new Sim({ hasBrief: true });
    refresh.lastAttempt = T0 - 1_000;
    refresh.produce(hint("end", 2, T0));
    refresh.advance(5 * 60_000);
    expect(refresh.throttled).toBe(1);
    expect(refresh.runs).toHaveLength(1);
    expect(refresh.runs[0]!.claimedAt).toBeGreaterThanOrEqual(T0 - 1_000 + THROTTLE_MS);
  });

  test("handoff never exceeds SQS's 15-minute delay; a long wait chains hops", () => {
    const s: BriefWakeState = { pendingSince: T0, evidenceAt: T0, dueAt: T0 + 3_600_000, generation: 1, leased: false, admission: "background", revision: 1 };
    expect(handoffPlan(s, T0)).toEqual({ action: "defer", delaySeconds: SQS_MAX_DELAY_SECONDS });
    expect(handoffPlan(s, T0 + 3_599_500)).toEqual({ action: "defer", delaySeconds: 1 });
    expect(handoffPlan(s, T0 + 3_600_000)).toEqual({ action: "run" });
    const sim = new Sim({ hasBrief: true });
    sim.lastAttempt = T0 - 3_600_000;
    sim.state = { ...s, lastTriggerId: hint("append", 1, T0).trigger_id };
    sim.send(sim.continuation());
    sim.advance(3_700_000, 5_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.runs[0]!.claimedAt - sim.runs[0]!.dueAt).toBeLessThanOrEqual(5_000);
  });
});
