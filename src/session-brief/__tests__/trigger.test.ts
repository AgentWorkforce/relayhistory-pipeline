/**
 * T144 offline fixtures for the session-brief trigger (trigger.ts): a deterministic simulation on a fake clock of
 *   - the durable wakeup rows (one per (org, workspace, harness, session)), armed only by committed evidence;
 *   - the hosted path: a standard queue (delay, visibility timeout, redelivery), a consumer that wakes the rows a
 *     hint addresses, hands off durably and acknowledges only after a run or a successful handoff, and a recovery
 *     tick that enqueues sweep hints every SESSION_BRIEF_SWEEP_SECONDS;
 *   - the self-host path: the native worker polling the rows every minute.
 * No network, no database. Every run records when it became claimable and when it was claimed.
 */
import { describe, expect, test } from "bun:test";
import {
  armIdentity, BRIEF_TRIGGER_CONTRACT, briefTriggerConfig, briefTriggerId, briefWakeHint, claimAt, claimPlan, finishPlan, handoffPlan,
  briefMiss, hintCovers, parseBriefWakeHint, planArm, planHint, SQS_MAX_DELAY_SECONDS, type BriefPhysicalIncarnation,
  type BriefWakeHint, type BriefWakeKind, type BriefWakeState,
} from "../trigger.ts";

const T0 = Date.UTC(2026, 10, 28, 12, 0, 0);
const TIMING = { maxDelaySeconds: 8, idleSeconds: 600, maxWaitSeconds: 3_600 };
const THROTTLE_S = 120;
const VISIBILITY_MS = 30_000;
const DIGEST = "a".repeat(64);
const inc = (harness: string): BriefPhysicalIncarnation => ({ source: harness, session: "phys-1", machine: "srv_00000000000000000000000000000001", generation: "1" });
const hint = (kind: BriefWakeKind, revision: number, at: number, harness = "claude"): BriefWakeHint =>
  briefWakeHint(kind, { org: "org-1", workspace: "ws-1", harness, session: "sess-1", revision, digest: DIGEST, first_uncovered_at: at }, inc(harness));
const v1 = (h: BriefWakeHint): BriefWakeHint => {
  const { harness: _h, ...rest } = h as BriefWakeHint & { harness?: string };
  return { ...rest, v: 1 } as BriefWakeHint;
};

interface Brief { revision: number; lastAttempt?: number; ready: boolean }
interface Run { harness: string; claimableAt: number; claimedAt: number; generation: number; dueAt: number }

class Sim {
  now = T0;
  rows = new Map<string, { state: BriefWakeState }>(); // by harness (one org/workspace/session in these fixtures)
  briefs = new Map<string, Brief>();
  /** The highest committed revision per harness (what a run reads from the authoritative pointers). */
  committed = new Map<string, number>();
  queue: { hint: BriefWakeHint; visibleAt: number; receives: number }[] = [];
  runs: Run[] = [];
  failSends = 0;
  failRuns = 0;
  inFlight = new Map<string, { generation: number }>();
  sweptAt: number[] = [];
  /** Every durable handoff: when it was sent and the delay it asked for. */
  handoffs: { at: number; delaySeconds: number }[] = [];
  private lastSweep = T0;
  private lastTick = T0;
  /**
   * tickSeconds: the worker tick that claims rows already due (hosted: the one-minute EventBridge {brief:true} tick;
   * self-host: the native worker's poll); 0/undefined = no tick (hints only).
   * sweepSeconds: SESSION_BRIEF_SWEEP_SECONDS, how often the tick also sweeps: it re-arms committed revisions that no
   * brief covers and that have no wakeup (a lost arm) and, hosted, enqueues a sweep hint for them; 0 = off.
   */
  constructor(public opts: { sweepSeconds?: number; tickSeconds?: number; hosted?: boolean } = {}) {}

  brief(h: string): Brief { return this.briefs.get(h) ?? { revision: -1, ready: false }; }

  /** The producer, right after the authoritative CAS: arm the durable row, then send the hint (which may be lost). */
  commit(harness: string, revision: number, opts: { kind?: "append" | "end"; lose?: boolean; loseArm?: boolean } = {}) {
    this.committed.set(harness, Math.max(this.committed.get(harness) ?? -1, revision));
    const prev = this.rows.get(harness)?.state ?? null;
    if (!opts.loseArm) this.rows.set(harness, { state: planArm(prev, this.now, this.brief(harness).ready, TIMING) });
    const h = hint(opts.kind ?? "append", revision, prev?.pendingSince ?? this.now, harness);
    if (!opts.lose) this.send(h);
    return h;
  }
  send(h: BriefWakeHint, delaySeconds = 0) {
    if (this.failSends > 0) { this.failSends--; throw new Error("sqs_unavailable"); }
    this.queue.push({ hint: h, visibleAt: this.now + delaySeconds * 1_000, receives: 0 });
  }
  claimable(h: string, s: BriefWakeState) {
    const b = this.brief(h);
    return claimAt(s, { hasBrief: b.ready, throttleSeconds: THROTTLE_S, ...(b.lastAttempt !== undefined ? { lastAttemptMs: b.lastAttempt } : {}) });
  }

  /** The hosted consumer for one message; true = acknowledge. */
  consume(h: BriefWakeHint): boolean {
    let earliest = Infinity;
    for (const [harness, row] of [...this.rows]) {
      if (!hintCovers(h, { org: "org-1", workspace: "ws-1", harness, session: "sess-1" })) continue;
      const b = this.brief(harness);
      const next = planHint(row.state, h, { nowMs: this.now, hasBrief: b.ready, timing: TIMING, ...(b.ready ? { coveredRevision: b.revision } : {}) });
      if (next) row.state = next;
      if (row.state.leased) continue; // in flight: the generation decides the trailing work
      const at = this.claimable(harness, row.state);
      if (at <= this.now) this.claim(harness);
      else earliest = Math.min(earliest, at);
    }
    if (earliest === Infinity) return true;
    const plan = handoffPlan(earliest, this.now);
    const delaySeconds = plan.action === "defer" ? plan.delaySeconds : 0;
    try { this.send(h, delaySeconds); } catch { return false; } // failed handoff: keep the original
    this.handoffs.push({ at: this.now, delaySeconds });
    return true;
  }

  claim(harness: string, hold = false) {
    const row = this.rows.get(harness)!;
    this.runs.push({ harness, claimableAt: this.claimable(harness, row.state), claimedAt: this.now, generation: row.state.generation, dueAt: row.state.dueAt });
    row.state = claimPlan(row.state);
    this.inFlight.set(harness, { generation: row.state.generation });
    if (!hold) this.finish(harness);
  }
  finish(harness: string) {
    const row = this.rows.get(harness)!;
    const g = this.inFlight.get(harness)!.generation;
    this.inFlight.delete(harness);
    const b = this.brief(harness);
    if (this.failRuns > 0) { // a retryable failure: the attempt counts, the row is rescheduled 60 s out (existing backoff)
      this.failRuns--;
      this.briefs.set(harness, { ...b, lastAttempt: this.now });
      row.state = { ...row.state, leased: false, dueAt: this.now + 60_000, backoff: true };
      return;
    }
    // The run read the authoritative pointers: it covers every revision committed so far.
    this.briefs.set(harness, { revision: this.committed.get(harness)!, ready: true, lastAttempt: this.now });
    const next = finishPlan(row.state, g, this.now, TIMING);
    if (next) row.state = next; else this.rows.delete(harness);
  }

  /** Each second: deliver visible messages; the hosted sweep tick; the self-host poll. */
  advance(ms: number) {
    const end = this.now + ms;
    while (this.now < end) {
      this.now += 1_000;
      for (const m of this.queue.filter((x) => x.visibleAt <= this.now)) {
        m.receives++;
        if (this.consume(m.hint)) this.queue.splice(this.queue.indexOf(m), 1);
        else m.visibleAt = this.now + VISIBILITY_MS;
      }
      const tick = this.opts.tickSeconds ?? 0;
      if (tick > 0 && this.now - this.lastTick >= tick * 1_000) {
        this.lastTick = this.now;
        const sweep = this.opts.sweepSeconds ?? 0;
        if (sweep > 0 && this.now - this.lastSweep >= sweep * 1_000) {
          this.lastSweep = this.now;
          for (const [harness, rev] of this.committed) // a lost arm: committed, uncovered, no wakeup
            if (!this.rows.has(harness) && rev > this.brief(harness).revision) {
              this.rows.set(harness, { state: planArm(null, this.now, this.brief(harness).ready, TIMING) });
              this.sweptAt.push(this.now);
            }
        }
        // Due-time gating [Pipeline 252419350106017792]: every tick (hosted: the one-minute tick) checks due work and
        // claims only rows claimable now; the sweep cadence above gates only the lost-hint (lost-arm) scan.
        for (const [harness, row] of [...this.rows]) if (!row.state.leased && this.claimable(harness, row.state) <= this.now) this.claim(harness);
      }
    }
  }
}

describe("session-brief trigger: hint contract", () => {
  test("the contract digest; v1 and v2 exact shapes", () => {
    expect(BRIEF_TRIGGER_CONTRACT).toBe("f01914a6cdc3db01353cb3a67f3e2cf1631b78da6a019d4582a532c6e7437d37");
    const h = hint("append", 3, T0);
    expect(h.v).toBe(2);
    expect(Object.keys(h).sort()).toEqual(["digest", "first_uncovered_at", "harness", "kind", "org", "revision", "session", "trigger_id", "v", "workspace"]);
    expect(parseBriefWakeHint(JSON.parse(JSON.stringify(h)))).toEqual(h);
    const one = v1(h);
    expect(parseBriefWakeHint(one)).toEqual(one);
    for (const bad of [{ ...h, v: 3 }, { ...one, harness: "claude" }, { ...h, harness: "Claude" }, { ...h, kind: "start" }, { ...h, extra: 1 },
      { ...h, org: "../x" }, { ...h, revision: -1 }, { ...h, revision: 1.5 }, { ...h, digest: "A".repeat(64) }, { ...h, first_uncovered_at: 1_700_000 },
      { ...h, trigger_id: "x" }, null, [], "hint"])
      expect(parseBriefWakeHint(bad)).toBeNull();
    const { harness: _h, ...noHarness } = h as BriefWakeHint & { harness: string };
    expect(parseBriefWakeHint(noHarness)).toBeNull(); // v2 without its harness
  });

  test("the tracing id is stable for one commit and differs for any other incarnation, kind, revision or harness", () => {
    const f = { org: "org-1", workspace: "ws-1", harness: "claude", session: "sess-1", revision: 3, digest: DIGEST };
    expect(briefTriggerId("append", f, inc("claude"))).toBe(briefTriggerId("append", f, { ...inc("claude") }));
    for (const other of [{ ...inc("claude"), generation: "2" }, { ...inc("claude"), machine: "srv_00000000000000000000000000000002" }, { ...inc("claude"), session: "phys-2" }])
      expect(briefTriggerId("append", f, other)).not.toBe(briefTriggerId("append", f, inc("claude")));
    expect(briefTriggerId("end", f, inc("claude"))).not.toBe(briefTriggerId("append", f, inc("claude")));
    expect(briefTriggerId("append", { ...f, revision: 4 }, inc("claude"))).not.toBe(briefTriggerId("append", f, inc("claude")));
    expect(briefTriggerId("append", { ...f, harness: "codex" }, inc("claude"))).not.toBe(briefTriggerId("append", f, inc("claude")));
  });

  test("settings: off by default; the existing sweep default 0 kept (direct worker unset/0), native chart 60, hosted 300", () => {
    expect(briefTriggerConfig({})).toEqual({ enabled: false, timing: TIMING, sweepSeconds: 0 });
    expect(briefTriggerConfig({ SESSION_BRIEF_ENABLED: "1" }).enabled).toBe(false);
    expect(briefTriggerConfig({ SESSION_BRIEF_ENABLED: "true" }).enabled).toBe(true);
    expect(briefTriggerConfig({ SESSION_BRIEF_ENABLED: "true" }).sweepSeconds).toBe(0); // direct worker, unset
    expect(briefTriggerConfig({ SESSION_BRIEF_SWEEP_SECONDS: "0" }).sweepSeconds).toBe(0); // direct worker, explicit 0
    expect(briefTriggerConfig({ SESSION_BRIEF_SWEEP_SECONDS: "60" }).sweepSeconds).toBe(60); // native chart default
    expect(briefTriggerConfig({ SESSION_BRIEF_SWEEP_SECONDS: "300" }).sweepSeconds).toBe(300); // hosted, explicit
    expect(briefTriggerConfig({ SESSION_BRIEF_QUEUE_URL: "https://sqs.us-east-1.amazonaws.com/111122223333/brief" }).queueUrl).toContain("sqs.");
    expect(() => briefTriggerConfig({ SESSION_BRIEF_QUEUE_URL: "https://example.com/q" })).toThrow(/SQS queue URL/);
  });
});

describe("session-brief trigger: due and claim eligibility", () => {
  test("a hint is monotone: never later, never pending_since or the generation, never creates a row", () => {
    const ctx = { nowMs: T0 + 30_000, hasBrief: true, timing: TIMING };
    expect(planHint(null, hint("append", 1, T0), ctx)).toBeNull();
    const armed = planArm(null, T0, true, TIMING);
    expect(armed.dueAt).toBe(T0 + 600_000);
    const ended = planHint(armed, hint("end", 1, T0), ctx)!;
    expect(ended.dueAt).toBe(T0 + 30_000); // refresh end: due now
    expect([ended.pendingSince, ended.generation, ended.evidenceAt]).toEqual([armed.pendingSince, armed.generation, armed.evidenceAt]);
    expect(planHint(ended, hint("append", 1, T0), { ...ctx, nowMs: T0 + 90_000 })).toBeNull(); // never later
    expect(planHint(armed, hint("sweep", 1, T0), ctx)).toBeNull();
    expect(planHint(armed, hint("end", 1, T0), { ...ctx, coveredRevision: 1 })).toBeNull(); // covered
  });

  test("first brief: due within N, an end makes it due now, later evidence never pushes it back, never throttled", () => {
    const armed = planArm(null, T0, false, TIMING);
    expect(armed.dueAt).toBe(T0 + 8_000);
    expect(planHint(armed, hint("end", 1, T0), { nowMs: T0 + 2_000, hasBrief: false, timing: TIMING })!.dueAt).toBe(T0 + 2_000);
    expect(planArm(armed, T0 + 5_000, false, TIMING).dueAt).toBe(T0 + 8_000);
    expect(claimAt(armed, { hasBrief: false, lastAttemptMs: T0 - 1_000, throttleSeconds: THROTTLE_S })).toBe(T0 + 8_000);
  });

  test("refresh where the throttle ends after max-wait: the throttle wins; scored from the upload, it is a miss", () => {
    // Pending since T0-3550 s, so max-wait is due at T0+50 s; the last attempt at T0, so the throttle ends at T0+120 s.
    const state: BriefWakeState = { pendingSince: T0 - 3_550_000, evidenceAt: T0, dueAt: T0 + 50_000, generation: 3, leased: false };
    const at = claimAt(state, { hasBrief: true, lastAttemptMs: T0, throttleSeconds: THROTTLE_S });
    expect(at).toBe(T0 + 120_000);
    // The score runs from the cohort's first upload HTTP attempt, never from due or claim: an upload at T0 claimed at
    // +120 s is a miss even if served the instant it is claimed.
    expect(briefMiss(T0, at)).toBe(true);
    expect(briefMiss(T0, T0 + 60_000)).toBe(false);
    expect(briefMiss(T0, T0 + 60_001)).toBe(true);
  });
});

describe("session-brief trigger: offline fixtures", () => {
  test("a single early hint with the recovery tick off is handed off durably and claimed at its due time", () => {
    const sim = new Sim();
    sim.commit("claude", 1); // the commit (and the upload's lower bound) at T0
    sim.advance(60_000);
    expect(sim.runs).toHaveLength(1);
    // The recorded timeline, relative to the commit at T0 (ms): delivered at +1000 (the simulator's first 1 s step),
    // not claimable until +8000 (first brief: pending + 8 s), so handed off at +1000 with a 7 s delay; the copy is
    // delivered and claimed at +8000, exactly at due.
    const timeline = { due: sim.runs[0]!.dueAt - T0, handoff: sim.handoffs.map((x) => ({ at: x.at - T0, delaySeconds: x.delaySeconds })),
      claim: sim.runs[0]!.claimedAt - T0 };
    expect(timeline).toEqual({ due: 8_000, handoff: [{ at: 1_000, delaySeconds: 7 }], claim: 8_000 });
    // GAP: there is no parsed authorized read in this offline core (no Reader); the score from the upload needs the
    // integration trace (sessions-latency-owner's membership read). The claim alone is within 60 s here.
    expect(briefMiss(T0, sim.runs[0]!.claimedAt)).toBe(false);
    expect(sim.queue).toHaveLength(0);
  });

  test("a failed handoff keeps the original; the work is claimed at its due time once a handoff succeeds", () => {
    const sim = new Sim();
    sim.briefs.set("claude", { revision: 0, ready: true, lastAttempt: T0 - 600_000 });
    sim.commit("claude", 1); // a refresh: due after the idle window
    sim.failSends = 2;
    sim.advance(90_000);
    expect(sim.failSends).toBe(0);
    expect(sim.runs).toHaveLength(0);
    expect(sim.queue).toHaveLength(1); // the delayed handoff copy; the original was acknowledged only after it
    sim.advance(15 * 60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.runs[0]!.claimableAt).toBe(T0 + 600_000);
    expect(sim.runs[0]!.claimedAt - sim.runs[0]!.claimableAt).toBeLessThanOrEqual(1_000);
  });

  for (const order of [["A", "B", "A"], ["B", "A", "B"]] as const) test(`${order.join("/")} redelivery converges: final row and claims as expected`, () => {
    const sim = new Sim();
    sim.briefs.set("claude", { revision: 0, ready: true, lastAttempt: T0 - 600_000 });
    const A = sim.commit("claude", 1, { lose: true });
    sim.now += 30_000;
    const B = sim.commit("claude", 2, { lose: true });
    const armed = { ...sim.rows.get("claude")!.state };
    const byName = { A, B };
    for (const n of order) { sim.send(byName[n]); sim.advance(5_000); }
    const row = sim.rows.get("claude")!.state;
    expect([row.dueAt, row.pendingSince, row.generation]).toEqual([armed.dueAt, armed.pendingSince, armed.generation]);
    sim.advance(15 * 60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.runs[0]!.claimableAt).toBe(armed.dueAt);
    for (const n of ["A", "B", "A"] as const) sim.send(byName[n]); // after completion: every replay is covered
    sim.advance(30 * 60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.rows.size).toBe(0);
  });

  test("a duplicate during the lease creates no trailing work; B committed during A's lease does", () => {
    const sim = new Sim();
    sim.briefs.set("claude", { revision: 0, ready: true, lastAttempt: T0 - 600_000 });
    const A = sim.commit("claude", 1, { kind: "end", lose: true });
    sim.claim("claude", true); // A's run in flight
    const g = sim.inFlight.get("claude")!.generation;
    sim.now += 2_000;
    sim.consume(A); // a duplicate during the lease
    expect(sim.rows.get("claude")!.state.generation).toBe(g);
    sim.now += 5_000;
    sim.commit("claude", 2, { lose: true }); // B: new committed evidence during the lease
    expect(sim.rows.get("claude")!.state.generation).toBe(g + 1);
    const bCommittedAt = sim.now;
    sim.now += 20_000;
    const finishedAt = sim.now;
    sim.finish("claude");
    const trailing = sim.rows.get("claude")!.state;
    expect(trailing.leased).toBe(false);
    // The trailing window restarts at the finish: late by the run's length (bounded by the lease), recorded here.
    expect(trailing.pendingSince).toBe(finishedAt);
    expect(finishedAt - bCommittedAt).toBe(20_000);
    sim.advance(20 * 60_000);
    expect(sim.runs).toHaveLength(1); // hint B was lost and the tick is off: the trailing row waits for recovery
    expect(sim.rows.size).toBe(1);
    // With the one-minute tick the trailing work is claimed; its delay is SCORED from B's commit (a lower bound for
    // B's first upload attempt), never reported as a pass: the restarted window makes it a miss.
    sim.opts.tickSeconds = 60;
    sim.advance(5 * 60_000);
    expect(sim.runs).toHaveLength(2);
    expect(briefMiss(bCommittedAt, sim.runs[1]!.claimedAt)).toBe(true);
  });

  test("during a backoff: a replay changes nothing; a genuinely new committed append re-arms and its hint applies", () => {
    const sim = new Sim();
    const A = sim.commit("claude", 1, { lose: true });
    sim.failRuns = 1;
    sim.now = sim.rows.get("claude")!.state.dueAt;
    sim.claim("claude"); // the first brief fails: rescheduled 60 s out, in backoff
    const backedOff = { ...sim.rows.get("claude")!.state };
    expect(backedOff.backoff).toBe(true);
    sim.now += 5_000;
    sim.consume(A); // replay: no-op, the retry floor stands
    expect(sim.rows.get("claude")!.state).toEqual(backedOff);
    expect(sim.runs).toHaveLength(1);
    sim.now += 5_000;
    const B = sim.commit("claude", 2, { lose: true }); // a NEW committed append: armWakeup resets attempts
    const rearmed = sim.rows.get("claude")!.state;
    expect(rearmed.backoff).toBeUndefined();
    expect(rearmed.generation).toBe(backedOff.generation + 1);
    expect(rearmed.dueAt).toBeLessThanOrEqual(backedOff.dueAt); // the first-brief bound, never later than promised
    sim.consume(B); // its hint applies normally now
    expect(sim.runs).toHaveLength(rearmed.dueAt <= sim.now ? 2 : 1);
    sim.advance(60_000);
    expect(sim.runs).toHaveLength(2);
  });

  test.todo("KNOWN GAP: B committed during A's lease keeps B's original uncovered time through finish and retry (needs trailing_since: a migration in a separate Pipeline registry slot)", () => {});

  test("an equal revision with different content (another machine, same harness): suppression never removes eligibility", () => {
    const sim = new Sim();
    // The brief covers revision 5 (from machine M1). Machine M2 commits ITS revision 5 (different content).
    sim.briefs.set("claude", { revision: 5, ready: true, lastAttempt: T0 - 600_000 });
    sim.committed.set("claude", 5);
    const m2 = sim.commit("claude", 5, { lose: true });
    const armed = { ...sim.rows.get("claude")!.state };
    sim.now += 10_000;
    sim.consume({ ...m2, kind: "end", digest: "b".repeat(64) }); // collides with the covered revision
    expect(sim.rows.get("claude")!.state.dueAt).toBe(armed.dueAt); // the early wake is skipped (the known limitation)...
    sim.advance(15 * 60_000);
    expect(sim.runs).toHaveLength(1); // ...but the uncovered work stays eligible and is claimed at its own due time
    expect(sim.runs[0]!.claimableAt).toBe(armed.dueAt);
  });

  test("two harnesses, same session id and equal revision: v1 keeps each harness's own coverage, throttle and retry floor", () => {
    const sim = new Sim();
    sim.briefs.set("claude", { revision: 5, ready: true, lastAttempt: T0 - 10_000 }); // covered 5, throttled until T0+110 s
    sim.briefs.set("codex", { revision: 4, ready: true, lastAttempt: T0 - 600_000 }); // not covered
    sim.committed.set("claude", 5);
    sim.commit("claude", 6, { lose: true });
    sim.commit("codex", 5, { lose: true });
    const claudeRow = sim.rows.get("claude")!;
    claudeRow.state = { ...claudeRow.state, backoff: true }; // claude is in a retry backoff
    const claudeBefore = { ...claudeRow.state };
    sim.now += 5_000;
    const codexBefore = { ...sim.rows.get("codex")!.state };
    sim.consume(v1(hint("end", 5, T0, "codex"))); // a legacy v1 hint at revision 5: wakes both harnesses' rows
    // claude: revision 5 is covered, and it is in backoff: unchanged, not claimed.
    expect(sim.rows.get("claude")!.state).toEqual(claudeBefore);
    // codex: uncovered, but an ambiguous v1 end ends nothing: woken like an append (its own due, nothing earlier).
    expect(sim.rows.get("codex")!.state.dueAt).toBe(codexBefore.dueAt);
    expect(sim.runs).toHaveLength(0);
    // A v1 hint naming claude's new revision 6 still respects claude's backoff (its retry floor).
    sim.consume(v1(hint("end", 6, T0, "claude")));
    expect(sim.rows.get("claude")!.state).toEqual(claudeBefore);
    // Each harness's own eligibility decides: codex is claimed at its own due time (consumed copies hand off).
    sim.advance(15 * 60_000);
    expect(sim.runs.map((r) => r.harness)).toContain("codex");
    expect(sim.runs.find((r) => r.harness === "codex")!.claimableAt).toBe(codexBefore.dueAt);
  });

  test("a retry after a failed run: the row stays, a replayed hint neither delays nor duplicates it", () => {
    const sim = new Sim({ tickSeconds: 60 });
    const A = sim.commit("claude", 1, { lose: true });
    sim.failRuns = 1;
    sim.advance(70_000); // the minute poll claims the first brief; the run fails and reschedules 60 s out
    expect(sim.runs).toHaveLength(1);
    const rescheduled = sim.rows.get("claude")!.state.dueAt;
    sim.consume(A); // a replay during the backoff: it must not pull the retry forward
    expect(sim.runs).toHaveLength(1);
    expect(sim.rows.get("claude")!.state.dueAt).toBe(rescheduled);
    expect(planHint(sim.rows.get("claude")!.state, hint("end", 1, T0), { nowMs: sim.now, hasBrief: false, timing: TIMING })).toBeNull();
    sim.advance(3 * 60_000);
    expect(sim.runs).toHaveLength(2);
    expect(sim.rows.size).toBe(0);
  });

  test("sweep fixture 1, hosted: explicit 300 s with one-minute ticks and due-time gating", () => {
    const sim = new Sim({ hosted: true, tickSeconds: 60, sweepSeconds: briefTriggerConfig({ SESSION_BRIEF_SWEEP_SECONDS: "300" }).sweepSeconds });
    sim.briefs.set("claude", { revision: 0, ready: true, lastAttempt: T0 - 600_000 });
    sim.commit("claude", 1, { lose: true }); // armed, the hint lost: the one-minute tick checks due work
    sim.advance(9 * 60_000);
    expect(sim.runs).toHaveLength(0); // not due yet: no tick claims early
    sim.advance(3 * 60_000);
    expect(sim.runs).toHaveLength(1);
    // The actual claim time, recorded: the first minute tick on or after the due time (T0 + 600 s).
    expect(sim.runs[0]!.claimedAt).toBe(T0 + 600_000);
    expect(sim.runs[0]!.claimedAt - sim.runs[0]!.claimableAt).toBeLessThanOrEqual(60_000); // diagnostic: due to claim
    expect(briefMiss(T0, sim.runs[0]!.claimedAt)).toBe(true); // scored from the upload: a 600 s refresh window misses
    // A lost ARM (no wakeup row at all) is recovered only by the sweep, at its 300 s cadence.
    sim.now += 1;
    sim.commit("claude", 2, { lose: true, loseArm: true });
    const lostAt = sim.now;
    sim.advance(20 * 60_000);
    expect(sim.sweptAt.length).toBeGreaterThanOrEqual(1);
    expect(sim.sweptAt[0]! - lostAt).toBeLessThanOrEqual(300_000 + 60_000);
    expect(sim.runs).toHaveLength(2);
  });

  test("sweep fixture 2, native Helm default 60 s: the poll sweeps every minute", () => {
    const sim = new Sim({ tickSeconds: 60, sweepSeconds: briefTriggerConfig({ SESSION_BRIEF_SWEEP_SECONDS: "60" }).sweepSeconds });
    sim.commit("claude", 1, { lose: true, loseArm: true }); // a first brief whose arm was lost
    sim.advance(3 * 60_000);
    expect(sim.sweptAt).toHaveLength(1);
    expect(sim.sweptAt[0]! - T0).toBeLessThanOrEqual(60_000);
    expect(sim.runs).toHaveLength(1);
    expect(sim.runs[0]!.claimedAt - sim.sweptAt[0]!).toBeLessThanOrEqual(60_000 + 8_000);
  });

  for (const env of [{}, { SESSION_BRIEF_SWEEP_SECONDS: "0" }] as const) test(`sweep fixture 3, direct worker ${"SESSION_BRIEF_SWEEP_SECONDS" in env ? "explicit 0" : "unset"}: no sweep; armed rows still claimed by the poll`, () => {
    const sweepSeconds = briefTriggerConfig({ SESSION_BRIEF_ENABLED: "true", ...env }).sweepSeconds;
    expect(sweepSeconds).toBe(0);
    const sim = new Sim({ tickSeconds: 60, sweepSeconds });
    sim.commit("claude", 1, { lose: true }); // armed, hint lost: the poll still claims it
    sim.commit("codex", 1, { lose: true, loseArm: true }); // arm lost: never recovered without the sweep
    sim.advance(20 * 60_000);
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude"]);
    expect(sim.sweptAt).toHaveLength(0);
  });

  test("two harnesses share a session, E2 (a confirmed end at revision 2) in only one: only that one is ended", () => {
    const sim = new Sim();
    for (const h of ["claude", "codex"]) sim.briefs.set(h, { revision: 1, ready: true, lastAttempt: T0 - 600_000 });
    sim.commit("claude", 2, { lose: true });
    sim.commit("codex", 2, { lose: true });
    const codexBefore = { ...sim.rows.get("codex")!.state };
    sim.now += 10_000;
    sim.consume(hint("end", 2, T0, "claude")); // E2, v2, claude only: the coalescing wait is removed for claude
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude"]);
    expect(sim.rows.get("codex")!.state).toEqual(codexBefore); // codex untouched
    // A v1 end at revision 2 is ambiguous: it wakes codex like an append (no early 'ended' claim).
    sim.consume(v1(hint("end", 2, T0, "codex")));
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude"]);
    expect(sim.rows.get("codex")!.state.dueAt).toBe(codexBefore.dueAt);
  });

  test("the same session id in two harnesses: v2 wakes only its own; v1 wakes both; coverage is per harness", () => {
    const sim = new Sim();
    for (const h of ["claude", "codex"]) sim.briefs.set(h, { revision: 0, ready: true, lastAttempt: T0 - 600_000 });
    sim.commit("claude", 1, { lose: true });
    sim.commit("codex", 1, { lose: true });
    sim.now += 10_000;
    sim.consume(hint("end", 1, T0, "claude")); // v2: claude only
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude"]);
    expect(sim.rows.has("codex")).toBe(true);
    sim.now += 10_000;
    sim.consume(v1(hint("end", 1, T0, "codex"))); // v1: wakes every harness's row, but an ambiguous end ends none
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude"]);
    sim.consume(hint("end", 1, T0, "codex")); // the confirmed (v2) end for codex
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude", "codex"]);
    sim.commit("codex", 2, { lose: true });
    sim.now += 10_000;
    sim.consume(v1(hint("end", 1, T0, "claude"))); // a revision-1 hint is covered for codex too
    expect(sim.runs).toHaveLength(2);
    sim.consume(hint("end", 2, T0, "codex"));
    expect(sim.runs).toHaveLength(2); // codex's last attempt was 10 s ago: the refresh throttle holds it
    sim.advance(130_000); // the handed-off copy arrives once the throttle releases
    expect(sim.runs.map((r) => r.harness)).toEqual(["claude", "codex", "codex"]);
    expect(sim.runs[2]!.claimableAt).toBe(sim.runs[1]!.claimedAt + THROTTLE_S * 1_000);
  });

  test("continuous arrivals on a briefed session: refreshes bounded by max-wait", () => {
    const sim = new Sim();
    sim.briefs.set("claude", { revision: 0, ready: true, lastAttempt: T0 - 600_000 });
    for (let i = 0; i < 120; i++) { sim.commit("claude", i + 1); sim.advance(60_000); }
    sim.advance(15 * 60_000);
    expect(sim.runs[0]!.claimableAt).toBe(T0 + 3_600_000);
    for (let i = 1; i < sim.runs.length - 1; i++) expect(sim.runs[i]!.claimedAt - sim.runs[i - 1]!.claimedAt).toBeGreaterThanOrEqual(3_600_000 - 60_000);
  });

  test("handoff never exceeds SQS's 15-minute delay; a long wait chains hops", () => {
    expect(handoffPlan(T0 + 3_600_000, T0)).toEqual({ action: "defer", delaySeconds: SQS_MAX_DELAY_SECONDS });
    expect(handoffPlan(T0 + 3_600_000, T0 + 3_599_500)).toEqual({ action: "defer", delaySeconds: 1 });
    expect(handoffPlan(T0 + 3_600_000, T0 + 3_600_000)).toEqual({ action: "run" });
  });
});

describe("session-brief trigger: durable commit identity (design r2)", () => {
  const id = (revision: number, o: { generation?: string; sha?: string; ended?: boolean } = {}) =>
    ({ generation: o.generation ?? "1", revision, sha256: o.sha ?? "a".repeat(64), ended: o.ended ?? false });

  test("an exact replay is a no-op; a committed replacement at the same machine revision arms", () => {
    const a = id(5);
    expect(armIdentity(null, null, a)).toEqual(a); // first commit: arms
    expect(armIdentity(a, null, id(5))).toBeNull(); // exact replay (or the indexer seeing the same commit): no-op
    expect(armIdentity(a, null, id(5, { sha: "b".repeat(64) }))).toEqual(id(5, { sha: "b".repeat(64) })); // new content
    expect(armIdentity(a, null, id(5, { generation: "2" }))).toEqual(id(5, { generation: "2" })); // head recreated
    expect(armIdentity(null, a, id(5))).toBeNull(); // consumed: covered, after completion
    expect(armIdentity(null, a, id(5, { generation: "2" }))).toEqual(id(5, { generation: "2" })); // a replacement is not covered
    expect(armIdentity(a, null, id(6))).toEqual(id(6)); // B: new evidence
  });

  test("a standalone end (no revision increment) arms once; a replayed pre-end append cannot clear it or re-arm", () => {
    const append = id(5), end = id(5, { ended: true });
    // Before any consumption: the end over the armed append arms (due now on the wakeup side).
    expect(armIdentity(append, null, end)).toEqual(end);
    expect(armIdentity(end, null, end)).toBeNull(); // a repeated end: no-op
    expect(armIdentity(end, null, append)).toBeNull(); // the pre-end append replayed after the end: the end stands
    // After the append was consumed, the standalone end is still new work (the session ended).
    expect(armIdentity(append, append, end)).toEqual(end);
    // After the end was consumed: neither the end nor the pre-end append re-arms.
    expect(armIdentity(end, end, end)).toBeNull();
    expect(armIdentity(end, end, append)).toBeNull();
    expect(armIdentity(null, end, append)).toBeNull(); // even with no armed record left
  });

  test("standalone end eligibility: a confirmed first end is due now; with a brief, coalescing is skipped and the throttle applies", () => {
    // First brief: a confirmed (v2) end makes the armed row due now, never throttled.
    const first = planArm(null, T0, false, TIMING);
    const firstEnd = planHint(first, hint("end", 1, T0), { nowMs: T0 + 1_000, hasBrief: false, timing: TIMING })!;
    expect(firstEnd.dueAt).toBe(T0 + 1_000);
    expect(claimAt(firstEnd, { hasBrief: false, lastAttemptMs: T0, throttleSeconds: THROTTLE_S })).toBe(T0 + 1_000);
    // Existing brief: the end removes the coalescing (idle) wait, but the refresh throttle still applies.
    const refresh = planArm(null, T0, true, TIMING);
    const refreshEnd = planHint(refresh, hint("end", 2, T0), { nowMs: T0 + 1_000, hasBrief: true, coveredRevision: 1, timing: TIMING })!;
    expect(refreshEnd.dueAt).toBe(T0 + 1_000);
    expect(claimAt(refreshEnd, { hasBrief: true, lastAttemptMs: T0 - 30_000, throttleSeconds: THROTTLE_S })).toBe(T0 - 30_000 + THROTTLE_S * 1_000);
  });
});
