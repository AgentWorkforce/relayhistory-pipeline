/** lazy-ack-t2-flip.ts: one autocommit statement per attempt, bounded backoff, "no_switch" as a clean result. */
import { describe, expect, test } from "bun:test";
import { FLIP_STATEMENT, LazyAckT2FlipNotSwitched, MODE_READBACK, flipLazyAckT2Mode as throwingFlip, isConnectionLoss, isFlipBusy, lazyAckT2FlipOutcome as flipLazyAckT2Mode, lazyAckT2FlipOutcome as lazyAckT2Outcome, type FlipQuery } from "../lazy-ack-t2-flip.ts";

const busy = () => Object.assign(Error("T2 staged ACK: flip barrier busy (an ingest or erase transaction is open); mode unchanged, retry"), { code: "55P03" });
const rowBusy = () => Object.assign(Error('could not obtain lock on row in relation "lazy_ack_t2_mode"'), { code: "55P03" });
function clock() { let t = 0; return { now: () => t, sleep: async (ms: number) => { t += ms; }, random: () => 0.5 }; }

describe("flipLazyAckT2Mode", () => {
  test("switched on the first free attempt; the statement is exactly FLIP_STATEMENT, never a BEGIN", async () => {
    const seen: string[] = [];
    const q: FlipQuery = async (sql, p) => { seen.push(sql); expect(p).toEqual(["t2"]); return { rows: [{ old: "main" }] }; };
    expect(await flipLazyAckT2Mode(q, "t2", clock())).toEqual({ result: "switched", from: "main", to: "t2", attempts: 1, elapsedMs: 0 });
    expect(seen).toEqual([FLIP_STATEMENT]);
  });
  test("busy (barrier or mode row) is retried with growing, capped backoff, then switched", async () => {
    const c = clock(), waits: number[] = [];
    let n = 0;
    const q: FlipQuery = async () => { n++; if (n < 4) throw n === 2 ? rowBusy() : busy(); return { rows: [{ old: "t2" }] }; };
    const r = await flipLazyAckT2Mode(q, "main", { ...c, sleep: async (ms) => { waits.push(ms); await c.sleep(ms); }, backoffMs: 10, maxBackoffMs: 30 });
    expect(r).toMatchObject({ result: "switched", from: "t2", to: "main", attempts: 4 });
    expect(waits).toEqual([5, 10, 15]); // full jitter (random 0.5) of 10, 20, 30 (capped)
  });
  test("budget exhausted: a clean no_switch result, not an error", async () => {
    const q: FlipQuery = async () => { throw busy(); };
    expect(await flipLazyAckT2Mode(q, "t2", { ...clock(), budgetMs: 100, backoffMs: 40, maxBackoffMs: 40 }))
      .toEqual({ result: "no_switch", to: "t2", attempts: 5, elapsedMs: 100, reason: "barrier_busy_budget_exhausted" }); // starts at 0,20,40,60,80; none at 100
    expect(await flipLazyAckT2Mode(q, "t2", { ...clock(), maxAttempts: 3 })).toMatchObject({ result: "no_switch", attempts: 3 });
  });
  test("any other error is thrown at once (not retried): wrong role, other lock timeouts, bad mode", async () => {
    for (const e of [Object.assign(Error("permission denied for function lazy_ack_t2_set_mode"), { code: "42501" }),
      Object.assign(Error("canceling statement due to lock timeout"), { code: "55P03" }), Object.assign(Error("x"), { code: "22023" })]) {
      let n = 0;
      await expect(flipLazyAckT2Mode(async () => { n++; throw e; }, "t2", clock())).rejects.toBe(e);
      expect(n).toBe(1);
    }
    await expect(flipLazyAckT2Mode(async () => ({ rows: [] }), "x" as never)).rejects.toThrow(TypeError);
    await expect(flipLazyAckT2Mode(async () => ({ rows: [{ old: null }] }), "t2", clock())).rejects.toThrow(/unexpected previous mode/);
  });
  test("isFlipBusy recognises only the two busy refusals", () => {
    expect([busy(), rowBusy()].map(isFlipBusy)).toEqual([true, true]);
    expect([Object.assign(Error("canceling statement due to lock timeout"), { code: "55P03" }), Object.assign(busy(), { code: "40001" }), null].map(isFlipBusy)).toEqual([false, false, false]);
  });
  test("the budget is a retry-start deadline: a sleep that lands exactly on, or past, the deadline starts no further attempt", async () => {
    for (const overshoot of [0, 1, 250]) {
      let t = 0, n = 0;
      // Busy once; the next attempt would succeed. The single backoff sleep ends at (or after) the deadline.
      const q: FlipQuery = async () => { n++; if (n === 1) throw busy(); return { rows: [{ old: "main" }] }; };
      const r = await flipLazyAckT2Mode(q, "t2", { budgetMs: 100, backoffMs: 1000, maxBackoffMs: 1000, random: () => 0.999, now: () => t,
        sleep: async (ms) => { expect(ms).toBeLessThanOrEqual(100); t += ms + overshoot; } });
      expect({ overshoot, r, n }).toEqual({ overshoot, r: { result: "no_switch", to: "t2", attempts: 1, elapsedMs: 100 + overshoot, reason: "barrier_busy_budget_exhausted" }, n: 1 });
    }
    // One millisecond before the deadline the next attempt still starts (and here succeeds).
    let t = 0, n = 0;
    const q: FlipQuery = async () => { n++; if (n === 1) throw busy(); return { rows: [{ old: "main" }] }; };
    expect(await flipLazyAckT2Mode(q, "t2", { budgetMs: 100, now: () => t, random: () => 0, sleep: async () => { t = 99; } }))
      .toMatchObject({ result: "switched", attempts: 2 });
  });
  test("an attempt already sent before the deadline is allowed to finish (it never waits on a lock)", async () => {
    let t = 0;
    const q: FlipQuery = async () => { t = 500; return { rows: [{ old: "t2" }] }; }; // the reply arrives after the deadline
    expect(await flipLazyAckT2Mode(q, "main", { budgetMs: 100, now: () => t })).toEqual({ result: "switched", from: "t2", to: "main", attempts: 1, elapsedMs: 500 });
  });
  test("connection lost after dispatch: the mode is read back on a new statement; the result is indeterminate, never assumed", async () => {
    for (const [err, back, observed] of [
      [Object.assign(Error("Connection terminated unexpectedly"), {}), "t2", "t2"],
      [Object.assign(Error("terminating connection due to administrator command"), { code: "57P01" }), "main", "main"],
      [Object.assign(Error("read ECONNRESET"), { code: "ECONNRESET" }), null, null],
    ] as const) {
      const seen: string[] = [];
      const q: FlipQuery = async (sql) => {
        seen.push(sql);
        if (sql === FLIP_STATEMENT) throw err;
        if (back === null) throw Error("still down");
        return { rows: [{ mode: back }] };
      };
      expect(await flipLazyAckT2Mode(q, "t2", clock())).toMatchObject({ result: "indeterminate", to: "t2", attempts: 1, observedMode: observed });
      expect(seen).toEqual([FLIP_STATEMENT, MODE_READBACK]);
    }
  });
  test("isConnectionLoss: no SQLSTATE, class 08 or 57P01-03 only", () => {
    expect([Error("x"), { code: "ECONNRESET" }, { code: "08006" }, { code: "57P01" }].map(isConnectionLoss)).toEqual([true, true, true, true]);
    expect([{ code: "55P03" }, { code: "42501" }, { code: "57014" }, { code: "40P01" }].map(isConnectionLoss)).toEqual([false, false, false, false]);
  });
  test("flipLazyAckT2Mode resolves only when switched: no_switch and indeterminate THROW a dedicated, readable error carrying the outcome", async () => {
    expect(await throwingFlip(async () => ({ rows: [{ old: "main" }] }), "t2", clock())).toMatchObject({ result: "switched" });
    const ns = await throwingFlip(async () => { throw busy(); }, "t2", { ...clock(), budgetMs: 50 }).then(() => null, (e) => e);
    expect(ns).toBeInstanceOf(LazyAckT2FlipNotSwitched);
    expect(ns.name).toBe("LazyAckT2NoSwitch");
    expect(ns.outcome).toMatchObject({ result: "no_switch", to: "t2" });
    expect(ns.message).toContain("mode unchanged");
    const ind = await throwingFlip(async (sql) => { if (sql === FLIP_STATEMENT) throw Error("Connection terminated"); return { rows: [{ mode: "t2" }] }; }, "t2", clock()).then(() => null, (e) => e);
    expect(ind).toBeInstanceOf(LazyAckT2FlipNotSwitched);
    expect(ind.name).toBe("LazyAckT2FlipIndeterminate");
    expect(ind.outcome).toMatchObject({ result: "indeterminate", observedMode: "t2" });
  });
  test("options must be finite and bounded; with valid options the first attempt is always dispatched", async () => {
    let n = 0;
    const q: FlipQuery = async () => { n++; return { rows: [{ old: "main" }] }; };
    for (const bad of [{ budgetMs: 0 }, { budgetMs: -1 }, { budgetMs: NaN }, { budgetMs: Infinity }, { maxAttempts: 0 }, { maxAttempts: 1.5 },
      { maxAttempts: Infinity }, { backoffMs: -1 }, { backoffMs: NaN }, { maxBackoffMs: Infinity }, { maxBackoffMs: -1 }])
      await expect(lazyAckT2Outcome(q, "t2", { ...clock(), ...bad })).rejects.toThrow(TypeError);
    expect(n).toBe(0); // nothing dispatched for invalid options
    // Smallest valid bounds: exactly one attempt, dispatched even though the budget is tiny.
    let t = 0;
    const busyOnce: FlipQuery = async () => { n++; t += 5; throw busy(); };
    expect(await lazyAckT2Outcome(busyOnce, "t2", { budgetMs: 1, maxAttempts: 1, now: () => t, sleep: async () => {} }))
      .toMatchObject({ result: "no_switch", attempts: 1 });
    expect(n).toBe(1);
  });
});
