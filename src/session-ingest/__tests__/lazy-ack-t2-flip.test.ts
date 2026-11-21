/** lazy-ack-t2-flip.ts: one autocommit statement per attempt, bounded backoff, "no_switch" as a clean result. */
import { describe, expect, test } from "bun:test";
import { FLIP_STATEMENT, flipLazyAckT2Mode, isFlipBusy, type FlipQuery } from "../lazy-ack-t2-flip.ts";

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
      .toEqual({ result: "no_switch", to: "t2", attempts: 6, elapsedMs: 100, reason: "barrier_busy_budget_exhausted" });
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
});
