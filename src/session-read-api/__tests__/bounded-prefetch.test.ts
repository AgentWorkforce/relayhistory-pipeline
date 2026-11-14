/** bounded-prefetch.ts: ordered, bounded-concurrency canonical chunk prefetch (READ_API_CANONICAL_FETCH_CONCURRENCY). */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { boundedPrefetch } from "../bounded-prefetch.ts";

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));
let unhandled: unknown[] = [];
const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
beforeEach(() => { unhandled = []; process.on("unhandledRejection", onUnhandled); });
afterEach(() => { process.off("unhandledRejection", onUnhandled); });
/** Lets any rejection that nobody handles reach the process listener before asserting. */
const settle = async () => { await tick(30); await new Promise((r) => setImmediate(r)); };

/** A fetch that records the peak number in flight; item i resolves after a jittered delay, out of order. */
function tracked(fail?: (i: number) => boolean) {
  const state = { inFlight: 0, peak: 0, started: [] as number[], settled: 0 };
  const fetch = async (i: number) => {
    state.started.push(i);
    state.peak = Math.max(state.peak, ++state.inFlight);
    try {
      await tick(((i * 7) % 5) + 1);
      if (fail?.(i)) throw new Error(`fetch ${i} failed`);
      return `body-${i}`;
    } finally { state.inFlight--; state.settled++; }
  };
  return { state, fetch };
}
const indices = (n: number) => Array.from({ length: n }, (_, i) => i);

test("50 items at N=8: strictly input order, more than one and at most 8 in flight", async () => {
  const { state, fetch } = tracked();
  const out: string[] = [];
  for await (const v of boundedPrefetch(indices(50), 8, fetch)) out.push(v);
  expect(out).toEqual(indices(50).map((i) => `body-${i}`));
  expect(state.peak).toBeGreaterThan(1);
  expect(state.peak).toBeLessThanOrEqual(8);
  expect(state.started).toEqual(indices(50));
  await settle();
  expect(unhandled).toEqual([]);
});

test("in flight plus held never exceeds N: item i+N starts only after the consumer took item i", async () => {
  const { state, fetch } = tracked();
  const n = 4;
  let taken = 0;
  for await (const _ of boundedPrefetch(indices(20), n, fetch)) {
    taken++;
    // While the consumer holds item `taken-1`, at most items up to taken-1+n-1 were started.
    expect(state.started.length).toBeLessThanOrEqual(taken - 1 + n);
    await tick(2);
  }
  expect(taken).toBe(20);
});

test.each([[1], [0], [-3], [Number.NaN]])("N=%p is sequential: never more than one in flight, each started on demand", async (n) => {
  const { state, fetch } = tracked();
  const out: string[] = [];
  for await (const v of boundedPrefetch(indices(12), n, fetch)) {
    out.push(v);
    // Nothing is fetched ahead of the consumer.
    expect(state.started.length).toBe(out.length);
  }
  expect(out).toEqual(indices(12).map((i) => `body-${i}`));
  expect(state.peak).toBe(1);
});

test.each([[8], [1]])("N=%p: a rejection at index 3 surfaces at index 3 after 0..2, with no unhandled sibling rejection", async (n) => {
  // Every item from 3 on rejects, so in-flight siblings reject after the consumer stopped.
  const { state, fetch } = tracked((i) => i >= 3);
  const out: string[] = [];
  let caught: unknown;
  try { for await (const v of boundedPrefetch(indices(20), n, fetch)) out.push(v); } catch (error) { caught = error; }
  expect(out).toEqual(["body-0", "body-1", "body-2"]);
  expect((caught as Error).message).toBe("fetch 3 failed");
  await settle();
  expect(state.settled).toBe(state.started.length); // every started sibling finished (rejected) ...
  expect(unhandled).toEqual([]); // ... and none was unhandled
  // No fetch is started after the failing position was consumed.
  expect(Math.max(...state.started)).toBeLessThan(3 + Math.max(n, 1));
});

test("a fetch that throws synchronously rejects at its own position", async () => {
  const out: number[] = [];
  const gen = boundedPrefetch(indices(10), 4, (i) => { if (i === 2) throw new Error("sync 2"); return Promise.resolve(i); });
  await expect((async () => { for await (const v of gen) out.push(v); })()).rejects.toThrow("sync 2");
  expect(out).toEqual([0, 1]);
  await settle();
  expect(unhandled).toEqual([]);
});

test("early consumer break: no unhandled rejection, the stop signal aborts, no new fetch starts", async () => {
  const signals: AbortSignal[] = [];
  const { state, fetch } = tracked((i) => i >= 1);
  let started = 0;
  for await (const v of boundedPrefetch(indices(30), 8, (i, signal) => { signals.push(signal); started++; return i === 0 ? Promise.resolve("body-0") : fetch(i); })) {
    expect(v).toBe("body-0");
    break;
  }
  expect(started).toBe(8);
  expect(signals.every((s) => s.aborted)).toBe(true);
  await settle();
  expect(state.inFlight).toBe(0);
  expect(started).toBe(8);
  expect(unhandled).toEqual([]);
});

test("empty input yields nothing and fetches nothing", async () => {
  let calls = 0;
  for await (const _ of boundedPrefetch([], 8, async () => { calls++; })) throw new Error("unreachable");
  expect(calls).toBe(0);
});
