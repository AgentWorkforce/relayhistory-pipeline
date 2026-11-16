/**
 * The self-host upload-inbox promotion pass (selfhost-admission.ts promoteUploads): fair across uploading
 * origins, oldest first, bounded concurrency, the batch bound and the per-key backoff, and the scan's log
 * fields (counts only, never a key).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey } from "../../shared/vendor.ts";
import { sanitizeLogField } from "../../shared/log.ts";
import { selectUploadsFairly, selfhostAdmissionConsumer, uploadOriginOf, uploadResultsField, type UploadCandidate } from "../selfhost-admission.ts";
import { uploadInboxRoot, type PromoteResult } from "../upload-admission.ts";
import { MemoryLedger, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const ROOT = "96bulls";
const key = (origin: string, n: number) => `${uploadInboxRoot(ROOT)}u=x/o=${origin}/b=${String(n).padStart(32, "0")}/d=${"a".repeat(32)}-w0.json.zst`;
const cand = (origin: string, n: number, at: number): UploadCandidate => ({ key: key(origin, n), lastModifiedMs: at, origin });

describe("selectUploadsFairly", () => {
  test("one origin with thousands of objects cannot starve the others: round-robin, oldest first per origin", () => {
    const big = Array.from({ length: 5000 }, (_, i) => cand("big", i, 1_000 + i));
    const small = [cand("s1", 0, 9_000), cand("s1", 1, 8_000), cand("s2", 0, 50_000)];
    const out = selectUploadsFairly([...big, ...small], 6);
    // Origins ordered by their oldest object (big 1000, s1 8000, s2 50000), then one each per round.
    expect(out).toEqual([key("big", 0), key("s1", 1), key("s2", 0), key("big", 1), key("s1", 0), key("big", 2)]);
  });

  test("bounded by the limit and by what is due; an empty input is empty", () => {
    expect(selectUploadsFairly([], 10)).toEqual([]);
    expect(selectUploadsFairly([cand("a", 0, 1), cand("b", 0, 2)], 10)).toHaveLength(2);
    expect(selectUploadsFairly(Array.from({ length: 50 }, (_, i) => cand("a", i, i)), 7)).toHaveLength(7);
  });

  test("the origin is the o= segment; a key without one groups under the empty origin", () => {
    expect(uploadOriginOf(key("abc123", 1))).toBe("abc123");
    expect(uploadOriginOf(`${uploadInboxRoot(ROOT)}junk`)).toBe("");
  });
});

describe("the promotion pass", () => {
  async function seed(n: number, origins: string[]) {
    const s = scratchStore();
    cleanups.push(s.done);
    for (let i = 0; i < n; i++) await s.store.put(objectKey(key(origins[i % origins.length]!, i)), new Uint8Array([1]));
    return s.store;
  }

  test("promotes with bounded concurrency: never more than `concurrency` in flight, and all `batchSize` chosen", async () => {
    const store = await seed(40, ["o1", "o2", "o3"]);
    let inFlight = 0, peak = 0;
    const promoted: string[] = [];
    const promote = async (k: string): Promise<PromoteResult> => {
      inFlight++; peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 5));
      inFlight--; promoted.push(k);
      return { action: "ack", outcome: "handed_off" };
    };
    const lines: Record<string, unknown>[] = [];
    const sink = (_m: string, f?: Record<string, unknown>) => { if (f) lines.push(f); };
    const consumer = selfhostAdmissionConsumer({
      mode: "on", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 3, log: { info: sink, warn: sink, error: sink },
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "canonical", target: { store, keyRoot: ROOT } },
      uploadInbox: { promote, batchSize: 25, concurrency: 4 },
    });
    await consumer.scan();
    await consumer.settle();
    await consumer.stop();
    expect(promoted).toHaveLength(25);
    expect(new Set(promoted).size).toBe(25);
    expect(peak).toBeLessThanOrEqual(4);
    expect(peak).toBeGreaterThan(1);
    const line = lines.find((l) => l.route === "ingest.upload_admission")!;
    expect(line).toMatchObject({ outcome: "ok", promoted: 25, upload_due: 40, upload_origins: 3, upload_results: "handed_off:25" });
  });

  test("a retry backs the key off: the next pass skips it and promotes the others", async () => {
    const store = await seed(3, ["o1"]);
    const calls: string[] = [];
    const promote = async (k: string): Promise<PromoteResult> => {
      calls.push(k);
      return k === key("o1", 0) ? { action: "retry", code: "upload_put_time_unavailable" } : { action: "retry", code: "busy" };
    };
    const consumer = selfhostAdmissionConsumer({
      mode: "on", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 3, nowMs: () => 1_000_000, log: { info: () => {}, warn: () => {}, error: () => {} },
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "canonical", target: { store, keyRoot: ROOT } },
      uploadInbox: { promote, batchSize: 10, concurrency: 8 },
    });
    await consumer.scan();
    await consumer.settle();
    expect(calls).toHaveLength(3);
    await consumer.scan();
    await consumer.settle();
    await consumer.stop();
    // Same clock: every key is inside its backoff window, so the second pass promotes nothing.
    expect(calls).toHaveLength(3);
  });
});

describe("readiness during a long promotion pass", () => {
  async function seed(n: number) {
    const s = scratchStore();
    cleanups.push(s.done);
    for (let i = 0; i < n; i++) await s.store.put(objectKey(key("o1", i)), new Uint8Array([1]));
    return s.store;
  }
  const quiet = { info: () => {}, warn: () => {}, error: () => {} };
  /** A consumer whose promote() advances the clock by `stepMs` per object and lets the test observe status mid-pass. */
  function consumerWith(store: Awaited<ReturnType<typeof seed>>, promote: (k: string) => Promise<PromoteResult>, clock: { t: number }) {
    return selfhostAdmissionConsumer({
      mode: "on", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 3, nowMs: () => clock.t, log: quiet,
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "canonical", target: { store, keyRoot: ROOT } },
      uploadInbox: { promote, batchSize: 10, concurrency: 1 },
    });
  }

  test("a pass far longer than the stale window stays ok while it makes progress", async () => {
    const store = await seed(10);
    const clock = { t: 1_000_000 };
    const states: string[] = [];
    let consumer!: ReturnType<typeof consumerWith>;
    consumer = consumerWith(store, async () => {
      clock.t += 20_000; // 10 promotions x 20 s = 200 s, more than 3x the 60 s window
      states.push(consumer.status().state);
      return { action: "ack", outcome: "handed_off" };
    }, clock);
    await consumer.scan();
    await consumer.settle();
    expect(states).toHaveLength(10);
    expect(states.every((s) => s === "ok")).toBe(true);
    // After the pass the progress clock is cleared: idle behaviour is the admission-scan age again.
    expect(consumer.status().lastProgressAgeMs).toBeNull();
    expect(consumer.status().state).toBe("stale"); // 200 s since the scan, no pass running
    await consumer.stop();
  });

  test("a pass stuck with no progress for the window goes stale", async () => {
    const store = await seed(2);
    const clock = { t: 2_000_000 };
    let release!: () => void;
    const stuck = new Promise<void>((r) => { release = r; });
    let consumer!: ReturnType<typeof consumerWith>;
    let entered!: () => void;
    const inPromote = new Promise<void>((r) => { entered = r; });
    consumer = consumerWith(store, async () => { entered(); await stuck; return { action: "ack", outcome: "handed_off" }; }, clock);
    const pass = consumer.scan();
    await inPromote;
    clock.t += 30_000;
    expect(consumer.status().state).toBe("ok");
    expect(consumer.status().lastProgressAgeMs).toBe(30_000);
    clock.t += 31_000; // 61 s with no progress
    expect(consumer.status().state).toBe("stale");
    expect(consumer.status().healthy).toBe(false);
    release();
    await pass;
    await consumer.settle();
    await consumer.stop();
  });

  test("idle behaviour is unchanged: ok within the window after a scan, stale beyond it, starting before any", async () => {
    const store = await seed(0);
    const clock = { t: 3_000_000 };
    const consumer = consumerWith(store, async () => ({ action: "ack", outcome: "handed_off" }), clock);
    expect(consumer.status().state).toBe("starting");
    await consumer.scan();
    await consumer.settle();
    expect(consumer.status()).toMatchObject({ state: "ok", lastProgressAgeMs: null });
    clock.t += 60_000;
    expect(consumer.status().state).toBe("ok");
    clock.t += 1;
    expect(consumer.status().state).toBe("stale");
    await consumer.stop();
  });
});

describe("the pass's log fields", () => {
  test("counts survive the sanitizer as bounded integers; the results string carries only fixed codes", () => {
    expect(sanitizeLogField("promoted", 25)).toBe(25);
    expect(sanitizeLogField("waiting", -1)).toBeUndefined();
    expect(sanitizeLogField("upload_due", "x")).toBeUndefined();
    expect(sanitizeLogField("upload_origins", 3)).toBe(3);
    expect(uploadResultsField({ handed_off: 3, upload_witness_expired: 1, [key("o1", 1)]: 9 })).toBe("handed_off:3,upload_witness_expired:1");
    expect(sanitizeLogField("upload_results", "handed_off:3")).toBe("handed_off:3");
  });
});
