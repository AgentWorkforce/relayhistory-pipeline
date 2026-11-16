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

describe("readiness during a long admission scan", () => {
  const quiet = { info: () => {}, warn: () => {}, error: () => {} };
  type Hook = (page: number) => Promise<void> | void;
  /**
   * A store whose admission-inbox listing returns `pages` empty pages, calling `hook` before each, and which
   * can be told to fail the listing. Every other call goes to a real scratch store.
   */
  function pagedStore(pages: number, hook: Hook) {
    const s = scratchStore();
    cleanups.push(s.done);
    const ctl = { fail: false, pages, hook };
    const store = new Proxy(s.store, {
      get(target, prop, receiver) {
        if (prop !== "list") return Reflect.get(target, prop, receiver);
        return async (prefix: unknown, cursor?: string) => {
          if (!String(prefix).includes(".admission-inbox")) return target.list(prefix as never, cursor);
          const n = cursor === undefined ? 0 : Number(cursor);
          await ctl.hook(n);
          if (ctl.fail) throw Object.assign(new Error("listing failed"), { code: "list_failed" });
          return { items: [], ...(n + 1 < ctl.pages ? { nextCursor: String(n + 1) } : {}) };
        };
      },
    }) as typeof s.store;
    return { store, ctl };
  }
  function consumerOn(store: ReturnType<typeof pagedStore>["store"], clock: { t: number }) {
    return selfhostAdmissionConsumer({
      mode: "on", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 3, nowMs: () => clock.t, log: quiet,
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "canonical", target: { store, keyRoot: ROOT } },
    });
  }

  test("a 150 s admission scan with steady pages stays ready throughout", async () => {
    const clock = { t: 10_000_000 };
    let consumer!: ReturnType<typeof consumerOn>;
    const states: string[] = [];
    const { store, ctl } = pagedStore(1, () => {});
    consumer = consumerOn(store, clock);
    await consumer.scan(); // a first successful scan, so the consumer is no longer starting
    await consumer.settle();
    ctl.pages = 15;
    ctl.hook = (n) => { if (n > 0) { clock.t += 10_000; states.push(consumer.status().state); } };
    await consumer.scan(); // 15 pages, 10 s apart: 140 s of listing, more than twice the 60 s window
    await consumer.settle();
    expect(states).toHaveLength(14);
    expect(states.every((s) => s === "ok")).toBe(true);
    expect(consumer.status()).toMatchObject({ state: "ok", lastProgressAgeMs: null });
    await consumer.stop();
  });

  test("an admission scan with no page for 61 s goes stale", async () => {
    const clock = { t: 11_000_000 };
    const { store, ctl } = pagedStore(1, () => {});
    const consumer = consumerOn(store, clock);
    await consumer.scan();
    await consumer.settle();
    let release!: () => void;
    const stuck = new Promise<void>((r) => { release = r; });
    let entered!: () => void;
    const inList = new Promise<void>((r) => { entered = r; });
    ctl.pages = 3;
    ctl.hook = async (n) => { if (n === 1) { entered(); await stuck; } };
    const pass = consumer.scan();
    await inList; // page 0 is listed; the listing of page 1 hangs
    clock.t += 30_000;
    expect(consumer.status().state).toBe("ok");
    clock.t += 31_000; // 61 s since page 0
    expect(consumer.status().state).toBe("stale");
    release();
    await pass;
    await consumer.settle();
    await consumer.stop();
  });

  test("a failed admission scan never counts as progress, and neither do the next scan's pages", async () => {
    const clock = { t: 12_000_000 };
    const { store, ctl } = pagedStore(1, () => {});
    const consumer = consumerOn(store, clock);
    await consumer.scan();
    await consumer.settle();
    // A long scan that lists pages, then fails: its pages are withdrawn when it fails.
    ctl.pages = 10;
    ctl.hook = (n) => { clock.t += 10_000; if (n === 9) ctl.fail = true; };
    await consumer.scan();
    await consumer.settle();
    expect(consumer.status().state).toBe("stale"); // about 100 s since the last successful scan
    expect(consumer.status().lastProgressAgeMs).toBeNull();
    // The next scan lists pages again, but they do not count until a scan succeeds.
    ctl.fail = false;
    const states: string[] = [];
    ctl.hook = (n) => { clock.t += 10_000; if (n > 0) states.push(consumer.status().state); };
    await consumer.scan();
    await consumer.settle();
    expect(states.every((s) => s === "stale")).toBe(true);
    // That scan succeeded: the consumer is ok again.
    expect(consumer.status().state).toBe("ok");
    await consumer.stop();
  });

  test("startup: a 90 s first scan with steady pages is ready throughout, with a numeric lastScanAgeMs", async () => {
    const clock = { t: 14_000_000 };
    let consumer!: ReturnType<typeof consumerOn>;
    const seen: Array<{ state: string; age: number | null }> = [];
    const { store } = pagedStore(10, (n) => {
      if (n > 0) { clock.t += 10_000; const s = consumer.status(); seen.push({ state: s.state, age: s.lastScanAgeMs }); }
    });
    consumer = consumerOn(store, clock);
    expect(consumer.status().state).toBe("starting");
    await consumer.scan(); // first scan: 10 pages, 90 s of listing, longer than the startup budget
    await consumer.settle();
    expect(seen).toHaveLength(9);
    expect(seen.every((s) => s.state === "ok" && typeof s.age === "number" && s.age >= 0)).toBe(true);
    expect(consumer.status().state).toBe("ok");
    await consumer.stop();
  });

  test("startup: a first scan with no page for 61 s is not ready", async () => {
    const clock = { t: 15_000_000 };
    let release!: () => void;
    const stuck = new Promise<void>((r) => { release = r; });
    let entered!: () => void;
    const inList = new Promise<void>((r) => { entered = r; });
    const { store } = pagedStore(3, async (n) => { if (n === 1) { entered(); await stuck; } });
    const consumer = consumerOn(store, clock);
    const pass = consumer.scan();
    await inList; // page 0 listed, page 1 hangs
    clock.t += 30_000;
    expect(consumer.status().state).toBe("ok");
    clock.t += 31_000;
    expect(consumer.status()).toMatchObject({ state: "starting", lastScanAgeMs: null });
    release();
    await pass;
    await consumer.settle();
    await consumer.stop();
  });

  test("startup: a failed first scan withdraws its pages, and a progressing retry is ready", async () => {
    const clock = { t: 16_000_000 };
    const { store, ctl } = pagedStore(5, (n) => { clock.t += 10_000; if (n === 4) ctl.fail = true; });
    const consumer = consumerOn(store, clock);
    await consumer.scan(); // fails on its last page
    await consumer.settle();
    expect(consumer.status()).toMatchObject({ state: "starting", lastScanAgeMs: null, lastProgressAgeMs: null });
    ctl.fail = false;
    const states: string[] = [];
    ctl.hook = (n) => { clock.t += 10_000; if (n > 0) states.push(consumer.status().state); };
    await consumer.scan();
    await consumer.settle();
    expect(states.length).toBeGreaterThan(0);
    expect(states.every((s) => s === "ok")).toBe(true);
    expect(consumer.status().state).toBe("ok");
    await consumer.stop();
  });

  test("idle behaviour is unchanged when listings are short", async () => {
    const clock = { t: 13_000_000 };
    const { store } = pagedStore(2, () => {});
    const consumer = consumerOn(store, clock);
    expect(consumer.status().state).toBe("starting");
    await consumer.scan();
    await consumer.settle();
    expect(consumer.status()).toMatchObject({ state: "ok", lastProgressAgeMs: null });
    clock.t += 60_001;
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
