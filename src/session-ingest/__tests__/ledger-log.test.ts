/**
 * ledger-log.ts: the per-stream numbered-log commit (docs/design/s3-ledger-log-commit.md). Runs on the fs store,
 * whose create-if-absent semantics are the object-store conformance cases every backend (S3, GCS, Azure) passes.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { ObjectStoreError, objectKey, readAll, type ObjectStore, type PutOpts } from "../../shared/vendor.ts";
import {
  appendBatches, enableLogMode, ledgerLogCommitMode, LOG_CHECKPOINT_EVERY, LogCorrupt, LogFenced, logKeys, LogUnavailable,
  openStream, putPayload, readEntries, takeStream, type LogBatch,
} from "../ledger-log.ts";
import { selfhostLedgerLogCommit } from "../selfhost-config.ts";
import { TENANT, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => { while (cleanups.length) cleanups.pop()!(); });
function fresh() { const s = scratchStore(); cleanups.push(s.done); return s.store; }
const keys = logKeys("96bulls", TENANT, "origin-1");
const noSleep = { sleep: async () => {} };
const W = (writer: string) => ({ writer, now: () => 1_800_000_000_000, ...noSleep });

async function batch(store: ObjectStore, id: string, body = id): Promise<LogBatch> {
  const ref = await putPayload(store, keys, new TextEncoder().encode(body));
  return { batchId: id, digest: `d-${body}`, receiptId: `r-${id}`, sessions: [["claude", `s-${id}`]], payloads: [ref] };
}

/** Wraps a store; `plan` decides per entry PUT whether to fail before the write, after it, or not at all. */
function faulty(inner: ObjectStore, plan: (key: string, n: number) => "before" | "after" | "ok") {
  let n = 0;
  const wrapped: ObjectStore = {
    backend: inner.backend,
    head: (k) => inner.head(k), get: (k, r) => inner.get(k, r), getObject: (k, r) => inner.getObject(k, r),
    list: (p, c) => inner.list(p, c), presignGet: (k, t) => inner.presignGet(k, t), delete: (k) => inner.delete(k),
    put: inner.put.bind(inner),
  };
  wrapped.put = async (key: Parameters<ObjectStore["put"]>[0], body: Uint8Array, opts?: PutOpts) => {
    const k = String(key);
    const mode = k.includes("/e/") ? plan(k, ++n) : "ok";
    if (mode === "before") throw new ObjectStoreError("transient", "fs", "injected before", { status: 503 });
    const ack = await inner.put(key, body, opts);
    if (mode === "after") throw new ObjectStoreError("transient", "fs", "injected after", { status: 503 });
    return ack;
  };
  return wrapped;
}

describe("flag", () => {
  test("default off; on/off parse; anything else throws", () => {
    expect(ledgerLogCommitMode(undefined)).toBe("off");
    expect(ledgerLogCommitMode("")).toBe("off");
    expect(ledgerLogCommitMode(" off ")).toBe("off");
    expect(ledgerLogCommitMode("on")).toBe("on");
    expect(() => ledgerLogCommitMode("yes")).toThrow();
  });
  test("self-host refuses on until the admission path is wired", () => {
    expect(() => selfhostLedgerLogCommit({ INGEST_LEDGER_LOG_COMMIT: "on" } as NodeJS.ProcessEnv)).toThrow(/not available yet/);
    expect(() => selfhostLedgerLogCommit({ INGEST_LEDGER_LOG_COMMIT: "1" } as NodeJS.ProcessEnv)).toThrow(/must be off or on/);
    expect(selfhostLedgerLogCommit({} as NodeJS.ProcessEnv)).toBe("off");
  });
});

describe("log mode marker", () => {
  test("a stream with no entry 0 is legacy", async () => {
    expect(await openStream(fresh(), keys)).toBeUndefined();
  });
  test("enableLogMode is idempotent and the opener holds epoch 1", async () => {
    const store = fresh();
    const a = await enableLogMode(store, keys, "cut-1", W("w1"));
    const b = await enableLogMode(store, keys, "cut-1", W("w2"));
    expect(a.head).toBe(0); expect(b.head).toBe(0); expect(a.epoch).toBe(1);
  });
});

describe("append", () => {
  test("commits batches as one entry; replays and conflicts are not rewritten", async () => {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w1"));
    const r1 = await appendBatches(store, s, [await batch(store, "b1"), await batch(store, "b2")], "cur-1", W("w1"));
    expect(r1.seq).toBe(1); expect(r1.admitted.map((b) => b.batchId)).toEqual(["b1", "b2"]);
    const r2 = await appendBatches(store, s, [await batch(store, "b1"), await batch(store, "b2", "other"), await batch(store, "b3")], "cur-2", W("w1"));
    expect(r2.seq).toBe(2); expect(r2.replays).toEqual(["b1"]); expect(r2.conflicts).toEqual(["b2"]);
    expect(r2.admitted.map((b) => b.batchId)).toEqual(["b3"]);
    const r3 = await appendBatches(store, s, [await batch(store, "b3")], null, W("w1"));
    expect(r3.seq).toBeNull(); expect(r3.replays).toEqual(["b3"]);
    // A batch repeated within one call is a replay of itself, never written twice.
    const r4 = await appendBatches(store, s, [await batch(store, "b4"), await batch(store, "b4")], null, W("w1"));
    expect(r4.admitted.length).toBe(1); expect(r4.replays).toEqual(["b4"]);
  });

  test("a reopened stream has the same head, cursor and dedupe window (with and without a checkpoint)", async () => {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w1"));
    for (let i = 1; i <= LOG_CHECKPOINT_EVERY + 3; i++) await appendBatches(store, s, [await batch(store, `b${i}`)], `cur-${i}`, W("w1"));
    const o = (await openStream(store, keys))!;
    expect(o.head).toBe(s.head); expect(o.headHash).toBe(s.headHash); expect(o.inboxCursor).toBe(s.inboxCursor);
    expect([...o.remembered]).toEqual([...s.remembered]);
    expect(await store.head(objectKey(keys.checkpoint(LOG_CHECKPOINT_EVERY)))).toBeDefined();
    // A checkpoint that does not match its entry is ignored, and the open replays from entry 0 instead.
    await store.put(objectKey(keys.head), new TextEncoder().encode(JSON.stringify({ v: 1, seq: 3 })));
    const o2 = (await openStream(store, keys))!;
    expect(o2.head).toBe(s.head); expect([...o2.remembered]).toEqual([...s.remembered]);
  });
});

describe("fencing: exactly one writer per stream", () => {
  test("a new owner's epoch entry fences the old writer's next append", async () => {
    const store = fresh();
    const old = await enableLogMode(store, keys, null, W("old"));
    await appendBatches(store, old, [await batch(store, "b1")], null, W("old"));
    const neu = (await takeStream(store, (await openStream(store, keys))!, W("new")));
    expect(neu.epoch).toBe(2);
    await expect(appendBatches(store, old, [await batch(store, "b2")], null, W("old"))).rejects.toBeInstanceOf(LogFenced);
    const r = await appendBatches(store, neu, [await batch(store, "b2")], null, W("new"));
    expect(r.seq).toBe(3);
  });

  test("concurrent appends at one seq: exactly one lands, the other is fenced", async () => {
    const store = fresh();
    await enableLogMode(store, keys, null, W("a"));
    const a = (await openStream(store, keys))!, b = (await openStream(store, keys))!;
    const results = await Promise.allSettled([
      appendBatches(store, a, [await batch(store, "x")], null, W("a")),
      appendBatches(store, b, [await batch(store, "y")], null, W("b")),
    ]);
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
    const lost = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
    expect(lost.reason).toBeInstanceOf(LogFenced);
    expect((await openStream(store, keys))!.head).toBe(1);
  });
});

describe("ambiguous commits: read back, never assume failure", () => {
  test("a PUT that landed but reported a 503 is treated as landed (no duplicate, no gap)", async () => {
    const inner = fresh();
    const store = faulty(inner, (_k, n) => (n === 2 ? "after" : "ok"));
    const s = await enableLogMode(store, keys, null, W("w"));
    const r = await appendBatches(store, s, [await batch(inner, "b1")], null, W("w"));
    expect(r.seq).toBe(1);
    const all = [];
    for await (const e of readEntries(inner, keys)) all.push(e);
    expect(all.map((e) => e.seq)).toEqual([0, 1]);
  });

  test("a PUT that never lands surfaces LogUnavailable and the retry resends the same bytes", async () => {
    const inner = fresh();
    let failing = true;
    const store = faulty(inner, (_k, n) => (n === 1 ? "ok" : failing ? "before" : "ok"));
    const s = await enableLogMode(store, keys, null, W("w"));
    await expect(appendBatches(store, s, [await batch(inner, "b1")], null, W("w"))).rejects.toBeInstanceOf(LogUnavailable);
    expect(s.pending?.seq).toBe(1);
    failing = false;
    // The retry carries a DIFFERENT batch: the pending entry must land first, unchanged, then the new one.
    const r = await appendBatches(store, s, [await batch(inner, "b2")], null, W("w"));
    expect(r.seq).toBe(2);
    const seqs: string[][] = [];
    for await (const e of readEntries(inner, keys)) if (e.kind === "batches") seqs.push(e.batches.map((b) => b.batchId));
    expect(seqs).toEqual([["b1"], ["b2"]]);
  });

  test("crash at every step: after any prefix of PUTs, a reopen sees a valid chain and no torn entry", async () => {
    for (let crashAt = 1; crashAt <= 6; crashAt++) {
      const inner = fresh();
      const store = faulty(inner, (_k, n) => (n >= crashAt ? "before" : "ok"));
      try {
        const s = await enableLogMode(store, keys, null, W("w"));
        for (let i = 1; i <= 4; i++) await appendBatches(store, s, [await batch(inner, `b${i}`)], null, W("w"));
      } catch (error) {
        expect(error instanceof LogUnavailable || error instanceof LogFenced).toBe(true);
      }
      const o = await openStream(inner, keys);
      const landed = o ? o.head + 1 : 0;
      expect(landed).toBe(Math.min(crashAt - 1, 5));
      if (o) expect(o.remembered.size).toBe(o.head);
    }
  });
});

describe("readers fail closed", () => {
  test("a gap, a broken chain or a stale epoch is LogCorrupt", async () => {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w"));
    await appendBatches(store, s, [await batch(store, "b1")], null, W("w"));
    const forged = { v: 1, seq: 3, prev: s.headHash, kind: "batches", epoch: 1, writer: "x", atMs: 1, batches: [await batch(store, "z")], inboxCursor: null };
    await store.put(objectKey(keys.entry(2)), new TextEncoder().encode(JSON.stringify(forged)));
    await expect(openStream(store, keys)).rejects.toBeInstanceOf(LogCorrupt);
  });

  test("payloads are content addressed and idempotent", async () => {
    const store = fresh();
    const a = await putPayload(store, keys, new TextEncoder().encode("same"));
    const b = await putPayload(store, keys, new TextEncoder().encode("same"));
    expect(a).toEqual(b);
    expect(new TextDecoder().decode(await readAll(await store.get(objectKey(a.key))))).toBe("same");
  });
});
