/**
 * ledger-log.ts: the per-stream numbered-log commit (docs/design/s3-ledger-log-commit.md). Runs on the fs store,
 * whose create-if-absent semantics are the object-store conformance cases every backend (S3, GCS, Azure) passes.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { ObjectStoreError, objectKey, readAll, type ObjectStore, type PutOpts } from "../../shared/vendor.ts";
import {
  appendBatches, enableLogMode, ledgerLogCommitMode, LOG_CHECKPOINT_EVERY, LOG_REMEMBERED_BATCHES, LogFenced, logKeys, LogUnavailable,
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

const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
const putRaw = (store: ObjectStore, key: string, bytes: Uint8Array) => store.put(objectKey(key), bytes, { contentType: "application/json" });
const rawEntry = async (store: ObjectStore, seq: number) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(keys.entry(seq)))).body)));
async function seqs(store: ObjectStore): Promise<number[]> { const out: number[] = []; for await (const e of readEntries(store, keys)) out.push(e.seq); return out; }

/** A store whose entry creates wait until `n` of them are in flight, so concurrent writers genuinely race. */
function racing(inner: ObjectStore, n: number) {
  let waiting: (() => void)[] = [];
  const store: ObjectStore = {
    backend: inner.backend,
    head: (k) => inner.head(k), get: (k, r) => inner.get(k, r), getObject: (k, r) => inner.getObject(k, r),
    list: (p, c) => inner.list(p, c), presignGet: (k, t) => inner.presignGet(k, t), delete: (k) => inner.delete(k),
    put: inner.put.bind(inner),
  };
  store.put = async (key: Parameters<ObjectStore["put"]>[0], body: Uint8Array, opts?: PutOpts) => {
    if (String(key).includes("/e/") && !String(key).endsWith("/e/00000000000000000000.json")) {
      await new Promise<void>((resolve) => { waiting.push(resolve); if (waiting.length >= n) { const w = waiting; waiting = []; for (const r of w) r(); } });
    }
    return inner.put(key, body, opts);
  };
  return store;
}

describe("log mode marker", () => {
  test("a stream with no entry 0 is legacy", async () => {
    expect(await openStream(fresh(), keys)).toBeUndefined();
  });
  test("enableLogMode is idempotent; the writer whose entry 0 landed owns epoch 1, a later opener does not", async () => {
    const store = fresh();
    const a = await enableLogMode(store, keys, "cut-1", W("w1"));
    const b = await enableLogMode(store, keys, "cut-1", W("w2"));
    expect([a.head, b.head, a.epoch, b.epoch]).toEqual([0, 0, 1, 1]);
    expect([a.epochWriter, b.epochWriter, a.legacyCutover]).toEqual(["w1", "w1", "cut-1"]);
    // Only the owner appends; the other opener is refused until it takes the stream.
    await expect(appendBatches(store, b, [await batch(store, "x")], null, W("w2"))).rejects.toMatchObject({ code: "log_not_owner" });
    expect((await appendBatches(store, a, [await batch(store, "x")], null, W("w1"))).seq).toBe(1);
  });
  test("a stream opened with another legacyCutover is refused, never silently merged", async () => {
    const store = fresh();
    await enableLogMode(store, keys, "cut-1", W("w1"));
    await expect(enableLogMode(store, keys, "cut-2", W("w2"))).rejects.toMatchObject({ code: "log_open_other_cutover" });
  });
  test("two openers racing for entry 0: exactly one owns it", async () => {
    const store = fresh();
    const [a, b] = await Promise.all([enableLogMode(store, keys, null, W("a")), enableLogMode(store, keys, null, W("b"))]);
    expect(a.epochWriter).toBe(b.epochWriter);
    expect(["a", "b"]).toContain(a.epochWriter);
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
    // A batch repeated within one call is a replay of itself, never written twice; another digest is a conflict.
    const r4 = await appendBatches(store, s, [await batch(store, "b4"), await batch(store, "b4"), await batch(store, "b4", "x")], null, W("w1"));
    expect(r4.admitted.length).toBe(1); expect(r4.replays).toEqual(["b4"]); expect(r4.conflicts).toEqual(["b4"]);
  });

  test("the dedupe window is LOG_REMEMBERED_BATCHES per stream: an evicted id is new again, a remembered one conflicts", async () => {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w"));
    const ref = (await batch(store, "p")).payloads;
    const mk = (id: string, digest = `d-${id}`): LogBatch => ({ batchId: id, digest, receiptId: `r-${id}`, sessions: [], payloads: ref });
    const total = LOG_REMEMBERED_BATCHES + 1;
    for (let i = 0; i < total; i += 512) await appendBatches(store, s, Array.from({ length: Math.min(512, total - i) }, (_, j) => mk(`b${i + j}`)), null, W("w"));
    expect(s.remembered.size).toBe(LOG_REMEMBERED_BATCHES);
    const r = await appendBatches(store, s, [mk("b0", "other"), mk(`b${total - 1}`, "other")], null, W("w"));
    expect(r.admitted.map((b) => b.batchId)).toEqual(["b0"]); // evicted: not remembered, so admitted again
    expect(r.conflicts).toEqual([`b${total - 1}`]);
  });

  test("a reopened stream has the same head, cursor, owner and window (with and without a checkpoint)", async () => {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w1"));
    for (let i = 1; i <= LOG_CHECKPOINT_EVERY + 3; i++) await appendBatches(store, s, [await batch(store, `b${i}`)], `cur-${i}`, W("w1"));
    const o = (await openStream(store, keys))!;
    expect([o.head, o.headHash, o.inboxCursor, o.epochWriter]).toEqual([s.head, s.headHash, s.inboxCursor, "w1"]);
    expect([...o.remembered]).toEqual([...s.remembered]);
    expect(await store.head(objectKey(keys.checkpoint(LOG_CHECKPOINT_EVERY)))).toBeDefined();
    // A hint naming a seq with no checkpoint, then a checkpoint whose hash does not match its entry: both ignored.
    await putRaw(store, keys.head, enc({ v: 1, seq: 3 }));
    expect((await openStream(store, keys))!.head).toBe(s.head);
    const cp = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(keys.checkpoint(LOG_CHECKPOINT_EVERY)))).body)));
    await putRaw(store, keys.checkpoint(5), enc({ ...cp, seq: 5, remembered: [["forged", "x"]] }));
    await putRaw(store, keys.head, enc({ v: 1, seq: 5 }));
    const o2 = (await openStream(store, keys))!;
    expect(o2.head).toBe(s.head); expect(o2.remembered.has("forged")).toBe(false);
  });

  test("a payload key that is not the stream's content address is refused before any write", async () => {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w"));
    const b = await batch(store, "b1");
    await expect(appendBatches(store, s, [{ ...b, payloads: [{ ...b.payloads[0]!, key: "elsewhere/x" }] }], null, W("w"))).rejects.toMatchObject({ code: "log_payload_foreign" });
    expect(await seqs(store)).toEqual([0]);
  });
});

describe("fencing: exactly one writer per stream", () => {
  test("a new owner's epoch entry fences the old writer's next append", async () => {
    const store = fresh();
    const old = await enableLogMode(store, keys, null, W("old"));
    await appendBatches(store, old, [await batch(store, "b1")], null, W("old"));
    const neu = await takeStream(store, (await openStream(store, keys))!, W("new"));
    expect([neu.epoch, neu.epochWriter]).toEqual([2, "new"]);
    await expect(appendBatches(store, old, [await batch(store, "b2")], null, W("old"))).rejects.toBeInstanceOf(LogFenced);
    expect((await appendBatches(store, neu, [await batch(store, "b2")], null, W("new"))).seq).toBe(3);
    // The fenced writer, reopened, is not the owner: refused before any write.
    await expect(appendBatches(store, (await openStream(store, keys))!, [await batch(store, "b3")], null, W("old"))).rejects.toMatchObject({ code: "log_not_owner" });
  });

  test("two appends genuinely racing for one seq (a store that releases both creates together): exactly one lands", async () => {
    const inner = fresh();
    await enableLogMode(inner, keys, null, W("a"));
    const store = racing(inner, 2);
    const s1 = (await openStream(inner, keys))!, s2 = (await openStream(inner, keys))!;
    const results = await Promise.allSettled([
      appendBatches(store, s1, [await batch(inner, "x")], null, W("a")),
      appendBatches(store, s2, [await batch(inner, "y")], null, W("a")),
    ]);
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
    expect((results.find((r) => r.status === "rejected") as PromiseRejectedResult).reason).toBeInstanceOf(LogFenced);
    expect(await seqs(inner)).toEqual([0, 1]);
  });

  test("two takers racing: one owns the stream, the loser backs off (log_take_lost) instead of re-taking", async () => {
    const inner = fresh();
    await enableLogMode(inner, keys, null, W("first"));
    const store = racing(inner, 2);
    const t1 = (await openStream(inner, keys))!, t2 = (await openStream(inner, keys))!;
    const results = await Promise.allSettled([takeStream(store, t1, W("t1")), takeStream(store, t2, W("t2"))]);
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
    const lost = (results.find((r) => r.status === "rejected") as PromiseRejectedResult).reason;
    expect(lost).toMatchObject({ code: "log_take_lost" });
    const o = (await openStream(inner, keys))!;
    expect([o.head, o.epoch]).toEqual([1, 2]); // one epoch entry only: no fence-each-other loop
    // The loser's state caught up to the winner, and it does not own it.
    const loserState = results[0]!.status === "rejected" ? t1 : t2;
    expect(loserState.epoch).toBe(2);
    expect(loserState.epochWriter).toBe(o.epochWriter);
  });

  test("an ambiguous take keeps its epoch entry pending and resends exactly it", async () => {
    const inner = fresh();
    await enableLogMode(inner, keys, null, W("first"));
    let failing = true;
    const store = faulty(inner, () => (failing ? "before" : "ok"));
    const s = (await openStream(inner, keys))!;
    await expect(takeStream(store, s, W("t"))).rejects.toBeInstanceOf(LogUnavailable);
    expect(s.pending?.entry.kind).toBe("epoch");
    failing = false;
    await takeStream(store, s, W("t")); // resolves the pending epoch entry, then takes again on top of it
    const o = (await openStream(inner, keys))!;
    expect([o.head, o.epoch, o.epochWriter]).toEqual([2, 3, "t"]);
  });
});

describe("ambiguous commits: read back, never assume failure", () => {
  test("a PUT that landed but reported a 503 is treated as landed (no duplicate, no gap)", async () => {
    const inner = fresh();
    const store = faulty(inner, (_k, n) => (n === 2 ? "after" : "ok"));
    const s = await enableLogMode(store, keys, null, W("w"));
    expect((await appendBatches(store, s, [await batch(inner, "b1")], null, W("w"))).seq).toBe(1);
    expect(await seqs(inner)).toEqual([0, 1]);
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
    expect((await appendBatches(store, s, [await batch(inner, "b2")], null, W("w"))).seq).toBe(2);
    const ids: string[][] = [];
    for await (const e of readEntries(inner, keys)) if (e.kind === "batches") ids.push(e.batches.map((b) => b.batchId));
    expect(ids).toEqual([["b1"], ["b2"]]);
  });

  test("crash at every prefix, failing before OR after the write, with the process state dropped: the chain stays valid and every batch lands at most once", async () => {
    for (const mode of ["before", "after"] as const) {
      for (let crashAt = 1; crashAt <= 6; crashAt++) {
        const inner = fresh();
        const store = faulty(inner, (_k, n) => (n >= crashAt ? mode : "ok"));
        try {
          const s = await enableLogMode(store, keys, null, W("w"));
          for (let i = 1; i <= 4; i++) await appendBatches(store, s, [await batch(inner, `b${i}`)], null, W("w"));
        } catch (error) {
          expect(error instanceof LogUnavailable || error instanceof LogFenced).toBe(true);
        }
        // A restart: no in-memory state survives. Reopen from the store, take the stream, finish the work.
        let o = await openStream(inner, keys);
        if (!o) o = await enableLogMode(inner, keys, null, W("w"));
        o = await takeStream(inner, o, W("w2"));
        for (let i = 1; i <= 4; i++) await appendBatches(inner, o, [await batch(inner, `b${i}`)], null, W("w2"));
        const ids: string[] = [];
        for await (const e of readEntries(inner, keys)) if (e.kind === "batches") ids.push(...e.batches.map((b) => b.batchId));
        expect(ids.sort()).toEqual(["b1", "b2", "b3", "b4"]);
      }
    }
  });
});

describe("readers fail closed", () => {
  async function fourEntries() {
    const store = fresh();
    const s = await enableLogMode(store, keys, null, W("w"));
    for (let i = 1; i <= 3; i++) await appendBatches(store, s, [await batch(store, `b${i}`)], null, W("w"));
    return { store, s };
  }
  test("a missing middle entry is LogCorrupt (log_entry_gap), never a shorter log", async () => {
    const { store } = await fourEntries();
    await store.delete(objectKey(keys.entry(2)));
    await expect(openStream(store, keys)).rejects.toMatchObject({ code: "log_entry_gap" });
    await expect(seqs(store)).rejects.toMatchObject({ code: "log_entry_gap" });
  });
  test("a lost entry 0 with entries present is LogCorrupt (log_open_missing): the stream never flips back to legacy", async () => {
    const { store } = await fourEntries();
    await store.delete(objectKey(keys.entry(0)));
    await expect(openStream(store, keys)).rejects.toMatchObject({ code: "log_open_missing" });
    await expect(seqs(store)).rejects.toMatchObject({ code: "log_open_missing" });
  });
  test("a bad prev hash is log_entry_chain", async () => {
    const { store } = await fourEntries();
    await putRaw(store, keys.entry(3), enc({ ...(await rawEntry(store, 3)), prev: "0".repeat(64) }));
    await expect(openStream(store, keys)).rejects.toMatchObject({ code: "log_entry_chain" });
  });
  test("a batches entry under a stale epoch is log_entry_stale_epoch", async () => {
    const { store, s } = await fourEntries();
    await takeStream(store, s, W("new")); // entry 4 is epoch 2
    const e5 = { ...(await rawEntry(store, 3)), seq: 5, prev: s.headHash, epoch: 1 };
    await putRaw(store, keys.entry(5), enc(e5));
    await expect(openStream(store, keys)).rejects.toMatchObject({ code: "log_entry_stale_epoch" });
  });
  test("malformed JSON is log_entry_invalid", async () => {
    const { store } = await fourEntries();
    await putRaw(store, keys.entry(2), new TextEncoder().encode("{not json"));
    await expect(openStream(store, keys)).rejects.toMatchObject({ code: "log_entry_invalid" });
  });
  test("an entry copied from another stream is log_entry_other_stream", async () => {
    const { store } = await fourEntries();
    await putRaw(store, keys.entry(3), enc({ ...(await rawEntry(store, 3)), stream: "96bulls/elsewhere" }));
    await expect(openStream(store, keys)).rejects.toMatchObject({ code: "log_entry_other_stream" });
  });

  test("payloads are content addressed and idempotent", async () => {
    const store = fresh();
    const a = await putPayload(store, keys, new TextEncoder().encode("same"));
    const b = await putPayload(store, keys, new TextEncoder().encode("same"));
    expect(a).toEqual(b);
    expect(new TextDecoder().decode(await readAll(await store.get(objectKey(a.key))))).toBe("same");
  });
});
