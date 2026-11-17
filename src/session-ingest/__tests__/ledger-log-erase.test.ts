/**
 * ledger-log-erase.ts (docs/design/s3-ledger-log-commit.md, Remaining 3): erasing a user's log payloads, on the fs
 * store, and the S3 ledger's reset running it before it completes or answers (INGEST_LEDGER_LOG_ERASE=on).
 *   - eraseLogPayloads: every payload of the user goes, in every stream (registered or not); other users' and the
 *     same user id in another workspace are untouched; a payload write racing the erase fails it (never success);
 *     store faults fail it (retryable);
 *   - eraseSupersededLogPayloads: exactly the unapplied batches a reset superseded (admitted.resets below the fence's),
 *     never a batch admitted after the reset, an in-flight (unreferenced) payload, a shared payload a live batch also
 *     names, or another user's; a corrupt stream or a store fault fails it, with one structured line (hash only); an
 *     append landing inside it is never wrongly taken;
 *   - discovery and cost: streams come from the user's stream index (malformed entries name nothing; an unindexed
 *     stream is the whole-user erase's until a registration indexes it); a reset on a long stream LISTs only the index,
 *     p/ and entry directories (never the log directory or m/), with O(log n) marker HEADs; materializedAtLeast is a
 *     lower bound;
 *   - the S3 ledger's reset: the payloads are gone before resetUploadedSessions answers, on a replay too, and an
 *     ordered reset's applied, stale and replay answers all erase first; an erase
 *     failure fails the reset and leaves it in flight (log admission keeps refusing), and the retry erases and
 *     completes it;
 *   - the switch: off by default, and `on` refused without INGEST_LEDGER=s3 and INGEST_LEDGER_LOG_COMMIT=on.
 */
import { createHash } from "node:crypto";
import { afterEach, describe, expect, test } from "bun:test";
import { logLine } from "../../shared/log.ts";
import { ObjectStoreError, objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { LedgerUnavailableError, type LedgerTenant } from "../ledger.ts";
import { appendBatches, enableLogMode, logKeys, logUserIndex, logUserRoot, putPayload, type LogAdmitSnapshot, type LogBatch, type LogKeys, type LogState } from "../ledger-log.ts";
import { eraseLogPayloads, eraseSupersededLogPayloads, ledgerLogEraseMode, userLogStreams } from "../ledger-log-erase.ts";
import { materializedAtLeast, materializedThrough, registerLogStream } from "../ledger-log-materialize.ts";
import { admissionFenceView, s3Ledger, s3LedgerKeys, s3LedgerObjects } from "../ledger-s3.ts";
import { selfhostLedgerLogErase } from "../selfhost-config.ts";
import { selfhostIngestRuntime, selfhostS3Ledger } from "../selfhost-server.ts";
import { TENANT, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const T0 = 1_800_000_000_000;
const OTHER_USER: LedgerTenant = { ...TENANT, userId: "user-2" };
const OTHER_WS: LedgerTenant = { ...TENANT, workspaceId: "ws-b" };

const cleanups: (() => void)[] = [];
afterEach(() => { while (cleanups.length) cleanups.pop()!(); });
function fresh() { const s = scratchStore(); cleanups.push(s.done); return s.store; }
const W = { writer: "w1", now: () => T0, sleep: async () => {} };
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;

async function keysUnder(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
    for (const i of page.items) if (String(i.key).startsWith(prefix)) out.push(String(i.key));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}

/** A log stream of `tenant` / `origin` in log mode, owned by W. */
async function stream(store: ObjectStore, tenant: LedgerTenant, origin: string): Promise<{ keys: LogKeys; state: LogState }> {
  const keys = logKeys(ROOT, tenant, origin);
  return { keys, state: await enableLogMode(store, keys, null, W) };
}
/** One logged batch with its own payload; `resets` is its admission snapshot (undefined: no snapshot). */
async function logged(store: ObjectStore, s: { keys: LogKeys; state: LogState }, id: string, resets: number | undefined, extra: LogBatch["payloads"] = []): Promise<string> {
  const ref = await putPayload(store, s.keys, new TextEncoder().encode(JSON.stringify({ id, nonce: Math.random() })));
  const admitted: LogAdmitSnapshot | undefined = resets === undefined ? undefined : { journalSeq: 0, resets, epochs: {} };
  const b: LogBatch = { batchId: id, digest: `d-${id}`, receiptId: `r-${id}`, sessions: [["claude", `s-${id}`]], payloads: [ref, ...extra], ...(admitted ? { admitted } : {}) };
  expect((await appendBatches(store, s.state, [b], null, W)).seq).not.toBeNull();
  return ref.key;
}

/** Wraps a store; `on(op, key)` may throw or act (a racing write) before the operation runs. */
function hooked(inner: ObjectStore, on: (op: "delete" | "list" | "put" | "head", key: string) => Promise<void> | void): ObjectStore {
  return {
    backend: inner.backend,
    head: async (k) => { await on("head", String(k)); return inner.head(k); },
    get: (k, r) => inner.get(k, r), getObject: (k, r) => inner.getObject(k, r),
    list: async (p, c) => { await on("list", String(p)); return inner.list(p, c); },
    presignGet: (k, t) => inner.presignGet(k, t),
    delete: async (k) => { await on("delete", String(k)); return inner.delete(k); },
    put: async (k, b, o) => { await on("put", String(k)); return inner.put(k, b, o); },
  };
}
const transient = () => new ObjectStoreError("transient", "fs", "injected", { status: 503 });
/** The materializer's markers for entries 0..through (it writes them densely, in order). */
async function mark(store: ObjectStore, keys: LogKeys, through: number) {
  for (let seq = 0; seq <= through; seq++) await store.put(objectKey(keys.marker(seq)), new TextEncoder().encode(JSON.stringify({ v: 1, seq })));
}
/** Every store call, as `op:key` (a list's key is its prefix). */
function counted(inner: ObjectStore) {
  const calls: string[] = [];
  const store: ObjectStore = {
    backend: inner.backend,
    head: (k) => { calls.push(`head:${k}`); return inner.head(k); },
    get: (k, r) => { calls.push(`get:${k}`); return inner.get(k, r); },
    getObject: (k, r) => { calls.push(`get:${k}`); return inner.getObject(k, r); },
    list: (p, c) => { calls.push(`list:${p}`); return inner.list(p, c); },
    presignGet: (k, t) => inner.presignGet(k, t),
    delete: (k) => { calls.push(`delete:${k}`); return inner.delete(k); },
    put: (k, b, o) => { calls.push(`put:${k}`); return inner.put(k, b, o); },
  };
  return { store, calls };
}
const sha256 = (v: string) => createHash("sha256").update(v).digest("hex");

describe("eraseLogPayloads (a whole-user erase)", () => {
  test("every payload of the user goes, in every stream, registered or not; other users and workspaces are untouched", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1"), b = await stream(store, TENANT, "origin-2");
    const mine = [await logged(store, a, "a1", 0), await logged(store, a, "a2", 0), await logged(store, b, "b1", 0)];
    const other = await stream(store, OTHER_USER, "origin-1"), ws = await stream(store, OTHER_WS, "origin-1");
    const theirs = [await logged(store, other, "o1", 0), await logged(store, ws, "w1", 0)];
    // Found in the user's own stream index (enableLogMode writes it): no registry pointer was ever written for these.
    expect((await userLogStreams(store, ROOT, TENANT)).map((k) => k.root).sort()).toEqual([a.keys.root, b.keys.root].sort());
    expect(await eraseLogPayloads(store, ROOT, TENANT)).toBe(3);
    for (const k of mine) expect(await exists(store, k)).toBe(false);
    for (const k of theirs) expect(await exists(store, k)).toBe(true);
    // The log itself (entries) is not the erase's: only the payloads.
    expect(await exists(store, a.keys.entry(1))).toBe(true);
    // Idempotent: nothing left, nothing deleted.
    expect(await eraseLogPayloads(store, ROOT, TENANT)).toBe(0);
  });

  test("a payload write racing the erase fails it (retryable), never success; the retry erases it", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    await logged(base, a, "a1", 0);
    let raced: string | undefined;
    // An in-flight admission's payload PUT lands while the erase deletes (after its listing).
    const store = hooked(base, async (op) => {
      if (op === "delete" && !raced) raced = (await putPayload(base, a.keys, new TextEncoder().encode("late"))).key;
    });
    await expect(eraseLogPayloads(store, ROOT, TENANT)).rejects.toMatchObject({ code: "ledger_log_erase_unverified" });
    expect(await exists(base, raced!)).toBe(true);
    expect(await eraseLogPayloads(base, ROOT, TENANT)).toBe(1);
    expect(await keysUnder(base, a.keys.payloads)).toEqual([]);
  });

  test("a store fault fails the erase as a retry (ledger_log_erase_failed), on a list or a delete", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const k = await logged(base, a, "a1", 0);
    for (const failOn of ["list", "delete"] as const) {
      const store = hooked(base, (op) => { if (op === failOn) throw transient(); });
      const err = await eraseLogPayloads(store, ROOT, TENANT).catch((e) => e);
      expect(err).toBeInstanceOf(LedgerUnavailableError);
      expect(err.code).toBe("ledger_log_erase_failed");
      expect(await exists(base, k)).toBe(true);
    }
  });
});

describe("eraseSupersededLogPayloads (a reset)", () => {
  test("exactly the unapplied batches the reset superseded; never a later batch, an in-flight payload, a shared one or another user's", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1"), b = await stream(store, TENANT, "origin-2");
    const before = [await logged(store, a, "a1", 0), await logged(store, b, "b1", 0)];
    const after = await logged(store, a, "a2", 1);
    const bare = await logged(store, a, "a3", undefined);
    // A payload a superseded batch and a live batch both name (admission never does this): kept.
    const shared = await putPayload(store, a.keys, new TextEncoder().encode("shared"));
    const doomedShared = await logged(store, a, "a4", 0, [shared]);
    await logged(store, a, "a5", 1, [shared]);
    // Put, not yet appended (an admission in flight): not referenced, not touched.
    const inflight = (await putPayload(store, a.keys, new TextEncoder().encode("in flight"))).key;
    const theirs = await logged(store, await stream(store, OTHER_USER, "origin-1"), "o1", 0);
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(3);
    for (const k of [...before, doomedShared]) expect(await exists(store, k)).toBe(false);
    for (const k of [after, bare, shared.key, inflight, theirs]) expect(await exists(store, k)).toBe(true);
    // No reset ever (0): nothing is superseded, nothing is read.
    expect(await eraseSupersededLogPayloads(fresh(), ROOT, TENANT, 0)).toBe(0);
  });

  test("an applied entry is not re-read: only entries after the materializer's cursor", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1");
    const applied = await logged(store, a, "a1", 0);
    const pending = await logged(store, a, "a2", 0);
    // Entries 0 and 1 applied (their markers), entry 1's payload not yet dropped: the materializer drops it; the reset
    // does not read it.
    await mark(store, a.keys, 1);
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(1);
    expect(await exists(store, pending)).toBe(false);
    expect(await exists(store, applied)).toBe(true);
  });

  test("fails closed: a corrupt stream, a store fault, or a payload still there after its delete", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const k = await logged(base, a, "a1", 0);
    const err = async (p: Promise<unknown>) => (await p.catch((e) => e)) as { code?: string };
    for (const failOn of ["list", "delete"] as const)
      expect((await err(eraseSupersededLogPayloads(hooked(base, (op) => { if (op === failOn) throw transient(); }), ROOT, TENANT, 1))).code).toBe("ledger_log_erase_failed");
    // A delete that answers success but leaves the object (a store that lies): unverified, never success.
    const lying: ObjectStore = { ...hooked(base, () => {}), delete: async () => {} };
    expect((await err(eraseSupersededLogPayloads(lying, ROOT, TENANT, 1))).code).toBe("ledger_log_erase_unverified");
    expect(await exists(base, k)).toBe(true);
    // The verification read itself failing: not reported done either (the delete did land).
    expect((await err(eraseSupersededLogPayloads(hooked(base, (op) => { if (op === "head") throw transient(); }), ROOT, TENANT, 1))).code).toBe("ledger_log_erase_failed");
    await base.put(objectKey(k), new TextEncoder().encode("restored for the next case"));
    // A corrupt entry (the chain no longer verifies): the reset cannot tell what is superseded, so it fails.
    await base.put(objectKey(a.keys.entry(1)), new TextEncoder().encode("{not json"));
    expect((await err(eraseSupersededLogPayloads(base, ROOT, TENANT, 1))).code).toBe("ledger_log_erase_failed");
    expect(await exists(base, k)).toBe(true);
  });

  test("a failure emits one structured line: route, outcome, closed codes and the stream root's hash, never an id or a key", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    await logged(base, a, "a1", 0);
    const lines: Record<string, unknown>[] = [];
    const log = (f: Record<string, unknown>) => { lines.push(f); };
    // A store fault on the stream's payload listing.
    const faulty = hooked(base, (op, key) => { if (op === "list" && key.endsWith("/p")) throw transient(); });
    await expect(eraseSupersededLogPayloads(faulty, ROOT, TENANT, 1, { log })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(lines).toEqual([{ route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "store", error_code: "transient", key_sha256: sha256(a.keys.root) }]);
    // A corrupt stream: the reader's closed code.
    await base.put(objectKey(a.keys.entry(1)), new TextEncoder().encode("{not json"));
    await expect(eraseSupersededLogPayloads(base, ROOT, TENANT, 1, { log })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(lines[1]).toMatchObject({ outcome: "ledger_log_erase_failed", kind: "log_corrupt", key_sha256: sha256(a.keys.root) });
    expect(String(lines[1]!.error_code)).toMatch(/^log_[a-z_]+$/);
    // The index listing itself failing: no stream to name.
    const blind = hooked(base, (op, key) => { if (op === "list" && key.endsWith("/log-index")) throw transient(); });
    await expect(eraseSupersededLogPayloads(blind, ROOT, TENANT, 1, { log })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(lines[2]).toEqual({ route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "store", error_code: "transient" });
    // Every field survives the shared log allowlist, and nothing of the tenant or the key does.
    for (const f of lines) {
      const line = JSON.parse(logLine("error", "ledger log erase failed", f)) as Record<string, unknown>;
      for (const k of Object.keys(f)) expect(line[k]).toBe(f[k]);
      const text = JSON.stringify(line);
      for (const id of [TENANT.orgId, TENANT.workspaceId, TENANT.userId, "origin-1", a.keys.root]) expect(text.includes(id)).toBe(false);
    }
  });

  test("an append landing inside the erase: a post-reset batch is never taken; a straggler written after the listing is left to the materializer", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const doomed = await logged(base, a, "a1", 0);
    let straggler: string | undefined, later: string | undefined;
    // After the erase listed p/ (its first marker HEAD): an admission that read the fence before the reset began
    // appends (old resets), and one admitted after it appends (new resets).
    const store = hooked(base, async (op, key) => {
      if (op === "head" && key.includes("/m/") && !straggler) {
        straggler = await logged(base, a, "a2", 0);
        later = await logged(base, a, "a3", 1);
      }
    });
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(1);
    expect(await exists(base, doomed)).toBe(false);
    expect(await exists(base, later!)).toBe(true);
    // The documented window: not listed, so not deleted by this erase (the materializer never writes it) ...
    expect(await exists(base, straggler!)).toBe(true);
    // ... and the reset's retry (or replay) takes it.
    expect(await eraseSupersededLogPayloads(base, ROOT, TENANT, 1)).toBe(1);
    expect(await exists(base, straggler!)).toBe(false);
    expect(await exists(base, later!)).toBe(true);
    // A batch appended while the erase deletes is not verified against (only the erase's own targets are).
    let raced: string | undefined;
    const doomed2 = await logged(base, a, "a4", 0);
    const racing = hooked(base, async (op) => { if (op === "delete" && !raced) raced = await logged(base, a, "a5", 1); });
    expect(await eraseSupersededLogPayloads(racing, ROOT, TENANT, 1)).toBe(1);
    expect(await exists(base, doomed2)).toBe(false);
    expect(await exists(base, raced!)).toBe(true);
  });
});

describe("finding the user's streams and what a reset costs", () => {
  test("the stream index: only well-formed entries under the user's own index; an unindexed stream is the whole-user erase's, and a registration indexes it", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1"), b = await stream(store, TENANT, "origin-2");
    const ka = await logged(store, a, "a1", 0), kb = await logged(store, b, "b1", 0);
    const index = logUserIndex(ROOT, TENANT);
    expect(index.startsWith(logUserRoot(ROOT, TENANT))).toBe(false);
    // Objects in the index that are not `o=<32 hex>.json` name nothing; an entry whose stream has no payload costs one LIST.
    for (const junk of ["o=zz.json", `o=${"a".repeat(32)}.txt`, `nested/o=${"b".repeat(32)}.json`, `o=${"c".repeat(31)}.json`])
      await store.put(objectKey(`${index}${junk}`), new TextEncoder().encode("{}"));
    await store.put(objectKey(`${index}o=${"d".repeat(32)}.json`), new TextEncoder().encode("{}"));
    expect((await userLogStreams(store, ROOT, TENANT)).map((k) => k.root).sort())
      .toEqual([a.keys.root, b.keys.root, `${logUserRoot(ROOT, TENANT)}o=${"d".repeat(32)}`].sort());
    // A stream switched before the index existed (its index object missing): the reset does not see it ...
    await store.delete(objectKey(b.keys.index));
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(1);
    expect(await exists(store, ka)).toBe(false);
    expect(await exists(store, kb)).toBe(true);
    // ... its next registration (admission registers before it takes) indexes it, and the reset then erases it.
    await registerLogStream(store, ROOT, TENANT, "origin-2");
    expect(await exists(store, b.keys.index)).toBe(true);
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(1);
    expect(await exists(store, kb)).toBe(false);
    // The whole-user erase also lists the directory itself, so an unindexed stream is erased all the same.
    const kb2 = await logged(store, b, "b2", 1);
    await store.delete(objectKey(b.keys.index));
    expect(await eraseLogPayloads(store, ROOT, TENANT)).toBe(1);
    expect(await exists(store, kb2)).toBe(false);
  });

  test("a reset on a long stream: LISTs are bounded (the index, p/, the entry directories), never the user's log directory or m/; O(log n) marker HEADs", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const N = 150, APPLIED = 140;
    const keys: string[] = [];
    for (let i = 1; i <= N; i++) keys.push(await logged(base, a, `b${i}`, i > APPLIED + 4 ? 0 : i > APPLIED ? 1 : 0));
    // Entries 0..APPLIED applied by the materializer: marked, payloads dropped.
    await mark(base, a.keys, APPLIED);
    for (const k of keys.slice(0, APPLIED)) await base.delete(objectKey(k));
    expect(await materializedAtLeast(base, a.keys)).toBe(APPLIED);
    expect(await materializedThrough(base, a.keys)).toBe(APPLIED);
    const { store, calls } = counted(base);
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(N - APPLIED - 4);
    for (const k of keys.slice(APPLIED, APPLIED + 4)) expect(await exists(base, k)).toBe(true);
    const lists = calls.filter((c) => c.startsWith("list:")).map((c) => c.slice("list:".length));
    const userRoot = logUserRoot(ROOT, TENANT).slice(0, -1);
    expect(lists.some((p) => p === userRoot || p === a.keys.root || p === a.keys.markers.slice(0, -1))).toBe(false);
    expect(lists.every((p) => p === logUserIndex(ROOT, TENANT).slice(0, -1) || p === a.keys.payloads.slice(0, -1) || p.startsWith(a.keys.entries))).toBe(true);
    expect(lists.length).toBeLessThanOrEqual(5);
    const markerHeads = calls.filter((c) => c.startsWith("head:") && c.includes("/m/")).length;
    expect(markerHeads).toBeLessThanOrEqual(2 * Math.ceil(Math.log2(N + 1)) + 2);
    // Entries read: from the checkpoint at or below the cursor, never the applied history from entry 1.
    const entryReads = calls.filter((c) => c.startsWith("get:") && c.includes("/e/")).length;
    expect(entryReads).toBeLessThanOrEqual(N - APPLIED + 64 + 4);
    // With nothing left in p/ (all applied), a reset costs the index LIST and one p/ LIST: no HEAD, no entry read.
    for (const k of keys.slice(APPLIED)) await base.delete(objectKey(k));
    calls.length = 0;
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(0);
    expect(calls).toEqual([`list:${logUserIndex(ROOT, TENANT).slice(0, -1)}`, `list:${a.keys.payloads.slice(0, -1)}`]);
  });

  test("materializedAtLeast never exceeds the cursor: dense markers give it exactly, a gap only lowers it", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1");
    expect(await materializedAtLeast(store, a.keys)).toBe(-1);
    for (const through of [0, 1, 2, 6, 7, 8, 40]) {
      await mark(store, a.keys, through);
      expect(await materializedAtLeast(store, a.keys)).toBe(through);
    }
    // Old markers removed (a future compaction): a lower bound, never more than the highest marker.
    await store.delete(objectKey(a.keys.marker(0)));
    expect(await materializedAtLeast(store, a.keys)).toBe(-1);
    await mark(store, a.keys, 0);
    await store.delete(objectKey(a.keys.marker(20)));
    const low = await materializedAtLeast(store, a.keys);
    expect(low).toBeLessThanOrEqual(40);
    expect(low).toBeGreaterThanOrEqual(0);
  });
});

describe("the S3 ledger's reset with resetLogErase (INGEST_LEDGER_LOG_ERASE=on)", () => {
  const ledgerWith = (store: ObjectStore, erase = (t: LedgerTenant, r: number) => eraseSupersededLogPayloads(store, ROOT, t, r)) =>
    s3Ledger(store, ROOT, { sleep: async () => {}, nowMs: () => T0, resetLogErase: erase });

  test("the superseded payloads are gone before the reset answers; a replay erases again, never a later batch", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1");
    const k1 = await logged(store, a, "a1", 0);
    const theirs = await logged(store, await stream(store, OTHER_USER, "origin-1"), "o1", 0);
    const seen: number[] = [];
    const ledger = ledgerWith(store, async (t, r) => { seen.push(r); return eraseSupersededLogPayloads(store, ROOT, t, r); });
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect(seen).toEqual([1]);
    expect(await exists(store, k1)).toBe(false);
    expect(await exists(store, theirs)).toBe(true);
    // A batch admitted after the reset (snapshot resets 1), then a replay of the same reset: it runs again, and the
    // later batch is not superseded, so it stays.
    const k2 = await logged(store, a, "a2", 1);
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect(seen).toEqual([1, 1]);
    expect(await exists(store, k2)).toBe(true);
  });

  test("an ordered reset: applied, stale (a lower version) and replay (the same version) all erase, with the fence's resets, before they answer", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1");
    const k0 = await logged(store, a, "a0", 0);
    const seen: number[] = [];
    const ledger = ledgerWith(store, async (t, r) => { seen.push(r); return eraseSupersededLogPayloads(store, ROOT, t, r); });
    const ctl = (version: string) => ({ epoch: "e".repeat(64), version, digest: (version === "6" ? "a" : "b").repeat(64), kind: "reset" as const });
    expect(await ledger.applyOrderedControl!({ tenant: TENANT, requestId: "q6", control: ctl("6") })).toMatchObject({ control: { outcome: "applied" } });
    expect(seen).toEqual([1]);
    expect(await exists(store, k0)).toBe(false);
    // A straggler (admitted before the reset began, appended after its erase) and a batch admitted after the reset.
    const straggler = await logged(store, a, "a1", 0);
    const later = await logged(store, a, "a2", 1);
    // Stale: no fence change, but the erase still runs with the fence's resets (1) before the stale answer.
    expect(await ledger.applyOrderedControl!({ tenant: TENANT, requestId: "q5", control: ctl("5") })).toMatchObject({ control: { outcome: "stale" }, deleted: 0 });
    expect(seen).toEqual([1, 1]);
    expect(await exists(store, straggler)).toBe(false);
    expect(await exists(store, later)).toBe(true);
    // Replay of the applied version: erases again, never the later batch.
    expect(await ledger.applyOrderedControl!({ tenant: TENANT, requestId: "q6", control: ctl("6") })).toMatchObject({ control: { outcome: "replay" } });
    expect(seen).toEqual([1, 1, 1]);
    expect(await exists(store, later)).toBe(true);
    const fence = admissionFenceView(await s3LedgerObjects(store, ROOT).readFence(s3LedgerKeys(ROOT, TENANT).fence));
    expect([fence.resets, fence.resetsCompleted]).toEqual([1, 1]);
    // A stale answer whose erase fails is not answered: the error surfaces (a retry), the fence is unchanged.
    const failing = ledgerWith(store, async () => { throw new LedgerUnavailableError("ledger_log_erase_failed"); });
    await expect(failing.applyOrderedControl!({ tenant: TENANT, requestId: "q4", control: ctl("4") })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    await expect(failing.applyOrderedControl!({ tenant: TENANT, requestId: "q6", control: ctl("6") })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
  });

  test("an erase failure fails the reset and leaves it in flight; the retry erases and completes it", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1");
    const k1 = await logged(store, a, "a1", 0);
    let fail = true;
    const ledger = ledgerWith(store, async (t, r) => {
      if (fail) throw new LedgerUnavailableError("ledger_log_erase_failed");
      return eraseSupersededLogPayloads(store, ROOT, t, r);
    });
    await expect(ledger.resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(await exists(store, k1)).toBe(true);
    // Begun, not completed: log admission refuses the user (ledger_reset_in_flight) while the payload is still there.
    const io = s3LedgerObjects(store, ROOT);
    let fence = admissionFenceView(await io.readFence(s3LedgerKeys(ROOT, TENANT).fence));
    expect(fence.resets).toBe(1);
    expect(fence.resetsCompleted).toBe(0);
    fail = false;
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect(await exists(store, k1)).toBe(false);
    fence = admissionFenceView(await io.readFence(s3LedgerKeys(ROOT, TENANT).fence));
    expect(fence.resetsCompleted).toBe(1);
  });

  test("self-host wiring (selfhostS3Ledger logErase): the erase runs before the SQL mirror; its failure fails the reset and SQL is never called", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const k1 = await logged(base, a, "a1", 0);
    const sql: string[] = [];
    const errors: [string, Record<string, unknown>][] = [];
    const logger = { error: (m: string, f: Record<string, unknown> = {}) => { errors.push([m, f]); } };
    const store = hooked(base, (op, key) => { if (op === "delete" && key.includes("/ledger/log/")) throw transient(); });
    const ledger = selfhostS3Ledger(ROOT, { store, run: (async (...args: unknown[]) => { sql.push(String(args[0])); throw new Error("sql"); }) as never }, undefined, { logModeGuard: true, logErase: true, logger });
    await expect(ledger.resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(sql).toEqual([]);
    // The failure is one error line naming the stream by hash only (the runbook's signal).
    expect(errors).toEqual([["ledger log erase failed", { route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "store", error_code: "transient", key_sha256: sha256(a.keys.root) }]]);
    expect(await exists(base, k1)).toBe(true);
    // Without the fault, the payload goes; the SQL mirror runs after it (and fails here, which fails the reset: a retry).
    const ok = selfhostS3Ledger(ROOT, { store: base, run: (async (...args: unknown[]) => { sql.push(String(args[0])); throw new Error("sql"); }) as never }, undefined, { logModeGuard: true, logErase: true });
    await expect(ok.resetUploadedSessions(TENANT, "req-1")).rejects.toThrow();
    expect(await exists(base, k1)).toBe(false);
    expect(sql.length).toBeGreaterThan(0);
  });

  test("unset (the default): a reset reads and deletes nothing in the log", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1");
    const k1 = await logged(store, a, "a1", 0);
    const touched: string[] = [];
    const watched = hooked(store, (op, key) => { if (key.includes("/ledger/log/")) touched.push(`${op}:${key}`); });
    expect(await s3Ledger(watched, ROOT, { sleep: async () => {}, nowMs: () => T0 }).resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect(touched).toEqual([]);
    expect(await exists(store, k1)).toBe(true);
  });
});

describe("INGEST_LEDGER_LOG_ERASE", () => {
  test("off by default; exactly on; on refused without INGEST_LEDGER=s3 and INGEST_LEDGER_LOG_COMMIT=on", () => {
    expect(ledgerLogEraseMode(undefined)).toBe("off");
    expect(ledgerLogEraseMode("")).toBe("off");
    expect(ledgerLogEraseMode("off")).toBe("off");
    expect(ledgerLogEraseMode("on")).toBe("on");
    expect(() => ledgerLogEraseMode("true")).toThrow();
    expect(selfhostLedgerLogErase({}, "s3", "on")).toBeUndefined();
    expect(selfhostLedgerLogErase({ INGEST_LEDGER_LOG_ERASE: "on" }, "s3", "on")).toBe(true);
    expect(() => selfhostLedgerLogErase({ INGEST_LEDGER_LOG_ERASE: "on" }, "s3", "off")).toThrow(/needs INGEST_LEDGER=s3 and INGEST_LEDGER_LOG_COMMIT=on/);
    expect(() => selfhostLedgerLogErase({ INGEST_LEDGER_LOG_ERASE: "on" }, undefined, "off")).toThrow(/needs/);
    expect(() => selfhostLedgerLogErase({ INGEST_LEDGER_LOG_ERASE: "yes" }, "s3", "on")).toThrow(/must be off or on/);
    // A direct runtime caller is refused the same way.
    expect(() => selfhostIngestRuntime({ keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 16, ledger: "s3", ledgerLogCommit: "off", ledgerLogErase: true },
      { run: (async () => { throw new Error("sql"); }) as never, store: fresh(), verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {} } as never))
      .toThrow(/INGEST_LEDGER_LOG_ERASE=on needs/);
  });
});

describe("slice 9: every stream is attempted, a throwing sink, the unverified kind, a missing marker 0", () => {
  test("a reset: one corrupt stream does not stop the others; each failing stream logs its own line; the erase still fails", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1"), b = await stream(base, TENANT, "origin-2"), c = await stream(base, TENANT, "origin-3");
    const ka = await logged(base, a, "a1", 0), kb = await logged(base, b, "b1", 0), kc = await logged(base, c, "c1", 0);
    // Corrupt a and c (sorted roots decide the order; whichever comes first, the healthy b is still erased).
    for (const s of [a, c]) await base.put(objectKey(s.keys.entry(1)), new TextEncoder().encode("{not json"));
    const lines: Record<string, unknown>[] = [];
    await expect(eraseSupersededLogPayloads(base, ROOT, TENANT, 1, { log: (f) => { lines.push(f); } })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(await exists(base, kb)).toBe(false);
    expect(await exists(base, ka)).toBe(true);
    expect(await exists(base, kc)).toBe(true);
    expect(lines.map((l) => l.key_sha256).sort()).toEqual([sha256(a.keys.root), sha256(c.keys.root)].sort());
    for (const l of lines) expect(l).toMatchObject({ route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "log_corrupt" });
  });

  test("the whole-user erase: a failing stream does not stop the others either", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1"), b = await stream(base, TENANT, "origin-2");
    const ka = await logged(base, a, "a1", 0), kb = await logged(base, b, "b1", 0);
    const lines: Record<string, unknown>[] = [];
    const store = hooked(base, (op, key) => { if (op === "delete" && key.startsWith(a.keys.payloads)) throw transient(); });
    await expect(eraseLogPayloads(store, ROOT, TENANT, { log: (f) => { lines.push(f); } })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(await exists(base, ka)).toBe(true);
    expect(await exists(base, kb)).toBe(false);
    expect(lines).toEqual([{ route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "store", error_code: "transient", key_sha256: sha256(a.keys.root) }]);
  });

  test("a sink that throws never replaces the erase's own error (per stream and on discovery)", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    await logged(base, a, "a1", 0);
    const log = () => { throw new Error("sink down"); };
    const faulty = hooked(base, (op, key) => { if (op === "list" && key.endsWith("/p")) throw transient(); });
    await expect(eraseSupersededLogPayloads(faulty, ROOT, TENANT, 1, { log })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    const blind = hooked(base, (op, key) => { if (op === "list" && key.endsWith("/log-index")) throw transient(); });
    await expect(eraseSupersededLogPayloads(blind, ROOT, TENANT, 1, { log })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    await expect(eraseLogPayloads(faulty, ROOT, TENANT, { log })).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
  });

  test("a payload still there after its delete logs kind unverified with its closed code", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    await logged(base, a, "a1", 0);
    const lines: Record<string, unknown>[] = [];
    const lying: ObjectStore = { ...hooked(base, () => {}), delete: async () => {} };
    await expect(eraseSupersededLogPayloads(lying, ROOT, TENANT, 1, { log: (f) => { lines.push(f); } })).rejects.toMatchObject({ code: "ledger_log_erase_unverified" });
    expect(lines).toEqual([{ route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "unverified", error_code: "ledger_log_erase_unverified", key_sha256: sha256(a.keys.root) }]);
    lines.length = 0;
    await expect(eraseLogPayloads(lying, ROOT, TENANT, { log: (f) => { lines.push(f); } })).rejects.toMatchObject({ code: "ledger_log_erase_unverified" });
    expect(lines).toEqual([{ route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind: "unverified", error_code: "ledger_log_erase_unverified", key_sha256: sha256(a.keys.root) }]);
  });

  test("marker 0 gone (a future compaction) with later markers present: the reset falls back to entry 0 and still erases", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const applied = await logged(base, a, "a1", 0);
    const pending = await logged(base, a, "a2", 0);
    const later = await logged(base, a, "a3", 1);
    await mark(base, a.keys, 1);
    await base.delete(objectKey(a.keys.marker(0)));
    expect(await materializedAtLeast(base, a.keys)).toBe(-1);
    const { store, calls } = counted(base);
    // From entry 0: the applied entry is re-read too, so its leftover payload goes with the unapplied one (the
    // materializer's own next step); the post-reset batch stays.
    expect(await eraseSupersededLogPayloads(store, ROOT, TENANT, 1)).toBe(2);
    for (const k of [applied, pending]) expect(await exists(base, k)).toBe(false);
    expect(await exists(base, later)).toBe(true);
    expect(calls.includes(`get:${a.keys.entry(0)}`)).toBe(true);
    expect(calls.includes(`get:${a.keys.entry(1)}`)).toBe(true);
  });

  test("self-host wiring: a logger that throws does not mask the erase failure, and SQL is never called", async () => {
    const base = fresh();
    const a = await stream(base, TENANT, "origin-1");
    const k1 = await logged(base, a, "a1", 0);
    const sql: string[] = [];
    const logger = { error: () => { throw new Error("logger down"); } };
    const store = hooked(base, (op, key) => { if (op === "delete" && key.includes("/ledger/log/")) throw transient(); });
    const ledger = selfhostS3Ledger(ROOT, { store, run: (async (...args: unknown[]) => { sql.push(String(args[0])); throw new Error("sql"); }) as never }, undefined, { logModeGuard: true, logErase: true, logger });
    await expect(ledger.resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(sql).toEqual([]);
    expect(await exists(base, k1)).toBe(true);
  });
});
