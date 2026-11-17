/**
 * ledger-log-erase.ts (docs/design/s3-ledger-log-commit.md, Remaining 3): erasing a user's log payloads, on the fs
 * store, and the S3 ledger's reset running it before it completes or answers (INGEST_LEDGER_LOG_ERASE=on).
 *   - eraseLogPayloads: every payload of the user goes, in every stream (registered or not); other users' and the
 *     same user id in another workspace are untouched; a payload write racing the erase fails it (never success);
 *     store faults fail it (retryable);
 *   - eraseSupersededLogPayloads: exactly the unapplied batches a reset superseded (admitted.resets below the fence's),
 *     never a batch admitted after the reset, an in-flight (unreferenced) payload, a shared payload a live batch also
 *     names, or another user's; a corrupt stream or a store fault fails it;
 *   - the S3 ledger's reset: the payloads are gone before resetUploadedSessions answers, on a replay too; an erase
 *     failure fails the reset and leaves it in flight (log admission keeps refusing), and the retry erases and
 *     completes it;
 *   - the switch: off by default, and `on` refused without INGEST_LEDGER=s3 and INGEST_LEDGER_LOG_COMMIT=on.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { ObjectStoreError, objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { LedgerUnavailableError, type LedgerTenant } from "../ledger.ts";
import { appendBatches, enableLogMode, logKeys, putPayload, type LogAdmitSnapshot, type LogBatch, type LogKeys, type LogState } from "../ledger-log.ts";
import { eraseLogPayloads, eraseSupersededLogPayloads, ledgerLogEraseMode, userLogStreams } from "../ledger-log-erase.ts";
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

describe("eraseLogPayloads (a whole-user erase)", () => {
  test("every payload of the user goes, in every stream, registered or not; other users and workspaces are untouched", async () => {
    const store = fresh();
    const a = await stream(store, TENANT, "origin-1"), b = await stream(store, TENANT, "origin-2");
    const mine = [await logged(store, a, "a1", 0), await logged(store, a, "a2", 0), await logged(store, b, "b1", 0)];
    const other = await stream(store, OTHER_USER, "origin-1"), ws = await stream(store, OTHER_WS, "origin-1");
    const theirs = [await logged(store, other, "o1", 0), await logged(store, ws, "w1", 0)];
    // Found by listing the user's own directory: no registry pointer was ever written for these streams.
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
    // Entry 1 applied (its marker), its payload not yet dropped: the materializer drops it; the reset does not read it.
    await store.put(objectKey(a.keys.marker(1)), new TextEncoder().encode(JSON.stringify({ v: 1, seq: 1 })));
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
    const store = hooked(base, (op, key) => { if (op === "delete" && key.includes("/ledger/log/")) throw transient(); });
    const ledger = selfhostS3Ledger(ROOT, { store, run: (async (...args: unknown[]) => { sql.push(String(args[0])); throw new Error("sql"); }) as never }, undefined, { logModeGuard: true, logErase: true });
    await expect(ledger.resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_log_erase_failed" });
    expect(sql).toEqual([]);
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
