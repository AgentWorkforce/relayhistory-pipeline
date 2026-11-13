/**
 * The S3 upload ledger (ledger-s3.ts, decision 0005) over the scratch fs object store, which honours
 * create-only (`ifNoneMatch`) and compare-and-swap (`ifMatch`) puts. Every claim kind of
 * sessions.ingest_claim_batch_mode (canonical), the commit's fence recheck and re-arm, revision-index CAS
 * races, #250's equal-revision rule, crash resumption through deliverPrepared, controls, and the refusals.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { deliverPrepared, IngestUnavailable } from "../deliver.ts";
import { LedgerUnavailableError, type ClaimInput, type CommitInput, type Ledger, type LedgerRecord } from "../ledger.ts";
import { ingestLedgerMode, s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { liveMachineId } from "../writer.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const MACHINE = liveMachineId(TENANT.workspaceId, ORIGIN, TENANT.userId);
const hex = (v: string) => createHash("sha256").update(v).digest("hex");
const keys = s3LedgerKeys(ROOT, TENANT);

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function fresh() {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}
const noSleep = { sleep: async () => {} };
const ledgerOn = (store: ObjectStore, opts = {}) => s3Ledger(store, ROOT, { ...noSleep, ...opts });

function rec(recordId: string, revision: number, o: Partial<LedgerRecord> = {}): LedgerRecord {
  return { source: "claude", sessionId: "sess-1", recordId, revisionId: `${recordId}:r${revision}`, revision, digest: hex(`${recordId}:${revision}`), ...o };
}
function claimInput(batchId: string, records: LedgerRecord[], o: Partial<ClaimInput> = {}): ClaimInput {
  const sessions = [...new Map(records.filter((r) => r.sessionId).map((r) => [`${r.source}\u001f${r.sessionId}`, [r.source, r.sessionId!] as [string, string]])).values()];
  return { tenant: TENANT, originId: ORIGIN, batchId, batchDigest: hex(`batch:${batchId}:${JSON.stringify(records)}`), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions, records, ...o };
}
const commitOf = (c: ClaimInput, claimToken: string, records = c.records): CommitInput => ({
  tenant: c.tenant, originId: c.originId, batchId: c.batchId, batchDigest: c.batchDigest, claimToken, sessions: c.sessions, records, machineId: MACHINE,
});
async function json(store: ObjectStore, key: string) {
  return JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));
}
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const indexKey = (sessionId: string | null, source = "claude") => keys.revisions(ORIGIN, sessionId === null ? null : source, sessionId);
async function claimed(ledger: Ledger, input: ClaimInput) {
  const r = await ledger.claim(input);
  if (r.kind !== "claimed" && r.kind !== "resume") throw new Error(`expected a claim, got ${r.kind}`);
  return r;
}

describe("INGEST_LEDGER", () => {
  test("unset, empty or sql is sql; exactly s3 is s3; anything else refuses", () => {
    expect(ingestLedgerMode(undefined)).toBe("sql");
    expect(ingestLedgerMode("")).toBe("sql");
    expect(ingestLedgerMode("sql")).toBe("sql");
    expect(ingestLedgerMode("s3")).toBe("s3");
    for (const bad of ["S3", " s3", "postgres", "on"]) expect(() => ingestLedgerMode(bad)).toThrow(/INGEST_LEDGER must be sql or s3/);
  });
});

describe("claim kinds (ingest_claim_batch_mode, canonical)", () => {
  test("claimed -> commit -> replay with the stored receipt; the batch record is pending then committed", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1), rec("r2", 1)]);
    const c = await claimed(ledger, input);
    expect(c).toMatchObject({ kind: "claimed", deleted: [], stale: [], resets: 0 });
    expect(c.claimToken).toMatch(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/);
    expect(await json(store, keys.batch(ORIGIN, "b1"))).toMatchObject({ v: 1, state: "pending", claimToken: c.claimToken, digest: input.batchDigest, receiptId: input.receiptId, fenceVersion: 0 });
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 0 });
    expect(await json(store, keys.batch(ORIGIN, "b1"))).toMatchObject({ state: "committed" });
    expect(await ledger.claim(input)).toEqual({ kind: "replay", receiptId: input.receiptId });
    // A commit retried after its answer was lost is idempotent.
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 0 });
  });

  test("resume: a pending claim with the same digest gets a new token; the older attempt's commit is superseded", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1)]);
    const first = await claimed(ledger, input);
    const second = await claimed(ledger, input);
    expect(second.kind).toBe("resume");
    expect(second.claimToken).not.toBe(first.claimToken);
    await expect(ledger.commit(commitOf(input, first.claimToken))).rejects.toMatchObject({ code: "ledger_attempt_superseded" });
    expect(await ledger.commit(commitOf(input, second.claimToken))).toEqual({ rearmed: 0 });
  });

  test("batch_conflict: a reused batch id with other content, pending or committed", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("r1", 1)]);
    const other = { ...claimInput("b1", [rec("r9", 1)]) };
    await claimed(ledger, a);
    expect(await ledger.claim(other)).toEqual({ kind: "batch_conflict", currentDigest: a.batchDigest });
    const c = await claimed(ledger, a);
    await ledger.commit(commitOf(a, c.claimToken));
    expect(await ledger.claim(other)).toEqual({ kind: "batch_conflict", currentDigest: a.batchDigest });
  });

  test("replay_fast: every record stored at or above its revision commits the batch in the claim", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("r1", 2), rec("r2", 1)]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    // An old Desktop re-sends the same history at the same and older revisions under a new batch id.
    const again = claimInput("b2", [rec("r1", 1), rec("r2", 1), rec("r1", 2)]);
    expect(await ledger.claim(again)).toEqual({ kind: "replay_fast", deleted: [], stale: [0, 1, 2], resets: 0 });
    expect(await json(store, keys.batch(ORIGIN, "b2"))).toMatchObject({ state: "committed", claimToken: null });
    expect(await ledger.claim(again)).toEqual({ kind: "replay", receiptId: again.receiptId });
    // A mixed batch: only the new revision is written.
    const mixed = claimInput("b3", [rec("r1", 2), rec("r1", 3)]);
    expect(await ledger.claim(mixed)).toMatchObject({ kind: "claimed", stale: [0] });
  });

  test("replay_fast on a pending batch (resume) commits it", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("r1", 1)]);
    await claimed(ledger, a);
    // The records became stored meanwhile (another batch carried them).
    const b = claimInput("b2", [rec("r1", 1)]);
    await ledger.commit(commitOf(b, (await claimed(ledger, b)).claimToken));
    expect(await ledger.claim(a)).toMatchObject({ kind: "replay_fast", stale: [0] });
    expect(await ledger.claim(a)).toEqual({ kind: "replay", receiptId: a.receiptId });
  });

  test("revision_conflict: in-batch reuse of a NEW revision with another revision id AND digest only; nothing is claimed", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const twin = claimInput("b1", [rec("r1", 1), rec("r1", 1, { revisionId: "r1:other", digest: hex("other") }), rec("r2", 1)]);
    const r = await ledger.claim(twin);
    // Identity order (record_id, revision, revision_id in byte order): "r1:other" < "r1:r1", so it is the peer.
    expect(r).toEqual({ kind: "revision_conflict", count: 1, conflicts: [{
      recordId: "r1", submittedRevisionId: "r1:r1", submittedRevision: 1, submittedDigest: rec("r1", 1).digest,
      currentRevisionId: "r1:other", currentRevision: 1, currentDigest: hex("other"),
    }] });
    expect(await exists(store, keys.batch(ORIGIN, "b1"))).toBe(false);
    // The same revision id with another digest, or another id with the same digest, is not a twin.
    expect((await ledger.claim(claimInput("b2", [rec("r1", 1), rec("r1", 1, { digest: hex("x") })]))).kind).toBe("claimed");
    expect((await ledger.claim(claimInput("b3", [rec("r1", 1), rec("r1", 1, { revisionId: "r1:y" })]))).kind).toBe("claimed");
  });

  test("revision_conflict is in-batch only (#250): a stored equal revision is stale, a stored lower one still judges the twin", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("r1", 1)]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    // Stored at 1: both rev-1 rows are stale (a replay whatever their digests), so no twin.
    expect(await ledger.claim(claimInput("b2", [rec("r1", 1, { revisionId: "x", digest: hex("x") }), rec("r1", 1, { revisionId: "y", digest: hex("y") })])))
      .toMatchObject({ kind: "replay_fast", stale: [0, 1] });
    // Rev 2 is new above the stored 1: the in-batch twin conflicts.
    expect(await ledger.claim(claimInput("b3", [rec("r1", 2), rec("r1", 2, { revisionId: "z", digest: hex("z") })]))).toMatchObject({ kind: "revision_conflict", count: 1 });
  });

  test("conflicts are capped at 100 with the full count", async () => {
    const ledger = ledgerOn(fresh());
    const records = Array.from({ length: 120 }, (_, i) => [rec(`r${i}`, 1), rec(`r${i}`, 1, { revisionId: `t${i}`, digest: hex(`t${i}`) })]).flat();
    const r = await ledger.claim(claimInput("b1", records));
    expect(r.kind).toBe("revision_conflict");
    if (r.kind === "revision_conflict") { expect(r.count).toBe(120); expect(r.conflicts).toHaveLength(100); }
  });
});

describe("equal revision, different digest (relayhistory-transform.ts chooseRelayhistoryRevision refuses those)", () => {
  test("a later batch is stale, and a racing commit keeps the first stored digest", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const x = rec("r1", 2, { revisionId: "r1:x", digest: hex("x") });
    const y = rec("r1", 2, { revisionId: "r1:y", digest: hex("y") });
    // Both claimed before either commits (neither sees the other's revision).
    const a = claimInput("bx", [x]), b = claimInput("by", [y]);
    const ca = await claimed(ledger, a), cb = await claimed(ledger, b);
    await ledger.commit(commitOf(a, ca.claimToken));
    await ledger.commit(commitOf(b, cb.claimToken));
    expect((await json(store, indexKey("sess-1"))).records).toEqual({ r1: [2, "r1:x", hex("x")] });
    // From now on the equal revision with the other digest is stale: it is never written again.
    expect(await ledger.claim(claimInput("bz", [y]))).toMatchObject({ kind: "replay_fast", stale: [0] });
    // A higher revision replaces it.
    const c = claimInput("b3", [rec("r1", 3)]);
    await ledger.commit(commitOf(c, (await claimed(ledger, c)).claimToken));
    expect((await json(store, indexKey("sess-1"))).records.r1[0]).toBe(3);
  });

  test("records with no session share one s=_origin index", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("n1", 1, { sessionId: null }), rec("n2", 1, { sessionId: null, source: "codex" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    expect(indexKey(null).endsWith("/s=_origin.json")).toBe(true);
    expect(Object.keys((await json(store, indexKey(null))).records).sort()).toEqual(["n1", "n2"]);
  });
});

/** The store with a barrier: the first `n` reads of `key` wait until all `n` have read (forces a lost CAS). */
function barrierOn(inner: ObjectStore, key: string, n: number) {
  let waiting: (() => void)[] = [];
  let reads = 0, lost = 0;
  const store: ObjectStore = {
    ...inner,
    backend: inner.backend,
    head: (k) => inner.head(k), get: (k, r) => inner.get(k, r), list: (p, c) => inner.list(p, c), presignGet: (k, t) => inner.presignGet(k, t), delete: (k) => inner.delete(k),
    getObject: async (k, r) => {
      if (String(k) === key && reads < n) {
        const got = await inner.getObject(k, r).catch((e) => { throw e; });
        reads++;
        if (reads < n) await new Promise<void>((res) => waiting.push(res));
        else { waiting.forEach((f) => f()); waiting = []; }
        return got;
      }
      return inner.getObject(k, r);
    },
    put: async (k, b, o) => {
      try { return await inner.put(k, b, o); } catch (e) { if (String(k) === key) lost++; throw e; }
    },
  };
  return { store, lost: () => lost };
}

describe("revision index CAS race", () => {
  test("two batches for the same session commit at once: one loses the CAS, re-reads, and both revisions are stored", async () => {
    const base = fresh();
    const setup = ledgerOn(base);
    const a = claimInput("ba", [rec("r1", 1)]), b = claimInput("bb", [rec("r2", 4)]);
    const ca = await claimed(setup, a), cb = await claimed(setup, b);
    // Seed the index so both reads see the same version (create-only races take the same path).
    const s = claimInput("bs", [rec("r0", 1)]);
    await setup.commit(commitOf(s, (await claimed(setup, s)).claimToken));
    const { store, lost } = barrierOn(base, indexKey("sess-1"), 2);
    const ledger = ledgerOn(store);
    await Promise.all([ledger.commit(commitOf(a, ca.claimToken)), ledger.commit(commitOf(b, cb.claimToken))]);
    expect(lost()).toBe(1);
    expect((await json(base, indexKey("sess-1"))).records).toEqual({ r0: [1, "r0:r1", rec("r0", 1).digest], r1: [1, "r1:r1", rec("r1", 1).digest], r2: [4, "r2:r4", rec("r2", 4).digest] });
  });

  test("a CAS that never wins is ledger_revision_contended, never a silent skip", async () => {
    const base = fresh();
    const setup = ledgerOn(base);
    const a = claimInput("ba", [rec("r1", 1)]);
    const ca = await claimed(setup, a);
    const store: ObjectStore = { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r), list: (p, c) => base.list(p, c), presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k),
      put: async (k, b, o) => { if (String(k) === indexKey("sess-1")) throw new ObjectStoreError("precondition_failed", "fs", "lost"); return base.put(k, b, o); } };
    await expect(ledgerOn(store, { casAttempts: 3 }).commit(commitOf(a, ca.claimToken))).rejects.toMatchObject({ code: "ledger_revision_contended" });
    expect(await json(base, keys.batch(ORIGIN, "ba"))).toMatchObject({ state: "pending" });
  });
});

describe("fences: selection, reset, and the commit's recheck", () => {
  test("a disabled session is deleted at claim (records dropped before the revision checks); re-enable lifts it", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false });
    const input = claimInput("b1", [rec("r1", 1), rec("r2", 1, { sessionId: "sess-2" })]);
    expect(await ledger.claim(input)).toMatchObject({ kind: "claimed", deleted: [["claude", "sess-2"]], stale: [] });
    const allDeleted = claimInput("b2", [rec("r3", 1, { sessionId: "sess-2" })]);
    expect(await ledger.claim(allDeleted)).toEqual({ kind: "replay_fast", deleted: [["claude", "sess-2"]], stale: [], resets: 0 });
    // Another origin is not fenced.
    expect(await ledger.claim(claimInput("b4", [rec("r2", 1, { sessionId: "sess-2" })], { originId: "origin-2" }))).toMatchObject({ kind: "claimed", deleted: [] });
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: true });
    expect(await ledger.claim(claimInput("b3", [rec("r4", 1, { sessionId: "sess-2" })]))).toMatchObject({ kind: "claimed", deleted: [] });
  });

  test("a disable forgets the session's stored revisions, so a re-enable writes them again", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("r1", 1)]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
    expect((await json(store, indexKey("sess-1"))).records).toEqual({});
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: true });
    expect(await ledger.claim(claimInput("b2", [rec("r1", 1)]))).toMatchObject({ kind: "claimed", stale: [] });
  });

  test("disable between claim and commit: rearmed, its revisions not stored, the batch committed", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1), rec("r2", 1, { sessionId: "sess-2" })]);
    const c = await claimed(ledger, input);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false });
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 1 });
    expect(await json(store, keys.rearm(ORIGIN, "claude", "sess-2"))).toMatchObject({ v: 1, originId: ORIGIN, source: "claude", sessionId: "sess-2", machineId: MACHINE });
    expect(await exists(store, keys.rearm(ORIGIN, "claude", "sess-1"))).toBe(false);
    expect((await json(store, indexKey("sess-1"))).records.r1).toBeDefined();
    expect(await exists(store, indexKey("sess-2"))).toBe(false);
    expect(await json(store, keys.batch(ORIGIN, "b1"))).toMatchObject({ state: "committed" });
  });

  test("reset between claim and commit of a session it owned: tombstoned, rearmed, committed", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const first = claimInput("b0", [rec("r0", 1)]);
    await ledger.commit(commitOf(first, (await claimed(ledger, first)).claimToken));
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, input);
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(1);
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 1 });
    expect(await exists(store, keys.rearm(ORIGIN, "claude", "sess-1"))).toBe(true);
    expect((await json(store, indexKey("sess-1"))).records).toEqual({});
    expect(await ledger.claim(claimInput("b2", [rec("r5", 1)]))).toMatchObject({ kind: "replay_fast", deleted: [["claude", "sess-1"]], resets: 1 });
  });

  test("reset between claim and commit of a NEW session: rearmed, and superseded (SQL's invalidated) so the retry re-claims", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, input);
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(0);
    await expect(ledger.commit(commitOf(input, c.claimToken))).rejects.toMatchObject({ code: "ledger_attempt_superseded" });
    expect(await exists(store, keys.rearm(ORIGIN, "claude", "sess-1"))).toBe(true);
    expect(await json(store, keys.batch(ORIGIN, "b1"))).toMatchObject({ state: "pending" });
    // The retry claims again against the new fence (the consumer's privacy-journal gate then decides).
    const again = await claimed(ledger, input);
    expect(again).toMatchObject({ kind: "resume", resets: 1 });
    expect(await ledger.commit(commitOf(input, again.claimToken))).toEqual({ rearmed: 0 });
  });

  test("disable then re-enable between claim and commit (epoch moved, not deleted): superseded, rearmed", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, input);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: true });
    await expect(ledger.commit(commitOf(input, c.claimToken))).rejects.toMatchObject({ code: "ledger_attempt_superseded" });
    expect(await exists(store, keys.rearm(ORIGIN, "claude", "sess-1"))).toBe(true);
  });

  test("a selection that lands during the commit's merge is caught by the second fence read", async () => {
    const base = fresh();
    const setup = ledgerOn(base);
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(setup, input);
    let fenceReads = 0;
    const store: ObjectStore = { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), list: (p, c2) => base.list(p, c2), presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k),
      getObject: (k, r) => { if (String(k) === keys.fence) fenceReads++; return base.getObject(k, r); },
      put: async (k, b, o) => {
        const ack = await base.put(k, b, o);
        // Right after the merge lands, the user disables the session (its own fence edit, then index clear).
        if (String(k) === indexKey("sess-1") && fenceReads === 1) await setup.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
        return ack;
      } };
    expect(await ledgerOn(store).commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 1 });
    expect((await json(base, indexKey("sess-1"))).records).toEqual({});
    expect(await exists(base, keys.rearm(ORIGIN, "claude", "sess-1"))).toBe(true);
  });

  test("reset is idempotent by request id, tombstones every owned session of every origin, and counts distinct sessions", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    for (const [batchId, originId, sessionId] of [["b1", ORIGIN, "s1"], ["b2", ORIGIN, "s2"], ["b3", "origin-2", "s1"]] as const) {
      const input = claimInput(batchId, [rec(`r-${batchId}`, 1, { sessionId })], { originId });
      await ledger.commit({ ...commitOf(input, (await claimed(ledger, input)).claimToken), machineId: liveMachineId(TENANT.workspaceId, originId, TENANT.userId) });
    }
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(2);
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(2);
    const fence = await json(store, keys.fence);
    expect(fence).toMatchObject({ resets: 1, resetRequests: { "req-1": 2 }, deleted: { [ORIGIN]: [["claude", "s1"], ["claude", "s2"]], "origin-2": [["claude", "s1"]] } });
    expect(await ledger.resetUploadedSessions(TENANT, "req-2")).toBe(0);
    expect((await json(store, keys.fence)).resets).toBe(2);
  });

  test("origin ids that name Object.prototype members fence like any other; __proto__ refuses (never a silently dropped deletion)", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    for (const originId of ["constructor", "toString", "hasOwnProperty"]) {
      await ledger.setSessionUploads({ tenant: TENANT, originId, sessions: [["claude", "sess-1"]], enabled: false });
      expect(await ledger.claim(claimInput(`b-${originId}`, [rec("r1", 1)], { originId }))).toMatchObject({ kind: "replay_fast", deleted: [["claude", "sess-1"]] });
    }
    expect((await json(store, keys.fence)).deleted.constructor).toEqual([["claude", "sess-1"]]);
    await expect(ledger.setSessionUploads({ tenant: TENANT, originId: "__proto__", sessions: [["claude", "s"]], enabled: false })).rejects.toMatchObject({ code: "ledger_input_refused" });
    await expect(ledger.claim(claimInput("bp", [rec("r1", 1)], { originId: "__proto__" }))).rejects.toMatchObject({ code: "ledger_input_refused" });
    await expect(ledger.resetUploadedSessions(TENANT, "__proto__")).rejects.toMatchObject({ code: "ledger_input_refused" });
  });

  test("an unreadable fence fails closed: a retry, never an empty fence", async () => {
    const store = fresh();
    await store.put(objectKey(keys.fence), new TextEncoder().encode("{\"v\":1}"), { contentType: "application/json" });
    const ledger = ledgerOn(store);
    await expect(ledger.claim(claimInput("b1", [rec("r1", 1)]))).rejects.toMatchObject({ code: "ledger_fence_invalid" });
    await expect(ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s"]], enabled: false })).rejects.toMatchObject({ code: "ledger_fence_invalid" });
    await store.put(objectKey(keys.fence), new TextEncoder().encode("not json"), { contentType: "application/json" });
    await expect(ledger.claim(claimInput("b1", [rec("r1", 1)]))).rejects.toBeInstanceOf(LedgerUnavailableError);
  });

  test("ordered controls: applied, replay with the first count, stale below, and a reused version with another digest refuses", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("r1", 1)]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    const control = (version: string, digest = "d".repeat(64)) => ({ epoch: "e".repeat(64), version, digest, kind: "selection" as const });
    const sel = { tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]] as [string, string][], enabled: false };
    expect(await ledger.applyOrderedControl!({ ...sel, control: control("5") })).toEqual({ control: { ...control("5"), outcome: "applied" }, updated: 1 });
    expect(await ledger.applyOrderedControl!({ ...sel, control: control("5") })).toEqual({ control: { ...control("5"), outcome: "replay" }, updated: 1 });
    expect(await ledger.applyOrderedControl!({ ...sel, control: control("4") })).toEqual({ control: { ...control("4"), outcome: "stale" }, updated: 0 });
    await expect(ledger.applyOrderedControl!({ ...sel, control: control("5", "f".repeat(64)) })).rejects.toMatchObject({ code: "ledger_control_conflict" });
    const reset = { epoch: "e".repeat(64), version: "6", digest: "a".repeat(64), kind: "reset" as const };
    expect(await ledger.applyOrderedControl!({ tenant: TENANT, requestId: "q", control: reset })).toEqual({ control: { ...reset, outcome: "applied" }, deleted: 0 });
    expect(await ledger.applyOrderedControl!({ tenant: TENANT, requestId: "q", control: reset })).toEqual({ control: { ...reset, outcome: "replay" }, deleted: 0 });
  });
});

describe("refusals: never a fallback", () => {
  test("buffered mode refuses at construction; SQL-only calls refuse with clear codes; slots are free", async () => {
    const store = fresh();
    expect(() => s3Ledger(store, ROOT, { acceptanceMode: "buffered" })).toThrow(LedgerUnavailableError);
    const ledger = ledgerOn(store);
    const cases: [string, () => Promise<unknown>][] = [
      ["ledger_s3_unsupported_buffered", () => ledger.prepareBufferStage!({} as never)],
      ["ledger_s3_unsupported_buffered", () => ledger.commitBuffered!({} as never)],
      ["ledger_s3_unsupported_staged_fanout", () => ledger.fanoutFence(TENANT, ORIGIN, [], {} as never)],
      ["ledger_s3_unsupported_staged_fanout", () => ledger.commitRevisions(TENANT, ORIGIN, MACHINE, [], {} as never)],
      ["ledger_s3_unsupported_babysitter_retention", () => ledger.registerBabysitterRetention!({} as never)],
      ["ledger_s3_unsupported_babysitter_retention", () => ledger.babysitterRetentionWritten!({} as never)],
      ["ledger_s3_unsupported_frozen_witness", () => ledger.frozenWitness!({} as never)],
      ["ledger_s3_unsupported_frozen_receipt", () => ledger.frozenReceipt!({} as never)],
      ["ledger_s3_unsupported_source_control", () => ledger.sourceControlBegin!({} as never)],
      ["ledger_s3_unsupported_privacy_authority", () => ledger.privacyAuthorityBegin!({} as never)],
      ["ledger_s3_custody_unsupported", () => ledger.claim({ ...claimInput("b", [rec("r1", 1)]), custody: {} as never })],
      ["ledger_s3_acquisition_unsupported", () => ledger.claim({ ...claimInput("b", [rec("r1", 1)]), acquisition: [] })],
      ["ledger_input_refused", () => ledger.claim({ ...claimInput("b", [rec("r1", 1)]), batchDigest: "nope" })],
      ["ledger_tenant_not_a_segment", () => ledger.claim({ ...claimInput("b", [rec("r1", 1)]), tenant: { ...TENANT, orgId: ".x" } })],
      ["ledger_no_claim", () => ledger.commit(commitOf(claimInput("never", [rec("r1", 1)]), "00000000-0000-4000-8000-000000000000"))],
    ];
    for (const [code, run] of cases) await expect(run()).rejects.toMatchObject({ code });
    expect(await ledger.acquireSlot({ tenant: TENANT, holder: "h", maxPerUser: 0, maxPerOrg: 0, ttlSeconds: 1 })).toEqual({ ok: true });
    await ledger.releaseSlot(TENANT, "h");
  });

  test("a store failure is a coded ledger error, never a message", async () => {
    const base = fresh();
    const store: ObjectStore = { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), list: (p, c) => base.list(p, c), presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k), put: (k, b, o) => base.put(k, b, o),
      getObject: async () => { throw new ObjectStoreError("transient", "fs", "secret key path"); } };
    const err = await ledgerOn(store).claim(claimInput("b1", [rec("r1", 1)])).catch((e) => e);
    expect(err).toBeInstanceOf(LedgerUnavailableError);
    expect(err.code).toBe("ledger_store_transient");
    expect(String(err)).not.toContain("secret key path");
  });
});

describe("through deliverPrepared (inline canonical writes)", () => {
  async function prepared(batchId = "batch-1") {
    return prepareDelivery(TENANT, await batch([record({ record_id: "r1" }), record({ record_id: "r2", session_id: "sess-2" })], { batch_id: batchId }));
  }

  test("a crash between the canonical write and the commit resumes: rewritten idempotently, then committed, then a replay", async () => {
    const store = fresh();
    const inner = ledgerOn(store);
    let crash = 1;
    const ledger: Ledger = { ...inner, commit: async (i) => { if (crash-- > 0) throw new Error("invocation died"); return inner.commit(i); } };
    const deps = { ledger, acceptanceMode: "canonical" as const, target: { store, keyRoot: ROOT } };
    const p = await prepared();
    const failed = await deliverPrepared(deps, TENANT, p).catch((e) => e);
    expect(failed).toBeInstanceOf(IngestUnavailable);
    expect(failed.stage).toBe("commit");
    expect(await json(store, keys.batch(ORIGIN, p.batchId))).toMatchObject({ state: "pending" });
    // The index never leads the manifest: nothing stored yet.
    expect(await exists(store, indexKey("sess-1"))).toBe(false);
    const resumed = await deliverPrepared(deps, TENANT, p);
    expect(resumed.outcome).toBe("accepted");
    expect(resumed.stats.manifests_written).toBe(0); // the manifests already hold this batch (replay extension)
    expect(await json(store, keys.batch(ORIGIN, p.batchId))).toMatchObject({ state: "committed" });
    expect((await deliverPrepared(deps, TENANT, p)).outcome).toBe("replay");
  });

  test("a crash after the index merge but before the batch record commit: the retry is a replay_fast", async () => {
    const base = fresh();
    let fail = true;
    const store: ObjectStore = { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r), list: (p, c) => base.list(p, c), presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k),
      put: async (k, b, o) => {
        if (fail && String(k).includes("/ledger/batches/") && new TextDecoder().decode(b).includes("\"state\":\"committed\"")) { fail = false; throw new ObjectStoreError("transient", "fs", "x"); }
        return base.put(k, b, o);
      } };
    const deps = { ledger: ledgerOn(store), acceptanceMode: "canonical" as const, target: { store, keyRoot: ROOT } };
    const p = await prepared();
    await expect(deliverPrepared(deps, TENANT, p)).rejects.toBeInstanceOf(IngestUnavailable);
    const again = await deliverPrepared(deps, TENANT, p);
    expect(again.outcome).toBe("replay_fast");
    expect(again.stats.stale).toBe(2);
    expect((await deliverPrepared(deps, TENANT, p)).outcome).toBe("replay");
  });
});
