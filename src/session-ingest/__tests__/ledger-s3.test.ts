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
import {
  applyFenceChange, checkIndexEntryInput, entryForgotten, fenceAuthorityKey, ingestLedgerMode, newFence, revisionShard, s3Ledger, s3LedgerKeys, s3LedgerObjects, sessionKeyHash, withSqlControlMirror,
  type FenceDoc,
} from "../ledger-s3.ts";
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
const SK = (source: string, sessionId: string) => sessionKeyHash(source, sessionId);
/** The origin's revision-index shard that holds `recordId`. */
const shardKey = (recordId: string, originId = ORIGIN) => keys.revisions(originId, recordId);
const entryOf = async (store: ObjectStore, recordId: string, originId = ORIGIN) =>
  (await exists(store, shardKey(recordId, originId))) ? (await json(store, shardKey(recordId, originId))).records[recordId] : undefined;
/** `n` distinct record ids that land in ONE revision-index shard (forces CAS races on one object). */
function sameShard(n: number): string[] {
  const out: string[] = [];
  const want = revisionShard("x0");
  for (let i = 0; out.length < n; i++) if (revisionShard(`x${i}`) === want) out.push(`x${i}`);
  return out;
}
/** A store that passes through to `base` except the given methods. */
function wrap(base: ObjectStore, over: Partial<Pick<ObjectStore, "getObject" | "put" | "list">>): ObjectStore {
  return { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r), list: (p, c) => base.list(p, c),
    presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k), put: (k, b, o) => base.put(k, b, o), ...over };
}
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
    expect(await entryOf(store, "r1")).toEqual([2, "r1:x", hex("x"), SK("claude", "sess-1"), 0, 0]);
    // From now on the equal revision with the other digest is stale: it is never written again.
    expect(await ledger.claim(claimInput("bz", [y]))).toMatchObject({ kind: "replay_fast", stale: [0] });
    // A higher revision replaces it.
    const c = claimInput("b3", [rec("r1", 3)]);
    await ledger.commit(commitOf(c, (await claimed(ledger, c)).claimToken));
    expect((await entryOf(store, "r1"))[0]).toBe(3);
  });

  test("records with no session are stored in the origin's index with no session (never forgotten by a control)", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("b1", [rec("n1", 1, { sessionId: null }), rec("n2", 1, { sessionId: null, source: "codex" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    expect(await entryOf(store, "n1")).toEqual([1, "n1:r1", rec("n1", 1).digest, null, 0, 0]);
    expect(await entryOf(store, "n2")).toEqual([1, "n2:r1", rec("n2", 1).digest, null, 0, 0]);
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect(await ledger.claim(claimInput("b2", [rec("n1", 1, { sessionId: null })]))).toMatchObject({ kind: "replay_fast", stale: [0] });
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
    const [x0, x1, x2] = sameShard(3) as [string, string, string];
    const a = claimInput("ba", [rec(x1, 1)]), b = claimInput("bb", [rec(x2, 4, { sessionId: "sess-2" })]);
    const ca = await claimed(setup, a), cb = await claimed(setup, b);
    // Seed the shard so both reads see the same version (create-only races take the same path).
    const s = claimInput("bs", [rec(x0, 1)]);
    await setup.commit(commitOf(s, (await claimed(setup, s)).claimToken));
    const { store, lost } = barrierOn(base, shardKey(x0), 2);
    const ledger = ledgerOn(store);
    await Promise.all([ledger.commit(commitOf(a, ca.claimToken)), ledger.commit(commitOf(b, cb.claimToken))]);
    expect(lost()).toBe(1);
    const g1 = [SK("claude", "sess-1"), 0, 0], g2 = [SK("claude", "sess-2"), 0, 0];
    expect((await json(base, shardKey(x0))).records).toEqual({ [x0]: [1, `${x0}:r1`, rec(x0, 1).digest, ...g1], [x1]: [1, `${x1}:r1`, rec(x1, 1).digest, ...g1], [x2]: [4, `${x2}:r4`, rec(x2, 4).digest, ...g2] });
  });

  test("a CAS that never wins is ledger_revision_contended, never a silent skip", async () => {
    const base = fresh();
    const setup = ledgerOn(base);
    const a = claimInput("ba", [rec("r1", 1)]);
    const ca = await claimed(setup, a);
    const store: ObjectStore = { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r), list: (p, c) => base.list(p, c), presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k),
      put: async (k, b, o) => { if (String(k) === shardKey("r1")) throw new ObjectStoreError("precondition_failed", "fs", "lost"); return base.put(k, b, o); } };
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
    // One fence CAS: the index is not rewritten, its generation (epoch 0) is now stale (the fence is at epoch 1).
    expect(await entryOf(store, "r1")).toEqual([1, "r1:r1", rec("r1", 1).digest, SK("claude", "sess-1"), 0, 0]);
    expect((await json(store, keys.fence)).epochs[ORIGIN]).toEqual({ [createHash("sha256").update(JSON.stringify(["claude", "sess-1"])).digest("hex").slice(0, 32)]: 1 });
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
    expect(await entryOf(store, "r1")).toBeDefined();
    expect(await entryOf(store, "r2")).toBeUndefined();
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
    // The entry keeps the pre-reset generation (resets 0): forgotten under the fence's resets 1.
    expect((await entryOf(store, "r0"))[5]).toBe(0);
    expect(await ledger.claim(claimInput("b2", [rec("r5", 1)]))).toMatchObject({ kind: "replay_fast", deleted: [["claude", "sess-1"]], resets: 1 });
    // Re-enabled: the old revisions are forgotten, so r0 is written again (not stale).
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: true });
    expect(await ledger.claim(claimInput("b3", [rec("r0", 1)]))).toMatchObject({ kind: "claimed", stale: [] });
  });

  test("reset between claim and commit of a session FIRST uploaded by that claim: the claim's ownership stub makes the reset own it, so it ends deleted and re-armed (ingest_session_ownership)", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, input);
    expect(await json(store, keys.owner(ORIGIN, "claude", "sess-1"))).toEqual({ v: 1, originId: ORIGIN, source: "claude", sessionId: "sess-1", epoch: 0, resets: 0 });
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(1);
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 1 });
    expect(await exists(store, keys.rearm(ORIGIN, "claude", "sess-1"))).toBe(true);
    expect(await json(store, keys.batch(ORIGIN, "b1"))).toMatchObject({ state: "committed" });
    expect((await json(store, keys.fence)).deleted).toEqual({ [ORIGIN]: [["claude", "sess-1"]] });
    expect(await entryOf(store, "r1")).toBeUndefined();
    expect(await ledger.claim(claimInput("b2", [rec("r2", 1)]))).toMatchObject({ kind: "replay_fast", deleted: [["claude", "sess-1"]], resets: 1 });
  });

  test("a reset that runs while a claim registers ownership: the claim sees the fence move and claims again under the new generation", async () => {
    const base = fresh();
    const setup = ledgerOn(base);
    let raced = false;
    const store = wrap(base, { put: async (k, b, o) => {
      // Right before the stub lands, a whole reset runs (it cannot see the stub yet).
      if (!raced && String(k) === keys.owner(ORIGIN, "claude", "sess-1")) { raced = true; expect(await setup.resetUploadedSessions(TENANT, "req-1")).toBe(0); }
      return base.put(k, b, o);
    } });
    const ledger = ledgerOn(store);
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, input);
    expect(c).toMatchObject({ kind: "claimed", resets: 1 });
    expect(await json(base, keys.batch(ORIGIN, "b1"))).toMatchObject({ resets: 1 });
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 0 });
    expect(await entryOf(base, "r1")).toEqual([1, "r1:r1", rec("r1", 1).digest, SK("claude", "sess-1"), 0, 1]);
    // The stub kept its oldest generation, so the NEXT reset owns the session.
    expect(await ledger.resetUploadedSessions(TENANT, "req-2")).toBe(1);
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

  test("a disable that lands during the commit's merge leaves those revisions stale (the claim's generation), so a re-enable rewrites them", async () => {
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
        if (String(k) === shardKey("r1") && fenceReads === 1) await setup.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
        return ack;
      } };
    // The disable came after the commit's fence read and after the objects were written: the disable's own
    // erasure sweeps them, so no re-arm; the merged revisions carry the claim's epoch (0) and are stale now.
    expect(await ledgerOn(store).commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 0 });
    expect(await entryOf(base, "r1")).toEqual([1, "r1:r1", rec("r1", 1).digest, SK("claude", "sess-1"), 0, 0]);
    await setup.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: true });
    expect(await setup.claim(claimInput("b2", [rec("r1", 1)]))).toMatchObject({ kind: "claimed", stale: [] });
  });

  test("a commit from before a disable+enable never overwrites the newer generation's index", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const old = claimInput("b-old", [rec("r1", 5)]);
    const co = await claimed(ledger, old);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: true });
    const fresh2 = claimInput("b-new", [rec("r2", 1)]);
    await ledger.commit(commitOf(fresh2, (await claimed(ledger, fresh2)).claimToken));
    expect(await entryOf(store, "r2")).toEqual([1, "r2:r1", rec("r2", 1).digest, SK("claude", "sess-1"), 1, 0]);
    // The old attempt is superseded (epoch moved) and its records never reach the epoch-1 index.
    await expect(ledger.commit(commitOf(old, co.claimToken))).rejects.toMatchObject({ code: "ledger_attempt_superseded" });
    expect(await entryOf(store, "r1")).toBeUndefined();
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
    expect(await entryOf(store, "r1")).toBeUndefined();
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

describe("resets: overlapping and interrupted (each tombstones every session owned below the reset count it sees)", () => {
  /** A store whose LIST of the ownership stubs fails `n` times: a reset that dies right after its begin (the bump). */
  function crashingList(base: ObjectStore, n: number) {
    let left = n;
    return wrap(base, { list: async (p, c) => {
      if (left > 0 && String(p) === keys.ownersRoot) { left--; throw new ObjectStoreError("transient", "fs", "x"); }
      return base.list(p, c);
    } });
  }
  async function committedSessions(ledger: Ledger, ids: string[], tag = "b") {
    for (const id of ids) {
      const input = claimInput(`${tag}-${id}`, [rec(`r-${tag}-${id}`, 1, { sessionId: id })]);
      await ledger.commit(commitOf(input, (await claimed(ledger, input)).claimToken));
    }
  }

  test("a reset that crashes after its bump: the retry (same request id) finishes it", async () => {
    const base = fresh();
    await committedSessions(ledgerOn(base), ["s1", "s2"]);
    await expect(ledgerOn(crashingList(base, 1)).resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_store_transient" });
    expect(await json(base, keys.fence)).toMatchObject({ resets: 1, resetRequests: { "req-1": null }, deleted: {} });
    expect(await ledgerOn(base).resetUploadedSessions(TENANT, "req-1")).toBe(2);
    expect(await json(base, keys.fence)).toMatchObject({ resets: 1, resetRequests: { "req-1": 2 }, deleted: { [ORIGIN]: [["claude", "s1"], ["claude", "s2"]] } });
  });

  test("R1 bumps and stalls, R2 bumps and stalls, then R1 finishes: everything owned at generation 0 is tombstoned (not only resets === count - 1)", async () => {
    const base = fresh();
    await committedSessions(ledgerOn(base), ["s1", "s2"]);
    await expect(ledgerOn(crashingList(base, 1)).resetUploadedSessions(TENANT, "req-1")).rejects.toBeInstanceOf(LedgerUnavailableError);
    await expect(ledgerOn(crashingList(base, 1)).resetUploadedSessions(TENANT, "req-2")).rejects.toBeInstanceOf(LedgerUnavailableError);
    expect((await json(base, keys.fence)).resets).toBe(2);
    expect(await ledgerOn(base).resetUploadedSessions(TENANT, "req-1")).toBe(2);
    // R1's finish also finished R2 (both were running at its listing): one CAS, both counts, resetsCompleted 2.
    const f = await json(base, keys.fence);
    expect(f).toMatchObject({ resets: 2, resetsCompleted: 2, resetRequests: { "req-1": 2, "req-2": 2 }, deleted: { [ORIGIN]: [["claude", "s1"], ["claude", "s2"]] } });
    expect(f.epochs[ORIGIN]).toEqual({ [SK("claude", "s1")]: 1, [SK("claude", "s2")]: 1 });
    // Replays answer their counts.
    expect(await ledgerOn(base).resetUploadedSessions(TENANT, "req-1")).toBe(2);
    expect(await ledgerOn(base).resetUploadedSessions(TENANT, "req-2")).toBe(2);
  });

  test("R1 stalls, R2 runs to completion, then R1's retry finishes with what is left", async () => {
    const base = fresh();
    await committedSessions(ledgerOn(base), ["s1"]);
    await expect(ledgerOn(crashingList(base, 1)).resetUploadedSessions(TENANT, "req-1")).rejects.toBeInstanceOf(LedgerUnavailableError);
    expect(await ledgerOn(base).resetUploadedSessions(TENANT, "req-2")).toBe(1);
    // R2's finish also finished R1, which was running at its listing: R1's retry replays the same count.
    expect(await ledgerOn(base).resetUploadedSessions(TENANT, "req-1")).toBe(1);
    expect(await json(base, keys.fence)).toMatchObject({ resets: 2, resetsCompleted: 2, deleted: { [ORIGIN]: [["claude", "s1"]] } });
  });

  test("a session re-enabled after a reset tombstoned it is not owned again until it uploads again", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    await committedSessions(ledger, ["s1"]);
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(1);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: true });
    expect(await ledger.resetUploadedSessions(TENANT, "req-2")).toBe(0);
    await committedSessions(ledger, ["s1"], "again");
    expect(await ledger.resetUploadedSessions(TENANT, "req-3")).toBe(1);
  });

  test("the reset reads ownership stubs with bounded concurrency (8)", async () => {
    const base = fresh();
    const ledger = ledgerOn(base);
    for (let i = 0; i < 20; i++) await claimed(ledger, claimInput(`b${i}`, [rec(`r${i}`, 1, { sessionId: `s${i}` })])); // a claim registers ownership
    let inFlight = 0, peak = 0;
    const store = wrap(base, { getObject: async (k, r) => {
      if (!String(k).startsWith(keys.ownersRoot + "/")) return base.getObject(k, r);
      inFlight++; peak = Math.max(peak, inFlight);
      await new Promise((res) => setTimeout(res, 2));
      try { return await base.getObject(k, r); } finally { inFlight--; }
    } });
    expect(await ledgerOn(store).resetUploadedSessions(TENANT, "req-1")).toBe(20);
    expect(peak).toBe(8);
  });
});

describe("revisions are judged per (origin, record id), as ingest_record_revisions and the reader's fold", () => {
  test("record R at revision 5 under session A, then R at revision 5 under session B with another digest: stale, never written", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("ba", [rec("R", 5, { sessionId: "A" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    const b = claimInput("bb", [rec("R", 5, { sessionId: "B", revisionId: "R:other", digest: hex("other") })]);
    expect(await ledger.claim(b)).toMatchObject({ kind: "replay_fast", stale: [0] });
    // A lower revision from any session is stale too.
    expect(await ledger.claim(claimInput("bc", [rec("R", 4, { sessionId: "C" })]))).toMatchObject({ kind: "replay_fast", stale: [0] });
    expect(await entryOf(store, "R")).toEqual([5, "R:r5", rec("R", 5).digest, SK("claude", "A"), 0, 0]);
  });

  test("R at revision 5 under a session, then under _origin (no session) with another digest: stale; and the reverse", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("ba", [rec("R", 5, { sessionId: "A" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    expect(await ledger.claim(claimInput("bo", [rec("R", 5, { sessionId: null, digest: hex("o") })]))).toMatchObject({ kind: "replay_fast", stale: [0] });
    const o = claimInput("bo2", [rec("Q", 3, { sessionId: null })]);
    await ledger.commit(commitOf(o, (await claimed(ledger, o)).claimToken));
    expect(await ledger.claim(claimInput("bq", [rec("Q", 3, { sessionId: "A", digest: hex("q") })]))).toMatchObject({ kind: "replay_fast", stale: [0] });
  });

  test("two claims race the same record id from different sessions at an equal revision: the first commit's entry is kept", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("ba", [rec("R", 5, { sessionId: "A" })]);
    const b = claimInput("bb", [rec("R", 5, { sessionId: "B", digest: hex("b") })]);
    const ca = await claimed(ledger, a), cb = await claimed(ledger, b);
    await ledger.commit(commitOf(b, cb.claimToken));
    await ledger.commit(commitOf(a, ca.claimToken));
    expect(await entryOf(store, "R")).toEqual([5, "R:r5", hex("b"), SK("claude", "B"), 0, 0]);
  });

  test("a higher revision moves the record to its new session; disabling the OLD session no longer forgets it", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("ba", [rec("R", 5, { sessionId: "A" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    const b = claimInput("bb", [rec("R", 6, { sessionId: "B" })]);
    await ledger.commit(commitOf(b, (await claimed(ledger, b)).claimToken));
    expect(await entryOf(store, "R")).toEqual([6, "R:r6", rec("R", 6).digest, SK("claude", "B"), 0, 0]);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "A"]], enabled: false });
    expect(await ledger.claim(claimInput("bc", [rec("R", 6, { sessionId: "C" })]))).toMatchObject({ kind: "replay_fast", stale: [0] });
  });

  test("disabling the session that stored R forgets R for the whole origin (SQL deletes that row); the merge replaces the forgotten entry", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("ba", [rec("R", 5, { sessionId: "A" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "A"]], enabled: false });
    const b = claimInput("bb", [rec("R", 5, { sessionId: "B", digest: hex("b") })]);
    const cb = await claimed(ledger, b);
    expect(cb.stale).toEqual([]);
    await ledger.commit(commitOf(b, cb.claimToken));
    expect(await entryOf(store, "R")).toEqual([5, "R:r5", hex("b"), SK("claude", "B"), 0, 0]);
  });

  test("record ids that name Object.prototype members are plain keys (stored, then stale)", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    for (const id of ["__proto__", "constructor", "toString"]) {
      const a = claimInput(`b-${id}`, [rec(id, 2)]);
      expect((await claimed(ledger, a)).stale).toEqual([]);
      await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
      expect(await ledger.claim(claimInput(`b2-${id}`, [rec(id, 2)]))).toMatchObject({ kind: "replay_fast", stale: [0] });
    }
  });

  test("another origin's index is separate", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const a = claimInput("ba", [rec("R", 5, { sessionId: "A" })]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    expect(await ledger.claim(claimInput("bb", [rec("R", 5, { sessionId: "A" })], { originId: "origin-2" }))).toMatchObject({ kind: "claimed", stale: [] });
  });
});

describe("the fence-authority marker (fail closed)", () => {
  const marker = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", writtenAtMs: 1 };
  const put = (store: ObjectStore, v: unknown) => store.put(objectKey(fenceAuthorityKey(ROOT)), new TextEncoder().encode(JSON.stringify(v)), { contentType: "application/json" });

  test("required: absent or invalid is a retry for every claim, never an admit; a valid marker admits; controls are never gated", async () => {
    const store = fresh();
    let now = 0;
    const ledger = ledgerOn(store, { fenceAuthority: "required", nowMs: () => now });
    const input = claimInput("b1", [rec("r1", 1)]);
    await expect(ledger.claim(input)).rejects.toMatchObject({ code: "ledger_fence_authority_missing" });
    expect(await exists(store, keys.batch(ORIGIN, "b1"))).toBe(false);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s9"]], enabled: false });
    for (const bad of [{ ...marker, workerHttpAdmission: "mirrored" }, { ...marker, workerHttpAdmission: undefined }, { ...marker, revisionBackfill: "partial" }, { ...marker, ownershipBackfill: undefined }, { ...marker, rearmSweeper: "pending" }, { ...marker, controlMirror: "off" }, { ...marker, fenceBackfill: "partial" }, { ...marker, extra: 1 }, { ...marker, writtenAtMs: 0 }, { v: 2 }]) {
      await put(store, bad);
      await expect(ledger.claim(input)).rejects.toMatchObject({ code: "ledger_fence_authority_invalid" });
    }
    await put(store, marker);
    expect((await ledger.claim(input)).kind).toBe("claimed");
    // Deleting it is the kill switch, after the cache (60 s by default).
    await store.delete(objectKey(fenceAuthorityKey(ROOT)));
    expect((await ledger.claim(claimInput("b2", [rec("r2", 1)]))).kind).toBe("claimed");
    now += 60_000;
    await expect(ledger.claim(claimInput("b3", [rec("r3", 1)]))).rejects.toMatchObject({ code: "ledger_fence_authority_missing" });
  });

  test("not required (the default): no marker read", async () => {
    const store = fresh();
    expect((await ledgerOn(store).claim(claimInput("b1", [rec("r1", 1)]))).kind).toBe("claimed");
  });
});

describe("self-host s3 controls also write SQL (withSqlControlMirror)", () => {
  function setupMirror(failSql = false) {
    const store = fresh();
    const order: string[] = [];
    const s3 = ledgerOn(store);
    const spy: Ledger = { ...s3,
      setSessionUploads: async (i) => { order.push(`s3:${i.enabled ? "enable" : "disable"}`); return s3.setSessionUploads(i); },
      resetUploadedSessions: async (t, r) => { order.push("s3:reset"); return s3.resetUploadedSessions(t, r); } };
    const sqlSel: unknown[] = [];
    const sql: Pick<Ledger, "setSessionUploads" | "resetUploadedSessions"> = {
      setSessionUploads: async (i) => { order.push(`sql:${i.enabled ? "enable" : "disable"}`); if (failSql) throw new Error("db down"); sqlSel.push({ originId: i.originId, sessions: i.sessions, enabled: i.enabled }); },
      resetUploadedSessions: async () => { order.push("sql:reset"); if (failSql) throw new Error("db down"); return 0; },
    };
    return { store, order, sqlSel, ledger: withSqlControlMirror(spy, sql, store, ROOT) };
  }

  test("selection: S3 first, then SQL; a SQL failure fails the control after S3 applied it", async () => {
    const { store, order, sqlSel, ledger } = setupMirror();
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false });
    expect(order).toEqual(["s3:disable", "sql:disable"]);
    expect(sqlSel).toEqual([{ originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false }]);
    expect((await json(store, keys.fence)).deleted).toEqual({ [ORIGIN]: [["claude", "s1"]] });
    const failing = setupMirror(true);
    await expect(failing.ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false })).rejects.toThrow("db down");
    expect(failing.order).toEqual(["s3:disable", "sql:disable"]);
  });

  test("reset: S3 first, then the SQL reset record, then every session the S3 fence deletes as a SQL disable", async () => {
    const { order, sqlSel, ledger } = setupMirror();
    const input = claimInput("b1", [rec("r1", 1, { sessionId: "s1" })]);
    await ledger.commit(commitOf(input, (await claimed(ledger, input)).claimToken));
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(1);
    expect(order).toEqual(["s3:reset", "sql:reset", "sql:disable"]);
    expect(sqlSel).toEqual([{ originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false }]);
  });

  test("ordered controls reconcile SQL to the fence, even a control S3 judged stale (S3 decides)", async () => {
    const { order, ledger } = setupMirror();
    const control = (version: string) => ({ epoch: "e".repeat(64), version, digest: "d".repeat(64), kind: "selection" as const });
    const sel = { tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]] as [string, string][], enabled: false };
    await ledger.applyOrderedControl!({ ...sel, control: control("5") });
    expect(order).toEqual(["sql:disable"]);
    await ledger.applyOrderedControl!({ ...sel, control: control("4") });
    expect(order).toEqual(["sql:disable", "sql:disable"]);
  });

  test("an S3 failure never reaches SQL", async () => {
    const store = fresh();
    await store.put(objectKey(keys.fence), new TextEncoder().encode("garbage"), { contentType: "application/json" });
    const calls: string[] = [];
    const ledger = withSqlControlMirror(ledgerOn(store), { setSessionUploads: async () => { calls.push("sql"); }, resetUploadedSessions: async () => { calls.push("sql"); return 0; } }, store, ROOT);
    await expect(ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false })).rejects.toMatchObject({ code: "ledger_fence_invalid" });
    await expect(ledger.resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_fence_invalid" });
    expect(calls).toEqual([]);
  });
});

describe("fence identity, keys and the exported fence rules", () => {
  test("session keys hash JSON, so a separator byte inside source cannot collide two sessions", async () => {
    expect(sessionKeyHash("a\u001fb", "c")).not.toBe(sessionKeyHash("a", "b\u001fc"));
    const store = fresh();
    const ledger = ledgerOn(store);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["a\u001fb", "c"]], enabled: false });
    expect((await json(store, keys.fence)).epochs[ORIGIN]).toEqual({ [sessionKeyHash("a\u001fb", "c")]: 1 });
    expect(keys.rearm(ORIGIN, "a\u001fb", "c")).not.toBe(keys.rearm(ORIGIN, "a", "b\u001fc"));
  });

  test("a fence deleted and recreated at the same version between claim and commit is rechecked (instance id), never skipped", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "other"]], enabled: false });
    const input = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, input);
    const first = await json(store, keys.fence);
    expect(await json(store, keys.batch(ORIGIN, "b1"))).toMatchObject({ fenceVersion: first.version, fenceInstance: first.instance });
    await store.delete(objectKey(keys.fence));
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
    const second = await json(store, keys.fence);
    expect(second.version).toBe(first.version);
    expect(second.instance).not.toBe(first.instance);
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 1 });
    expect(await entryOf(store, "r1")).toBeUndefined();
  });

  test("readFence tells absent from present-and-empty; editFence creates through newFence", async () => {
    const store = fresh();
    const io = s3LedgerObjects(store, ROOT, noSleep);
    expect(await io.readFence(keys.fence)).toEqual({ state: "absent" });
    await io.editFence(keys.fence, (f) => ({ next: f, result: undefined }));
    const read = await io.readFence(keys.fence);
    expect(read.state).toBe("present");
    if (read.state === "present") expect(read.fence).toMatchObject({ v: 1, version: 1, resets: 0, lastResetAtMs: 0, deleted: {}, epochs: {}, resetRequests: {} });
    expect(newFence().instance).not.toBe(newFence().instance);
    expect(() => newFence(() => "not-a-uuid")).toThrow(LedgerUnavailableError);
  });

  test("applyFenceChange: a new deletion bumps the epoch once; a new reset id increments resets and raises lastResetAtMs, never lowers it", () => {
    const f: FenceDoc = newFence();
    expect(applyFenceChange(f, { deletions: [{ originId: ORIGIN, sessions: [["claude", "s1"], ["claude", "s1"]] }] })).toEqual({ changed: true, newlyDeleted: [[ORIGIN, "claude", "s1"]], reset: null });
    expect(applyFenceChange(f, { deletions: [{ originId: ORIGIN, sessions: [["claude", "s1"]] }] })).toEqual({ changed: false, newlyDeleted: [], reset: null });
    expect(f.epochs[ORIGIN]).toEqual({ [SK("claude", "s1")]: 1 });
    expect(applyFenceChange(f, { reset: { requestId: "q1", atMs: 2000, result: 3 } })).toMatchObject({ changed: true, reset: "recorded" });
    expect(applyFenceChange(f, { reset: { requestId: "q2", atMs: 1000, result: null } })).toMatchObject({ reset: "recorded" });
    expect(applyFenceChange(f, { reset: { requestId: "q1", atMs: 9000, result: 3 } })).toMatchObject({ changed: false, reset: "known" });
    expect(f).toMatchObject({ resets: 2, lastResetAtMs: 2000, resetRequests: { q1: 3, q2: null } });
    expect(() => applyFenceChange(f, { deletions: [{ originId: "__proto__", sessions: [["a", "b"]] }] })).toThrow(LedgerUnavailableError);
    expect(() => applyFenceChange(f, { reset: { requestId: "q3", atMs: -1, result: null } })).toThrow(LedgerUnavailableError);
  });

  test("every reset raises lastResetAtMs to its time", async () => {
    const store = fresh();
    let now = 5_000;
    const ledger = ledgerOn(store, { nowMs: () => now });
    await ledger.resetUploadedSessions(TENANT, "req-1");
    expect((await json(store, keys.fence)).lastResetAtMs).toBe(5_000);
    now = 9_000;
    await ledger.resetUploadedSessions(TENANT, "req-2");
    expect((await json(store, keys.fence)).lastResetAtMs).toBe(9_000);
  });
});

describe("second review: unfinished resets, mirror reconcile, claim restarts", () => {
  const crashList = (base: ObjectStore) => {
    let left = 1;
    return wrap(base, { list: async (p, c) => { if (left > 0 && String(p) === keys.ownersRoot) { left--; throw new ObjectStoreError("transient", "fs", "x"); } return base.list(p, c); } });
  };

  test("a reset begun but unfinished forgets NOTHING (entries judged as before); the next claim finishes it, tombstones, then forgets", async () => {
    const base = fresh();
    const ledger = ledgerOn(base);
    const a = claimInput("b1", [rec("r1", 1)]);
    await ledger.commit(commitOf(a, (await claimed(ledger, a)).claimToken));
    await expect(ledgerOn(crashList(base)).resetUploadedSessions(TENANT, "req-1")).rejects.toBeInstanceOf(LedgerUnavailableError);
    const begun: FenceDoc = await json(base, keys.fence);
    expect(begun).toMatchObject({ resets: 1, resetsCompleted: 0, resetRequests: { "req-1": null }, deleted: {} });
    // Judged as before by any reader of the exported rules (the drain): the entry is not forgotten.
    expect(entryForgotten(begun, ORIGIN, await entryOf(base, "r1"))).toBe(false);
    // The next claim (any session) finishes the reset before judging.
    const other = claimInput("b2", [rec("q1", 1, { sessionId: "sess-9" })]);
    expect(await claimed(ledger, other)).toMatchObject({ kind: "claimed", resets: 1 });
    const done: FenceDoc = await json(base, keys.fence);
    expect(done).toMatchObject({ resets: 1, resetsCompleted: 1, resetRequests: { "req-1": 1 }, deleted: { [ORIGIN]: [["claude", "sess-1"]] } });
    expect(entryForgotten(done, ORIGIN, await entryOf(base, "r1"))).toBe(true);
    expect(await ledger.claim(claimInput("b3", [rec("r1", 1)]))).toMatchObject({ kind: "replay_fast", deleted: [["claude", "sess-1"]] });
    // The reset's own retry replays the count the claim recorded.
    expect(await ledger.resetUploadedSessions(TENANT, "req-1")).toBe(1);
  });

  test("an unrelated session's fence edits never restart a claim (one attempt suffices); a change to the claim's own session does", async () => {
    const base = fresh();
    const setup = ledgerOn(base);
    let n = 0;
    const noisy = wrap(base, { put: async (k, b, o) => {
      if (String(k).includes("/ledger/owners/")) await setup.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", `other-${n++}`]], enabled: false });
      return base.put(k, b, o);
    } });
    expect(await ledgerOn(noisy, { casAttempts: 1 }).claim(claimInput("b1", [rec("r1", 1)]))).toMatchObject({ kind: "claimed", deleted: [] });
    let once = false;
    const related = wrap(base, { put: async (k, b, o) => {
      if (!once && String(k) === keys.owner(ORIGIN, "claude", "sess-2")) { once = true; await setup.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false }); }
      return base.put(k, b, o);
    } });
    await expect(ledgerOn(related, { casAttempts: 1 }).claim(claimInput("b2", [rec("r2", 1, { sessionId: "sess-2" })]))).rejects.toMatchObject({ code: "ledger_batch_contended" });
    // With a retry left it claims again and sees the deletion.
    expect(await ledgerOn(base).claim(claimInput("b2", [rec("r2", 1, { sessionId: "sess-2" })]))).toMatchObject({ kind: "replay_fast", deleted: [["claude", "sess-2"]] });
  });

  test("mirror: a competing control that lands during this control's SQL write is reconciled; SQL ends at the fence's final state", async () => {
    const store = fresh();
    const s3 = ledgerOn(store);
    const sqlState = new Map<string, boolean>();
    const writes: string[] = [];
    let mirror: Ledger;
    let raced = false;
    const sql: Pick<Ledger, "setSessionUploads" | "resetUploadedSessions"> = {
      setSessionUploads: async (i) => {
        if (!raced && !i.enabled) {
          // The disable's SQL write is slow: a re-enable runs S3 + SQL to completion first, then the disable lands.
          raced = true;
          await mirror.setSessionUploads({ ...i, enabled: true });
        }
        for (const [s, id] of i.sessions) { sqlState.set(`${s}/${id}`, i.enabled); writes.push(`${id}:${i.enabled ? "on" : "off"}`); }
      },
      resetUploadedSessions: async () => 0,
    };
    mirror = withSqlControlMirror(s3, sql, store, ROOT);
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false });
    expect((await json(store, keys.fence)).deleted).toEqual({});
    expect(writes).toEqual(["s1:on", "s1:off", "s1:on"]);
    expect(sqlState.get("claude/s1")).toBe(true);
  });

  test("mirror: a fence that never settles fails the control (bounded), never a silent divergence", async () => {
    const store = fresh();
    const s3 = ledgerOn(store);
    let k = 0;
    const sql: Pick<Ledger, "setSessionUploads" | "resetUploadedSessions"> = {
      setSessionUploads: async () => { await s3.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", `churn-${k++}`]], enabled: false }); },
      resetUploadedSessions: async () => 0,
    };
    await expect(withSqlControlMirror(s3, sql, store, ROOT, { rounds: 3 }).setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false }))
      .rejects.toMatchObject({ code: "ledger_control_mirror_unstable" });
    expect(k).toBe(3);
  });
});

describe("third review follow-ups", () => {
  const crashOwnersList = (base: ObjectStore) => {
    let left = 1;
    return wrap(base, { list: async (p, c) => { if (left > 0 && String(p) === keys.ownersRoot) { left--; throw new ObjectStoreError("transient", "fs", "x"); } return base.list(p, c); } });
  };
  async function committed(ledger: Ledger, batchId: string, sessionId: string) {
    const input = claimInput(batchId, [rec(`r-${batchId}`, 1, { sessionId })]);
    await ledger.commit(commitOf(input, (await claimed(ledger, input)).claimToken));
  }

  test("self-host (claimCompletesResets false): a claim never completes a running reset, so completion always goes through the control path's SQL mirror", async () => {
    const base = fresh();
    await committed(ledgerOn(base), "b0", "s1");
    await expect(ledgerOn(crashOwnersList(base)).resetUploadedSessions(TENANT, "req-1")).rejects.toBeInstanceOf(LedgerUnavailableError);
    const selfhost = ledgerOn(base, { claimCompletesResets: false });
    // The claim admits against the fence as it is (the reset is running) and completes nothing.
    expect(await claimed(selfhost, claimInput("b1", [rec("q1", 1, { sessionId: "s9" })]))).toMatchObject({ kind: "claimed", resets: 1 });
    expect(await json(base, keys.fence)).toMatchObject({ resets: 1, resetsCompleted: 0, resetRequests: { "req-1": null }, deleted: {} });
    // The reset's retry through the mirror completes it: S3 first, then SQL for every tombstoned session.
    const sqlWrites: string[] = [];
    const mirror = withSqlControlMirror(selfhost, {
      setSessionUploads: async (i) => { for (const [, id] of i.sessions) sqlWrites.push(`${id}:${i.enabled ? "on" : "off"}`); },
      resetUploadedSessions: async () => { sqlWrites.push("reset"); return 0; },
    }, base, ROOT);
    expect(await mirror.resetUploadedSessions(TENANT, "req-1")).toBe(1);
    expect(sqlWrites[0]).toBe("reset");
    expect(sqlWrites.slice(1).sort()).toEqual(["s1:off", "s9:on"]);
    expect(await json(base, keys.fence)).toMatchObject({ resetsCompleted: 1, deleted: { [ORIGIN]: [["claude", "s1"]] } });
  });

  test("hosted (default): a claim completes a running reset, backed off before it claims again", async () => {
    const base = fresh();
    await committed(ledgerOn(base), "b0", "s1");
    await expect(ledgerOn(crashOwnersList(base)).resetUploadedSessions(TENANT, "req-1")).rejects.toBeInstanceOf(LedgerUnavailableError);
    const sleeps: number[] = [];
    const ledger = s3Ledger(base, ROOT, { sleep: async (ms) => { sleeps.push(ms); }, random: () => 0.5 });
    expect(await claimed(ledger, claimInput("b1", [rec("q1", 1, { sessionId: "s9" })]))).toMatchObject({ kind: "claimed", resets: 1 });
    expect(sleeps.length).toBe(1);
    expect((await json(base, keys.fence)).resetsCompleted).toBe(1);
  });

  test("a corrupt owner stub fails only the reset's completion (coded), never an unrelated claim", async () => {
    const base = fresh();
    await committed(ledgerOn(base), "b0", "s1");
    await expect(ledgerOn(crashOwnersList(base)).resetUploadedSessions(TENANT, "req-1")).rejects.toBeInstanceOf(LedgerUnavailableError);
    await base.put(objectKey(keys.owner(ORIGIN, "claude", "broken")), new TextEncoder().encode("{\"v\":9}"), { contentType: "application/json" });
    const ledger = ledgerOn(base);
    expect(await claimed(ledger, claimInput("b1", [rec("q1", 1, { sessionId: "s9" })]))).toMatchObject({ kind: "claimed" });
    await expect(ledger.resetUploadedSessions(TENANT, "req-1")).rejects.toMatchObject({ code: "ledger_reset_owner_invalid" });
    expect(await json(base, keys.fence)).toMatchObject({ resetsCompleted: 0, resetRequests: { "req-1": null } });
  });

  test("a finisher never closes the ordered mark of a reset that began after its listing (that reset's retry is not a replay before its tombstones)", async () => {
    const base = fresh();
    await committed(ledgerOn(base), "b0", "s1");
    // Reset A begins and stops before it lists.
    await expect(ledgerOn(crashOwnersList(base)).resetUploadedSessions(TENANT, "req-A")).rejects.toBeInstanceOf(LedgerUnavailableError);
    const y = { epoch: "e".repeat(64), version: "7", digest: "c".repeat(64), kind: "reset" as const };
    let yBegun = false;
    // A's retry: right after its LIST returns, ordered reset Y begins (and stops before its own listing).
    const aStore = wrap(base, { list: async (p, c) => {
      const page = await base.list(p, c);
      if (!yBegun && String(p) === keys.ownersRoot) {
        yBegun = true;
        await expect(ledgerOn(crashOwnersList(base)).applyOrderedControl!({ tenant: TENANT, requestId: "req-Y", control: y })).rejects.toBeInstanceOf(LedgerUnavailableError);
      }
      return page;
    } });
    expect(await ledgerOn(aStore).resetUploadedSessions(TENANT, "req-A")).toBe(1);
    const f = await json(base, keys.fence);
    expect(f.ordered).toMatchObject({ kind: "reset", requestId: "req-Y", result: null });
    expect(f).toMatchObject({ resets: 2, resetsCompleted: 1, resetRequests: { "req-A": 1, "req-Y": null } });
    // Y's retry finishes Y (applied, not replay) and only then closes its mark.
    expect(await ledgerOn(base).applyOrderedControl!({ tenant: TENANT, requestId: "req-Y", control: y })).toMatchObject({ control: { outcome: "applied" } });
    expect(await json(base, keys.fence)).toMatchObject({ resetsCompleted: 2, resetRequests: { "req-Y": 0 }, ordered: { requestId: "req-Y", result: 0 } });
  });

  test("a fence written before resetsCompleted: equal to resets when no reset is running, else fail closed", async () => {
    const store = fresh();
    const legacy = { ...newFence(), resets: 2, lastResetAtMs: 5, resetRequests: { q1: 0, q2: 1 } } as Record<string, unknown>;
    delete legacy.resetsCompleted;
    await store.put(objectKey(keys.fence), new TextEncoder().encode(JSON.stringify(legacy)), { contentType: "application/json" });
    const io = s3LedgerObjects(store, ROOT, noSleep);
    const read = await io.readFence(keys.fence);
    expect(read.state === "present" && read.fence.resetsCompleted).toBe(2);
    expect((await ledgerOn(store).claim(claimInput("b1", [rec("r1", 1)]))).kind).toBe("claimed");
    await store.put(objectKey(keys.fence), new TextEncoder().encode(JSON.stringify({ ...legacy, resetRequests: { q1: 0, q2: null } })), { contentType: "application/json" });
    await expect(io.readFence(keys.fence)).rejects.toMatchObject({ code: "ledger_fence_invalid" });
    await expect(ledgerOn(store).claim(claimInput("b2", [rec("r2", 1)]))).rejects.toMatchObject({ code: "ledger_fence_invalid" });
  });

  test("deleting the marker stops NEW claims after the cache window, but a batch already claimed still commits", async () => {
    const store = fresh();
    let now = 0;
    const marker = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", writtenAtMs: 1 };
    await store.put(objectKey(fenceAuthorityKey(ROOT)), new TextEncoder().encode(JSON.stringify(marker)), { contentType: "application/json" });
    const ledger = ledgerOn(store, { fenceAuthority: "required", nowMs: () => now });
    const a = claimInput("b1", [rec("r1", 1)]);
    const c = await claimed(ledger, a);
    await store.delete(objectKey(fenceAuthorityKey(ROOT)));
    now += 60_000;
    await expect(ledger.claim(claimInput("b2", [rec("r2", 1)]))).rejects.toMatchObject({ code: "ledger_fence_authority_missing" });
    expect(await ledger.commit(commitOf(a, c.claimToken))).toEqual({ rearmed: 0 });
  });
});

describe("revision-only index entries (seeded proven records, lazy native slots)", () => {
  const io = (store: ObjectStore) => s3LedgerObjects(store, ROOT, noSleep);
  const absentView = newFence();
  const entry = (recordId: string, revision: number, revisionId: string | null, digest: string | null, sessionId: string | null = "sess-1") =>
    ({ recordId, revision, revisionId, digest, sessionKey: sessionId === null ? null : SK("claude", sessionId), epoch: 0, resets: 0 });

  test("a revision-only entry judges stale by revision alone, whatever the submitted digest, as SQL's claim step (c)", async () => {
    const store = fresh();
    await io(store).mergeIndex(shardKey("R"), absentView, ORIGIN, [entry("R", 5, null, null)]);
    await io(store).mergeIndex(shardKey("L"), absentView, ORIGIN, [entry("L", 3, null, hex("lazy"), null)]); // a lazy native slot: digest, no revision id
    expect(await entryOf(store, "R")).toEqual([5, null, null, SK("claude", "sess-1"), 0, 0]);
    expect(await entryOf(store, "L")).toEqual([3, null, hex("lazy"), null, 0, 0]);
    const ledger = ledgerOn(store);
    expect(await ledger.claim(claimInput("b1", [rec("R", 5, { digest: hex("any") }), rec("R", 4), rec("L", 3, { sessionId: "sess-2", digest: hex("other") })])))
      .toMatchObject({ kind: "replay_fast", stale: [0, 1, 2] });
    const up = claimInput("b2", [rec("R", 6)]);
    expect(await claimed(ledger, up)).toMatchObject({ kind: "claimed", stale: [] });
    // Max wins: the full revision 6 replaces the revision-only 5.
    await ledger.commit(commitOf(up, (await claimed(ledger, up)).claimToken));
    expect(await entryOf(store, "R")).toEqual([6, "R:r6", rec("R", 6).digest, SK("claude", "sess-1"), 0, 0]);
  });

  test("at an equal revision neither form overwrites the other (the stored entry is kept), and a higher one of either form wins", async () => {
    const store = fresh();
    await io(store).mergeIndex(shardKey("A"), absentView, ORIGIN, [entry("A", 5, "A:r5", hex("a"))]);
    await io(store).mergeIndex(shardKey("A"), absentView, ORIGIN, [entry("A", 5, null, null)]);
    expect(await entryOf(store, "A")).toEqual([5, "A:r5", hex("a"), SK("claude", "sess-1"), 0, 0]);
    await io(store).mergeIndex(shardKey("B"), absentView, ORIGIN, [entry("B", 5, null, null)]);
    await io(store).mergeIndex(shardKey("B"), absentView, ORIGIN, [entry("B", 5, "B:r5", hex("b"))]);
    expect(await entryOf(store, "B")).toEqual([5, null, null, SK("claude", "sess-1"), 0, 0]);
    await io(store).mergeIndex(shardKey("A"), absentView, ORIGIN, [entry("A", 6, null, null)]);
    expect(await entryOf(store, "A")).toEqual([6, null, null, SK("claude", "sess-1"), 0, 0]);
    // In one merge, a full entry is preferred over a revision-only one at the same revision.
    await io(store).mergeIndex(shardKey("C"), absentView, ORIGIN, [entry("C", 2, null, null), entry("C", 2, "C:r2", hex("c"))]);
    expect(await entryOf(store, "C")).toEqual([2, "C:r2", hex("c"), SK("claude", "sess-1"), 0, 0]);
  });

  test("malformed entries refuse (a digest that is not hex, an empty revision id)", async () => {
    const store = fresh();
    await expect(io(store).mergeIndex(shardKey("A"), absentView, ORIGIN, [entry("A", 1, null, "nothex")])).rejects.toMatchObject({ code: "ledger_input_refused" });
    await expect(io(store).mergeIndex(shardKey("A"), absentView, ORIGIN, [entry("A", 1, "", null)])).rejects.toMatchObject({ code: "ledger_input_refused" });
    expect(() => checkIndexEntryInput(entry("A", 0, null, null))).toThrow(LedgerUnavailableError);
  });
});

describe("layered review P2-A: a SQL reset never keeps a tombstone S3 has lifted", () => {
  test("disabled then re-enabled through the mirror (no upload since), then reset: S3 keeps it live, and SQL ends live too", async () => {
    const store = fresh();
    const s3 = ledgerOn(store);
    // A session S3 knows (owner stub from a backfill or an earlier claim), owned in SQL as well.
    const a = claimInput("b0", [rec("r0", 1, { sessionId: "s1" })]);
    await s3.commit(commitOf(a, (await claimed(s3, a)).claimToken));
    const sqlDeleted = new Set<string>();
    const sqlOwned = new Set<string>(["s1"]);
    const mirror = withSqlControlMirror(s3, {
      setSessionUploads: async (i) => { for (const [, id] of i.sessions) { if (i.enabled) sqlDeleted.delete(id); else sqlDeleted.add(id); } },
      // SQL's own reset tombstones everything SQL owns (ingest_record_revisions UNION ingest_session_ownership).
      resetUploadedSessions: async () => { for (const id of sqlOwned) sqlDeleted.add(id); return sqlOwned.size; },
    }, store, ROOT);
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false });
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: true });
    expect(sqlDeleted.has("s1")).toBe(false);
    // The S3 reset does not own s1 (its stub is the pre-disable epoch), so S3 keeps it live.
    expect(await mirror.resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect((await json(store, keys.fence)).deleted).toEqual({});
    // SQL's reset tombstoned it; the reconcile after it lifts the tombstone S3 does not hold.
    expect(sqlDeleted.has("s1")).toBe(false);
  });

  test("the same through an ordered reset", async () => {
    const store = fresh();
    const s3 = ledgerOn(store);
    const a = claimInput("b0", [rec("r0", 1, { sessionId: "s1" })]);
    await s3.commit(commitOf(a, (await claimed(s3, a)).claimToken));
    const sqlDeleted = new Set<string>();
    const mirror = withSqlControlMirror(s3, {
      setSessionUploads: async (i) => { for (const [, id] of i.sessions) { if (i.enabled) sqlDeleted.delete(id); else sqlDeleted.add(id); } },
      resetUploadedSessions: async () => { sqlDeleted.add("s1"); return 1; },
    }, store, ROOT);
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: false });
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "s1"]], enabled: true });
    await mirror.applyOrderedControl!({ tenant: TENANT, requestId: "req-o", control: { epoch: "e".repeat(64), version: "3", digest: "d".repeat(64), kind: "reset" } });
    expect(sqlDeleted.has("s1")).toBe(false);
  });
});
