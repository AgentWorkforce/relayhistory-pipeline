/**
 * The user-erase generation proof (src/session-ingest/user-erase-generation.ts) on the fs store, over the REAL S3
 * ledger (ledger-s3.ts), the real log streams (ledger-log.ts) and the real hold-bridge replay (hold-bridge.ts):
 *   - the ordinal: bound in the reset's own begin CAS; a crash after the begin and its retry keep it; competing resets
 *     get distinct ordinals although their resetRequests counts are equal; copies of SQL's resets get none; a forged
 *     fence (repeated ordinal, ordinal above resets, ordinal without a request) is invalid;
 *   - a NONEMPTY erase history in one replay: an erase before the hold's install (refused, no effect), one racing the
 *     install (ambiguous, refused), one racing the seal (between install and seal: proven), one during the hold
 *     (proven), and an erase committed while the replay pass ran (picked up by the re-read); bridge_complete ok only
 *     over the proven ones;
 *   - a paused pre-reset write resumed after the reset: fenced at commit, re-armed, never re-published; the proof
 *     holds the erase until the sweeper's done record for that exact marker exists;
 *   - a retry after release: the known reset no-ops and the generation-scoped payload erase keeps the post-release
 *     upload (the unconditional eraseLogPayloads would have deleted it: pinned);
 *   - byte classes and bindings: publication generations, the admission inbox, old live owner stubs, old payloads,
 *     the erase's key root, a pending reset, a tampered receipt.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import type { ClaimInput, CommitInput, LedgerRecord, LedgerTenant } from "../ledger.ts";
import { appendBatches, enableLogMode, logKeys, putPayload, type LogBatch, type LogKeys, type LogState } from "../ledger-log.ts";
import { eraseGenerationLogPayloads, eraseLogPayloads } from "../ledger-log-erase.ts";
import { applyFenceChange, newFence, s3Ledger, s3LedgerKeys, sessionKeyHash, validFence, type FenceDoc } from "../ledger-s3.ts";
import { bridgeCompleteVerdict, bridgeDestination, bridgeStore, erasureReceiptKey, replayErasure, requestHash, replayErasures, type Erasure, type Hold, type ReplayDeps, type UserGeneration } from "../hold-bridge.ts";
import { ERASE_ORDER_MARGIN_MS, generationEvidence, generationPreEffect, generationReceiptKey, produceGenerationReceipt, verifyGeneration, type GenerationBinding, type GenerationPorts } from "../user-erase-generation.ts";
import { rearmDoneKey } from "../rearm-sweeper.ts";
import { originInboxDir } from "../async-admission.ts";
import { liveMachineId } from "../writer.ts";
import { scratchStore } from "./support.ts";
import { userEraseByteReceipt, userEraseByteReceiptKey, writeUserEraseByteReceipt, type StoreBucket, type StoreDestination } from "../../session-erase/user-erase-byte-receipt.ts";
import { userErasePrefixes } from "../../session-erase/user-erase.ts";
import { ledgerHomeKey } from "../ledger-store.ts";
import { createHash } from "node:crypto";

setDefaultTimeout(60_000);
const ROOT = "96bulls";
const ORG = "11111111-2222-4333-8444-555555555555";
const E = "0f0e0d0c-0b0a-4908-8706-050403020100";
const SHA = "a".repeat(64);
const NOW = Date.parse("2026-11-27T06:00:00Z");
const INSTALLED = NOW - 3_600_000, SEALED = NOW - 1_800_000, FULL_START = NOW - 2_700_000;
const ORIGIN = "origin-1";
const A: LedgerTenant = { orgId: ORG, workspaceId: "ws-1", userId: "user-a" };
const B: LedgerTenant = { orgId: ORG, workspaceId: "ws-1", userId: "user-b" };
const W = { writer: "w1", now: () => NOW, sleep: async () => {} };
const hex = (v: string) => createHash("sha256").update(v).digest("hex");
const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));

const cleanups: (() => void)[] = [];
afterEach(() => { cleanups.splice(0).forEach((f) => f()); ERASED.clear(); });
const BUCKET = "bucket-under-test";
const MAIN: StoreBucket = { bucket: BUCKET, region: "us-east-1", endpoint: null };
const DEST: StoreDestination = { keyRoot: ROOT, main: MAIN, ledger: null };
/** Requests the tests' SQL erase ran for: request id -> tenant (the stand-in for operator_user_erasures). */
const ERASED = new Map<string, LedgerTenant>();
/**
 * `inner` playing the store the SQL erase's byte phase ran on (`erasedHere`): on first read of a registered request's
 * byte receipt it writes the real receipt (user-erase-byte-receipt.ts), as the erase job would at bytes_verified, with
 * the destination it erased and its exact prefixes. A store that is not that one never has it.
 */
function erasedOn(inner: ObjectStore, at: StoreBucket & { keyRoot: string } = { ...MAIN, keyRoot: ROOT }): ObjectStore {
  const wrapped: ObjectStore = { ...inner, backend: inner.backend, head: (k) => inner.head(k), get: (k, r) => inner.get(k, r), list: (p, c) => inner.list(p, c),
    presignGet: (k, t) => inner.presignGet(k, t), delete: (k) => inner.delete(k), put: (k, b, o) => inner.put(k, b, o),
    getObject: async (k, r) => {
      for (const [id, t] of ERASED) if (String(k) === userEraseByteReceiptKey(ROOT, id) && (await inner.head(k)) === undefined)
        await writeUserEraseByteReceipt(inner, userEraseByteReceipt({ requestId: id, ...t }, at, userErasePrefixes(ROOT, { requestId: id, ...t }, [ORIGIN]), NOW), id);
      return inner.getObject(k, r);
    } };
  return wrapped;
}
const asBridge = (inner: ObjectStore, dest: Partial<StoreDestination> = {}, erasedHere = true) =>
  bridgeStore({ ...DEST, ...dest, credentials: { accessKeyId: "test", secretAccessKey: "test" } as never }, () => (erasedHere ? erasedOn(inner) : inner));
const fresh = (erasedHere = true) => { const s = scratchStore(); cleanups.push(s.done); return asBridge(s.store, {}, erasedHere); };
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const json = async (store: ObjectStore, key: string) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));
const fenceOf = (store: ObjectStore, t: LedgerTenant): Promise<FenceDoc> => json(store, s3LedgerKeys(ROOT, t).fence);
const ledgerOn = (store: ObjectStore) => s3Ledger(store, ROOT, { sleep: async () => {} });

const fenced = { sql: true, direct: true, fallback: true };
/** The ports plus, for these tests only, the store the proof functions are called with (the bridge passes its own). */
type P = GenerationPorts & { store: ObjectStore };
function ports(store: ObjectStore, over: Partial<GenerationPorts> = {}): P {
  return { store, now: () => NOW, userOrigins: async () => [ORIGIN], admissionFencedBefore: async () => fenced, ...over };
}
const evidenceOf = (p: P, b: GenerationBinding) => generationEvidence(p.store, p, b);
const preOf = (p: P, b: GenerationBinding) => generationPreEffect(p.store, p, b);
const produceOf = (p: P, b: GenerationBinding) => produceGenerationReceipt(p.store, p, b);
const verifyOf = (p: P, b: GenerationBinding) => verifyGeneration(p.store, p, b);
const holdView: GenerationBinding["hold"] = { state: "marker_attempted", installedMs: INSTALLED, sealedMs: SEALED };
const full: GenerationBinding["full"] = { run: "full-run-1", startedMs: FULL_START, evidenceSha256: SHA };
function binding(t: LedgerTenant, requestId: string, atMs: number, over: Partial<GenerationBinding> = {}): GenerationBinding {
  ERASED.set(requestId, t);
  return { destination: DEST, keyRoot: ROOT, tenant: t, requestId, epoch: E, hold: holdView, full, erasure: { atMs, bytesVerified: true }, ...over };
}
/** A tenant's admission pseudo-machine id (async-admission.ts admissionUserDir's last segment). */
const adminMachine = (t: LedgerTenant) => s3LedgerKeys(ROOT, t).dir.split("/m=").pop()!;
/** An attribution port naming other tenants' admission pseudo-machines as theirs (stand-in for the authoritative source). */
const othersAdmin = (...ts: LedgerTenant[]) => async (b: GenerationBinding) =>
  new Map(ts.filter((t) => t.userId !== b.tenant.userId).map((t) => [adminMachine(t), "other" as const]));
const generation = (store: ObjectStore, over: Partial<GenerationPorts> = {}): UserGeneration => ({
  ports: ports(store, over),
  binding: (tenant, e) => binding(tenant, e.requestId, e.atMs, { erasure: { atMs: e.atMs, bytesVerified: e.bytesVerified } }),
});
/**
 * The bridge builds a user erase's effects from its own store, so they are observed through it: `reset:<user>` for a
 * write of the user's fence, `payloads:<user>` for a delete of a log payload (consecutive repeats collapsed).
 */
function observed(base: ObjectStore, calls: string[]): ObjectStore {
  const users = [A, B, { ...A, userId: "user-c" }, { ...A, userId: "user-d" }];
  const who = (k: string) => users.find((t) => k.startsWith(s3LedgerKeys(ROOT, t).dir + "/"))?.userId ?? "other";
  const note = (v: string) => { if (calls[calls.length - 1] !== v) calls.push(v); };
  return asBridge({ ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r),
    presignGet: (k, t) => base.presignGet(k, t), list: (p, c) => base.list(p, c),
    delete: async (k) => { if (/\/ledger\/log\/o=[0-9a-f]{32}\/p\//u.test(String(k))) note(`payloads:${who(String(k))}`); return base.delete(k); },
    put: async (k, b, o) => { if (String(k).endsWith("/upload-fences.json")) note(`reset:${who(String(k))}`); return base.put(k, b, o); } }, bridgeDestination(base) ?? {}, false);
}
function bridgeDeps(base: ObjectStore, calls: string[] = [], over: Partial<ReplayDeps> = {}): ReplayDeps {
  const store = observed(base, calls);
  return {
    store, keyRoot: ROOT, org: ORG, epoch: E, now: () => NOW, ledgerOptions: { sleep: async () => {} },
    setSessionUploads: async () => { throw new Error("no session erase here"); },
    generation: generation(store),
    ...over,
  };
}
const hold: Hold = { epoch: E, state: "marker_attempted", installedMs: INSTALLED, sealedMs: SEALED, attemptedMs: NOW - 600_000, attemptEvent: true };
const user = (t: LedgerTenant, requestId: string, atMs: number): Erasure => (ERASED.set(requestId, t), { kind: "user", requestId, workspaceId: t.workspaceId, userId: t.userId, atMs, bytesVerified: true });

// ---- the S3-ledger side: claims, commits, log batches ----

function rec(recordId: string, sessionId = "sess-1"): LedgerRecord {
  return { source: "claude", sessionId, recordId, revisionId: `${recordId}:r1`, revision: 1, digest: hex(recordId) };
}
function claimInput(t: LedgerTenant, batchId: string, records: LedgerRecord[]): ClaimInput {
  const sessions = [...new Map(records.map((r) => [r.sessionId!, ["claude", r.sessionId!] as [string, string]])).values()];
  return { tenant: t, originId: ORIGIN, batchId, batchDigest: hex(`batch:${batchId}`), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions, records };
}
const commitOf = (c: ClaimInput, claimToken: string): CommitInput => ({
  tenant: c.tenant, originId: c.originId, batchId: c.batchId, batchDigest: c.batchDigest, claimToken, sessions: c.sessions, records: c.records,
  machineId: liveMachineId(c.tenant.workspaceId, ORIGIN, c.tenant.userId),
});
async function upload(store: ObjectStore, t: LedgerTenant, batchId: string, sessionId: string) {
  const ledger = ledgerOn(store), c = claimInput(t, batchId, [rec(`r-${batchId}`, sessionId)]);
  const r = await ledger.claim(c);
  if (r.kind !== "claimed") throw new Error(`expected claimed, got ${r.kind}`);
  return { claim: c, token: r.claimToken, commit: () => ledger.commit(commitOf(c, r.claimToken)) };
}
async function stream(store: ObjectStore, t: LedgerTenant): Promise<{ keys: LogKeys; state: LogState }> {
  const keys = logKeys(ROOT, t, ORIGIN);
  return { keys, state: await enableLogMode(store, keys, null, W) };
}
/** One logged batch admitted at fence generation `resets`, with its own payload. Returns the payload key. */
async function logged(store: ObjectStore, s: { keys: LogKeys; state: LogState }, id: string, resets: number): Promise<string> {
  const ref = await putPayload(store, s.keys, enc({ id, nonce: Math.random() }));
  const b: LogBatch = { batchId: id, digest: `d-${id}`, receiptId: `r-${id}`, sessions: [["claude", `s-${id}`]], payloads: [ref], admitted: { journalSeq: 0, resets, epochs: {} } };
  expect((await appendBatches(store, s.state, [b], null, W)).seq).not.toBeNull();
  return ref.key;
}

describe("the ordinal: R's generation, bound in its own begin CAS", () => {
  test("bound at begin; competing resets get distinct ordinals although their tombstone counts are equal", async () => {
    const store = fresh(), ledger = ledgerOn(store);
    await ledger.resetUploadedSessions(A, "req-1");
    await ledger.resetUploadedSessions(A, "req-2");
    const f = await fenceOf(store, A);
    expect(f.resetOrdinals).toEqual({ "req-1": 1, "req-2": 2 });
    // resetRequests holds the count each tombstoned: equal here, so it can never identify the generation.
    expect(f.resetRequests).toEqual({ "req-1": 0, "req-2": 0 });
    // A replay of a known request changes nothing.
    await ledger.resetUploadedSessions(A, "req-1");
    expect((await fenceOf(store, A)).resetOrdinals).toEqual({ "req-1": 1, "req-2": 2 });
  });

  test("a crash after the begin CAS: the retry finishes it and keeps the ordinal; a reset begun meanwhile gets the next one", async () => {
    const base = fresh();
    let failList = true;
    const store: ObjectStore = { ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), getObject: (k, r) => base.getObject(k, r),
      presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k), put: (k, b, o) => base.put(k, b, o),
      list: async (p, c) => { if (failList && String(p).endsWith("/ledger/owners")) { failList = false; throw new Error("injected crash"); } return base.list(p, c); } };
    await expect(ledgerOn(store).resetUploadedSessions(A, "req-1")).rejects.toThrow();
    let f = await fenceOf(base, A);
    expect(f.resetRequests).toEqual({ "req-1": null });
    expect(f.resetOrdinals).toEqual({ "req-1": 1 });
    // Before the retry, a competing reset begins (and finishes both).
    await ledgerOn(base).resetUploadedSessions(A, "req-2");
    await ledgerOn(base).resetUploadedSessions(A, "req-1");
    f = await fenceOf(base, A);
    expect(f.resetOrdinals).toEqual({ "req-1": 1, "req-2": 2 });
    expect(f.resetsCompleted).toBe(2);
  });

  test("copies of SQL's resets (backfill, control mirror) and legacy fences carry no ordinal, and are never proven", async () => {
    const f = newFence();
    applyFenceChange(f, { reset: { requestId: "copied-finished", atMs: NOW, result: 4 } });
    applyFenceChange(f, { reset: { requestId: "copied-running", atMs: NOW, result: null } });
    expect(f.resetOrdinals).toBeUndefined();
    expect(() => applyFenceChange(newFence(), { reset: { requestId: "x", atMs: NOW, result: 1, bindOrdinal: true } })).toThrow();
    // A user whose fence knows R only as a copy: the pre-effect check refuses it.
    const store = fresh();
    await store.put(objectKey(s3LedgerKeys(ROOT, A).fence), enc({ ...f, resetsCompleted: 2, resetRequests: { "copied-finished": 4, "copied-running": 0 } }));
    await expect(preOf(ports(store), binding(A, "copied-finished", NOW))).rejects.toMatchObject({ code: "generation_ordinal_missing" });
    // A forged fence is invalid: a repeated ordinal, one above resets, one without a request.
    const ok = { ...newFence(), resets: 2, resetsCompleted: 2, resetRequests: { a: 0, b: 0 } };
    expect(validFence({ ...ok, resetOrdinals: { a: 1, b: 2 } })).toBe(true);
    for (const bad of [{ a: 1, b: 1 }, { a: 3 }, { c: 1 }, { a: 0 }, { a: 1.5 }, []]) expect(validFence({ ...ok, resetOrdinals: bad })).toBe(false);
  });
});

describe("a nonempty erase history through the bridge replay", () => {
  test("before the install: refused; racing the install: ambiguous; racing the seal and during the hold: proven; bridge_complete only over the proven", async () => {
    const store = fresh(), calls: string[] = [];
    // Each user uploaded before the hold (generation 0): one log batch each and one claimed session.
    for (const t of [A, B]) { await logged(store, await stream(store, t), `old-${t.userId}`, 0); await (await upload(store, t, `pre-${t.userId}`, "sess-old")).commit(); }
    const C: LedgerTenant = { ...A, userId: "user-c" }, D: LedgerTenant = { ...A, userId: "user-d" };
    const preInstall = user(C, "req-pre", INSTALLED - 60_000);
    const racingInstall = user(D, "req-install", INSTALLED + ERASE_ORDER_MARGIN_MS - 1);
    const racingSeal = user(A, "req-seal", SEALED - 1);
    const duringHold = user(B, "req-hold", SEALED + 60_000);
    // Both users' admission directories share the workspace: each is attributed to its own user for the other's proof.
    const d = bridgeDeps(store, calls, { generation: generation(store, { machineOwners: othersAdmin(A, B) }) });
    const outcomes: Record<string, string> = {};
    for (const e of [preInstall, racingInstall, racingSeal, duringHold]) outcomes[e.requestId] = await replayErasure(d, e);
    expect(outcomes).toEqual({ "req-pre": "erasure_generation_unproven", "req-install": "erasure_generation_unproven", "req-seal": "replayed", "req-hold": "replayed" });
    // The refused ones had no effect at all: no reset, no payload erase, no receipt.
    expect(calls).toEqual(["reset:user-a", "payloads:user-a", "reset:user-b", "payloads:user-b"]);
    for (const e of [preInstall, racingInstall]) {
      expect(await exists(store, erasureReceiptKey(ROOT, ORG, E, e.requestId))).toBe(false);
      expect(await exists(store, generationReceiptKey(ROOT, ORG, E, e.requestId))).toBe(false);
    }
    // The proven ones: old payloads gone, old session tombstoned, a generation receipt with hashes only.
    for (const [t, e] of [[A, racingSeal], [B, duringHold]] as const) {
      expect(await eraseLogPayloads(store, ROOT, t)).toBe(0);
      expect((await fenceOf(store, t)).deleted[ORIGIN]).toEqual([["claude", "sess-old"]]);
      const r = await json(store, generationReceiptKey(ROOT, ORG, E, e.requestId));
      expect(r).toMatchObject({ v: 1, kind: "user-erase-generation", epoch: E, keyRoot: ROOT, fence: { generation: 1, resetsCompleted: 1 },
        bytes: { uploadGenerations: 0, inbox: 0, payloads: 0, ownersOld: 0, rearmMarkers: 0 }, producedAtMs: NOW });
      for (const id of [t.userId, t.workspaceId, ORG, e.requestId, "sess-old"]) expect(JSON.stringify(r)).not.toContain(id);
    }
    const proofs = { generation: d.generation };
    expect(await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [racingSeal, duringHold], proofs)).toEqual({ code: "ok", erasures: 2, receipted: 2 });
    expect(await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [preInstall, racingSeal, duringHold], proofs)).toEqual({ code: "erasure_unreplayed", erasures: 3, receipted: 2 });
    expect(await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [racingSeal, duringHold], {})).toMatchObject({ code: "erasure_unreplayed", receipted: 0 });
  });

  test("an erase committed while the replay pass runs: the re-read picks it up, it gets the next ordinal, and the set settles", async () => {
    const store = fresh(), calls: string[] = [];
    const first = user(A, "req-1", SEALED + 1_000), late = user(A, "req-2", SEALED + 2_000);
    let count = 1, reads = 0;
    const d = bridgeDeps(store, calls);
    const r = await replayErasures(d, async () => count, async () => {
      reads++;
      // The second erase commits during the first pass (after the first read of the set).
      if (reads === 1) { count = 2; return [first]; }
      return [first, late];
    });
    expect(r).toMatchObject({ settled: true, erasureCount: 2, replayed: 2, erasure_generation_unproven: 0 });
    expect((await fenceOf(store, A)).resetOrdinals).toEqual({ "req-1": 1, "req-2": 2 });
    expect(calls).toEqual(["reset:user-a"]); // two resets of one user, no payloads
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [first, late], { generation: d.generation })).code).toBe("ok");
  });
});

describe("old-generation writes and new uploads around the boundary", () => {
  test("a paused pre-reset write resumed after the reset is fenced and re-armed, never re-published; the proof waits for its sweep", async () => {
    const store = fresh();
    const paused = await upload(store, A, "paused", "sess-1"); // claimed at generation 0, canonical bytes being written
    const e = user(A, "req-1", SEALED + 1_000);
    const d = bridgeDeps(store);
    expect(await replayErasure(d, e)).toBe("replayed"); // nothing re-armed yet
    // The paused claim resumes after the erase verified, and commits: fenced (the reset moved), re-armed, index not merged.
    expect(await paused.commit()).toEqual({ rearmed: 1 });
    const marker = s3LedgerKeys(ROOT, A).rearm(ORIGIN, "claude", "sess-1");
    expect(await exists(store, marker)).toBe(true);
    // Its revision is forgotten: the same record claimed again is not stale (and the session stays deleted).
    expect(await ledgerOn(store).claim(claimInput(A, "again", [rec("r-paused")]))).toMatchObject({ deleted: [["claude", "sess-1"]] });
    // The proof no longer holds until the sweeper erased that exact marker's bytes.
    expect(await verifyGeneration(d.store, d.generation!.ports, binding(A, "req-1", e.atMs))).toBe("generation_rearm_pending");
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("erasure_unreplayed");
    expect(await replayErasure(d, e)).toBe("erasure_generation_unproven");
    // A done record for OTHER bytes does not count; the one for these bytes does.
    const raw = await readAll((await store.getObject(objectKey(marker))).body);
    await store.put(objectKey(rearmDoneKey(marker, "0".repeat(32))), enc({ v: 1, outcome: "erased", marker: "0".repeat(32), atMs: NOW }));
    expect(await verifyGeneration(d.store, d.generation!.ports, binding(A, "req-1", e.atMs))).toBe("generation_rearm_pending");
    const digest = createHash("sha256").update(raw).digest("hex").slice(0, 32);
    await store.put(objectKey(rearmDoneKey(marker, digest)), enc({ v: 1, outcome: "erased", marker: digest, atMs: NOW }));
    expect(await verifyGeneration(d.store, d.generation!.ports, binding(A, "req-1", e.atMs))).toBeNull();
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("ok");
  });

  test("a retry after release: the known reset no-ops and the scoped payload erase keeps the post-release upload (unconditional erase would not)", async () => {
    const store = fresh(), s = await stream(store, A);
    const old = await logged(store, s, "old", 0);
    const e = user(A, "req-1", SEALED + 1_000), calls: string[] = [];
    const d = bridgeDeps(store, calls);
    expect(await replayErasure(d, e)).toBe("replayed");
    expect(await exists(store, old)).toBe(false);
    // Released; the user uploads again: a new session at generation 1 and a log batch admitted at 1.
    await (await upload(store, A, "new", "sess-new")).commit();
    const fresh1 = await logged(store, s, "new", 1);
    // The bridge's replay is retried (its erasure receipt lost, e.g. a crash before the write).
    await store.delete(objectKey(erasureReceiptKey(ROOT, ORG, E, "req-1")));
    expect(await replayErasure(d, e)).toBe("replayed");
    // The known reset changed no fence and the scoped erase deleted nothing.
    expect(calls).toEqual(["reset:user-a", "payloads:user-a"]);
    expect(await exists(store, fresh1)).toBe(true);
    const f = await fenceOf(store, A);
    expect(f.resets).toBe(1);
    expect(f.deleted[ORIGIN] ?? []).not.toContainEqual(["claude", "sess-new"]);
    // The generation receipt is unchanged by the retry (re-derived and matched, not rewritten).
    expect((await json(store, generationReceiptKey(ROOT, ORG, E, "req-1"))).producedAtMs).toBe(NOW);
    // Pinned: the unconditional whole-user erase WOULD delete the post-release upload.
    expect(await eraseLogPayloads(store, ROOT, A)).toBe(1);
    expect(await exists(store, fresh1)).toBe(false);
  });
});

test("a caller binding naming another epoch, request, tenant or key root never proves or receipts this erase", async () => {
  const store = fresh(), e = user(A, "req-1", SEALED + 1_000), calls: string[] = [];
  const good = generation(store);
  for (const over of [{ epoch: "1f0e0d0c-0b0a-4908-8706-050403020100" }, { requestId: "req-2" }, { tenant: B }, { keyRoot: "other" }, { erasure: { atMs: e.atMs + 1, bytesVerified: true } }]) {
    const g: UserGeneration = { ports: good.ports, binding: (t, x) => ({ ...good.binding(t, x), ...over }) };
    expect(await replayErasure(bridgeDeps(store, calls, { generation: g }), e)).toBe("erasure_generation_unproven");
    expect(calls).toEqual([]);
  }
  const d = bridgeDeps(store, calls);
  expect(await replayErasure(d, e)).toBe("replayed");
  const swapped: UserGeneration = { ports: good.ports, binding: (t, x) => ({ ...good.binding(t, x), epoch: "1f0e0d0c-0b0a-4908-8706-050403020100" }) };
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: swapped })).code).toBe("erasure_unreplayed");
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: good })).code).toBe("ok");
});

describe("negative controls (layered-sessions-pipeline counterexamples at 48175448)", () => {
  test("(1) a payload put before the reset, crash before its append: an orphan the proof must refuse, never pass", async () => {
    const store = fresh(), s = await stream(store, A);
    const orphan = await putPayload(store, s.keys, enc({ orphan: true })); // crash here: never appended
    const e = user(A, "req-1", SEALED + 1_000), d = bridgeDeps(store);
    expect(await replayErasure(d, e)).toBe("erasure_generation_unproven");
    expect(await exists(store, orphan.key)).toBe(true); // the scoped erase never deletes what it cannot attribute
    expect(await exists(store, erasureReceiptKey(ROOT, ORG, E, "req-1"))).toBe(false);
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("erasure_unreplayed");
    // Orphan collection takes it; the retry then proves.
    await store.delete(objectKey(orphan.key));
    expect(await replayErasure(d, e)).toBe("replayed");
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("ok");
  });

  test("(1b) applied residue and an unsnapshotted batch's payload refuse; a payload positively bound at >= G is kept and passes", async () => {
    const store = fresh(), s = await stream(store, A);
    await ledgerOn(store).resetUploadedSessions(A, "req-1");
    const b = binding(A, "req-1", SEALED + 1_000);
    const newer = await logged(store, s, "newer", 1);
    expect((await evidenceOf(ports(store), b)).bytes.payloads).toBe(0);
    expect(await exists(store, newer)).toBe(true);
    // The same batch's entry marked applied (below the cursor) while its payload is still present: residue.
    const keysOf = s.keys;
    for (let seq = 0; seq <= 1; seq++) await store.put(objectKey(keysOf.marker(seq)), enc({ v: 1, seq }));
    await expect(evidenceOf(ports(store), b)).rejects.toMatchObject({ code: "generation_payloads_remain" });
    // A batch with no admission snapshot: not positively bound.
    const store2 = fresh(), s2 = await stream(store2, A);
    await ledgerOn(store2).resetUploadedSessions(A, "req-1");
    const ref = await putPayload(store2, s2.keys, enc({ x: 1 }));
    await appendBatches(store2, s2.state, [{ batchId: "u", digest: "d-u", receiptId: "r-u", sessions: [["claude", "s-u"]], payloads: [ref] }], null, W);
    await expect(evidenceOf(ports(store2), b)).rejects.toMatchObject({ code: "generation_payloads_remain" });
  });

  test("(2) split stores: a generation receipt proved in store B never counts for store A, which kept an old payload", async () => {
    const a = fresh(), bStore = fresh();
    const old = await logged(a, await stream(a, A), "old", 0);
    const e = user(A, "req-1", SEALED + 1_000);
    await ledgerOn(bStore).resetUploadedSessions(A, "req-1");
    await produceOf(ports(bStore), binding(A, "req-1", e.atMs));
    await ledgerOn(a).resetUploadedSessions(A, "req-1");
    await a.put(objectKey(erasureReceiptKey(ROOT, ORG, E, "req-1")), enc({ v: 1, kind: "hold-bridge-erasure", epoch: E, request: requestHash("req-1"), erasure: "user",
      counts: { reset: 0, payloads: 0, owners: 0 }, appliedAtMs: NOW }));
    expect(await exists(a, old)).toBe(true);
    const g = generation(a);
    expect((await bridgeCompleteVerdict(a, ROOT, ORG, hold, E, [e], { generation: g })).code).toBe("erasure_unreplayed");
    expect(await verifyGeneration(a, g.ports, binding(A, "req-1", e.atMs))).toBe("generation_ordinal_missing");
    await a.put(objectKey(generationReceiptKey(ROOT, ORG, E, "req-1")), await readAll((await bStore.getObject(objectKey(generationReceiptKey(ROOT, ORG, E, "req-1")))).body));
    expect(await verifyGeneration(a, g.ports, binding(A, "req-1", e.atMs))).toBe("generation_payloads_remain");
    expect((await bridgeCompleteVerdict(a, ROOT, ORG, hold, E, [e], { generation: g })).code).toBe("erasure_unreplayed");
  });

  test("(2c) physical store B configured with bucket label A throughout: no byte receipt there, refused before any effect; A keeps its old payload", async () => {
    // The SQL erase ran on physical A (its byte receipt is there); the bridge was misconfigured onto physical B, every
    // label saying A.
    const physA = fresh(true), physB = fresh(false);
    const old = await logged(physA, await stream(physA, A), "old", 0);
    const e = user(A, "req-1", SEALED + 1_000), calls: string[] = [];
    const d = bridgeDeps(physB, calls);
    expect(await replayErasure(d, e)).toBe("erasure_generation_unproven");
    expect(calls).toEqual([]);
    expect(await exists(physB, s3LedgerKeys(ROOT, A).fence)).toBe(false);
    expect((await bridgeCompleteVerdict(physB, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("erasure_unreplayed");
    await expect(preOf(ports(physB), binding(A, "req-1", e.atMs))).rejects.toMatchObject({ code: "generation_byte_receipt_missing" });
    expect(await exists(physA, old)).toBe(true);
    // The same bridge on physical A proves.
    expect(await replayErasure(bridgeDeps(physA), e)).toBe("replayed");
    expect(await exists(physA, old)).toBe(false);
  });

  test("(2b) the destination comes from the store's construction config and is proven in the store: region, endpoint, key root, routing", async () => {
    const e = user(A, "req-1", SEALED + 1_000);
    // A store not built by bridgeStore has no destination; a `bucket` field beside it is ignored.
    const raw = scratchStore(); cleanups.push(raw.done);
    const loose = { ...bridgeDeps(fresh()), store: raw.store, bucket: BUCKET } as unknown as ReplayDeps;
    expect(await replayErasure(loose, e)).toBe("erasure_generation_unproven");
    expect(await exists(raw.store, s3LedgerKeys(ROOT, A).fence)).toBe(false);
    // The byte phase ran at another region or endpoint than this store's config: its receipt names it, refused.
    for (const at of [{ ...MAIN, region: "eu-west-1" }, { ...MAIN, endpoint: "https://other.example" }]) {
      const s1 = scratchStore(); cleanups.push(s1.done);
      const store = bridgeStore({ ...DEST, credentials: {} as never }, () => erasedOn(s1.store, { ...at, keyRoot: ROOT }));
      const calls: string[] = [];
      expect(await replayErasure(bridgeDeps(store, calls), e)).toBe("erasure_generation_unproven");
      expect(calls).toEqual([]);
      await expect(preOf(ports(store), binding(A, "req-1", e.atMs))).rejects.toMatchObject({ code: "generation_destination_mismatch" });
    }
    // A caller binding naming another destination than the store's own: refused before any effect.
    const store = fresh(), calls: string[] = [];
    const g = generation(store);
    const other: UserGeneration = { ports: g.ports, binding: (t, x) => ({ ...g.binding(t, x), destination: { ...DEST, main: { ...MAIN, bucket: "other-bucket" } } }) };
    expect(await replayErasure(bridgeDeps(store, calls, { generation: other }), e)).toBe("erasure_generation_unproven");
    expect(calls).toEqual([]);
    expect(await replayErasure(bridgeDeps(fresh(), [], { keyRoot: "other-root" }), e)).toBe("erasure_generation_unproven");
    // Routing: a relocated ledger bucket must be named by the ledger-home marker IN the store; without it, refused.
    const m = scratchStore(), l = scratchStore(); cleanups.push(m.done, l.done);
    const LEDGER: StoreBucket = { bucket: "ledger-bucket", region: "us-east-1", endpoint: null };
    const routed = bridgeStore({ ...DEST, ledger: LEDGER, credentials: {} as never }, (c) => (c.bucket === BUCKET ? erasedOn(m.store) : l.store));
    expect(bridgeDestination(routed)).toEqual({ ...DEST, ledger: LEDGER });
    const rb = binding(A, "req-1", e.atMs, { destination: { ...DEST, ledger: LEDGER } });
    await expect(preOf(ports(routed), rb)).rejects.toMatchObject({ code: "generation_destination_mismatch" });
    await routed.put(objectKey(ledgerHomeKey(ROOT)), enc({ v: 1, home: "ledger-bucket", writtenAtMs: NOW }));
    expect(await preOf(ports(routed), rb)).toEqual({ generation: null });
    // Without routing, a home marker naming another bucket (the ledger moved away) refuses.
    const moved = fresh();
    await moved.put(objectKey(ledgerHomeKey(ROOT)), enc({ v: 1, home: "ledger-bucket", writtenAtMs: NOW }));
    await expect(preOf(ports(moved), binding(A, "req-1", e.atMs))).rejects.toMatchObject({ code: "generation_destination_mismatch" });
    expect(() => bridgeStore({ ...DEST, ledger: MAIN, credentials: {} as never }, () => raw.store)).toThrow();
  });
});

describe("negative controls (layered-sessions-pipeline counterexamples at 24b1b97a)", () => {
  test("(3) an admission-shaped name under an UNATTRIBUTED machine (an origin missing from SQL and stubs) refuses, through replay and the gate", async () => {
    const store = fresh(), e = user(A, "req-1", SEALED + 1_000), calls: string[] = [];
    const body = `${originInboxDir(ROOT, A, "origin-missing-everywhere")}/ledger/log/o=unexpected/p/body`;
    await store.put(objectKey(body), enc({ bytes: "old" }));
    const d = bridgeDeps(store, calls);
    expect(await replayErasure(d, e)).toBe("erasure_generation_unproven");
    expect(await exists(store, erasureReceiptKey(ROOT, ORG, E, "req-1"))).toBe(false);
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("erasure_unreplayed");
    expect(await exists(store, body)).toBe(true);
    await expect(evidenceOf(ports(store), binding(A, "req-1", e.atMs))).rejects.toMatchObject({ code: "generation_machine_unattributed" });
    // Another tenant's real admission directory, attributed: passes; unattributed: refuses.
    const st = fresh(), b = binding(A, "req-1", e.atMs);
    await ledgerOn(st).resetUploadedSessions(A, "req-1");
    await ledgerOn(st).resetUploadedSessions(B, "req-b");
    await expect(evidenceOf(ports(st), b)).rejects.toMatchObject({ code: "generation_machine_unattributed" });
    expect((await evidenceOf(ports(st, { machineOwners: othersAdmin(A, B) }), b)).fence.generation).toBe(1);
  });

  test("(4) a byte receipt for another SQL origin set refuses BEFORE any write (no reset, no delete), and again after", async () => {
    const store = fresh(), s = await stream(store, A);
    const old = await logged(store, s, "old", 0);
    const e = user(A, "req-1", SEALED + 1_000), calls: string[] = [];
    // The receipt (erasedOn) covers [ORIGIN]; this erase's SQL view names a second origin.
    const g = generation(store, { userOrigins: async () => [ORIGIN, "origin-2"] });
    expect(await replayErasure(bridgeDeps(store, calls, { generation: g }), e)).toBe("erasure_generation_unproven");
    expect(calls).toEqual([]);
    expect(await exists(store, s3LedgerKeys(ROOT, A).fence)).toBe(false);
    expect(await exists(store, old)).toBe(true);
    await expect(preOf(ports(store, { userOrigins: async () => [ORIGIN, "origin-2"] }), binding(A, "req-1", e.atMs))).rejects.toMatchObject({ code: "generation_destination_mismatch" });
    // A port that throws is refused before any write too.
    const calls2: string[] = [];
    expect(await replayErasure(bridgeDeps(store, calls2, { generation: generation(store, { userOrigins: async () => { throw new Error("x"); } }) }), e)).toBe("erasure_generation_unproven");
    expect(calls2).toEqual([]);
    // After the effects, the post-effect evidence re-checks it: matching origins prove, a changed set refuses.
    expect(await replayErasure(bridgeDeps(store), e)).toBe("replayed");
    await expect(evidenceOf(ports(store, { userOrigins: async () => [ORIGIN, "origin-2"] }), binding(A, "req-1", e.atMs))).rejects.toMatchObject({ code: "generation_destination_mismatch" });
  });
});

describe("byte classes, bindings and receipts", () => {
  async function proven(store: ObjectStore) {
    await ledgerOn(store).resetUploadedSessions(A, "req-1");
    return binding(A, "req-1", SEALED + 1_000);
  }

  test("every class below G must be empty: publication generations, the inbox, live old owner stubs, old payloads", async () => {
    const machine = liveMachineId(A.workspaceId, ORIGIN, A.userId), other = liveMachineId(B.workspaceId, ORIGIN, B.userId);
    const gen = (m: string) => `${ROOT}/${ORG}/upload-generations/g=01234567-1234-4234-8234-0123456789ab/v1/ws=ws-1/m=${m}/h=claude/d=2026/11/01/s=s/publications/p/manifest.json`;
    let store = fresh(); let b = await proven(store);
    await store.put(objectKey(gen(other)), enc({}));
    // Another user's machine counts only when attributed: unattributed refuses, `other` passes.
    await expect(evidenceOf(ports(store), b)).rejects.toMatchObject({ code: "generation_machine_unattributed" });
    const attributed = ports(store, { machineOwners: async () => new Map([[other, "other" as const]]) });
    expect((await evidenceOf(attributed, b)).bytes.origins).toBe(2);
    // A machine in another workspace is not judged here.
    await store.put(objectKey(gen(liveMachineId("ws-2", ORIGIN, A.userId)).replace("/ws=ws-1/", "/ws=ws-2/")), enc({}));
    expect((await evidenceOf(attributed, b)).bytes.origins).toBe(2);
    await store.put(objectKey(gen(machine)), enc({}));
    await expect(evidenceOf(attributed, b)).rejects.toMatchObject({ code: "generation_bytes_remain" });
    // Attributing a derived machine of this user to someone else is a contradiction.
    await expect(evidenceOf(ports(store, { machineOwners: async () => new Map([[machine, "other" as const]]) }), b)).rejects.toMatchObject({ code: "generation_unreadable" });

    store = fresh(); b = await proven(store);
    await store.put(objectKey(`${originInboxDir(ROOT, A, ORIGIN)}/r=${"c".repeat(64)}/a=${"0".repeat(15)}-${"d".repeat(16)}.ndjson.zst`), enc({}));
    await expect(evidenceOf(ports(store), b)).rejects.toMatchObject({ code: "generation_bytes_remain" });
    // An outcome record is not a body.
    store = fresh(); b = await proven(store);
    await store.put(objectKey(`${originInboxDir(ROOT, A, ORIGIN)}/r=${"c".repeat(64)}/outcome-admitted.json`), enc({}));
    expect((await evidenceOf(ports(store), b)).bytes.inbox).toBe(0);

    store = fresh(); b = await proven(store);
    const keys = s3LedgerKeys(ROOT, A);
    await store.put(objectKey(keys.owner(ORIGIN, "claude", "late")), enc({ v: 1, originId: ORIGIN, source: "claude", sessionId: "late", epoch: 0, resets: 0 }));
    await expect(evidenceOf(ports(store), b)).rejects.toMatchObject({ code: "generation_owner_live" });

    store = fresh(); b = await proven(store);
    const unindexed = await stream(store, A);
    await logged(store, unindexed, "straggler", 0);
    await expect(evidenceOf(ports(store), b)).rejects.toMatchObject({ code: "generation_payloads_remain" });
    await eraseGenerationLogPayloads(store, ROOT, A, 1);
    expect((await evidenceOf(ports(store), b)).bytes.payloads).toBe(0);
  });

  test("an origin with bytes but no owner stub and no SQL row is still found (every machine under the workspace)", async () => {
    const unknownOrigin = "origin-without-stub-or-row";
    const m = liveMachineId(A.workspaceId, unknownOrigin, A.userId);
    for (const key of [`${ROOT}/${ORG}/upload-generations/g=01234567-1234-4234-8234-0123456789ab/v1/ws=ws-1/m=${m}/h=claude/d=2026/11/01/s=s/manifest.json`,
      `${originInboxDir(ROOT, A, unknownOrigin)}/r=${"c".repeat(64)}/a=${"0".repeat(15)}-${"d".repeat(16)}.ndjson.zst`]) {
      const store = fresh(), b = await proven(store);
      await store.put(objectKey(key), enc({}));
      await expect(evidenceOf(ports(store), b)).rejects.toMatchObject({ code: "generation_machine_unattributed" });
      // The attribution port naming it this user's: bytes remain.
      await expect(evidenceOf(ports(store, { machineOwners: async () => new Map([[m, "self" as const]]) }), b)).rejects.toMatchObject({ code: "generation_bytes_remain" });
      // And through the bridge: no erasure receipt.
      expect(await replayErasure(bridgeDeps(store), user(A, "req-1", SEALED + 1_000))).toBe("erasure_generation_unproven");
      expect(await exists(store, erasureReceiptKey(ROOT, ORG, E, "req-1"))).toBe(false);
    }
  });

  test("a key that does not parse, in any class in the user's scope, refuses (never skipped)", async () => {
    const keys = s3LedgerKeys(ROOT, A), inbox = `${ROOT}/.admission-inbox/${ORG}/v1/ws=ws-1`;
    for (const odd of [
      `${ROOT}/${ORG}/upload-generations/g=01234567-1234-4234-8234-0123456789ab/v2/ws=ws-1/m=x/manifest.json`,
      `${ROOT}/${ORG}/upload-generations/not-a-generation/v1/ws=ws-1/m=x/manifest.json`,
      `${inbox}/m=${liveMachineId(A.workspaceId, ORIGIN, A.userId)}/r=${"c".repeat(64)}/a=1-2.ndjson`,
      `${inbox}/m=someone/stray.bin`,
      `${keys.dir}/stray.json`,
      `${keys.dir}/ledger/log/o=unexpected/p/body`,
      `${keys.dir}/ledger/log/o=${"a".repeat(32)}/p/not-a-payload`,
      `${keys.dir}/ledger/unknown-class/x.json`,
      `${keys.dir}/ledger/rearm/o=${"a".repeat(32)}/${"b".repeat(32)}.tmp`,
      `${keys.dir}/ledger/rearm/stray.json`,
      `${keys.ownersRoot}/o=${"a".repeat(32)}/stray.json`,
    ]) {
      const store = fresh(), b = await proven(store);
      await store.put(objectKey(odd), enc({}));
      // Refused either way (a payload-shaped key inside a real stream root is also counted by the payload class).
      const code = await evidenceOf(ports(store), b).then(() => null, (e) => e.code);
      expect(odd.includes("/p/not-a-payload") ? ["generation_unreadable", "generation_payloads_remain"] : ["generation_unreadable"]).toContain(code);
    }
    // The lead's reproduction, through the release gate: a payload in a malformed stream root of the user's own
    // admission directory survives the replay and must hold bridge_complete.
    {
      const st = fresh(), e = user(A, "req-1", SEALED + 1_000), d = bridgeDeps(st);
      await st.put(objectKey(`${keys.dir}/ledger/log/o=unexpected/p/body`), enc({ bytes: "old" }));
      expect(await replayErasure(d, e)).toBe("erasure_generation_unproven");
      expect((await bridgeCompleteVerdict(st, ROOT, ORG, hold, E, [e], { generation: d.generation })).code).toBe("erasure_unreplayed");
    }
    // A ledger-shaped object under one of this user's UPLOAD machines is not the ledger's: judged as bytes.
    for (const name of ["ledger/log/o=x/p/y", "upload-fences.json", "privacy-journal.json"]) {
      const st = fresh(), bb = await proven(st);
      await st.put(objectKey(`${inbox}/m=${liveMachineId(A.workspaceId, ORIGIN, A.userId)}/${name}`), enc({}));
      await expect(evidenceOf(ports(st), bb)).rejects.toMatchObject({ code: "generation_bytes_remain" });
    }
    // The sweeper's done and corrupt records are recognized, not refused.
    const store = fresh(), b = await proven(store);
    await store.put(objectKey(`${keys.dir}/ledger/rearm/o=${"a".repeat(32)}/${"b".repeat(32)}.corrupt/${"c".repeat(32)}.json`), enc({}));
    expect((await evidenceOf(ports(store), b)).bytes.rearmMarkers).toBe(0);
  });

  test("bindings: bytes_verified, the erase's key root, the bound FULL, the hold state, a pending reset, admission evidence", async () => {
    const store = fresh(), b = await proven(store);
    const pre = (bb: GenerationBinding, p: P = ports(store)) => preOf(p, bb).then(() => null, (e) => e.code);
    expect(await pre(b)).toBeNull();
    expect(await pre({ ...b, erasure: { ...b.erasure, bytesVerified: false } })).toBe("erasure_bytes_unverified");
    expect(await pre({ ...b, destination: { ...DEST, keyRoot: "other-root" } })).toBe("generation_hold_invalid");
    expect(await pre({ ...b, destination: { ...DEST, main: { ...MAIN, bucket: "other-bucket" } } })).toBe("generation_destination_mismatch");
    expect(await pre({ ...b, destination: { ...DEST, main: { ...MAIN, bucket: "" } } })).toBe("generation_hold_invalid");
    // A byte receipt for another request or tenant at this key: refused.
    const st2 = fresh(false);
    await ledgerOn(st2).resetUploadedSessions(A, "req-1");
    await writeUserEraseByteReceipt(st2, userEraseByteReceipt({ requestId: "req-1", ...B }, { ...MAIN, keyRoot: ROOT }, [], NOW), "req-1");
    expect(await pre(b, ports(st2))).toBe("generation_destination_mismatch");
    expect(await pre({ ...b, full: null })).toBe("generation_full_unbound");
    expect(await pre({ ...b, full: { ...full!, startedMs: INSTALLED } })).toBe("generation_full_unbound");
    expect(await pre({ ...b, full: { ...full!, startedMs: SEALED + 1 } })).toBe("generation_full_unbound");
    expect(await pre({ ...b, hold: { ...holdView, state: "held", sealedMs: null } })).toBe("generation_hold_invalid");
    expect(await pre({ ...b, epoch: "not-a-uuid" })).toBe("generation_hold_invalid");
    for (const a of [undefined, async () => ({ sql: true, direct: true, fallback: false }), async () => { throw new Error("x"); }])
      expect(await pre(b, ports(store, { admissionFencedBefore: a as GenerationPorts["admissionFencedBefore"] }))).toBe("generation_admission_unproven");
    const f = await fenceOf(store, A);
    await store.put(objectKey(s3LedgerKeys(ROOT, A).fence), enc({ ...f, v: 2, pending: { k: { kind: "reset", requestId: "other" } } }));
    expect(await pre(b)).toBe("generation_reset_pending");
  });

  test("the receipt: create-only, re-derived and matched on retry and at the gate; tampering or a moved fence refuses", async () => {
    const store = fresh(), b = await proven(store), p = ports(store);
    const r = await produceOf(p, b);
    expect(await produceOf({ ...p, now: () => NOW + 1 }, b)).toEqual(r);
    expect(await verifyOf(p, b)).toBeNull();
    const key = generationReceiptKey(ROOT, ORG, E, "req-1");
    for (const tampered of [{ ...r, fence: { ...r.fence, generation: 2 } }, { ...r, full: "b".repeat(64) }, { ...r, bytes: { ...r.bytes, payloads: 1 } }, { ...r, extra: 1 }]) {
      await store.delete(objectKey(key));
      await store.put(objectKey(key), enc(tampered));
      expect(await verifyOf(p, b)).toBe("generation_receipt_mismatch");
      await expect(produceOf(p, b)).rejects.toMatchObject({ code: "generation_receipt_mismatch" });
    }
    await store.delete(objectKey(key));
    await store.put(objectKey(key), enc(r));
    // Another bound FULL or another user's erase never matches this receipt.
    expect(await verifyOf(p, { ...b, full: { ...full!, evidenceSha256: "c".repeat(64) } })).toBe("generation_receipt_mismatch");
    // A recreated fence (new instance) is another generation line: refused.
    const f = await fenceOf(store, A);
    await store.put(objectKey(s3LedgerKeys(ROOT, A).fence), enc({ ...f, instance: "ffffffff-ffff-4fff-8fff-ffffffffffff" }));
    expect(await verifyOf(p, b)).toBe("generation_receipt_mismatch");
    // Later resets only move resetsCompleted forward: still the same proof.
    await store.put(objectKey(s3LedgerKeys(ROOT, A).fence), enc(f));
    await ledgerOn(store).resetUploadedSessions(A, "req-later");
    expect(await verifyOf(p, b)).toBeNull();
    expect(sessionKeyHash("claude", "x")).toMatch(/^[0-9a-f]{32}$/);
  });
});
