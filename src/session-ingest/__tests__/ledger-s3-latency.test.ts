/**
 * The S3 ledger's latency changes (decision 0005 amendment): the 4096-shard revision index and its one-time reshard
 * from the 64-shard layout, the claim's and commit's concurrent reads, the `parallel` option, and the admission
 * phase timings. Each ordering guarantee the concurrent reads could have weakened is asserted on the store's own
 * request order: ownership stubs before the claim's fence re-read, the commit's fence read after every canonical
 * write, the index merges after it and the batch record's commit last.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { ADMISSION_PHASE_FIELDS, sanitizeLogField } from "../../shared/log.ts";
import { deliverPrepared } from "../deliver.ts";
import { LedgerUnavailableError, type ClaimInput, type CommitInput, type LedgerRecord } from "../ledger.ts";
import { LEGACY_REVISION_SHARDS, REVISION_SHARDS, SERVER_LEDGER_PARALLEL, revisionShard, s3Ledger, s3LedgerKeys, s3LedgerObjects, serverLedgerParallel, sessionKeyHash } from "../ledger-s3.ts";
import { reshardRevisions } from "../revision-reshard.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { liveMachineId } from "../writer.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const MACHINE = liveMachineId(TENANT.workspaceId, ORIGIN, TENANT.userId);
const hex = (v: string) => createHash("sha256").update(v).digest("hex");
const keys = s3LedgerKeys(ROOT, TENANT);
const enc = new TextEncoder();

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function fresh() {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}
const noSleep = { sleep: async () => {} };
const json = async (store: ObjectStore, key: string) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
function rec(recordId: string, revision: number, o: Partial<LedgerRecord> = {}): LedgerRecord {
  return { source: "claude", sessionId: "sess-1", recordId, revisionId: `${recordId}:r${revision}`, revision, digest: hex(`${recordId}:${revision}`), ...o };
}
function claimInput(batchId: string, records: LedgerRecord[]): ClaimInput {
  const sessions = [...new Map(records.filter((r) => r.sessionId).map((r) => [`${r.source}\u001f${r.sessionId}`, [r.source, r.sessionId!] as [string, string]])).values()];
  return { tenant: TENANT, originId: ORIGIN, batchId, batchDigest: hex(`batch:${batchId}:${JSON.stringify(records)}`), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions, records };
}
const commitOf = (c: ClaimInput, claimToken: string): CommitInput => ({
  tenant: c.tenant, originId: c.originId, batchId: c.batchId, batchDigest: c.batchDigest, claimToken, sessions: c.sessions, records: c.records, machineId: MACHINE,
});
/** A store that logs every request as `<op> <key>` (in completion order for puts, issue order for reads). */
function logged(base: ObjectStore, log: string[], over: Partial<Pick<ObjectStore, "getObject" | "put">> = {}): ObjectStore {
  return {
    ...base, backend: base.backend, head: (k) => base.head(k), get: (k, r) => base.get(k, r), list: (p, c) => base.list(p, c),
    presignGet: (k, t) => base.presignGet(k, t), delete: (k) => base.delete(k),
    getObject: async (k, r) => { log.push(`get ${String(k)}`); return (over.getObject ?? base.getObject.bind(base))(k, r); },
    put: async (k, b, o) => { const out = await (over.put ?? base.put.bind(base))(k, b, o); log.push(`put ${String(k)}`); return out; },
  };
}
/** The legacy 64-shard name of a record id (sha256(recordId)[0] % 64, two hex digits). */
const legacyShard = (recordId: string) => (createHash("sha256").update(recordId).digest()[0]! % LEGACY_REVISION_SHARDS).toString(16).padStart(2, "0");
const legacyKey = (recordId: string, originId = ORIGIN) => keys.revisionShard(originId, legacyShard(recordId));

describe("the 4096-shard revision index", () => {
  test("a record's shard is three hex digits below 4096, so no key can be a legacy (two-digit) one", () => {
    expect(REVISION_SHARDS).toBe(4096);
    const seen = new Set<string>();
    for (let i = 0; i < 20_000; i++) {
      const s = revisionShard(`r${i}`);
      expect(s).toMatch(/^[0-9a-f]{3}$/);
      expect(parseInt(s, 16)).toBeLessThan(4096);
      seen.add(s);
    }
    expect(seen.size).toBeGreaterThan(3900); // spread over (nearly) all shards
    expect(keys.revisions(ORIGIN, "r1")).toMatch(/\/ledger\/revisions\/o=[0-9a-f]{32}\/r=[0-9a-f]{3}\.json$/);
  });

  test("the shard is the first three hex digits (12 bits) of sha256(recordId), as decision 0005 says", () => {
    // Known vectors: sha256("") = e3b0c442..., sha256("r1") = 82f..., sha256("record-123") = 35b..., sha256("a") = ca97...
    expect(revisionShard("")).toBe("e3b");
    expect(revisionShard("r1")).toBe("82f");
    expect(revisionShard("record-123")).toBe("35b");
    expect(revisionShard("a")).toBe("ca9");
    for (let i = 0; i < 1000; i++) expect(revisionShard(`id-${i}`)).toBe(hex(`id-${i}`).slice(0, 3));
  });

  test("a merge into a legacy (two-digit) shard key is refused", async () => {
    const io = s3LedgerObjects(fresh(), ROOT);
    const fence = (await import("../ledger-s3.ts")).newFence();
    await expect(io.mergeIndex(legacyKey("r1"), fence, ORIGIN, [{ recordId: "r1", revision: 1, revisionId: "r1:r1", digest: hex("x"), sessionKey: null, epoch: 0, resets: 0 }]))
      .rejects.toMatchObject({ code: "ledger_input_refused" });
  });
});

describe("parallel", () => {
  test("INGEST_LEDGER_PARALLEL: unset or empty is SERVER_LEDGER_PARALLEL, 1..64 integers are taken, anything else refuses", () => {
    expect(serverLedgerParallel({})).toBe(SERVER_LEDGER_PARALLEL);
    expect(serverLedgerParallel({ INGEST_LEDGER_PARALLEL: "" })).toBe(SERVER_LEDGER_PARALLEL);
    for (const ok of ["1", "8", "64"]) expect(serverLedgerParallel({ INGEST_LEDGER_PARALLEL: ok })).toBe(Number(ok));
    for (const bad of ["0", "65", "08", "1.5", "x", " 8", "-1", "100"]) expect(() => serverLedgerParallel({ INGEST_LEDGER_PARALLEL: bad })).toThrow(LedgerUnavailableError);
  });

  test("1..64 integers only; servers use SERVER_LEDGER_PARALLEL", () => {
    for (const bad of [0, 65, 1.5, -1, Number.NaN]) expect(() => s3Ledger(fresh(), ROOT, { parallel: bad })).toThrow(LedgerUnavailableError);
    for (const ok of [1, 6, 32, 64]) expect(() => s3Ledger(fresh(), ROOT, { parallel: ok })).not.toThrow();
    expect(SERVER_LEDGER_PARALLEL).toBeGreaterThanOrEqual(1);
    expect(SERVER_LEDGER_PARALLEL).toBeLessThanOrEqual(64);
  });
});

describe("concurrent reads keep every ordering guarantee", () => {
  test("claim: a batch-record failure is answered before a fence failure read at the same time", async () => {
    const base = fresh();
    const input = claimInput("b1", [rec("r1", 1)]);
    await base.put(objectKey(keys.batch(ORIGIN, "b1")), enc.encode("{not json"), { contentType: "application/json" });
    await base.put(objectKey(keys.fence), enc.encode("{not json"), { contentType: "application/json" });
    await expect(s3Ledger(base, ROOT, noSleep).claim(input)).rejects.toMatchObject({ code: "ledger_batch_record_invalid" });
  });

  test("claim: a replay (committed batch id) reads no index shard", async () => {
    const base = fresh();
    const input = claimInput("b1", [rec("r1", 1), rec("r2", 1)]);
    const first = s3Ledger(base, ROOT, noSleep);
    const c = await first.claim(input);
    if (c.kind !== "claimed") throw new Error(c.kind);
    await first.commit(commitOf(input, c.claimToken));
    const log: string[] = [];
    expect((await s3Ledger(logged(base, log), ROOT, noSleep).claim(input)).kind).toBe("replay");
    expect(log.filter((l) => l.includes("/ledger/revisions/"))).toEqual([]);
    // A batch-id 409 (same id, other digest) reads none either.
    log.length = 0;
    expect((await s3Ledger(logged(base, log), ROOT, noSleep).claim({ ...input, batchDigest: hex("other") })).kind).toBe("batch_conflict");
    expect(log.filter((l) => l.includes("/ledger/revisions/"))).toEqual([]);
  });

  test("claim: a corrupt shard holding only a deleted session's records never fails the batch", async () => {
    const base = fresh();
    const ledger = s3Ledger(base, ROOT, noSleep);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false });
    let d = 0;
    while (revisionShard(`d${d}`) === revisionShard("live-1")) d++;
    await base.put(objectKey(keys.revisions(ORIGIN, `d${d}`)), enc.encode("{corrupt"), { contentType: "application/json" });
    const input = claimInput("b-mixed", [rec("live-1", 1), rec(`d${d}`, 1, { sessionId: "sess-2" })]);
    expect((await ledger.claim(input)).kind).toBe("claimed");
    // The same corrupt shard under a LIVE record still fails closed.
    const bad = claimInput("b-live", [rec(`d${d}`, 1)]);
    await expect(ledger.claim(bad)).rejects.toMatchObject({ code: "ledger_revision_index_invalid" });
  });

  test("commit: a missing claim is answered as ledger_no_claim even when the fence read also fails", async () => {
    const base = fresh();
    const input = claimInput("b1", [rec("r1", 1)]);
    await base.put(objectKey(keys.fence), enc.encode("{not json"), { contentType: "application/json" });
    await expect(s3Ledger(base, ROOT, noSleep).commit(commitOf(input, "00000000-0000-4000-8000-000000000000"))).rejects.toMatchObject({ code: "ledger_no_claim" });
  });

  test("through deliverPrepared: stubs before the claim's fence re-read; the commit's fence read after every canonical write; merges after it; the batch commit last", async () => {
    const base = fresh();
    const log: string[] = [];
    const store = logged(base, log);
    const deps = { ledger: s3Ledger(store, ROOT, { ...noSleep, parallel: 32 }), acceptanceMode: "canonical" as const, target: { store, keyRoot: ROOT } };
    const p = await prepareDelivery(TENANT, await batch(Array.from({ length: 40 }, (_, i) => record({ record_id: `r${i}` }))));
    expect((await deliverPrepared(deps, TENANT, p)).outcome).toBe("accepted");
    const at = (pred: (l: string) => boolean, from = 0) => { const i = log.findIndex((l, j) => j >= from && pred(l)); expect(i).toBeGreaterThanOrEqual(0); return i; };
    const lastAt = (pred: (l: string) => boolean) => log.findLastIndex(pred);
    const fenceGets = log.map((l, i) => [l, i] as const).filter(([l]) => l === `get ${keys.fence}`).map(([, i]) => i);
    expect(fenceGets.length).toBe(3); // claim, claim re-read, commit
    const ownerPut = lastAt((l) => l.startsWith("put ") && l.includes("/ledger/owners/"));
    expect(ownerPut).toBeLessThan(fenceGets[1]!); // ownership before the claim's fence re-read
    const batchCreate = at((l) => l === `put ${keys.batch(ORIGIN, p.batchId)}`);
    const firstCanonical = at((l) => l.startsWith("put ") && !l.includes("/.admission-inbox/"));
    expect(batchCreate).toBeLessThan(firstCanonical); // the claim lands before any canonical write
    const lastCanonical = lastAt((l) => l.startsWith("put ") && !l.includes("/.admission-inbox/"));
    expect(fenceGets[2]!).toBeGreaterThan(lastCanonical); // the commit's fence recheck follows the canonical writes
    const firstMerge = at((l) => l.startsWith("put ") && l.includes("/ledger/revisions/"));
    expect(firstMerge).toBeGreaterThan(fenceGets[2]!);
    const batchCommit = lastAt((l) => l === `put ${keys.batch(ORIGIN, p.batchId)}`);
    expect(batchCommit).toBeGreaterThan(lastAt((l) => l.startsWith("put ") && l.includes("/ledger/revisions/")));
    expect(await json(base, keys.batch(ORIGIN, p.batchId))).toMatchObject({ state: "committed" });
  });

  test("a session disabled between claim and commit is still fenced (re-armed, its revisions never stored)", async () => {
    const store = fresh();
    const ledger = s3Ledger(store, ROOT, noSleep);
    const input = claimInput("b1", [rec("r1", 1), rec("r2", 1, { sessionId: "sess-2" })]);
    const c = await ledger.claim(input);
    if (c.kind !== "claimed") throw new Error(c.kind);
    await ledger.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false });
    expect(await ledger.commit(commitOf(input, c.claimToken))).toEqual({ rearmed: 1 });
    expect((await json(store, keys.revisions(ORIGIN, "r1"))).records.r1[0]).toBe(1);
    expect(await exists(store, keys.revisions(ORIGIN, "r2"))).toBe(false);
    expect(await exists(store, keys.rearm(ORIGIN, "claude", "sess-2"))).toBe(true);
  });
});

describe("reshardRevisions (64 -> 4096 shards)", () => {
  async function seedLegacy(store: ObjectStore, entries: Record<string, [number, string | null, string | null, string | null, number, number]>) {
    const shards = new Map<string, Record<string, unknown>>();
    for (const [id, e] of Object.entries(entries)) {
      const k = legacyKey(id);
      shards.set(k, { ...(shards.get(k) ?? {}), [id]: e });
    }
    for (const [k, records] of shards)
      await store.put(objectKey(k), enc.encode(JSON.stringify({ v: 1, originId: ORIGIN, shard: /r=([0-9a-f]{2})\.json$/.exec(k)![1], records })), { contentType: "application/json" });
    return [...shards.keys()];
  }
  const SK1 = sessionKeyHash("claude", "sess-1"), SK2 = sessionKeyHash("claude", "sess-2");
  const prefix = `${ROOT}/.admission-inbox/${TENANT.orgId}/`;

  test("dry run reads only; execute carries every live entry, so a re-upload is stale again; reruns change nothing", async () => {
    const base = fresh();
    const entries = Object.fromEntries(Array.from({ length: 300 }, (_, i) => [`r${i}`, [2, `r${i}:r2`, hex(`r${i}:2`), SK1, 0, 0] as [number, string, string, string, number, number]]));
    const legacy = await seedLegacy(base, entries);
    const before = await Promise.all(legacy.map((k) => json(base, k)));
    const puts: string[] = [];
    const store = logged(base, puts);
    const dry = await reshardRevisions(store, ROOT, { prefix, execute: false });
    expect(dry).toMatchObject({ legacy_shards: legacy.length, entries: 300, would_merge: 300, merged: 0, invalid: 0, failed: 0 });
    expect(puts.filter((l) => l.startsWith("put "))).toEqual([]);
    const run = await reshardRevisions(store, ROOT, { prefix, execute: true });
    expect(run).toMatchObject({ entries: 300, merged: 300, invalid: 0, failed: 0 });
    for (const id of ["r0", "r150", "r299"]) expect((await json(base, keys.revisions(ORIGIN, id))).records[id]).toEqual(entries[id]);
    // A claim of a carried record at its stored revision is stale (#250 holds again).
    const c = await s3Ledger(base, ROOT, noSleep).claim(claimInput("b-old", [rec("r7", 2)]));
    expect(c.kind).toBe("replay_fast");
    // Rerun: the same merges, no change; legacy shards untouched.
    puts.length = 0;
    expect(await reshardRevisions(store, ROOT, { prefix, execute: true })).toMatchObject({ merged: 300, failed: 0 });
    expect(puts.filter((l) => l.startsWith("put "))).toEqual([]);
    expect(await Promise.all(legacy.map((k) => json(base, k)))).toEqual(before);
  });

  test("an entry the fence has forgotten (its session disabled since) is not carried", async () => {
    const store = fresh();
    await seedLegacy(store, { a1: [1, "a1:r1", hex("a1"), SK1, 0, 0], b1: [1, "b1:r1", hex("b1"), SK2, 0, 0] });
    await s3Ledger(store, ROOT, noSleep).setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false });
    expect(await reshardRevisions(store, ROOT, { prefix, execute: true })).toMatchObject({ entries: 2, failed: 0 });
    expect((await json(store, keys.revisions(ORIGIN, "a1"))).records.a1).toBeDefined();
    expect(await exists(store, keys.revisions(ORIGIN, "b1"))).toBe(false);
  });

  test("max wins against a newer admission: a reshard never lowers a stored revision", async () => {
    const store = fresh();
    await seedLegacy(store, { x: [1, "x:r1", hex("x:1"), SK1, 0, 0] });
    const ledger = s3Ledger(store, ROOT, noSleep);
    const input = claimInput("b-new", [rec("x", 3)]);
    const c = await ledger.claim(input);
    if (c.kind !== "claimed") throw new Error(c.kind);
    await ledger.commit(commitOf(input, c.claimToken));
    await reshardRevisions(store, ROOT, { prefix, execute: true });
    expect((await json(store, keys.revisions(ORIGIN, "x"))).records.x[0]).toBe(3);
  });

  test("an invalid legacy shard, or one naming another origin than its key, is counted and skipped", async () => {
    const store = fresh();
    await store.put(objectKey(legacyKey("q1")), enc.encode("{bad"), { contentType: "application/json" });
    const k2 = legacyKey("q2-other");
    await store.put(objectKey(k2), enc.encode(JSON.stringify({ v: 1, originId: "origin-2", shard: /r=([0-9a-f]{2})\.json$/.exec(k2)![1], records: { "q2-other": [1, null, null, null, 0, 0] } })), { contentType: "application/json" });
    expect(await reshardRevisions(store, ROOT, { prefix, execute: true })).toMatchObject({ legacy_shards: 2, invalid: 2, merged: 0 });
  });

  test("verify: before execute every live legacy entry is missing; after execute none is; forgotten entries pass", async () => {
    const store = fresh();
    await seedLegacy(store, { v1: [2, "v1:r2", hex("v1"), SK1, 0, 0], v2: [1, "v2:r1", hex("v2"), SK1, 0, 0], g1: [1, "g1:r1", hex("g1"), SK2, 0, 0] });
    await s3Ledger(store, ROOT, noSleep).setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false });
    const puts: string[] = [];
    const before = await reshardRevisions(logged(store, puts), ROOT, { prefix, execute: false, verify: true });
    expect(before).toMatchObject({ entries: 3, verified: 1, missing: 2, invalid: 0, failed: 0 }); // g1: forgotten
    expect(puts.filter((l) => l.startsWith("put "))).toEqual([]); // verify is read-only
    await reshardRevisions(store, ROOT, { prefix, execute: true });
    expect(await reshardRevisions(store, ROOT, { prefix, execute: false, verify: true })).toMatchObject({ entries: 3, verified: 3, missing: 0 });
  });

  test("verify: a new-layout entry at a LOWER revision of the same generation is missing", async () => {
    const store = fresh();
    await seedLegacy(store, { w: [5, "w:r5", hex("w5"), SK1, 0, 0] });
    await s3LedgerObjects(store, ROOT).mergeIndex(keys.revisions(ORIGIN, "w"), (await import("../ledger-s3.ts")).newFence(), ORIGIN,
      [{ recordId: "w", revision: 2, revisionId: "w:r2", digest: hex("w2"), sessionKey: SK1, epoch: 0, resets: 0 }]);
    expect(await reshardRevisions(store, ROOT, { prefix, execute: false, verify: true })).toMatchObject({ verified: 0, missing: 1 });
  });

  test("execute and verify together are refused", async () => {
    await expect(reshardRevisions(fresh(), ROOT, { prefix, execute: true, verify: true })).rejects.toThrow("reshard_mode_refused");
  });

  test("--org keeps its trailing slash: org-a does not pick up org-ab's shards", async () => {
    const store = fresh();
    await seedLegacy(store, { o1: [1, "o1:r1", hex("o1"), SK1, 0, 0] });
    const other = s3LedgerKeys(ROOT, { ...TENANT, orgId: `${TENANT.orgId}b` });
    const ok = other.revisionShard(ORIGIN, legacyShard("o2"));
    await store.put(objectKey(ok), enc.encode(JSON.stringify({ v: 1, originId: ORIGIN, shard: legacyShard("o2"), records: { o2: [1, null, null, null, 0, 0] } })), { contentType: "application/json" });
    expect(ok.startsWith(`${ROOT}/.admission-inbox/${TENANT.orgId}b/`)).toBe(true);
    expect(await reshardRevisions(store, ROOT, { prefix, execute: false })).toMatchObject({ legacy_shards: 1, entries: 1 });
  });

  test("a prefix outside the admission inbox is refused", async () => {
    await expect(reshardRevisions(fresh(), ROOT, { prefix: `${ROOT}/org-a/`, execute: false })).rejects.toThrow("reshard_prefix_refused");
  });
});

describe("admission phase timings", () => {
  test("each phase field is an allowlisted, bounded count; anything else is dropped", () => {
    expect([...ADMISSION_PHASE_FIELDS.keys()].sort()).toEqual(["admission_gate", "claim", "commit", "finish", "objects", "read"]);
    for (const field of ADMISSION_PHASE_FIELDS.values()) {
      expect(sanitizeLogField(field, 1234)).toBe(1234);
      expect(sanitizeLogField(field, "1234")).toBeUndefined();
      expect(sanitizeLogField(field, -1)).toBeUndefined();
      expect(sanitizeLogField(field, 10 ** 12)).toBe(3_600_000);
    }
    expect(sanitizeLogField("claim_ms", 5)).toBeUndefined();
  });

  test("deliverPrepared times the admission gate as its own phase", async () => {
    const store = fresh();
    const deps = { ledger: s3Ledger(store, ROOT, noSleep), acceptanceMode: "canonical" as const, target: { store, keyRoot: ROOT }, admissionGate: async () => {} };
    const p = await prepareDelivery(TENANT, await batch([record({ record_id: "r1" })]));
    const r = await deliverPrepared(deps, TENANT, p);
    expect(Object.keys(r.stats.timing_ms)).toEqual(expect.arrayContaining(["claim", "admission_gate", "objects", "commit"]));
  });
});
