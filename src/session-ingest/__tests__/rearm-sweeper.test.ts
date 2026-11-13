/**
 * The S3 ledger re-arm sweeper (rearm-sweeper.ts) over the scratch fs object store and the store eraser: markers come
 * from the real ledger (claim -> control -> commit), canonical objects sit where writer.ts puts them, and the erase runs
 * through the shared executor (selfhost-eraser.ts eraseMachineSessionBytes).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { storeEraser, type PagedEraser } from "../../migration/neon-s3-erasure/eraser.ts";
import { LIVE_SKEW_MS } from "../../migration/neon-s3-erasure/job.ts";
import type { ClaimInput, LedgerRecord, LedgerTenant } from "../ledger.ts";
import { s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { parseRearmKey, rearmCorruptKey, rearmCursorKey, rearmDoneKey, rearmScopes, REARM_SKEW_MS, RearmSweepRefused, sweepRearmMarkers, type RearmSweepOptions } from "../rearm-sweeper.ts";
import { exitCodeForError, exitCodeForPass, rearmSweeperConfig, rearmSweeperEnabled, runRearmSweeperLoop, runRearmSweeperPass } from "../rearm-sweeper-main.ts";
import { liveMachineId } from "../writer.ts";
import { TENANT, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const hex = (v: string) => createHash("sha256").update(v).digest("hex");
const machineOf = (t: LedgerTenant) => liveMachineId(t.workspaceId, ORIGIN, t.userId);
const LATER = () => Date.now() + 10 * 60_000; // past the skew window of everything a test wrote

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function fresh() {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}
const ledgerOn = (store: ObjectStore) => s3Ledger(store, ROOT, { sleep: async () => {} });

function rec(recordId: string, revision: number, sessionId = "sess-1"): LedgerRecord {
  return { source: "claude", sessionId, recordId, revisionId: `${recordId}:r${revision}`, revision, digest: hex(`${recordId}:${revision}`) };
}
function claimInput(tenant: LedgerTenant, batchId: string, records: LedgerRecord[]): ClaimInput {
  const sessions = [...new Map(records.map((r) => [r.sessionId, ["claude", r.sessionId!] as [string, string]])).values()];
  return { tenant, originId: ORIGIN, batchId, batchDigest: hex(`batch:${batchId}`), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions, records };
}
const selection = (tenant: LedgerTenant, enabled: boolean, sessionId = "sess-1") => ({ tenant, originId: ORIGIN, sessions: [["claude", sessionId]] as [string, string][], enabled });

/** A commit the disable reached after its claim: the ledger writes the session's re-arm marker. */
async function rearmed(store: ObjectStore, tenant: LedgerTenant, batchId: string, sessionId = "sess-1", reenable = false) {
  const ledger = ledgerOn(store);
  const input = claimInput(tenant, batchId, [rec(`r-${batchId}`, 1, sessionId)]);
  const c = await ledger.claim(input);
  if (c.kind !== "claimed" && c.kind !== "resume") throw new Error(`unexpected ${c.kind}`);
  await ledger.setSessionUploads(selection(tenant, false, sessionId));
  if (reenable) await ledger.setSessionUploads(selection(tenant, true, sessionId));
  const commit = ledger.commit({ tenant, originId: ORIGIN, batchId, batchDigest: input.batchDigest, claimToken: c.claimToken, sessions: input.sessions, records: input.records, machineId: machineOf(tenant) });
  if (reenable) await expect(commit).rejects.toMatchObject({ code: "ledger_attempt_superseded" });
  else expect(await commit).toEqual({ rearmed: 1 });
  return s3LedgerKeys(ROOT, tenant).rearm(ORIGIN, "claude", sessionId);
}

const put = (store: ObjectStore, key: string, body = "x") => store.put(objectKey(key), new TextEncoder().encode(body), { contentType: "application/octet-stream" });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const text = async (store: ObjectStore, key: string) => new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body));
async function allKeys(store: ObjectStore, prefix = ROOT): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do { const p = await store.list(objectKey(prefix), cursor); out.push(...p.items.map((i) => String(i.key))); cursor = p.nextCursor; } while (cursor);
  return out.sort();
}

/** One machine's copy of a session, where writer.ts writeGroup puts it: two dates and the live pointer. */
function canonical(tenant: LedgerTenant, sessionId = "sess-1", machine = machineOf(tenant)) {
  const org = `${ROOT}/${tenant.orgId}`;
  const base = `${org}/v1/ws=${tenant.workspaceId}/m=${machine}/h=claude`;
  return [
    `${base}/d=2026/11/01/s=${sessionId}/manifest.json`, `${base}/d=2026/11/01/s=${sessionId}/raw/c-0001.ndjson.zst`,
    `${base}/d=2026/11/02/s=${sessionId}/manifest.json`, `${org}/live-sessions/v1/ws=${tenant.workspaceId}/m=${machine}/h=claude/s=${sessionId}.json`,
  ];
}
async function seed(store: ObjectStore, keys: string[]) { for (const k of keys) await put(store, k); }

const sweep = (store: ObjectStore, opts: Partial<RearmSweepOptions> = {}, eraser: PagedEraser = storeEraser(store)) =>
  sweepRearmMarkers({ store, eraser }, { keyRoot: ROOT, bucket: "transcripts", now: LATER, ...opts });
const doneOf = async (store: ObjectStore, marker: string) => {
  const digest = hex(await text(store, marker)).slice(0, 32);
  const key = rearmDoneKey(marker, digest);
  return (await exists(store, key)) ? JSON.parse(await text(store, key)) : null;
};

describe("re-arm sweeper", () => {
  test("a marker of a deleted session: that machine's copy erased (every date, pointer), completion recorded, the marker kept", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    const mine = canonical(TENANT);
    const otherSession = canonical(TENANT, "sess-2");
    const otherMachine = canonical(TENANT, "sess-1", `srv_${"a".repeat(32)}`); // another user's origin: same session id
    await seed(store, [...mine, ...otherSession, ...otherMachine]);
    const r = await sweep(store);
    expect(r).toMatchObject({ dryRun: false, complete: true, resume: null, failures: {} });
    expect(r.counts).toMatchObject({ listed: 1, erased: 1, failed: 0, corrupt: 0 });
    expect(r.counts.versions).toBe(mine.length);
    for (const k of mine) expect(await exists(store, k)).toBe(false);
    for (const k of [...otherSession, ...otherMachine]) expect(await exists(store, k)).toBe(true);
    expect(await exists(store, marker)).toBe(true); // the ledger never deletes
    expect(await doneOf(store, marker)).toMatchObject({ v: 1, outcome: "erased" });
  });

  test("a rerun is a no-op: alreadyDone, nothing erased or written", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    expect((await sweep(store)).counts.erased).toBe(1);
    await seed(store, canonical(TENANT, "sess-2"));
    const before = await allKeys(store);
    const r = await sweep(store);
    expect(r.counts).toMatchObject({ listed: 1, alreadyDone: 1, erased: 0, superseded: 0, versions: 0 });
    expect(await allKeys(store)).toEqual(before);
  });

  test("a session re-enabled since: superseded, nothing erased, completion recorded", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await ledgerOn(store).setSessionUploads(selection(TENANT, true));
    await seed(store, canonical(TENANT));
    const r = await sweep(store);
    expect(r.counts).toMatchObject({ superseded: 1, erased: 0, versions: 0 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(true);
    expect(await doneOf(store, marker)).toMatchObject({ outcome: "superseded" });
  });

  test("disable then re-enable before the commit: its marker is superseded", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1", "sess-1", true);
    expect(await exists(store, marker)).toBe(true);
    expect((await sweep(store)).counts).toMatchObject({ superseded: 1, erased: 0 });
  });

  test("a corrupt marker is reported, not fatal: the other markers are still processed", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    const dir = s3LedgerKeys(ROOT, TENANT).dir;
    await put(store, `${dir}/ledger/rearm/o=${"0".repeat(32)}/${"1".repeat(32)}.json`, "{not json");
    // Valid JSON, but at a key ledger-s3 would never write it to (another session's key).
    const real = JSON.parse(await text(store, s3LedgerKeys(ROOT, TENANT).rearm(ORIGIN, "claude", "sess-1")));
    await put(store, s3LedgerKeys(ROOT, TENANT).rearm(ORIGIN, "claude", "sess-9"), JSON.stringify(real));
    const r = await sweep(store);
    expect(r.counts).toMatchObject({ listed: 3, corrupt: 2, erased: 1, failed: 0 });
    expect(r.complete).toBe(true);
  });

  test("a new re-arm after done rewrites the marker (new content) and is processed again", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    expect((await sweep(store)).counts.erased).toBe(1);
    const first = JSON.parse(await text(store, marker));
    // Re-enabled, uploaded again, disabled before that commit: the ledger re-arms the same marker key.
    await ledgerOn(store).setSessionUploads(selection(TENANT, true));
    await rearmed(store, TENANT, "b2");
    const second = JSON.parse(await text(store, marker));
    expect(second.atMs).toBeGreaterThan(first.atMs);
    await seed(store, canonical(TENANT));
    const r = await sweep(store);
    expect(r.counts).toMatchObject({ listed: 1, erased: 1, alreadyDone: 0 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(false);
    expect((await sweep(store)).counts).toMatchObject({ alreadyDone: 1, erased: 0 });
  });

  test("dry run writes nothing and deletes nothing; it classifies and counts", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    await rearmed(store, TENANT, "b2", "sess-2");
    await ledgerOn(store).setSessionUploads(selection(TENANT, true, "sess-2"));
    await seed(store, [...canonical(TENANT), ...canonical(TENANT, "sess-2")]);
    const before = await allKeys(store);
    const r = await sweep(store, { dryRun: true });
    expect(r.dryRun).toBe(true);
    expect(r.counts).toMatchObject({ listed: 2, wouldErase: 1, wouldSupersede: 1, erased: 0, superseded: 0 });
    expect(r.counts.versions).toBe(canonical(TENANT).length);
    expect(await allKeys(store)).toEqual(before);
  });

  test("versions newer than the fence read (less the skew) are kept: pending, no completion, erased on a later pass", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    const r = await sweep(store, { now: Date.now });
    expect(r.counts).toMatchObject({ pending: 1, erased: 0 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(true);
    expect(await doneOf(store, marker)).toBeNull();
    expect((await sweep(store)).counts.erased).toBe(1);
  });

  test("a re-enable during the erase stops it before the next page's deletes: reenabled, no completion", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    const inner = storeEraser(store);
    const racing: PagedEraser = {
      ...inner,
      async eraseStep(prefix, opts) {
        await ledgerOn(store).setSessionUploads(selection(TENANT, true));
        return inner.eraseStep(prefix, opts);
      },
    };
    const r = await sweep(store, {}, racing);
    expect(r.counts).toMatchObject({ reenabled: 1, erased: 0 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(true);
    expect(await doneOf(store, marker)).toBeNull();
    expect((await sweep(store)).counts.superseded).toBe(1); // the next pass sees the re-enable
  });

  test("a reset begun but not finished: resetRunning; an absent or unreadable fence: nothing erased or recorded", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
    const fence = JSON.parse(await text(store, fenceKey));
    // Unfinished reset of a session the fence no longer deletes: its tombstones have not landed yet.
    await ledgerOn(store).setSessionUploads(selection(TENANT, true));
    const reenabled = JSON.parse(await text(store, fenceKey));
    await put(store, fenceKey, JSON.stringify({ ...reenabled, resets: reenabled.resets + 1, resetRequests: { req: null } }));
    expect((await sweep(store)).counts).toMatchObject({ resetRunning: 1, superseded: 0 });
    await put(store, fenceKey, "{broken");
    expect((await sweep(store)).counts).toMatchObject({ fenceUnreadable: 1 });
    await store.delete(objectKey(fenceKey));
    expect((await sweep(store)).counts).toMatchObject({ fenceAbsent: 1 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(true);
    expect(await doneOf(store, marker)).toBeNull();
    expect(fence.deleted[ORIGIN]).toEqual([["claude", "sess-1"]]);
  });

  test("org scope, bounded passes and resume", async () => {
    const store = fresh();
    const other = { ...TENANT, orgId: "org-b" };
    await rearmed(store, TENANT, "b1");
    await rearmed(store, other, "b1");
    await seed(store, [...canonical(TENANT), ...canonical(other)]);
    expect((await sweep(store, { orgs: ["org-c"], dryRun: true })).counts.listed).toBe(0);
    const first = await sweep(store, { orgs: ["org-a", "org-b"], maxMarkers: 1 });
    expect(first).toMatchObject({ complete: false, resume: { scope: 1 } });
    expect(first.counts.erased).toBe(1);
    for (const k of canonical(other)) expect(await exists(store, k)).toBe(true);
    const second = await sweepRearmMarkers({ store, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts", orgs: ["org-a", "org-b"], now: LATER }, first.resume);
    expect(second).toMatchObject({ complete: true, resume: null });
    expect(second.counts).toMatchObject({ listed: 1, erased: 1 });
    for (const k of canonical(other)) expect(await exists(store, k)).toBe(false);
    // Unscoped: every org, both already done.
    expect((await sweep(store)).counts).toMatchObject({ listed: 2, alreadyDone: 2 });
  });

  test("out of budget part way through a page: stops before the next marker, the next pass lists that page again", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    await rearmed(store, TENANT, "b2", "sess-2");
    await seed(store, [...canonical(TENANT), ...canonical(TENANT, "sess-2")]);
    const first = await sweep(store, { maxMarkers: 1, concurrency: 1 });
    expect(first).toMatchObject({ complete: false, resume: { scope: 0 } });
    expect(first.resume!.afterKey).toBeString();
    expect(first.counts).toMatchObject({ listed: 1, erased: 1 });
    const second = await sweepRearmMarkers({ store, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts", maxMarkers: 1, concurrency: 1, now: LATER }, first.resume);
    expect(second).toMatchObject({ complete: true, resume: null });
    // It continues AFTER the last finished key: the first marker is not even read again.
    expect(second.counts).toMatchObject({ listed: 1, alreadyDone: 0, erased: 1 });
    // A deadline already passed: nothing is listed at all.
    expect(await sweep(store, { deadlineAt: 0 })).toMatchObject({ complete: false, counts: { listed: 0 } });
  });

  test("refuses bad options; a marker key parse ignores inbox objects", () => {
    const store = fresh();
    expect(sweep(store, { concurrency: 0 })).rejects.toBeInstanceOf(RearmSweepRefused);
    expect(sweep(store, { orgs: ["../x"] })).rejects.toBeInstanceOf(RearmSweepRefused);
    const dir = s3LedgerKeys(ROOT, TENANT).dir;
    expect(parseRearmKey(ROOT, `${dir}/ledger/rearm/o=${"0".repeat(32)}/${"1".repeat(32)}.json`)).toEqual({ dir, orgId: TENANT.orgId, workspaceSegment: TENANT.workspaceId });
    expect(parseRearmKey(ROOT, `${dir}/ledger/rearm/o=${"0".repeat(32)}/${"1".repeat(32)}.done/${"2".repeat(32)}.json`)).toBeNull();
    expect(parseRearmKey(ROOT, `${dir}/privacy-journal.json`)).toBeNull();
  });

  test("a commit that finds a corrupt marker replaces it with a valid one (never wedged), which is then erased", async () => {
    const store = fresh();
    const marker = s3LedgerKeys(ROOT, TENANT).rearm(ORIGIN, "claude", "sess-1");
    await put(store, marker, "{corrupt");
    expect((await sweep(store)).counts).toMatchObject({ corrupt: 1 });
    await rearmed(store, TENANT, "b1");
    expect(JSON.parse(await text(store, marker))).toMatchObject({ v: 1, source: "claude", sessionId: "sess-1", machineId: machineOf(TENANT) });
    await seed(store, canonical(TENANT));
    expect((await sweep(store)).counts).toMatchObject({ erased: 1, corrupt: 0 });
  });

  test("the Lambda handler is off by default and refuses static keys", async () => {
    const { handler } = await import("../rearm-sweeper-lambda.ts");
    const saved = { flag: process.env.INGEST_REARM_SWEEPER_ENABLED, creds: process.env.INGEST_REARM_CREDENTIALS };
    try {
      delete process.env.INGEST_REARM_SWEEPER_ENABLED;
      expect(await handler()).toEqual({ enabled: false });
      process.env.INGEST_REARM_SWEEPER_ENABLED = "true";
      process.env.INGEST_REARM_CREDENTIALS = "keys";
      await expect(handler()).rejects.toThrow("rearm_sweep_credentials_refused");
    } finally {
      if (saved.flag === undefined) delete process.env.INGEST_REARM_SWEEPER_ENABLED; else process.env.INGEST_REARM_SWEEPER_ENABLED = saved.flag;
      if (saved.creds === undefined) delete process.env.INGEST_REARM_CREDENTIALS; else process.env.INGEST_REARM_CREDENTIALS = saved.creds;
    }
  });

  test("the skew equals the hosted live lane's", () => {
    expect(REARM_SKEW_MS).toBe(LIVE_SKEW_MS);
  });
});

describe("re-arm sweeper: review fixes", () => {
  const ownerKey = (sessionId = "sess-1", tenant = TENANT) => s3LedgerKeys(ROOT, tenant).owner(ORIGIN, "claude", sessionId);

  test("stuck markers that cannot progress cost no budget: the real one after them is erased in the same pass", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    const dir = s3LedgerKeys(ROOT, TENANT).dir;
    for (let i = 0; i < 5; i++) await put(store, `${dir}/ledger/rearm/o=${"0".repeat(32)}/${String(i).repeat(32)}.json`, "{stuck");
    const r = await sweep(store, { maxMarkers: 1, concurrency: 1 });
    expect(r.counts).toMatchObject({ corrupt: 5, erased: 1 });
    expect(r.complete).toBe(true);
  });

  test("a stale saved list cursor: the scope is listed again from its start, still skipping what was finished", async () => {
    const store = fresh();
    const m1 = await rearmed(store, TENANT, "b1");
    const m2 = await rearmed(store, TENANT, "b2", "sess-2");
    const [firstKey] = [m1, m2].sort();
    const failing = new Proxy(store, { get(t, k) {
      if (k === "list") return async (p: unknown, cursor?: string) => { if (cursor === "stale") throw new ObjectStoreError("backend", "s3", "LIST", { status: 400 }); return t.list(p as never, cursor); };
      const v = (t as any)[k]; return typeof v === "function" ? v.bind(t) : v;
    } }) as ObjectStore;
    const r = await sweepRearmMarkers({ store: failing, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts", now: LATER }, { scope: 0, cursor: "stale", afterKey: firstKey });
    expect(r).toMatchObject({ complete: true, counts: { listed: 1, erased: 1 } });
  });

  test("a corrupt marker is recorded next to itself (findable without a key in any log); a dry run records nothing", async () => {
    const store = fresh();
    const dir = s3LedgerKeys(ROOT, TENANT).dir;
    const key = `${dir}/ledger/rearm/o=${"0".repeat(32)}/${"1".repeat(32)}.json`;
    await put(store, key, "{broken");
    const record = rearmCorruptKey(key, hex("{broken").slice(0, 32));
    expect((await sweep(store, { dryRun: true })).counts.corrupt).toBe(1);
    expect(await exists(store, record)).toBe(false);
    expect((await sweep(store)).counts.corrupt).toBe(1);
    expect(JSON.parse(await text(store, record))).toMatchObject({ v: 1, reason: "unparseable" });
  });

  test("access denied (a 403, e.g. ListBucket conditioned on a prefix makes a missing HEAD 403) aborts the pass with a coded refusal", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    const denying = new Proxy(store, { get(t, k) {
      if (k === "head") return async () => { throw new ObjectStoreError("unauthorized", "s3", "HEAD", { status: 403 }); };
      const v = (t as any)[k]; return typeof v === "function" ? v.bind(t) : v;
    } }) as ObjectStore;
    const err = await sweepRearmMarkers({ store: denying, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts", now: LATER }).catch((e) => e);
    expect(err).toBeInstanceOf(RearmSweepRefused);
    expect(err.code).toBe("access_denied");
    expect(exitCodeForError(err)).toBe(2);
    const listDenied = new Proxy(store, { get(t, k) {
      if (k === "list") return async () => { throw new ObjectStoreError("unauthorized", "s3", "LIST", { status: 403 }); };
      const v = (t as any)[k]; return typeof v === "function" ? v.bind(t) : v;
    } }) as ObjectStore;
    expect(sweepRearmMarkers({ store: listDenied, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts" })).rejects.toMatchObject({ code: "access_denied" });
  });

  test("the marker's machine is bound to its user's ownership stub: mismatch, unbound and missing stubs erase nothing", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    expect(JSON.parse(await text(store, ownerKey()))).toMatchObject({ machineId: machineOf(TENANT) });
    const good = await text(store, marker);
    const stub = await text(store, ownerKey());
    // A marker naming another machine (another user's prefix): refused.
    await put(store, marker, JSON.stringify({ ...JSON.parse(good), machineId: `srv_${"b".repeat(32)}` }));
    expect((await sweep(store)).counts).toMatchObject({ machineMismatch: 1, erased: 0 });
    await put(store, marker, good);
    const { machineId: _, ...unbound } = JSON.parse(stub);
    await put(store, ownerKey(), JSON.stringify(unbound));
    expect((await sweep(store)).counts).toMatchObject({ machineUnbound: 1, erased: 0 });
    await store.delete(objectKey(ownerKey()));
    expect((await sweep(store)).counts).toMatchObject({ ownerMissing: 1, erased: 0 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(true);
    await put(store, ownerKey(), stub);
    expect((await sweep(store)).counts).toMatchObject({ erased: 1 });
  });

  test("the ledger refuses a commit whose machine is not the tenant's, and binds an older stub's machine on the next claim", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput(TENANT, "c1", [rec("r-c1", 1, "sess-5")]);
    const c = await ledger.claim(input);
    if (c.kind !== "claimed") throw new Error("unexpected");
    await expect(ledger.commit({ tenant: TENANT, originId: ORIGIN, batchId: "c1", batchDigest: input.batchDigest, claimToken: c.claimToken, sessions: input.sessions,
      records: input.records, machineId: `srv_${"c".repeat(32)}` })).rejects.toMatchObject({ code: "ledger_input_refused" });
    const { machineId: _, ...legacy } = JSON.parse(await text(store, ownerKey("sess-5")));
    await put(store, ownerKey("sess-5"), JSON.stringify(legacy));
    await ledger.claim(claimInput(TENANT, "c2", [rec("r-c2", 1, "sess-5")]));
    expect(JSON.parse(await text(store, ownerKey("sess-5")))).toMatchObject({ machineId: machineOf(TENANT), epoch: legacy.epoch, resets: legacy.resets });
  });

  test("a layout collision (a:b and a_b share s=a_b) with a live session erases nothing; once both are deleted it erases", async () => {
    const store = fresh();
    await ledgerOn(store).claim(claimInput(TENANT, "bx", [rec("r-bx", 1, "a_b")])); // a live session of the same origin
    await rearmed(store, TENANT, "b1", "a:b");
    await seed(store, canonical(TENANT, "a_b")); // the one physical dir both map to
    expect((await sweep(store)).counts).toMatchObject({ layoutCollision: 1, erased: 0 });
    for (const k of canonical(TENANT, "a_b")) expect(await exists(store, k)).toBe(true);
    await ledgerOn(store).setSessionUploads(selection(TENANT, false, "a_b"));
    expect((await sweep(store)).counts).toMatchObject({ erased: 1, layoutCollision: 0 });
    for (const k of canonical(TENANT, "a_b")) expect(await exists(store, k)).toBe(false);
  });
});

describe("re-arm sweeper: follow-ups (unstamped stubs, progress, bounded scans)", () => {
  const ownerKey = (sessionId = "sess-1", tenant = TENANT) => s3LedgerKeys(ROOT, tenant).owner(ORIGIN, "claude", sessionId);
  const unstamp = async (store: ObjectStore, sessionId = "sess-1") => {
    const { machineId: _, ...legacy } = JSON.parse(await text(store, ownerKey(sessionId)));
    await put(store, ownerKey(sessionId), JSON.stringify(legacy));
  };
  /** A canonical manifest as writer.ts writes it: the uploader and session in extensions.96bulls. */
  const manifestOf = (userId: string, sessionId = "sess-1") => JSON.stringify({ extensions: { "96bulls": { user_id: userId, canonical_session_id: sessionId } } });

  test("the commit stamps the ownership stub's machine before the marker (an old-code stub is bound by the fenced commit)", async () => {
    const store = fresh();
    const ledger = ledgerOn(store);
    const input = claimInput(TENANT, "b1", [rec("r-b1", 1)]);
    const c = await ledger.claim(input);
    if (c.kind !== "claimed") throw new Error("unexpected");
    await unstamp(store); // as written by code from before the binding (a rolling deploy)
    await ledger.setSessionUploads(selection(TENANT, false));
    expect(await ledger.commit({ tenant: TENANT, originId: ORIGIN, batchId: "b1", batchDigest: input.batchDigest, claimToken: c.claimToken, sessions: input.sessions,
      records: input.records, machineId: machineOf(TENANT) })).toEqual({ rearmed: 1 });
    expect(JSON.parse(await text(store, ownerKey()))).toMatchObject({ machineId: machineOf(TENANT) });
    await seed(store, canonical(TENANT));
    expect((await sweep(store)).counts).toMatchObject({ erased: 1, machineUnbound: 0, machineDerived: 0 });
  });

  test("an existing unstamped stub: the machine is derived from the session's manifest and verified, never trusted", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await unstamp(store);
    const [manifest, ...rest] = canonical(TENANT);
    await put(store, manifest!, manifestOf("someone-else")); // a user whose admission dir and machine are not these
    await seed(store, rest);
    expect((await sweep(store)).counts).toMatchObject({ machineUnbound: 1, erased: 0 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(true);
    await put(store, manifest!, manifestOf(TENANT.userId));
    expect((await sweep(store)).counts).toMatchObject({ machineDerived: 1, erased: 1 });
    for (const k of canonical(TENANT)) expect(await exists(store, k)).toBe(false);
    expect(await doneOf(store, marker)).toMatchObject({ outcome: "erased" });
  });

  test("an unstamped stub with nothing under the marker's machine: completed as empty, nothing erased", async () => {
    const store = fresh();
    const marker = await rearmed(store, TENANT, "b1");
    await unstamp(store);
    await seed(store, canonical(TENANT, "sess-2")); // another session's bytes stay
    expect((await sweep(store, { dryRun: true })).counts).toMatchObject({ emptyUnbound: 1 });
    expect(await doneOf(store, marker)).toBeNull();
    expect((await sweep(store)).counts).toMatchObject({ emptyUnbound: 1, erased: 0 });
    expect(await doneOf(store, marker)).toMatchObject({ outcome: "empty" });
    for (const k of canonical(TENANT, "sess-2")) expect(await exists(store, k)).toBe(true);
  });

  test("a pass that dies mid-way resumes right after the last finished marker (saved per marker)", async () => {
    const store = fresh();
    const markers = [await rearmed(store, TENANT, "b1", "s-a"), await rearmed(store, TENANT, "b2", "s-b"), await rearmed(store, TENANT, "b3", "s-c")].sort();
    for (const sid of ["s-a", "s-b", "s-c"]) await seed(store, canonical(TENANT, sid));
    // The third marker (in key order) kills the pass: a 403 on its completion-record HEAD.
    const dying = `${markers[2]!.slice(0, -".json".length)}.done/`;
    const killing = new Proxy(store, { get(t, k) {
      if (k === "head") return async (key: unknown) => { if (String(key).startsWith(dying)) throw new ObjectStoreError("unauthorized", "s3", "HEAD", { status: 403 }); return t.head(key as never); };
      const v = (t as any)[k]; return typeof v === "function" ? v.bind(t) : v;
    } }) as ObjectStore;
    const env = { ...ENTRY_ENV, INGEST_REARM_CONCURRENCY: "1" };
    await expect(runRearmSweeperPass(env, [], { store: () => killing, eraser: () => storeEraser(store), now: LATER })).rejects.toMatchObject({ code: "access_denied" });
    const saved = JSON.parse(await text(store, rearmCursorKey(ROOT, rearmScopes(ROOT, null))));
    expect(saved.resume.afterKey).toBe(markers[1]);
    const { output } = await runRearmSweeperPass(env, [], { store: () => store, eraser: () => storeEraser(store), now: LATER });
    expect(output).toMatchObject({ complete: true, counts: { listed: 1, erased: 1, alreadyDone: 0 } });
  });

  test("the collision scan is bounded: over the limit it refuses, out of time it defers the marker to the next pass", async () => {
    const store = fresh();
    await ledgerOn(store).claim(claimInput(TENANT, "bx", [rec("r-bx", 1, "sess-2")])); // a second stub in the origin
    await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    expect((await sweep(store, { ownerScanLimit: 1 })).counts).toMatchObject({ layoutScanTooLarge: 1, erased: 0 });
    // The clock jumps past the deadline as soon as the owners are listed: the marker is deferred, not finished.
    let late = false;
    const slow = new Proxy(store, { get(t, k) {
      if (k === "list") return async (p: unknown, c?: string) => { if (String(p).includes("/ledger/owners/")) late = true; return t.list(p as never, c); };
      const v = (t as any)[k]; return typeof v === "function" ? v.bind(t) : v;
    } }) as ObjectStore;
    const deadlineAt = Date.now() + 60 * 60_000;
    const r = await sweepRearmMarkers({ store: slow, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts", deadlineAt, now: () => (late ? deadlineAt + 1 : LATER()) });
    expect(r).toMatchObject({ complete: false, counts: { deferred: 1, erased: 0 } });
    expect(r.resume!.afterKey).toBeUndefined(); // the deferred marker is NOT behind the saved position
    const again = await sweepRearmMarkers({ store, eraser: storeEraser(store) }, { keyRoot: ROOT, bucket: "transcripts", now: LATER }, r.resume);
    expect(again.counts).toMatchObject({ listed: 1, erased: 1 });
  });
});

const ENTRY_ENV = { INGEST_REARM_SWEEPER_ENABLED: "true", INGEST_REARM_BUCKET: "transcripts", INGEST_REARM_REGION: "us-east-1", INGEST_REARM_CREDENTIALS: "keys",
  INGEST_REARM_S3_ACCESS_KEY_ID: "test-access-key-id", INGEST_REARM_S3_SECRET_ACCESS_KEY: "test-only-secret" };

describe("re-arm sweeper entry", () => {
  const ENV = { INGEST_REARM_SWEEPER_ENABLED: "true", INGEST_REARM_BUCKET: "transcripts", INGEST_REARM_REGION: "us-east-1", INGEST_REARM_CREDENTIALS: "keys",
    INGEST_REARM_S3_ACCESS_KEY_ID: "test-access-key-id", INGEST_REARM_S3_SECRET_ACCESS_KEY: "test-only-secret" };

  test("default off: nothing else is read", async () => {
    expect(rearmSweeperEnabled({})).toBe(false);
    expect(rearmSweeperEnabled({ INGEST_REARM_SWEEPER_ENABLED: "false" })).toBe(false);
    expect(() => rearmSweeperEnabled({ INGEST_REARM_SWEEPER_ENABLED: "1" })).toThrow(RearmSweepRefused);
    expect((await runRearmSweeperPass({ INGEST_REARM_BUCKET: "!!" }, [])).output).toEqual({ enabled: false });
  });

  test("config: bounds, flags and argv", () => {
    expect(rearmSweeperConfig(ENV, ["--dry-run", "--interval-ms", "30000"])).toMatchObject({ dryRun: true, intervalMs: 30_000, concurrency: 4, maxMarkers: 500, keyRoot: "96bulls", credentials: "keys" });
    expect(rearmSweeperConfig({ ...ENV, INGEST_REARM_ORGS: "org-a, org-b" }).orgs).toEqual(["org-a", "org-b"]);
    for (const bad of [{ INGEST_REARM_CONCURRENCY: "17" }, { INGEST_REARM_INTERVAL_MS: "5" }, { INGEST_REARM_ORGS: "../x" }, { INGEST_REARM_CREDENTIALS: "env" }, { INGEST_REARM_DRY_RUN: "yes" }, { INGEST_REARM_BUCKET: "" }])
      expect(() => rearmSweeperConfig({ ...ENV, ...bad })).toThrow(RearmSweepRefused);
  });

  test("N stuck markers that DO spend budget ahead of a real one: the durable cursor gets the real one erased within bounded passes, then wraps", async () => {
    const store = fresh();
    const stuck = Array.from({ length: 5 }, (_, i) => ({ ...TENANT, orgId: `org-stuck-${i}` }));
    for (const t of stuck) { await rearmed(store, t, "b1"); await seed(store, canonical(t)); }
    const real = { ...TENANT, orgId: "org-z" }; // lists after every stuck org
    await rearmed(store, real, "b1");
    await seed(store, canonical(real));
    // The stuck orgs' erases never verify (pending forever): each spends budget on every pass.
    const inner = storeEraser(store);
    const eraser: PagedEraser = { ...inner, verifyErased: (prefix, o) => prefix.includes("/org-stuck-")
      ? Promise.resolve({ status: "pending", reason: "remaining", remaining: 1, listed: 1 }) : inner.verifyErased(prefix, o) };
    const env = { ...ENV, INGEST_REARM_MAX_MARKERS: "2", INGEST_REARM_CONCURRENCY: "1" };
    const overrides = { store: () => store, eraser: () => eraser, now: LATER };
    let passes = 0, erased = 0;
    while (erased === 0 && passes < 4) {
      const { output } = await runRearmSweeperPass(env, [], overrides); // a fresh invocation each time: no in-memory state
      passes++;
      if (!output.enabled) throw new Error("disabled");
      expect(output.cursor).toBe("saved");
      erased = output.counts.erased;
    }
    expect(erased).toBe(1);
    expect(passes).toBe(3); // 2 + 2 + (1 + the real one)
    for (const k of canonical(real)) expect(await exists(store, k)).toBe(false);
    const cursorKey = rearmCursorKey(ROOT, rearmScopes(ROOT, null));
    expect(JSON.parse(await text(store, cursorKey))).toMatchObject({ v: 1, resume: null }); // complete: the next pass starts over
    const { output } = await runRearmSweeperPass(env, [], overrides);
    expect(output).toMatchObject({ complete: false, counts: { pending: 2, alreadyDone: 0 } }); // wrapped to the start
    // A dry run never reads or writes the cursor.
    const before = await text(store, cursorKey);
    await runRearmSweeperPass({ ...env, INGEST_REARM_DRY_RUN: "1" }, [], overrides);
    expect(await text(store, cursorKey)).toBe(before);
  });

  test("a pass through the entry: counts only, never a key or tenant id; the loop runs passes until stopped", async () => {
    const store = fresh();
    await rearmed(store, TENANT, "b1");
    await seed(store, canonical(TENANT));
    const overrides = { store: () => store, eraser: () => storeEraser(store), now: LATER };
    const { output } = await runRearmSweeperPass(ENV, [], overrides);
    expect(output).toMatchObject({ enabled: true, complete: true, counts: { erased: 1 } });
    expect(exitCodeForPass(output)).toBe(0);
    const line = JSON.stringify(output);
    for (const id of [TENANT.orgId, TENANT.workspaceId, TENANT.userId, "sess-1", ORIGIN, ROOT]) expect(line).not.toContain(id);
    const lines: unknown[] = [];
    let passes = 0;
    await runRearmSweeperLoop(ENV, [], (l) => lines.push(l), () => passes >= 2, async () => { passes++; }, overrides);
    expect(lines).toHaveLength(2);
    expect(lines[1]).toMatchObject({ event: "rearm_sweep", counts: { alreadyDone: 1 } });
  });
});
