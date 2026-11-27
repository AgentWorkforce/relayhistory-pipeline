/**
 * The re-arm sweeper's per-org pass receipts (rearm-sweeper-receipt.ts): only a real, full, clean pass writes one; the
 * reader refuses anything missing, malformed, stale or from the future.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { storeEraser, type PagedEraser } from "../../migration/neon-s3-erasure/eraser.ts";
import type { ClaimInput, LedgerRecord, LedgerTenant } from "../ledger.ts";
import { s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { rearmScopeHash, rearmScopes, type RearmSweepCounts } from "../rearm-sweeper.ts";
import { exitCodeForError, exitCodeForPass, rearmFailure, runRearmSweeperLoop, runRearmSweeperPass } from "../rearm-sweeper-main.ts";
import { ObjectStoreError } from "../../shared/vendor.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import {
  freshSweeperPass, rearmBucketHash, REARM_ALL_ORGS, REARM_RECEIPT_FUTURE_SKEW_MS, rearmPassLatestKey, rearmPassReceiptEligibility, rearmPassReceiptKey,
  rearmPassesPrefix, rearmPassRevision, rearmPassWorkload, validRearmPassReceipt, writeRearmPassReceipt, type RearmPassReceipt,
} from "../rearm-sweeper-receipt.ts";
import { liveMachineId } from "../writer.ts";
import { ledgerRoutedStore } from "../ledger-store.ts";
import { TENANT, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const hex = (v: string) => createHash("sha256").update(v).digest("hex");
const machineOf = (t: LedgerTenant) => liveMachineId(t.workspaceId, ORIGIN, t.userId);
const T0 = Date.now() + 10 * 60_000; // past the skew window of everything a test wrote

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function fresh() {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}

function rec(recordId: string, revision: number, sessionId = "sess-1"): LedgerRecord {
  return { source: "claude", sessionId, recordId, revisionId: `${recordId}:r${revision}`, revision, digest: hex(`${recordId}:${revision}`) };
}
function claimInput(tenant: LedgerTenant, batchId: string, records: LedgerRecord[]): ClaimInput {
  const sessions = [...new Map(records.map((r) => [r.sessionId, ["claude", r.sessionId!] as [string, string]])).values()];
  return { tenant, originId: ORIGIN, batchId, batchDigest: hex(`batch:${batchId}`), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions, records };
}
/** A commit the disable reached after its claim: the ledger writes the session's re-arm marker. */
async function rearmed(store: ObjectStore, tenant: LedgerTenant, batchId: string) {
  const ledger = s3Ledger(store, ROOT, { sleep: async () => {} });
  const input = claimInput(tenant, batchId, [rec(`r-${batchId}`, 1)]);
  const c = await ledger.claim(input);
  if (c.kind !== "claimed" && c.kind !== "resume") throw new Error(`unexpected ${c.kind}`);
  await ledger.setSessionUploads({ tenant, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
  expect(await ledger.commit({ tenant, originId: ORIGIN, batchId, batchDigest: input.batchDigest, claimToken: c.claimToken, sessions: input.sessions, records: input.records, machineId: machineOf(tenant) }))
    .toEqual({ rearmed: 1 });
  return s3LedgerKeys(ROOT, tenant).rearm(ORIGIN, "claude", "sess-1");
}
async function allKeys(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do { const p = await store.list(objectKey(prefix), cursor); out.push(...p.items.map((i) => String(i.key))); cursor = p.nextCursor; } while (cursor);
  return out.sort();
}
const text = async (store: ObjectStore, key: string) => new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body));
const putJson = (store: ObjectStore, key: string, body: unknown) =>
  store.put(objectKey(key), new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)), { contentType: "application/json" });

const ENV = { INGEST_REARM_SWEEPER_ENABLED: "true", INGEST_REARM_BUCKET: "transcripts", INGEST_REARM_REGION: "us-east-1", INGEST_REARM_CREDENTIALS: "keys",
  INGEST_REARM_S3_ACCESS_KEY_ID: "test-access-key-id", INGEST_REARM_S3_SECRET_ACCESS_KEY: "test-only-secret" } as NodeJS.ProcessEnv;
const ORG_A = "org-a", ORG_B = "org-b";
const SCOPED = { ...ENV, INGEST_REARM_ORGS: `${ORG_B},${ORG_A}` };
const clock = (start = T0) => { let t = start; return () => (t += 1000); };
const overridesFor = (store: ObjectStore, eraser: PagedEraser = storeEraser(store), now = clock()) => ({ store: () => store, eraser: () => eraser, now });
const passes = (store: ObjectStore) => allKeys(store, rearmPassesPrefix(ROOT));
const HOUR = 60 * 60_000;
/** The ledger-backed tests: the first claim in a file warms up slowly. */
const SLOW = 30_000;

const zeroCounts = (): RearmSweepCounts => ({ listed: 0, erased: 0, superseded: 0, alreadyDone: 0, pending: 0, resetRunning: 0, reenabled: 0, fenceAbsent: 0,
  fenceUnreadable: 0, corrupt: 0, ownerMissing: 0, machineMismatch: 0, machineUnbound: 0, layoutCollision: 0, layoutScanTooLarge: 0, machineDerived: 0,
  emptyUnbound: 0, deferred: 0, gone: 0, failed: 0, wouldErase: 0, wouldSupersede: 0, versions: 0 });
const receiptFor = (org: string, startedAtMs: number, over: Partial<RearmPassReceipt> = {}): RearmPassReceipt => ({
  v: 1, kind: "rearm-sweeper-pass", org, enabled: true, dryRun: false, complete: true, fromStart: true, failures: 0, startedAtMs, finishedAtMs: startedAtMs + 5000,
  scopeHash: rearmScopeHash(rearmScopes(ROOT, [org])), scopes: 1, keyRoot: ROOT, bucketHash: rearmBucketHash("transcripts"), revision: {}, workload: {},
  counts: zeroCounts(), ...over,
});

describe("rearm sweeper pass receipts: the writer", () => {
  test("a disabled run writes nothing (and reads no setting)", async () => {
    const store = fresh();
    for (const env of [{}, { ...SCOPED, INGEST_REARM_SWEEPER_ENABLED: "false" }, { ...SCOPED, INGEST_REARM_SWEEPER_ENABLED: "" }]) {
      const { output } = await runRearmSweeperPass(env as NodeJS.ProcessEnv, [], overridesFor(store));
      expect(output).toEqual({ enabled: false });
    }
    expect(await allKeys(store, ROOT)).toEqual([]);
    expect(await freshSweeperPass(store, ROOT, ORG_A, T0, HOUR)).toEqual({ ok: false, reason: "missing" });
  });

  test("a dry run writes nothing, by env or by flag", async () => {
    const store = fresh();
    await rearmed(store, { ...TENANT, orgId: ORG_A }, "b1");
    const before = await allKeys(store, ROOT);
    for (const [env, argv] of [[{ ...SCOPED, INGEST_REARM_DRY_RUN: "1" }, []], [SCOPED, ["--dry-run"]]] as const) {
      const { output } = await runRearmSweeperPass(env, argv, overridesFor(store));
      expect(output).toMatchObject({ enabled: true, dryRun: true, complete: true, receipt: "skipped", receiptSkip: "dry_run" });
    }
    expect(await allKeys(store, ROOT)).toEqual(before);
    expect(await passes(store)).toEqual([]);
  }, SLOW);

  test("a full clean pass writes one receipt per org and moves each latest pointer; the reader accepts it", async () => {
    const store = fresh();
    await rearmed(store, { ...TENANT, orgId: ORG_A }, "b1");
    const env = { ...SCOPED, GITHUB_SHA: "a".repeat(40), INGEST_REARM_IMAGE_DIGEST: `sha256:${"b".repeat(64)}`, INGEST_REARM_CRONJOB_NAME: "rearm-sweeper",
      INGEST_REARM_JOB_NAME: "rearm-sweeper-29000000", INGEST_REARM_JOB_UID: "0f0e0d0c-0b0a-4908-8706-050403020100", INGEST_REARM_POD_UID: "not-a-uid" };
    const { output } = await runRearmSweeperPass(env, [], overridesFor(store));
    expect(output).toMatchObject({ enabled: true, complete: true, counts: { erased: 1, failed: 0 }, receipt: "written" });
    expect(exitCodeForPass(output)).toBe(0);
    const keys = await passes(store);
    expect(keys).toHaveLength(4);
    for (const org of [ORG_A, ORG_B]) expect(keys).toContain(rearmPassLatestKey(ROOT, org));
    const got = await freshSweeperPass(store, ROOT, ORG_A, T0 + 60_000, HOUR);
    if (!got.ok) throw new Error(got.reason);
    expect(got.scope).toBe("org");
    expect(got.receipt).toMatchObject({ org: ORG_A, keyRoot: ROOT, scopes: 2, scopeHash: rearmScopeHash(rearmScopes(ROOT, [ORG_A, ORG_B])),
      bucketHash: rearmBucketHash("transcripts"), revision: { gitSha: "a".repeat(40), imageDigest: `sha256:${"b".repeat(64)}` },
      workload: { cronJobName: "rearm-sweeper", jobName: "rearm-sweeper-29000000", jobUid: "0f0e0d0c-0b0a-4908-8706-050403020100" }, counts: { erased: 1 } });
    expect(got.receipt.workload).not.toHaveProperty("podUid"); // malformed provenance is dropped, never fatal
    expect(got.receipt.finishedAtMs).toBeGreaterThanOrEqual(got.receipt.startedAtMs);
    expect((await freshSweeperPass(store, ROOT, ORG_B, T0 + 60_000, HOUR)).ok).toBe(true);
    expect(await freshSweeperPass(store, ROOT, "org-c", T0 + 60_000, HOUR)).toEqual({ ok: false, reason: "missing" });
    // Counts and codes only: the line and the receipt carry no id, key or bucket name.
    const line = JSON.stringify(output);
    for (const id of [ORG_A, ORG_B, TENANT.workspaceId, TENANT.userId, "sess-1", ORIGIN, ROOT, "transcripts"]) expect(line).not.toContain(id);
    const body = await text(store, rearmPassReceiptKey(ROOT, ORG_A, got.receipt.startedAtMs));
    for (const id of [TENANT.workspaceId, TENANT.userId, "sess-1", ORIGIN, "transcripts"]) expect(body).not.toContain(id);
  }, SLOW);

  test("an every-org sweeper writes the `.all` receipt, which covers any org", async () => {
    const store = fresh();
    const { output } = await runRearmSweeperPass(ENV, [], overridesFor(store));
    expect(output).toMatchObject({ receipt: "written" });
    expect(await passes(store)).toContain(rearmPassLatestKey(ROOT, REARM_ALL_ORGS));
    expect(await freshSweeperPass(store, ROOT, "any-org", T0 + 60_000, HOUR)).toMatchObject({ ok: true, scope: "all" });
  });

  test("a pass with a failed marker writes nothing (for any org) and exits 3", async () => {
    const store = fresh();
    await rearmed(store, { ...TENANT, orgId: ORG_A }, "b1");
    const inner = storeEraser(store);
    const boom = () => Promise.reject(Object.assign(new Error("boom"), { code: "s3_500" }));
    const eraser = Object.fromEntries(Object.entries(inner).map(([k, v]) => [k, typeof v === "function" ? boom : v])) as unknown as PagedEraser;
    const { output } = await runRearmSweeperPass(SCOPED, [], overridesFor(store, eraser));
    expect(output).toMatchObject({ enabled: true, receipt: "skipped", receiptSkip: "failed" });
    if (!output.enabled) throw new Error("disabled");
    expect(output.counts.failed).toBeGreaterThan(0);
    expect(exitCodeForPass(output)).toBe(3);
    expect(await passes(store)).toEqual([]);
  }, SLOW);

  test("an incomplete pass writes nothing; the pass that resumes it writes nothing; the next full pass writes", async () => {
    const store = fresh();
    for (const org of [ORG_A, ORG_B]) await rearmed(store, { ...TENANT, orgId: org }, "b1");
    const env = { ...SCOPED, INGEST_REARM_MAX_MARKERS: "1", INGEST_REARM_CONCURRENCY: "1" };
    const now = clock();
    const first = await runRearmSweeperPass(env, [], overridesFor(store, storeEraser(store), now));
    expect(first.output).toMatchObject({ complete: false, receipt: "skipped", receiptSkip: "incomplete" });
    expect(await passes(store)).toEqual([]);
    const second = await runRearmSweeperPass(env, [], overridesFor(store, storeEraser(store), now));
    expect(second.output).toMatchObject({ complete: true, receipt: "skipped", receiptSkip: "resumed" });
    expect(await passes(store)).toEqual([]);
    const third = await runRearmSweeperPass(env, [], overridesFor(store, storeEraser(store), now));
    expect(third.output).toMatchObject({ complete: true, receipt: "written", counts: { alreadyDone: 2 } });
    expect((await freshSweeperPass(store, ROOT, ORG_A, now(), HOUR)).ok).toBe(true);
  }, SLOW);

  test("eligibility: every gate, codes only", () => {
    const ok = { dryRun: false, fromStart: true, cursor: "saved" as const, complete: true, counts: zeroCounts(), failures: {} };
    expect(rearmPassReceiptEligibility(ok)).toBeNull();
    expect(rearmPassReceiptEligibility({ ...ok, dryRun: true })).toBe("dry_run");
    expect(rearmPassReceiptEligibility({ ...ok, cursor: "memory" })).toBe("dry_run");
    expect(rearmPassReceiptEligibility({ ...ok, fromStart: false })).toBe("resumed");
    expect(rearmPassReceiptEligibility({ ...ok, complete: false })).toBe("incomplete");
    expect(rearmPassReceiptEligibility({ ...ok, counts: { ...zeroCounts(), failed: 1 } })).toBe("failed");
    expect(rearmPassReceiptEligibility({ ...ok, failures: { s3_500: 1 } })).toBe("failed");
    expect(rearmPassReceiptEligibility({ ...ok, cursor: "contended" })).toBe("contended");
  });

  test("receipts are create-only and the latest pointer only moves forward", async () => {
    const store = fresh();
    expect(await writeRearmPassReceipt(store, receiptFor(ORG_A, T0))).toBe("written");
    expect(await writeRearmPassReceipt(store, receiptFor(ORG_A, T0, { finishedAtMs: T0 + 9000 }))).toBe("contended"); // same key: never overwritten
    expect(JSON.parse(await text(store, rearmPassReceiptKey(ROOT, ORG_A, T0))).finishedAtMs).toBe(T0 + 5000);
    expect(await writeRearmPassReceipt(store, receiptFor(ORG_A, T0 + HOUR))).toBe("written");
    expect(await writeRearmPassReceipt(store, receiptFor(ORG_A, T0 + 1000))).toBe("written"); // older: receipt kept, pointer stays
    expect(JSON.parse(await text(store, rearmPassLatestKey(ROOT, ORG_A))).startedAtMs).toBe(T0 + HOUR);
    await expect(writeRearmPassReceipt(store, receiptFor(ORG_A, T0, { dryRun: true } as never))).rejects.toMatchObject({ code: "receipt_invalid" });
    await expect(writeRearmPassReceipt(store, receiptFor("../x", T0))).rejects.toMatchObject({ code: "receipt_invalid" });
  });

  test("through a separate ledger bucket the receipts stay beside the cursor (no new grant)", async () => {
    const main = fresh(), ledger = fresh();
    const routed = ledgerRoutedStore(main, ledger, ROOT);
    expect(await writeRearmPassReceipt(routed, receiptFor(ORG_A, T0))).toBe("written");
    expect(await allKeys(ledger, ROOT)).toEqual([]);
    expect(await allKeys(main, rearmPassesPrefix(ROOT))).toHaveLength(2);
  });
});

describe("rearm sweeper pass receipts: freshSweeperPass", () => {
  test("staleness: fresh within maxAgeMs, stale after; future beyond the skew is refused", async () => {
    const store = fresh();
    await writeRearmPassReceipt(store, receiptFor(ORG_A, T0));
    const fin = T0 + 5000;
    expect(await freshSweeperPass(store, ROOT, ORG_A, fin + HOUR, HOUR)).toMatchObject({ ok: true, ageMs: HOUR });
    expect(await freshSweeperPass(store, ROOT, ORG_A, fin + HOUR + 1, HOUR)).toEqual({ ok: false, reason: "stale" });
    expect(await freshSweeperPass(store, ROOT, ORG_A, fin - 1000, HOUR)).toMatchObject({ ok: true, ageMs: 0 });
    expect(await freshSweeperPass(store, ROOT, ORG_A, fin - REARM_RECEIPT_FUTURE_SKEW_MS - 1, HOUR)).toEqual({ ok: false, reason: "future" });
    // A newer pass makes it fresh again.
    await writeRearmPassReceipt(store, receiptFor(ORG_A, T0 + 2 * HOUR));
    expect(await freshSweeperPass(store, ROOT, ORG_A, fin + 2 * HOUR + 1, HOUR)).toMatchObject({ ok: true });
  });

  test("the fresher of the org's and the every-org receipt counts", async () => {
    const store = fresh();
    await writeRearmPassReceipt(store, receiptFor(ORG_A, T0));
    await writeRearmPassReceipt(store, receiptFor(REARM_ALL_ORGS, T0 + 2 * HOUR, { scopeHash: rearmScopeHash(rearmScopes(ROOT, null)) }));
    expect(await freshSweeperPass(store, ROOT, ORG_A, T0 + 2 * HOUR + 10_000, HOUR)).toMatchObject({ ok: true, scope: "all" });
  });

  test("a malformed receipt or pointer is refused, at either scope", async () => {
    const cases: [string, (s: ObjectStore) => Promise<unknown>][] = [
      ["pointer not json", (s) => putJson(s, rearmPassLatestKey(ROOT, ORG_A), "{nope")],
      ["pointer wrong org", async (s) => putJson(s, rearmPassLatestKey(ROOT, ORG_A), { ...JSON.parse(await text(s, rearmPassLatestKey(ROOT, ORG_A))), org: ORG_B })],
      ["pointer extra field", async (s) => putJson(s, rearmPassLatestKey(ROOT, ORG_A), { ...JSON.parse(await text(s, rearmPassLatestKey(ROOT, ORG_A))), live: true })],
      ["receipt edited (hash mismatch)", (s) => putJson(s, rearmPassReceiptKey(ROOT, ORG_A, T0), receiptFor(ORG_A, T0, { finishedAtMs: T0 + 9_000_000 }))],
      ["receipt missing behind pointer", async (s) => s.delete?.(objectKey(rearmPassReceiptKey(ROOT, ORG_A, T0)))],
      ["the every-org pointer is garbage", (s) => putJson(s, rearmPassLatestKey(ROOT, REARM_ALL_ORGS), "[]")],
    ];
    for (const [name, corrupt] of cases) {
      const store = fresh();
      await writeRearmPassReceipt(store, receiptFor(ORG_A, T0));
      await corrupt(store);
      expect({ name, got: await freshSweeperPass(store, ROOT, ORG_A, T0 + 10_000, HOUR) }).toEqual({ name, got: { ok: false, reason: "malformed" } });
    }
  });

  test("a receipt whose pointer and bytes agree but whose content is not a clean pass is refused", async () => {
    const bad: Partial<RearmPassReceipt>[] = [
      { dryRun: true } as never, { complete: false } as never, { failures: 1 } as never, { fromStart: false } as never, { enabled: false } as never,
      { counts: { ...zeroCounts(), failed: 1 } }, { counts: { ...zeroCounts(), deferred: 1 } }, { counts: { listed: 1 } as never },
      { keyRoot: "other" }, { org: ORG_B }, { finishedAtMs: T0 - 1 }, { scopeHash: "x" }, { bucketHash: "transcripts" },
      { revision: { gitSha: "nope" } }, { workload: { podUid: "x" } }, { extra: 1 } as never,
    ];
    for (const over of bad) expect({ over, ok: validRearmPassReceipt(receiptFor(ORG_A, T0, over), ROOT, ORG_A) }).toEqual({ over, ok: false });
    expect(validRearmPassReceipt(receiptFor(ORG_A, T0), ROOT, ORG_A)).toBe(true);
    // Hand-planted (bypassing the writer) with a consistent pointer: still refused.
    const store = fresh();
    const planted = new TextEncoder().encode(JSON.stringify(receiptFor(ORG_A, T0, { dryRun: true } as never)));
    await store.put(objectKey(rearmPassReceiptKey(ROOT, ORG_A, T0)), planted, { contentType: "application/json" });
    await putJson(store, rearmPassLatestKey(ROOT, ORG_A), { v: 1, kind: "rearm-sweeper-latest", org: ORG_A, startedAtMs: T0, finishedAtMs: T0 + 5000,
      receiptSha256: createHash("sha256").update(planted).digest("hex") });
    expect(await freshSweeperPass(store, ROOT, ORG_A, T0 + 10_000, HOUR)).toEqual({ ok: false, reason: "malformed" });
  });

  test("bad requests are refused without reading", async () => {
    const store = fresh();
    for (const [root, org, now, max] of [["Bad", ORG_A, T0, HOUR], [ROOT, ".all", T0, HOUR], [ROOT, "../x", T0, HOUR], [ROOT, ORG_A, 0, HOUR],
      [ROOT, ORG_A, T0, 0], [ROOT, ORG_A, T0, 8 * 24 * HOUR], [ROOT, ORG_A, T0, 1.5]] as const) {
      expect(await freshSweeperPass(store, root, org, now, max)).toEqual({ ok: false, reason: "invalid_request" });
    }
  });

  test("provenance readers drop malformed values", () => {
    expect(rearmPassRevision({ GITHUB_SHA: "abc", INGEST_REARM_IMAGE_DIGEST: "latest" })).toEqual({});
    expect(rearmPassRevision({ GITHUB_SHA: "A".repeat(40) })).toEqual({ gitSha: "a".repeat(40) });
    expect(rearmPassWorkload({ INGEST_REARM_JOB_NAME: "Bad_Name", INGEST_REARM_POD_NAMESPACE: "ingest", AWS_LAMBDA_FUNCTION_NAME: "fn-1" }))
      .toEqual({ namespace: "ingest", lambdaFunction: "fn-1" });
  });
});

describe("rearm sweeper: a failed pass logs an allow-listed reason, and writes no receipt", () => {
  /** The store's methods, bound (its private fields need the real receiver), with `op` replaced. */
  const wrap = (store: ObjectStore, op: string, fn: (real: (...a: unknown[]) => unknown) => (...a: unknown[]) => unknown): ObjectStore =>
    new Proxy(store, { get: (t, k) => {
      const v = Reflect.get(t, k, t);
      const bound = typeof v === "function" ? v.bind(t) : v;
      return k === op ? fn(bound) : bound;
    } }) as ObjectStore;
  /** A store whose `op` always fails with `error`. */
  const failing = (store: ObjectStore, op: "getObject" | "list" | "put", error: unknown) => wrap(store, op, () => () => Promise.reject(error));
  const SECRETISH = "96bulls/.ledger/rearm-sweeper/cursor-x.json org-a transcripts";

  test("classes: s3 (status), timeout (transport cause), kms, fence, credentials, unknown; never the message", () => {
    expect(rearmFailure(new ObjectStoreError("transient", "s3", `${SECRETISH}: HTTP 503`, { status: 503 }))).toEqual({ reason: "s3", status: 503 });
    expect(rearmFailure(new ObjectStoreError("transient", "s3", SECRETISH, { transportCause: "etimedout" }))).toEqual({ reason: "timeout", cause: "etimedout" });
    expect(rearmFailure(new ObjectStoreError("backend", "s3", `${SECRETISH}: HTTP 400: <Code>KMS.DisabledException</Code>`, { status: 400 }))).toEqual({ reason: "kms", status: 400 });
    expect(rearmFailure(Object.assign(new Error("S3 DELETE failed"), { code: "s3_500" }))).toEqual({ reason: "s3", status: 500 });
    expect(rearmFailure(new LedgerUnavailableError("ledger_fence_conflict"))).toMatchObject({ reason: "fence" });
    const ledgerStore = Object.assign(new LedgerUnavailableError("ledger_store_transient"), { storeStatus: 500 });
    expect(rearmFailure(ledgerStore)).toEqual({ reason: "s3", status: 500 });
    expect(rearmFailure(Object.assign(new Error("pod identity: http 500"), { code: "credentials_unavailable" }))).toEqual({ reason: "credentials" });
    expect(rearmFailure(Object.assign(new Error("x"), { name: "AbortError" }))).toEqual({ reason: "timeout" });
    expect(rearmFailure(new Error(SECRETISH))).toEqual({ reason: "unknown" });
    expect(rearmFailure("boom")).toEqual({ reason: "unknown" });
    // Untrusted fields are dropped, not echoed.
    expect(rearmFailure(Object.assign(new LedgerUnavailableError("ledger_store_backend"), { storeStatus: 99999, storeCause: SECRETISH }))).toEqual({ reason: "s3" });
  });

  test("a pass that throws carries its phase; the loop's line is codes only; no receipt is written", async () => {
    const store = fresh();
    const cursorDown = failing(store, "getObject", new ObjectStoreError("transient", "s3", `${SECRETISH}: HTTP 503`, { status: 503 }));
    const err = await runRearmSweeperPass(SCOPED, [], { ...overridesFor(store), store: () => cursorDown }).catch((e) => e);
    expect(exitCodeForError(err)).toBe(4);
    expect(rearmFailure(err)).toEqual({ reason: "s3", phase: "cursor", status: 503 });
    const listDown = failing(store, "list", new ObjectStoreError("transient", "s3", SECRETISH, { transportCause: "timeout" }));
    const lines: unknown[] = [];
    let n = 0;
    await runRearmSweeperLoop(SCOPED, [], (l) => lines.push(l), () => n >= 1, async () => { n++; }, { ...overridesFor(store), store: () => listDown });
    expect(lines).toEqual([{ event: "rearm_sweep", error: "rearm_sweep_failed", reason: "timeout", phase: "sweep", cause: "timeout" }]);
    for (const id of [ORG_A, ORG_B, ROOT, "transcripts"]) expect(JSON.stringify(lines)).not.toContain(id);
    expect(await passes(store)).toEqual([]);
  });

  test("a receipt write that fails is reported as a code and does not fail the pass", async () => {
    const store = fresh();
    const putDown = wrap(store, "put", (real) => (key, ...rest) =>
      String(key).includes("/passes/") ? Promise.reject(new ObjectStoreError("transient", "s3", SECRETISH, { status: 500 })) : real(key, ...rest));
    const { output } = await runRearmSweeperPass(SCOPED, [], { ...overridesFor(store), store: () => putDown });
    expect(output).toMatchObject({ enabled: true, complete: true, receipt: "failed", receiptError: "store_error" });
    expect(exitCodeForPass(output)).toBe(0);
    expect(JSON.stringify(output)).not.toContain(SECRETISH);
  });
});
