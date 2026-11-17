/**
 * The production pilot path of the ledger-log stream operator (docs/design/s3-ledger-log-commit.md "Production
 * pilot"): ledger-log-stream-production.ts (contract), ledger-log-stream-ops.ts (preflight, production switch and
 * close, measure), scripts/ledger-log-stream.ts (CLI guard), scripts/ledger-log-pilot*.ts and
 * .github/workflows/ledger-log-pilot.yml (the dormant workflow). Every refusal and the happy paths, on an fs store with
 * the objects written directly (no edge): no credentials, no network.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { appendBatches, logKeys, openStream, takeStream } from "../ledger-log.ts";
import { logStreamRegistryKey } from "../ledger-log-materialize.ts";
import { fenceAuthorityKey, fenceAuthorityStopKey, orgFenceAuthorityKey, s3LedgerKeys } from "../ledger-s3.ts";
import {
  closeLogStream, logCommitProofKey, logModeNonProdKey, measureArrivalRate, originKeySegment, streamPreflight, streamStatus, switchToLogMode,
  type StepResult,
} from "../ledger-log-stream-ops.ts";
import {
  bucketBindingSha256, confirmationExpiry, confirmationRefusal, LEDGER_LOG_PILOT_PLAN_ARTIFACT, LEDGER_LOG_PILOT_WORKFLOW,
  LEDGER_LOG_PRODUCTION_ENABLED, LEDGER_LOG_PRODUCTION_PLAN_TTL_MS, ledgerLogPilotDispatch, ledgerLogPilotRunName, ledgerLogPilotTarget,
  operatorCodeSha, pilotStream, preflightStateSha256, productionConfirmation, productionContextRefusal, streamIdentitySha256,
  type ProductionContext,
} from "../ledger-log-stream-production.ts";
import { SCHEMA_RELEASE as S } from "../../hoopdb/schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../private-ingest-release.ts";
import { TENANT, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const BUCKET = "fixture-ledger-bucket";
const SHA = "a".repeat(40);
const NOW = 1_800_000_000_000;
const cleanups: (() => unknown)[] = [];
afterEach(async () => { for (const f of cleanups.splice(0).reverse()) await f(); });

const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};
const putJson = (store: ObjectStore, key: string, v: unknown) =>
  store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(v)), { contentType: "application/json" });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const authority = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on",
  rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: NOW };
const legacyDoc = (store: ObjectStore, batchId: string, state: "pending" | "committed", originId = ORIGIN) =>
  putJson(store, s3LedgerKeys(ROOT, TENANT).batch(originId, batchId), { v: 1, originId, batchId, state });

/** A production-like bucket: committed legacy traffic, the org on the S3 ledger, the proof, and NO non-production marker. */
async function prodBucket() {
  const s = scratchStore();
  cleanups.push(s.done);
  await legacyDoc(s.store, "b1", "committed");
  await legacyDoc(s.store, "b2", "committed");
  await putJson(s.store, fenceAuthorityKey(ROOT), authority);
  await putJson(s.store, logCommitProofKey(ROOT), { v: 1, mode: "on" });
  return s.store;
}
const ctx = (over: Partial<ProductionContext> = {}): ProductionContext =>
  ({ bucket: BUCKET, codeSha: SHA, planRef: "local", nowMs: NOW, enabled: true, ...over });
const noWait = { settleMs: 0, sleep: async () => {} };
const plan = async (store: ObjectStore, action: "switch" | "close", over: Partial<ProductionContext> = {}) =>
  action === "switch"
    ? switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: false, writer: "op", production: ctx(over), ...noWait })
    : closeLogStream(store, ROOT, TENANT, ORIGIN, { apply: false, production: ctx(over), ...noWait });
const ids = [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN, BUCKET];
const noRawIds = (r: unknown) => { const t = JSON.stringify(r); for (const id of ids) expect(t).not.toContain(id); };

describe("contract (ledger-log-stream-production.ts)", () => {
  test("the switch ships dormant, and the phrase binds every field", () => {
    expect(LEDGER_LOG_PRODUCTION_ENABLED).toBe(false);
    const f = { action: "switch" as const, bucketSha256: "1".repeat(64), streamSha256: "2".repeat(64), codeSha: SHA, stateSha256: "3".repeat(64),
      expiresAt: "2027-01-15T08:00:00.000Z", planRef: "local" };
    const p = productionConfirmation(f);
    expect(p).toBe(`LEDGER-LOG-SWITCH BUCKET ${"1".repeat(64)} STREAM ${"2".repeat(64)} SHA ${SHA} STATE ${"3".repeat(64)} EXPIRES 2027-01-15T08:00:00.000Z PLAN local`);
    expect(confirmationExpiry(p)).toBe(f.expiresAt);
    expect(confirmationExpiry(p.replace(".000Z", "Z"))).toBeUndefined();
    expect(confirmationExpiry(p.replace("2027-01-15", "2027-02-30"))).toBeUndefined();
    const at = Date.parse(f.expiresAt);
    const { expiresAt: _e, ...rebuilt } = f;
    const c = (confirmation: string, nowMs = at - 60_000) => confirmationRefusal({ ...ctx(), confirmation, nowMs }, rebuilt);
    expect(c(p)).toBeUndefined();
    expect(c(p, at)).toBe("production_confirmation_expired");
    expect(c(p, at - LEDGER_LOG_PRODUCTION_PLAN_TTL_MS - 1)).toBe("production_confirmation_expiry_too_far");
    for (const bad of [p + " ", p.toLowerCase(), p.replace("SWITCH", "CLOSE"), p.replace(SHA, "b".repeat(40)), p.replace("1".repeat(64), "4".repeat(64)),
      p.replace("2".repeat(64), "4".repeat(64)), p.replace("3".repeat(64), "4".repeat(64)), p.replace("PLAN local", "PLAN 7"), ""])
      expect(c(bad)).toBe("production_confirmation_mismatch");
    // Digests are distinct per bucket, key root, stream and state.
    expect(bucketBindingSha256(BUCKET, ROOT)).not.toBe(bucketBindingSha256(BUCKET, "other"));
    expect(bucketBindingSha256(BUCKET, ROOT)).not.toBe(bucketBindingSha256(BUCKET + "x", ROOT));
    expect(streamIdentitySha256(ROOT, TENANT, ORIGIN)).not.toBe(streamIdentitySha256(ROOT, TENANT, "origin-2"));
    expect(preflightStateSha256("switch", "legacy", null)).not.toBe(preflightStateSha256("close", "legacy", null));
  });

  test("context shape: code sha, plan ref, dormant switch, confirmation presence", () => {
    expect(productionContextRefusal(ctx({ codeSha: "abc" }), false)).toBe("production_code_sha");
    expect(productionContextRefusal(ctx({ planRef: "0" }), false)).toBe("production_plan_ref");
    expect(productionContextRefusal(ctx({ bucket: "" }), false)).toBe("production_bucket");
    expect(productionContextRefusal(ctx({ confirmation: "LEDGER-LOG-SWITCH" }), false)).toBe("production_confirmation_on_plan");
    expect(productionContextRefusal(ctx(), false)).toBeUndefined();
    // The plan is allowed while dormant; the apply is not.
    expect(productionContextRefusal(ctx({ enabled: false }), false)).toBeUndefined();
    expect(productionContextRefusal(ctx({ enabled: false, confirmation: "LEDGER-LOG-SWITCH x" }), true)).toBe("production_dormant");
    expect(productionContextRefusal({ bucket: BUCKET, codeSha: SHA, planRef: "local", confirmation: "LEDGER-LOG-SWITCH x" }, true)).toBe("production_dormant");
    expect(productionContextRefusal(ctx(), true)).toBe("production_confirmation_required");
    expect(productionContextRefusal(ctx({ confirmation: "APPLY x" }), true)).toBe("production_confirmation_required");
  });

  test("operatorCodeSha: GITHUB_SHA in Actions; locally a clean worktree on origin/main", async () => {
    expect(operatorCodeSha({ GITHUB_ACTIONS: "true", GITHUB_SHA: SHA }, () => { throw new Error("no git"); })).toBe(SHA);
    expect(await code(() => operatorCodeSha({ GITHUB_ACTIONS: "true", GITHUB_SHA: "x" }, () => ""))).toBe("production_code_sha");
    const git = (o: { head?: string; dirty?: string; onMain?: boolean }) => (args: string[]) => {
      if (args[0] === "rev-parse") return (o.head ?? SHA) + "\n";
      if (args[0] === "status") return o.dirty ?? "";
      if (o.onMain === false) throw new Error("exit 1");
      return "";
    };
    expect(operatorCodeSha({}, git({}))).toBe(SHA);
    expect(await code(() => operatorCodeSha({}, git({ head: "nope" })))).toBe("production_code_sha");
    expect(await code(() => operatorCodeSha({}, git({ dirty: " M src/x.ts\n" })))).toBe("production_worktree_dirty");
    expect(await code(() => operatorCodeSha({}, git({ onMain: false })))).toBe("production_code_not_on_main");
    expect(await code(() => operatorCodeSha({}, () => { throw new Error("not a repo"); }))).toBe("production_code_sha");
  });

  test("pilot stream secret, dispatch inputs and target variables are shape-checked", async () => {
    const good = JSON.stringify({ org: "org-a", workspace: "ws-a", user: "user-1", origin: ORIGIN });
    expect(pilotStream(good)).toEqual({ tenant: TENANT, originId: ORIGIN });
    for (const bad of [undefined, "", "[]", "null", JSON.stringify({ org: "org-a", workspace: "ws-a", user: "user-1" }),
      JSON.stringify({ org: "org a", workspace: "ws-a", user: "user-1", origin: ORIGIN }),
      JSON.stringify({ org: "org-a", workspace: "ws-a", user: "user-1", origin: ORIGIN, extra: "x" })])
      expect(await code(() => pilotStream(bad))).toBe("ledger_log_pilot_stream");
    const phrase = `LEDGER-LOG-SWITCH BUCKET ${"1".repeat(64)} STREAM ${"2".repeat(64)} SHA ${SHA} STATE ${"3".repeat(64)} EXPIRES 2027-01-15T08:00:00.000Z PLAN 42`;
    const env = (mode: string, extra: Record<string, string> = {}) => ({ LEDGER_LOG_PILOT_MODE: mode, LEDGER_LOG_PILOT_SHA: SHA, ...extra });
    const apply = env("switch", { LEDGER_LOG_PILOT_PLAN_RUN: "42", LEDGER_LOG_PILOT_CONFIRMATION: phrase });
    expect(ledgerLogPilotDispatch(env("status"), false).mode).toBe("status");
    expect(ledgerLogPilotDispatch(env("plan-switch"), false).planRun).toBeNull();
    expect(ledgerLogPilotDispatch(env("measure", { LEDGER_LOG_PILOT_WINDOW_SECONDS: "900" }), false).windowSeconds).toBe(900);
    expect(await code(() => ledgerLogPilotDispatch(apply, false))).toBe("ledger_log_pilot_dormant");
    expect(ledgerLogPilotDispatch(apply, true).planRun).toBe("42");
    expect(await code(() => ledgerLogPilotDispatch(env("undo"), true))).toBe("ledger_log_pilot_inputs");
    expect(await code(() => ledgerLogPilotDispatch(env("status", { LEDGER_LOG_PILOT_SHA: "abc" }), true))).toBe("ledger_log_pilot_inputs");
    expect(await code(() => ledgerLogPilotDispatch(env("status", { LEDGER_LOG_PILOT_PLAN_RUN: "42" }), true))).toBe("ledger_log_pilot_plan_run");
    expect(await code(() => ledgerLogPilotDispatch({ ...apply, LEDGER_LOG_PILOT_PLAN_RUN: "" }, true))).toBe("ledger_log_pilot_plan_run");
    expect(await code(() => ledgerLogPilotDispatch(env("plan-switch", { LEDGER_LOG_PILOT_CONFIRMATION: phrase }), true))).toBe("ledger_log_pilot_confirmation");
    for (const bad of [phrase.replace("SWITCH", "CLOSE"), phrase.replace(SHA, "b".repeat(40)), phrase.replace("PLAN 42", "PLAN 43"),
      phrase.replace(".000Z", "Z"), "APPLY x"])
      expect(await code(() => ledgerLogPilotDispatch({ ...apply, LEDGER_LOG_PILOT_CONFIRMATION: bad }, true))).toBe("ledger_log_pilot_confirmation");
    expect(await code(() => ledgerLogPilotDispatch(env("status", { LEDGER_LOG_PILOT_WINDOW_SECONDS: "60" }), true))).toBe("ledger_log_pilot_window");
    expect(await code(() => ledgerLogPilotDispatch(env("measure", { LEDGER_LOG_PILOT_WINDOW_SECONDS: "0" }), true))).toBe("ledger_log_pilot_window");
    // Fixture account and key ids only (never a real one).
    const acct = "000000000000";
    const target = { LEDGER_LOG_PILOT_BUCKET: BUCKET, LEDGER_LOG_PILOT_REGION: "us-west-2",
      LEDGER_LOG_PILOT_KMS_KEY_ARN: `arn:aws:kms:us-west-2:${acct}:key/00000000-0000-0000-0000-000000000000`,
      LEDGER_LOG_PILOT_READ_ROLE_ARN: `arn:aws:iam::${acct}:role/pilot-read`, LEDGER_LOG_PILOT_WRITE_ROLE_ARN: `arn:aws:iam::${acct}:role/pilot-write` };
    expect(ledgerLogPilotTarget(target).keyRoot).toBe("96bulls");
    expect(await code(() => ledgerLogPilotTarget({ ...target, LEDGER_LOG_PILOT_BUCKET: "Bad_Bucket" }))).toBe("ledger_log_pilot_target_bucket");
    expect(await code(() => ledgerLogPilotTarget({ ...target, LEDGER_LOG_PILOT_REGION: "us-east-1" }))).toBe("ledger_log_pilot_target_kms");
    expect(await code(() => ledgerLogPilotTarget({ ...target, LEDGER_LOG_PILOT_WRITE_ROLE_ARN: target.LEDGER_LOG_PILOT_READ_ROLE_ARN })))
      .toBe("ledger_log_pilot_target_roles");
    expect(await code(() => ledgerLogPilotTarget({ ...target, LEDGER_LOG_PILOT_KEY_ROOT: "../x" }))).toBe("ledger_log_pilot_target_key_root");
  });
});

describe("production switch (ledger-log-stream-ops.ts)", () => {
  test("default refusal stays: no production context on a bucket without the non-production marker", async () => {
    const store = await prodBucket();
    const r = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", ...noWait });
    expect([r.applied, r.refused]).toEqual([false, "bucket_not_declared_nonprod"]);
    expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
  });

  test("happy path: the plan writes nothing and prints the phrase; the apply with that phrase switches and registers", async () => {
    const store = await prodBucket();
    const p = await plan(store, "switch");
    expect([p.applied, p.refused, p.action]).toEqual([false, undefined, "switch (dry run)"]);
    expect(p.preflight).toMatchObject({ mode: "legacy", legacyCommitted: 2, legacyPending: 0, fenceAuthority: "deployment", userFence: "absent",
      logCommitProven: true, registered: false });
    const phrase = p.production!.confirmation!;
    expect(phrase.startsWith(`LEDGER-LOG-SWITCH BUCKET ${bucketBindingSha256(BUCKET, ROOT)} STREAM ${streamIdentitySha256(ROOT, TENANT, ORIGIN)} SHA ${SHA} `)).toBe(true);
    expect(p.production!.expiresAt).toBe(new Date(NOW + LEDGER_LOG_PRODUCTION_PLAN_TTL_MS).toISOString());
    expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
    noRawIds(p);
    const done = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: phrase, nowMs: NOW + 60_000 }), ...noWait });
    expect([done.applied, done.refused, done.status.mode, done.status.legacyCommitted]).toEqual([true, undefined, "log", 2]);
    expect(done.production!.approvalSha256).toMatch(/^[0-9a-f]{64}$/);
    expect(done.production!.confirmation).toBeUndefined();
    expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(true);
    expect((await openStream(store, logKeys(ROOT, TENANT, ORIGIN)))!.legacyCutover).toBe("committed=2");
    noRawIds(done);
    // A second apply of the same phrase: the stream is in log mode now.
    const again = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: phrase, nowMs: NOW + 60_000 }), ...noWait });
    expect(again.refused).toBe("stream_is_log");
  });

  test("every preflight refusal, each before any write", async () => {
    const cases: Array<[string, (s: ObjectStore) => Promise<unknown>]> = [
      ["stream_not_found", async (s) => { await s.delete(objectKey(s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b1"))); await s.delete(objectKey(s3LedgerKeys(ROOT, TENANT).batch(ORIGIN, "b2"))); }],
      ["fence_authority_absent", (s) => s.delete(objectKey(fenceAuthorityKey(ROOT)))],
      ["fence_authority_invalid", (s) => putJson(s, fenceAuthorityKey(ROOT), { ...authority, controlMirror: "off" })],
      ["fence_authority_invalid", (s) => putJson(s, orgFenceAuthorityKey(ROOT, TENANT.orgId), { ...authority, orgId: "org-b" })],
      ["fence_authority_stopped", (s) => putJson(s, fenceAuthorityStopKey(ROOT), { v: 1, stop: "all", writtenAtMs: NOW })],
      ["fence_authority_stopped", (s) => putJson(s, fenceAuthorityStopKey(ROOT), { junk: true })],
      ["user_fence_reset_in_flight", (s) => putJson(s, s3LedgerKeys(ROOT, TENANT).fence, { v: 1, resets: 2, resetsCompleted: 1 })],
      ["user_fence_reset_in_flight", (s) => putJson(s, s3LedgerKeys(ROOT, TENANT).fence,
        { v: 2, resets: 1, resetsCompleted: 1, pending: { k: { kind: "reset", requestId: "r" } } })],
      ["user_fence_unreadable", (s) => s.put(objectKey(s3LedgerKeys(ROOT, TENANT).fence), new TextEncoder().encode("{not json"))],
      ["log_commit_not_proven_on", (s) => s.delete(objectKey(logCommitProofKey(ROOT)))],
      ["legacy_batches_pending", (s) => legacyDoc(s, "b3", "pending")],
    ];
    for (const [expected, mutate] of cases) {
      const store = await prodBucket();
      await mutate(store);
      const p = await plan(store, "switch");
      expect([expected, p.refused, p.production?.confirmation]).toEqual([expected, expected, undefined]);
      // The apply refuses the same way, whatever phrase it carries.
      const a = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: "LEDGER-LOG-SWITCH x" }), ...noWait });
      expect([expected, a.applied, a.refused]).toEqual([expected, false, expected]);
      expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
    }
    // An org marker that is valid for this org is enough on its own.
    const store = await prodBucket();
    await store.delete(objectKey(fenceAuthorityKey(ROOT)));
    await putJson(store, orgFenceAuthorityKey(ROOT, TENANT.orgId), { ...authority, orgId: TENANT.orgId });
    expect((await plan(store, "switch")).preflight!.fenceAuthority).toBe("org");
  });

  test("the apply refuses while dormant, without a phrase, and with any phrase not rebuilt from this preflight", async () => {
    const store = await prodBucket();
    const phrase = (await plan(store, "switch")).production!.confirmation!;
    const apply = (over: Partial<ProductionContext>) =>
      switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: phrase, ...over }), ...noWait });
    const refusals: Array<[Partial<ProductionContext>, string]> = [
      [{ enabled: false }, "production_dormant"],
      [{ confirmation: undefined }, "production_confirmation_required"],
      [{ bucket: "another-bucket" }, "production_confirmation_mismatch"],
      [{ codeSha: "b".repeat(40) }, "production_confirmation_mismatch"],
      [{ planRef: "42" }, "production_confirmation_mismatch"],
      [{ confirmation: phrase.replace("LEDGER-LOG-SWITCH", "LEDGER-LOG-CLOSE") }, "production_confirmation_mismatch"],
      [{ nowMs: NOW + LEDGER_LOG_PRODUCTION_PLAN_TTL_MS }, "production_confirmation_expired"],
      [{ nowMs: NOW - 60_000 }, "production_confirmation_expiry_too_far"],
    ];
    for (const [over, expected] of refusals) {
      const r = await apply(over);
      expect([expected, r.applied, r.refused]).toEqual([expected, false, expected]);
    }
    // Another stream of the same user: the phrase names a different stream.
    await legacyDoc(store, "c1", "committed", "origin-2");
    const other = await switchToLogMode(store, ROOT, TENANT, "origin-2", { apply: true, writer: "op", production: ctx({ confirmation: phrase }), ...noWait });
    expect(other.refused).toBe("production_confirmation_mismatch");
    expect(await exists(store, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
    expect(await exists(store, logKeys(ROOT, TENANT, "origin-2").entry(0))).toBe(false);
  });

  test("a legacy claim that starts after the status read is caught by the last listing before entry 0", async () => {
    const base = await prodBucket();
    await putJson(base, logModeNonProdKey(ROOT), { v: 1 });
    let injected = false;
    // The proof HEAD runs after the status read and before the last listing: a claim lands right there.
    const store = new Proxy(base, { get(target, prop, receiver) {
      const v = Reflect.get(target, prop, receiver);
      if (prop !== "head") return typeof v === "function" ? v.bind(target) : v;
      return async (key: unknown) => {
        if (!injected && String(key).endsWith("/.ledger/log-commit-on.json")) { injected = true; await legacyDoc(target, "mid", "pending"); }
        return (v as ObjectStore["head"]).call(target, key as never);
      };
    } }) as ObjectStore;
    const r = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", ...noWait });
    expect([injected, r.applied, r.refused, r.status.legacyPending]).toEqual([true, false, "legacy_batches_pending", 1]);
    expect(await exists(base, logKeys(ROOT, TENANT, ORIGIN).entry(0))).toBe(false);
  });

  test("settle: a legacy commit behind entry 0 is legacy_raced_switch; the materializer's batch docs for logged batches are not", async () => {
    // Raced: a legacy writer commits a batch that is not in the log during the wait.
    let store = await prodBucket();
    let phrase = (await plan(store, "switch")).production!.confirmation!;
    let r: StepResult = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: phrase }), settleMs: 1,
      sleep: async () => { await legacyDoc(store, "late", "committed"); } });
    expect([r.applied, r.refused]).toEqual([true, "legacy_raced_switch"]);
    expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(false);
    // Not raced: admission appends a batch to the log and the materializer commits its legacy batch doc during the wait.
    store = await prodBucket();
    phrase = (await plan(store, "switch")).production!.confirmation!;
    r = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: phrase }), settleMs: 1,
      sleep: async () => {
        const state = await takeStream(store, (await openStream(store, logKeys(ROOT, TENANT, ORIGIN)))!, { writer: "consumer" });
        await appendBatches(store, state, [{ batchId: "logged", digest: "d", receiptId: "r", sessions: [], payloads: [] }], null, { writer: "consumer" });
        await legacyDoc(store, "logged", "committed");
      } });
    expect([r.applied, r.refused, r.status.mode]).toEqual([true, undefined, "log"]);
    expect(await exists(store, logStreamRegistryKey(ROOT, TENANT, ORIGIN))).toBe(true);
  });
});

describe("production close (rollback)", () => {
  async function logStream() {
    const store = await prodBucket();
    const phrase = (await plan(store, "switch")).production!.confirmation!;
    expect((await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", production: ctx({ confirmation: phrase }), ...noWait })).refused).toBeUndefined();
    const keys = logKeys(ROOT, TENANT, ORIGIN);
    const state = await takeStream(store, (await openStream(store, keys))!, { writer: "consumer" });
    await appendBatches(store, state, [{ batchId: "x1", digest: "d", receiptId: "r", sessions: [], payloads: [] }], null, { writer: "consumer" });
    return { store, keys, state };
  }

  test("refuses until every entry is applied, then closes; the receipt reports the stream after the settle", async () => {
    const { store, keys, state } = await logStream();
    const head = state.head;
    const p0 = await plan(store, "close");
    expect([p0.refused, p0.preflight!.head, p0.preflight!.epoch]).toEqual(["entries_not_applied", head, 2]);
    for (let seq = 0; seq <= head; seq++) await putJson(store, keys.marker(seq), { v: 1 });
    const p = await plan(store, "close");
    expect([p.refused, p.preflight!.registered, p.action]).toEqual([undefined, true, "close (dry run)"]);
    expect(await exists(store, keys.closed)).toBe(false);
    const phrase = p.production!.confirmation!;
    expect(phrase.startsWith("LEDGER-LOG-CLOSE ")).toBe(true);
    noRawIds(p);
    // A cached writer appends the entry it had in flight during the settle: the receipt shows the head moving.
    const done = await closeLogStream(store, ROOT, TENANT, ORIGIN, { apply: true, production: ctx({ confirmation: phrase }), settleMs: 1,
      sleep: async () => { await appendBatches(store, state, [{ batchId: "x2", digest: "d", receiptId: "r", sessions: [], payloads: [] }], null, { writer: "consumer" }); } });
    expect([done.applied, done.refused, done.status.mode, done.after!.mode]).toEqual([true, undefined, "closed", "closed"]);
    expect(done.after!.head).toBe(head + 1);
    expect(done.after!.materializedThrough).toBe(head);
    noRawIds(done);
    // Idempotent and one way: a second close (and a switch) refuse on the mode.
    expect((await plan(store, "close")).refused).toBe("stream_is_closed");
    expect((await plan(store, "switch")).refused).toBe("stream_is_closed");
    expect((await streamStatus(store, ROOT, TENANT, ORIGIN)).mode).toBe("closed");
  });

  test("close refusals: a legacy stream, dormant, a switch phrase, and a stopped admission does not block a rollback", async () => {
    const legacy = await prodBucket();
    expect((await plan(legacy, "close")).refused).toBe("stream_is_legacy");
    const { store, keys, state } = await logStream();
    for (let seq = 0; seq <= state.head; seq++) await putJson(store, keys.marker(seq), { v: 1 });
    await putJson(store, fenceAuthorityStopKey(ROOT), { v: 1, stop: "all", writtenAtMs: NOW });
    const p = await plan(store, "close");
    expect([p.refused, p.preflight!.fenceAuthority]).toEqual([undefined, "stopped"]);
    const phrase = p.production!.confirmation!;
    const close = (over: Partial<ProductionContext>) => closeLogStream(store, ROOT, TENANT, ORIGIN, { apply: true, production: ctx({ confirmation: phrase, ...over }), ...noWait });
    expect((await close({ enabled: false })).refused).toBe("production_dormant");
    expect((await close({ confirmation: phrase.replace("LEDGER-LOG-CLOSE", "LEDGER-LOG-SWITCH") })).refused).toBe("production_confirmation_mismatch");
    expect(await exists(store, keys.closed)).toBe(false);
    expect((await close({})).applied).toBe(true);
  });

  test("a corrupt log refuses both actions", async () => {
    const { store, keys } = await logStream();
    await store.delete(objectKey(keys.entry(0)));
    const r = await streamPreflight(store, ROOT, TENANT, ORIGIN, "close");
    expect([r.report.mode, r.report.refused]).toEqual(["corrupt", "stream_log_corrupt"]);
    expect((await streamPreflight(store, ROOT, TENANT, ORIGIN, "switch")).report.refused).toBe("stream_log_corrupt");
  });

  test("the non-production path is unchanged: the marker, no phrase, no settle on close", async () => {
    const store = await prodBucket();
    await putJson(store, logModeNonProdKey(ROOT), { v: 1 });
    const r = await switchToLogMode(store, ROOT, TENANT, ORIGIN, { apply: true, writer: "op", ...noWait });
    expect([r.applied, r.refused, r.preflight, r.production]).toEqual([true, undefined, undefined, undefined]);
    await putJson(store, logKeys(ROOT, TENANT, ORIGIN).marker(0), { v: 1 });
    let slept = false;
    const c = await closeLogStream(store, ROOT, TENANT, ORIGIN, { apply: true, sleep: async () => { slept = true; } });
    expect([c.applied, c.after, slept]).toEqual([true, undefined, false]);
  });
});

describe("measure (read only)", () => {
  /** Lists with fixed lastModified values per key (the fs store's mtime is the test's own clock). */
  function clocked(base: ObjectStore, at: Map<string, number>) {
    return new Proxy(base, { get(target, prop, receiver) {
      const v = Reflect.get(target, prop, receiver);
      if (prop === "put" || prop === "delete") return () => { throw new Error("measure must not write"); };
      if (prop !== "list") return typeof v === "function" ? v.bind(target) : v;
      return async (...args: unknown[]) => {
        const page = await (v as ObjectStore["list"]).apply(target, args as never);
        return { ...page, items: page.items.map((i) => ({ ...i, lastModified: new Date(at.get(String(i.key)) ?? 0) })) };
      };
    } }) as ObjectStore;
  }

  test("per-origin rate over the window, hottest first, peaks per bin, a logged batch counted once, never an id", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const at = new Map<string, number>();
    const keys = s3LedgerKeys(ROOT, TENANT);
    // origin-1: 120 legacy batches in the last 10 minutes, 90 of them inside one minute (1.5/s peak).
    for (let i = 0; i < 120; i++) {
      await legacyDoc(s.store, `h${i}`, "committed");
      at.set(keys.batch(ORIGIN, `h${i}`), NOW - (i < 90 ? 30_000 : 300_000 + i));
    }
    // ...and 5 outside the window.
    for (let i = 0; i < 5; i++) { await legacyDoc(s.store, `old${i}`, "committed"); at.set(keys.batch(ORIGIN, `old${i}`), NOW - 3_600_000); }
    // origin-2: log mode, 3 entries of 2 batches inside the window; one batch also has its materialized legacy doc.
    const lk = logKeys(ROOT, TENANT, "origin-2");
    const { enableLogMode } = await import("../ledger-log.ts");
    const state = await enableLogMode(s.store, lk, "committed=0", { writer: "w", now: () => NOW - 100_000 });
    for (let e = 0; e < 3; e++)
      await appendBatches(s.store, state, [0, 1].map((j) => ({ batchId: `l${e}-${j}`, digest: "d", receiptId: "r", sessions: [], payloads: [] })), null,
        { writer: "w", now: () => NOW - 60_000 + e });
    for (let seq = 0; seq <= 3; seq++) at.set(lk.entry(seq), NOW - 60_000);
    await legacyDoc(s.store, "l0-0", "committed", "origin-2");
    at.set(keys.batch("origin-2", "l0-0"), NOW - 50_000);
    const store = clocked(s.store, at);
    const r = await measureArrivalRate(store, ROOT, TENANT, undefined, { windowMs: 600_000, nowMs: NOW });
    expect(r.truncated).toBe(false);
    expect(r.origins.map((o) => o.originKey)).toEqual([originKeySegment(ROOT, TENANT, ORIGIN).slice(2, 14), originKeySegment(ROOT, TENANT, "origin-2").slice(2, 14)]);
    expect(r.origins[0]).toMatchObject({ legacyBatches: 120, logEntries: 0, batches: 120, batchesPerSecond: 0.2, peakBinBatchesPerSecond: 1.5, aboveVisibleRate: true });
    expect(r.origins[1]).toMatchObject({ legacyBatches: 0, logEntries: 4, logBatches: 6, batches: 6, batchesPerSecond: 0.01, peakBinBatchesPerSecond: 0.1, aboveVisibleRate: false });
    noRawIds(r);
    // One origin only.
    const one = await measureArrivalRate(store, ROOT, TENANT, "origin-2", { windowMs: 600_000, nowMs: NOW });
    expect(one.origins.map((o) => o.batches)).toEqual([6]);
    // A listing budget that runs out says so.
    const cut = await measureArrivalRate(store, ROOT, TENANT, undefined, { windowMs: 600_000, nowMs: NOW, maxPages: 1 });
    expect(cut.truncated).toBe(true);
    expect(await code(() => measureArrivalRate(store, ROOT, TENANT, undefined, { windowMs: 0 }))).toBe("measure_window");
  });
});

describe("CLI guard (scripts/ledger-log-stream.ts)", () => {
  test("--production: only switch/close, never with the non-prod flag, a phrase only on apply, dormant refuses the apply", async () => {
    const { mutationRefusal } = await import("../../../scripts/ledger-log-stream.ts");
    for (const command of ["switch", "close"]) {
      expect(mutationRefusal(command, "sessionsprod", false, false, true, false, false)).toBeUndefined();
      expect(mutationRefusal(command, "sessionsprod", false, false, true, true, true)).toBe("production_confirmation_on_plan");
      expect(mutationRefusal(command, "sessionsprod", true, false, true, true, false)).toBe("production_dormant");
      expect(mutationRefusal(command, "sessionsprod", true, false, true, false, true)).toBe("production_confirmation_required");
      expect(mutationRefusal(command, "sessionsprod", true, false, true, true, true)).toBeUndefined();
      expect(mutationRefusal(command, "sessionsprod", true, true, true, true, true)).toBe("production_flags_conflict");
      // Without --production the old allowlist holds, and a phrase alone is refused.
      expect(mutationRefusal(command, "sessionsprod", true, true)).toBe("bucket_name_production_like");
      expect(mutationRefusal(command, "sessions-dev-bench", false, false, false, true)).toBe("confirmation_without_production");
    }
    for (const command of ["migrate-registry", "status", "measure"])
      expect(mutationRefusal(command, "sessionsprod", false, false, true, false, true)).toBe("production_command_unsupported");
    expect(mutationRefusal("measure", "sessionsprod", false, false)).toBeUndefined();
  });
});

describe("dormant workflow (.github/workflows/ledger-log-pilot.yml)", () => {
  const sha = SHA, planRun = "424242";
  const phrase = `LEDGER-LOG-SWITCH BUCKET ${"1".repeat(64)} STREAM ${"2".repeat(64)} SHA ${sha} STATE ${"3".repeat(64)} EXPIRES 2027-01-15T08:00:00.000Z PLAN ${planRun}`;
  function fixture() {
    const actor = { id: S.dispatcher.id, login: S.dispatcher.login, type: "User" };
    const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: LEDGER_LOG_PILOT_WORKFLOW, run_attempt: 1,
      actor, triggering_actor: { ...actor }, display_title: ledgerLogPilotRunName("switch", sha), created_at: new Date().toISOString() };
    return {
      ref: { object: { sha } }, run,
      environment: { id: 7, name: "ledger-log-pilot", protection_rules: [{ type: "branch_policy" }] as any[],
        deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
      branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
      runs: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
      jobs: { total_count: 6, jobs: ["checks", "session-read-worker / worker", "unit-tests", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
        .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
      approvals: [] as any[],
      plan: { ...run, actor: { ...actor }, triggering_actor: { ...actor }, id: Number(planRun), status: "completed", conclusion: "success",
        display_title: ledgerLogPilotRunName("plan-switch", sha), created_at: new Date(Date.now() - 600_000).toISOString() },
      artifacts: { total_count: 1, artifacts: [{ name: LEDGER_LOG_PILOT_PLAN_ARTIFACT, expired: false }] as any[] },
    };
  }
  type Fixture = ReturnType<typeof fixture>;
  const get = (f: Fixture) => async (path: string): Promise<any> => {
    if (path.endsWith("/git/ref/heads/main")) return f.ref;
    if (path.endsWith("/actions/runs/100")) return f.run;
    if (path.endsWith("/actions/runs/100/approvals")) return f.approvals;
    if (path.endsWith(`/actions/runs/${planRun}`)) return f.plan;
    if (path.endsWith(`/actions/runs/${planRun}/artifacts?per_page=100`)) return f.artifacts;
    if (path.endsWith("/environments/" + f.environment.name)) return f.environment;
    if (path.includes("deployment-branch-policies")) return f.branches;
    if (path.includes("/actions/workflows/")) return f.runs;
    if (path.includes("/runs/10/")) return f.jobs;
    throw new Error("unexpected " + path);
  };
  const context = { GITHUB_REPOSITORY: S.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
    GITHUB_REF: "refs/heads/main", GITHUB_SHA: sha, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100" };
  const applyEnv = { ...context, LEDGER_LOG_PILOT_MODE: "switch", LEDGER_LOG_PILOT_SHA: sha, LEDGER_LOG_PILOT_PLAN_RUN: planRun, LEDGER_LOG_PILOT_CONFIRMATION: phrase };
  const gate = async (e: Record<string, string>, f: Fixture, enabled = true) => {
    const { ledgerLogPilotGithubGate } = await import("../../../scripts/ledger-log-pilot-github.ts");
    return code(() => ledgerLogPilotGithubGate(e, get(f), true, { enabled }));
  };

  test("gate: dormant apply refuses; read modes and a bound apply pass", async () => {
    expect(await gate(applyEnv, fixture(), false)).toBe("ledger_log_pilot_dormant");
    expect(await gate(applyEnv, fixture())).toBeNull();
    for (const mode of ["status", "measure", "plan-switch", "plan-close"]) {
      const f = fixture();
      f.run.display_title = ledgerLogPilotRunName(mode, sha);
      expect(await gate({ ...context, LEDGER_LOG_PILOT_MODE: mode, LEDGER_LOG_PILOT_SHA: sha }, f, false)).toBeNull();
    }
  });

  test("gate: main, context, run, environment, CI, plan run, artifact and approval refusals", async () => {
    const failures: Array<[Record<string, string>, (f: Fixture) => void, string]> = [
      [applyEnv, (f) => { f.ref.object.sha = "b".repeat(40); }, "ledger_log_pilot_gate_main_moved"],
      [{ ...applyEnv, GITHUB_SHA: "b".repeat(40) }, () => {}, "ledger_log_pilot_gate_context"],
      [{ ...applyEnv, GITHUB_RUN_ATTEMPT: "2" }, () => {}, "ledger_log_pilot_gate_context"],
      [{ ...applyEnv, GITHUB_REF: "refs/heads/dev" }, () => {}, "ledger_log_pilot_gate_context"],
      [applyEnv, (f) => { f.run.actor.id = 42; }, "ledger_log_pilot_gate_run"],
      [applyEnv, (f) => { f.run.display_title = ledgerLogPilotRunName("status", sha); }, "ledger_log_pilot_gate_run"],
      [applyEnv, (f) => { f.run.path = ".github/workflows/backlog-ops.yml"; }, "ledger_log_pilot_gate_run"],
      [applyEnv, (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, "ledger_log_pilot_gate_environment"],
      [applyEnv, (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, "ledger_log_pilot_gate_ci"],
      [applyEnv, (f) => { f.plan.display_title = ledgerLogPilotRunName("plan-close", sha); }, "ledger_log_pilot_gate_plan_run"],
      [applyEnv, (f) => { f.plan.conclusion = "failure"; }, "ledger_log_pilot_gate_plan_run"],
      [applyEnv, (f) => { f.plan.actor.id = 42; }, "ledger_log_pilot_gate_plan_run"],
      [applyEnv, (f) => { f.plan.head_sha = "b".repeat(40); }, "ledger_log_pilot_gate_plan_run"],
      [applyEnv, (f) => { f.plan.created_at = new Date(Date.now() - LEDGER_LOG_PRODUCTION_PLAN_TTL_MS - 1000).toISOString(); }, "ledger_log_pilot_gate_plan_run"],
      [applyEnv, (f) => { f.artifacts.artifacts = []; }, "ledger_log_pilot_gate_plan_artifact"],
      [applyEnv, (f) => { f.artifacts.artifacts[0]!.expired = true; }, "ledger_log_pilot_gate_plan_artifact"],
      [applyEnv, (f) => { f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); },
        "ledger_log_pilot_gate_human_approval"],
    ];
    for (const [input, mutate, expected] of failures) {
      const f = fixture();
      mutate(f);
      expect(await gate(input, f)).toBe(expected);
    }
  });

  test("the plan artifact must hold exactly the typed phrase", async () => {
    const { planArtifactConfirmation, pilotMasks } = await import("../../../scripts/ledger-log-pilot.ts");
    expect(planArtifactConfirmation(JSON.stringify({ production: { confirmation: phrase } }))).toBe(phrase);
    for (const bad of ["", "{}", JSON.stringify({ production: { confirmation: 7 } }), JSON.stringify({ production: { confirmation: "APPLY x" } })])
      expect(await code(() => planArtifactConfirmation(bad))).toBe("ledger_log_pilot_plan_artifact");
    expect(pilotMasks(["a", "b"])).toBe("::add-mask::a\n::add-mask::b");
  });

  test("workflow shape: one secret in one step, no input interpolated into a command, OIDC only in the operate job", () => {
    const workflow = readFileSync(LEDGER_LOG_PILOT_WORKFLOW, "utf8");
    expect(workflow.match(/secrets\.[A-Z_]+/gu)).toEqual(["secrets.LEDGER_LOG_PILOT_STREAM"]);
    expect(workflow).toContain("options: [status, measure, plan-switch, switch, plan-close, close]");
    expect(workflow).toContain("group: ${{ github.event_name == 'workflow_dispatch' && 'guarded-session-schema-production'");
    expect(workflow.match(/id-token: write/gu)).toHaveLength(1);
    for (const line of workflow.split("\n").filter((l) => l.trim().startsWith("run:"))) expect(line).not.toContain("${{");
    expect(workflow).toContain("name: ledger-log-pilot-plan");
    expect(workflow).toContain("retention-days: 1");
    // The gate job has no environment (a missing one must never be auto-created unprotected).
    const gateJob = workflow.slice(workflow.indexOf("  prerequisite-gate:"), workflow.indexOf("  operate:"));
    expect(gateJob).not.toContain("environment:\n");
  });
});
