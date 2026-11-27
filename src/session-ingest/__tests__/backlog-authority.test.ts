/**
 * The backlog-ops authority modes (src/session-ingest/backlog-authority.ts, scripts/backlog-authority.ts) and their
 * wiring through the dispatch, the GitHub gate and the credential step (src/session-ingest/backlog-ops.ts,
 * scripts/backlog-ops-github.ts, scripts/backlog-ops.ts): reshard-dry, authority-dry, authority-execute, and a catch-up
 * bound to an authority-dry. GitHub, STS, Cloudflare and the database are fakes; stores are scratch stores.
 */
import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { backlogOpsGithubGate } from "../../../scripts/backlog-ops-github.ts";
import { backlogOpsChildSpawn, backlogOpsRun, checkBoundReceipts } from "../../../scripts/backlog-ops.ts";
import { authorityStdin, main as authorityMain, parseAuthorityArgs, readOnlySql } from "../../../scripts/backlog-authority.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { AUTHORITY_SQL, AUTHORITY_STATS_SQL, evidenceHash, type AuthorityBaseline, type AuthorityEvidenceReport } from "../authority-evidence.ts";
import {
  BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS,
  BACKLOG_OPS_SWEEPER_SPEC_MAX_AGE_MS,
  authorityExecute,
  checkAuthorityDryReceipt,
  checkBaselineReceipt,
  checkReshardReceipt,
  cloudflareGet,
  fromReceiptShape,
  orgFenceAuthorityDoc,
  orgHashes,
  rearmServingSpecKey,
  reshardVerifyOrgs,
  sweeperExpectation,
  toReceiptShape,
  validServingSpec,
  writeOrgMarker,
  type RearmServingSpec,
} from "../backlog-authority.ts";
import {
  BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED,
  BACKLOG_OPS_FRESH_BACKFILL_MS,
  BACKLOG_OPS_RECEIPT_ARTIFACT,
  BACKLOG_OPS_RECEIPT_CONTRACT,
  BACKLOG_OPS_WORKFLOW,
  backlogOpsBindings,
  backlogOpsCloudflareToken,
  backlogOpsHomeId,
  backlogOpsConfirmation,
  backlogOpsDispatch,
  backlogOpsEnvironment,
  backlogOpsLedgerId,
  backlogOpsLedgerTarget,
  backlogOpsReceipt,
  backlogOpsRevisionsSince,
  backlogOpsRole,
  backlogOpsRunName,
  backlogOpsScope,
  backlogOpsTarget,
  backlogOpsToolArgs,
  countsOnly,
  type BacklogOpsMode,
} from "../backlog-ops.ts";
import { orgFenceAuthorityKey, validOrgFenceAuthority } from "../ledger-s3.ts";
import { PRIVATE_INGEST_RELEASE } from "../private-ingest-release.ts";
import { rearmBucketHash, writeRearmPassReceipt, type RearmPassReceipt } from "../rearm-sweeper-receipt.ts";
import { rearmScopeHash, rearmScopes } from "../rearm-sweeper.ts";
import { scratchStore } from "./support.ts";
import { authorityReceiptSummary, type AuthorityReceiptKind } from "../backlog-authority-schema.ts";

const sha = "1".repeat(40), DRY = "111", BACKFILL = "222", FULL = "444", RESHARD = "555", AUTH = "666";
const ORG1 = "11111111-2222-4333-8444-555555555555", ORG2 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const ORGS = [ORG1, ORG2];
const SCOPE = { BACKLOG_OPS_ORGS: `${ORG2},${ORG1}` };
const SCOPE_ID = backlogOpsScope(SCOPE).id;
const TOKEN = "cf_" + "a".repeat(40);
const SINCE = 1_700_000_000_000;
const code = async (fn: () => unknown) => { try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const codeSync = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000",
  BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-read",
  BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-write",
};
const LEDGER = { BACKLOG_OPS_LEDGER_BUCKET: "96bulls-aws-sessions-prod-ledger-000000000000", BACKLOG_OPS_LEDGER_REGION: "us-west-2" };
const aws = { accessKeyId: "ASIAEXAMPLEEXAMPLE", secretAccessKey: "s", sessionToken: "t" };

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const fresh = () => { const s = scratchStore(); cleanups.push(s.done); return s.store; };

/** The dispatch environment of each authority-related run. */
function env(mode: BacklogOpsMode, extra: Record<string, string> = {}): Record<string, string> {
  const base: Record<string, string> = { BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: sha };
  if (mode === "authority-execute") Object.assign(base, { BACKLOG_OPS_DRY_RUN: DRY, BACKLOG_OPS_BACKFILL_RUN: BACKFILL, BACKLOG_OPS_FULL_BACKFILL_RUN: FULL, BACKLOG_OPS_RESHARD_RUN: RESHARD,
    BACKLOG_OPS_CONFIRMATION: `AUTHORITY-EXECUTE ${sha} DRY ${DRY} BACKFILL ${BACKFILL} FULL ${FULL} RESHARD ${RESHARD}` });
  if (mode === "backfill-catchup-dry") Object.assign(base, { BACKLOG_OPS_FULL_BACKFILL_RUN: FULL, BACKLOG_OPS_AUTHORITY_RUN: AUTH });
  if (mode === "backfill-catchup-execute") Object.assign(base, { BACKLOG_OPS_DRY_RUN: DRY, BACKLOG_OPS_FULL_BACKFILL_RUN: FULL, BACKLOG_OPS_AUTHORITY_RUN: AUTH,
    BACKLOG_OPS_CONFIRMATION: `BACKFILL-CATCHUP-EXECUTE ${sha} DRY ${DRY} FULL ${FULL} AUTHORITY ${AUTH}` });
  return { ...base, ...extra };
}
const on = (e: Record<string, string>) => backlogOpsDispatch(e, true, true);

// ---- dispatch ----

test("reshard-dry is dry only: reshard-execute refuses with its own code before the dormant switch", () => {
  expect(codeSync(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "reshard-execute", BACKLOG_OPS_SHA: sha }, false))).toBe("backlog_ops_reshard_dry_only");
  expect(codeSync(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "presence-execute", BACKLOG_OPS_SHA: sha }, false))).toBe("backlog_ops_presence_dry_only");
  for (const mode of ["reshard-dry", "authority-dry"] as const) {
    expect(codeSync(() => backlogOpsDispatch(env(mode), false, false))).toBeNull(); // dry: no switch applies
    for (const k of ["BACKLOG_OPS_DRY_RUN", "BACKLOG_OPS_FULL_BACKFILL_RUN", "BACKLOG_OPS_RESHARD_RUN", "BACKLOG_OPS_AUTHORITY_RUN", "BACKLOG_OPS_CONFIRMATION"])
      expect(codeSync(() => on(env(mode, { [k]: k === "BACKLOG_OPS_CONFIRMATION" ? "X" : "777" })))).not.toBeNull();
    expect(backlogOpsBindings(on(env(mode)))).toEqual([]);
  }
  // reshard-dry alone may bind BACKFILL: the completed catch-up it verifies after.
  expect(backlogOpsBindings(on(env("reshard-dry", { BACKLOG_OPS_BACKFILL_RUN: BACKFILL }))).map((b) => [b.kind, b.mode, b.run]))
    .toEqual([["backfill", "backfill-catchup-execute", BACKFILL]]);
  expect(codeSync(() => on(env("authority-dry", { BACKLOG_OPS_BACKFILL_RUN: BACKFILL })))).toBe("backlog_ops_backfill_run");
  expect(codeSync(() => on(env("reshard-dry", { BACKLOG_OPS_BACKFILL_RUN: "0" })))).toBe("backlog_ops_backfill_run");
});

test("authority-execute is HELD in code: refused before any GitHub read even with executes enabled", async () => {
  expect(BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED).toBe(false);
  expect(codeSync(() => backlogOpsDispatch(env("authority-execute"), true))).toBe("backlog_ops_authority_held");
  // The dormant switch still comes first.
  expect(codeSync(() => backlogOpsDispatch(env("authority-execute"), false, true))).toBe("backlog_ops_dormant");
  const seen: string[] = [];
  expect(await code(() => backlogOpsGithubGate({ ...context, ...env("authority-execute") }, async (p) => { seen.push(p); return {}; }, true, { enabled: true })))
    .toBe("backlog_ops_authority_held");
  expect(seen).toEqual([]);
  // The tool refuses its execute too, before stdin, a database or a store.
  expect(await code(() => authorityMain(["execute", "--bucket", target.BACKLOG_OPS_BUCKET, "--region", "us-east-1", "--kms-key-arn", target.BACKLOG_OPS_KMS_KEY_ARN, "--org", ORG1], {})))
    .toBe("authority_execute_held");
});

test("authority-execute binds DRY, BACKFILL (the catch-up), FULL and RESHARD with its exact confirmation", () => {
  const d = on(env("authority-execute"));
  expect(d.confirmation).toBe(backlogOpsConfirmation(d));
  expect(backlogOpsBindings(d).map((b) => [b.kind, b.mode, b.run])).toEqual([
    ["dry", "authority-dry", DRY], ["backfill", "backfill-catchup-execute", BACKFILL], ["full", "backfill-execute", FULL], ["reshard", "reshard-dry", RESHARD]]);
  const bad = (extra: Record<string, string>) => codeSync(() => on(env("authority-execute", extra)));
  expect(bad({ BACKLOG_OPS_RESHARD_RUN: "" })).toBe("backlog_ops_reshard_run");
  expect(bad({ BACKLOG_OPS_FULL_BACKFILL_RUN: "" })).toBe("backlog_ops_full_backfill_run");
  expect(bad({ BACKLOG_OPS_BACKFILL_RUN: "" })).toBe("backlog_ops_backfill_run");
  expect(bad({ BACKLOG_OPS_DRY_RUN: "" })).toBe("backlog_ops_dry_run");
  expect(bad({ BACKLOG_OPS_EXPORT_RUN: "333" })).toBe("backlog_ops_export_run");
  expect(bad({ BACKLOG_OPS_AUTHORITY_RUN: AUTH })).toBe("backlog_ops_authority_run");
  expect(bad({ BACKLOG_OPS_RESHARD_RUN: DRY })).toBe("backlog_ops_bindings");
  for (const c of [`AUTHORITY-EXECUTE ${sha} DRY ${DRY} BACKFILL ${BACKFILL} FULL ${FULL}`, `AUTHORITY-EXECUTE ${sha} DRY ${DRY} BACKFILL ${BACKFILL} RESHARD ${RESHARD} FULL ${FULL}`,
    `authority-execute ${sha} DRY ${DRY} BACKFILL ${BACKFILL} FULL ${FULL} RESHARD ${RESHARD}`, ""])
    expect(bad({ BACKLOG_OPS_CONFIRMATION: c })).toBe("backlog_ops_confirmation");
  // RESHARD anywhere else refuses; AUTHORITY only on a catch-up.
  for (const mode of ["backfill-execute", "drain-dry", "export-dry", "reshard-dry", "authority-dry"] as const)
    expect(codeSync(() => on({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: sha, BACKLOG_OPS_RESHARD_RUN: RESHARD }))).not.toBeNull();
  for (const mode of ["backfill-dry", "drain-dry", "export-dry", "reshard-dry", "authority-dry"] as const)
    expect(codeSync(() => on({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: sha, BACKLOG_OPS_AUTHORITY_RUN: AUTH }))).toBe("backlog_ops_authority_run");
  // A hand-built dispatch cannot drop a binding.
  expect(codeSync(() => backlogOpsBindings({ ...d, reshardRun: null }))).toBe("backlog_ops_bindings");
  expect(codeSync(() => backlogOpsBindings({ ...on(env("backfill-execute", { BACKLOG_OPS_DRY_RUN: DRY, BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${sha} DRY ${DRY}` })), reshardRun: RESHARD })))
    .toBe("backlog_ops_bindings");
});

test("a catch-up may bind an authority-dry (AUTHORITY, in its confirmation); its receipt names that run", () => {
  const d = on(env("backfill-catchup-execute"));
  expect(d.confirmation).toBe(`BACKFILL-CATCHUP-EXECUTE ${sha} DRY ${DRY} FULL ${FULL} AUTHORITY ${AUTH}`);
  expect(backlogOpsBindings(d).map((b) => [b.kind, b.mode])).toEqual([["dry", "backfill-catchup-dry"], ["full", "backfill-execute"], ["authority", "authority-dry"]]);
  expect(codeSync(() => on(env("backfill-catchup-execute", { BACKLOG_OPS_CONFIRMATION: `BACKFILL-CATCHUP-EXECUTE ${sha} DRY ${DRY} FULL ${FULL}` })))).toBe("backlog_ops_confirmation");
  expect(codeSync(() => on(env("backfill-catchup-execute", { BACKLOG_OPS_AUTHORITY_RUN: "0" })))).toBe("backlog_ops_authority_run");
  expect(codeSync(() => on(env("backfill-catchup-execute", { BACKLOG_OPS_AUTHORITY_RUN: FULL })))).toBe("backlog_ops_bindings");
  // Without AUTHORITY a catch-up is byte-identical to before (confirmation, argv, receipt).
  const plain = on({ BACKLOG_OPS_MODE: "backfill-catchup-execute", BACKLOG_OPS_SHA: sha, BACKLOG_OPS_DRY_RUN: DRY, BACKLOG_OPS_FULL_BACKFILL_RUN: FULL,
    BACKLOG_OPS_CONFIRMATION: `BACKFILL-CATCHUP-EXECUTE ${sha} DRY ${DRY} FULL ${FULL}` });
  expect(plain.authorityRun).toBeNull();
  const t = backlogOpsTarget(target);
  expect(backlogOpsToolArgs(plain, t, backlogOpsScope(SCOPE), SINCE).args).not.toContain("--authority-baseline");
  expect(backlogOpsToolArgs(d, t, backlogOpsScope(SCOPE), SINCE).args).toContain("--authority-baseline");
  expect(codeSync(() => backlogOpsToolArgs(d, t, backlogOpsScope({}), SINCE))).toBe("backlog_ops_scope_required");
  const r = backlogOpsReceipt(d, "9", 0, "{}", SCOPE_ID, "none", "none", HOME);
  expect([r.authority_run, r.full_run, r.home]).toEqual([AUTH, FULL, HOME]);
  const p = backlogOpsReceipt(plain, "9", 0, "{}", SCOPE_ID);
  for (const k of ["authority_run", "full_run", "home"]) expect(k in p).toBe(false);
  // The home id: required exactly on an authority receipt.
  expect(codeSync(() => backlogOpsReceipt(d, "9", 0, "{}", SCOPE_ID))).toBe("backlog_ops_home_id");
  // Any ledger-id tool may record it (a FULL backfill-execute does); a tool outside them may not.
  expect(backlogOpsReceipt(plain, "9", 0, "{}", SCOPE_ID, "none", "none", HOME).home).toBe(HOME);
  expect(codeSync(() => backlogOpsReceipt(on({ BACKLOG_OPS_MODE: "export-dry", BACKLOG_OPS_SHA: sha }), "9", 0, "{}", SCOPE_ID, "none", "none", HOME))).toBe("backlog_ops_home_id");
  expect(codeSync(() => backlogOpsReceipt(d, "9", 0, "{}", SCOPE_ID, "none", "none", "home_x"))).toBe("backlog_ops_home_id");
  // Lists only in an authority receipt.
  expect(codeSync(() => backlogOpsReceipt(plain, "9", 0, JSON.stringify({ l: [1] }), SCOPE_ID))).toBe("backlog_ops_receipt_shape");
  expect(backlogOpsReceipt(d, "9", 0, JSON.stringify({ l: [1] }), SCOPE_ID, "none", "none", HOME).summary).toEqual({ l: [1] });
});

test("argv, role, database and t: fixed per mode; the per-org tools need a scoped run", () => {
  const t = backlogOpsTarget(target), scope = backlogOpsScope(SCOPE);
  const r = backlogOpsToolArgs(on(env("reshard-dry")), t, scope);
  expect(r).toEqual({ script: "scripts/backlog-authority.ts", needsDb: false,
    args: ["reshard-verify", "--bucket", t.bucket, "--region", t.region, "--org", ORG1, "--org", ORG2] });
  expect(backlogOpsToolArgs(on(env("authority-dry")), t, scope)).toEqual({ script: "scripts/backlog-authority.ts", needsDb: true, args: ["evidence", "--org", ORG1, "--org", ORG2] });
  expect(backlogOpsToolArgs(on(env("authority-execute")), t, scope)).toEqual({ script: "scripts/backlog-authority.ts", needsDb: true,
    args: ["execute", "--bucket", t.bucket, "--region", t.region, "--kms-key-arn", t.kmsKeyArn, "--org", ORG1, "--org", ORG2] });
  for (const mode of ["reshard-dry", "authority-dry", "authority-execute"] as const)
    expect(codeSync(() => backlogOpsToolArgs(on(env(mode)), t, backlogOpsScope({})))).toBe("backlog_ops_scope_required");
  expect(backlogOpsRole(on(env("reshard-dry")), t)).toBe(t.dryRoleArn);
  expect(backlogOpsRole(on(env("authority-dry")), t)).toBe(t.dryRoleArn);
  expect(backlogOpsRole(on(env("authority-execute")), t)).toBe(t.executeRoleArn);
  expect(backlogOpsEnvironment("authority-execute")).toBe("session-backlog-execute");
  expect(backlogOpsEnvironment("reshard-dry")).toBe("session-schema-production");
  // authority-execute binds FULL, so it takes the gate's t (checked against its catch-up's record).
  expect(backlogOpsRevisionsSince({ BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) }, on(env("authority-execute")))).toBe(SINCE);
  expect(codeSync(() => backlogOpsRevisionsSince({}, on(env("authority-execute"))))).toBe("backlog_ops_revisions_since");
  expect(codeSync(() => backlogOpsRevisionsSince({ BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) }, on(env("authority-dry"))))).toBe("backlog_ops_revisions_since");
});

test("the Cloudflare token: authority modes only, shape-checked, never in an error", () => {
  for (const mode of ["authority-dry", "authority-execute"] as const) {
    expect(backlogOpsCloudflareToken({ BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env(mode)))).toBe(TOKEN);
    expect(codeSync(() => backlogOpsCloudflareToken({}, on(env(mode))))).toBe("backlog_ops_cloudflare_token_missing");
    for (const bad of ["short", "a b".repeat(20), TOKEN + "!"])
      expect(codeSync(() => backlogOpsCloudflareToken({ BACKLOG_OPS_CLOUDFLARE_TOKEN: bad }, on(env(mode))))).toBe("backlog_ops_cloudflare_token_invalid");
  }
  for (const mode of ["reshard-dry", "backfill-catchup-execute"] as const) {
    expect(backlogOpsCloudflareToken({}, on(env(mode)))).toBeNull();
    expect(codeSync(() => backlogOpsCloudflareToken({ BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env(mode))))).toBe("backlog_ops_cloudflare_token_unexpected");
  }
});

// ---- receipts ----

function evidenceReport(status: AuthorityEvidenceReport["status"] = "authority_evidence_unbound", orgs = ORGS): AuthorityEvidenceReport {
  return {
    status, orgCount: orgs.length, observationWindowMs: 120_000, startedAtMs: SINCE, finishedAtMs: SINCE + 200_000,
    ingestDeploymentHash: evidenceHash("deployment", "d"), relayhistoryDeploymentHash: evidenceHash("deployment", "r"),
    orgs: orgs.map((o) => ({
      orgHash: evidenceHash("org", o), ok: status === "authority_evidence_corroborated", refusals: status === "authority_evidence_corroborated" ? [] : ["sql_unbound"],
      stop: { ok: true, code: null, servingVersions: 1, deploymentsWalked: 3, stopDeployments: 2, effectiveStopMs: SINCE - 3_600_000, stopDeploymentHash: evidenceHash("deployment", "s") },
      mirror: { ok: true, code: null },
      queueDrained: { ok: true, code: null, backlogReads: [0, 0], dlqBacklogReads: [3, 3], dlqConsumers: 0, observedWindowMs: 120_000 },
      sqlQuiet: { ok: status === "authority_evidence_corroborated", code: status === "authority_evidence_corroborated" ? null : "sql_unbound", reads: 2,
        counts: { batchesClaimedSince: 0, batchesCommittedSince: 0, batchesPending: 0, revisionsUpdatedSince: 0, revisionsCount: 120, ownershipCount: 40 },
        fingerprint: { revisionsCount: 120, revisionsDigest: "h" + "ab".repeat(16), ownershipCount: 40, ownershipDigest: "h" + "cd".repeat(16) },
        revisionsStable: true, ownershipStable: true, bound: status === "authority_evidence_corroborated" },
      turnsNeon: { ok: true, code: null, servingVersions: 1, path: "neon" },
    })),
  } as AuthorityEvidenceReport;
}
function baseline(orgs = ORGS, status: AuthorityBaseline["status"] = "authority_baseline_ready"): AuthorityBaseline {
  return { kind: "authority_baseline", v: 1, status, orgCount: orgs.length, orgs: orgs.map((o) => ({
    orgHash: evidenceHash("org", o), ok: status === "authority_baseline_ready", refusals: [], stopDeploymentHash: evidenceHash("deployment", "s"),
    effectiveStopMs: SINCE - 3_600_000, readAtMs: SINCE + 300_000, snapshot: { xmin: 1000, xmax: 1005, xip: [1001, 1003] },
    fingerprint: { revisionsCount: 120, revisionsDigest: "h" + "ab".repeat(16), ownershipCount: 40, ownershipDigest: "h" + "cd".repeat(16) },
    stats: { batchesDeleted: 7, statsResetMs: 0, startedMs: SINCE - 86_400_000 } })) } as AuthorityBaseline;
}
const HOME = backlogOpsHomeId({ bucket: target.BACKLOG_OPS_BUCKET, region: target.BACKLOG_OPS_REGION }, null);
const AUTHORITY_RECEIPT_MODES: readonly BacklogOpsMode[] = ["reshard-dry", "authority-dry", "authority-execute", "backfill-catchup-execute", "backfill-catchup-dry", "backfill-execute"];
const rcpt = (mode: BacklogOpsMode, run: string, summary: unknown, extra: Record<string, unknown> = {}) =>
  JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode, sha, run, scope: SCOPE_ID, ledger: "none", ...(AUTHORITY_RECEIPT_MODES.includes(mode) ? { home: HOME } : {}),
    exitCode: 0, summary, ...extra });
const reshardSummary = (over: Record<string, unknown> = {}, orgs = ORGS) => ({ mode: "dry_run", tool: "reshard_verify", org_count: orgs.length, verify_passed: true,
  per_org: orgs.map((o) => ({ org_hash: evidenceHash("org", o), verify_passed: true, listed: 5, legacy_shards: 2, entries: 9, verified: 9, missing: 0, invalid: 0, failed: 0,
    merged: 0, would_merge: 0, shards: 0, refused: null })), ...over });
const fullOk = { mode: "execute", sections: { fences: "complete", ownership: "complete", revisions: "complete" } };
const zeros = (...k: string[]) => Object.fromEntries(k.map((x) => [x, 0]));
/** A catch-up's whole summary, exactly as upload-fence-backfill.ts writes it (BackfillSummary), with the baseline. */
const catchupOk = (b: unknown = toReceiptShape(baseline())) => ({ mode: "execute", users: 2,
  fences: zeros("deletion_rows", "reset_rows", "created", "updated", "unchanged", "would_create", "would_update", "pairs_added", "resets_added", "deletions_superseded",
    "deletions_unproven", "cold_resets", "needs_review", "unreadable", "contended", "tenant_invalid", "input_invalid", "failed", "resets_in_flight"),
  ownership: { ...zeros("rows", "registered", "would_register", "unchanged", "skipped_deleted", "skipped_no_fence", "invalid", "failed"), failed_codes: {} },
  revisions: { ...zeros("rows", "seed_rows", "slot_rows", "slots_empty", "merged", "would_merge", "skipped_deleted", "skipped_no_fence", "invalid", "shards", "failed"), failed_codes: {} },
  sections: fullOk.sections,
  fingerprint: zeros("revisions_count", "revisions_max_updated_ms", "revisions_hash", "ownership_count", "ownership_hash", "slots_owned_count", "slots_revision_sum", "slots_hash"),
  revisions_since_ms: SINCE, authority_baseline: b });

test("the receipt shape: the evidence module's output round-trips through countsOnly and back, with no id", () => {
  for (const v of [evidenceReport(), evidenceReport("authority_evidence_corroborated"), baseline()]) {
    const stored = toReceiptShape(v);
    expect(countsOnly(stored, 0, true)).toEqual(stored);
    expect(fromReceiptShape(JSON.parse(JSON.stringify(stored)))).toEqual(v);
    const text = JSON.stringify(stored);
    for (const o of ORGS) expect(text).not.toContain(o);
  }
  expect(countsOnly({ mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }, 0, true)).toBeTruthy();
  expect(countsOnly(reshardSummary(), 0, true)).toBeTruthy();
  expect(countsOnly(catchupOk(), 0, true)).toBeTruthy();
});

test("reshard receipts: verify_passed for exactly the scope's orgs, each clean", () => {
  const ok = (s: unknown, orgs = ORGS) => codeSync(() => checkReshardReceipt(rcpt("reshard-dry", RESHARD, s, { after_run: BACKFILL }), orgs, BACKFILL));
  expect(ok(reshardSummary())).toBeNull();
  const NV = "backlog_ops_reshard_not_verified";
  expect(ok(reshardSummary({ verify_passed: false }))).toBe(NV);
  expect(ok(reshardSummary({ mode: "execute" }))).toBe(NV);
  expect(ok(reshardSummary({}, [ORG1]))).toBe(NV);
  expect(ok(reshardSummary(), [ORG1])).toBe(NV);
  for (const k of ["missing", "invalid", "failed"]) {
    const s = reshardSummary();
    (s.per_org[1] as Record<string, unknown>)[k] = 1;
    expect(ok(s)).toBe(NV);
  }
  const s = reshardSummary();
  s.per_org[0]!.verify_passed = false;
  expect(ok(s)).toBe(NV);
  expect(codeSync(() => checkReshardReceipt("{", ORGS, BACKFILL))).toBe(NV);
  // It must name the catch-up it verified after (completed before it ran), exactly.
  expect(codeSync(() => checkReshardReceipt(rcpt("reshard-dry", RESHARD, reshardSummary()), ORGS, BACKFILL))).toBe("backlog_ops_reshard_not_after");
  expect(codeSync(() => checkReshardReceipt(rcpt("reshard-dry", RESHARD, reshardSummary(), { after_run: "999" }), ORGS, BACKFILL))).toBe("backlog_ops_reshard_not_after");
});

test("authority-dry receipts: unbound, exactly the scope's orgs, each refused for sql_unbound alone", () => {
  const ok = (r: AuthorityEvidenceReport, orgs = ORGS, mode = "dry_run") =>
    codeSync(() => checkAuthorityDryReceipt(rcpt("authority-dry", DRY, { mode, evidence: toReceiptShape(r) }), orgs));
  expect(ok(evidenceReport())).toBeNull();
  expect(checkAuthorityDryReceipt(rcpt("authority-dry", DRY, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }), ORGS)).toEqual(evidenceReport());
  const NR = "backlog_ops_authority_not_ready";
  expect(ok(evidenceReport("authority_evidence_refused"))).toBe(NR);
  expect(ok(evidenceReport("authority_evidence_corroborated"))).toBe(NR);
  expect(ok(evidenceReport(), [ORG1])).toBe(NR);
  expect(ok(evidenceReport("authority_evidence_unbound", [ORG1, ORG1]))).toBe(NR);
  expect(ok(evidenceReport(), ORGS, "execute")).toBe(NR);
  const two = evidenceReport();
  two.orgs[0]!.refusals = ["sql_unbound", "mirror_off"];
  expect(ok(two)).toBe(NR);
});

test("baseline receipts: the catch-up's, bound to exactly this DRY, ready, over exactly the scope", () => {
  const ok = (summary: unknown, extra: Record<string, unknown> = { authority_run: DRY, full_run: FULL }, orgs = ORGS) =>
    codeSync(() => checkBaselineReceipt(rcpt("backfill-catchup-execute", BACKFILL, summary, extra), orgs, DRY, FULL));
  expect(ok(catchupOk())).toBeNull();
  expect(checkBaselineReceipt(rcpt("backfill-catchup-execute", BACKFILL, catchupOk(), { authority_run: DRY, full_run: FULL }), ORGS, DRY, FULL)).toEqual(baseline());
  expect(ok(catchupOk(), {})).toBe("backlog_ops_bound_baseline_run");
  // The exact FULL id (a derived t alone could alias two FULL runs created in the same second).
  expect(ok(catchupOk(), { authority_run: DRY })).toBe("backlog_ops_bound_baseline_full");
  expect(ok(catchupOk(), { authority_run: DRY, full_run: "999" })).toBe("backlog_ops_bound_baseline_full");
  expect(ok(catchupOk(), { authority_run: AUTH, full_run: FULL })).toBe("backlog_ops_bound_baseline_run");
  expect(ok(catchupOk(null))).toBe("backlog_ops_bound_baseline_missing");
  expect(ok(catchupOk(toReceiptShape(baseline(ORGS, "authority_baseline_refused"))))).toBe("backlog_ops_bound_baseline_not_ready");
  expect(ok(catchupOk(toReceiptShape(baseline([ORG1]))))).toBe("backlog_ops_bound_baseline_not_ready");
  expect(ok(catchupOk(), { authority_run: DRY, full_run: FULL }, [ORG1])).toBe("backlog_ops_bound_baseline_not_ready");
  const b = baseline();
  (b.orgs[1] as { ok: boolean }).ok = false;
  expect(ok(catchupOk(toReceiptShape(b)))).toBe("backlog_ops_bound_baseline_not_ready");
});

// ---- the credential step ----

const RT = "/tmp/rt-authority";
const path = (kind: string) => `${RT}/backlog-ops-bound/${kind}/backlog-ops-receipt.json`;
function executeFiles(over: Partial<Record<string, string>> = {}): Record<string, string> {
  return {
    [path("dry")]: rcpt("authority-dry", DRY, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }),
    [path("backfill")]: rcpt("backfill-catchup-execute", BACKFILL, catchupOk(), { authority_run: DRY, full_run: FULL }),
    [path("full")]: rcpt("backfill-execute", FULL, fullOk),
    [path("reshard")]: rcpt("reshard-dry", RESHARD, reshardSummary(), { after_run: BACKFILL }),
    ...over,
  } as Record<string, string>;
}
const reader = (files: Record<string, string>) => (p: string) => { if (!(p in files)) throw new Error("missing"); return files[p]!; };
const execEnv = (extra: Record<string, string> = {}) => ({ ...target, ...SCOPE, ...env("authority-execute"), RUNNER_TEMP: RT, ...extra });

test("the credential step, authority-execute: every binding checked before any credential; the baseline handed on", () => {
  const e = execEnv(), d = on(e);
  const bound = checkBoundReceipts(e, d, reader(executeFiles()), SINCE);
  expect(bound.baseline).toEqual(baseline());
  const refuses = (files: Partial<Record<string, string>>, ee = e) => codeSync(() => checkBoundReceipts(ee, on(ee), reader(executeFiles(files)), SINCE));
  // Each binding missing.
  for (const k of ["dry", "backfill", "full", "reshard"]) {
    const files = executeFiles();
    delete files[path(k)];
    expect(codeSync(() => checkBoundReceipts(e, d, reader(files), SINCE))).toBe(`backlog_ops_bound_${k}_missing`);
  }
  // Wrong mode in each slot.
  expect(refuses({ [path("dry")]: rcpt("reshard-dry", DRY, reshardSummary()) })).toBe("backlog_ops_bound_authority_receipt");
  expect(refuses({ [path("backfill")]: rcpt("backfill-refresh-execute", BACKFILL, catchupOk(), { authority_run: DRY, full_run: FULL }) })).toBe("backlog_ops_bound_backfill_receipt");
  expect(refuses({ [path("full")]: rcpt("backfill-catchup-execute", FULL, catchupOk()) })).toBe("backlog_ops_bound_backfill_receipt");
  expect(refuses({ [path("reshard")]: rcpt("authority-dry", RESHARD, reshardSummary()) })).toBe("backlog_ops_bound_reshard_receipt");
  // Content.
  expect(refuses({ [path("reshard")]: rcpt("reshard-dry", RESHARD, reshardSummary({ verify_passed: false }), { after_run: BACKFILL }) })).toBe("backlog_ops_reshard_not_verified");
  expect(refuses({ [path("reshard")]: rcpt("reshard-dry", RESHARD, reshardSummary()) })).toBe("backlog_ops_reshard_not_after");
  expect(refuses({ [path("dry")]: rcpt("authority-dry", DRY, { mode: "dry_run", evidence: toReceiptShape(evidenceReport("authority_evidence_refused")) }) }))
    .toBe("backlog_ops_authority_not_ready");
  expect(refuses({ [path("backfill")]: rcpt("backfill-catchup-execute", BACKFILL, catchupOk(), { authority_run: AUTH, full_run: FULL }) })).toBe("backlog_ops_bound_baseline_run");
  expect(refuses({ [path("backfill")]: rcpt("backfill-catchup-execute", BACKFILL, { ...catchupOk(), revisions_since_ms: SINCE + 1 }, { authority_run: DRY, full_run: FULL }) }))
    .toBe("backlog_ops_bound_backfill_since");
  // Exact scope everywhere: an unscoped FULL or reshard never stands in.
  const unscoped = (raw: string) => JSON.stringify({ ...JSON.parse(raw), scope: "all" });
  for (const k of ["dry", "backfill", "full", "reshard"]) expect(refuses({ [path(k)]: unscoped(executeFiles()[path(k)]!) })).toMatch(/_scope$/u);
  expect(codeSync(() => checkBoundReceipts({ ...e, BACKLOG_OPS_ORGS: "" }, on({ ...e, BACKLOG_OPS_ORGS: "" }), reader(executeFiles()), SINCE))).toBe("backlog_ops_scope_required");
  // The ledger target: every bound receipt recorded this environment's.
  const le = execEnv(LEDGER), lid = backlogOpsLedgerId({ mode: "reshard-dry" }, backlogOpsLedgerTarget(le, target));
  const lhome = backlogOpsHomeId({ bucket: target.BACKLOG_OPS_BUCKET, region: target.BACKLOG_OPS_REGION }, backlogOpsLedgerTarget(le, target));
  expect(lhome).not.toBe(HOME);
  const withLedger = (raw: string) => { const r = JSON.parse(raw); return JSON.stringify({ ...r, ledger: lid, ...(r.home ? { home: lhome } : {}) }); };
  const all = Object.fromEntries(Object.entries(executeFiles()).map(([p, r]) => [p, withLedger(r)]));
  expect(codeSync(() => checkBoundReceipts(le, on(le), reader(all), SINCE))).toBeNull();
  expect(codeSync(() => checkBoundReceipts(le, on(le), reader({ ...all, [path("reshard")]: executeFiles()[path("reshard")]! }), SINCE))).toBe("backlog_ops_bound_reshard_ledger");
  expect(codeSync(() => checkBoundReceipts(e, d, reader(all), SINCE))).toMatch(/_ledger$/u);
  // The store-home pair, exactly (never `none`): a receipt from another Locker bucket, or without one, refuses.
  const other = backlogOpsHomeId({ bucket: "other-transcripts-bucket", region: "us-east-1" }, null);
  // FULL too: every backfill receipt records the home, so a FULL from another Locker never stands in.
  for (const k of ["dry", "backfill", "full", "reshard"]) {
    expect(refuses({ [path(k)]: JSON.stringify({ ...JSON.parse(executeFiles()[path(k)]!), home: other }) })).toBe(`backlog_ops_bound_${k}_home`);
    const { home: _h, ...rest } = JSON.parse(executeFiles()[path(k)]!);
    expect(refuses({ [path(k)]: JSON.stringify(rest) })).toBe(`backlog_ops_bound_${k}_home`);
  }
});

test("the credential step, catch-up with AUTHORITY: the authority-dry checked, its evidence handed to the backfill", () => {
  const e = { ...target, ...SCOPE, ...env("backfill-catchup-dry"), RUNNER_TEMP: RT };
  const files = { [path("full")]: rcpt("backfill-execute", FULL, fullOk), [path("authority")]: rcpt("authority-dry", AUTH, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }) };
  expect(checkBoundReceipts(e, on(e), reader(files), SINCE).evidence).toEqual(evidenceReport());
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader({ [path("full")]: files[path("full")]! }), SINCE))).toBe("backlog_ops_bound_authority_missing");
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader({ ...files, [path("authority")]: rcpt("authority-dry", AUTH, { mode: "dry_run",
    evidence: toReceiptShape(evidenceReport("authority_evidence_refused")) }) }), SINCE))).toBe("backlog_ops_authority_not_ready");
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader({ ...files, [path("authority")]: JSON.stringify({ ...JSON.parse(files[path("authority")]!), scope: "all" }) }), SINCE)))
    .toBe("backlog_ops_bound_authority_scope");
});

test("the credential step, reshard-dry with BACKFILL: the completed catch-up, exact scope and home; no t", () => {
  const e = { ...target, ...SCOPE, ...env("reshard-dry", { BACKLOG_OPS_BACKFILL_RUN: BACKFILL }), RUNNER_TEMP: RT };
  const files = { [path("backfill")]: rcpt("backfill-catchup-execute", BACKFILL, catchupOk(), { authority_run: DRY, full_run: FULL }) };
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader(files), null))).toBeNull();
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader({}), null))).toBe("backlog_ops_bound_backfill_missing");
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader({ [path("backfill")]: JSON.stringify({ ...JSON.parse(files[path("backfill")]!), home: "home_" + "0".repeat(32) }) }), null)))
    .toBe("backlog_ops_bound_backfill_home");
  expect(codeSync(() => checkBoundReceipts(e, on(e), reader({ [path("backfill")]: JSON.stringify({ ...JSON.parse(files[path("backfill")]!), scope: "all" }) }), null)))
    .toBe("backlog_ops_bound_backfill_scope");
  expect(backlogOpsReceipt(on(e), "9", 0, "{}", SCOPE_ID, "none", "none", HOME).after_run).toBe(BACKFILL);
  expect("after_run" in backlogOpsReceipt(on(env("reshard-dry")), "9", 0, "{}", SCOPE_ID, "none", "none", HOME)).toBe(false);
});

test("the child's stdin: the token for authority modes; the baseline for the execute; the evidence for a catch-up", () => {
  const withSince = { BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) };
  const dry = backlogOpsChildSpawn({ ...target, ...SCOPE, BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env("authority-dry")), aws, "postgresql://x");
  expect(JSON.parse(dry.input!)).toEqual({ dbUrl: "postgresql://x", cloudflareToken: TOKEN });
  expect(dry.args.join(" ")).not.toContain(TOKEN);
  expect(Object.values(dry.env).join(" ")).not.toContain(TOKEN);
  const ex = backlogOpsChildSpawn({ ...target, ...SCOPE, ...LEDGER, ...withSince, BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env("authority-execute")), aws, "postgresql://x",
    null, { baseline: baseline() });
  expect(JSON.parse(ex.input!)).toEqual({ dbUrl: "postgresql://x", cloudflareToken: TOKEN, baseline: JSON.parse(JSON.stringify(baseline())), ledgerBucket: LEDGER.BACKLOG_OPS_LEDGER_BUCKET });
  // The authority tool takes no ledger bucket in its environment (only the guard).
  expect(ex.env.INGEST_LEDGER_BUCKET).toBeUndefined();
  const cu = backlogOpsChildSpawn({ ...target, ...SCOPE, ...withSince }, on(env("backfill-catchup-execute")), aws, "postgresql://x", null, { evidence: evidenceReport() });
  expect(JSON.parse(cu.input!)).toEqual({ dbUrl: "postgresql://x", authorityEvidence: JSON.parse(JSON.stringify(evidenceReport())) });
  const rs = backlogOpsChildSpawn({ ...target, ...SCOPE, ...LEDGER }, on(env("reshard-dry")), aws, null);
  expect(rs.input).toBeNull();
  expect(rs.env.INGEST_LEDGER_BUCKET).toBe(LEDGER.BACKLOG_OPS_LEDGER_BUCKET);
  expect(rs.env.INGEST_LEDGER_HOME_GUARD).toBe("on");
  // Mismatched bindings refuse.
  expect(codeSync(() => backlogOpsChildSpawn({ ...target, ...SCOPE, ...withSince, BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env("authority-execute")), aws, "postgresql://x")))
    .toBe("backlog_ops_authority_binding");
  expect(codeSync(() => backlogOpsChildSpawn({ ...target, ...SCOPE, ...withSince }, on(env("backfill-catchup-execute")), aws, "postgresql://x"))).toBe("backlog_ops_authority_binding");
  expect(codeSync(() => backlogOpsChildSpawn({ ...target, ...SCOPE, BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env("authority-dry")), aws, "postgresql://x", null, { evidence: {} })))
    .toBe("backlog_ops_authority_binding");
  expect(codeSync(() => backlogOpsChildSpawn({ ...target, ...SCOPE }, on(env("authority-dry")), aws, "postgresql://x"))).toBe("backlog_ops_cloudflare_token_missing");
  expect(codeSync(() => backlogOpsChildSpawn({ ...target, ...SCOPE, BACKLOG_OPS_CLOUDFLARE_TOKEN: TOKEN }, on(env("reshard-dry")), aws, null))).toBe("backlog_ops_cloudflare_token_unexpected");
});

// ---- the GitHub gate ----

const context = {
  GITHUB_REPOSITORY: P.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main", GITHUB_SHA: sha, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100",
};
const hoursAgo = (h: number) => new Date(Date.now() - h * 3600_000).toISOString();
function fixture(mode: BacklogOpsMode) {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const bound = (id: string, m: BacklogOpsMode, h: number) => ({ id: Number(id), head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: BACKLOG_OPS_WORKFLOW,
    run_attempt: 1, status: "completed", conclusion: "success", actor: { ...actor }, triggering_actor: { ...actor }, display_title: backlogOpsRunName(m, sha), created_at: hoursAgo(h) });
  const dryMode: BacklogOpsMode = mode === "authority-execute" ? "authority-dry" : "backfill-catchup-dry";
  return {
    run: { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: BACKLOG_OPS_WORKFLOW, run_attempt: 1, actor, triggering_actor: { ...actor },
      display_title: backlogOpsRunName(mode, sha) },
    environment: { id: 7, name: backlogOpsEnvironment(mode), protection_rules: [{ type: "branch_policy" }], deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    bound: { [DRY]: bound(DRY, dryMode, 4), [BACKFILL]: bound(BACKFILL, "backfill-catchup-execute", 2), [FULL]: bound(FULL, "backfill-execute", 10),
      [RESHARD]: bound(RESHARD, "reshard-dry", 1), [AUTH]: bound(AUTH, "authority-dry", 5) } as Record<string, any>,
  };
}
type Fx = ReturnType<typeof fixture>;
const get = (f: Fx) => async (p: string): Promise<any> => {
  if (p.endsWith("/git/ref/heads/main")) return { object: { sha } };
  if (p.endsWith("/actions/runs/100")) return f.run;
  if (p.endsWith("/actions/runs/100/approvals")) return [];
  for (const id of [DRY, BACKFILL, FULL, RESHARD, AUTH]) {
    if (p.endsWith(`/actions/runs/${id}`)) return f.bound[id];
    if (p.endsWith(`/actions/runs/${id}/artifacts?per_page=100`)) return { total_count: 1, artifacts: [{ name: BACKLOG_OPS_RECEIPT_ARTIFACT, expired: false }] };
  }
  if (p.endsWith("/environments/" + f.environment.name)) return f.environment;
  if (p.includes("deployment-branch-policies")) return { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] };
  if (p.includes("/actions/workflows/")) return { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] };
  if (p.includes("/runs/10/")) return { total_count: 5, jobs: ["checks", "session-read-worker / worker", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
    .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) };
  throw new Error("unexpected " + p);
};
const gate = (mode: BacklogOpsMode, mutate: (f: Fx) => void = () => {}) => {
  const f = fixture(mode);
  mutate(f);
  return code(() => backlogOpsGithubGate({ ...context, ...env(mode) }, get(f), true, { enabled: true, authorityEnabled: true }));
};

test("gate, authority-execute: DRY an authority-dry and FULL a backfill-execute before the catch-up; the catch-up fresh; RESHARD a reshard-dry", async () => {
  expect(await gate("authority-execute")).toBeNull();
  const f = fixture("authority-execute");
  const r = await backlogOpsGithubGate({ ...context, ...env("authority-execute") }, get(f), true, { enabled: true, authorityEnabled: true });
  expect(r.revisionsSinceMs).toBe(Date.parse(f.bound[FULL].created_at) - 15 * 60_000);
  const cases: Array<[(f: Fx) => void, string]> = [
    [(f) => { f.bound[BACKFILL].created_at = hoursAgo(BACKLOG_OPS_FRESH_BACKFILL_MS / 3600_000 + 0.1); }, "backlog_ops_gate_backfill_run"],
    [(f) => { f.bound[BACKFILL].display_title = backlogOpsRunName("backfill-refresh-execute", sha); }, "backlog_ops_gate_backfill_run"],
    [(f) => { f.bound[BACKFILL].display_title = backlogOpsRunName("backfill-execute", sha); }, "backlog_ops_gate_backfill_run"],
    [(f) => { f.bound[DRY].display_title = backlogOpsRunName("reshard-dry", sha); }, "backlog_ops_gate_dry_run"],
    [(f) => { f.bound[RESHARD].display_title = backlogOpsRunName("authority-dry", sha); }, "backlog_ops_gate_reshard_run"],
    [(f) => { f.bound[RESHARD].conclusion = "failure"; }, "backlog_ops_gate_reshard_run"],
    [(f) => { f.bound[RESHARD].created_at = hoursAgo(25); }, "backlog_ops_gate_reshard_run"],
    // A verify from before the catch-up never stands in: it must verify the state the marker would cover.
    [(f) => { f.bound[RESHARD].created_at = hoursAgo(2.5); }, "backlog_ops_gate_reshard_order"],
    [(f) => { f.bound[FULL].created_at = hoursAgo(1); }, "backlog_ops_gate_full_order"],
    [(f) => { f.bound[DRY].created_at = hoursAgo(1); }, "backlog_ops_gate_order"],
    [(f) => { f.bound[FULL].created_at = hoursAgo(24.1); }, "backlog_ops_gate_full_run"],
  ];
  for (const [m, want] of cases) expect(await gate("authority-execute", m)).toBe(want);
});

test("gate, catch-up with AUTHORITY: an authority-dry under 24 h, before the catch-up's own DRY", async () => {
  expect(await gate("backfill-catchup-dry")).toBeNull();
  expect(await gate("backfill-catchup-execute")).toBeNull();
  expect(await gate("backfill-catchup-execute", (f) => { f.bound[AUTH].created_at = hoursAgo(3); })).toBe("backlog_ops_gate_order");
  expect(await gate("backfill-catchup-dry", (f) => { f.bound[AUTH].display_title = backlogOpsRunName("authority-execute", sha); })).toBe("backlog_ops_gate_authority_run");
  expect(await gate("backfill-catchup-dry", (f) => { f.bound[AUTH].created_at = hoursAgo(25); })).toBe("backlog_ops_gate_authority_run");
  expect(await gate("reshard-dry")).toBeNull();
  expect(await gate("authority-dry")).toBeNull();
});

// ---- reshard-dry ----

const putJson = (store: ObjectStore, key: string, body: unknown) =>
  store.put(objectKey(key), new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)), { contentType: "application/json" });
const ROOT = "96bulls";

test("reshard-dry: one verdict per org (hashed), never stopping early; a legacy entry missing from the new layout fails that org only", async () => {
  const store = fresh();
  const dir = `${ROOT}/.admission-inbox/${ORG2}/v1/ws=w/m=srv_${"0".repeat(32)}`;
  const originHash = createHash("sha256").update("origin-1").digest("hex").slice(0, 32);
  await putJson(store, `${dir}/ledger/revisions/o=${originHash}/r=0a.json`, { v: 1, originId: "origin-1", shard: "0a",
    records: { "rec-1": [3, "rv", null, null, 0, 0] } });
  const s = await reshardVerifyOrgs(store, ROOT, ORGS);
  expect(s.org_count).toBe(2);
  expect(s.verify_passed).toBe(false);
  const by = Object.fromEntries(s.per_org.map((p) => [p.org_hash, p]));
  expect(by[evidenceHash("org", ORG1)]!.verify_passed).toBe(true); // no legacy shard at all: nothing to carry over
  expect(by[evidenceHash("org", ORG2)]!.verify_passed).toBe(false);
  expect(by[evidenceHash("org", ORG2)]!.missing).toBe(1);
  expect(JSON.stringify(s)).not.toContain(ORG2);
  expect(countsOnly(s, 0, true)).toBeTruthy();
  expect(codeSync(() => checkReshardReceipt(rcpt("reshard-dry", RESHARD, s, { after_run: BACKFILL }), ORGS, BACKFILL))).toBe("backlog_ops_reshard_not_verified");
  expect(await code(() => reshardVerifyOrgs(store, ROOT, ["org-a"]))).toBe("backlog_ops_reshard_org");
});

// ---- the sweeper's serving spec and the markers ----

const DIGEST = `sha256:${"b".repeat(64)}`;
const NOW = Date.parse("2026-11-26T12:00:00Z");
const spec = (over: Partial<RearmServingSpec> = {}): RearmServingSpec => ({ v: 1, kind: "rearm-sweeper-serving-spec", observedAtMs: NOW - 60_000, namespace: "ingest",
  cronJobName: "rearm-sweeper", cronJobUid: "0f0e0d0c-0b0a-4908-8706-050403020100", suspended: false, orgs: [ORG1, ORG2], bucket: "transcripts", ledgerBucket: null,
  imageDigests: [DIGEST], ...over });
const run = { bucket: "transcripts", ledgerBucket: null, orgs: ORGS };

test("the serving spec: exact shape; fresh, not suspended, this run's homes, covering the scope", () => {
  expect(validServingSpec(spec())).toBe(true);
  for (const bad of [{ ...spec(), extra: 1 }, { ...spec(), imageDigests: [] }, { ...spec(), imageDigests: ["latest"] }, { ...spec(), orgs: [] }, { ...spec(), cronJobUid: "x" },
    { ...spec(), ledgerBucket: "transcripts" }, (() => { const s: Record<string, unknown> = { ...spec() }; delete s.suspended; return s; })()])
    expect(validServingSpec(bad)).toBe(false);
  const e = sweeperExpectation(spec(), run, NOW);
  expect(e).toEqual({ orgs: [ORG1, ORG2], bucket: "transcripts", ledgerBucket: null, writer: { namespace: "ingest", cronJobName: "rearm-sweeper" }, revisions: { imageDigests: [DIGEST] } });
  expect(sweeperExpectation(null, run, NOW)).toBe("sweeper_spec_missing");
  expect(sweeperExpectation(spec({ observedAtMs: NOW - BACKLOG_OPS_SWEEPER_SPEC_MAX_AGE_MS - 1 }), run, NOW)).toBe("sweeper_spec_stale");
  expect(sweeperExpectation(spec({ observedAtMs: NOW + 10 * 60_000 }), run, NOW)).toBe("sweeper_spec_future");
  expect(sweeperExpectation(spec({ suspended: true }), run, NOW)).toBe("sweeper_suspended");
  expect(sweeperExpectation(spec({ bucket: "other" }), run, NOW)).toBe("sweeper_home_mismatch");
  expect(sweeperExpectation(spec({ ledgerBucket: "ledger" }), run, NOW)).toBe("sweeper_home_mismatch");
  expect(sweeperExpectation(spec(), { ...run, ledgerBucket: "ledger" }, NOW)).toBe("sweeper_home_mismatch");
  expect(sweeperExpectation(spec({ orgs: [ORG1] }), run, NOW)).toBe("sweeper_out_of_scope");
  expect(typeof sweeperExpectation(spec({ orgs: null }), run, NOW)).toBe("object");
});

test("markers: create-only; ANY object already there is a `conflict`, never overwritten; read back", async () => {
  const store = fresh();
  expect(await writeOrgMarker(store, ROOT, ORG1, NOW)).toBe("written");
  const text = async (k: string) => new TextDecoder().decode(await readAll((await store.getObject(objectKey(k))).body));
  const doc = JSON.parse(await text(orgFenceAuthorityKey(ROOT, ORG1)));
  expect(validOrgFenceAuthority(doc)).toBe(true);
  expect(doc).toEqual(orgFenceAuthorityDoc(ORG1, NOW));
  // Even a valid marker for the same org: an earlier or delayed write is not this run's (no epoch protocol yet).
  expect(await writeOrgMarker(store, ROOT, ORG1, NOW + 5)).toBe("conflict");
  expect(JSON.parse(await text(orgFenceAuthorityKey(ROOT, ORG1))).writtenAtMs).toBe(NOW); // not overwritten
  await putJson(store, orgFenceAuthorityKey(ROOT, ORG2), { ...orgFenceAuthorityDoc(ORG1, NOW) }); // another org's marker under ORG2's key
  expect(await writeOrgMarker(store, ROOT, ORG2, NOW)).toBe("conflict");
  await putJson(store, orgFenceAuthorityKey(ROOT, "33333333-2222-4333-8444-555555555555"), "{not json");
  expect(await writeOrgMarker(store, ROOT, "33333333-2222-4333-8444-555555555555", NOW)).toBe("conflict");
  expect(await code(() => writeOrgMarker(store, ROOT, "org-a", NOW))).toBe("marker_org");
  const denied = { ...store, put: async () => { throw new ObjectStoreError("unauthorized", "s3", "AccessDenied", { status: 403 }); } } as unknown as ObjectStore;
  expect(await code(() => writeOrgMarker(denied, ROOT, ORG2, NOW))).toBe("marker_access_denied");
  // A put that "succeeds" but leaves something else behind is caught by the read back.
  const lying = new Proxy(store, { get: (t, p) => p === "put" ? async () => {} : (t as any)[p]?.bind?.(t) ?? (t as any)[p] }) as ObjectStore;
  expect(await code(() => writeOrgMarker(lying, ROOT, "44444444-2222-4333-8444-555555555555", NOW))).toBe("marker_readback_mismatch");
});

function passReceipt(org: string, finishedAtMs: number, over: Partial<RearmPassReceipt> = {}): RearmPassReceipt {
  const scopes = rearmScopes(ROOT, ORGS);
  return { v: 1, kind: "rearm-sweeper-pass", org, enabled: true, dryRun: false, complete: true, fromStart: true, failures: 0, startedAtMs: finishedAtMs - 1000, finishedAtMs,
    scopeHash: rearmScopeHash(scopes), scopes: scopes.length, keyRoot: ROOT, bucketHash: rearmBucketHash("transcripts"), revision: { imageDigest: DIGEST },
    workload: { namespace: "ingest", cronJobName: "rearm-sweeper", jobName: "rearm-sweeper-29000000", jobUid: "0f0e0d0c-0b0a-4908-8706-050403020100" },
    counts: { listed: 0, erased: 0, superseded: 0, alreadyDone: 0, pending: 0, resetRunning: 0, reenabled: 0, fenceAbsent: 0, fenceUnreadable: 0, corrupt: 0, ownerMissing: 0,
      machineMismatch: 0, machineUnbound: 0, layoutCollision: 0, layoutScanTooLarge: 0, machineDerived: 0, emptyUnbound: 0, deferred: 0, gone: 0, failed: 0, wouldErase: 0,
      wouldSupersede: 0, versions: 0 }, ...over };
}

async function world(opts: { report?: AuthorityEvidenceReport; spec?: RearmServingSpec | null; passes?: Partial<Record<string, number>>; clock?: () => number;
  fence?: ((org: string, nowMs: number) => Promise<boolean>) | undefined } = {}) {
  const store = fresh();
  if (opts.spec !== null) await putJson(store, rearmServingSpecKey(ROOT), opts.spec ?? spec());
  for (const org of ORGS) {
    const at = opts.passes?.[org] ?? NOW - 3600_000;
    if (at > 0) await writeRearmPassReceipt(store, passReceipt(org, at));
  }
  const report = opts.report ?? { ...evidenceReport("authority_evidence_corroborated"), finishedAtMs: NOW - 1000 };
  const calls: unknown[] = [];
  const deps = { orgs: ORGS, baseline: baseline(), store, keyRoot: ROOT, bucket: "transcripts", ledgerBucket: null, now: opts.clock ?? (() => NOW),
    sources: {} as never, evidence: (async (orgs: unknown, _src: unknown, o: unknown) => { calls.push([orgs, o]); return report; }) as never,
    writerFence: opts.fence ?? (async () => true) };
  return { store, deps, calls };
}
const markers = async (store: ObjectStore) => {
  const out: string[] = [];
  for (const o of ORGS) { try { await store.getObject(objectKey(orgFenceAuthorityKey(ROOT, o))); out.push(o); } catch { /* absent */ } }
  return out;
};

test("authority-execute: the bound evidence, the sweeper, then every marker; counts and hashes only", async () => {
  const w = await world();
  const s = await authorityExecute(w.deps);
  expect(s.status).toBe("authority_written");
  expect(s.markers).toEqual({ written: 2, conflict: 0, not_written: 0 });
  expect(s.per_org.map((p) => p.sweeper)).toEqual(["ok", "ok"]);
  expect((w.calls[0] as unknown[])[1]).toEqual({ bound: baseline() }); // the evidence is bound to the catch-up's baseline
  expect(await markers(w.store)).toEqual(ORGS);
  expect(countsOnly(s, 0, true)).toBeTruthy();
  for (const o of ORGS) expect(JSON.stringify(s)).not.toContain(o);
  // A rerun (a later clock, as any real rerun has) finds both markers: conflicts, nothing overwritten, a failure.
  const again = await authorityExecute({ ...w.deps, now: () => NOW + 1000 });
  expect([again.status, again.refused_code]).toEqual(["authority_incomplete", "marker_conflict"]);
  expect(again.markers).toEqual({ written: 0, conflict: 2, not_written: 0 });
});

test("authority-execute refusals: nothing is written unless the evidence passed and every org's sweeper is live", async () => {
  const cases: Array<[Parameters<typeof world>[0], string]> = [
    [{ report: evidenceReport("authority_evidence_unbound") }, "evidence_not_corroborated"],
    [{ report: evidenceReport("authority_evidence_refused") }, "evidence_not_corroborated"],
    [{ spec: null }, "sweeper_spec_missing"],
    [{ spec: spec({ suspended: true }) }, "sweeper_suspended"],
    [{ spec: spec({ bucket: "elsewhere" }) }, "sweeper_home_mismatch"],
    [{ spec: spec({ observedAtMs: NOW - 2 * 3600_000 }) }, "sweeper_spec_stale"],
    [{ spec: spec({ imageDigests: [`sha256:${"c".repeat(64)}`] }) }, "sweeper_mismatch"],
    [{ passes: { [ORG2]: 0 } }, "sweeper_missing"],
    [{ passes: { [ORG2]: NOW - BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS - 60_000 } }, "sweeper_stale"],
  ];
  for (const [opts, want] of cases) {
    const w = await world(opts);
    const s = await authorityExecute(w.deps);
    expect([s.status, s.refused_code]).toEqual(["authority_refused", want]);
    expect(s.markers.written).toBe(0);
    expect(await markers(w.store)).toEqual([]);
  }
  // An invalid spec document refuses too.
  const w = await world({ spec: null });
  await putJson(w.store, rearmServingSpecKey(ROOT), { ...spec(), extra: true });
  expect((await authorityExecute(w.deps)).refused_code).toBe("sweeper_spec_invalid");
});

test("authority-execute: corroboration is not authority; without an enforced writer fence nothing is written", async () => {
  const w = await world();
  const s = await authorityExecute({ ...w.deps, writerFence: undefined });
  expect([s.status, s.refused_code]).toEqual(["authority_refused", "writer_fence_unavailable"]);
  expect(await markers(w.store)).toEqual([]);
  // The fence is asked per org, right before its write; a refusal (or a throw) stops there.
  const asked: string[] = [];
  const w2 = await world({ fence: async (org) => { asked.push(org); return org === ORG1; } });
  const s2 = await authorityExecute(w2.deps);
  expect([s2.status, s2.refused_code]).toEqual(["authority_incomplete", "writer_fence_not_held"]);
  expect(asked).toEqual(ORGS);
  expect(await markers(w2.store)).toEqual([ORG1]);
  const w3 = await world({ fence: async () => { throw new Error("boom " + ORG1); } });
  const s3 = await authorityExecute(w3.deps);
  expect([s3.status, s3.refused_code]).toEqual(["authority_refused", "writer_fence_not_held"]);
  expect(JSON.stringify(s3)).not.toContain(ORG1);
  // The org's own verdict must be ok in the report.
  const r = { ...evidenceReport("authority_evidence_corroborated"), finishedAtMs: NOW - 1000 };
  r.orgs[1] = { ...r.orgs[1]!, ok: false };
  const w4 = await world({ report: r });
  expect((await authorityExecute(w4.deps)).markers.written).toBe(1);
});

test("authority-execute: the report's age is re-checked before EACH write; a lapse stops the writes there", async () => {
  // The clock jumps past the 5-minute handoff after the first marker.
  let t = NOW, writes = 0;
  const w = await world({ clock: () => t });
  const put = w.store.put.bind(w.store);
  w.store.put = (async (...a: Parameters<ObjectStore["put"]>) => {
    const r = await put(...a);
    if (String(a[0]).includes("/fence-authority/")) { writes++; t = NOW + 6 * 60_000; }
    return r;
  }) as ObjectStore["put"];
  const s = await authorityExecute(w.deps);
  expect([s.status, s.refused_code]).toEqual(["authority_incomplete", "marker_handoff_lapsed"]);
  expect(writes).toBe(1);
  expect(s.markers).toEqual({ written: 1, conflict: 0, not_written: 1 });
  // A conflict on one org: the other is still written, the run is incomplete.
  const w2 = await world();
  await putJson(w2.store, orgFenceAuthorityKey(ROOT, ORG2), { junk: true });
  const s2 = await authorityExecute(w2.deps);
  expect([s2.status, s2.refused_code]).toEqual(["authority_incomplete", "marker_conflict"]);
  expect(s2.markers).toEqual({ written: 1, conflict: 1, not_written: 0 });
});

// ---- the tool ----

test("the tool's argv and stdin are strict; SQL runs each statement in its own READ ONLY transaction", async () => {
  expect(parseAuthorityArgs(["evidence", "--org", ORG1])).toEqual({ command: "evidence", orgs: [ORG1] });
  for (const bad of [["evidence"], ["evidence", "--org", "org-a"], ["evidence", "--org", ORG1, "--bucket", "b"], ["reshard-verify", "--org", ORG1],
    ["execute", "--bucket", "bb", "--region", "us-east-1", "--org", ORG1], ["delete", "--org", ORG1], ["evidence", "--org", ORG1, "--org", ORG1], ["evidence", "--org", ORG1, "x"]])
    expect(codeSync(() => parseAuthorityArgs(bad))).not.toBeNull();
  expect(authorityStdin(JSON.stringify({ dbUrl: "postgresql://x", cloudflareToken: TOKEN }), "evidence").cloudflareToken).toBe(TOKEN);
  for (const [line, cmd] of [[JSON.stringify({ dbUrl: "postgresql://x" }), "evidence"], [JSON.stringify({ dbUrl: "postgresql://x", cloudflareToken: "x" }), "evidence"],
    [JSON.stringify({ dbUrl: "postgresql://x", cloudflareToken: TOKEN, extra: 1 }), "evidence"], [JSON.stringify({ dbUrl: "postgresql://x", cloudflareToken: TOKEN }), "execute"],
    ["nope", "evidence"]] as const)
    expect(codeSync(() => authorityStdin(line, cmd))).not.toBeNull();
  const seen: string[] = [];
  const sql = readOnlySql({ query: async (text) => { seen.push(text.trim().split("\n")[0]!); return { rows: [{ ok: 1 }] }; } });
  await Promise.all([sql("SELECT 1", []), sql("SELECT 2", [])]);
  expect(seen).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SET LOCAL statement_timeout = 600000", "SELECT 1", "ROLLBACK",
    "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SET LOCAL statement_timeout = 600000", "SELECT 2", "ROLLBACK"]);
});

test("the Cloudflare transport: fixed origin and account, bearer token, the whole body back; a bad path refuses", async () => {
  const calls: Array<[string, RequestInit | undefined]> = [];
  const fetcher = (async (url: string, init?: RequestInit) => { calls.push([url, init]); return new Response(JSON.stringify({ success: true, result: [1], result_info: { page: 1 } })); }) as unknown as typeof fetch;
  const cf = cloudflareGet("f".repeat(32), TOKEN, fetcher);
  expect(await cf("/queues?page=1&per_page=500")).toEqual({ success: true, result: [1], result_info: { page: 1 } });
  expect(calls[0]![0]).toBe(`https://api.cloudflare.com/client/v4/accounts/${"f".repeat(32)}/queues?page=1&per_page=500`);
  expect((calls[0]![1]!.headers as Record<string, string>).Authorization).toBe(`Bearer ${TOKEN}`);
  expect(await code(() => cf("//evil.example/x"))).toBe("cloudflare_path");
  expect(await code(() => cf("/../x"))).toBe("cloudflare_path");
  expect(codeSync(() => cloudflareGet("acct", TOKEN, fetcher))).toBe("cloudflare_config");
  expect(codeSync(() => cloudflareGet("f".repeat(32), "short", fetcher))).toBe("cloudflare_config");
});

void AUTHORITY_SQL; void AUTHORITY_STATS_SQL; void orgHashes;

// ---- the catch-up's baseline, inside the backfill's input snapshot ----

test("a catch-up with AUTHORITY runs authorityBaseline INSIDE the input fingerprint's REPEATABLE READ READ ONLY transaction", async () => {
  const { backfillUploadFences } = await import("../upload-fence-backfill.ts");
  const log: string[] = [];
  const authorityRow = { visible_tables: "3", tables: "3", batches_claimed_since: "0", batches_committed_since: "0", batches_pending: "0", batches_new_since_base: "0",
    revisions_updated_since: "0", revisions_count: "120", revisions_new_since_base: "0", revisions_digest: "h" + "ab".repeat(16), ownership_count: "40",
    ownership_new_since_base: "0", ownership_digest: "h" + "cd".repeat(16), snapshot: "1000:1005:1001,1003", isolation: "repeatable read", read_only: "on", read_at_ms: String(SINCE) };
  const statsRow = { tables: "1", batches_deleted: "7", stats_reset_ms: "0", started_ms: String(SINCE - 86_400_000), track_counts: "on" };
  let stop = false;
  const sql = { query: async (text: string) => {
    const head = text === AUTHORITY_SQL ? "AUTHORITY_SQL" : text === AUTHORITY_STATS_SQL ? "AUTHORITY_STATS_SQL" : text.trim().split(/\s+/u).slice(0, 2).join(" ");
    if (stop) throw new Error("after_snapshot");
    log.push(head);
    if (head === "ROLLBACK") stop = true; // the first transaction (the fingerprint's) is all this test needs
    if (text.startsWith("SHOW transaction_read_only")) return { rows: [{ transaction_read_only: "on" }] };
    if (text === AUTHORITY_SQL) return { rows: [authorityRow] };
    if (text === AUTHORITY_STATS_SQL) return { rows: [statsRow] };
    return { rows: [{ n: "0", max_ms: "0", h: "0", rev_sum: "0" }] };
  } };
  const opts = { sql, store: fresh(), keyRoot: ROOT, dryRun: true, orgs: ORGS, revisionsSinceMs: SINCE, authorityBaseline: { orgs: ORGS, evidence: evidenceReport() } };
  expect(await code(() => backfillUploadFences(opts))).not.toBeNull(); // stopped by the fake after the snapshot
  const begin = log.indexOf("BEGIN ISOLATION"), end = log.indexOf("ROLLBACK");
  expect(begin).toBe(0);
  const inTx = log.slice(begin, end);
  // Both orgs' baseline reads (and the deletion counters), after the fingerprint reads, before the ROLLBACK.
  expect(inTx.filter((l) => l === "AUTHORITY_SQL").length).toBe(2);
  expect(inTx.filter((l) => l === "AUTHORITY_STATS_SQL").length).toBe(2);
  expect(inTx.indexOf("AUTHORITY_SQL")).toBeGreaterThan(inTx.lastIndexOf("SELECT count(*)::bigint"));
  // A real authorityBaseline (the same fake rows) fits the receipt schema exactly.
  const { authorityBaseline } = await import("../authority-evidence.ts");
  const real = await authorityBaseline(ORGS, evidenceReport(), async (text) => (text === AUTHORITY_SQL ? [authorityRow] : [statsRow]));
  expect(real.status).toBe("authority_baseline_ready");
  expect(authorityReceiptSummary("catchup", { ...catchupOk(toReceiptShape(real)) }).ok).toBe(true);
  // Refused before any read: no catch-up (no t), an org list other than the run's, or an unscoped run.
  for (const bad of [{ ...opts, revisionsSinceMs: undefined }, { ...opts, authorityBaseline: { orgs: [ORG1], evidence: evidenceReport() } },
    { ...opts, orgs: undefined }])
    expect(await code(() => backfillUploadFences(bad as never))).toMatch(/backfill_authority_baseline_invalid$/u);
});

test("the backfill CLI: --authority-baseline only on a scoped catch-up; the evidence comes on stdin beside the URL", async () => {
  const { parseBackfillArgs, runBackfill } = await import("../../../scripts/upload-fence-backfill.ts");
  const base = ["--dry-run", "--bucket", "transcripts-bucket", "--org", ORG1, "--section", "revisions", "--revisions-since-ms", String(SINCE)];
  expect(parseBackfillArgs([...base, "--authority-baseline"]).authorityBaseline).toBe(true);
  expect(parseBackfillArgs(base).authorityBaseline).toBeUndefined();
  expect(codeSync(() => parseBackfillArgs(["--dry-run", "--bucket", "transcripts-bucket", "--org", ORG1, "--authority-baseline"]))).not.toBeNull();
  expect(codeSync(() => parseBackfillArgs(["--dry-run", "--bucket", "transcripts-bucket", "--revisions-since-ms", String(SINCE), "--authority-baseline"]))).not.toBeNull();
  const deps = { sql: { query: async () => ({ rows: [] }) }, env: {}, credentials: { accessKeyId: "a", secretAccessKey: "b" }, ledgerBucket: null };
  expect((await code(() => runBackfill(parseBackfillArgs([...base, "--authority-baseline"]), deps)))).toMatch(/backfill_authority_baseline_invalid$/u);
  expect((await code(() => runBackfill(parseBackfillArgs(base), { ...deps, authorityEvidence: {} })))).toMatch(/backfill_authority_baseline_invalid$/u);
});

// ---- no message-shaped string reaches a logged receipt ----

const LEAK = "synthetic_private_value";

test("reshard-dry: a store error's code-shaped message never becomes a refusal code", async () => {
  const store = fresh();
  const failing = { ...store, list: async () => { throw new Error(LEAK); }, getObject: store.getObject.bind(store), put: store.put.bind(store) } as unknown as ObjectStore;
  const s = await reshardVerifyOrgs(failing, ROOT, ORGS);
  expect(s.per_org.map((p) => p.refused)).toEqual(["reshard_failed", "reshard_failed"]);
  expect(JSON.stringify(s)).not.toContain(LEAK);
  expect(authorityReceiptSummary("reshard", s).ok).toBe(true);
});

test("the receipt schemas: exact keys, lists, hash positions and closed codes per mode; refusals through a closed map", () => {
  const R = { refused: "backlog_ops_receipt_schema" };
  const good: Array<[AuthorityReceiptKind, unknown]> = [["reshard", reshardSummary()], ["authority_dry", { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }],
    ["catchup", catchupOk()], ["catchup", { ...catchupOk(), mode: "dry_run" }]];
  for (const [k, v] of good) expect(authorityReceiptSummary(k, JSON.parse(JSON.stringify(v)))).toEqual({ summary: JSON.parse(JSON.stringify(v)), ok: true });
  const bad: Array<[AuthorityReceiptKind, (v: any) => void, unknown]> = [
    // An unexpected key anywhere, a nested object or list where a count belongs, a hash in an unexpected field.
    ["reshard", (v) => { v.extra = 1; }, reshardSummary()],
    ["reshard", (v) => { v.per_org[0].nested = { x: 1 }; }, reshardSummary()],
    ["reshard", (v) => { v.per_org[0].listed = [1]; }, reshardSummary()],
    ["reshard", (v) => { v.tool = "h_" + "a".repeat(32); }, reshardSummary()],
    ["reshard", (v) => { v.per_org[0].refused = LEAK; }, reshardSummary()],
    ["reshard", (v) => { v.per_org[0].org_hash = LEAK; }, reshardSummary()],
    ["authority_dry", (v) => { v.evidence.status = LEAK; }, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }],
    ["authority_dry", (v) => { v.evidence.orgs[0].refusals = [LEAK]; }, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }],
    ["authority_dry", (v) => { v.evidence.orgs[0].stop.extra = [[1]]; }, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }],
    ["authority_dry", (v) => { v.evidence.orgs[0].sql_quiet.fingerprint.revisions_digest = "h_" + "a".repeat(32); }, { mode: "dry_run", evidence: toReceiptShape(evidenceReport()) }],
    ["catchup", (v) => { v.authority_baseline.orgs[0].refusals = [LEAK]; }, catchupOk()],
    ["catchup", (v) => { v.fences[LEAK] = 1; }, catchupOk()],
    ["catchup", (v) => { delete v.authority_baseline; }, catchupOk()],
    ["catchup", (v) => { v.sections.revisions = LEAK; }, catchupOk()],
  ];
  for (const [k, mutate, base] of bad) {
    const v = JSON.parse(JSON.stringify(base));
    mutate(v);
    expect(authorityReceiptSummary(k, v)).toEqual({ summary: R, ok: false });
  }
  // A backfill section's failed_codes keeps only known keys; others are summed under `other`.
  const fc = JSON.parse(JSON.stringify(catchupOk()));
  fc.revisions.failed_codes = { [LEAK]: 2, fence_unreadable: 1, another_code: 3 };
  const out = authorityReceiptSummary("catchup", fc);
  expect(out.ok).toBe(true);
  expect((out.summary as any).revisions.failed_codes).toEqual({ other: 5, fence_unreadable: 1 });
  expect(JSON.stringify(out)).not.toContain(LEAK);
  // Refusals: a closed map per kind, else the generic code.
  expect(authorityReceiptSummary("catchup", { refused: LEAK })).toEqual({ summary: { refused: "backfill_refused" }, ok: false });
  expect(authorityReceiptSummary("catchup", { refused: "ledger_home_moved" })).toEqual({ summary: { refused: "ledger_home_moved" }, ok: false });
  expect(authorityReceiptSummary("reshard", { refused: LEAK })).toEqual({ summary: { refused: "internal_error" }, ok: false });
  expect(authorityReceiptSummary("authority_execute", { refused: "authority_execute_held" })).toEqual({ summary: { refused: "authority_execute_held" }, ok: false });
});

/** The credential step end to end, with fake OIDC/STS and a fake tool child; returns the logged receipt line. */
async function runStep(mode: BacklogOpsMode, stdout: string, extra: Record<string, string> = {}, childCode = 0, bound: Record<string, string> = {}) {
  const temp = mkdtempSync(join(tmpdir(), "bo-auth-"));
  cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
  for (const [kind, raw] of Object.entries(bound)) {
    mkdirSync(join(temp, "backlog-ops-bound", kind), { recursive: true });
    writeFileSync(join(temp, "backlog-ops-bound", kind, "backlog-ops-receipt.json"), raw);
  }
  const xml = `<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
    <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
  const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml)) as unknown as typeof fetch;
  const lines: string[] = [];
  const log = console.log;
  console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
  let exit: number;
  try {
    exit = await backlogOpsRun({ ...target, ...SCOPE, ...env(mode), GITHUB_SHA: sha, GITHUB_RUN_ID: "900", RUNNER_TEMP: temp,
      ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer", ...extra },
      { fetcher, runChild: async () => ({ code: childCode, stdout }) });
  } finally { console.log = log; }
  const line = lines.find((l) => l.startsWith("backlog_ops_receipt "))!;
  return { exit, receipt: JSON.parse(line.slice("backlog_ops_receipt ".length)),
    file: readFileSync(join(temp, "backlog-ops-out", "backlog-ops-receipt.json"), "utf8"), line };
}

test("the logged and persisted receipt: an off-schema authority summary is refused, the value never logged or written", async () => {
  const ok = await runStep("reshard-dry", JSON.stringify(reshardSummary()));
  expect(ok.exit).toBe(0);
  expect(ok.receipt.home).toBe(HOME);
  expect(ok.receipt.summary.per_org.length).toBe(2);
  expect(JSON.parse(ok.file)).toEqual(ok.receipt);
  for (const leaky of [
    (() => { const v = reshardSummary(); (v.per_org[0] as Record<string, unknown>).refused = LEAK; return v; })(),
    (() => { const v = reshardSummary(); (v.per_org[0] as Record<string, unknown>)[LEAK] = { x: 1 }; return v; })(),
    (() => { const v = reshardSummary(); (v as Record<string, unknown>).note = ["h_" + "a".repeat(32), [1]]; return v; })(),
  ]) {
    const bad = await runStep("reshard-dry", JSON.stringify(leaky));
    expect(bad.exit).toBe(1);
    expect(bad.receipt.summary).toEqual({ refused: "backlog_ops_receipt_schema" });
    expect(bad.line).not.toContain(LEAK);
    expect(bad.file).not.toContain(LEAK);
    expect(bad.line).not.toContain("note");
  }
  // A refusal from the tool, exit 1: only a closed code survives.
  const refused = await runStep("reshard-dry", JSON.stringify({ refused: LEAK }), {}, 1);
  expect(refused.receipt.summary).toEqual({ refused: "internal_error" });
  expect(refused.line + refused.file).not.toContain(LEAK);
});

test("FULL backfill-execute through both sinks: a code-shaped refusal or an off-schema summary never reaches the log or the file; legitimate codes survive", async () => {
  const ownerUrl = `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full`;
  const exec = { BACKLOG_OPS_MODE: "backfill-execute", BACKLOG_OPS_DRY_RUN: DRY, BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${sha} DRY ${DRY}`, SUPABASE_MIGRATION_OWNER_URL: ownerUrl };
  const full = (() => { const v = JSON.parse(JSON.stringify(catchupOk())); delete v.authority_baseline; delete v.revisions_since_ms; return v; })();
  const ok = await runStep("backfill-execute", JSON.stringify(full), exec, 0, { dry: rcpt("backfill-dry", DRY, { ...full, mode: "dry_run" }) });
  expect(ok.exit).toBe(0);
  expect(ok.receipt.summary).toEqual(full);
  expect(ok.receipt.home).toBe(HOME); // FULL attests its home
  for (const [stdout, code, want] of [
    [JSON.stringify({ refused: LEAK }), 1, { refused: "backfill_refused" }],
    [JSON.stringify({ refused: "sqlstate_57014" }), 1, { refused: "sqlstate_57014" }],
    [JSON.stringify({ refused: "ledger_home_moved" }), 1, { refused: "ledger_home_moved" }],
    [JSON.stringify({ ...full, [LEAK]: 1 }), 0, { refused: "backlog_ops_receipt_schema" }],
    [JSON.stringify({ ...full, revisions: { ...full.revisions, failed_codes: { [LEAK]: 1 } } }), 0, null],
    // Malformed raw counts never fold into a valid-looking total: a cancelling pair, fractions, an unsafe sum.
    [JSON.stringify({ ...full, revisions: { ...full.revisions, failed_codes: { unknown_a: -1, unknown_b: 1 } } }), 0, { refused: "backlog_ops_receipt_schema" }],
    [JSON.stringify({ ...full, revisions: { ...full.revisions, failed_codes: { unknown_a: 0.5, unknown_b: 0.5 } } }), 0, { refused: "backlog_ops_receipt_schema" }],
    [JSON.stringify({ ...full, ownership: { ...full.ownership, failed_codes: { unknown_a: Number.MAX_SAFE_INTEGER, unknown_b: 2 } } }), 0, { refused: "backlog_ops_receipt_schema" }],
  ] as const) {
    const r = await runStep("backfill-execute", stdout, exec, code, { dry: rcpt("backfill-dry", DRY, { ...full, mode: "dry_run" }) });
    if (want) expect(r.receipt.summary).toEqual(want);
    else expect(r.receipt.summary.revisions.failed_codes).toEqual({ other: 1 });
    expect(r.line).not.toContain(LEAK);
    expect(r.file).not.toContain(LEAK);
    if (want && "refused" in want) { expect(r.exit).toBe(1); expect(JSON.parse(r.file).summary).toEqual(want); }
  }
});
