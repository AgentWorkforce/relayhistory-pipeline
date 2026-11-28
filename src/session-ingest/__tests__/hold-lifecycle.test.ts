/**
 * The hold-lifecycle modes' wiring (no database): dispatch and the code hold, bindings and confirmations, the gate's
 * seal order, the credential step's bound receipts (the dry's approved orgs, the wave id over their epochs, the FULL
 * digest over its exact bytes and its start), the child's stdin, the exact receipt schema, and the held tool. The real
 * SQL semantics are hold-lifecycle.pglite.test.ts and hold-lifecycle.postgres.test.ts.
 */
import { afterEach, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { evidenceHash } from "../authority-evidence.ts";
import { BACKLOG_OPS_CATCHUP_MARGIN_MS, BACKLOG_OPS_HOLD_EXECUTE_ENABLED, BACKLOG_OPS_RECEIPT_CONTRACT, backlogOpsBindings, backlogOpsDispatch, backlogOpsHomeId,
  backlogOpsScope, backlogOpsTarget, backlogOpsToolArgs, type BacklogOpsMode } from "../backlog-ops.ts";
import { checkHoldDryReceipt, holdReceiptSummary, holdRun, holdSqlCode, installRequestId } from "../hold-lifecycle.ts";
import { waveId } from "../org-wave.ts";
import { backlogOpsChildSpawn, checkBoundReceipts } from "../../../scripts/backlog-ops.ts";

const cleanups: (() => void)[] = [];
afterEach(() => { for (const c of cleanups.splice(0)) c(); });
const code = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const acode = async (fn: () => Promise<unknown>) => { try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const SHA = "1".repeat(40);
const A = "5e5e5e5e-0000-4000-8000-00000000000a", B = "5e5e5e5e-0000-4000-8000-00000000000b";
const ORGS = [A, B];
const SCOPE_ENV = { BACKLOG_OPS_ORGS: ORGS.join(",") };
const SCOPE_ID = backlogOpsScope(SCOPE_ENV).id;
const H = (o: string) => evidenceHash("org", o);
const E = (n: string) => "h_" + n.repeat(32);
const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/dry", BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/exec",
};
const on = (env: Record<string, string>) => backlogOpsDispatch(env, true, false, false, false, true);
const dryRow = (o: string, hold: string, epoch: string | null, verdict = "ready") =>
  ({ org_hash: H(o), epoch_hash: epoch, hold, pending_batches: 0, canonical_open: 0, verdict, refused: verdict === "ready" ? null : "hold_pending_batch" });
const drySummary = (rows: ReturnType<typeof dryRow>[]) => ({ mode: "dry_run", tool: "hold_lifecycle", step: "dry", org_count: rows.length, wave_org_count: rows.length,
  done_count: rows.filter((r) => r.verdict === "ready").length, complete: rows.every((r) => r.verdict === "ready"), per_org: rows });

test("dispatch: hold-dry binds nothing; every execute is held in code, binds a hold-dry, and the seal and rollback name their WAVE", () => {
  expect(BACKLOG_OPS_HOLD_EXECUTE_ENABLED).toBe(false);
  for (const mode of ["hold-install-execute", "hold-seal-execute", "hold-rollback-execute"])
    expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: SHA }))).toBe("backlog_ops_hold_held");
  expect(backlogOpsBindings(on({ BACKLOG_OPS_MODE: "hold-dry", BACKLOG_OPS_SHA: SHA }))).toEqual([]);
  const inst = on({ BACKLOG_OPS_MODE: "hold-install-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "801", BACKLOG_OPS_CONFIRMATION: `HOLD-INSTALL-EXECUTE ${SHA} DRY 801` });
  expect(backlogOpsBindings(inst)).toEqual([{ run: "801", mode: "hold-dry", kind: "dry" }]);
  const W = E("7");
  const seal = on({ BACKLOG_OPS_MODE: "hold-seal-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "803", BACKLOG_OPS_FULL_BACKFILL_RUN: "802", BACKLOG_OPS_WAVE_ID: W,
    BACKLOG_OPS_CONFIRMATION: `HOLD-SEAL-EXECUTE ${SHA} DRY 803 FULL 802 WAVE ${W}` });
  expect(backlogOpsBindings(seal).map((b) => [b.kind, b.mode])).toEqual([["dry", "hold-dry"], ["full", "backfill-execute"]]);
  expect(code(() => on({ BACKLOG_OPS_MODE: "hold-seal-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "803", BACKLOG_OPS_WAVE_ID: W,
    BACKLOG_OPS_CONFIRMATION: `HOLD-SEAL-EXECUTE ${SHA} DRY 803 WAVE ${W}` }))).toBe("backlog_ops_full_backfill_run");
  expect(code(() => on({ BACKLOG_OPS_MODE: "hold-rollback-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "804", BACKLOG_OPS_CONFIRMATION: `HOLD-ROLLBACK-EXECUTE ${SHA} DRY 804` })))
    .toBe("backlog_ops_wave_id");
  expect(code(() => on({ BACKLOG_OPS_MODE: "hold-install-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "801", BACKLOG_OPS_WAVE_ID: W,
    BACKLOG_OPS_CONFIRMATION: `HOLD-INSTALL-EXECUTE ${SHA} DRY 801 WAVE ${W}` }))).toBe("backlog_ops_wave_id");
  const t = backlogOpsTarget(target), scope = backlogOpsScope(SCOPE_ENV);
  expect(backlogOpsToolArgs(seal, t, scope)).toEqual({ script: "scripts/backlog-hold.ts", needsDb: true,
    args: ["seal", "--sha", SHA, "--dry-run", "803", ...ORGS.flatMap((o) => ["--org", o])] });
  expect(code(() => backlogOpsToolArgs(seal, t, backlogOpsScope({})))).toBe("backlog_ops_scope_required");
});

test("the dry's approval: install takes READY orgs with no live hold; seal and rollback take READY held/sealed orgs at their epochs", () => {
  const raw = JSON.stringify({ summary: drySummary([dryRow(A, "absent", null), dryRow(B, "held", E("b"))]) });
  expect(checkHoldDryReceipt(raw, ORGS, "install")).toEqual([{ orgHash: H(A), epochHash: null }]);
  expect(checkHoldDryReceipt(raw, ORGS, "seal")).toEqual([{ orgHash: H(B), epochHash: E("b") }]);
  expect(code(() => checkHoldDryReceipt(JSON.stringify({ summary: drySummary([dryRow(A, "absent", null, "refused"), dryRow(B, "absent", null, "refused")]) }), ORGS, "install")))
    .toBe("backlog_ops_bound_hold_dry"); // no org ready: the dry did not succeed
  expect(code(() => checkHoldDryReceipt(raw, [A], "install"))).toBe("backlog_ops_bound_hold_dry"); // another scope
  expect(code(() => checkHoldDryReceipt(JSON.stringify({ summary: { ...drySummary([dryRow(A, "absent", null), dryRow(B, "held", E("b"))]), done_count: 1 } }), ORGS, "install")))
    .toBe("backlog_ops_bound_hold_dry"); // an aggregate is derived
});

test("credential step (seal): FULL digest over its exact bytes, its start from the gate's t plus the margin, the WAVE over the approved epochs", () => {
  const tmp = mkdtempSync(join(tmpdir(), "hold-bound-"));
  cleanups.push(() => rmSync(tmp, { recursive: true, force: true }));
  const t = backlogOpsTarget(target), home = backlogOpsHomeId({ bucket: t.bucket, region: t.region }, null);
  const rcpt = (mode: BacklogOpsMode, run: string, summary: unknown) =>
    JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode, sha: SHA, run, scope: SCOPE_ID, ledger: "none", home, exitCode: 0, summary }, null, 2);
  const zeros = (...k: string[]) => Object.fromEntries(k.map((x) => [x, 0]));
  const fullSummary = { mode: "execute", users: 1, fences: zeros("failed"), ownership: { ...zeros("failed"), failed_codes: {} }, revisions: { ...zeros("failed"), failed_codes: {} },
    sections: { fences: "complete", ownership: "complete", revisions: "complete" } };
  const files = { dry: rcpt("hold-dry", "803", drySummary([dryRow(A, "held", E("a")), dryRow(B, "held", E("b"))])), full: rcpt("backfill-execute", "802", fullSummary) };
  for (const [k, v] of Object.entries(files)) { mkdirSync(join(tmp, "backlog-ops-bound", k), { recursive: true }); writeFileSync(join(tmp, "backlog-ops-bound", k, "backlog-ops-receipt.json"), v); }
  const SINCE = 1_700_000_000_000;
  const approved = [{ orgHash: H(A), epochHash: E("a") }, { orgHash: H(B), epochHash: E("b") }];
  const W = waveId({ sha: SHA, scope: SCOPE_ID, runs: { dry: "803", full: "802" }, orgs: approved });
  const d = on({ BACKLOG_OPS_MODE: "hold-seal-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "803", BACKLOG_OPS_FULL_BACKFILL_RUN: "802", BACKLOG_OPS_WAVE_ID: W,
    BACKLOG_OPS_CONFIRMATION: `HOLD-SEAL-EXECUTE ${SHA} DRY 803 FULL 802 WAVE ${W}` });
  const env = { ...target, ...SCOPE_ENV, RUNNER_TEMP: tmp };
  const b = checkBoundReceipts(env, d, undefined, SINCE);
  expect(b.hold).toEqual({ approved, full: { run: "802", startedMs: SINCE + BACKLOG_OPS_CATCHUP_MARGIN_MS, evidence: createHash("sha256").update(files.full).digest("hex") } });
  expect(b.fullEvidence).toBeUndefined();
  // Another WAVE (other epochs approved) refuses.
  const W2 = E("9");
  const d2 = on({ BACKLOG_OPS_MODE: "hold-seal-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "803", BACKLOG_OPS_FULL_BACKFILL_RUN: "802", BACKLOG_OPS_WAVE_ID: W2,
    BACKLOG_OPS_CONFIRMATION: `HOLD-SEAL-EXECUTE ${SHA} DRY 803 FULL 802 WAVE ${W2}` });
  expect(code(() => checkBoundReceipts(env, d2, undefined, SINCE))).toBe("backlog_ops_bound_hold_wave");
  // The child gets the approved orgs and the FULL on stdin; no org on argv beyond --org, none in env.
  const aws = { accessKeyId: "ASIA" + "A".repeat(16), secretAccessKey: "s", sessionToken: "t" };
  const child = backlogOpsChildSpawn({ ...env, BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) }, d, aws, "postgresql://x", null, b);
  expect(JSON.parse(child.input!)).toEqual({ dbUrl: "postgresql://x", approved, full: b.hold!.full });
  expect(code(() => backlogOpsChildSpawn({ ...env, BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) }, d, aws, "postgresql://x", null, {}))).toBe("backlog_ops_authority_binding");
});

test("holdRun input: refused before any read on a bad wave, a missing FULL, or a rollback without evidence", async () => {
  let reads = 0;
  const base = { orgs: ORGS, sha: SHA, now: () => Date.now(), withSession: async () => { reads++; throw new Error("no"); } } as const;
  for (const bad of [
    { step: "install" as const, dryRun: "801" },
    { step: "install" as const, dryRun: "801", approved: [{ orgHash: H(A), epochHash: E("a") }] },
    { step: "seal" as const, dryRun: "803", approved: [{ orgHash: H(A), epochHash: E("a") }] },
    { step: "rollback" as const, dryRun: "804", approved: [{ orgHash: H(A), epochHash: E("a") }] },
    { step: "seal" as const, dryRun: "803", approved: [{ orgHash: H("dddddddd-0000-4000-8000-000000000000"), epochHash: E("a") }], full: { run: "802", startedMs: 1, evidence: "f".repeat(64) } },
    { step: "dry" as const, dryRun: "801", approved: [] },
  ]) expect(await acode(() => holdRun({ ...base, ...bad } as never))).toBe("hold_input");
  expect(await acode(() => holdRun({ ...base, step: "dry", orgs: [A, A] } as never))).toBe("wave_org_duplicate");
  expect(reads).toBe(0);
  expect(installRequestId(SHA, "801", A)).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
  expect(installRequestId(SHA, "801", A)).toBe(installRequestId(SHA, "801", A));
  expect(installRequestId(SHA, "801", A)).not.toBe(installRequestId(SHA, "805", A));
  // SQL errors map to fixed codes; nothing of their text is kept.
  expect(holdSqlCode({ code: "RH042", message: `held ${A}` }, "install")).toBe("hold_pending_batch");
  expect(holdSqlCode({ code: "55000", message: "session ingest: org erasure window FULL not between install and seal" }, "seal")).toBe("hold_full_order");
  expect(holdSqlCode({ code: "XX000", message: A }, "seal")).toBe("hold_seal_failed");
});

test("the receipt schema is exact and self-consistent; the tool's executes are held and print only fixed codes", () => {
  const ok = holdReceiptSummary(drySummary([dryRow(A, "absent", null), dryRow(B, "held", E("b"))]), { step: "dry", orgs: ORGS });
  expect(ok.ok).toBe(true);
  expect(holdReceiptSummary({ ...drySummary([dryRow(A, "absent", null), dryRow(B, "held", E("b"))]), step: "seal" }, { step: "dry", orgs: ORGS }).ok).toBe(false);
  expect(holdReceiptSummary({ ...drySummary([dryRow(A, "absent", null)]) }, { step: "dry", orgs: ORGS }).ok).toBe(false);
  expect(holdReceiptSummary(drySummary([{ ...dryRow(A, "absent", null), org_hash: A as never }, dryRow(B, "held", E("b"))]), { step: "dry", orgs: ORGS }).ok).toBe(false);
  expect(holdReceiptSummary({ refused: `x ${A}` }, { step: "seal", orgs: ORGS }).summary).toEqual({ refused: "internal_error" });
  const root = fileURLToPath(new URL("../../../", import.meta.url));
  for (const step of ["install", "seal", "rollback"]) {
    const r = spawnSync(process.execPath, [join(root, "scripts/backlog-hold.ts"), step, "--sha", SHA, "--dry-run", "801", "--org", A, "--org", B], { input: "", encoding: "utf8",
      env: { PATH: process.env.PATH ?? "" } });
    expect(r.stdout.trim()).toBe(JSON.stringify({ refused: "hold_execute_held" }));
    expect(r.stdout + r.stderr).not.toContain(A);
  }
});
