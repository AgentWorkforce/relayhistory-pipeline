/**
 * Existing-owner apply for the HISTORY migration track (supabase/history-migrations).
 * Mirrors the reviewed ingest owner-apply protocol (scripts/schema-release-owner-apply.ts):
 *   plan     read-only inspection on the owner session -> .96bulls/history-apply/plan.json
 *   (approval) the operator writes .96bulls/history-apply/approval.json bound to that plan
 *   apply    re-validates source/plan/approval, re-inspects (exact ledger), applies under the
 *            history lock through the owner-fenced client, then verifies readiness = pass.
 * It reuses the reviewed owner session/fence helpers and never touches the ingest track,
 * its ledger, readiness marker or generation.
 *
 *   bun scripts/history-owner-apply.ts plan  --source-sha <40hex>
 *   bun scripts/history-owner-apply.ts apply --source-sha <40hex> --plan-sha <64hex> --approval-sha <64hex>
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import { parseCliLogin } from "./schema-release-owner-operator-plan.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { SCHEMA_RELEASE as P } from "../src/hoopdb/schema-release-contract.ts";
import { OWNER_APPLY as A, ownerFencedClient, withOwnerSession } from "../src/hoopdb/owner-operator-apply.ts";
import { APPROVER } from "../stacks/aws/deploy-guard.ts";
import { historyGeneration, historyReady, inspectHistory, runHistoryMigrations } from "../src/history-db/migrate.ts";

export const HISTORY_APPLY = {
  protocol: "history-owner-apply-v1",
  execution: "ssh-sf-frame-history-apply",
  plan: ".96bulls/history-apply/plan.json",
  approval: ".96bulls/history-apply/approval.json",
  maxAgeMs: 3_600_000,
} as const;
const CLI = "/home/khaliqgant/.local/bin/supabase";
const digest = (t: string) => createHash("sha256").update(t).digest("hex");
const hex = (v: unknown, n: number) => typeof v === "string" && new RegExp(`^[0-9a-f]{${n}}$`).test(v);
export function historyRefuse(code: string): never {
  throw Object.assign(new Error(code), { code });
}
function fresh(at: unknown, now: number) {
  if (typeof at !== "string" || !Number.isFinite(Date.parse(at)) || Date.parse(at) > now + 30_000 || now - Date.parse(at) >= HISTORY_APPLY.maxAgeMs)
    historyRefuse("history_artifact_stale");
}

export function historyApplyArgs(argv: string[]) {
  const mode = argv[0];
  const get = (k: string) => { const i = argv.indexOf(k); return i > 0 ? argv[i + 1] : undefined; };
  const sourceSha = get("--source-sha"), planSha = get("--plan-sha"), approvalSha = get("--approval-sha");
  if (mode === "plan" && argv.length === 3 && hex(sourceSha, 40)) return { mode, sourceSha: sourceSha! };
  if (mode === "apply" && argv.length === 7 && hex(sourceSha, 40) && hex(planSha, 64) && hex(approvalSha, 64))
    return { mode, sourceSha: sourceSha!, planSha: planSha!, approvalSha: approvalSha! };
  return historyRefuse("history_apply_args");
}

/** Pure: the approval binds the exact plan bytes, source, generation and ledger, and is fresh. */
export function validateHistoryApproval(planText: string, approvalText: string, sourceSha: string, now = Date.now()) {
  let plan: any, approval: any;
  try { plan = JSON.parse(planText); approval = JSON.parse(approvalText); } catch { historyRefuse("history_artifact_json"); }
  if (plan?.version !== 1 || plan.kind !== HISTORY_APPLY.protocol || plan.status !== "planned_readonly_not_apply_approval" ||
      plan.project !== A.project || plan.sourceSha !== sourceSha || !hex(plan.generation, 64) || !hex(plan.ledgerSha256, 64) || !Array.isArray(plan.inventory))
    historyRefuse("history_plan_binding");
  fresh(plan.at, now);
  if (approval?.version !== 1 || approval.decision !== "approved" || approval.by !== APPROVER || approval.execution !== HISTORY_APPLY.execution ||
      typeof approval.quote !== "string" || approval.quote.trim().length < 8 || approval.project !== A.project ||
      approval.sourceSha !== sourceSha || approval.generation !== plan.generation || approval.ledgerSha256 !== plan.ledgerSha256 ||
      approval.planSha256 !== digest(planText))
    historyRefuse("history_approval_required");
  fresh(approval.at, now);
  if (Date.parse(approval.at) < Date.parse(plan.at) - 30_000) historyRefuse("history_approval_predates_plan");
  return { plan, approval };
}

async function main(argv = process.argv.slice(2)) {
  const input = historyApplyArgs(argv);
  const run = (exe: string, args: string[]) => execFileSync(exe, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30_000, maxBuffer: 65_536 });
  const recheckSource = async () => {
    if (run("git", ["rev-parse", "HEAD"]).trim() !== input.sourceSha) historyRefuse("history_source_sha");
    const dirty = [...run("git", ["diff", "HEAD", "--name-only", "-z"]).split("\0"), ...run("git", ["ls-files", "--others", "--exclude-standard", "-z"]).split("\0")]
      .filter((f) => f && f !== "supabase/.temp/cli-latest");
    if (dirty.length) historyRefuse("history_dirty_checkout");
  };
  await recheckSource();
  let planText = "", approvalText = "";
  if (input.mode === "apply") {
    planText = await readFile(HISTORY_APPLY.plan, "utf8");
    approvalText = await readFile(HISTORY_APPLY.approval, "utf8");
    if (digest(planText) !== input.planSha || digest(approvalText) !== input.approvalSha) historyRefuse("history_artifact_digest");
    validateHistoryApproval(planText, approvalText, input.sourceSha);
  }
  if (run(CLI, ["--version"]).trim() !== "2.122.0") historyRefuse("history_cli_version");
  let raw = run(CLI, ["db", "dump", "--linked", "--project-ref", A.project, "--dry-run"]);
  const login = parseCliLogin(raw);
  raw = "";
  const client = new pg.Client({
    host: P.host, port: 5432, database: A.database, user: A.login + "." + A.project, password: login.password,
    ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: P.host },
    connectionTimeoutMillis: 10_000, query_timeout: 300_000,
    options: "-c statement_timeout=300000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=60000",
    application_name: "96bulls-history-" + input.mode,
  });
  login.password = "";
  client.on("error", () => {});
  return withOwnerSession(client, async (pid) => {
    const fenced = ownerFencedClient(client, pid);
    if (input.mode === "plan") {
      await fenced.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      let report;
      try { report = await inspectHistory(fenced); } finally { await client.query("ROLLBACK"); }
      const body = JSON.stringify({ version: 1, kind: HISTORY_APPLY.protocol, status: "planned_readonly_not_apply_approval",
        at: new Date().toISOString(), project: A.project, sourceSha: input.sourceSha, ...report }, null, 2) + "\n";
      await mkdir(".96bulls/history-apply", { recursive: true });
      await writeFile(HISTORY_APPLY.plan, body, { mode: 0o600 });
      return { status: "history_plan_readonly_not_approval", planSha256: digest(body), generation: report.generation,
        ledgerSha256: report.ledgerSha256, pending: report.inventory.filter((s: any) => s.pending).map((s: any) => s.name) };
    }
    const { plan } = validateHistoryApproval(planText, approvalText, input.sourceSha);
    if ((await historyGeneration()) !== plan.generation) historyRefuse("history_generation_changed");
    const result = await runHistoryMigrations(fenced, {
      expectedLedgerSha256: plan.ledgerSha256,
      beforeWrite: async () => { await recheckSource(); validateHistoryApproval(planText, approvalText, input.sourceSha); },
    });
    if (!(await historyReady(fenced, plan.generation))) historyRefuse("history_apply_verification");
    return { status: "history_apply_verified", ...result };
  });
}

if (import.meta.main)
  main().then((r) => console.log(JSON.stringify(r))).catch((error) => {
    const code = typeof error?.code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(error.code) ? error.code : "history_apply_failed";
    console.log(JSON.stringify({ status: "refused", code }));
    process.exitCode = 1;
  });
