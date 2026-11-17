/**
 * Workflow entry point (database job of .github/workflows/ingest-org-admission-repair.yml) for the dormant org
 * admission repair. The org list comes from the dispatch event payload (never step env) and every id is masked in
 * the job log before anything else is printed. Mode, SHA, plan run, org list, dormant switch and confirmation shape are checked before the owner
 * credential is read; admit reads the plan artifact the workflow downloaded from the plan run. The connection is the
 * guarded schema release's verify-full owner client (releaseClient) and must prove the pinned owner identity
 * (requireReleaseOwner) before any query. Writes the plan artifact file (plan mode) and a sanitized receipt (always,
 * REFUSED on error; never an org id); errors print a fixed code (plus a table name and org index for per-org
 * refusals). Nothing here runs unless the workflow is dispatched.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { releaseClient, releaseErrorCode, requireReleaseOwner } from "../src/hoopdb/schema-release-inspect.ts";
import {
  orgAdmissionRepairDispatch,
  orgAdmissionRepairEnv,
  orgAdmissionRepairInputs,
  orgAdmissionRepairMasks,
  runOrgAdmissionRepair,
} from "../src/hoopdb/ingest-org-admission-repair-operator.ts";
import { ORG_ADMISSION_REPAIR_PLAN_FILE, ORG_ADMISSION_REPAIR_RECEIPT_FILE } from "../src/hoopdb/org-admission-repair-contract.ts";

const PLAN = resolve(ORG_ADMISSION_REPAIR_PLAN_FILE), RECEIPT = resolve(ORG_ADMISSION_REPAIR_RECEIPT_FILE);
const write = (path: string, value: unknown) => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
};
const stamp = () => ({ at: new Date().toISOString(), sourceCommit: process.env.GITHUB_SHA ?? null, credentialsPersisted: false });
/** A per-org refusal's detail: `<table> org#<i>` or `org#<i>` only (table names are schema names; never an org id). */
const DETAIL = /^(?:[a-z0-9_]{1,63} )?org#[0-9]{1,2}$/u;

async function main() {
  const env = orgAdmissionRepairEnv(process.env);
  const dispatch = orgAdmissionRepairDispatch(env);
  console.log(orgAdmissionRepairMasks(dispatch.orgs));
  let plan: unknown;
  if (dispatch.mode === "admit") {
    try {
      plan = JSON.parse(readFileSync(PLAN, "utf8"));
    } catch {
      throw Object.assign(new Error("org_admission_repair_plan"), { code: "org_admission_repair_plan" });
    }
  }
  const input = orgAdmissionRepairInputs(env, { plan });
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  await client.connect();
  try {
    await requireReleaseOwner(client);
    const result = await runOrgAdmissionRepair(client, input);
    if (result.plan) write(PLAN, result.plan);
    const receipt = { ...result.receipt, ...stamp() };
    write(RECEIPT, receipt);
    console.log(JSON.stringify(receipt));
  } finally {
    await client.end().catch(() => {});
  }
}

if (import.meta.main)
  main().catch((error) => {
    const code = releaseErrorCode(error);
    const raw = (error as { detail?: unknown } | null)?.detail;
    const detail = typeof raw === "string" && DETAIL.test(raw) ? raw : null;
    const mode = process.env.ORG_REPAIR_MODE;
    write(RECEIPT, {
      status: "ORG_ADMISSION_REPAIR_REFUSED", mode: mode === "plan" || mode === "admit" || mode === "readback" ? mode : null,
      code, detail, committed: code === "org_admission_repair_commit_unknown" ? null : false, ...stamp(),
    });
    console.error("ingest_org_admission_repair_refused " + code + (detail ? " " + detail : ""));
    process.exitCode = 1;
  });
