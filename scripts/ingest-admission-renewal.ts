/**
 * Workflow entry point (database job of .github/workflows/ingest-admission-renewal.yml) for the dormant admission
 * renewal. Mode, SHA, plan run, dormant switch and confirmation shape are checked before the owner credential is
 * read; generation() of this checkout (the dispatched SHA) is the expected generation; renew reads the plan
 * artifact the workflow downloaded from the plan run. The connection is the guarded schema release's verify-full
 * owner client (releaseClient) and must prove the pinned owner identity (requireReleaseOwner) before any query.
 * Writes the plan artifact file (plan mode) and a sanitized receipt (always, REFUSED on error); errors print a fixed
 * code only. Nothing here runs unless the workflow is dispatched.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { generation } from "../src/hoopdb/migrate.ts";
import { releaseClient, releaseErrorCode, requireReleaseOwner } from "../src/hoopdb/schema-release-inspect.ts";
import {
  admissionRenewalDispatch,
  admissionRenewalInputs,
  runAdmissionRenewal,
} from "../src/hoopdb/ingest-admission-renewal-operator.ts";
import { ADMISSION_RENEWAL_PLAN_FILE, ADMISSION_RENEWAL_RECEIPT_FILE } from "../src/hoopdb/admission-renewal-contract.ts";

const PLAN = resolve(ADMISSION_RENEWAL_PLAN_FILE), RECEIPT = resolve(ADMISSION_RENEWAL_RECEIPT_FILE);
const write = (path: string, value: unknown) => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
};
const stamp = () => ({ at: new Date().toISOString(), sourceCommit: process.env.GITHUB_SHA ?? null, credentialsPersisted: false });

async function main() {
  const dispatch = admissionRenewalDispatch(process.env);
  let plan: unknown;
  if (dispatch.mode === "renew") {
    try {
      plan = JSON.parse(readFileSync(PLAN, "utf8"));
    } catch {
      throw Object.assign(new Error("admission_renewal_plan"), { code: "admission_renewal_plan" });
    }
  }
  const input = admissionRenewalInputs(process.env, { expectedGeneration: await generation(), plan });
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  await client.connect();
  try {
    await requireReleaseOwner(client);
    const result = await runAdmissionRenewal(client, input);
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
    write(RECEIPT, {
      status: "ADMISSION_RENEWAL_REFUSED", mode: process.env.RENEWAL_MODE === "plan" || process.env.RENEWAL_MODE === "renew" ||
        process.env.RENEWAL_MODE === "readback" ? process.env.RENEWAL_MODE : null,
      code, committed: code === "admission_renewal_commit_unknown" ? null : false, ...stamp(),
    });
    console.error("ingest_admission_renewal_refused " + code);
    process.exitCode = 1;
  });
