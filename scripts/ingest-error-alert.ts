/**
 * Workflow entry point (alerting job of .github/workflows/ingest-error-alert.yml) for the ingest DB-error alert
 * (src/session-ingest/ingest-error-alert.ts; runbooks/history-620-operations.md "Ingest DB-error alert").
 *
 *   plan   read-only: checks the webhook destination and diffs the live policy (by name) against the body in
 *          this commit; prints the action, drifted field names, the body digest and the exact apply confirmation.
 *   apply  re-plans, requires `APPLY-INGEST-DB-ERROR-ALERT <sha> BODY <digest>` for that same body, creates or
 *          updates the one policy, then reads it back and refuses any drift.
 *
 * Inputs: INGEST_ALERT_MODE / INGEST_ALERT_SHA / INGEST_ALERT_CONFIRMATION (dispatch), CLOUDFLARE_ACCOUNT_ID (must
 * equal the tracked config/cloudflare/private-ingest-production.json account), INGEST_ALERT_WEBHOOK_ID (environment
 * variable: the Notifications destination id) and CLOUDFLARE_ALERTING_API_TOKEN (environment secret). Writes a
 * sanitized receipt (no ids, URL or token); errors print a fixed code only. Nothing runs unless dispatched.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import {
  applyIngestErrorAlert,
  cloudflareAlertingApi,
  ingestErrorAlertAccount,
  ingestErrorAlertConfirmation,
  ingestErrorAlertDispatch,
  planIngestErrorAlert,
  refuseIngestErrorAlert,
} from "../src/session-ingest/ingest-error-alert.ts";

const PROFILE = resolve("config/cloudflare/private-ingest-production.json");
const RECEIPT = resolve("release-receipt/ingest-error-alert.json");
const write = (value: unknown) => {
  mkdirSync(dirname(RECEIPT), { recursive: true });
  writeFileSync(RECEIPT, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
};
const stamp = () => ({ at: new Date().toISOString(), sourceCommit: process.env.GITHUB_SHA ?? null });

export async function runIngestErrorAlert(env: Record<string, string | undefined>, fetcher: typeof fetch = fetch) {
  const dispatch = ingestErrorAlertDispatch(env);
  // The checkout is the dispatched commit: the body applied is the body reviewed at that sha.
  if (env.GITHUB_SHA !== dispatch.sha) refuseIngestErrorAlert("ingest_error_alert_sha_mismatch");
  const accountId = ingestErrorAlertAccount(JSON.parse(readFileSync(PROFILE, "utf8")), env);
  const webhookId = env.INGEST_ALERT_WEBHOOK_ID ?? "";
  const api = cloudflareAlertingApi(env.CLOUDFLARE_ALERTING_API_TOKEN ?? "", accountId, dispatch.mode, fetcher);
  if (dispatch.mode === "plan") {
    const plan = await planIngestErrorAlert(api, accountId, webhookId);
    return { status: "INGEST_ERROR_ALERT_PLANNED", mode: "plan", action: plan.action, drift: plan.drift, digest: plan.digest,
      confirmation: plan.action === "noop" ? null : ingestErrorAlertConfirmation(dispatch.sha, plan.digest) };
  }
  const applied = await applyIngestErrorAlert(api, { accountId, webhookId, sha: dispatch.sha, confirmation: dispatch.confirmation });
  return { status: "INGEST_ERROR_ALERT_APPLIED", mode: "apply", action: applied.action, drift: applied.drift, digest: applied.digest,
    readback: applied.readback };
}

if (import.meta.main)
  runIngestErrorAlert(process.env)
    .then((result) => {
      const receipt = { ...result, ...stamp() };
      write(receipt);
      console.log(JSON.stringify(receipt));
    })
    .catch((error) => {
      const raw = (error as { code?: unknown })?.code;
      const code = typeof raw === "string" && /^[a-z0-9_]{1,64}$/u.test(raw) ? raw : "failed";
      write({ status: "INGEST_ERROR_ALERT_REFUSED", code, ...stamp() });
      console.error("ingest_error_alert_refused " + code);
      process.exitCode = 1;
    });
