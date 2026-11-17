/**
 * The operate step of .github/workflows/ledger-log-pilot.yml (dormant): one production ledger-log stream, through the
 * same operations as scripts/ledger-log-stream.ts (src/session-ingest/ledger-log-stream-ops.ts), with the production
 * context. Runs only after scripts/ledger-log-pilot-github.ts --approved passed in the protected environment.
 *
 *   status        READ ONLY (read role).
 *   measure       READ ONLY (read role): batch arrival rate per origin of the pilot stream's user (window_seconds).
 *   plan-switch   READ ONLY (read role): the preflight and the confirmation, published as the plan artifact.
 *   plan-close    the same for close.
 *   switch/close  write role, dormant (LEDGER_LOG_PRODUCTION_ENABLED): the plan run's artifact must hold exactly the
 *                 typed confirmation; the operation rebuilds it from a fresh preflight before it writes anything.
 *
 * The stream is the environment SECRET LEDGER_LOG_PILOT_STREAM (masked before anything is printed); bucket, region,
 * key root, KMS key and roles are the environment's protected variables. Role credentials come from GitHub OIDC +
 * STS (scripts/backlog-ops.ts assumeRole), in memory only. Prints and writes only digests, counts and fixed codes.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { assertLedgerHomeHere, LedgerHomeRefused } from "../src/session-ingest/ledger-store.ts";
import { closeLogStream, measureArrivalRate, streamStatus, switchToLogMode } from "../src/session-ingest/ledger-log-stream-ops.ts";
import {
  isPilotApply, ledgerLogPilotDispatch, ledgerLogPilotTarget, pilotAction, pilotStream, type ProductionContext,
} from "../src/session-ingest/ledger-log-stream-production.ts";
import { assumeRole } from "./backlog-ops.ts";

export const PILOT_PLAN_FILE = "ledger-log-pilot-plan.json";
export const PILOT_RECEIPT_FILE = "ledger-log-pilot-receipt.json";

/** GitHub workflow commands that mask the stream's ids in the rest of the job's log (defence in depth). */
export const pilotMasks = (ids: readonly string[]) => ids.map((v) => `::add-mask::${v}`).join("\n");

/** The confirmation the plan artifact recorded; an apply's input must equal it byte for byte. */
export function planArtifactConfirmation(raw: string): string {
  let v: unknown;
  try { v = JSON.parse(raw); } catch { v = undefined; }
  const c = (v as { production?: { confirmation?: unknown } } | undefined)?.production?.confirmation;
  if (typeof c !== "string" || !c.startsWith("LEDGER-LOG-")) throw Object.assign(new Error("ledger_log_pilot_plan_artifact"), { code: "ledger_log_pilot_plan_artifact" });
  return c;
}

async function main(env: Record<string, string | undefined>): Promise<number> {
  const d = ledgerLogPilotDispatch(env);
  const stream = pilotStream(env.LEDGER_LOG_PILOT_STREAM);
  console.log(pilotMasks([stream.tenant.orgId, stream.tenant.workspaceId, stream.tenant.userId, stream.originId]));
  const t = ledgerLogPilotTarget(env);
  console.log(pilotMasks([t.bucket]));
  const out = env.LEDGER_LOG_PILOT_OUT ?? "";
  if (!out) throw Object.assign(new Error("ledger_log_pilot_out"), { code: "ledger_log_pilot_out" });
  mkdirSync(out, { recursive: true, mode: 0o700 });
  const apply = isPilotApply(d.mode), action = pilotAction(d.mode);
  if (apply) {
    // The plan run's artifact (downloaded by the workflow) must hold exactly the typed confirmation.
    const planPath = join(env.LEDGER_LOG_PILOT_PLAN_DIR ?? "", PILOT_PLAN_FILE);
    if (!existsSync(planPath) || planArtifactConfirmation(readFileSync(planPath, "utf8")) !== d.confirmation)
      throw Object.assign(new Error("ledger_log_pilot_plan_artifact"), { code: "ledger_log_pilot_plan_artifact" });
  }
  const creds = await assumeRole(env, apply ? t.writeRoleArn : t.readRoleArn, t.region, `ledger-log-pilot-${d.mode}-${env.GITHUB_RUN_ID}`.slice(0, 64));
  const store = createS3Store({ bucket: t.bucket, region: t.region,
    credentials: { accessKeyId: creds.accessKeyId, secretAccessKey: creds.secretAccessKey, sessionToken: creds.sessionToken },
    ...(apply ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: t.kmsKeyArn } : {}) }) as unknown as ObjectStore;
  try { await assertLedgerHomeHere(store, t.keyRoot, t.bucket); } catch (error) {
    throw Object.assign(new Error("ledger_home"), { code: error instanceof LedgerHomeRefused ? error.code : "ledger_home_unavailable" });
  }
  const production: ProductionContext = { bucket: t.bucket, codeSha: d.sha,
    planRef: apply ? d.planRun! : env.GITHUB_RUN_ID!, ...(apply ? { confirmation: d.confirmation } : {}) };
  const { tenant, originId } = stream;
  const result = d.mode === "status" ? await streamStatus(store, t.keyRoot, tenant, originId)
    : d.mode === "measure" ? await measureArrivalRate(store, t.keyRoot, tenant, undefined, { windowMs: d.windowSeconds * 1000 })
    : action === "switch" ? await switchToLogMode(store, t.keyRoot, tenant, originId, { apply, writer: `operator_${crypto.randomUUID()}`, production })
    : await closeLogStream(store, t.keyRoot, tenant, originId, { apply, production });
  const receipt = { mode: d.mode, sha: d.sha, run: env.GITHUB_RUN_ID, result };
  writeFileSync(join(out, PILOT_RECEIPT_FILE), JSON.stringify(receipt, null, 2) + "\n", { mode: 0o600 });
  const refused = "refused" in result ? result.refused : undefined;
  if (d.mode.startsWith("plan-") && !refused) writeFileSync(join(out, PILOT_PLAN_FILE), JSON.stringify(result, null, 2) + "\n", { mode: 0o600 });
  console.log(JSON.stringify(receipt));
  return refused ? 1 : 0;
}

if (import.meta.main)
  main(process.env)
    .then((code) => { process.exitCode = code; })
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("ledger_log_pilot_refused " + (typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
