/** CI adapter for the private-ingest retained-plan operator. No provider mutation here. */
import { createHash } from "node:crypto";
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { assertCloudflareStage, type CloudflareStageInputs } from "../stacks/cloudflare/alchemy.cloudflare.run.ts";
import { inputsDigest } from "../stacks/aws/deploy-guard.ts";
import { EXECUTION_EXCEPTION } from "./private-ingest-operator.ts";
import {
  PRIVATE_INGEST_RELEASE as P,
  privateIngestActiveVersion,
  privateIngestRefuse,
  privateIngestReleasePhase,
  privateIngestReleaseInputs,
  privateIngestTransitionReadback,
  privateIngestVersionReadback,
  type CloudflareWorkerVersion,
} from "../src/session-ingest/private-ingest-release.ts";
import {
  REVIEWED_CUSTODY_SHADOW_MATCH,
  custodyEvidenceText,
  requireCustodyEvidenceText,
  requireCustodyShadowMatch,
  requireRelayhistoryWitness,
} from "../src/session-ingest/private-ingest-custody-evidence.ts";

const SOURCE = resolve("config/cloudflare/private-ingest-production.json");
const RUNTIME = resolve(".96bulls/cf-sessions-prod/cloudflare-inputs.json");
const PREFIX = resolve("docs/plans/cf-sessions-prod");
const CUSTODY_EVIDENCE = PREFIX + ".custody-evidence.json";
const RECEIPT = resolve("release-receipt/private-ingest-readback.json");
// Preflight runs before the retained operator's clean-source gate. Keep its diagnostic receipt in
// the already-ignored, mode-0600 runtime tree so it cannot make the checkout dirty.
const PREFLIGHT = resolve(".96bulls/private-ingest-release/preflight.json");
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");

function verifyCustodyEvidenceFile() {
  let text: string;
  try { text = readFileSync(CUSTODY_EVIDENCE, "utf8"); }
  catch { return privateIngestRefuse("private_ingest_custody_evidence_missing"); }
  requireCustodyEvidenceText(text);
}

function trackedInput(): { text: string; input: CloudflareStageInputs } {
  const tracked = JSON.parse(readFileSync(SOURCE, "utf8")) as CloudflareStageInputs;
  if (tracked.approval !== undefined) privateIngestRefuse("private_ingest_tracked_approval");
  if (Object.hasOwn(tracked.sessionIngest as object, "kmsKeyArn"))
    privateIngestRefuse("private_ingest_tracked_target");
  const kmsKeyArn = process.env.PRIVATE_INGEST_KMS_KEY_ARN ?? "";
  const input = { ...tracked, sessionIngest: { ...tracked.sessionIngest, kmsKeyArn } };
  const text = JSON.stringify(input, null, 2) + "\n";
  assertCloudflareStage("cf-sessions-prod", "plan", input, {
    CLOUDFLARE_ACCOUNT_ID: P.accountId,
  });
  privateIngestReleasePhase(input);
  return { text, input };
}

function output(values: Record<string, string>) {
  if (process.env.GITHUB_OUTPUT)
    appendFileSync(process.env.GITHUB_OUTPUT, Object.entries(values).map(([key, value]) => `${key}=${value}\n`).join(""));
  console.log(JSON.stringify(values));
}

export function stagePrivateIngestInput() {
  const { text, input } = trackedInput();
  mkdirSync(dirname(RUNTIME), { recursive: true });
  writeFileSync(RUNTIME, text, { mode: 0o600 });
  const evidenceText = custodyEvidenceText();
  mkdirSync(dirname(CUSTODY_EVIDENCE), { recursive: true });
  writeFileSync(CUSTODY_EVIDENCE, evidenceText, { mode: 0o600 });
  const values = { phase: privateIngestReleasePhase(input), inputs_sha256: inputsDigest(text), custody_evidence_sha256: sha256(evidenceText) };
  output(values);
  return values;
}

export function approvePrivateIngestCapture() {
  const { input } = trackedInput(), runtimeText = readFileSync(RUNTIME, "utf8");
  verifyCustodyEvidenceFile();
  if (inputsDigest(runtimeText) !== inputsDigest(JSON.stringify(input)))
    privateIngestRefuse("private_ingest_runtime_input_drift");
  const provenanceText = readFileSync(PREFIX + ".provenance.json", "utf8"),
    planText = readFileSync(PREFIX + ".plan.txt", "utf8"),
    provenance = JSON.parse(provenanceText) as {
      source?: { commit?: unknown };
      inputs?: { sha256?: unknown };
      outputs?: { plan?: { sha256?: unknown } };
    },
    sourceCommit = String(provenance.source?.commit ?? ""),
    inputSha = inputsDigest(runtimeText), planSha = sha256(planText), provenanceSha = sha256(provenanceText);
  if (!/^[0-9a-f]{40}$/u.test(sourceCommit) || provenance.inputs?.sha256 !== inputSha ||
      provenance.outputs?.plan?.sha256 !== planSha)
    privateIngestRefuse("private_ingest_capture_contract");
  const approved: CloudflareStageInputs = {
    ...input,
    approval: {
      decision: "approved",
      by: P.approval.by,
      // The GitHub gate proved both dispatch identities are Khaliq's exact user. Bind that run's
      // creation date; the source contract separately retains his rollout quote/date.
      date: (() => {
        const value = process.env.PRIVATE_INGEST_AUTHORIZATION_DATE ?? "";
        if (!/^20[0-9]{2}-[0-9]{2}-[0-9]{2}$/u.test(value))
          privateIngestRefuse("private_ingest_authorization_date");
        return value;
      })(),
      quote: P.approval.quote,
      execution: EXECUTION_EXCEPTION,
      capture: {
        provenanceSha256: provenanceSha,
        planSha256: planSha,
        inputsSha256: inputSha,
        sourceCommit,
      },
    } as CloudflareStageInputs["approval"] & { execution: string },
  };
  writeFileSync(RUNTIME, JSON.stringify(approved, null, 2) + "\n", { mode: 0o600 });
  const values = { phase: privateIngestReleasePhase(input), inputs_sha256: inputSha, provenance_sha256: provenanceSha };
  output(values);
  return values;
}

async function cloudflareGet(path: string, worker: typeof P.worker | typeof P.relayhistoryWorker = P.worker): Promise<unknown> {
  const token = process.env.CLOUDFLARE_API_TOKEN;
  if (!token || token.length < 16) privateIngestRefuse("private_ingest_cloudflare_token");
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${P.accountId}/workers/scripts/${worker}${path}`,
    {
      headers: { Authorization: `Bearer ${token}` },
      redirect: "error",
      signal: AbortSignal.timeout(15_000),
    },
  );
  if (!response.ok) privateIngestRefuse("private_ingest_cloudflare_read");
  const text = await response.text();
  if (text.length > 2 * 1024 * 1024) privateIngestRefuse("private_ingest_cloudflare_read_bound");
  const body = JSON.parse(text) as { success?: unknown; result?: unknown };
  if (body.success !== true || body.result === undefined)
    privateIngestRefuse("private_ingest_cloudflare_response");
  return body.result;
}

export async function readBackPrivateIngest() {
  const { input } = trackedInput(), active = privateIngestActiveVersion(await cloudflareGet("/deployments"));
  const version = await cloudflareGet(`/versions/${active.versionId}`) as CloudflareWorkerVersion;
  const receipt = {
    ...privateIngestVersionReadback(input, active, version),
    custodyEvidenceSha256: sha256(custodyEvidenceText()),
    reviewedSourceShadowReceiptSha256: REVIEWED_CUSTODY_SHADOW_MATCH?.receiptSha256 ?? null,
    at: new Date().toISOString(),
    sourceCommit: process.env.GITHUB_SHA ?? null,
    providerWrites: 0,
    credentialsPersisted: false,
  };
  mkdirSync(dirname(RECEIPT), { recursive: true });
  writeFileSync(RECEIPT, JSON.stringify(receipt, null, 2) + "\n", { mode: 0o600 });
  console.log(JSON.stringify(receipt));
  return receipt;
}

export async function preflightPrivateIngest() {
  const { input } = trackedInput(), active = privateIngestActiveVersion(await cloudflareGet("/deployments"));
  const version = await cloudflareGet(`/versions/${active.versionId}`) as CloudflareWorkerVersion;
  verifyCustodyEvidenceFile();
  const transition = privateIngestTransitionReadback(input, active, version);
  const shadowReceiptSha256 = transition.from === "step-0-shadow" && transition.to === "step-1-custody"
    ? requireCustodyShadowMatch({ ...active, phase: transition.from }, input.sessionIngest.expectedGeneration!)
    : null;
  if (process.env.PRIVATE_INGEST_RELEASE_MODE === "deploy") {
    const overlapGeneration = input.sessionIngest.overlapGeneration;
    const release = privateIngestReleaseInputs(process.env, overlapGeneration);
    // SHADOW is bound to the step-0-shadow -> step-1-custody edge; OVERLAP to a tracked overlap
    // profile. The transition gate holds the phase fixed while {G, H} is live, so both never co-occur.
    const expected = `DEPLOY ${P.worker} ${release.sha} PLAN ${release.planRun}` +
      (shadowReceiptSha256 === null ? "" : ` SHADOW ${shadowReceiptSha256}`) +
      (overlapGeneration === undefined ? "" : ` OVERLAP ${overlapGeneration}`);
    if (release.confirmation !== expected) privateIngestRefuse("private_ingest_shadow_confirmation");
  }
  let rollbackWitness = null;
  if (transition.from === "step-1-custody" && transition.to === "step-0-shadow") {
    const rhcActive = privateIngestActiveVersion(await cloudflareGet("/deployments", P.relayhistoryWorker));
    const rhcVersion = await cloudflareGet(`/versions/${rhcActive.versionId}`, P.relayhistoryWorker) as CloudflareWorkerVersion;
    rollbackWitness = requireRelayhistoryWitness(rhcActive, rhcVersion);
  }
  const receipt = {
    ...transition,
    shadowReceiptSha256,
    custodyEvidenceSha256: sha256(custodyEvidenceText()),
    rollbackWitness,
    at: new Date().toISOString(),
    sourceCommit: process.env.GITHUB_SHA ?? null,
    providerWrites: 0,
    credentialsPersisted: false,
  };
  mkdirSync(dirname(PREFLIGHT), { recursive: true });
  writeFileSync(PREFLIGHT, JSON.stringify(receipt, null, 2) + "\n", { mode: 0o600 });
  console.log(JSON.stringify(receipt));
  return receipt;
}

if (import.meta.main) {
  const command = process.argv[2];
  Promise.resolve(
    command === "stage" ? stagePrivateIngestInput()
      : command === "approve" ? approvePrivateIngestCapture()
      : command === "preflight" ? preflightPrivateIngest()
      : command === "readback" ? readBackPrivateIngest()
      : privateIngestRefuse("private_ingest_ci_command"),
  ).catch((error) => {
    const code = typeof (error as { code?: unknown })?.code === "string"
      ? (error as { code: string }).code
      : "private_ingest_ci_failed";
    console.error(code);
    process.exitCode = 1;
  });
}
