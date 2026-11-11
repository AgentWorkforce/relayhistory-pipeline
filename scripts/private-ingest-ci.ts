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
  privateIngestVersionReadback,
  type CloudflareWorkerVersion,
} from "../src/session-ingest/private-ingest-release.ts";

const SOURCE = resolve("config/cloudflare/private-ingest-production.json");
const RUNTIME = resolve(".96bulls/cf-sessions-prod/cloudflare-inputs.json");
const PREFIX = resolve("docs/plans/cf-sessions-prod");
const RECEIPT = resolve("release-receipt/private-ingest-readback.json");
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");

function trackedInput(): { text: string; input: CloudflareStageInputs } {
  const text = readFileSync(SOURCE, "utf8"), input = JSON.parse(text) as CloudflareStageInputs;
  if (input.approval !== undefined) privateIngestRefuse("private_ingest_tracked_approval");
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
  const values = { phase: privateIngestReleasePhase(input), inputs_sha256: inputsDigest(text) };
  output(values);
  return values;
}

export function approvePrivateIngestCapture() {
  const { input } = trackedInput(), runtimeText = readFileSync(RUNTIME, "utf8");
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
      date: P.approval.date,
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

async function cloudflareGet(path: string): Promise<unknown> {
  const token = process.env.CLOUDFLARE_API_TOKEN;
  if (!token || token.length < 16) privateIngestRefuse("private_ingest_cloudflare_token");
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${P.accountId}/workers/scripts/${P.worker}${path}`,
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

if (import.meta.main) {
  const command = process.argv[2];
  Promise.resolve(
    command === "stage" ? stagePrivateIngestInput()
      : command === "approve" ? approvePrivateIngestCapture()
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
