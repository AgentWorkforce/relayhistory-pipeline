/**
 * CI-only production release contract for 96bulls-session-ingest.
 *
 * The desired profile is tracked in the repository. The workflow materializes it into the
 * gitignored Alchemy input path, but no workflow input is allowed to alter the profile. The
 * three authorized rollout states are deliberately sequential: frozen custody in drain,
 * the synchronous frozen-receipt bridge in drain, then async admission on.
 */
import { createHash } from "node:crypto";
import {
  ADMISSION_QUEUE,
  COMPATIBILITY_DATE,
  COMPATIBILITY_FLAGS,
  FANOUT_QUEUE,
  SESSION_INGEST_WORKER,
  S3_ACCESS_KEY_SECRET,
  S3_SECRET_KEY_SECRET,
  SIGNING_KEY_SECRET,
  resolvedAsyncAdmission,
  sessionIngestVars,
  type SessionIngestInputs,
} from "../../stacks/cloudflare/session-ingest.ts";
import type { CloudflareStageInputs } from "../../stacks/cloudflare/alchemy.cloudflare.run.ts";

export const PRIVATE_INGEST_RELEASE = {
  repository: "AgentWorkforce/relayhistory-pipeline",
  repositoryId: "1437714333",
  releaseActorGithubUserId: 1724137,
  environment: "private-ingest-production",
  workflow: ".github/workflows/deploy-private-ingest-worker.yml",
  accountId: "f7232cb80f6fab86a95426302af243e4",
  worker: SESSION_INGEST_WORKER,
  approval: {
    by: "Khaliq Gant",
    directDate: "2026-11-11",
    quote: "authorized to do step 1,2,4",
  },
} as const;

export type PrivateIngestReleasePhase =
  | "baseline-drain"
  | "step-1-custody"
  | "step-2-frozen-receipt"
  | "step-4-async-on";

export function privateIngestRefuse(code: string): never {
  throw Object.assign(new Error(code), { code });
}

export function privateIngestReleaseInputs(env: Record<string, string | undefined>) {
  const mode = env.PRIVATE_INGEST_RELEASE_MODE,
    sha = env.PRIVATE_INGEST_RELEASE_SHA,
    planRun = env.PRIVATE_INGEST_RELEASE_PLAN_RUN ?? "",
    confirmation = env.PRIVATE_INGEST_RELEASE_CONFIRMATION ?? "";
  if ((mode !== "plan" && mode !== "deploy") || !/^[0-9a-f]{40}$/u.test(sha ?? ""))
    privateIngestRefuse("private_ingest_release_inputs");
  if (mode === "plan" && (confirmation || planRun))
    privateIngestRefuse("private_ingest_release_plan_confirmation");
  if (mode === "deploy" && (!/^[1-9][0-9]*$/u.test(planRun) ||
      confirmation !== `DEPLOY ${PRIVATE_INGEST_RELEASE.worker} ${sha} PLAN ${planRun}`))
    privateIngestRefuse("private_ingest_release_confirmation");
  return { mode, sha: sha!, planRun, confirmation } as const;
}

const COMMON_PROFILE = {
  acceptanceMode: "buffered",
  accountId: PRIVATE_INGEST_RELEASE.accountId,
  expectedGeneration: "7db65adb97f034a7b4cc2c85c87da2ebadf969f6bb74096f33ae00ad83e500a4",
  hyperdriveId: "0fcc0265e96f43a39976b5aa4263f4a6",
  secretsStoreId: "59e0dcc52f0a4bd7863730548299fb2f",
  maxPerUser: 4,
  maxPerOrg: 32,
  inlineMaxSessions: 0,
  replicaMode: "off",
  claimLockBusy: "on",
} as const;

const KMS_ARN = /^arn:aws:kms:us-east-1:[0-9]{12}:key\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

/** Exact tracked production profile. A newly added stack option is off until this contract reviews it. */
export function privateIngestReleasePhase(input: CloudflareStageInputs): PrivateIngestReleasePhase {
  const profile = input.sessionIngest as unknown as Record<string, unknown>;
  for (const [key, value] of Object.entries(COMMON_PROFILE))
    if (profile[key] !== value) privateIngestRefuse("private_ingest_profile_common");
  if (!KMS_ARN.test(String(profile.kmsKeyArn ?? "")) ||
      (profile.custodySource !== undefined && profile.custodySource !== "frozen-supabase"))
    privateIngestRefuse("private_ingest_profile_target");
  const allowed = new Set([
    ...Object.keys(COMMON_PROFILE),
    "kmsKeyArn",
    "custodySource",
    "asyncAdmission",
    "syncAdmissionFrozenReceipt",
  ]);
  if (Object.keys(profile).some((key) => !allowed.has(key)))
    privateIngestRefuse("private_ingest_profile_extra");
  const asyncAdmission = profile.asyncAdmission,
    frozenReceipt = profile.syncAdmissionFrozenReceipt;
  if (profile.custodySource === undefined && asyncAdmission === "drain" && frozenReceipt === undefined)
    return "baseline-drain";
  if (asyncAdmission === "drain" && frozenReceipt === undefined)
    return "step-1-custody";
  if (asyncAdmission === "drain" && frozenReceipt === "on")
    return "step-2-frozen-receipt";
  if (asyncAdmission === "on" && frozenReceipt === "on")
    return "step-4-async-on";
  return privateIngestRefuse("private_ingest_profile_phase");
}

export type CloudflareVersionBinding = {
  name?: unknown;
  type?: unknown;
  text?: unknown;
  id?: unknown;
  queue_name?: unknown;
  store_id?: unknown;
  secret_name?: unknown;
};

export type CloudflareActiveDeployment = {
  id?: unknown;
  created_on?: unknown;
  versions?: Array<{ version_id?: unknown; percentage?: unknown }>;
};

export type CloudflareWorkerVersion = {
  id?: unknown;
  number?: unknown;
  created_on?: unknown;
  resources?: {
    bindings?: CloudflareVersionBinding[];
    script_runtime?: {
      compatibility_date?: unknown;
      compatibility_flags?: unknown;
    };
  };
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

/** Select the current deployment's sole 100% version. Split or missing current state is never guessed through. */
export function privateIngestActiveVersion(value: unknown): { deploymentId: string; versionId: string; createdOn: string | null } {
  const deployments = (value as { deployments?: CloudflareActiveDeployment[] })?.deployments;
  // Cloudflare may retain older deployment records after the current one. The API orders the
  // current deployment first (the same contract used by the Reader preservation gate).
  if (!Array.isArray(deployments) || deployments.length === 0)
    privateIngestRefuse("private_ingest_live_deployment_count");
  const deployment = deployments[0]!, versions = deployment.versions;
  if (!UUID.test(String(deployment.id ?? "")) || !Array.isArray(versions) || versions.length !== 1 ||
      versions[0]?.percentage !== 100 || !UUID.test(String(versions[0]?.version_id ?? "")))
    privateIngestRefuse("private_ingest_live_deployment_split");
  return {
    deploymentId: String(deployment.id),
    versionId: String(versions[0]!.version_id),
    createdOn: typeof deployment.created_on === "string" ? deployment.created_on : null,
  };
}

function named(bindings: CloudflareVersionBinding[], name: string): CloudflareVersionBinding {
  const rows = bindings.filter((binding) => binding.name === name);
  if (rows.length !== 1) privateIngestRefuse("private_ingest_live_binding_inventory");
  return rows[0]!;
}

function expectTarget(binding: CloudflareVersionBinding, values: Record<string, unknown>): void {
  for (const [key, value] of Object.entries(values))
    if (binding[key as keyof CloudflareVersionBinding] !== value)
      privateIngestRefuse("private_ingest_live_binding_target");
}

export interface PrivateIngestReadback {
  status: "PRIVATE_INGEST_LIVE_READBACK_PASS";
  phase: PrivateIngestReleasePhase;
  deploymentId: string;
  versionId: string;
  createdOn: string | null;
  bindingCount: number;
  profileSha256: string;
}

export interface PrivateIngestTransitionReadback {
  status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS";
  from: PrivateIngestReleasePhase;
  to: PrivateIngestReleasePhase;
  deploymentId: string;
  versionId: string;
}

/**
 * Verify the deployed version against the tracked profile. The provider may include module bindings;
 * those are allowed, but every INGEST_* variable and every runtime binding is exact and unique.
 */
function privateIngestVersionEvidence(
  input: CloudflareStageInputs,
  active: ReturnType<typeof privateIngestActiveVersion>,
  version: CloudflareWorkerVersion,
): Pick<PrivateIngestReadback, "bindingCount" | "profileSha256"> {
  if (version.id !== active.versionId || !Array.isArray(version.resources?.bindings))
    privateIngestRefuse("private_ingest_live_version_identity");
  const bindings = version.resources.bindings;
  const names = bindings.map((binding) => binding.name).filter((name): name is string => typeof name === "string");
  if (new Set(names).size !== names.length)
    privateIngestRefuse("private_ingest_live_binding_duplicate");
  const vars = sessionIngestVars(input.sessionIngest);
  for (const [name, text] of Object.entries(vars))
    expectTarget(named(bindings, name), { type: "plain_text", text });
  const expectedIngest = new Set([
    ...Object.keys(vars),
    "INGEST_FANOUT",
    "INGEST_ADMISSION",
    "INGEST_SIGNING_KEY",
  ]);
  if (names.some((name) => name.startsWith("INGEST_") && !expectedIngest.has(name)))
    privateIngestRefuse("private_ingest_live_unknown_flag");
  expectTarget(named(bindings, "HYPERDRIVE"), { type: "hyperdrive", id: input.sessionIngest.hyperdriveId });
  expectTarget(named(bindings, "INGEST_FANOUT"), { type: "queue", queue_name: FANOUT_QUEUE });
  if (resolvedAsyncAdmission(input.sessionIngest) !== "off")
    expectTarget(named(bindings, "INGEST_ADMISSION"), { type: "queue", queue_name: ADMISSION_QUEUE });
  for (const [name, secretName] of [
    ["S3_ACCESS_KEY_ID", S3_ACCESS_KEY_SECRET],
    ["S3_SECRET_ACCESS_KEY", S3_SECRET_KEY_SECRET],
    ["INGEST_SIGNING_KEY", SIGNING_KEY_SECRET],
  ] as const)
    expectTarget(named(bindings, name), {
      type: "secrets_store_secret",
      store_id: input.sessionIngest.secretsStoreId,
      secret_name: secretName,
    });
  const runtime = version.resources.script_runtime;
  if (runtime?.compatibility_date !== COMPATIBILITY_DATE)
    privateIngestRefuse("private_ingest_live_compatibility_date");
  const flags = runtime?.compatibility_flags;
  if (!Array.isArray(flags) || COMPATIBILITY_FLAGS.some((flag) => !flags.includes(flag)))
    privateIngestRefuse("private_ingest_live_compatibility_flags");
  const profileSha256 = createHash("sha256").update(JSON.stringify(input.sessionIngest)).digest("hex");
  return { bindingCount: bindings.length, profileSha256 };
}

export function privateIngestVersionReadback(
  input: CloudflareStageInputs,
  active: ReturnType<typeof privateIngestActiveVersion>,
  version: CloudflareWorkerVersion,
): PrivateIngestReadback {
  const phase = privateIngestReleasePhase(input);
  return {
    status: "PRIVATE_INGEST_LIVE_READBACK_PASS",
    phase,
    deploymentId: active.deploymentId,
    versionId: active.versionId,
    createdOn: active.createdOn,
    ...privateIngestVersionEvidence(input, active, version),
  };
}

export function privateIngestProfileForPhase(
  phase: PrivateIngestReleasePhase,
  kmsKeyArn: string,
): CloudflareStageInputs {
  const sessionIngest: SessionIngestInputs = {
    ...COMMON_PROFILE,
    kmsKeyArn,
    ...(phase === "baseline-drain" ? {} : { custodySource: "frozen-supabase" as const }),
    asyncAdmission: phase === "step-4-async-on" ? "on" : "drain",
    ...(phase === "baseline-drain" || phase === "step-1-custody"
      ? {}
      : { syncAdmissionFrozenReceipt: "on" as const }),
  };
  return { sessionIngest };
}

/** A forward step must be observed independently on the live 100% version; rollbacks unwind one step. */
export function privateIngestTransitionReadback(
  input: CloudflareStageInputs,
  active: ReturnType<typeof privateIngestActiveVersion>,
  version: CloudflareWorkerVersion,
): PrivateIngestTransitionReadback {
  const to = privateIngestReleasePhase(input), kmsKeyArn = input.sessionIngest.kmsKeyArn;
  const candidates = [
    ["baseline-drain", privateIngestProfileForPhase("baseline-drain", kmsKeyArn)],
    ["step-1-custody", privateIngestProfileForPhase("step-1-custody", kmsKeyArn)],
    ["step-2-frozen-receipt", privateIngestProfileForPhase("step-2-frozen-receipt", kmsKeyArn)],
    ["step-4-async-on", privateIngestProfileForPhase("step-4-async-on", kmsKeyArn)],
  ] as const;
  const matches = candidates.filter(([, candidate]) => {
    try { privateIngestVersionEvidence(candidate, active, version); return true; }
    catch { return false; }
  });
  if (matches.length !== 1) privateIngestRefuse("private_ingest_live_phase");
  const from = matches[0]![0];
  const allowed: Record<PrivateIngestReleasePhase, readonly PrivateIngestTransitionReadback["from"][]> = {
    "baseline-drain": ["baseline-drain", "step-1-custody"],
    "step-1-custody": ["baseline-drain", "step-1-custody", "step-2-frozen-receipt"],
    "step-2-frozen-receipt": ["step-1-custody", "step-2-frozen-receipt", "step-4-async-on"],
    "step-4-async-on": ["step-2-frozen-receipt", "step-4-async-on"],
  };
  if (!allowed[to].includes(from)) privateIngestRefuse("private_ingest_live_transition");
  return { status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS", from, to, deploymentId: active.deploymentId, versionId: active.versionId };
}
