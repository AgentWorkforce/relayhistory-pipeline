/**
 * CI-only production release contract for 96bulls-session-ingest.
 *
 * The desired profile is tracked in the repository. The workflow materializes it into the
 * gitignored Alchemy input path, but no workflow input is allowed to alter the profile. The
 * custody shadow preparation and three rollout states are explicit: shadow in drain,
 * frozen custody in drain, the synchronous frozen-receipt bridge in drain, then async on.
 *
 * Generation overlap (docs/design/zero-503-schema-windows.md section 3.2) is a second, orthogonal axis
 * and is dormant unless PRIVATE_INGEST_OVERLAP names a reviewed G -> H pair. With a reviewed pair the
 * only generation sequence is G -> G+H -> H (rollback G+H -> G), each as its own reviewed deploy of the
 * tracked profile, with the rollout phase held fixed. Every base phase, step-0-shadow included, has a
 * `<phase>-overlap` twin. With the constant null every plan, confirmation, readback and transition is
 * byte-identical to the no-overlap contract.
 */
import { createHash } from "node:crypto";
import { custodyEvidenceConfirmationSuffix } from "./private-ingest-custody-evidence.ts";
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
  relayhistoryWorker: "relayhistory-api",
  approval: {
    by: "Khaliq Gant",
    directDate: "2026-11-11",
    quote: "authorized to do step 1,2,4",
  },
} as const;

export type PrivateIngestBasePhase =
  | "baseline-drain"
  | "step-0-shadow"
  | "step-1-custody"
  | "step-2-frozen-receipt"
  | "step-4-async-on";

/** `<phase>-overlap` is the same rollout phase with the Worker accepting exactly {G, H}. */
export type PrivateIngestReleasePhase = PrivateIngestBasePhase | `${PrivateIngestBasePhase}-overlap`;

/** A reviewed generation overlap: the Worker currently bound to `from` (G) may also accept `to` (H). */
export interface PrivateIngestOverlap {
  readonly from: string;
  readonly to: string;
}

/**
 * The generation half of the contract: the reviewed expected generation and the reviewed overlap
 * pair, if any. Production always uses PRIVATE_INGEST_GENERATION_CONTRACT; the parameter exists so
 * tests can exercise a non-dormant pair without editing the reviewed constants.
 */
export interface PrivateIngestGenerationContract {
  readonly generation: string;
  readonly overlap: PrivateIngestOverlap | null;
}

export function privateIngestRefuse(code: string): never {
  throw Object.assign(new Error(code), { code });
}

const HEX64 = /^[0-9a-f]{64}$/u;

/**
 * `profileOverlap` is the tracked profile's overlapGeneration (undefined when absent). Only when it is
 * present does the deploy confirmation grow ` OVERLAP <H>`, so Khaliq types the generation he is
 * adding to the live Worker. Absent, the confirmation is unchanged.
 */
export function privateIngestReleaseInputs(env: Record<string, string | undefined>, profileOverlap?: unknown) {
  const mode = env.PRIVATE_INGEST_RELEASE_MODE,
    sha = env.PRIVATE_INGEST_RELEASE_SHA,
    planRun = env.PRIVATE_INGEST_RELEASE_PLAN_RUN ?? "",
    confirmation = env.PRIVATE_INGEST_RELEASE_CONFIRMATION ?? "";
  if ((mode !== "plan" && mode !== "deploy") || !/^[0-9a-f]{40}$/u.test(sha ?? ""))
    privateIngestRefuse("private_ingest_release_inputs");
  if (mode === "plan" && (confirmation || planRun))
    privateIngestRefuse("private_ingest_release_plan_confirmation");
  if (profileOverlap !== undefined && (typeof profileOverlap !== "string" || !HEX64.test(profileOverlap)))
    privateIngestRefuse("private_ingest_release_overlap");
  const overlapSuffix = profileOverlap === undefined ? "" : ` OVERLAP ${profileOverlap}`;
  if (mode === "deploy" && (!/^[1-9][0-9]*$/u.test(planRun) ||
      (confirmation !== `DEPLOY ${PRIVATE_INGEST_RELEASE.worker} ${sha} PLAN ${planRun}${overlapSuffix}` &&
       confirmation !== `DEPLOY ${PRIVATE_INGEST_RELEASE.worker} ${sha} PLAN ${planRun}${custodyEvidenceConfirmationSuffix()}${overlapSuffix}`)))
    privateIngestRefuse("private_ingest_release_confirmation");
  return { mode, sha: sha!, planRun, confirmation } as const;
}

const COMMON_PROFILE = {
  acceptanceMode: "buffered",
  accountId: PRIVATE_INGEST_RELEASE.accountId,
  expectedGeneration: "52a3f2f05d3dfa2342d3828c6b14eeeb61f1e47e5b97cddffb91dd0cfee04c0f",
  hyperdriveId: "0fcc0265e96f43a39976b5aa4263f4a6",
  secretsStoreId: "59e0dcc52f0a4bd7863730548299fb2f",
  maxPerUser: 4,
  maxPerOrg: 32,
  inlineMaxSessions: 0,
  replicaMode: "off",
  claimLockBusy: "on",
} as const;

/**
 * Reviewed generation overlap pair, pinned in source like expectedGeneration. null (today) means the
 * contract is dormant: any tracked overlapGeneration is refused and a live INGEST_ACCEPT_GENERATIONS
 * binding is refused as undeclared.
 *
 * Trade-off: pinning both generations means opening, finishing or abandoning a window costs a reviewed
 * source change, not a workflow input, and a stale pair can block a later window until it is edited.
 * That cost is deliberate. The Worker's accept set is the trust boundary for which schema
 * generation receives writes, so it gets the same review as expectedGeneration. No dispatch input,
 * environment variable or live value can widen it.
 *
 * Lifecycle: (1) set {from: G, to: H} while COMMON_PROFILE.expectedGeneration is G; (2) after PASS
 * at H and the rebind, move expectedGeneration to H and keep the pair for the G+H -> H deploy; (3) set
 * this back to null once the live Worker reads back exactly {H}.
 *
 * Post-apply recovery (2026-11-13, docs/design/zero-503-schema-windows.md section 3.2): readiness is
 * already PASS at H = 52a3f2f0... (guarded apply run 43471961540 at 1b2f5bca, release target
 * upstream:20261105047000 at eac3e98b) while the Worker, the lazy policy and the admission rows are
 * still at G = 7db65adb... and no window was opened, so buffered uploads failed closed. The guarded
 * recovery deployed G+H at the unchanged rollout phase, then rebind run 43485447067 moved the policy
 * and all 1043 admission rows to H; readback run 43485770890 proved marker, policy and admissions at H.
 * This is lifecycle step (2): deploy H-only at the same baseline phase while retaining the reviewed
 * pair solely to authorize the G+H -> H transition. Step (3) removes the pair in a later reviewed
 * source change after the live Worker reads back exactly H.
 */
export const PRIVATE_INGEST_OVERLAP: PrivateIngestOverlap | null = {
  from: "7db65adb97f034a7b4cc2c85c87da2ebadf969f6bb74096f33ae00ad83e500a4",
  to: "52a3f2f05d3dfa2342d3828c6b14eeeb61f1e47e5b97cddffb91dd0cfee04c0f",
};

export const PRIVATE_INGEST_GENERATION_CONTRACT: PrivateIngestGenerationContract = {
  generation: COMMON_PROFILE.expectedGeneration,
  overlap: PRIVATE_INGEST_OVERLAP,
};

function checkedContract(contract: PrivateIngestGenerationContract): PrivateIngestGenerationContract {
  const overlap = contract.overlap;
  if (!HEX64.test(contract.generation) || (overlap !== null && (!HEX64.test(overlap.from) ||
      !HEX64.test(overlap.to) || overlap.from === overlap.to ||
      (contract.generation !== overlap.from && contract.generation !== overlap.to))))
    privateIngestRefuse("private_ingest_overlap_contract");
  return contract;
}

/** Shape checks that need no contract; run first so each refusal stays distinct. */
function overlapShape(profile: Record<string, unknown>): string | undefined {
  const value = profile.overlapGeneration;
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !HEX64.test(value))
    privateIngestRefuse("private_ingest_profile_overlap_malformed");
  if (value === profile.expectedGeneration) privateIngestRefuse("private_ingest_profile_overlap_equal");
  if (profile.acceptanceMode !== "buffered") privateIngestRefuse("private_ingest_profile_overlap_mode");
  return value;
}

const KMS_ARN = /^arn:aws:kms:us-east-1:[0-9]{12}:key\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

/** Exact tracked production profile. A newly added stack option is off until this contract reviews it. */
export function privateIngestReleasePhase(
  input: CloudflareStageInputs,
  contract: PrivateIngestGenerationContract = PRIVATE_INGEST_GENERATION_CONTRACT,
): PrivateIngestReleasePhase {
  const c = checkedContract(contract);
  const profile = input.sessionIngest as unknown as Record<string, unknown>;
  const overlap = overlapShape(profile);
  for (const [key, value] of Object.entries(COMMON_PROFILE))
    if (profile[key] !== (key === "expectedGeneration" ? c.generation : value))
      privateIngestRefuse("private_ingest_profile_common");
  if (!KMS_ARN.test(String(profile.kmsKeyArn ?? "")) ||
      (profile.custodySource !== undefined && profile.custodySource !== "shadow" && profile.custodySource !== "frozen-supabase"))
    privateIngestRefuse("private_ingest_profile_target");
  const allowed = new Set([
    ...Object.keys(COMMON_PROFILE),
    "kmsKeyArn",
    "custodySource",
    "asyncAdmission",
    "syncAdmissionFrozenReceipt",
    "overlapGeneration",
  ]);
  if (Object.keys(profile).some((key) => !allowed.has(key)))
    privateIngestRefuse("private_ingest_profile_extra");
  if (overlap !== undefined) {
    if (c.overlap === null) privateIngestRefuse("private_ingest_profile_overlap_unreviewed");
    // The pair is only offered while the reviewed expected generation is still G.
    if (c.generation !== c.overlap.from) privateIngestRefuse("private_ingest_profile_overlap_stale");
    if (overlap !== c.overlap.to) privateIngestRefuse("private_ingest_profile_overlap_unexpected");
  }
  const base = basePhase(profile);
  return overlap === undefined ? base : `${base}-overlap`;
}

function basePhase(profile: Record<string, unknown>): PrivateIngestBasePhase {
  const asyncAdmission = profile.asyncAdmission,
    frozenReceipt = profile.syncAdmissionFrozenReceipt;
  if (profile.custodySource === undefined || profile.custodySource === "shadow") {
    if (asyncAdmission !== "drain" || frozenReceipt !== undefined)
      privateIngestRefuse("private_ingest_profile_phase");
    return profile.custodySource === "shadow" ? "step-0-shadow" : "baseline-drain";
  }
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
  /** Present only while a reviewed overlap pair exists: the live and target generation sets. */
  generationFrom?: PrivateIngestGenerationState;
  generationTo?: PrivateIngestGenerationState;
}

/** G = single reviewed `from`, G+H = overlap, H = single reviewed `to`. */
export type PrivateIngestGenerationState = "G" | "G+H" | "H";

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
  // Causal revert detection: a live accept set the profile does not declare means the overlap was
  // removed from source while the Worker still accepts {G, H}. That is never read back as a pass.
  if (vars.INGEST_ACCEPT_GENERATIONS === undefined && names.includes("INGEST_ACCEPT_GENERATIONS"))
    privateIngestRefuse("private_ingest_live_undeclared_overlap");
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

function profileFor(
  base: PrivateIngestBasePhase,
  kmsKeyArn: string,
  expectedGeneration: string,
  overlapGeneration?: string,
): CloudflareStageInputs {
  const sessionIngest: SessionIngestInputs = {
    ...COMMON_PROFILE,
    expectedGeneration,
    kmsKeyArn,
    ...(base === "baseline-drain" ? {} : { custodySource: base === "step-0-shadow" ? "shadow" as const : "frozen-supabase" as const }),
    asyncAdmission: base === "step-4-async-on" ? "on" : "drain",
    ...(base === "step-2-frozen-receipt" || base === "step-4-async-on"
      ? { syncAdmissionFrozenReceipt: "on" as const }
      : {}),
    ...(overlapGeneration === undefined ? {} : { overlapGeneration }),
  };
  return { sessionIngest };
}

const BASE_PHASES = [
  "baseline-drain",
  "step-0-shadow",
  "step-1-custody",
  "step-2-frozen-receipt",
  "step-4-async-on",
] as const;

function splitPhase(phase: PrivateIngestReleasePhase): { base: PrivateIngestBasePhase; overlap: boolean } {
  const overlap = phase.endsWith("-overlap");
  const base = (overlap ? phase.slice(0, -"-overlap".length) : phase) as PrivateIngestBasePhase;
  if (!(BASE_PHASES as readonly string[]).includes(base)) privateIngestRefuse("private_ingest_profile_phase");
  return { base, overlap };
}

export function privateIngestProfileForPhase(
  phase: PrivateIngestReleasePhase,
  kmsKeyArn: string,
  contract: PrivateIngestGenerationContract = PRIVATE_INGEST_GENERATION_CONTRACT,
): CloudflareStageInputs {
  const c = checkedContract(contract), { base, overlap } = splitPhase(phase);
  if (!overlap) return profileFor(base, kmsKeyArn, c.generation);
  if (c.overlap === null) privateIngestRefuse("private_ingest_profile_overlap_unreviewed");
  if (c.generation !== c.overlap.from) privateIngestRefuse("private_ingest_profile_overlap_stale");
  return profileFor(base, kmsKeyArn, c.overlap.from, c.overlap.to);
}

/**
 * A forward step must be observed independently on the live 100% version; rollbacks unwind one step,
 * including custody shadow (baseline-drain <-> step-0-shadow <-> step-1-custody, no direct skip).
 * The generation axis moves only with the rollout phase held fixed: G -> G+H -> H, or G+H -> G.
 */
export function privateIngestTransitionReadback(
  input: CloudflareStageInputs,
  active: ReturnType<typeof privateIngestActiveVersion>,
  version: CloudflareWorkerVersion,
  contract: PrivateIngestGenerationContract = PRIVATE_INGEST_GENERATION_CONTRACT,
): PrivateIngestTransitionReadback {
  const c = checkedContract(contract);
  const to = privateIngestReleasePhase(input, c), kmsKeyArn = input.sessionIngest.kmsKeyArn;
  const target = splitPhase(to);
  // A dormant contract never admits a live accept set: reverting the reviewed pair while the Worker
  // still accepts {G, H} is refused here with its own code instead of an ambiguous phase miss.
  if (c.overlap === null && Array.isArray(version.resources?.bindings) &&
      version.resources.bindings.some((binding) => binding.name === "INGEST_ACCEPT_GENERATIONS"))
    privateIngestRefuse("private_ingest_live_undeclared_overlap");
  const label = (generation: string): PrivateIngestGenerationState =>
    c.overlap === null || generation === c.overlap.from ? "G" : "H";
  const candidates: Array<[PrivateIngestBasePhase, PrivateIngestGenerationState, boolean, CloudflareStageInputs]> = [];
  for (const base of BASE_PHASES) {
    candidates.push([base, label(c.generation), false, profileFor(base, kmsKeyArn, c.generation)]);
    if (c.overlap !== null) {
      const other = c.generation === c.overlap.from ? c.overlap.to : c.overlap.from;
      candidates.push([base, label(other), false, profileFor(base, kmsKeyArn, other)]);
      candidates.push([base, "G+H", true, profileFor(base, kmsKeyArn, c.overlap.from, c.overlap.to)]);
    }
  }
  const matches = candidates.filter(([, , , candidate]) => {
    try { privateIngestVersionEvidence(candidate, active, version); return true; }
    catch { return false; }
  });
  if (matches.length !== 1) privateIngestRefuse("private_ingest_live_phase");
  const [fromBase, generationFrom, fromOverlap] = matches[0]!;
  const generationTo: PrivateIngestGenerationState = target.overlap ? "G+H" : label(c.generation);
  const from: PrivateIngestReleasePhase = fromOverlap ? `${fromBase}-overlap` : fromBase;
  // step-0-shadow is mandatory between baseline-drain and step-1-custody in both directions.
  const allowed: Record<PrivateIngestBasePhase, readonly PrivateIngestBasePhase[]> = {
    "baseline-drain": ["baseline-drain", "step-0-shadow"],
    "step-0-shadow": ["baseline-drain", "step-0-shadow", "step-1-custody"],
    "step-1-custody": ["step-0-shadow", "step-1-custody", "step-2-frozen-receipt"],
    "step-2-frozen-receipt": ["step-1-custody", "step-2-frozen-receipt", "step-4-async-on"],
    "step-4-async-on": ["step-2-frozen-receipt", "step-4-async-on"],
  };
  const generationMoves: ReadonlyArray<readonly [PrivateIngestGenerationState, PrivateIngestGenerationState]> = [
    ["G", "G+H"], // open: add H while still bound to G
    ["G+H", "H"], // finish: after PASS at H and the rebind
    ["G+H", "G"], // rollback: drop H, still bound to G
  ];
  // While {G, H} is live the rollout phase is frozen: only a same-phase redeploy is a no-move.
  if (generationFrom === generationTo
    ? !allowed[target.base].includes(fromBase) || (generationTo === "G+H" && fromBase !== target.base)
    : fromBase !== target.base ||
      !generationMoves.some(([a, b]) => a === generationFrom && b === generationTo))
    privateIngestRefuse("private_ingest_live_transition");
  return {
    status: "PRIVATE_INGEST_LIVE_TRANSITION_PASS",
    from,
    to,
    deploymentId: active.deploymentId,
    versionId: active.versionId,
    ...(c.overlap === null ? {} : { generationFrom, generationTo }),
  };
}
