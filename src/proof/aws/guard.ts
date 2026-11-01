/**
 * The hard guard for the isolated AWS self-host proof (docs/aws-e2e-proof.md).
 *
 * Every step that touches a live target passes through here first. A run acts
 * on exactly one of two hardcoded targets, chosen by `--target`: the proof
 * stage beside sessions-dev (account 500268966557) or the Pippen sandbox of
 * agent aws-proof (account 361835732085, caller pinned to the agent-session
 * role). Anything else is refused before a process is spawned. There is no
 * override flag and no variable that widens either target.
 *
 *   dry-run    (default) print the exact commands; touch nothing.
 *   read-only  run non-mutating steps. Allowed on the local kind cluster
 *              `kind-96bulls-e2e`, or on the proof cluster with --confirm-live.
 *   live       run every step, mutating ones included. Proof cluster only, and
 *              only with --confirm-live and a verified account + cluster.
 */

/** The isolated proof account (Khaliq, 2026-10-30). Never a production account. */
export const PROOF_ACCOUNT_ID = "500268966557";
/** The proof stage; stacks/aws/eks.ts names clusters `96bulls-<stage>`. */
export const PROOF_STAGE = "aws-proof";
export const PROOF_CLUSTER = `96bulls-${PROOF_STAGE}`;
/** The proof stage's only region. Every ARN, context and AWS call must carry it. */
export const PROOF_REGION = "us-west-2";
/** The only local cluster this harness may read; it never mutates it. */
export const LOCAL_READONLY_CONTEXT = "kind-96bulls-e2e";

/**
 * The second, equally hardcoded proof target: the Pippen Tools sandbox account
 * of agent `aws-proof` (Khaliq, 2026-11-01), in the Sandbox OU of the 96Bulls
 * org. Its teardown is `pippen sandbox down aws-proof`, a verified sweep of
 * everything tagged `pippen:sandbox=true` plus a zero-non-baseline-IAM check.
 * Credentials there come only from the pippen `agent-session` role, so the
 * guard also pins the caller's assumed-role ARN.
 */
export const SANDBOX_ACCOUNT_ID = "361835732085";
/** The Pippen sandbox agent name: `pippen sandbox up|down aws-proof`. */
export const PIPPEN_SANDBOX_AGENT = "aws-proof";
export const SANDBOX_STAGE = "aws-proof-sandbox";
export const SANDBOX_CLUSTER = `96bulls-${SANDBOX_STAGE}`;
export const SANDBOX_REGION = "us-west-2";
/** `aws sts get-caller-identity` Arn inside the sandbox: the pippen agent-session role, session `agent-<agent>`. */
export const SANDBOX_CALLER_ROLE = { roleName: "agent-session", sessionName: `agent-${PIPPEN_SANDBOX_AGENT}` } as const;
export const SANDBOX_CALLER_ARN = `arn:aws:sts::${SANDBOX_ACCOUNT_ID}:assumed-role/${SANDBOX_CALLER_ROLE.roleName}/${SANDBOX_CALLER_ROLE.sessionName}`;

export type ProofTargetId = "proof" | "pippen-sandbox";

/**
 * One hardcoded proof target. There are exactly two (PROOF_TARGETS); nothing
 * builds one from config or the environment, and every check compares against
 * these frozen values only.
 */
export interface ProofTarget {
  id: ProofTargetId;
  accountId: string;
  stage: string;
  clusterName: string;
  region: string;
  /** When set, `sts get-caller-identity` Arn must equal this exactly before any live step. */
  callerArn?: string;
  /** The stage's teardown of its AWS resources; printed, never run by the harness. */
  destroyCommand: string;
  /** Extra tag filters the post-destroy absence check lists (beyond `96bulls:stack=<stage>`). */
  sweepTags: ReadonlyArray<{ key: string; value: string }>;
}

/** The original target beside aws-sessions-dev. Its behavior is unchanged by the sandbox target. */
export const SESSIONS_DEV_PROOF_TARGET: ProofTarget = Object.freeze({
  id: "proof",
  accountId: PROOF_ACCOUNT_ID,
  stage: PROOF_STAGE,
  clusterName: PROOF_CLUSTER,
  region: PROOF_REGION,
  destroyCommand: `bin/96bulls down ${PROOF_STAGE}`,
  sweepTags: Object.freeze([]),
}) as ProofTarget;

export const PIPPEN_SANDBOX_PROOF_TARGET: ProofTarget = Object.freeze({
  id: "pippen-sandbox",
  accountId: SANDBOX_ACCOUNT_ID,
  stage: SANDBOX_STAGE,
  clusterName: SANDBOX_CLUSTER,
  region: SANDBOX_REGION,
  callerArn: SANDBOX_CALLER_ARN,
  destroyCommand: `pippen sandbox down ${PIPPEN_SANDBOX_AGENT}`,
  sweepTags: Object.freeze([Object.freeze({ key: "pippen:sandbox", value: "true" })]),
}) as ProofTarget;

export const PROOF_TARGETS: Readonly<Record<ProofTargetId, ProofTarget>> = Object.freeze({ proof: SESSIONS_DEV_PROOF_TARGET, "pippen-sandbox": PIPPEN_SANDBOX_PROOF_TARGET });

/** The hardcoded target whose stage this is, or undefined (kind, or anything unknown). */
export function proofTargetForStage(stage: string): ProofTarget | undefined {
  return Object.values(PROOF_TARGETS).find((t) => t.stage === stage);
}

export type Mode = "dry-run" | "read-only" | "live";

export class GuardError extends Error {
  constructor(
    readonly code:
      | "missing_confirm_live"
      | "wrong_account"
      | "wrong_cluster"
      | "wrong_region"
      | "cluster_unbound"
      | "post_destroy_read_only"
      | "context_mismatch"
      | "production_name"
      | "local_is_read_only"
      | "unverified_identity"
      | "wrong_caller",
    message: string,
  ) {
    super(message);
    this.name = "GuardError";
  }
}

/** Names that must never be acted on even if someone renames the proof stage to match. */
const PRODUCTION_SHAPED = /(^|[-_/:.])(prod|production|sessions-dev|hosted|live)([-_/:.]|$)/i;

export interface Identity {
  /** `aws sts get-caller-identity` Account, as observed, never as configured. */
  accountId?: string;
  /** `aws sts get-caller-identity` Arn, as observed. Required (and pinned) only on a target with `callerArn`. */
  callerArn?: string;
  /** The EKS cluster name the operator asked for (config). */
  clusterName: string;
  /** The kube context the harness will pass as --context. */
  kubeContext: string;
  /** The cluster ARN as `aws eks describe-cluster` reports it (not as a kubeconfig names it). */
  kubeContextCluster?: string;
  /** Post-destroy verification: identity is the STS account (+ the pinned region), there is no cluster to bind. */
  postDestroy?: boolean;
  /**
   * True only when describe-cluster succeeded for the proof cluster and the
   * kube context's server and CA equal its endpoint and CA (cluster-binding.ts).
   */
  clusterBound?: boolean;
}

export interface GuardInput extends Identity {
  mode: Mode;
  confirmLive: boolean;
  /** True when the step mutates state (create, patch, restart, upgrade, delete). */
  mutating: boolean;
}

/** EKS kubeconfig entries name the cluster by ARN: arn:aws:eks:<region>:<account>:cluster/<name>. */
const EKS_ARN = /^arn:aws:eks:([a-z0-9-]+):(\d{12}):cluster\/([A-Za-z0-9_-]+)$/;

export function parseEksArn(arn: string | undefined): { region: string; accountId: string; clusterName: string } | undefined {
  const m = arn ? EKS_ARN.exec(arn.trim()) : null;
  return m ? { region: m[1]!, accountId: m[2]!, clusterName: m[3]! } : undefined;
}

/**
 * Throws unless the step may run. Dry-run always passes: it executes nothing.
 * The decision depends only on observed identity, never on a flag that can
 * widen the target.
 */
export function assertMayRun(g: GuardInput, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET): void {
  if (g.mode === "dry-run") return;
  if (g.postDestroy) {
    if (g.mutating) throw new GuardError("post_destroy_read_only", "refusing: post-destroy verification only reads");
    return assertPostDestroyTarget(g, t);
  }
  if (g.kubeContext === LOCAL_READONLY_CONTEXT) {
    if (g.mutating || g.mode === "live")
      throw new GuardError("local_is_read_only", `refusing: ${LOCAL_READONLY_CONTEXT} is read-only for this harness; nothing was changed`);
    return;
  }
  assertProofTarget(g, t);
}

/**
 * The account + cluster + flag check every non-local step needs, read-only
 * included, against one hardcoded target (default: the sessions-dev proof).
 */
export function assertProofTarget(g: Omit<GuardInput, "mode" | "mutating">, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET): void {
  assertKnownTarget(t);
  if (!g.confirmLive) throw new GuardError("missing_confirm_live", "refusing: live targets need --confirm-live; nothing was run");
  for (const name of [g.clusterName, g.kubeContext, g.kubeContextCluster ?? ""])
    if (PRODUCTION_SHAPED.test(name)) throw new GuardError("production_name", `refusing: '${name}' looks like a production or shared target`);
  if (!g.accountId) throw new GuardError("unverified_identity", "refusing: caller account not verified with sts get-caller-identity");
  if (g.accountId !== t.accountId)
    throw new GuardError("wrong_account", `refusing: caller account ${g.accountId} is not the proof account ${t.accountId}`);
  assertCaller(g, t);
  if (g.clusterName !== t.clusterName)
    throw new GuardError("wrong_cluster", `refusing: cluster '${g.clusterName}' is not the proof cluster '${t.clusterName}'`);
  const arn = parseEksArn(g.kubeContextCluster);
  if (!arn || arn.accountId !== t.accountId || arn.clusterName !== t.clusterName)
    throw new GuardError("context_mismatch", `refusing: kube context '${g.kubeContext}' does not point at ${t.clusterName} in ${t.accountId}`);
  if (arn.region !== t.region) throw new GuardError("wrong_region", `refusing: kube context cluster is in ${arn.region}, not ${t.region}`);
  if (g.clusterBound !== true) throw new GuardError("cluster_unbound", "refusing: the kube context is not bound to the live cluster's endpoint and CA (describe-cluster)");
  // The context name itself, when it is an EKS ARN, must agree too.
  const named = parseEksArn(g.kubeContext);
  if (named && (named.region !== t.region || named.accountId !== t.accountId || named.clusterName !== t.clusterName))
    throw new GuardError("wrong_region", `refusing: kube context name ${g.kubeContext} is not the proof cluster in ${t.region}`);
}

/** Only the two frozen targets are ever accepted; a look-alike object is refused. */
function assertKnownTarget(t: ProofTarget): void {
  if (PROOF_TARGETS[t.id] !== t) throw new GuardError("wrong_account", "refusing: not a hardcoded proof target");
}

/** On a target that pins its caller (the Pippen sandbox), the sts Arn must be exactly that role session. */
function assertCaller(g: { callerArn?: string }, t: ProofTarget): void {
  if (t.callerArn === undefined) return;
  if (!g.callerArn) throw new GuardError("unverified_identity", "refusing: caller ARN not verified with sts get-caller-identity");
  if (g.callerArn !== t.callerArn)
    throw new GuardError("wrong_caller", `refusing: caller ${g.callerArn} is not the pippen agent-session role ${t.callerArn}`);
}

/**
 * The post-destroy guard: --confirm-live and the sts-observed proof account.
 * Region is pinned per step by the fence; the target identity (stage, cluster,
 * context) comes from the admitted prior report, whose target is the exact
 * proof constant. No kube context is involved: the cluster is meant to be gone.
 */
export function assertPostDestroyTarget(g: { confirmLive: boolean; accountId?: string; callerArn?: string; clusterName: string }, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET): void {
  assertKnownTarget(t);
  if (!g.confirmLive) throw new GuardError("missing_confirm_live", "refusing: live targets need --confirm-live; nothing was run");
  if (!g.accountId) throw new GuardError("unverified_identity", "refusing: caller account not verified with sts get-caller-identity");
  if (g.accountId !== t.accountId) throw new GuardError("wrong_account", `refusing: caller account ${g.accountId} is not the proof account ${t.accountId}`);
  assertCaller(g, t);
  if (g.clusterName !== t.clusterName) throw new GuardError("wrong_cluster", `refusing: cluster '${g.clusterName}' is not the proof cluster '${t.clusterName}'`);
}
