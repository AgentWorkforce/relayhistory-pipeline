/**
 * The hard guard for the isolated AWS self-host proof (docs/aws-e2e-proof.md).
 *
 * Every step that touches a live target passes through here first. The proof
 * may only ever act on one AWS account and one EKS cluster; anything else is
 * refused before a process is spawned. There is no override flag.
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

export type Mode = "dry-run" | "read-only" | "live";

export class GuardError extends Error {
  constructor(
    readonly code:
      | "missing_confirm_live"
      | "wrong_account"
      | "wrong_cluster"
      | "wrong_region"
      | "context_mismatch"
      | "production_name"
      | "local_is_read_only"
      | "unverified_identity",
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
  /** The EKS cluster name the operator asked for (config). */
  clusterName: string;
  /** The kube context the harness will pass as --context. */
  kubeContext: string;
  /** The current context's cluster server/ARN as `kubectl config view` reports it. */
  kubeContextCluster?: string;
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
export function assertMayRun(g: GuardInput): void {
  if (g.mode === "dry-run") return;
  if (g.kubeContext === LOCAL_READONLY_CONTEXT) {
    if (g.mutating || g.mode === "live")
      throw new GuardError("local_is_read_only", `refusing: ${LOCAL_READONLY_CONTEXT} is read-only for this harness; nothing was changed`);
    return;
  }
  assertProofTarget(g);
}

/** The account + cluster + flag check every non-local step needs, read-only included. */
export function assertProofTarget(g: Omit<GuardInput, "mode" | "mutating">): void {
  if (!g.confirmLive) throw new GuardError("missing_confirm_live", "refusing: live targets need --confirm-live; nothing was run");
  for (const name of [g.clusterName, g.kubeContext, g.kubeContextCluster ?? ""])
    if (PRODUCTION_SHAPED.test(name)) throw new GuardError("production_name", `refusing: '${name}' looks like a production or shared target`);
  if (!g.accountId) throw new GuardError("unverified_identity", "refusing: caller account not verified with sts get-caller-identity");
  if (g.accountId !== PROOF_ACCOUNT_ID)
    throw new GuardError("wrong_account", `refusing: caller account ${g.accountId} is not the proof account ${PROOF_ACCOUNT_ID}`);
  if (g.clusterName !== PROOF_CLUSTER)
    throw new GuardError("wrong_cluster", `refusing: cluster '${g.clusterName}' is not the proof cluster '${PROOF_CLUSTER}'`);
  const arn = parseEksArn(g.kubeContextCluster);
  if (!arn || arn.accountId !== PROOF_ACCOUNT_ID || arn.clusterName !== PROOF_CLUSTER)
    throw new GuardError("context_mismatch", `refusing: kube context '${g.kubeContext}' does not point at ${PROOF_CLUSTER} in ${PROOF_ACCOUNT_ID}`);
  if (arn.region !== PROOF_REGION) throw new GuardError("wrong_region", `refusing: kube context cluster is in ${arn.region}, not ${PROOF_REGION}`);
  // The context name itself, when it is an EKS ARN, must agree too.
  const named = parseEksArn(g.kubeContext);
  if (named && (named.region !== PROOF_REGION || named.accountId !== PROOF_ACCOUNT_ID || named.clusterName !== PROOF_CLUSTER))
    throw new GuardError("wrong_region", `refusing: kube context name ${g.kubeContext} is not the proof cluster in ${PROOF_REGION}`);
}
