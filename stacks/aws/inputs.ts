/**
 * The AWS stage's inputs (`.96bulls/<stage>/aws-inputs.json`, written by hand
 * and never committed) and their validation. Pure, so a stage's inputs can be
 * checked without providers or credentials; the stack calls `validateAwsInputs`
 * before it declares anything. `stacks/aws/examples/aws-proof.inputs.example.json`
 * is the fresh, fully hardened proof stage, kept valid by a test.
 */
import { assertServicesImage, clusterEndpointAccess, hoopdbBackend, lockerLifecycleRules, type HoopDbBackend } from "./hardening.ts";
import { findPlaceholders, type Approval } from "./deploy-guard.ts";
import { isDisposableStage, isProductionLike } from "./hardening.ts";
import { resolveTenantOrgs } from "./tenants.ts";
import { assertTranscriptStoreInputs, type TranscriptStoreInputs } from "./transcript-store.ts";
import { PIPPEN_PROOF_SANDBOX } from "./sandbox-rules.ts";

/** versions.lock.json `images.services-aws.approved`: the services images an AWS stage may run. */
export interface ApprovedServicesImage {
  repository: string;
  digest: string;
  sourceRevision: string;
}
import { assertRelaycastInputs, assertSessionDirectoryPreconditions, ingressExposure, type IngestExposureInput, type RelaycastInputs } from "./proof-addons.ts";

export interface AwsInputs {
  /** The target account id, repeated on the command line to deploy. */
  accountId: string;
  region: string;
  /** AWS ARN partition. Required and exactly `aws` for the production transcript store. */
  partition?: string;
  /** Legacy free-text clearance; informational only. Deploy and destroy need `approval`. */
  clearedBy?: string;
  /** Khaliq's approval for exact operations on this exact stage/account/region (deploy-guard.ts). */
  approval?: Approval;
  /**
   * Proof and sandbox stages only: let destroy empty the versioned buckets (every
   * version and delete marker) so `down` completes. Must be in place at deploy
   * time: Alchemy reads it from the state of the bucket being deleted.
   */
  teardown?: { purgeBucketsOnDestroy: true };
  budgetUsd: number;
  budgetEmail: string;
  /** Where HoopDb lives (RDS/Supabase URL in Secrets Manager); not created here. */
  hoopdbSecretArn?: string;
  /**
   * Account-wide resources another stage in this account already owns: the
   * `96bulls-services` ECR repository, the GitHub OIDC provider and the image
   * publisher role. Set for a second stage in the same account (e.g. `aws-proof`
   * beside `aws-sessions-dev`), which then uses that repository.
   */
  accountShared?: { ownerStage: string };
  /**
   * A Pippen Tools sandbox account (`sandbox-<agent>`): every resource carries
   * `pippen:sandbox=true` for the sweeper, every role the stage creates carries
   * the sandbox's permissions boundary, and no GitHub OIDC provider or publisher
   * role is created (the sandbox baseline forbids them).
   */
  pippenSandbox?: { permissionsBoundaryArn: string };
  /**
   * Read-only cross-account pull from this stage's `96bulls-services` ECR
   * repository (ecrPullPolicy). Each entry is a 12-digit account id that gets
   * BatchGetImage, GetDownloadUrlForLayer, BatchCheckLayerAvailability and
   * DescribeImages (read-only image metadata: the pulling stage's image check), and
   * nothing else. Omit or [] for no repository policy at all. Alchemy removes
   * a repository policy the inputs do not declare, so a live grant must be
   * listed here before the next `up`.
   */
  ecr?: { pullAccounts?: string[] };
  /** Opt-in and IRREVERSIBLE once applied: expire superseded Locker versions after this many days (>= 30). */
  lockerNoncurrentVersionDays?: number;
  /** Khaliq, 2026-10-28: EKS is the first cloud compute target. Omit to declare the baseline only. */
  eks?: {
    chartVersion: string; runtimeSecretName: string; relayauthJwksUrl: string;
    /** One tenant org (existing shape). */
    tenantOrgId?: string;
    /** Several tenant orgs; AlleyOop accepts exactly these (tenants.ts). tenantOrgIds[0] must equal tenantOrgId if both are set. */
    tenantOrgIds?: string[]; relayhistoryOrigin: string; relayauthIssuer?: string;
    relayauth?: { chartVersion: string; secretName: string };
    /** The digest publish-image-ecr pushed to the services repository. Bare `sha256:<64 hex>`; tags are refused. */
    imageDigest: string;
    /**
     * Pull the services image from another account's ECR repository by digest
     * (e.g. a fresh sandbox without its own publish). That repository's policy
     * must allow this account to pull; changing it is a separate approval.
     */
    imageRepository?: string;
    /**
     * Step 2 of the NetworkPolicy rollout. Omit for step 1. Set only after step 1
     * applied the policies and someone checked them: the hash of that exact policy
     * set (the stack prints it), who checked, and when. The stack also requires
     * Alchemy state to show every one of those policies applied.
     */
    networkPolicyEnforcement?: { policySetSha256: string; verifiedBy: string; verifiedOn: string };
    /** "keep": keep the old shared pods role (unattached) until the per-SA roles are verified; remove later. */
    legacyPodsRole?: "keep";
    /** The full 40-hex commit that image was built from. Required: the stage refuses an image without source provenance. */
    imageSourceRevision: string;
    /** EKS API: private endpoint always; public only for these operator CIDRs (omit or [] = private only; never 0.0.0.0/0). */
    apiAccess?: { publicAccessCidrs?: string[] };
    /** HoopDb: "external" (default; HOOPDB_URL from runtimeSecretName, i.e. Supabase) or "cnpg" (in-cluster, proof stages only). */
    hoopdb?: { backend?: HoopDbBackend };
    /** Relaycast core chart; chartVersion and imageDigest are inputs so 0.2.0 slots in without a code change. */
    relaycast?: RelaycastInputs;
    /** Search-path NetworkPolicies for the #73 PostgREST profile (follow-up wires the Deployment). Needs cnpg + relayauth. */
    sessionDirectory?: { enabled: boolean };
    /** Ingest hostname on Cloudflare DNS (needs CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID at deploy). scheme defaults to internet-facing; WAF always attached. */
    ingress?: { hostname: string; cloudflareZoneId: string; zoneDomain: string; cloudflareTokenSecret: string; certValidation?: { name: string; value: string } } & IngestExposureInput;
  };
  /**
   * The production transcript store (transcript-store.ts) instead of the baseline:
   * S3 only, us-east-1 only, never purged. Excludes eks, supabase, teardown,
   * accountShared, pippenSandbox and lockerNoncurrentVersionDays.
   */
  transcriptStore?: TranscriptStoreInputs;
  /** Khaliq, 2026-10-28: dev HoopDb is a Supabase project in the org the 96Bulls team owns. */
  supabase?: { organizationId: string; projectName: string; region: string; plan?: "free" | "pro"; dbPassRef: string };
}

/** Everything the EKS stage would refuse, checked up front. Returns the resolved choices. */
export function validateAwsInputs(stage: string, inputs: AwsInputs, approvedImages?: ApprovedServicesImage[]) {
  if (inputs.teardown && isProductionLike(stage))
    throw new Error(`96bulls: teardown.purgeBucketsOnDestroy is never allowed on a production-looking stage (${stage}). Nothing was created.`);
  if (inputs.teardown && !isDisposableStage(stage))
    throw new Error(`96bulls: teardown.purgeBucketsOnDestroy is only for proof and sandbox stages, not ${stage}. Nothing was created.`);
  if (inputs.teardown && inputs.teardown.purgeBucketsOnDestroy !== true) throw new Error("96bulls: teardown.purgeBucketsOnDestroy must be true when teardown is set. Nothing was created.");
  if (!/^\d{12}$/.test(inputs.accountId ?? "")) throw new Error("96bulls: accountId must be a 12-digit AWS account id. Nothing was created.");
  const placeholders = findPlaceholders(inputs);
  if (placeholders.length) throw new Error(`96bulls: aws-inputs.json still has placeholder values at ${placeholders.join(", ")}. Nothing was created.`);
  if (inputs.pippenSandbox && !new RegExp(`^arn:aws:iam::${inputs.accountId}:policy/[\\w+=,.@/-]+$`).test(inputs.pippenSandbox.permissionsBoundaryArn ?? ""))
    throw new Error("96bulls: pippenSandbox.permissionsBoundaryArn must be a policy ARN in this account. Nothing was created.");
  if (inputs.pippenSandbox && (inputs.ecr?.pullAccounts?.length ?? 0) > 0)
    throw new Error("96bulls: a Pippen sandbox stage declares no ECR repository policy (its boundary denies ecr:SetRepositoryPolicy); remove ecr.pullAccounts. Nothing was created.");
  if (inputs.pippenSandbox && inputs.accountShared) throw new Error("96bulls: a Pippen sandbox account is fresh; it cannot share another stage's account resources. Nothing was created.");
  assertPippenProofSandbox(stage, inputs);
  ecrPullPolicy(inputs);
  if (inputs.transcriptStore && inputs.ecr?.pullAccounts?.length) throw new Error("96bulls: ecr.pullAccounts needs a stage with the services repository; the transcript store has none. Nothing was created.");
  lockerLifecycleRules(inputs.lockerNoncurrentVersionDays);
  assertTranscriptStoreInputs(stage, inputs as unknown as Parameters<typeof assertTranscriptStoreInputs>[1]);
  if (inputs.accountShared) {
    const owner = inputs.accountShared.ownerStage;
    if (!/^aws-[a-z0-9-]+$/.test(owner ?? "") || owner === stage)
      throw new Error(`96bulls: accountShared.ownerStage must name another aws-* stage in this account, got ${JSON.stringify(owner)}. Nothing was created.`);
  }
  const eks = inputs.eks;
  if (!eks) return { endpointAccess: undefined, hoopdb: undefined };
  if (eks.relayauth && eks.relayauthIssuer)
    throw new Error("96bulls: eks.relayauth (self-hosted) and eks.relayauthIssuer are both set; the self-hosted issuer is the in-cluster RelayAuth. Remove relayauthIssuer. Nothing was created.");
  assertServicesImage(eks);
  resolveTenantOrgs(eks);
  if (eks.imageRepository !== undefined && !/^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com\/96bulls-services$/.test(eks.imageRepository))
    throw new Error(`96bulls: eks.imageRepository must be an ECR 96bulls-services repository URI, got ${JSON.stringify(eks.imageRepository)}. Nothing was created.`);
  if (eks.legacyPodsRole !== undefined && eks.legacyPodsRole !== "keep") throw new Error('96bulls: eks.legacyPodsRole may only be "keep". Nothing was created.');
  if (typeof eks.networkPolicyEnforcement === "boolean")
    throw new Error("96bulls: eks.networkPolicyEnforcement is a verification record, not a boolean: { policySetSha256, verifiedBy, verifiedOn } from a checked phase-1 apply. Nothing was created.");
  if (approvedImages) {
    const repository = eks.imageRepository ?? sharedServicesRepository(inputs.accountId, inputs.region);
    const hit = approvedImages.find((a) => a.digest === eks.imageDigest && a.sourceRevision === eks.imageSourceRevision && a.repository === repository);
    if (!hit)
      throw new Error(`96bulls: services image ${eks.imageDigest} from ${eks.imageSourceRevision} is not an approved services-aws entry in versions.lock.json for ${repository}. Nothing was created.`);
  }
  const endpointAccess = clusterEndpointAccess(eks.apiAccess?.publicAccessCidrs);
  const hoopdb = hoopdbBackend(stage, eks.hoopdb?.backend);
  if (eks.relaycast) assertRelaycastInputs(eks.relaycast);
  if (eks.sessionDirectory?.enabled) assertSessionDirectoryPreconditions({ hoopdb, relayauthTls: Boolean(eks.relayauth) });
  if (eks.ingress) ingressExposure({ ...(eks.ingress.scheme ? { scheme: eks.ingress.scheme } : {}), ...(eks.ingress.rateLimitPer5Min !== undefined ? { rateLimitPer5Min: eks.ingress.rateLimitPer5Min } : {}) });
  return { endpointAccess, hoopdb };
}

/**
 * The Pippen sandbox proof stage and its account are pinned to each other
 * (sandbox-rules.ts PIPPEN_PROOF_SANDBOX): aws-proof-sandbox only in
 * 361835732085/us-west-2, always as a Pippen sandbox (permissions boundary on
 * every role, sweeper tag on every resource, never accountShared), and that
 * account hosts no other stage.
 */
export function assertPippenProofSandbox(stage: string, inputs: Pick<AwsInputs, "accountId" | "region" | "pippenSandbox" | "accountShared">): void {
  const sbx = PIPPEN_PROOF_SANDBOX;
  if (stage !== sbx.stage) {
    if (inputs.accountId === sbx.accountId)
      throw new Error(`96bulls: account ${sbx.accountId} is the Pippen sandbox of agent ${sbx.agent}; only stage ${sbx.stage} may target it, not ${stage}. Nothing was created.`);
    return;
  }
  if (inputs.accountId !== sbx.accountId || inputs.region !== sbx.region)
    throw new Error(`96bulls: stage ${sbx.stage} runs only in the Pippen sandbox ${sbx.accountId}/${sbx.region}, not ${inputs.accountId}/${inputs.region}. Nothing was created.`);
  if (!inputs.pippenSandbox)
    throw new Error(`96bulls: stage ${sbx.stage} needs pippenSandbox.permissionsBoundaryArn (the sandbox boundary in ${sbx.accountId}). Nothing was created.`);
  if (inputs.accountShared)
    throw new Error(`96bulls: stage ${sbx.stage} is a fresh Pippen sandbox; it cannot set accountShared. Nothing was created.`);
}

/** The production account. It never pulls from (or is granted on) a non-production repository. */
export const PRODUCTION_ACCOUNT_ID = "941433935742";
/** The Pippen sandbox proof account's grant keeps the Sid Khaliq approved and applied (2026-11-01). */
const PULL_SIDS: Readonly<Record<string, string>> = Object.freeze({ [PIPPEN_PROOF_SANDBOX.accountId]: "PippenSandboxAwsProofPull" });
/** Pull, plus ecr:DescribeImages: the pulling stage's preflight (preflight.ts verifyServicesImage) reads the pinned digest's tags. */
export const ECR_PULL_ACTIONS = Object.freeze(["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability", "ecr:DescribeImages"] as const);

/**
 * The services repository policy for `ecr.pullAccounts`, as the exact JSON
 * text the stack sets, or undefined (no policy) when the list is absent or
 * empty. One read-only statement per account, principal `<id>:root`. Throws
 * on anything else: a non-12-digit id or wildcard, a duplicate, the stage's
 * own account, the production account, or a stage that owns no repository.
 */
export function ecrPullPolicy(inputs: Pick<AwsInputs, "accountId" | "accountShared" | "ecr">): string | undefined {
  const fail = (why: string): never => {
    throw new Error(`96bulls: ecr.pullAccounts ${why}. Nothing was created.`);
  };
  if (inputs.ecr === undefined) return undefined;
  if (typeof inputs.ecr !== "object" || inputs.ecr === null || Object.keys(inputs.ecr).some((k) => k !== "pullAccounts")) fail("must be the only key of ecr");
  const ids = inputs.ecr.pullAccounts;
  if (ids === undefined) return undefined;
  if (!Array.isArray(ids)) return fail("must be an array of 12-digit account ids");
  if (ids.length === 0) return undefined;
  if (inputs.accountShared) fail("needs the stage that owns the 96bulls-services repository; this stage uses accountShared");
  for (const id of ids) {
    if (typeof id !== "string" || !/^\d{12}$/.test(id)) fail(`entries must be 12-digit account ids, got ${JSON.stringify(id)}`);
    if (id === inputs.accountId) fail(`must not list the stage's own account ${id}`);
    if (id === PRODUCTION_ACCOUNT_ID) fail(`must never list the production account ${id}`);
  }
  if (new Set(ids).size !== ids.length) fail("must not repeat an account");
  return JSON.stringify({
    Version: "2012-10-17",
    Statement: ids.map((id) => ({ Sid: PULL_SIDS[id] ?? `CrossAccountPull${id}`, Effect: "Allow", Principal: { AWS: `arn:aws:iam::${id}:root` }, Action: [...ECR_PULL_ACTIONS] })),
  });
}

/** This account's services repository, when another stage owns it. */
export const sharedServicesRepository = (accountId: string, region: string) => `${accountId}.dkr.ecr.${region}.amazonaws.com/96bulls-services`;
