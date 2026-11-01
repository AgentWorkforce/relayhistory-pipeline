/**
 * Guarded 96bulls AWS stage. The aws-sessions-prod transcript store was applied
 * in two passes on 2026-10-31 (runbooks/sessions-prod-transcript-store.md);
 * other stages have their own evidence. Kept in its own Alchemy config so the
 * local stack never loads AWS providers or looks for credentials.
 *
 *   bin/96bulls plan aws-sandbox     -> alchemy plan   --config stacks/aws/alchemy.aws.run.ts --stage aws-sandbox
 *   bin/96bulls up   aws-sandbox     -> refused until cleared
 *
 * What it declares (account baseline + Locker), mirroring the local stage:
 *   KMS key + alias, Locker bucket (versioned, SSE-KMS, no public access),
 *   CloudTrail (own versioned bucket, SSE-KMS with the trail's own key), monthly
 *   budget, GitHub OIDC provider and a 96bulls-deployer role trusted only for
 *   this repository; with `eks`, the hardened EKS proof stage (eks.ts).
 *   With `transcriptStore` (aws-sessions-prod): the production transcript bucket
 *   only, with its CMK, access logs, CloudTrail data events, budget and IAM
 *   users (transcript-store.ts), instead of the baseline.
 *   Hosted production HoopDb is Supabase and production AWS is S3 only
 *   (docs/decisions/0001-production-data-plane.md); the in-cluster CNPG HoopDb
 *   is for the isolated proof stage.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as AWS from "@alchemy.run/aws";
import { getAccountId } from "@alchemy.run/aws/AuthProvider";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { PIPPEN_PROOF_SANDBOX, pippenProofSandboxCallerArn, SANDBOX_RULES, assertRegion } from "./sandbox-rules.ts";
import { eksStage } from "./eks.ts";
import { denyInsecureTransport, ecrPushPolicy, githubImagePublishTrust, lockerLifecycleRules, trailKeyPolicy } from "./hardening.ts";
import { sharedServicesRepository, validateAwsInputs, type ApprovedServicesImage, type AwsInputs } from "./inputs.ts";
import { assertApprovalBinding, assertAwsDeploymentTarget, assertClearance, awsMode, type AwsMode } from "./deploy-guard.ts";
import { snapshotStateLayer } from "./state-snapshot.ts";
import { verifyDeployCapture } from "./capture-verify.ts";
import { alchemyStateBucket, alchemyStatePrefix, awsCliRunner, verifyAccountOwner, verifyCallerArn, verifyNetworkPolicyPhase, verifyServicesImage } from "./preflight.ts";
import { networkPolicies } from "./proof-addons.ts";
import { hoopdbBackend } from "./hardening.ts";
import { spawnSync } from "node:child_process";
import { Project as SupabaseProject, SupabaseProvider } from "../supabase/project.ts";
import { assertTranscriptStorePlanContract, TRANSCRIPT_STAGE, transcriptStoreStage } from "./transcript-store.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const REPO = "AgentWorkforce/96BullSessions";
/**
 * The repo has GitHub immutable OIDC subjects on, so the sub carries numeric ids:
 * `repo:AgentWorkforce@252050005/96BullSessions@1437714333:environment:<env>`
 * (checked with `gh api repos/AgentWorkforce/96BullSessions/actions/oidc/customization/sub`).
 */
const OIDC_SUB_PREFIX = "repo:AgentWorkforce@252050005/96BullSessions@1437714333";

/** Written by hand (not by the CLI) once Khaliq clears the account: see docs/aws-stage.md and stacks/aws/inputs.ts. */
function readInputs(stage: string): AwsInputs & { mode: AwsMode } {
  const path = join(ROOT, ".96bulls", stage, "aws-inputs.json");
  let inputs: AwsInputs;
  let inputsText: string;
  try {
    inputsText = readFileSync(path, "utf8");
    inputs = JSON.parse(inputsText) as AwsInputs;
  } catch {
    throw new Error(`96bulls: the AWS stage ${stage} is not cleared: ${path} does not exist (see docs/aws-stage.md). Nothing was created.`);
  }
  // The operation comes from Alchemy's own argv, so a direct `alchemy destroy` is gated too.
  const mode = awsMode(process.env, process.argv);
  if (process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT && mode !== "plan")
    throw new Error(`96bulls: NINETY_SIX_BULLS_STATE_SNAPSHOT is for plans only; a ${mode} must use the live state. Nothing was changed.`);
  // Placeholders anywhere, the typed account (and stage), Khaliq's approval record, destroyable stage.
  assertClearance(mode, stage, inputs, process.env);
  // A deploy approval is for one reviewed capture (docs/plans/<stage>.*), not for any plan.
  if (mode === "deploy") {
    const plans = join(ROOT, "docs", "plans");
    let provenanceText: string, planText: string, detailedPlanText: string;
    try {
      provenanceText = readFileSync(join(plans, `${stage}.provenance.json`), "utf8");
      planText = readFileSync(join(plans, `${stage}.plan.txt`), "utf8");
      detailedPlanText = readFileSync(join(plans, `${stage}.plan.detailed.txt`), "utf8");
    } catch {
      throw new Error(`96bulls: deploy needs the reviewed capture docs/plans/${stage}.{provenance.json,plan.txt,plan.detailed.txt}. Nothing was changed.`);
    }
    assertApprovalBinding(inputs.approval?.capture, { provenanceText, planText, inputsText });
    if (stage === TRANSCRIPT_STAGE) assertTranscriptStorePlanContract(planText, { detailedPlanText, provenanceText });
    // And this deploy really is that capture: source commit and tree, live state, plan files,
    // inputs. Here, not only in bin/96bulls, so a direct `alchemy deploy` is held to it too.
    verifyDeployCapture(
      {
        root: ROOT,
        run: (cmd, args) => {
          const r = spawnSync(cmd, args, { cwd: ROOT, encoding: "utf8", env: process.env, maxBuffer: 256 * 1024 * 1024 });
          if (r.status !== 0) throw new Error(`${cmd} ${args.slice(0, 2).join(" ")} failed: ${(r.stderr || "").trim().split("\n").pop()}`);
          return r.stdout;
        },
        readFile: (path) => { try { return readFileSync(path, "utf8"); } catch { return undefined; } },
      },
      stage,
      inputsText,
    );
  }
  assertRegion(inputs.region);
  const lock = JSON.parse(readFileSync(join(ROOT, "versions.lock.json"), "utf8")) as { images: { "services-aws"?: { approved: ApprovedServicesImage[] } } };
  validateAwsInputs(stage, inputs, lock.images["services-aws"]?.approved ?? []);
  return { ...inputs, mode };
}

/** Pippen Tools' sweeper deletes what carries `pippen:sandbox=true` (pippen-tools docs/architecture.md). */
let sandboxTag: Record<string, string> = {};
const tags = (stage: string, component: string) => ({ "96bulls:stack": stage, "96bulls:component": component, "96bulls:repo": REPO, ...sandboxTag });

export default Alchemy.Stack(
  "96bulls-aws",
  {
    providers: Layer.mergeAll(AWS.providers(), Kubernetes.providers(), Cloudflare.providers(), SupabaseProvider()),
    // A captured plan runs against a read-only snapshot (state-snapshot.ts): AWS.state writes a
    // lease and may reconcile the bucket even for `alchemy plan`.
    state: process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT
      ? snapshotStateLayer(process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT)
      : AWS.state({ prefix: "96bulls" }),
  },
  Effect.gen(function* () {
    const stage = yield* Stage;
    if (!stage.startsWith("aws-")) throw new Error(`96bulls: this config is for aws-* stages, got ${stage}`);
    const inputs = readInputs(stage);
    sandboxTag = inputs.pippenSandbox ? { "pippen:sandbox": "true" } : {};
    // Before the first resource: the credentials the providers will use must belong to the
    // inputs' account, and the providers' region must be the inputs' region.
    const awsEnvironment = yield* AWS.AWSEnvironment.current;
    const liveCredentials = yield* awsEnvironment.credentials.pipe(Effect.orDie);
    const callerAccountId = yield* getAccountId(liveCredentials as never).pipe(Effect.orDie);
    assertAwsDeploymentTarget({ accountId: inputs.accountId, region: inputs.region }, { accountId: callerAccountId as string, region: awsEnvironment.region as string });
    // The Pippen sandbox proof stage, every mode: only this lease's agent-session role session may act there.
    if (stage === PIPPEN_PROOF_SANDBOX.stage)
      verifyCallerArn(awsCliRunner((cmd, args) => spawnSync(cmd, args, { encoding: "utf8", env: process.env })), pippenProofSandboxCallerArn(process.env));
    // Read-only AWS checks with the same exported credentials (preflight.ts). Skipped on destroy:
    // tearing down must not depend on the image or the policy phase still being valid.
    if (inputs.mode !== "destroy") {
      const aws = awsCliRunner((cmd, args) => spawnSync(cmd, args, { encoding: "utf8", env: process.env }));
      if (inputs.eks)
        verifyServicesImage(aws, {
          repositoryUri: inputs.eks.imageRepository ?? sharedServicesRepository(inputs.accountId, inputs.region),
          imageDigest: inputs.eks.imageDigest,
          imageSourceRevision: inputs.eks.imageSourceRevision,
        });
      if (inputs.accountShared) verifyAccountOwner(aws, { accountId: inputs.accountId, region: inputs.region, ownerStage: inputs.accountShared.ownerStage });
      if (inputs.eks?.networkPolicyEnforcement)
        verifyNetworkPolicyPhase(aws, {
          stateBucket: alchemyStateBucket(inputs.accountId, inputs.region),
          statePrefix: alchemyStatePrefix(stage),
          region: inputs.region,
          policies: networkPolicies({
            hoopdb: hoopdbBackend(stage, inputs.eks.hoopdb?.backend),
            relayauth: Boolean(inputs.eks.relayauth),
            relaycast: Boolean(inputs.eks.relaycast),
            sessionDirectory: Boolean(inputs.eks.sessionDirectory?.enabled),
            ingestScheme: inputs.eks.ingress?.scheme ?? "internet-facing",
          }),
          record: inputs.eks.networkPolicyEnforcement,
        });
    }
    const purge = inputs.teardown?.purgeBucketsOnDestroy === true;
    const name = (base: string) => `96bulls-${stage}-${base}`;

    // Production transcript store (transcript-store.ts): S3 only (decision 0001), so none of the
    // baseline below (Locker, ECR, GitHub OIDC, EKS, Supabase). validateAwsInputs refused those inputs.
    if (stage === TRANSCRIPT_STAGE) {
      const store = yield* transcriptStoreStage({
        stage,
        accountId: inputs.accountId,
        region: inputs.region,
        budgetUsd: inputs.budgetUsd,
        budgetEmail: inputs.budgetEmail,
        transcriptStore: inputs.transcriptStore!,
        tags: (component) => tags(stage, component),
      });
      return { stage, accountId: inputs.accountId, region: inputs.region, transcriptStore: store };
    }

    // Account baseline: one KMS key for everything we store.
    const key = yield* AWS.KMS.Key("locker-key", {
      description: `96bulls ${stage}: Locker and logs`,
      enableKeyRotation: true,
      deletionWindow: "30 days",
      tags: tags(stage, "kms"),
    });
    yield* AWS.KMS.Alias("locker-key-alias", { aliasName: `alias/${name("locker")}`, targetKeyId: key.keyId });

    // Locker: S3 behind the adapter. Versioned, encrypted, never public.
    const locker = yield* AWS.S3.Bucket("locker", {
      bucketName: `${name("locker")}-${inputs.accountId}`,
      // Proof/sandbox stages with teardown.purgeBucketsOnDestroy only: destroy empties every version.
      ...(purge ? { forceDestroy: true } : {}),
      versioning: "Enabled",
      encryption: { sseAlgorithm: "aws:kms", kmsMasterKeyId: key.keyArn, bucketKeyEnabled: true },
      publicAccessBlock: { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true },
      // TLS only; superseded manifest versions and abandoned uploads expire.
      policy: denyInsecureTransport(`arn:aws:s3:::${name("locker")}-${inputs.accountId}`),
      lifecycleRules: lockerLifecycleRules(inputs.lockerNoncurrentVersionDays) as never,
      tags: tags(stage, "locker"),
    });

    // CloudTrail into its own versioned bucket, encrypted with the trail's own key: the key
    // policy lets CloudTrail generate data keys for this trail only (hardening.ts trailKeyPolicy).
    const trailKey = yield* AWS.KMS.Key("trail-key", {
      description: `96bulls ${stage}: CloudTrail log files`,
      enableKeyRotation: true,
      deletionWindow: "30 days",
      policy: JSON.stringify(trailKeyPolicy(inputs.accountId, inputs.region, name("trail"))),
      tags: tags(stage, "cloudtrail"),
    });
    yield* AWS.KMS.Alias("trail-key-alias", { aliasName: `alias/${name("trail")}`, targetKeyId: trailKey.keyId });
    const trailBucket = yield* AWS.S3.Bucket("trail-logs", {
      bucketName: `${name("trail")}-${inputs.accountId}`,
      ...(purge ? { forceDestroy: true } : {}),
      versioning: "Enabled",
      encryption: { sseAlgorithm: "aws:kms", kmsMasterKeyId: trailKey.keyArn, bucketKeyEnabled: true },
      publicAccessBlock: { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true },
      // Bucket `policy` takes the statements only.
      policy: [
          ...denyInsecureTransport(`arn:aws:s3:::${name("trail")}-${inputs.accountId}`),
          { Sid: "AWSCloudTrailAclCheck", Effect: "Allow", Principal: { Service: "cloudtrail.amazonaws.com" }, Action: ["s3:GetBucketAcl"], Resource: `arn:aws:s3:::${name("trail")}-${inputs.accountId}` },
          { Sid: "AWSCloudTrailWrite", Effect: "Allow", Principal: { Service: "cloudtrail.amazonaws.com" }, Action: ["s3:PutObject"], Resource: `arn:aws:s3:::${name("trail")}-${inputs.accountId}/AWSLogs/${inputs.accountId}/*`, Condition: { StringEquals: { "s3:x-amz-acl": "bucket-owner-full-control" } } },
      ],
      tags: tags(stage, "cloudtrail"),
    });
    yield* AWS.CloudTrail.Trail("trail", {
      trailName: name("trail"),
      s3BucketName: trailBucket.bucketName,
      isMultiRegionTrail: true,
      enableLogFileValidation: true,
      kmsKeyId: trailKey.keyArn as unknown as string,
      isLogging: true,
      tags: tags(stage, "cloudtrail"),
    });

    // The budget cap: sandbox stages use the settled $50 ceiling with the $25 alarm; other stages what inputs say.
    const isSandbox = stage.startsWith("aws-sandbox-");
    const budgetUsd = isSandbox ? SANDBOX_RULES.budgetUsd : inputs.budgetUsd;
    const alarmUsd = isSandbox ? SANDBOX_RULES.budgetAlarmUsd : Math.round(inputs.budgetUsd * 0.8);
    yield* AWS.Budgets.Budget("budget", {
      budgetName: name("monthly"),
      budgetType: "COST",
      timeUnit: "MONTHLY",
      budgetLimit: { amount: String(budgetUsd), unit: "USD" },
      notifications: [
        { notificationType: "ACTUAL", comparisonOperator: "GREATER_THAN", threshold: alarmUsd, thresholdType: "ABSOLUTE_VALUE", subscribers: [{ subscriptionType: "EMAIL", address: inputs.budgetEmail }] },
        { notificationType: "FORECASTED", comparisonOperator: "GREATER_THAN", threshold: budgetUsd, thresholdType: "ABSOLUTE_VALUE", subscribers: [{ subscriptionType: "EMAIL", address: inputs.budgetEmail }] },
      ],
    });

    // ECR: the hosted service image lives in the account, private (Khaliq, 2026-10-28: not public).
    // A second stage in the account (accountShared) uses the owner stage's repository.
    const ecr = inputs.accountShared
      ? undefined
      : yield* AWS.ECR.Repository("services-image", {
          repositoryName: "96bulls-services",
          imageTagMutability: "IMMUTABLE",
          scanOnPush: true,
          // Expire only untagged leftovers (failed or superseded multi-arch pushes). Published images are
          // tagged sha-<commit> and never expire automatically, so a pinned, deployed digest is never deleted.
          lifecyclePolicyText: JSON.stringify({
            rules: [{ rulePriority: 1, description: "expire untagged images after 14 days", selection: { tagStatus: "untagged", countType: "sinceImagePushed", countUnit: "days", countNumber: 14 }, action: { type: "expire" } }],
          }),
          tags: tags(stage, "ecr"),
        });

    // GitHub OIDC: the only way CI reaches the account, and only to push the services image.
    // Sandbox accounts get neither (their boundary forbids new OIDC providers and trust edits), and
    // a stage sharing the account (accountShared) relies on the owner stage's provider and role.
    const deployer = isSandbox || inputs.accountShared || inputs.pippenSandbox
      ? undefined
      : yield* Effect.gen(function* () {
          const oidc = yield* AWS.IAM.OpenIDConnectProvider("github-oidc", {
            url: "https://token.actions.githubusercontent.com",
            clientIDList: ["sts.amazonaws.com"],
            tags: tags(stage, "iam"),
          });
          return yield* AWS.IAM.Role("deployer", {
            roleName: name("deployer"),
            description: `96bulls ${stage}: ECR push of the services image, assumed only by ${REPO} jobs in the image-publish environment`,
            assumeRolePolicyDocument: githubImagePublishTrust(oidc.openIDConnectProviderArn as unknown as string, OIDC_SUB_PREFIX) as never,
            // ECR push only: no Locker, no KMS (hardening.ts ecrPushPolicy).
            inlinePolicies: { ecrPush: ecrPushPolicy(inputs.region, inputs.accountId) as never },
            maxSessionDuration: "1 hour",
            tags: tags(stage, "iam"),
          });
        });

    // HoopDb for this stage: a Supabase project (dev), resolved password via op:// at create time only.
    const hoopdb = inputs.supabase
      ? yield* SupabaseProject("hoopdb", {
          organizationId: inputs.supabase.organizationId,
          name: inputs.supabase.projectName,
          region: inputs.supabase.region,
          plan: inputs.supabase.plan ?? "free",
          dbPassRef: inputs.supabase.dbPassRef,
        })
      : undefined;

    // Compute: EKS with the 96bulls umbrella chart.
    const eks = inputs.eks
      ? yield* eksStage({
          stage,
          accountId: inputs.accountId,
          chartVersion: inputs.eks.chartVersion,
          runtimeSecretName: inputs.eks.runtimeSecretName,
          lockerBucket: `${name("locker")}-${inputs.accountId}`,
          lockerRegion: inputs.region,
          ...(inputs.eks.tenantOrgId !== undefined ? { tenantOrgId: inputs.eks.tenantOrgId } : {}),
          ...(inputs.eks.tenantOrgIds ? { tenantOrgIds: inputs.eks.tenantOrgIds } : {}),
          relayauthJwksUrl: inputs.eks.relayauthJwksUrl,
          relayhistoryOrigin: inputs.eks.relayhistoryOrigin,
          ...(inputs.eks.relayauthIssuer ? { relayauthIssuer: inputs.eks.relayauthIssuer } : {}),
          ...(inputs.eks.relayauth ? { relayauth: inputs.eks.relayauth } : {}),
          imageRepository: inputs.eks.imageRepository ?? (ecr ? (ecr.repositoryUri as unknown as string) : sharedServicesRepository(inputs.accountId, inputs.region)),
          ...(inputs.eks.networkPolicyEnforcement ? { networkPolicyEnforcement: true } : {}),
          ...(purge ? { purgeOnDestroy: true } : {}),
          ...(inputs.eks.legacyPodsRole ? { legacyPodsRole: inputs.eks.legacyPodsRole } : {}),
          imageDigest: inputs.eks.imageDigest,
          imageSourceRevision: inputs.eks.imageSourceRevision,
          ...(inputs.eks.apiAccess ? { apiAccess: inputs.eks.apiAccess } : {}),
          ...(inputs.eks.hoopdb ? { hoopdb: inputs.eks.hoopdb } : {}),
          ...(inputs.eks.relaycast ? { relaycast: inputs.eks.relaycast } : {}),
          ...(inputs.eks.sessionDirectory ? { sessionDirectory: inputs.eks.sessionDirectory } : {}),
          ...(inputs.eks.ingress ? { ingress: inputs.eks.ingress } : {}),
          lockerBucketArn: `arn:aws:s3:::${name("locker")}-${inputs.accountId}`,
          lockerKmsKeyArn: key.keyArn as unknown as string,
          ...(inputs.pippenSandbox
            ? { permissionsBoundaryArn: inputs.pippenSandbox.permissionsBoundaryArn }
            : isSandbox
              ? { permissionsBoundaryArn: `arn:aws:iam::${inputs.accountId}:policy/${SANDBOX_RULES.permissionsBoundary}` }
              : {}),
          tags: tags(stage, "eks"),
        })
      : undefined;

    return {
      stage,
      hoopdb: hoopdb ? { ref: hoopdb.ref, restUrl: hoopdb.restUrl } : null,
      eks: eks ?? null,
      accountId: inputs.accountId,
      region: inputs.region,
      locker: { bucket: locker.bucketName, kmsKeyArn: key.keyArn },
      imageRepository: ecr ? ecr.repositoryUri : sharedServicesRepository(inputs.accountId, inputs.region),
      deployerRoleArn: deployer?.roleArn ?? null,
      hoopdbSecretArn: inputs.hoopdbSecretArn ?? null,
    };
  }),
);
