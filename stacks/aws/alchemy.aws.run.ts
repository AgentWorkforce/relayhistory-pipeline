/**
 * 96bulls AWS stage: DRAFT, CODE ONLY. Nothing here has been planned or
 * applied against any account, and it refuses to run until the account work
 * is cleared by Khaliq (docs/aws-stage.md). Kept in its own Alchemy config so
 * the local stack never loads AWS providers or looks for credentials.
 *
 *   bin/96bulls plan aws-sandbox     -> alchemy plan   --config stacks/aws/alchemy.aws.run.ts --stage aws-sandbox
 *   bin/96bulls up   aws-sandbox     -> refused until cleared
 *
 * What it declares (account baseline + Locker), mirroring the local stage:
 *   KMS key + alias, Locker bucket (versioned, SSE-KMS, no public access),
 *   CloudTrail (own versioned bucket, SSE-KMS with the trail's own key), monthly
 *   budget, GitHub OIDC provider and a 96bulls-deployer role trusted only for
 *   this repository; with `eks`, the hardened EKS proof stage (eks.ts).
 *   Hosted production HoopDb is Supabase and production AWS is S3 only
 *   (docs/decisions/0001-production-data-plane.md); the in-cluster CNPG HoopDb
 *   is for the isolated proof stage.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as AWS from "@alchemy.run/aws";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { SANDBOX_RULES, assertRegion } from "./sandbox-rules.ts";
import { eksStage } from "./eks.ts";
import { denyInsecureTransport, ecrPushPolicy, githubImagePublishTrust, lockerLifecycleRules, trailKeyPolicy, type HoopDbBackend } from "./hardening.ts";
import type { IngestExposureInput, RelaycastInputs } from "./proof-addons.ts";
import { Project as SupabaseProject, SupabaseProvider } from "../supabase/project.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const REPO = "AgentWorkforce/96BullSessions";
/**
 * The repo has GitHub immutable OIDC subjects on, so the sub carries numeric ids:
 * `repo:AgentWorkforce@252050005/96BullSessions@1437714333:environment:<env>`
 * (checked with `gh api repos/AgentWorkforce/96BullSessions/actions/oidc/customization/sub`).
 */
const OIDC_SUB_PREFIX = "repo:AgentWorkforce@252050005/96BullSessions@1437714333";

/** Written by hand (not by the CLI) once Khaliq clears the account: see docs/aws-stage.md. */
interface AwsInputs {
  /** The target account id, repeated on the command line to deploy. */
  accountId: string;
  region: string;
  /** Khaliq's words, verbatim, with the date: the record that the stage was cleared. */
  clearedBy: string;
  budgetUsd: number;
  budgetEmail: string;
  /** Where HoopDb lives (RDS/Supabase URL in Secrets Manager); not created here. */
  hoopdbSecretArn?: string;
  /** Khaliq, 2026-10-28: EKS is the first cloud compute target. Omit to declare the baseline only. */
  eks?: {
    chartVersion: string; runtimeSecretName: string; tenantOrgId: string; relayauthJwksUrl: string; relayhistoryOrigin: string; relayauthIssuer?: string;
    relayauth?: { chartVersion: string; secretName: string };
    /** The digest publish-image-ecr pushed to THIS account's repository (each account builds its own). Bare `sha256:<64 hex>`; tags are refused. */
    imageDigest: string;
    /** The full 40-hex commit that image was built from. Required: the stage refuses an image without source provenance. */
    imageSourceRevision: string;
    /** EKS API: private endpoint always; public only for these operator CIDRs (omit or [] = private only; never 0.0.0.0/0). */
    apiAccess?: { publicAccessCidrs?: string[] };
    /** HoopDb: "external" (default; HOOPDB_URL from runtimeSecretName, i.e. Supabase) or "cnpg" (in-cluster, proof stage only). */
    hoopdb?: { backend?: HoopDbBackend };
    /** Relaycast core chart; chartVersion and imageDigest are inputs so 0.2.0 slots in without a code change. */
    relaycast?: RelaycastInputs;
    /** Search-path NetworkPolicies for the #73 PostgREST profile (follow-up wires the Deployment). Needs cnpg + relayauth. */
    sessionDirectory?: { enabled: boolean };
    /** Ingest hostname on Cloudflare DNS (needs CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID at deploy). scheme defaults to internet-facing; WAF always attached. */
    ingress?: { hostname: string; cloudflareZoneId: string; zoneDomain: string; cloudflareTokenSecret: string; certValidation?: { name: string; value: string } } & IngestExposureInput;
  };
  /** Khaliq, 2026-10-28: dev HoopDb is a Supabase project in the org the 96Bulls team owns. */
  supabase?: { organizationId: string; projectName: string; region: string; plan?: "free" | "pro"; dbPassRef: string };
}

function readInputs(stage: string): AwsInputs {
  const path = join(ROOT, ".96bulls", stage, "aws-inputs.json");
  let inputs: AwsInputs;
  try {
    inputs = JSON.parse(readFileSync(path, "utf8")) as AwsInputs;
  } catch {
    throw new Error(`96bulls: the AWS stage ${stage} is not cleared: ${path} does not exist (see docs/aws-stage.md). Nothing was created.`);
  }
  if (!inputs.clearedBy?.trim()) throw new Error(`96bulls: ${path} has no clearedBy; the AWS stage is not cleared`);
  if (process.env.NINETY_SIX_BULLS_CONFIRM_ACCOUNT !== inputs.accountId)
    throw new Error("96bulls: set NINETY_SIX_BULLS_CONFIRM_ACCOUNT=<account id> to run the AWS stage; nothing was created");
  assertRegion(inputs.region);
  return inputs;
}

const tags = (stage: string, component: string) => ({ "96bulls:stack": stage, "96bulls:component": component, "96bulls:repo": REPO });

export default Alchemy.Stack(
  "96bulls-aws",
  {
    providers: Layer.mergeAll(AWS.providers(), Kubernetes.providers(), Cloudflare.providers(), SupabaseProvider()),
    state: AWS.state({ prefix: "96bulls" }),
  },
  Effect.gen(function* () {
    const stage = yield* Stage;
    if (!stage.startsWith("aws-")) throw new Error(`96bulls: this config is for aws-* stages, got ${stage}`);
    const inputs = readInputs(stage);
    const name = (base: string) => `96bulls-${stage}-${base}`;

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
      versioning: "Enabled",
      encryption: { sseAlgorithm: "aws:kms", kmsMasterKeyId: key.keyArn, bucketKeyEnabled: true },
      publicAccessBlock: { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true },
      // TLS only; superseded manifest versions and abandoned uploads expire.
      policy: denyInsecureTransport(`arn:aws:s3:::${name("locker")}-${inputs.accountId}`),
      lifecycleRules: lockerLifecycleRules() as never,
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
    const ecr = yield* AWS.ECR.Repository("services-image", {
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
    // Sandbox accounts get neither (their boundary forbids new OIDC providers and trust edits).
    const deployer = isSandbox
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
          tenantOrgId: inputs.eks.tenantOrgId,
          relayauthJwksUrl: inputs.eks.relayauthJwksUrl,
          relayhistoryOrigin: inputs.eks.relayhistoryOrigin,
          ...(inputs.eks.relayauthIssuer ? { relayauthIssuer: inputs.eks.relayauthIssuer } : {}),
          ...(inputs.eks.relayauth ? { relayauth: inputs.eks.relayauth } : {}),
          imageRepository: ecr.repositoryUri as unknown as string,
          imageDigest: inputs.eks.imageDigest,
          imageSourceRevision: inputs.eks.imageSourceRevision,
          ...(inputs.eks.apiAccess ? { apiAccess: inputs.eks.apiAccess } : {}),
          ...(inputs.eks.hoopdb ? { hoopdb: inputs.eks.hoopdb } : {}),
          ...(inputs.eks.relaycast ? { relaycast: inputs.eks.relaycast } : {}),
          ...(inputs.eks.sessionDirectory ? { sessionDirectory: inputs.eks.sessionDirectory } : {}),
          ...(inputs.eks.ingress ? { ingress: inputs.eks.ingress } : {}),
          lockerBucketArn: `arn:aws:s3:::${name("locker")}-${inputs.accountId}`,
          lockerKmsKeyArn: key.keyArn as unknown as string,
          ...(isSandbox ? { permissionsBoundaryArn: `arn:aws:iam::${inputs.accountId}:policy/${SANDBOX_RULES.permissionsBoundary}` } : {}),
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
      imageRepository: ecr.repositoryUri,
      deployerRoleArn: deployer?.roleArn ?? null,
      hoopdbSecretArn: inputs.hoopdbSecretArn ?? null,
    };
  }),
);
