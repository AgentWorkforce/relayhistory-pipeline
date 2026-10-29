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
 *   CloudTrail (own bucket), monthly budget, GitHub OIDC provider and a
 *   96bulls-deployer role trusted only for this repository. HoopDb (RDS or
 *   Supabase, Q5) and compute (ECS Fargate first, EKS via the chart later) are
 *   deliberately absent until decided.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as AWS from "@alchemy.run/aws";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const REPO = "AgentWorkforce/96BullSessions";

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
}

function readInputs(stage: string): AwsInputs {
  const path = join(ROOT, ".96bulls", stage, "aws-inputs.json");
  const inputs = JSON.parse(readFileSync(path, "utf8")) as AwsInputs;
  if (!inputs.clearedBy?.trim()) throw new Error(`96bulls: ${path} has no clearedBy; the AWS stage is not cleared`);
  if (process.env.NINETY_SIX_BULLS_CONFIRM_ACCOUNT !== inputs.accountId)
    throw new Error("96bulls: set NINETY_SIX_BULLS_CONFIRM_ACCOUNT=<account id> to run the AWS stage; nothing was created");
  return inputs;
}

const tags = (stage: string, component: string) => ({ "96bulls:stack": stage, "96bulls:component": component, "96bulls:repo": REPO });

export default Alchemy.Stack(
  "96bulls-aws",
  {
    providers: Layer.mergeAll(AWS.providers()),
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
      tags: tags(stage, "locker"),
    });

    // CloudTrail into its own bucket (CloudTrail needs a bucket policy; AES256 keeps the trail simple).
    const trailBucket = yield* AWS.S3.Bucket("trail-logs", {
      bucketName: `${name("trail")}-${inputs.accountId}`,
      encryption: { sseAlgorithm: "AES256" },
      publicAccessBlock: { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true },
      // Bucket `policy` takes the statements only.
      policy: [
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
      isLogging: true,
      tags: tags(stage, "cloudtrail"),
    });

    // The budget cap Khaliq sets for the account.
    yield* AWS.Budgets.Budget("budget", {
      budgetName: name("monthly"),
      budgetType: "COST",
      timeUnit: "MONTHLY",
      budgetLimit: { amount: String(inputs.budgetUsd), unit: "USD" },
      notifications: [
        { notificationType: "ACTUAL", comparisonOperator: "GREATER_THAN", threshold: 80, thresholdType: "PERCENTAGE", subscribers: [{ subscriptionType: "EMAIL", address: inputs.budgetEmail }] },
        { notificationType: "FORECASTED", comparisonOperator: "GREATER_THAN", threshold: 100, thresholdType: "PERCENTAGE", subscribers: [{ subscriptionType: "EMAIL", address: inputs.budgetEmail }] },
      ],
    });

    // GitHub OIDC: the only way CI deploys. Trusted for this repository alone.
    const oidc = yield* AWS.IAM.OpenIDConnectProvider("github-oidc", {
      url: "https://token.actions.githubusercontent.com",
      clientIDList: ["sts.amazonaws.com"],
      tags: tags(stage, "iam"),
    });
    const deployer = yield* AWS.IAM.Role("deployer", {
      roleName: name("deployer"),
      description: `96bulls ${stage} deployer, assumed only by GitHub Actions for ${REPO}`,
      assumeRolePolicyDocument: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: { Federated: oidc.openIDConnectProviderArn },
            Action: ["sts:AssumeRoleWithWebIdentity"],
            Condition: {
              StringEquals: { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
              StringLike: { "token.actions.githubusercontent.com:sub": `repo:${REPO}:*` },
            },
          },
        ],
      },
      inlinePolicies: {
        locker: {
          Version: "2012-10-17",
          Statement: [
            { Effect: "Allow", Action: ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetBucketVersioning"], Resource: [`arn:aws:s3:::${name("locker")}-${inputs.accountId}`] },
            { Effect: "Allow", Action: ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:ListBucketVersions"], Resource: [`arn:aws:s3:::${name("locker")}-${inputs.accountId}/*`] },
            { Effect: "Allow", Action: ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"], Resource: [key.keyArn as unknown as string] },
          ],
        },
      },
      maxSessionDuration: "1 hour",
      tags: tags(stage, "iam"),
    });

    return {
      stage,
      accountId: inputs.accountId,
      region: inputs.region,
      locker: { bucket: locker.bucketName, kmsKeyArn: key.keyArn },
      deployerRoleArn: deployer.roleArn,
      hoopdbSecretArn: inputs.hoopdbSecretArn ?? null,
    };
  }),
);
