/**
 * The production transcript store: the S3 bucket RelayHistory content lands in
 * (decision 0001: production AWS is S3 only; the migration plan's S5/S6/P9).
 *
 * A stage with `transcriptStore` in its inputs declares this and nothing else
 * from the AWS baseline: no Locker, no ECR, no GitHub OIDC, no EKS, no Supabase.
 *
 *   transcripts bucket   96bulls-<stage>-transcripts-<account>   versioned, SSE-KMS (own CMK, no bucket key),
 *                        bucket-owner-enforced, no public access, TLS 1.2+ only, Object Lock off,
 *                        noncurrent versions expire after 1 day (K-45 48 h purge), access logs on
 *   access-log bucket    96bulls-<stage>-access-logs-<account>   SSE-S3 (S3 server access logs cannot
 *                        be delivered to an SSE-KMS bucket), 90-day retention
 *   CloudTrail           management events + S3 data events for the transcripts bucket only,
 *                        into its own SSE-KMS bucket (the trail key policy from hardening.ts)
 *   IAM users            writer (the RelayHistory Worker), reader (the indexer), eraser (the purge job).
 *                        Users, not roles: a Cloudflare Worker cannot assume a role. No access keys
 *                        are declared here: Alchemy state is plaintext, so keys are created out of
 *                        band straight into 1Password (runbooks/sessions-prod-transcript-store.md).
 *
 * Key layout, the 96Bulls pipeline's org-prefix mode (tenants.ts, packages/object-store/src/layout.ts):
 *
 *   96bulls/<org>/v1/ws=<workspace>/m=<machine>/h=<harness>/d=<yyyy>/<mm>/<dd>/s=<session>/
 *       manifest.json | raw/... | summary/{short,long}.json.enc
 *
 * The writer's grants follow what relayhistory-cloud's Worker puts today
 * (lib/pipeline/raw-chunks.ts raw/, reconcile.ts manifest.json,
 * summary-objects.ts summary/); a stage that writes `normalized/` needs a
 * reviewed grant change, not a wider wildcard.
 *
 * Encryption: one dedicated CMK for the bucket with S3 Bucket Keys OFF, so every
 * object gets its own data key whose encryption context is the object's ARN
 * (`aws:s3:arn`, which carries `96bulls/<org>/v1/ws=<workspace>/`). The key
 * policy only lets the named users use the key through S3, for objects under
 * `96bulls/`, and every Decrypt/GenerateDataKey lands in CloudTrail with the
 * object ARN, i.e. per-workspace attribution. Per-workspace CMKs are not used:
 * see "Why one CMK" in the runbook.
 *
 * Pure functions, unit-tested in stacks/aws/__tests__/transcript-store.test.ts;
 * `transcriptStoreStage` declares the resources.
 */
import * as AWS from "@alchemy.run/aws";
import * as Effect from "effect/Effect";
import { LAYOUT_VERSION } from "../../packages/object-store/src/layout.ts";
import { KMS_ADMIN_ACTIONS, denyInsecureTransport, trailKeyPolicy } from "./hardening.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";

/** K-45 (Khaliq, 2026-10-31): logical delete immediately, physical purge within 48 h. */
export const ERASURE_SLA_HOURS = 48;
/** Superseded and deleted versions expire after this many days (S3 rounds up to the next 00:00 UTC). */
export const NONCURRENT_VERSION_DAYS = 1;
/** Abandoned multipart uploads hold transcript bytes too. */
export const ABORT_INCOMPLETE_UPLOAD_DAYS = 1;
/** Access logs and CloudTrail carry object keys (ids), never content. */
export const ACCESS_LOG_RETENTION_DAYS = 90;
export const TRAIL_LOG_RETENTION_DAYS = 365;
/** Regional boundary (migration plan section 6): transcript content stays in us-east-1. */
export const TRANSCRIPT_REGION = "us-east-1";
export const ACCESS_LOG_PREFIX = "s3-access/transcripts/";

export interface TranscriptStoreInputs {
  /**
   * Log management events too (default true). Set false only when another trail
   * in the account already logs them (a second copy is billed).
   */
  managementEvents?: boolean;
}

export interface TranscriptNames {
  bucket: string;
  bucketArn: string;
  accessLogBucket: string;
  trailBucket: string;
  trail: string;
  keyAlias: `alias/${string}`;
  trailKeyAlias: `alias/${string}`;
  budget: string;
  userPath: string;
  writer: string;
  reader: string;
  eraser: string;
}

export function transcriptNames(stage: string, accountId: string): TranscriptNames {
  const n = (base: string) => `96bulls-${stage}-${base}`;
  return {
    bucket: `${n("transcripts")}-${accountId}`,
    bucketArn: `arn:aws:s3:::${n("transcripts")}-${accountId}`,
    accessLogBucket: `${n("access-logs")}-${accountId}`,
    trailBucket: `${n("trail")}-${accountId}`,
    trail: n("trail"),
    keyAlias: `alias/${n("transcripts")}`,
    trailKeyAlias: `alias/${n("trail")}`,
    budget: n("monthly"),
    userPath: "/96bulls/",
    writer: n("transcript-writer"),
    reader: n("transcript-reader"),
    eraser: n("transcript-eraser"),
  };
}

export const userArn = (accountId: string, path: string, name: string) => `arn:aws:iam::${accountId}:user${path}${name}`;

/** Every session object, in every org: `<bucket>/96bulls/*\/v1/*<tail>`. */
const sessionObjects = (bucketArn: string, tail: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*${tail}`;
const allObjects = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*`;

/**
 * Inputs a transcript-store stage must not carry: it is S3 only (decision 0001),
 * production-grade (no purge) and in us-east-1 (regional boundary).
 */
export function assertTranscriptStoreInputs(stage: string, inputs: Record<string, unknown> & { region: string; transcriptStore?: TranscriptStoreInputs }) {
  const t = inputs.transcriptStore;
  if (!t) return;
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${stage} is a transcript-store stage: ${why}. Nothing was created.`);
  };
  if (typeof t !== "object" || Array.isArray(t)) refuse("transcriptStore must be an object");
  for (const k of Object.keys(t)) if (k !== "managementEvents") refuse(`transcriptStore.${k} is not a known setting`);
  if (t.managementEvents !== undefined && typeof t.managementEvents !== "boolean") refuse("transcriptStore.managementEvents must be a boolean");
  if (inputs.region !== TRANSCRIPT_REGION) refuse(`transcript content must stay in ${TRANSCRIPT_REGION} (migration plan, regional boundaries), got ${inputs.region}`);
  if (stage.startsWith("aws-sandbox-") || inputs.pippenSandbox) refuse("a sandbox account never holds transcripts");
  // S3 only: production AWS is S3 (decision 0001), and nothing here may empty a bucket.
  for (const k of ["eks", "supabase", "teardown", "accountShared", "lockerNoncurrentVersionDays", "hoopdbSecretArn"] as const)
    if (inputs[k] !== undefined) refuse(`${k} is not allowed (S3 only; never purged on destroy)`);
}

/**
 * Lifecycle for the transcripts bucket. A deleted or overwritten object becomes
 * a noncurrent version and expires after NONCURRENT_VERSION_DAYS; S3 rounds the
 * due time up to the next 00:00 UTC, so a version is due at most
 * 24 h * (days + 1) = 48 h after it became noncurrent. Lifecycle execution is
 * asynchronous, so it is the backstop: the erasure job deletes every version
 * explicitly (DeleteObjectVersion), which is immediate.
 */
export function transcriptLifecycleRules() {
  return [
    { ID: "expire-noncurrent-versions", Status: "Enabled", Filter: { Prefix: "" }, NoncurrentVersionExpiration: { NoncurrentDays: NONCURRENT_VERSION_DAYS } },
    { ID: "remove-expired-delete-markers", Status: "Enabled", Filter: { Prefix: "" }, Expiration: { ExpiredObjectDeleteMarker: true } },
    { ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: ABORT_INCOMPLETE_UPLOAD_DAYS } },
  ];
}

export function accessLogLifecycleRules() {
  return [
    { ID: "expire-access-logs", Status: "Enabled", Filter: { Prefix: "" }, Expiration: { Days: ACCESS_LOG_RETENTION_DAYS } },
    { ID: "expire-noncurrent-versions", Status: "Enabled", Filter: { Prefix: "" }, NoncurrentVersionExpiration: { NoncurrentDays: NONCURRENT_VERSION_DAYS } },
    { ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: ABORT_INCOMPLETE_UPLOAD_DAYS } },
  ];
}

export function trailLifecycleRules() {
  return [
    { ID: "expire-trail-logs", Status: "Enabled", Filter: { Prefix: "" }, Expiration: { Days: TRAIL_LOG_RETENTION_DAYS } },
    { ID: "expire-noncurrent-versions", Status: "Enabled", Filter: { Prefix: "" }, NoncurrentVersionExpiration: { NoncurrentDays: 30 } },
    { ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 7 } },
  ];
}

/**
 * Bucket policy for the transcripts bucket (statements only, as AWS.S3.Bucket takes them):
 * TLS (1.2+) only; only the eraser deletes objects or versions; a PUT that names
 * any encryption other than SSE-KMS with this bucket's key is refused (SSE-C is
 * blocked separately by the encryption configuration).
 */
export function transcriptBucketPolicy(i: { bucketArn: string; keyArn: string; eraserArn: string }) {
  return [
    ...denyInsecureTransport(i.bucketArn),
    {
      Sid: "DenyTlsBelow12",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:*"],
      Resource: [i.bucketArn, `${i.bucketArn}/*`],
      Condition: { NumericLessThan: { "s3:TlsVersion": "1.2" } },
    },
    {
      Sid: "OnlyTheEraserDeletes",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      Resource: [`${i.bucketArn}/*`],
      Condition: { ArnNotEquals: { "aws:PrincipalArn": i.eraserArn } },
    },
    {
      Sid: "DenyOtherEncryption",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:PutObject"],
      Resource: [`${i.bucketArn}/*`],
      Condition: { StringNotEquals: { "s3:x-amz-server-side-encryption": "aws:kms" }, Null: { "s3:x-amz-server-side-encryption": "false" } },
    },
    {
      Sid: "DenyOtherKmsKey",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:PutObject"],
      Resource: [`${i.bucketArn}/*`],
      Condition: { StringNotEquals: { "s3:x-amz-server-side-encryption-aws-kms-key-id": i.keyArn }, Null: { "s3:x-amz-server-side-encryption-aws-kms-key-id": "false" } },
    },
  ];
}

/** S3 server access logs into the log bucket, from the transcripts bucket only. */
export function accessLogBucketPolicy(i: { logBucketArn: string; sourceBucketArn: string; accountId: string }) {
  return [
    ...denyInsecureTransport(i.logBucketArn),
    {
      Sid: "S3ServerAccessLogsFromTranscripts",
      Effect: "Allow" as const,
      Principal: { Service: "logging.s3.amazonaws.com" },
      Action: ["s3:PutObject"],
      Resource: `${i.logBucketArn}/${ACCESS_LOG_PREFIX}*`,
      Condition: { ArnLike: { "aws:SourceArn": i.sourceBucketArn }, StringEquals: { "aws:SourceAccount": i.accountId } },
    },
  ];
}

/**
 * The transcripts CMK's policy. The account root administers the key and cannot
 * use it (no `kms:*`). Only the writer and reader may use it, only through S3 in
 * this region, only from this account, and only for objects under `96bulls/` in
 * this bucket (the S3 encryption context, per object since bucket keys are off).
 * The eraser and every operator role have no use of it: deleting needs no key,
 * and nobody reads transcripts with the console.
 */
export function transcriptKeyPolicy(i: { accountId: string; region: string; bucketName: string; writerArn: string; readerArn: string }) {
  const via = {
    StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
    StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${TENANT_KEY_BASE}/*` },
  };
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "AccountAdministersKey", Effect: "Allow" as const, Principal: { AWS: `arn:aws:iam::${i.accountId}:root` }, Action: [...KMS_ADMIN_ACTIONS], Resource: "*" },
      { Sid: "WriterEncryptsAndReadsViaS3", Effect: "Allow" as const, Principal: { AWS: i.writerArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition: via },
      { Sid: "ReaderDecryptsViaS3", Effect: "Allow" as const, Principal: { AWS: i.readerArn }, Action: ["kms:Decrypt"], Resource: "*", Condition: via },
    ],
  };
}

/** HEAD on a missing key answers 404 only with s3:ListBucket (hardening.ts listBucket); keys, never content. */
const listBucket = (bucketArn: string) => ({ Sid: "ListForMissingKeySemantics", Effect: "Allow" as const, Action: ["s3:ListBucket"], Resource: [bucketArn] });

/** Writer and reader never delete, never read old versions (that would undo a logical delete), never change the bucket. */
const noDeleteNoHistory = (bucketArn: string, extra: string[] = []) => ({
  Sid: "NoDeleteNoHistoryNoAdmin",
  Effect: "Deny" as const,
  Action: [
    "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:ListBucketVersions",
    "s3:PutBucketPolicy", "s3:DeleteBucketPolicy", "s3:PutLifecycleConfiguration", "s3:PutEncryptionConfiguration",
    "s3:PutBucketVersioning", "s3:PutObjectAcl", "s3:PutBucketAcl", "s3:DeleteBucket",
    ...extra,
  ],
  Resource: [bucketArn, `${bucketArn}/*`],
});

/** The RelayHistory Worker: read session objects; write raw chunks, manifests and encrypted summaries. */
export function transcriptWriterPolicy(bucketArn: string) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      listBucket(bucketArn),
      { Sid: "ReadSessionObjects", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: [sessionObjects(bucketArn, "")] },
      {
        Sid: "WriteRawChunksManifestsSummaries",
        Effect: "Allow" as const,
        Action: ["s3:PutObject"],
        Resource: [sessionObjects(bucketArn, "/raw/*"), sessionObjects(bucketArn, "/manifest.json"), sessionObjects(bucketArn, "/summary/*")],
      },
      noDeleteNoHistory(bucketArn),
    ],
  };
}

/** The history indexer: list and read session objects, nothing else. */
export function transcriptReaderPolicy(bucketArn: string) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      listBucket(bucketArn),
      { Sid: "ReadSessionObjects", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: [sessionObjects(bucketArn, "")] },
      noDeleteNoHistory(bucketArn, ["s3:PutObject"]),
    ],
  };
}

/**
 * The erasure job: list every version under `96bulls/` and delete them. No
 * content access (no GetObject, no key use) and no writes.
 */
export function transcriptEraserPolicy(bucketArn: string) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "ListVersionsUnderPrefix",
        Effect: "Allow" as const,
        Action: ["s3:ListBucket", "s3:ListBucketVersions"],
        Resource: [bucketArn],
        Condition: { StringLike: { "s3:prefix": [`${TENANT_KEY_BASE}/*`] } },
      },
      { Sid: "DeleteEveryVersion", Effect: "Allow" as const, Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: [allObjects(bucketArn)] },
      {
        Sid: "NoContentNoWrites",
        Effect: "Deny" as const,
        Action: ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:PutBucketPolicy", "s3:PutLifecycleConfiguration", "s3:PutBucketVersioning", "s3:DeleteBucket"],
        Resource: [bucketArn, `${bucketArn}/*`],
      },
    ],
  };
}

/** Management events (unless another trail has them) and S3 data events for the transcripts bucket only. */
export function transcriptTrailSelectors(bucketName: string, managementEvents: boolean) {
  return [
    ...(managementEvents ? [{ name: "management-events", fieldSelectors: [{ field: "eventCategory", equals: ["Management"] }] }] : []),
    {
      name: "transcript-object-events",
      fieldSelectors: [
        { field: "eventCategory", equals: ["Data"] },
        { field: "resources.type", equals: ["AWS::S3::Object"] },
        { field: "resources.ARN", startsWith: [`arn:aws:s3:::${bucketName}/`] },
      ],
    },
  ];
}

const PUBLIC_ACCESS_BLOCK = { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true } as const;

export interface TranscriptStageInputs {
  stage: string;
  accountId: string;
  region: string;
  budgetUsd: number;
  budgetEmail: string;
  transcriptStore: TranscriptStoreInputs;
  tags: (component: string) => Record<string, string>;
}

/** Declares the transcript store. Production stages are never destroyed (deploy-guard NON_DESTROYABLE). */
export const transcriptStoreStage = (i: TranscriptStageInputs) =>
  Effect.gen(function* () {
    const n = transcriptNames(i.stage, i.accountId);
    const eraserArn = userArn(i.accountId, n.userPath, n.eraser);
    const user = (id: string, userName: string, purpose: string, policy: object) =>
      AWS.IAM.User(id, {
        userName,
        path: n.userPath,
        inlinePolicies: { transcripts: policy as never },
        tags: { ...i.tags("iam"), "96bulls:purpose": purpose },
      });

    // Principals first: the key policy names them, and KMS refuses a principal that does not exist (yet).
    const writer = yield* user("transcript-writer", n.writer, "relayhistory-worker-writes-transcripts", transcriptWriterPolicy(n.bucketArn));
    const reader = yield* user("transcript-reader", n.reader, "history-indexer-reads-transcripts", transcriptReaderPolicy(n.bucketArn));
    const eraser = yield* user("transcript-eraser", n.eraser, "erasure-job-deletes-versions", transcriptEraserPolicy(n.bucketArn));

    // CloudTrail into its own bucket and key, as in the baseline (hardening.ts trailKeyPolicy).
    const trailKey = yield* AWS.KMS.Key("trail-key", {
      description: `96bulls ${i.stage}: CloudTrail log files`,
      enableKeyRotation: true,
      deletionWindow: "30 days",
      policy: JSON.stringify(trailKeyPolicy(i.accountId, i.region, n.trail)),
      tags: i.tags("cloudtrail"),
    });
    yield* AWS.KMS.Alias("trail-key-alias", { aliasName: n.trailKeyAlias, targetKeyId: trailKey.keyId });
    const trailArn = `arn:aws:s3:::${n.trailBucket}`;
    const trailBucket = yield* AWS.S3.Bucket("trail-logs", {
      bucketName: n.trailBucket,
      versioning: "Enabled",
      encryption: { sseAlgorithm: "aws:kms", kmsMasterKeyId: trailKey.keyArn, bucketKeyEnabled: true, blockedEncryptionTypes: ["SSE-C"] },
      publicAccessBlock: PUBLIC_ACCESS_BLOCK,
      objectOwnership: "BucketOwnerEnforced",
      policy: [
        ...denyInsecureTransport(trailArn),
        { Sid: "AWSCloudTrailAclCheck", Effect: "Allow", Principal: { Service: "cloudtrail.amazonaws.com" }, Action: ["s3:GetBucketAcl"], Resource: trailArn },
        { Sid: "AWSCloudTrailWrite", Effect: "Allow", Principal: { Service: "cloudtrail.amazonaws.com" }, Action: ["s3:PutObject"], Resource: `${trailArn}/AWSLogs/${i.accountId}/*`, Condition: { StringEquals: { "s3:x-amz-acl": "bucket-owner-full-control" } } },
      ] as never,
      lifecycleRules: trailLifecycleRules() as never,
      tags: i.tags("cloudtrail"),
    });
    const trail = yield* AWS.CloudTrail.Trail("trail", {
      trailName: n.trail,
      s3BucketName: trailBucket.bucketName,
      isMultiRegionTrail: true,
      enableLogFileValidation: true,
      kmsKeyId: trailKey.keyArn as unknown as string,
      isLogging: true,
      advancedEventSelectors: transcriptTrailSelectors(n.bucket, i.transcriptStore.managementEvents ?? true),
      tags: i.tags("cloudtrail"),
    });

    const key = yield* AWS.KMS.Key("transcripts-key", {
      description: `96bulls ${i.stage}: transcript objects (SSE-KMS, per-object data keys)`,
      enableKeyRotation: true,
      deletionWindow: "30 days",
      policy: transcriptKeyPolicy({
        accountId: i.accountId,
        region: i.region,
        bucketName: n.bucket,
        writerArn: writer.userArn as unknown as string,
        readerArn: reader.userArn as unknown as string,
      }) as never,
      // Also orders the key after the trail: the trail audits its use from the first call, and the
      // trail chain gives IAM time to propagate the new users KMS validates as principals.
      tags: { ...i.tags("kms"), "96bulls:audit-trail": trail.trailName as unknown as string },
    });
    yield* AWS.KMS.Alias("transcripts-key-alias", { aliasName: n.keyAlias, targetKeyId: key.keyId });

    const accessLogs = yield* AWS.S3.Bucket("access-logs", {
      bucketName: n.accessLogBucket,
      versioning: "Enabled",
      // S3 server access logging delivers only to SSE-S3 buckets.
      encryption: { sseAlgorithm: "AES256", blockedEncryptionTypes: ["SSE-C"] },
      publicAccessBlock: PUBLIC_ACCESS_BLOCK,
      objectOwnership: "BucketOwnerEnforced",
      policy: accessLogBucketPolicy({ logBucketArn: `arn:aws:s3:::${n.accessLogBucket}`, sourceBucketArn: n.bucketArn, accountId: i.accountId }) as never,
      lifecycleRules: accessLogLifecycleRules() as never,
      tags: i.tags("access-logs"),
    });

    const transcripts = yield* AWS.S3.Bucket("transcripts", {
      bucketName: n.bucket,
      // No force-destroy, no Object Lock: erasure must be able to remove every version.
      objectLockEnabled: false,
      versioning: "Enabled",
      encryption: { sseAlgorithm: "aws:kms", kmsMasterKeyId: key.keyArn, bucketKeyEnabled: false, blockedEncryptionTypes: ["SSE-C"] },
      publicAccessBlock: PUBLIC_ACCESS_BLOCK,
      objectOwnership: "BucketOwnerEnforced",
      policy: transcriptBucketPolicy({ bucketArn: n.bucketArn, keyArn: key.keyArn as unknown as string, eraserArn }) as never,
      lifecycleRules: transcriptLifecycleRules() as never,
      logging: { targetBucket: accessLogs.bucketName as unknown as string, targetPrefix: ACCESS_LOG_PREFIX, targetObjectKeyFormat: { PartitionedPrefix: { PartitionDateSource: "EventTime" } } as never },
      tags: i.tags("transcripts"),
    });

    yield* AWS.Budgets.Budget("budget", {
      budgetName: n.budget,
      budgetType: "COST",
      timeUnit: "MONTHLY",
      budgetLimit: { amount: String(i.budgetUsd), unit: "USD" },
      notifications: [
        { notificationType: "ACTUAL", comparisonOperator: "GREATER_THAN", threshold: Math.round(i.budgetUsd * 0.8), thresholdType: "ABSOLUTE_VALUE", subscribers: [{ subscriptionType: "EMAIL", address: i.budgetEmail }] },
        { notificationType: "FORECASTED", comparisonOperator: "GREATER_THAN", threshold: i.budgetUsd, thresholdType: "ABSOLUTE_VALUE", subscribers: [{ subscriptionType: "EMAIL", address: i.budgetEmail }] },
      ],
    });

    return {
      bucket: transcripts.bucketName,
      kmsKeyArn: key.keyArn,
      kmsKeyAlias: n.keyAlias,
      keyPrefix: TENANT_KEY_BASE,
      accessLogBucket: accessLogs.bucketName,
      trail: trail.trailName,
      writerUser: writer.userName,
      readerUser: reader.userName,
      eraserUser: eraser.userName,
    };
  });
