/**
 * Direct-to-S3 desktop upload: the hosted infrastructure (decision 0004, cloud-ef step 4).
 * Everything here is behind the transcript stage's one default-off input
 * `historyUploadDirectInfra` (env HISTORY_UPLOAD_DIRECT_INFRA=on). With it off nothing
 * below is declared and every existing policy renders exactly as before.
 *
 *   desktop-upload role   96bulls-<stage>-desktop-upload    assumed only by relay-control's role
 *                         (`transcriptStore.uploadControlRoleArn`) with a source identity;
 *                         s3:PutObject on `96bulls/.upload-inbox/*` only, create-only, SSE-KMS
 *                         required. relay-control narrows each session to one org/workspace/upload
 *                         with an inline session policy. No List, no Get, no Delete.
 *   upload-admission role 96bulls-<stage>-upload-admission  the consumer Lambda's execution role
 *                         (the Lambda itself is a separate change): consumes the upload queue,
 *                         reads/tombstones/deletes inbox objects, and promotes through the
 *                         srv-writer layout (`.admission-inbox` and `m=srv_*`).
 *   upload-admission      SQS queue + DLQ (S3 events from this bucket only), DLQ depth alarm.
 *   bucket notification   Id `96bulls-upload-inbox`, written together with the manifests rule by
 *                         scripts/sessions-prod-bucket-notification.sh (one Put owns both Ids).
 *
 * The inbox is walled off in both directions: the bucket policy denies `s3:*` on it to every
 * principal but the two roles above and the eraser, and the writer, reader and srv-writer users
 * carry an explicit identity Deny as well.
 *
 * Not expressible in IAM (enforced by the consumer instead, decision 0004 refusal codes):
 * the `x-amz-checksum-sha256` value (`upload_digest_mismatch`) and `Content-Encoding: zstd`
 * (`unsupported_encoding`). S3 itself verifies a sent checksum against the body.
 */
import { INBOX_UPLOAD_NAMESPACE, UPLOAD_OBJECT_SUFFIX } from "../../src/session-ingest/upload-inbox-contract.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";

/** Key prefix of every desktop upload: `96bulls/.upload-inbox/<org>/v1/ws=<ws>/u=<uh>/...`. */
export const UPLOAD_INBOX_PREFIX = `${TENANT_KEY_BASE}/${INBOX_UPLOAD_NAMESPACE}/`;
export const UPLOAD_INBOX_NOTIFICATION_ID = "96bulls-upload-inbox";
export const UPLOAD_INBOX_NOTIFICATION_SUFFIX = UPLOAD_OBJECT_SUFFIX;
/** The consumer Lambda's timeout (set by the Lambda change; keep these in step). */
export const UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS = 300;
/** AWS guidance for SQS-triggered Lambdas: visibility at least 6x the function timeout. */
export const UPLOAD_ADMISSION_VISIBILITY_TIMEOUT_SECONDS = 6 * UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS;
export const UPLOAD_ADMISSION_MAX_RECEIVE_COUNT = 8;
export const UPLOAD_ADMISSION_DLQ_RETENTION = "14 days";
export const UPLOAD_ADMISSION_QUEUE_RETENTION = "4 days";

/** Alchemy resource ids this flag adds (for a future reviewed plan-contract rollout phase). */
export const UPLOAD_INBOX_RESOURCES = [
  "desktop-upload-role", "upload-admission-role",
  "upload-admission-dlq", "upload-admission-events",
  "upload-admission-dlq-alarm", "upload-admission-age-alarm",
] as const;

/** HISTORY_UPLOAD_DIRECT_INFRA: unset, "" or "off" is off; "on" is on; anything else refuses. */
export function parseHistoryUploadDirectInfra(value: string | undefined): boolean {
  if (value === undefined || value === "" || value === "off") return false;
  if (value === "on") return true;
  throw new Error(`96bulls: HISTORY_UPLOAD_DIRECT_INFRA must be "on" or "off" (or unset), got ${JSON.stringify(value)}. Nothing was created.`);
}

/**
 * The consumer's server-side-encryption requirement on hosted (decision 0004): always exactly
 * aws:kms, so an inbox object without SSE-KMS is refused (`upload_unencrypted`). The consumer
 * reads it from HISTORY_UPLOAD_INBOX_SSE (src/session-ingest/upload-admission.ts
 * UPLOAD_INBOX_SSE_VAR). Self-host keeps its own default ("none", e.g. MinIO without KMS;
 * selfhost-config.ts); this hosted requirement never applies there.
 */
export const UPLOAD_INBOX_SSE_ENV = "HISTORY_UPLOAD_INBOX_SSE";
export const HOSTED_UPLOAD_INBOX_SSE = "aws:kms";

/**
 * `transcriptStore.uploadInboxSse`. Whenever present it must be exactly "aws:kms": none, "", null
 * or any other value refuses (hosted never runs the consumer without the SSE-KMS check). With
 * `required` (historyUploadDirectInfra on) it must also be present.
 */
export function assertHostedUploadInboxSse(value: unknown, required: boolean, refuse: (why: string) => never): asserts value is typeof HOSTED_UPLOAD_INBOX_SSE | undefined {
  if (value === undefined) {
    if (required) refuse(`transcriptStore.uploadInboxSse is required and must be exactly "${HOSTED_UPLOAD_INBOX_SSE}" (hosted runs the consumer with ${UPLOAD_INBOX_SSE_ENV}=${HOSTED_UPLOAD_INBOX_SSE})`);
    return;
  }
  if (value !== HOSTED_UPLOAD_INBOX_SSE)
    refuse(`transcriptStore.uploadInboxSse must be exactly "${HOSTED_UPLOAD_INBOX_SSE}" on hosted, got ${typeof value === "string" ? JSON.stringify(value) : typeof value}; "none", "" and unset are for self-host only`);
}

/** The upload-admission consumer's environment on hosted; the consumer Lambda (a separate change) must use it. */
export function uploadAdmissionEnvironment(sse: typeof HOSTED_UPLOAD_INBOX_SSE) {
  if (sse !== HOSTED_UPLOAD_INBOX_SSE) throw new Error(`96bulls: ${UPLOAD_INBOX_SSE_ENV} must be "${HOSTED_UPLOAD_INBOX_SSE}" on hosted. Nothing was created.`);
  return { [UPLOAD_INBOX_SSE_ENV]: sse } as const;
}

const ROLE_ARN_RE = /^arn:aws:iam::\d{12}:role\/[A-Za-z0-9_+=,.@/-]+$/;
/** relay-control's role: any account (control may live elsewhere), a named role, partition aws. */
export function assertUploadControlRoleArn(value: unknown, refuse: (why: string) => never): asserts value is string | undefined {
  if (value !== undefined && (typeof value !== "string" || !ROLE_ARN_RE.test(value)))
    refuse("transcriptStore.uploadControlRoleArn must be a named IAM role ARN in partition aws");
}

export interface UploadInboxNames {
  rolePath: string;
  uploadRole: string;
  uploadRoleArn: string;
  consumerRole: string;
  consumerRoleArn: string;
  function: string;
  queue: string;
  dlq: string;
  queueArn: string;
  dlqArn: string;
  queueUrl: string;
}

export function uploadInboxNames(stage: string, accountId: string, region: string): UploadInboxNames {
  const n = (base: string) => `96bulls-${stage}-${base}`;
  const rolePath = "/96bulls/";
  return {
    rolePath,
    uploadRole: n("desktop-upload"),
    uploadRoleArn: `arn:aws:iam::${accountId}:role${rolePath}${n("desktop-upload")}`,
    consumerRole: n("upload-admission"),
    consumerRoleArn: `arn:aws:iam::${accountId}:role${rolePath}${n("upload-admission")}`,
    function: n("upload-admission"),
    queue: n("upload-admission"),
    dlq: n("upload-admission-dlq"),
    queueArn: `arn:aws:sqs:${region}:${accountId}:${n("upload-admission")}`,
    dlqArn: `arn:aws:sqs:${region}:${accountId}:${n("upload-admission-dlq")}`,
    queueUrl: `https://sqs.${region}.amazonaws.com/${accountId}/${n("upload-admission")}`,
  };
}

export const uploadInboxObjects = (bucketArn: string) => `${bucketArn}/${UPLOAD_INBOX_PREFIX}*`;
const keyArnPattern = (region: string, accountId: string) => `arn:aws:kms:${region}:${accountId}:key/*`;

/**
 * Only relay-control's role, and only while stamping a source identity on the session.
 * sts:TagSession is required because EKS Pod Identity supplies transitive session tags,
 * which must be accepted when relay-control chains into this role.
 */
export function desktopUploadTrustPolicy(i: { controlRoleArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "RelayControlAssumesWithSourceIdentity",
        Effect: "Allow" as const,
        Principal: { AWS: i.controlRoleArn },
        Action: ["sts:AssumeRole", "sts:SetSourceIdentity", "sts:TagSession"],
        Condition: { Null: { "sts:SourceIdentity": "false" } },
      },
    ],
  };
}

/**
 * Create-only PutObject under the inbox, SSE-KMS required (the bucket policy's
 * SseHeaderMustNameThisKey narrows the key id to exactly the transcripts CMK). The key ARN
 * cannot appear here: the key policy names this role, so the role exists before the key.
 */
export function desktopUploadPolicy(i: { bucketArn: string; accountId: string; region: string; keyAlias: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "PutUploadInboxCreateOnlySseKms",
        Effect: "Allow" as const,
        Action: ["s3:PutObject"],
        Resource: [uploadInboxObjects(i.bucketArn)],
        Condition: {
          StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms", "s3:if-none-match": "*" },
          StringLike: { "s3:x-amz-server-side-encryption-aws-kms-key-id": keyArnPattern(i.region, i.accountId) },
        },
      },
      {
        Sid: "UseTranscriptKeyToEncryptViaS3",
        Effect: "Allow" as const,
        Action: ["kms:GenerateDataKey", "kms:Encrypt"],
        Resource: [keyArnPattern(i.region, i.accountId)],
        Condition: {
          StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` },
          "ForAnyValue:StringEquals": { "kms:ResourceAliases": i.keyAlias },
        },
      },
    ],
  };
}

export function uploadAdmissionTrustPolicy() {
  return {
    Version: "2012-10-17" as const,
    Statement: [{ Sid: "LambdaAssumes", Effect: "Allow" as const, Principal: { Service: "lambda.amazonaws.com" }, Action: ["sts:AssumeRole"] }],
  };
}

/**
 * The upload-admission consumer: take the inbox notification, read the upload, promote it
 * through the srv-writer layout (`promotionObjects`, the same grant the srv-writer holds:
 * `.admission-inbox` and the `m=srv_*` tenant layout), tombstone and delete the upload.
 * Deletes only inbox objects; never old versions, never the bucket.
 */
export function uploadAdmissionPolicy(i: { bucketArn: string; accountId: string; region: string; keyAlias: string; queueArn: string; functionName: string; promotionObjects: string[] }) {
  const inbox = uploadInboxObjects(i.bucketArn);
  const sseKms = {
    StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms" },
    StringLike: { "s3:x-amz-server-side-encryption-aws-kms-key-id": keyArnPattern(i.region, i.accountId) },
  };
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "ConsumeUploadAdmissionQueue",
        Effect: "Allow" as const,
        Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
        Resource: [i.queueArn],
      },
      { Sid: "ReadAndDeleteUploads", Effect: "Allow" as const, Action: ["s3:GetObject", "s3:DeleteObject"], Resource: [inbox] },
      { Sid: "TombstoneUploadsSseKmsOnly", Effect: "Allow" as const, Action: ["s3:PutObject"], Resource: [inbox], Condition: sseKms },
      { Sid: "ReadPromotionObjects", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: i.promotionObjects },
      { Sid: "WritePromotionObjectsSseKmsOnly", Effect: "Allow" as const, Action: ["s3:PutObject"], Resource: i.promotionObjects, Condition: sseKms },
      {
        Sid: "UseTranscriptKeyViaS3",
        Effect: "Allow" as const,
        Action: ["kms:GenerateDataKey", "kms:Decrypt"],
        Resource: [keyArnPattern(i.region, i.accountId)],
        Condition: {
          StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` },
          "ForAnyValue:StringEquals": { "kms:ResourceAliases": i.keyAlias },
        },
      },
      // Missing keys must answer 404 (create-only reads), as for the srv-writer: key names only.
      { Sid: "ListForMissingKeySemantics", Effect: "Allow" as const, Action: ["s3:ListBucket"], Resource: [i.bucketArn] },
      {
        Sid: "OwnLogs",
        Effect: "Allow" as const,
        Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Resource: [`arn:aws:logs:${i.region}:${i.accountId}:log-group:/aws/lambda/${i.functionName}`, `arn:aws:logs:${i.region}:${i.accountId}:log-group:/aws/lambda/${i.functionName}:*`],
      },
      { Sid: "DeleteOnlyUploads", Effect: "Deny" as const, Action: ["s3:DeleteObject"], NotResource: [inbox] },
      {
        Sid: "NoHistoryNoAdmin",
        Effect: "Deny" as const,
        Action: [
          "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:ListBucketVersions",
          "s3:PutBucketPolicy", "s3:DeleteBucketPolicy", "s3:PutLifecycleConfiguration", "s3:PutEncryptionConfiguration",
          "s3:PutBucketVersioning", "s3:PutObjectAcl", "s3:PutBucketAcl", "s3:DeleteBucket", "s3:PutBucketNotification",
        ],
        Resource: [i.bucketArn, `${i.bucketArn}/*`],
      },
    ],
  };
}

/** Identity-side wall for every session-object principal that is not the consumer. */
export const denyUploadInboxStatement = (bucketArn: string) => ({
  Sid: "DenyUploadInbox",
  Effect: "Deny" as const,
  Action: ["s3:*"],
  Resource: [uploadInboxObjects(bucketArn)],
});

/**
 * Bucket-policy statements (flag on): only the two inbox roles and the eraser touch the
 * inbox at all, and only the consumer and the eraser delete there. This covers the pipeline
 * role, the read API, the buffered GC and every operator role, whatever their identity grants.
 */
export function uploadInboxBucketStatements(i: { bucketArn: string; eraserArn: string; uploadRoleArn: string; consumerRoleArn: string }) {
  const inbox = uploadInboxObjects(i.bucketArn);
  return [
    {
      Sid: "UploadInboxOnlyItsRoles",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:*"],
      Resource: [inbox],
      Condition: { ArnNotEquals: { "aws:PrincipalArn": [i.consumerRoleArn, i.uploadRoleArn, i.eraserArn] } },
    },
    {
      Sid: "UploadInboxOnlyEraserOrConsumerDeletes",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      Resource: [inbox],
      Condition: { ArnNotEquals: { "aws:PrincipalArn": [i.eraserArn, i.consumerRoleArn] } },
    },
  ];
}

/** Key-policy statements (flag on). The upload role encrypts inbox objects only; the consumer reads and promotes. */
export function uploadInboxKeyStatements(i: { accountId: string; region: string; bucketName: string; uploadRoleArn: string; consumerRoleArn: string }) {
  const viaFor = (glob: string) => ({
    StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
    StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${glob}` },
  });
  return [
    { Sid: "DesktopUploadEncryptsInboxViaS3", Effect: "Allow" as const, Principal: { AWS: i.uploadRoleArn }, Action: ["kms:GenerateDataKey", "kms:Encrypt"], Resource: "*", Condition: viaFor(`${UPLOAD_INBOX_PREFIX}*`) },
    { Sid: "UploadAdmissionEncryptsAndReadsViaS3", Effect: "Allow" as const, Principal: { AWS: i.consumerRoleArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition: viaFor(`${TENANT_KEY_BASE}/*`) },
  ];
}

/** S3 events from this bucket only; the consumer role consumes (no SendMessage). TLS only. */
export function uploadAdmissionQueuePolicy(i: { queueArn: string; bucketArn: string; accountId: string; consumerRoleArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "DenyInsecureTransport", Effect: "Deny" as const, Principal: "*", Action: "sqs:*", Resource: i.queueArn, Condition: { Bool: { "aws:SecureTransport": "false" } } },
      {
        Sid: "TranscriptBucketUploadEvents",
        Effect: "Allow" as const,
        Principal: { Service: "s3.amazonaws.com" },
        Action: "sqs:SendMessage",
        Resource: i.queueArn,
        Condition: { ArnEquals: { "aws:SourceArn": i.bucketArn }, StringEquals: { "aws:SourceAccount": i.accountId } },
      },
      {
        Sid: "UploadAdmissionConsumes",
        Effect: "Allow" as const,
        Principal: { AWS: i.consumerRoleArn },
        Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
        Resource: i.queueArn,
      },
    ],
  };
}

export function uploadAdmissionDlqPolicy(i: { dlqArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "DenyInsecureTransport", Effect: "Deny" as const, Principal: "*", Action: "sqs:*", Resource: i.dlqArn, Condition: { Bool: { "aws:SecureTransport": "false" } } },
    ],
  };
}

/** The second bucket notification: every finished upload object to the upload-admission queue. */
export function uploadInboxQueueConfiguration(queueArn: string) {
  return {
    Id: UPLOAD_INBOX_NOTIFICATION_ID,
    QueueArn: queueArn,
    Events: ["s3:ObjectCreated:*"],
    Filter: { Key: { FilterRules: [{ Name: "prefix", Value: UPLOAD_INBOX_PREFIX }, { Name: "suffix", Value: UPLOAD_INBOX_NOTIFICATION_SUFFIX }] } },
  };
}
