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
 *   upload-admission      the consumer Lambda (src/session-ingest/upload-admission-lambda.ts), reserved
 *                         concurrency 4, fed by an SQS event source mapping (batch size 1,
 *                         ReportBatchItemFailures). Its execution role is the one Alchemy's Lambda
 *                         provider owns (it cannot take a separately declared role), so, as for the
 *                         session pipeline and the read API, every grant is a resource policy naming
 *                         that role: the bucket (uploadAdmissionBucketStatements: read/tombstone/delete
 *                         inbox objects, promote through the srv-writer layout `.admission-inbox` and
 *                         `m=srv_*`), the key (uploadInboxKeyStatements), the queue and its secret.
 *   upload-admission-secret  Secrets Manager container `96bulls/<stage>/upload-admission` (value set out
 *                         of band from 1Password: UPLOAD_WITNESS_KEY only).
 *                         Decision 0005: the consumer admits itself with the S3 upload ledger
 *                         (INGEST_LEDGER=s3, src/session-ingest/ledger-s3.ts): no Cloudflare queue, no SQL.
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
/** The consumer Lambda's timeout. */
export const UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS = 300;
/** AWS guidance for SQS-triggered Lambdas: visibility at least 6x the function timeout. */
export const UPLOAD_ADMISSION_VISIBILITY_TIMEOUT_SECONDS = 6 * UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS;
export const UPLOAD_ADMISSION_MAX_RECEIVE_COUNT = 8;
export const UPLOAD_ADMISSION_DLQ_RETENTION = "14 days";
export const UPLOAD_ADMISSION_QUEUE_RETENTION = "4 days";

/** Decision 0004: reserved concurrency 4 bounds the consumer (and its downstream claims). */
export const UPLOAD_ADMISSION_RESERVED_CONCURRENCY = 4;
export const UPLOAD_ADMISSION_MEMORY_MB = 1024;

/** Alchemy resource ids this flag adds (transcript-store.ts plan contract: "upload inbox rollout" / "update"). */
export const UPLOAD_INBOX_RESOURCES = [
  "desktop-upload-role", "upload-admission", "upload-admission-secret",
  "upload-admission-dlq", "upload-admission-events", "upload-admission-mapping",
  "upload-admission-dlq-alarm", "upload-admission-age-alarm", "upload-admission-errors-alarm",
] as const;
/**
 * The resources whose policies name the consumer function's (Alchemy-owned) role. A function update re-plans
 * its `roleArn` as `(known after apply)`, so each of these may show exactly that role-ARN cascade.
 */
export const UPLOAD_ADMISSION_ROLE_GRANT_RESOURCES = ["transcripts", "transcripts-key", "upload-admission-events", "upload-admission-secret"] as const;

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
  function: string;
  secret: string;
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
    function: n("upload-admission"),
    secret: `96bulls/${stage}/upload-admission`,
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

/**
 * The upload-admission consumer's grants on the bucket (its Alchemy-owned role holds no S3 identity policy):
 * read the upload, tombstone and delete it, and promote it through the srv-writer layout
 * (`promotionObjects`, the srv-writer's object grant: `.admission-inbox` and the `m=srv_*` tenant layout).
 * PUTs carry no SSE header and get the bucket default (this key); DenyOtherEncryption and
 * SseHeaderMustNameThisKey refuse anything else. Deletes stay inbox-only (OnlyTheEraserDeletes and the inbox
 * wall), and the explicit deny keeps version history and bucket administration out of reach.
 */
/**
 * The fence-authority marker the consumer reads before it admits (decision 0005, ledger-s3.ts fenceAuthorityKey and
 * FenceAuthorityDoc; INGEST_KEY_ROOT is TENANT_KEY_BASE). Read only: an operator writes it, never this role.
 */
export const FENCE_AUTHORITY_MARKER_KEY = `${TENANT_KEY_BASE}/.ledger/fence-authority.json`;

export function uploadAdmissionBucketStatements(i: { bucketArn: string; consumerRoleArn: string; promotionObjects: string[] }) {
  const inbox = uploadInboxObjects(i.bucketArn);
  const principal = { AWS: i.consumerRoleArn };
  return [
    { Sid: "UploadAdmissionReadsTombstonesDeletesUploads", Effect: "Allow" as const, Principal: principal, Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource: [inbox] },
    { Sid: "UploadAdmissionPromotes", Effect: "Allow" as const, Principal: principal, Action: ["s3:GetObject", "s3:PutObject"], Resource: i.promotionObjects },
    // Exactly the one marker key, GET only (no wildcard): without it every record is a retry (fails closed).
    { Sid: "UploadAdmissionReadsFenceAuthority", Effect: "Allow" as const, Principal: principal, Action: ["s3:GetObject"], Resource: [`${i.bucketArn}/${FENCE_AUTHORITY_MARKER_KEY}`] },
    // Missing keys must answer 404 (create-only reads, the status object), as for the srv-writer: key names only.
    { Sid: "UploadAdmissionListsForMissingKeys", Effect: "Allow" as const, Principal: principal, Action: ["s3:ListBucket"], Resource: [i.bucketArn] },
    {
      Sid: "UploadAdmissionNoHistoryNoAdmin",
      Effect: "Deny" as const,
      Principal: "*",
      Action: [
        "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:ListBucketVersions",
        "s3:PutBucketPolicy", "s3:DeleteBucketPolicy", "s3:PutLifecycleConfiguration", "s3:PutEncryptionConfiguration",
        "s3:PutBucketVersioning", "s3:PutObjectAcl", "s3:PutBucketAcl", "s3:DeleteBucket", "s3:PutBucketNotification",
      ],
      Resource: [i.bucketArn, `${i.bucketArn}/*`],
      Condition: { ArnEquals: { "aws:PrincipalArn": i.consumerRoleArn } },
    },
  ];
}

/** Only the consumer's role reads its secret. */
export function uploadAdmissionSecretPolicy(i: { roleArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [{ Sid: "UploadAdmissionReads", Effect: "Allow" as const, Principal: { AWS: i.roleArn }, Action: "secretsmanager:GetSecretValue", Resource: "*" }],
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
export function uploadInboxBucketStatements(i: { bucketArn: string; eraserArn: string; uploadRoleArn?: string; consumerRoleArn: string; presignIssuerArn?: string }) {
  const inbox = uploadInboxObjects(i.bucketArn);
  return [
    {
      Sid: "UploadInboxOnlyItsRoles",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:*"],
      Resource: [inbox],
      Condition: { ArnNotEquals: { "aws:PrincipalArn": [i.consumerRoleArn, ...(i.uploadRoleArn ? [i.uploadRoleArn] : []), i.eraserArn, ...(i.presignIssuerArn ? [i.presignIssuerArn] : [])] } },
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
export function uploadInboxKeyStatements(i: { accountId: string; region: string; bucketName: string; uploadRoleArn?: string; consumerRoleArn: string; presignIssuerArn?: string }) {
  const viaFor = (glob: string) => ({
    StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
    StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${glob}` },
  });
  return [
    // Only with the desktop-upload role (STS uploads); a presign-only stack has no such principal.
    ...(i.uploadRoleArn ? [{ Sid: "DesktopUploadEncryptsInboxViaS3", Effect: "Allow" as const, Principal: { AWS: i.uploadRoleArn }, Action: ["kms:GenerateDataKey", "kms:Encrypt"], Resource: "*", Condition: viaFor(`${UPLOAD_INBOX_PREFIX}*`) }] : []),
    { Sid: "UploadAdmissionEncryptsAndReadsViaS3", Effect: "Allow" as const, Principal: { AWS: i.consumerRoleArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition: viaFor(`${TENANT_KEY_BASE}/*`) },
    ...(i.presignIssuerArn
      ? [
          // A presigned PUT is made as the issuer user: S3 asks KMS for the inbox object's data key as that user.
          { Sid: "PresignIssuerEncryptsInboxViaS3", Effect: "Allow" as const, Principal: { AWS: i.presignIssuerArn }, Action: ["kms:GenerateDataKey", "kms:Encrypt"], Resource: "*", Condition: viaFor(`${UPLOAD_INBOX_PREFIX}*`) },
          // The witness seq: the issuer reads privacy journals (written SSE-KMS by the Worker / consumer).
          { Sid: "PresignIssuerReadsJournalsViaS3", Effect: "Allow" as const, Principal: { AWS: i.presignIssuerArn }, Action: ["kms:Decrypt"], Resource: "*", Condition: viaFor(PRIVACY_JOURNAL_GLOB) },
        ]
      : []),
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

/**
 * `transcriptStore.uploadAdmission`: the consumer Lambda's runtime settings. Required when
 * historyUploadDirectInfra is on; inert (but still validated) otherwise.
 *
 *   direct              HISTORY_UPLOAD_DIRECT on the consumer: "off" | "allowlist" | "on". Required, never
 *                       defaulted. "off": every record fails without reading storage (redrives, then the DLQ).
 *   allowlist           allowlist only (and required then): `org:<id>` / `ws:<id>` entries
 *                       (HISTORY_UPLOAD_DIRECT_ALLOWLIST). It must match the issuer's allowlist: a tenant the
 *                       consumer excludes waits (retry, then the DLQ), never refuses.
 *
 * Not inputs: UPLOAD_WITNESS_KEY lives in the `upload-admission-secret` container, set out of band (never in
 * env, state or source). Decision 0005 removed the Cloudflare queue hand-off: the consumer admits itself.
 */
export interface UploadAdmissionInputs {
  direct: "off" | "allowlist" | "on";
  allowlist?: string[];
}

const ALLOWLIST_ENTRY = /^(org|ws):(?!\.)[A-Za-z0-9._-]{1,128}$/;

export function assertUploadAdmissionInputs(value: unknown, required: boolean, refuse: (why: string) => never): asserts value is UploadAdmissionInputs | undefined {
  if (value === undefined) {
    if (required) refuse("transcriptStore.uploadAdmission is required (direct)");
    return;
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return refuse("transcriptStore.uploadAdmission must be an object");
  const v = value as Record<string, unknown>;
  for (const k of Object.keys(v))
    if (k !== "direct" && k !== "allowlist") refuse(`transcriptStore.uploadAdmission.${k} is not a known setting`);
  if (v.direct !== "off" && v.direct !== "allowlist" && v.direct !== "on") refuse('transcriptStore.uploadAdmission.direct must be "off", "allowlist" or "on"');
  if (v.direct === "allowlist") {
    if (!Array.isArray(v.allowlist) || v.allowlist.length === 0 || v.allowlist.length > 100 || !v.allowlist.every((e) => typeof e === "string" && ALLOWLIST_ENTRY.test(e)) || new Set(v.allowlist).size !== v.allowlist.length)
      refuse("transcriptStore.uploadAdmission.allowlist must be 1-100 distinct org:<id> / ws:<id> entries with direct \"allowlist\"");
  } else if (v.allowlist !== undefined) refuse('transcriptStore.uploadAdmission.allowlist is only for direct "allowlist"');
}

/**
 * The consumer's environment: settings and ids only (src/session-ingest/upload-admission-hosted.ts reads them).
 * HISTORY_UPLOAD_INBOX_SSE comes from uploadAdmissionEnvironment (always aws:kms on hosted). INGEST_LEDGER is
 * always s3: the consumer refuses to start with anything else (decision 0005).
 */
export function uploadAdmissionFunctionEnv(i: { stage: string; bucket: string; names: UploadInboxNames; p: UploadAdmissionInputs; sse: typeof HOSTED_UPLOAD_INBOX_SSE }) {
  return {
    DEPLOYMENT: i.stage,
    HISTORY_UPLOAD_DIRECT: i.p.direct,
    ...(i.p.direct === "allowlist" ? { HISTORY_UPLOAD_DIRECT_ALLOWLIST: i.p.allowlist!.join(",") } : {}),
    ...uploadAdmissionEnvironment(i.sse),
    UPLOAD_ADMISSION_BUCKET: i.bucket,
    UPLOAD_ADMISSION_SECRET_ID: i.names.secret,
    INGEST_KEY_ROOT: TENANT_KEY_BASE,
    INGEST_LEDGER: "s3",
    NODE_OPTIONS: "--enable-source-maps",
  };
}

/** The consumer function: SQS-driven only (no Function URL, no other invoker), reserved concurrency 4. */
export function uploadAdmissionFunctionProps(i: { stage: string; bucket: string; names: UploadInboxNames; p: UploadAdmissionInputs; sse: typeof HOSTED_UPLOAD_INBOX_SSE }) {
  return {
    functionName: i.names.function,
    main: "src/session-ingest/upload-admission-lambda.ts",
    handler: "handler",
    runtime: "nodejs24.x",
    architecture: "arm64",
    memorySize: UPLOAD_ADMISSION_MEMORY_MB,
    timeout: `${UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS} seconds`,
    reservedConcurrentExecutions: UPLOAD_ADMISSION_RESERVED_CONCURRENCY,
    env: uploadAdmissionFunctionEnv(i),
    logging: { retention: "30 days" },
    functionUrl: false as const,
    build: { tsconfig: "tsconfig.json" },
  };
}

/** SQS -> the consumer: one message per invocation, partial-batch failures reported per record. */
export function uploadAdmissionMappingProps(i: { functionName: string; queueArn: string; tags: Record<string, string> }) {
  return {
    functionName: i.functionName,
    eventSourceArn: i.queueArn,
    batchSize: 1,
    functionResponseTypes: ["ReportBatchItemFailures"],
    // The function's reserved concurrency (4) already caps it; the mapping never asks for more.
    scalingConfig: { MaximumConcurrency: UPLOAD_ADMISSION_RESERVED_CONCURRENCY },
    enabled: true,
    tags: i.tags,
  };
}

/**
 * The hosted presigned issuer (decision 0004 workstream 1, hosted Cloud variant; AgentWorkforce/cloud#6244).
 * Cloud runs on Cloudflare Workers with no AWS identity to assume the desktop-upload role, so it answers
 * `kind: "presigned"` only, signing each URL with the keys of this dedicated IAM user (keys created out of band,
 * never in state or source). Opt in with `transcriptStore.uploadPresignIssuer: true`, read only while
 * historyUploadDirectInfra is on.
 *
 * What the vendored relay-control issuer signs, and so all this user may do:
 *   - PUT `96bulls/.upload-inbox/<org>/v1/ws=<ws>/u=<uh>/...`, query-string signed (presigned), create-only
 *     (`If-None-Match: *`), SSE-KMS with the transcripts key, plus x-amz-checksum-sha256, content-encoding zstd
 *     and the witness metadata (S3 verifies the checksum; the consumer refuses everything else, see the header);
 *   - GET `96bulls/.admission-inbox/<org>/v1/ws=<ws>/m=srv_<uh>/privacy-journal.json`, header signed (a 404 is
 *     seq 0, so it also needs ListBucket: without it S3 answers 403 and the issuer fails closed);
 *   - KMS through S3 only: encrypt inbox objects, decrypt privacy journals (uploadInboxKeyStatements).
 * No delete, no other read, no list outside `.admission-inbox/` when a prefix is given, nothing outside S3/KMS.
 */
export const PRESIGN_ISSUER_RESOURCE = "upload-presign-issuer";
/** A presigned URL older than this is refused by IAM whatever its X-Amz-Expires (relay-control caps it lower). */
export const PRESIGN_MAX_SIGNATURE_AGE_MS = 60 * 60 * 1000;
export const PRIVACY_JOURNAL_GLOB = `${TENANT_KEY_BASE}/.admission-inbox/*/v1/ws=*/m=srv_*/privacy-journal.json`;
export const presignIssuerUserName = (stage: string) => `96bulls-${stage}-upload-presign-issuer`;

export function uploadPresignIssuerPolicy(i: { bucketArn: string; accountId: string; region: string; keyAlias: string }) {
  const inbox = uploadInboxObjects(i.bucketArn);
  const journals = `${i.bucketArn}/${PRIVACY_JOURNAL_GLOB}`;
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "PresignedCreateOnlyInboxPutSseKms",
        Effect: "Allow" as const,
        Action: ["s3:PutObject"],
        Resource: [inbox],
        Condition: {
          StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms", "s3:if-none-match": "*", "s3:authType": "REST-QUERY-STRING" },
          StringLike: { "s3:x-amz-server-side-encryption-aws-kms-key-id": keyArnPattern(i.region, i.accountId) },
          NumericLessThanEquals: { "s3:signatureAge": String(PRESIGN_MAX_SIGNATURE_AGE_MS) },
        },
      },
      { Sid: "ReadPrivacyJournals", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: [journals], Condition: { StringEquals: { "s3:authType": "REST-HEADER" } } },
      // A missing journal must answer 404 (seq 0), not 403: S3 requires ListBucket for that.
      { Sid: "ListForMissingJournal", Effect: "Allow" as const, Action: ["s3:ListBucket"], Resource: [i.bucketArn] },
      {
        Sid: "NoListingOutsideAdmissionInbox",
        Effect: "Deny" as const,
        Action: ["s3:ListBucket"],
        Resource: [i.bucketArn],
        Condition: { Null: { "s3:prefix": "false" }, StringNotLike: { "s3:prefix": `${TENANT_KEY_BASE}/.admission-inbox/*` } },
      },
      {
        Sid: "UseTranscriptKeyViaS3",
        Effect: "Allow" as const,
        Action: ["kms:GenerateDataKey", "kms:Encrypt", "kms:Decrypt"],
        Resource: [keyArnPattern(i.region, i.accountId)],
        Condition: {
          StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` },
          "ForAnyValue:StringEquals": { "kms:ResourceAliases": i.keyAlias },
        },
      },
      { Sid: "NothingElseInS3", Effect: "Deny" as const, Action: ["s3:*"], NotResource: [inbox, journals, i.bucketArn] },
      { Sid: "NoOtherObjectActions", Effect: "Deny" as const, NotAction: ["s3:PutObject", "s3:GetObject", "s3:ListBucket"], Resource: [inbox, journals, i.bucketArn] },
      { Sid: "OnlyS3AndKms", Effect: "Deny" as const, NotAction: ["s3:*", "kms:*"], Resource: "*" },
    ],
  };
}
