/**
 * The P1 search-split rehearsal in a Pippen non-prod sandbox (stage aws-rehearse-p1-1126,
 * us-west-2), and the read-only state-bucket mode every Pippen sandbox stage other than the proof
 * stage must use (docs/handoffs/pippen-rehearsal-p1-1126.md).
 *
 * The sandbox's Alchemy state bucket, `pippen-sbx-alchemy-state-<account>-<region>-an`, is shared by
 * every stage in the account. ensurePippenStateBucket (state-bucket.ts) creates it and converges its
 * versioning, encryption, public access block, ownership controls and tags bucket-wide, which a
 * per-stage key prefix cannot bound. In `pippenSandbox.stateBucket: "validate-only"` mode the state
 * layer never creates or changes the bucket: it reads its settings and refuses on any difference,
 * with fixed error codes (StateBucketError). The caller must be this stage's lease session, and the
 * state prefix must be exactly this stage's. Teardown deletes only this stage's state keys.
 *
 * Nothing here runs unless a stage sets that mode; the proof stage and every other stage are as before.
 */
import { PIPPEN_PROOF_SANDBOX } from "./sandbox-rules.ts";
import { alchemyStatePrefix, pippenStateBucket, type AwsRun } from "./state-bucket.ts";

export const REHEARSAL_STAGE = "aws-rehearse-p1-1126";
export const REHEARSAL_REGION = "us-west-2";
/** When the rehearsal lease policy stops allowing anything (and starts denying everything). */
export const REHEARSAL_LEASE_EXPIRES_AT = "2026-11-28T00:00:00Z";
/** Stages allowed in validate-only mode, each with its exact state prefix. */
export const VALIDATE_ONLY_STAGES: Readonly<Record<string, string>> = Object.freeze({ [REHEARSAL_STAGE]: "96bulls/96bulls-aws/aws-rehearse-p1-1126/" });

export type StateBucketMode = "ensure" | "validate-only";

/** The mode a stage's pippenSandbox input selects: validate-only when set so, else the proof stage's ensure. */
export function stateBucketMode(pippenSandbox: unknown): StateBucketMode {
  return (pippenSandbox as { stateBucket?: unknown } | undefined)?.stateBucket === "validate-only" ? "validate-only" : "ensure";
}

/**
 * The inputs rule: `pippenSandbox.stateBucket` is absent or "validate-only". A stage declared for
 * validate-only (VALIDATE_ONLY_STAGES: it shares an existing sandbox's state bucket) must set it,
 * and only such a stage may; the proof stage and every other existing stage keep the ensure path
 * exactly as before (a fresh sandbox account has no bucket to share).
 */
export function assertStateBucketMode(stage: string, pippenSandbox: unknown): void {
  const declared = stage in VALIDATE_ONLY_STAGES;
  if (declared && !pippenSandbox) throw new Error(`96bulls: stage ${stage} runs only in a Pippen sandbox (pippenSandbox with stateBucket "validate-only"). Nothing was created.`);
  if (!pippenSandbox) return;
  const v = (pippenSandbox as { stateBucket?: unknown }).stateBucket;
  if (v !== undefined && v !== "validate-only") throw new Error('96bulls: pippenSandbox.stateBucket may only be "validate-only". Nothing was created.');
  if (declared && v !== "validate-only")
    throw new Error(`96bulls: stage ${stage} shares the sandbox's state bucket; it needs pippenSandbox.stateBucket "validate-only" (it may never create or converge it). Nothing was created.`);
  if (stage === PIPPEN_PROOF_SANDBOX.stage && v !== undefined) throw new Error(`96bulls: ${PIPPEN_PROOF_SANDBOX.stage} keeps its own state bucket path. Nothing was created.`);
  if (v === "validate-only" && !(stage in VALIDATE_ONLY_STAGES)) throw new Error(`96bulls: stage ${stage} has no declared state prefix for validate-only mode. Nothing was created.`);
}

export type StateBucketErrorCode =
  | "state_bucket_absent"
  | "state_bucket_unreadable"
  | "state_bucket_versioning_drift"
  | "state_bucket_encryption_drift"
  | "state_bucket_public_access_drift"
  | "state_bucket_ownership_drift"
  | "state_bucket_tagging_drift"
  | "state_prefix_mismatch"
  | "state_caller_mismatch"
  | "state_teardown_outside_prefix"
  | "state_teardown_failed";

/** A refusal with a fixed code and message: no CLI output or exception text is carried. */
export class StateBucketError extends Error {
  constructor(readonly code: StateBucketErrorCode) {
    super(`96bulls: Pippen sandbox state refused (${code}). Nothing was changed.`);
    this.name = "StateBucketError";
  }
}

/**
 * The stage's exact state prefix: `96bulls/96bulls-aws/<stage>/`, derived from the stage and equal to
 * the declared one, never wider (a parent such as `96bulls/96bulls-aws/` or a sibling stage refuses).
 */
export function rehearsalStatePrefix(stage: string): string {
  const declared = VALIDATE_ONLY_STAGES[stage];
  const derived = `${alchemyStatePrefix(stage)}/`;
  if (declared === undefined || derived !== declared || !/^96bulls\/96bulls-aws\/aws-[a-z0-9-]+\/$/.test(derived)) throw new StateBucketError("state_prefix_mismatch");
  return derived;
}

/** Whether `prefix` is exactly the stage's state prefix. */
export function assertStatePrefix(stage: string, prefix: string): void {
  if (prefix !== rehearsalStatePrefix(stage)) throw new StateBucketError("state_prefix_mismatch");
}

const PIPPEN_SESSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

/**
 * The caller ARN this stage's run must have: the pippen lease's agent-session role session
 * (`agent-<session_id>`) in the stage's account, the session from NINETY_SIX_BULLS_PIPPEN_SESSION,
 * and, under `pippen sandbox exec`, the same lease and account in its env.
 */
export function leaseCallerArn(accountId: string, env: Record<string, string | undefined>): string {
  const sbx = PIPPEN_PROOF_SANDBOX;
  const id = env[sbx.sessionEnv];
  if (!/^\d{12}$/.test(accountId) || id === undefined || !PIPPEN_SESSION_ID.test(id)) throw new StateBucketError("state_caller_mismatch");
  if (env[sbx.execSessionEnv] !== undefined && env[sbx.execSessionEnv] !== id) throw new StateBucketError("state_caller_mismatch");
  if (env[sbx.execAccountEnv] !== undefined && env[sbx.execAccountEnv] !== accountId) throw new StateBucketError("state_caller_mismatch");
  return `arn:aws:sts::${accountId}:assumed-role/${sbx.callerRole.roleName}/${sbx.callerRole.sessionPrefix}${id}`;
}

/** STS's caller must be exactly the lease session for this stage. */
export function assertLeaseCaller(aws: AwsRun, accountId: string, env: Record<string, string | undefined>): void {
  const expected = leaseCallerArn(accountId, env);
  let arn: unknown;
  try {
    arn = (JSON.parse(aws(["sts", "get-caller-identity", "--output", "json"]) || "{}") as { Arn?: unknown }).Arn;
  } catch {
    throw new StateBucketError("state_caller_mismatch");
  }
  if (arn !== expected) throw new StateBucketError("state_caller_mismatch");
}

/** The settings the shared bucket must already have (those ensurePippenStateBucket gives it). */
export const EXPECTED_STATE_BUCKET = Object.freeze({
  versioning: "Enabled",
  sseAlgorithm: "AES256",
  bucketKeyEnabled: false,
  publicAccessBlock: Object.freeze({ BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true }),
  ownership: "BucketOwnerEnforced",
  /** The tags every stage relies on (the stack tag names the proof stage that made it, so it is not required). */
  tags: Object.freeze([
    { Key: "pippen:sandbox", Value: "true" },
    { Key: "96bulls:component", Value: "alchemy-state" },
  ]),
});

const read = (aws: AwsRun, args: string[], absent: RegExp | undefined, drift: StateBucketErrorCode): unknown => {
  let out: string;
  try {
    out = aws(args);
  } catch (e) {
    const text = e instanceof Error ? e.message : String(e);
    if (/NoSuchBucket|\(404\)/.test(text)) throw new StateBucketError("state_bucket_absent");
    if (absent && absent.test(text)) throw new StateBucketError(drift);
    throw new StateBucketError("state_bucket_unreadable");
  }
  try {
    return out.trim() ? JSON.parse(out) : {};
  } catch {
    throw new StateBucketError("state_bucket_unreadable");
  }
};

/**
 * validate-only: read the shared state bucket's five settings and refuse on any difference. Only
 * reads (GetBucketVersioning, which also answers NoSuchBucket for an absent bucket,
 * GetBucketEncryption, GetPublicAccessBlock, GetBucketOwnershipControls, GetBucketTagging): never
 * CreateBucket or any Put. HeadBucket is not used: it needs s3:ListBucket without a prefix
 * condition, which the lease policy does not grant. Returns the bucket name.
 */
export function validatePippenStateBucket(aws: AwsRun, i: { accountId: string; region: string }): string {
  const bucket = pippenStateBucket(i.accountId, i.region);
  const r = ["--bucket", bucket, "--region", i.region, "--output", "json"];
  const e = EXPECTED_STATE_BUCKET;
  const versioning = read(aws, ["s3api", "get-bucket-versioning", ...r], undefined, "state_bucket_versioning_drift") as { Status?: unknown };
  if (versioning.Status !== e.versioning) throw new StateBucketError("state_bucket_versioning_drift");
  const enc = read(aws, ["s3api", "get-bucket-encryption", ...r], /ServerSideEncryptionConfigurationNotFound/, "state_bucket_encryption_drift") as {
    ServerSideEncryptionConfiguration?: { Rules?: { ApplyServerSideEncryptionByDefault?: { SSEAlgorithm?: unknown; KMSMasterKeyID?: unknown }; BucketKeyEnabled?: unknown }[] };
  };
  const rules = enc.ServerSideEncryptionConfiguration?.Rules ?? [];
  const rule = rules[0];
  if (rules.length !== 1 || rule?.ApplyServerSideEncryptionByDefault?.SSEAlgorithm !== e.sseAlgorithm || rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID !== undefined || (rule.BucketKeyEnabled ?? false) !== e.bucketKeyEnabled)
    throw new StateBucketError("state_bucket_encryption_drift");
  const pab = (read(aws, ["s3api", "get-public-access-block", ...r], /NoSuchPublicAccessBlockConfiguration/, "state_bucket_public_access_drift") as { PublicAccessBlockConfiguration?: Record<string, unknown> }).PublicAccessBlockConfiguration;
  if (!Object.entries(e.publicAccessBlock).every(([k, v]) => pab?.[k] === v)) throw new StateBucketError("state_bucket_public_access_drift");
  const own = read(aws, ["s3api", "get-bucket-ownership-controls", ...r], /OwnershipControlsNotFoundError/, "state_bucket_ownership_drift") as { OwnershipControls?: { Rules?: { ObjectOwnership?: unknown }[] } };
  const ownRules = own.OwnershipControls?.Rules ?? [];
  if (ownRules.length !== 1 || ownRules[0]?.ObjectOwnership !== e.ownership) throw new StateBucketError("state_bucket_ownership_drift");
  const tags = (read(aws, ["s3api", "get-bucket-tagging", ...r], /NoSuchTagSet/, "state_bucket_tagging_drift") as { TagSet?: { Key?: unknown; Value?: unknown }[] }).TagSet ?? [];
  if (!e.tags.every((t) => tags.some((x) => x.Key === t.Key && x.Value === t.Value))) throw new StateBucketError("state_bucket_tagging_drift");
  return bucket;
}

/**
 * validate-only, as the state layer and the capture run it before Alchemy's first state operation:
 * the caller is this stage's lease session, the prefix is exactly this stage's, and the shared
 * bucket is already in shape. Nothing is created or written.
 */
export function assertValidateOnlyState(aws: AwsRun, i: { accountId: string; region: string; stage: string; env: Record<string, string | undefined> }): { bucket: string; prefix: string } {
  const prefix = rehearsalStatePrefix(i.stage);
  assertLeaseCaller(aws, i.accountId, i.env);
  return { bucket: validatePippenStateBucket(aws, i), prefix };
}

interface VersionEntry {
  Key?: unknown;
  VersionId?: unknown;
}

/**
 * Every object version and delete marker under the stage's exact prefix (ListObjectVersions with
 * KeyMarker/VersionIdMarker pagination). Refuses, before anything is deleted, when any listed key is
 * outside the prefix or an entry has no key or version id.
 */
export function listStageStateVersions(aws: AwsRun, i: { bucket: string; region: string; stage: string }): { Key: string; VersionId: string }[] {
  const prefix = rehearsalStatePrefix(i.stage);
  const out: { Key: string; VersionId: string }[] = [];
  let marker: { key?: string; version?: string } = {};
  for (let page = 0; page < 10_000; page++) {
    let listed: { Versions?: VersionEntry[]; DeleteMarkers?: VersionEntry[]; IsTruncated?: unknown; NextKeyMarker?: unknown; NextVersionIdMarker?: unknown };
    try {
      const text = aws([
        "s3api", "list-object-versions", "--bucket", i.bucket, "--region", i.region, "--prefix", prefix, "--output", "json", "--no-paginate",
        ...(marker.key !== undefined ? ["--key-marker", marker.key] : []),
        ...(marker.version !== undefined ? ["--version-id-marker", marker.version] : []),
      ]);
      listed = text.trim() ? JSON.parse(text) : {};
    } catch {
      throw new StateBucketError("state_teardown_failed");
    }
    for (const v of [...(listed.Versions ?? []), ...(listed.DeleteMarkers ?? [])]) {
      if (typeof v.Key !== "string" || typeof v.VersionId !== "string" || !v.VersionId) throw new StateBucketError("state_teardown_failed");
      if (!v.Key.startsWith(prefix) || v.Key.length === prefix.length) throw new StateBucketError("state_teardown_outside_prefix");
      out.push({ Key: v.Key, VersionId: v.VersionId });
    }
    if (listed.IsTruncated !== true) return out;
    marker = {
      ...(typeof listed.NextKeyMarker === "string" ? { key: listed.NextKeyMarker } : {}),
      ...(typeof listed.NextVersionIdMarker === "string" ? { version: listed.NextVersionIdMarker } : {}),
    };
    if (marker.key === undefined) throw new StateBucketError("state_teardown_failed");
  }
  throw new StateBucketError("state_teardown_failed");
}

/**
 * Teardown of this stage's state only: list (above, refusing anything outside the prefix), then
 * DeleteObjects by exact key and version id, at most 1000 per call, rechecking every key against
 * the prefix before each call. `execute: false` (the default) lists and deletes nothing. Sibling
 * stages and the bucket itself are never touched.
 */
export function teardownStageState(aws: AwsRun, i: { bucket: string; region: string; stage: string; execute?: boolean }): { listed: number; deleted: number } {
  const prefix = rehearsalStatePrefix(i.stage);
  const all = listStageStateVersions(aws, i);
  if (!i.execute) return { listed: all.length, deleted: 0 };
  let deleted = 0;
  for (let n = 0; n < all.length; n += 1000) {
    const batch = all.slice(n, n + 1000);
    if (!batch.every((o) => o.Key.startsWith(prefix) && o.Key.length > prefix.length)) throw new StateBucketError("state_teardown_outside_prefix");
    let result: { Errors?: unknown[] };
    try {
      const text = aws(["s3api", "delete-objects", "--bucket", i.bucket, "--region", i.region, "--output", "json", "--delete", JSON.stringify({ Objects: batch, Quiet: true })]);
      result = text.trim() ? JSON.parse(text) : {};
    } catch {
      throw new StateBucketError("state_teardown_failed");
    }
    if ((result.Errors ?? []).length > 0) throw new StateBucketError("state_teardown_failed");
    deleted += batch.length;
  }
  return { listed: all.length, deleted };
}

/** The v2 (search-split) resources the rehearsal stage declares, by name (session-pipeline.ts naming). */
export function rehearsalResourceNames(stage: string = REHEARSAL_STAGE) {
  const n = (base: string) => `96bulls-${stage}-${base}`;
  return {
    function: n("session-pipeline"),
    queues: [n("session-events"), n("session-events-dlq"), n("session-search"), n("session-search-dlq")],
    rules: [n("session-reconcile"), n("session-brief")],
    alarms: [`${n("session-events-dlq")}-not-empty`, `${n("session-events")}-backlog-age`, `${n("session-pipeline")}-errors`, `${n("session-search-dlq")}-not-empty`, `${n("session-search")}-backlog-age`],
    logGroup: `/aws/lambda/${n("session-pipeline")}`,
    secret: `96bulls/${stage}/session-pipeline`,
  };
}

/**
 * The rehearsal lease's identity policy: state objects only under the stage's exact prefix, listing
 * only with that s3:prefix, the bucket's settings readable but never writable, no object access
 * anywhere else, the v2 resources named `96bulls-<stage>-*` (and the stage's secret path and log
 * group) only, in us-west-2 only, and nothing after `expiresAt`.
 */
export function rehearsalLeasePolicy(i: { accountId: string; stage?: string; region?: string; expiresAt?: string }) {
  const stage = i.stage ?? REHEARSAL_STAGE;
  const region = i.region ?? REHEARSAL_REGION;
  if (region !== REHEARSAL_REGION) throw new StateBucketError("state_prefix_mismatch");
  const expiresAt = i.expiresAt ?? REHEARSAL_LEASE_EXPIRES_AT;
  const prefix = rehearsalStatePrefix(stage);
  const bucket = pippenStateBucket(i.accountId, region);
  const bucketArn = `arn:aws:s3:::${bucket}`;
  const acct = i.accountId;
  const name = `96bulls-${stage}-*`;
  const live = { DateLessThan: { "aws:CurrentTime": expiresAt } };
  const regional = { StringEquals: { "aws:RequestedRegion": region } };
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "StateListOnlyThisPrefix", Effect: "Allow" as const, Action: ["s3:ListBucket", "s3:ListBucketVersions"], Resource: bucketArn, Condition: { ...live, StringLike: { "s3:prefix": [`${prefix}*`] } } },
      {
        Sid: "StateObjectsOnlyThisPrefix",
        Effect: "Allow" as const,
        Action: ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"],
        Resource: `${bucketArn}/${prefix}*`,
        Condition: live,
      },
      {
        Sid: "StateBucketSettingsReadOnly",
        Effect: "Allow" as const,
        Action: ["s3:GetBucketVersioning", "s3:GetEncryptionConfiguration", "s3:GetBucketPublicAccessBlock", "s3:GetBucketOwnershipControls", "s3:GetBucketTagging"],
        Resource: bucketArn,
        Condition: live,
      },
      {
        Sid: "NeverChangeTheSharedBucket",
        Effect: "Deny" as const,
        Action: [
          "s3:CreateBucket", "s3:PutBucket*", "s3:DeleteBucket*", "s3:PutEncryptionConfiguration", "s3:PutBucketVersioning", "s3:PutBucketPublicAccessBlock",
          "s3:PutBucketOwnershipControls", "s3:PutBucketTagging", "s3:PutLifecycleConfiguration", "s3:PutBucketPolicy", "s3:PutBucketAcl",
        ],
        Resource: [bucketArn, "arn:aws:s3:::pippen-sbx-alchemy-state-*"],
      },
      { Sid: "NeverListOtherPrefixes", Effect: "Deny" as const, Action: ["s3:ListBucket", "s3:ListBucketVersions"], Resource: bucketArn, Condition: { StringNotLike: { "s3:prefix": [`${prefix}*`] } } },
      {
        Sid: "NeverOtherStateObjects",
        Effect: "Deny" as const,
        Action: ["s3:GetObject*", "s3:PutObject*", "s3:DeleteObject*", "s3:RestoreObject", "s3:ReplicateObject"],
        NotResource: [`${bucketArn}/${prefix}*`, `arn:aws:s3:::${name}/*`, `arn:aws:s3:::pippen-sbx-${name}/*`],
      },
      {
        Sid: "V2Queues",
        Effect: "Allow" as const,
        Action: [
          "sqs:CreateQueue", "sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:SetQueueAttributes", "sqs:TagQueue", "sqs:UntagQueue", "sqs:ListQueueTags",
          "sqs:DeleteQueue", "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
        ],
        Resource: `arn:aws:sqs:${region}:${acct}:${name}`,
        Condition: { ...live, ...regional },
      },
      {
        Sid: "V2Function",
        Effect: "Allow" as const,
        Action: [
          "lambda:CreateFunction", "lambda:GetFunction", "lambda:GetFunctionConfiguration", "lambda:UpdateFunctionCode", "lambda:UpdateFunctionConfiguration", "lambda:DeleteFunction",
          "lambda:AddPermission", "lambda:RemovePermission", "lambda:GetPolicy", "lambda:TagResource", "lambda:UntagResource", "lambda:ListTags", "lambda:InvokeFunction",
        ],
        Resource: `arn:aws:lambda:${region}:${acct}:function:${name}`,
        Condition: { ...live, ...regional },
      },
      {
        Sid: "V2EventSourceMappingsOnTheFunction",
        Effect: "Allow" as const,
        Action: ["lambda:CreateEventSourceMapping", "lambda:GetEventSourceMapping", "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping", "lambda:ListEventSourceMappings"],
        Resource: "*",
        Condition: { ...live, ...regional, ArnLike: { "lambda:FunctionArn": `arn:aws:lambda:${region}:${acct}:function:${name}` } },
      },
      { Sid: "V2Rules", Effect: "Allow" as const, Action: ["events:PutRule", "events:DescribeRule", "events:DeleteRule", "events:PutTargets", "events:RemoveTargets", "events:ListTargetsByRule", "events:TagResource", "events:UntagResource", "events:ListTagsForResource", "events:EnableRule", "events:DisableRule"], Resource: `arn:aws:events:${region}:${acct}:rule/${name}`, Condition: { ...live, ...regional } },
      { Sid: "V2Alarms", Effect: "Allow" as const, Action: ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:TagResource", "cloudwatch:UntagResource", "cloudwatch:ListTagsForResource"], Resource: `arn:aws:cloudwatch:${region}:${acct}:alarm:${name}`, Condition: { ...live, ...regional } },
      { Sid: "V2LogGroup", Effect: "Allow" as const, Action: ["logs:CreateLogGroup", "logs:PutRetentionPolicy", "logs:DeleteLogGroup", "logs:TagResource", "logs:UntagResource", "logs:ListTagsForResource"], Resource: [`arn:aws:logs:${region}:${acct}:log-group:/aws/lambda/${name}`, `arn:aws:logs:${region}:${acct}:log-group:/aws/lambda/${name}:*`], Condition: { ...live, ...regional } },
      { Sid: "V2Secret", Effect: "Allow" as const, Action: ["secretsmanager:CreateSecret", "secretsmanager:DescribeSecret", "secretsmanager:PutResourcePolicy", "secretsmanager:GetResourcePolicy", "secretsmanager:DeleteSecret", "secretsmanager:TagResource", "secretsmanager:UntagResource"], Resource: `arn:aws:secretsmanager:${region}:${acct}:secret:96bulls/${stage}/*`, Condition: { ...live, ...regional } },
      { Sid: "V2ReadOnlyLists", Effect: "Allow" as const, Action: ["sqs:ListQueues", "cloudwatch:DescribeAlarms", "logs:DescribeLogGroups", "events:ListRules"], Resource: "*", Condition: { ...live, ...regional } },
      { Sid: "OnlyThisRegion", Effect: "Deny" as const, NotAction: ["iam:*", "sts:*", "s3:*"], Resource: "*", Condition: { StringNotEquals: { "aws:RequestedRegion": region } } },
      { Sid: "NothingAfterExpiry", Effect: "Deny" as const, Action: "*", Resource: "*", Condition: { DateGreaterThan: { "aws:CurrentTime": expiresAt } } },
    ],
  };
}
