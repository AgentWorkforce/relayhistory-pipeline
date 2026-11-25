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
/** The end of the rehearsal's authority: end of day 2026-11-26 Pacific (PST, UTC-8). */
export const REHEARSAL_AUTHORITY_END = "2026-11-27T08:00:00Z";
/**
 * The lease policy's absolute hard cutoff: every Allow ends and a Deny on everything starts. It is
 * the authority's end. The effective lease ends at the EARLIEST of teardown completion, 6 h from
 * issuance (REHEARSAL_LEASE_TTL_HOURS, the broker's issuance parameter) and this cutoff.
 */
export const REHEARSAL_LEASE_EXPIRES_AT = REHEARSAL_AUTHORITY_END;
/** The broker's issuance TTL for the rehearsal lease (not expressible in the policy, which carries the absolute cutoff). */
export const REHEARSAL_LEASE_TTL_HOURS = 6;
/** Stages allowed in validate-only mode, each with its exact state prefix. */
export const VALIDATE_ONLY_STAGES: Readonly<Record<string, string>> = Object.freeze({ [REHEARSAL_STAGE]: "96bulls/96bulls-aws/aws-rehearse-p1-1126/" });

/**
 * Which state backend a stage gets: Alchemy's default store (no pippenSandbox), the pippen ensure
 * path (the proof stage and other fresh-account sandbox stages, as before), or validate-only.
 */
export type StateBackend = "default" | "ensure" | "validate-only";

/**
 * The backend for a stage, decided from VALIDATED inputs before anything is selected, so missing or
 * malformed inputs can never fall through to ensure (or to Alchemy's default store). A declared
 * validate-only stage (VALIDATE_ONLY_STAGES) must have well-formed inputs: a 12-digit account, the
 * pinned region, pippenSandbox with a policy-ARN boundary in that account and stateBucket
 * "validate-only"; anything else throws. For every other stage the old choice is kept, except that
 * a stateBucket value there is refused.
 */
export function stateBackendFor(stage: string, inputs: unknown): StateBackend {
  const v = (inputs ?? null) as { accountId?: unknown; region?: unknown; pippenSandbox?: unknown } | null;
  if (stage in VALIDATE_ONLY_STAGES) {
    if (v === null || typeof v !== "object") throw new StateBucketError("state_inputs_invalid");
    const sbx = v.pippenSandbox as { permissionsBoundaryArn?: unknown; stateBucket?: unknown } | undefined | null;
    if (typeof v.accountId !== "string" || !/^\d{12}$/.test(v.accountId) || v.region !== REHEARSAL_REGION) throw new StateBucketError("state_inputs_invalid");
    if (sbx === null || typeof sbx !== "object" || sbx.stateBucket !== "validate-only") throw new StateBucketError("state_inputs_invalid");
    if (typeof sbx.permissionsBoundaryArn !== "string" || !new RegExp(`^arn:aws:iam::${v.accountId}:policy/[\\w+=,.@/-]+$`).test(sbx.permissionsBoundaryArn)) throw new StateBucketError("state_inputs_invalid");
    return "validate-only";
  }
  if (v === null || typeof v !== "object" || !v.pippenSandbox) return "default";
  if ((v.pippenSandbox as { stateBucket?: unknown }).stateBucket !== undefined) throw new StateBucketError("state_inputs_invalid");
  return "ensure";
}

/**
 * The inputs rule (stacks/aws/inputs.ts validateAwsInputs): the same decision as stateBackendFor,
 * with the stage's error text.
 */
export function assertStateBucketMode(stage: string, inputs: { accountId?: unknown; region?: unknown; pippenSandbox?: unknown }): void {
  try {
    stateBackendFor(stage, inputs);
  } catch {
    if (stage in VALIDATE_ONLY_STAGES)
      throw new Error(`96bulls: stage ${stage} runs only in a Pippen sandbox in ${REHEARSAL_REGION} with pippenSandbox { permissionsBoundaryArn, stateBucket: "validate-only" } (it may never create or converge the shared state bucket). Nothing was created.`);
    throw new Error('96bulls: pippenSandbox.stateBucket is only for a declared validate-only stage. Nothing was created.');
  }
}

export type StateBucketErrorCode =
  | "state_inputs_invalid"
  | "state_lease_expired"
  | "state_teardown_not_empty"
  | "state_resources_remain"
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
 * (`agent-<session_id>`). The account and the lease are bound to the broker's attestation, not to
 * the local inputs alone: `pippen sandbox exec` must have set PIPPEN_SANDBOX_ACCOUNT_ID and
 * PIPPEN_SANDBOX_SESSION_ID, and both must equal the inputs' account and
 * NINETY_SIX_BULLS_PIPPEN_SESSION. STS then proves the credentials are that session.
 */
export function leaseCallerArn(accountId: string, env: Record<string, string | undefined>): string {
  const sbx = PIPPEN_PROOF_SANDBOX;
  const id = env[sbx.sessionEnv];
  if (!/^\d{12}$/.test(accountId) || id === undefined || !PIPPEN_SESSION_ID.test(id)) throw new StateBucketError("state_caller_mismatch");
  if (env[sbx.execSessionEnv] !== id || env[sbx.execAccountEnv] !== accountId) throw new StateBucketError("state_caller_mismatch");
  return `arn:aws:sts::${accountId}:assumed-role/${sbx.callerRole.roleName}/${sbx.callerRole.sessionPrefix}${id}`;
}

/** STS's caller must be exactly the attested lease session for this stage. */
export function assertLeaseCaller(aws: AwsRun, accountId: string, env: Record<string, string | undefined>): void {
  const expected = leaseCallerArn(accountId, env);
  let who: { Arn?: unknown; Account?: unknown };
  try {
    who = JSON.parse(aws(["sts", "get-caller-identity", "--output", "json"]) || "{}") as typeof who;
  } catch {
    throw new StateBucketError("state_caller_mismatch");
  }
  if (who.Arn !== expected || who.Account !== accountId) throw new StateBucketError("state_caller_mismatch");
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
 * the caller is this stage's attested lease session, the prefix is exactly this stage's, and the
 * shared bucket is already in shape. Nothing is created or written.
 */
export function assertValidateOnlyState(aws: AwsRun, i: { accountId: string; region: string; stage: string; env: Record<string, string | undefined> }): { bucket: string; prefix: string } {
  const prefix = rehearsalStatePrefix(i.stage);
  if (i.region !== REHEARSAL_REGION) throw new StateBucketError("state_inputs_invalid");
  assertLeaseCaller(aws, i.accountId, i.env);
  return { bucket: validatePippenStateBucket(aws, i), prefix };
}

/**
 * The one entry both the capture (src/cli/aws-provenance-io.ts) and the stack's state layer
 * (stacks/aws/alchemy.aws.run.ts) use: decide the backend from the validated inputs
 * (stateBackendFor, which throws on missing or malformed inputs for a validate-only stage), then
 * either check the bucket (validate-only, no writes) or run `ensure` (the old path). Returns the backend.
 */
export function prepareStateBackend(
  aws: AwsRun,
  i: { stage: string; inputs: unknown; env: Record<string, string | undefined> },
  ensure: (aws: AwsRun, t: { accountId: string; region: string; stage: string }) => unknown,
): StateBackend {
  const backend = stateBackendFor(i.stage, i.inputs);
  const v = i.inputs as { accountId: string; region: string };
  if (backend === "validate-only") assertValidateOnlyState(aws, { accountId: v.accountId, region: v.region, stage: i.stage, env: i.env });
  else if (backend === "ensure") ensure(aws, { accountId: v.accountId, region: v.region, stage: i.stage });
  return backend;
}

interface VersionEntry {
  Key?: unknown;
  VersionId?: unknown;
}

/** The fields a ListObjectVersions response may carry (no delimiter is used, so no CommonPrefixes). */
const LIST_VERSIONS_FIELDS = new Set(["Versions", "DeleteMarkers", "IsTruncated", "KeyMarker", "VersionIdMarker", "NextKeyMarker", "NextVersionIdMarker", "Name", "Prefix", "MaxKeys", "EncodingType", "RequestCharged"]);

/** One ListObjectVersions page, strictly: blank or unparseable output, an unknown field, a non-boolean IsTruncated, a missing continuation marker or a non-array list refuse. */
function listVersionsPage(aws: AwsRun, args: string[], prefix: string) {
  let text: string;
  try {
    text = aws(args);
  } catch {
    throw new StateBucketError("state_teardown_failed");
  }
  if (!text || !text.trim()) throw new StateBucketError("state_teardown_failed");
  let page: Record<string, unknown>;
  try {
    page = JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new StateBucketError("state_teardown_failed");
  }
  if (page === null || typeof page !== "object" || Array.isArray(page)) throw new StateBucketError("state_teardown_failed");
  if (Object.keys(page).some((k) => !LIST_VERSIONS_FIELDS.has(k))) throw new StateBucketError("state_teardown_failed");
  if (typeof page.IsTruncated !== "boolean") throw new StateBucketError("state_teardown_failed");
  if (page.Prefix !== undefined && page.Prefix !== prefix) throw new StateBucketError("state_teardown_failed");
  for (const k of ["Versions", "DeleteMarkers"]) if (page[k] !== undefined && !Array.isArray(page[k])) throw new StateBucketError("state_teardown_failed");
  if (page.IsTruncated && !(typeof page.NextKeyMarker === "string" && page.NextKeyMarker && typeof page.NextVersionIdMarker === "string" && page.NextVersionIdMarker))
    throw new StateBucketError("state_teardown_failed");
  return page as { Versions?: VersionEntry[]; DeleteMarkers?: VersionEntry[]; IsTruncated: boolean; NextKeyMarker?: string; NextVersionIdMarker?: string };
}

/**
 * Every object version and delete marker under the stage's exact prefix (ListObjectVersions with
 * KeyMarker/VersionIdMarker pagination, each page validated strictly). Refuses, before anything is
 * deleted, when any listed key is outside the prefix or an entry has no key or version id.
 */
export function listStageStateVersions(aws: AwsRun, i: { bucket: string; region: string; stage: string }): { Key: string; VersionId: string }[] {
  const prefix = rehearsalStatePrefix(i.stage);
  const out: { Key: string; VersionId: string }[] = [];
  let marker: { key: string; version: string } | undefined;
  for (let n = 0; n < 10_000; n++) {
    const page = listVersionsPage(
      aws,
      [
        "s3api", "list-object-versions", "--bucket", i.bucket, "--region", i.region, "--prefix", prefix, "--output", "json", "--no-paginate",
        ...(marker ? ["--key-marker", marker.key, "--version-id-marker", marker.version] : []),
      ],
      prefix,
    );
    for (const v of [...(page.Versions ?? []), ...(page.DeleteMarkers ?? [])]) {
      if (v === null || typeof v !== "object" || typeof v.Key !== "string" || typeof v.VersionId !== "string" || !v.VersionId) throw new StateBucketError("state_teardown_failed");
      if (!v.Key.startsWith(prefix) || v.Key.length === prefix.length) throw new StateBucketError("state_teardown_outside_prefix");
      out.push({ Key: v.Key, VersionId: v.VersionId });
    }
    if (!page.IsTruncated) return out;
    marker = { key: page.NextKeyMarker!, version: page.NextVersionIdMarker! };
  }
  throw new StateBucketError("state_teardown_failed");
}

/**
 * Teardown of this stage's state only: list (above, refusing anything outside the prefix), then
 * DeleteObjects by exact key and version id, at most 1000 per call, rechecking every key against
 * the prefix before each call, and then an independent re-list that must show zero versions and
 * zero delete markers (`state_teardown_not_empty` otherwise) before success is reported.
 * `execute: false` (the default) lists and deletes nothing. Sibling stages and the bucket itself
 * are never touched. Run it only after assertRehearsalResourcesGone.
 */
export function teardownStageState(aws: AwsRun, i: { bucket: string; region: string; stage: string; execute?: boolean }): { listed: number; deleted: number; remaining: number } {
  const prefix = rehearsalStatePrefix(i.stage);
  const all = listStageStateVersions(aws, i);
  if (!i.execute) return { listed: all.length, deleted: 0, remaining: all.length };
  let deleted = 0;
  for (let n = 0; n < all.length; n += 1000) {
    const batch = all.slice(n, n + 1000);
    if (!batch.every((o) => o.Key.startsWith(prefix) && o.Key.length > prefix.length)) throw new StateBucketError("state_teardown_outside_prefix");
    let result: { Errors?: unknown; Deleted?: unknown };
    try {
      const text = aws(["s3api", "delete-objects", "--bucket", i.bucket, "--region", i.region, "--output", "json", "--delete", JSON.stringify({ Objects: batch, Quiet: true })]);
      result = text.trim() ? JSON.parse(text) : {};
    } catch {
      throw new StateBucketError("state_teardown_failed");
    }
    if (result === null || typeof result !== "object" || (result.Errors !== undefined && !(Array.isArray(result.Errors) && result.Errors.length === 0))) throw new StateBucketError("state_teardown_failed");
    deleted += batch.length;
  }
  // Explicit empty-prefix evidence: an independent re-list of versions and delete markers.
  const remaining = listStageStateVersions(aws, i).length;
  if (remaining !== 0) throw new StateBucketError("state_teardown_not_empty");
  return { listed: all.length, deleted, remaining };
}

/**
 * The rehearsal's synthetic inventory (session-pipeline.ts naming for the v2 search split): the
 * function, four queues, two mappings, five alarms (the three the pipeline always has plus the two
 * search alarms) and the function's log group. The pipeline's reconcile and brief schedules and its
 * secret are not part of it (and the lease grants nothing on them): the non-prod render must leave
 * them out for this stage.
 */
export function rehearsalResourceNames(stage: string = REHEARSAL_STAGE) {
  const n = (base: string) => `96bulls-${stage}-${base}`;
  return {
    function: n("session-pipeline"),
    queues: [n("session-events"), n("session-events-dlq"), n("session-search"), n("session-search-dlq")],
    alarms: [`${n("session-events-dlq")}-not-empty`, `${n("session-events")}-backlog-age`, `${n("session-pipeline")}-errors`, `${n("session-search-dlq")}-not-empty`, `${n("session-search")}-backlog-age`],
    logGroup: `/aws/lambda/${n("session-pipeline")}`,
  };
}

const NOT_FOUND = /ResourceNotFoundException|NonExistentQueue|QueueDoesNotExist/;

/**
 * Before the state is discarded: every recorded physical resource is gone. Each must read back as
 * not found (the function, each queue, each recorded mapping UUID), the alarms must list empty, and
 * no log group of the function's exact name may remain. Any resource that still exists refuses
 * (`state_resources_remain`); any other failure refuses (`state_teardown_failed`). Read-only.
 */
export function assertRehearsalResourcesGone(aws: AwsRun, i: { region: string; stage: string; mappingUuids: readonly string[] }): void {
  rehearsalStatePrefix(i.stage);
  const names = rehearsalResourceNames(i.stage);
  const r = ["--region", i.region, "--output", "json"];
  const gone = (args: string[]) => {
    try {
      aws([...args, ...r]);
    } catch (e) {
      if (NOT_FOUND.test(e instanceof Error ? e.message : String(e))) return;
      throw new StateBucketError("state_teardown_failed");
    }
    throw new StateBucketError("state_resources_remain");
  };
  gone(["lambda", "get-function-configuration", "--function-name", names.function]);
  for (const q of names.queues) gone(["sqs", "get-queue-url", "--queue-name", q]);
  for (const u of i.mappingUuids) {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(u)) throw new StateBucketError("state_teardown_failed");
    gone(["lambda", "get-event-source-mapping", "--uuid", u]);
  }
  const read = (args: string[]) => {
    try {
      const text = aws([...args, ...r]);
      if (!text.trim()) throw new Error("blank");
      return JSON.parse(text) as Record<string, unknown>;
    } catch {
      throw new StateBucketError("state_teardown_failed");
    }
  };
  const alarms = read(["cloudwatch", "describe-alarms", "--alarm-names", ...names.alarms]);
  if (!Array.isArray(alarms.MetricAlarms) || (alarms.CompositeAlarms !== undefined && !Array.isArray(alarms.CompositeAlarms))) throw new StateBucketError("state_teardown_failed");
  if (alarms.MetricAlarms.length > 0 || ((alarms.CompositeAlarms as unknown[] | undefined)?.length ?? 0) > 0) throw new StateBucketError("state_resources_remain");
  const groups = read(["logs", "describe-log-groups", "--log-group-name-prefix", names.logGroup]);
  if (!Array.isArray(groups.logGroups)) throw new StateBucketError("state_teardown_failed");
  if ((groups.logGroups as { logGroupName?: unknown }[]).some((g) => g?.logGroupName === names.logGroup)) throw new StateBucketError("state_resources_remain");
}

/** The tag every rehearsal resource carries, required on create and on every act (lease policy). */
export const REHEARSAL_TAG = Object.freeze({ key: "rehearsal", value: "p1-1126" });

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
const iso = (ms: number) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");

/**
 * The effective lease end: the EARLIEST of the hard cutoff (2026-11-27T08:00:00Z, exclusive),
 * issuance + 6 h, teardown completion, and a requested end. A requested end later than the
 * earliest of the others is refused, never silently shortened.
 */
export function rehearsalLeaseEnd(i: { issuedAt?: string; teardownCompletedAt?: string; requested?: string } = {}): string {
  for (const t of [i.issuedAt, i.teardownCompletedAt, i.requested]) if (t !== undefined && !ISO.test(t)) throw new StateBucketError("state_lease_expired");
  const caps = [Date.parse(REHEARSAL_AUTHORITY_END)];
  if (i.issuedAt !== undefined) caps.push(Date.parse(i.issuedAt) + REHEARSAL_LEASE_TTL_HOURS * 3_600_000);
  if (i.teardownCompletedAt !== undefined) caps.push(Date.parse(i.teardownCompletedAt));
  const cap = Math.min(...caps);
  if (i.issuedAt !== undefined && Date.parse(i.issuedAt) >= cap) throw new StateBucketError("state_lease_expired");
  if (i.requested === undefined) return iso(cap);
  if (Date.parse(i.requested) > cap) throw new StateBucketError("state_lease_expired");
  return i.requested;
}

/**
 * The rehearsal lease's session policies, sized for STS: one small inline session policy (the
 * denies: the exclusive cutoff, other regions, any write to a shared state bucket, untagging the
 * rehearsal tag) and three managed session policies the broker creates from `managed` and passes by
 * ARN (AssumeRole `PolicyArns`): state access on the stage prefix only; creating the v2 resources
 * only with the rehearsal tag and this stage's names; acting on them only when they carry the tag.
 * The session's permissions are the role's policies intersected with these. Every Allow ends before
 * the end (DateLessThan), and a Deny on everything applies at and after it (DateGreaterThanEquals).
 */
export function rehearsalLeasePolicies(i: { accountId: string; stage?: string; region?: string; issuedAt?: string; expiresAt?: string }) {
  const stage = i.stage ?? REHEARSAL_STAGE;
  const region = i.region ?? REHEARSAL_REGION;
  if (region !== REHEARSAL_REGION || !/^\d{12}$/.test(i.accountId)) throw new StateBucketError("state_inputs_invalid");
  const end = rehearsalLeaseEnd({ ...(i.issuedAt ? { issuedAt: i.issuedAt } : {}), ...(i.expiresAt ? { requested: i.expiresAt } : {}) });
  const prefix = rehearsalStatePrefix(stage);
  const bucket = pippenStateBucket(i.accountId, region);
  const bucketArn = `arn:aws:s3:::${bucket}`;
  const a = i.accountId;
  const name = `96bulls-${stage}-*`;
  const fnArn = `arn:aws:lambda:${region}:${a}:function:${name}`;
  const logArn = `arn:aws:logs:${region}:${a}:log-group:/aws/lambda/${name}`;
  const before = { DateLessThan: { "aws:CurrentTime": end } };
  const tagged = { StringEquals: { [`aws:ResourceTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value } };
  const tagging = { StringEquals: { [`aws:RequestTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value } };
  const statement = (Sid: string, Action: string[], Resource: string | string[], Condition: Record<string, unknown>) => ({ Sid, Effect: "Allow" as const, Action, Resource, Condition: { ...before, ...Condition } });
  const inline = {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "End", Effect: "Deny" as const, Action: "*", Resource: "*", Condition: { DateGreaterThanEquals: { "aws:CurrentTime": end } } },
      { Sid: "Region", Effect: "Deny" as const, NotAction: ["iam:*", "sts:*", "s3:*"], Resource: "*", Condition: { StringNotEquals: { "aws:RequestedRegion": region } } },
      { Sid: "SharedBucket", Effect: "Deny" as const, Action: ["s3:CreateBucket", "s3:PutBucket*", "s3:DeleteBucket*", "s3:PutEncryptionConfiguration", "s3:PutLifecycleConfiguration"], Resource: "arn:aws:s3:::pippen-sbx-alchemy-state-*" },
      { Sid: "KeepTag", Effect: "Deny" as const, Action: ["sqs:UntagQueue", "lambda:UntagResource", "cloudwatch:UntagResource", "logs:UntagResource"], Resource: "*", Condition: { "ForAnyValue:StringEquals": { "aws:TagKeys": [REHEARSAL_TAG.key] } } },
    ],
  };
  const managed = [
    {
      name: `96bulls-rehearse-p1-1126-state`,
      document: {
        Version: "2012-10-17" as const,
        Statement: [
          statement("List", ["s3:ListBucket", "s3:ListBucketVersions"], bucketArn, { StringLike: { "s3:prefix": [`${prefix}*`] } }),
          statement("Objects", ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"], `${bucketArn}/${prefix}*`, {}),
          statement("Settings", ["s3:GetBucketVersioning", "s3:GetEncryptionConfiguration", "s3:GetBucketPublicAccessBlock", "s3:GetBucketOwnershipControls", "s3:GetBucketTagging"], bucketArn, {}),
          { Sid: "OtherPrefixes", Effect: "Deny" as const, Action: ["s3:ListBucket", "s3:ListBucketVersions"], Resource: bucketArn, Condition: { StringNotLike: { "s3:prefix": [`${prefix}*`] } } },
          { Sid: "NotThisPrefix", Effect: "Deny" as const, Action: ["s3:GetObject*", "s3:PutObject*", "s3:DeleteObject*"], NotResource: `${bucketArn}/${prefix}*` },
        ],
      },
    },
    {
      name: `96bulls-rehearse-p1-1126-create`,
      document: {
        Version: "2012-10-17" as const,
        Statement: [
          statement("Queues", ["sqs:CreateQueue", "sqs:TagQueue"], `arn:aws:sqs:${region}:${a}:${name}`, tagging),
          statement("Function", ["lambda:CreateFunction", "lambda:TagResource"], fnArn, tagging),
          statement("Mappings", ["lambda:CreateEventSourceMapping", "lambda:TagResource"], "*", { ...tagging, ArnLike: { "lambda:FunctionArn": fnArn } }),
          statement("Alarms", ["cloudwatch:PutMetricAlarm", "cloudwatch:TagResource"], `arn:aws:cloudwatch:${region}:${a}:alarm:${name}`, tagging),
          statement("Logs", ["logs:CreateLogGroup", "logs:TagResource"], [logArn, `${logArn}:*`], tagging),
        ],
      },
    },
    {
      name: `96bulls-rehearse-p1-1126-act`,
      document: {
        Version: "2012-10-17" as const,
        Statement: [
          statement("Queues", ["sqs:GetQueueAttributes", "sqs:SetQueueAttributes", "sqs:ListQueueTags", "sqs:DeleteQueue", "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"], `arn:aws:sqs:${region}:${a}:${name}`, tagged),
          statement("Function", [
            "lambda:GetFunction", "lambda:GetFunctionConfiguration", "lambda:UpdateFunctionCode", "lambda:UpdateFunctionConfiguration", "lambda:DeleteFunction", "lambda:PublishVersion",
            "lambda:ListAliases", "lambda:ListVersionsByFunction", "lambda:AddPermission", "lambda:RemovePermission", "lambda:GetPolicy", "lambda:ListTags", "lambda:InvokeFunction",
          ], fnArn, tagged),
          statement("Mappings", ["lambda:GetEventSourceMapping", "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping"], "*", { ArnLike: { "lambda:FunctionArn": fnArn } }),
          statement("Alarms", ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:ListTagsForResource"], `arn:aws:cloudwatch:${region}:${a}:alarm:${name}`, tagged),
          statement("Logs", ["logs:PutRetentionPolicy", "logs:DeleteLogGroup", "logs:ListTagsForResource", "logs:StartQuery", "logs:FilterLogEvents"], [logArn, `${logArn}:*`], tagged),
          statement("Reads", ["sqs:GetQueueUrl", "sqs:ListQueues", "lambda:ListEventSourceMappings", "cloudwatch:DescribeAlarms", "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData", "logs:DescribeLogGroups", "logs:GetQueryResults"], "*", {}),
        ],
      },
    },
  ];
  return { expiresAt: end, inline, managed };
}

/**
 * The mapping UUIDs Alchemy recorded for this stage (its state objects `<prefix>session-events-mapping.json`
 * and `<prefix>session-search-mapping.json`, attr.uuid), for assertRehearsalResourcesGone. An absent
 * object records nothing; an unreadable or malformed one refuses.
 */
export function recordedMappingUuids(aws: AwsRun, i: { bucket: string; region: string; stage: string }): string[] {
  const prefix = rehearsalStatePrefix(i.stage);
  const out: string[] = [];
  for (const id of ["session-events-mapping", "session-search-mapping"]) {
    let text: string;
    try {
      text = aws(["s3", "cp", `s3://${i.bucket}/${prefix}${id}.json`, "-", "--region", i.region]);
    } catch (e) {
      if (/NoSuchKey|\(404\)/.test(e instanceof Error ? e.message : String(e))) continue;
      throw new StateBucketError("state_teardown_failed");
    }
    let uuid: unknown;
    try {
      uuid = (JSON.parse(text) as { attr?: { uuid?: unknown } }).attr?.uuid;
    } catch {
      throw new StateBucketError("state_teardown_failed");
    }
    if (typeof uuid !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(uuid)) throw new StateBucketError("state_teardown_failed");
    out.push(uuid);
  }
  return out;
}
