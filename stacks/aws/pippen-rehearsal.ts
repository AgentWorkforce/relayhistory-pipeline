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
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
    // The execution role and what teardown does with it (pinned with the account by the issuer).
    const role = (sbx as { executionRole?: { arn?: unknown; disposition?: unknown } }).executionRole;
    if (role === null || typeof role !== "object" || typeof role.arn !== "string" || !new RegExp(`^arn:aws:iam::${v.accountId}:role/(?:[A-Za-z0-9_+=,.@-]+/)*[A-Za-z0-9_+=,.@-]{1,64}$`).test(role.arn))
      throw new StateBucketError("state_inputs_invalid");
    if (role.disposition !== "deleted" && !(role.disposition === "retained" && role.arn.startsWith(`arn:aws:iam::${v.accountId}:role/pippen/workload/`))) throw new StateBucketError("state_inputs_invalid");
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
  | "state_issuer_pin_mismatch"
  | "state_provenance_mismatch"
  | "state_not_claimed"
  | "state_authorization_mismatch"
  | "state_gone_unproven"
  | "state_receipt_mismatch"
  | "state_manifest_invalid"
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

/** The env variable the broker sets at issuance with the issuer pin (rehearsalIssuerPin). */
export const ISSUER_PIN_ENV = "PIPPEN_REHEARSAL_ISSUER_PIN_SHA256";
/** The env variable the broker sets at issuance with the lease's issuance time (ISO, UTC). */
export const ISSUED_AT_ENV = "PIPPEN_REHEARSAL_ISSUED_AT";
/** The operator's id for this rehearsal run (a UUID), the same for the manifest and the teardown. */
export const RUN_ID_ENV = "NINETY_SIX_BULLS_REHEARSAL_RUN_ID";

/** What the issuer pins: the approved stage, region, account, boundary, execution role and its disposition, and the lease. */
export interface IssuerPinned {
  stage: string;
  region: string;
  accountId: string;
  boundaryArn: string;
  roleArn: string;
  roleDisposition: "deleted" | "retained";
  sessionId: string;
  issuedAt: string;
}

/**
 * The issuer pin: sha256 of the canonical JSON of what the issuer approved (IssuerPinned). The broker
 * computes it from its own records, never from the stage inputs, and passes it at issuance in
 * PIPPEN_REHEARSAL_ISSUER_PIN_SHA256; the real values never enter the repository.
 *
 * Provenance: the code checks that the inputs and the lease env hash to the pin it is given. It
 * cannot prove where the pin came from: only the broker can (its issuance receipt). The check is
 * necessary, not sufficient.
 */
export function rehearsalIssuerPin(i: IssuerPinned): string {
  return createHash("sha256").update(JSON.stringify([i.stage, i.region, i.accountId, i.boundaryArn, i.roleArn, i.roleDisposition, i.sessionId, i.issuedAt])).digest("hex");
}

/** The validated context of one rehearsal run: the pinned values, the pin itself, and the run id. */
export interface RehearsalContext extends IssuerPinned {
  pin: string;
  runId: string;
}

/**
 * Everything that can be checked without AWS, before any AWS call: the inputs (stateBackendFor), the
 * lease env (account, session, issuance time), the issuer pin over all of them, and that the lease
 * has not ended (rehearsalLeaseEnd from the issuance time). Returns undefined for any stage that is
 * not a validate-only stage (nothing to check).
 */
export function rehearsalRunContext(stage: string, inputs: unknown, env: Record<string, string | undefined>, now: number = Date.now()): RehearsalContext | undefined {
  if (stateBackendFor(stage, inputs) !== "validate-only") return undefined;
  const v = inputs as { accountId: string; region: string; pippenSandbox: { permissionsBoundaryArn: string; executionRole: { arn: string; disposition: "deleted" | "retained" } } };
  leaseCallerArn(v.accountId, env);
  const issuedAt = env[ISSUED_AT_ENV];
  if (issuedAt === undefined || !ISO.test(issuedAt)) throw new StateBucketError("state_issuer_pin_mismatch");
  const pinned: IssuerPinned = {
    stage,
    region: v.region,
    accountId: v.accountId,
    boundaryArn: v.pippenSandbox.permissionsBoundaryArn,
    roleArn: v.pippenSandbox.executionRole.arn,
    roleDisposition: v.pippenSandbox.executionRole.disposition,
    sessionId: env[PIPPEN_PROOF_SANDBOX.sessionEnv]!,
    issuedAt,
  };
  const pin = env[ISSUER_PIN_ENV];
  if (pin === undefined || !/^[0-9a-f]{64}$/.test(pin) || pin !== rehearsalIssuerPin(pinned)) throw new StateBucketError("state_issuer_pin_mismatch");
  if (!(now >= Date.parse(issuedAt) && now < Date.parse(rehearsalLeaseEnd({ issuedAt })))) throw new StateBucketError("state_lease_expired");
  const runId = env[RUN_ID_ENV];
  if (runId === undefined || !UUID.test(runId)) throw new StateBucketError("state_inputs_invalid");
  return { ...pinned, pin, runId };
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
 * validate-only, as the state layer and the capture run it before Alchemy's first state operation.
 * READ-ONLY: it never writes (the marker is written only by the explicit claim, claimStageState).
 * The caller is this run's attested lease session (STS), the prefix is exactly this stage's, the
 * shared bucket is in shape, and the stage's state must be claimed by THIS run (its marker present
 * and exactly this run's: run id, lease session, issuance time, pin). An unclaimed (empty) prefix
 * refuses (`state_not_claimed`). While the marker is the only current object (stage A not applied),
 * none of the stage's names may be in use (assertRehearsalNamesUnused).
 */
export function assertValidateOnlyState(aws: AwsRun, ctx: RehearsalContext): { bucket: string; prefix: string } {
  const prefix = rehearsalStatePrefix(ctx.stage);
  if (ctx.region !== REHEARSAL_REGION) throw new StateBucketError("state_inputs_invalid");
  assertLeaseCaller(aws, ctx.accountId, { [PIPPEN_PROOF_SANDBOX.sessionEnv]: ctx.sessionId, [PIPPEN_PROOF_SANDBOX.execSessionEnv]: ctx.sessionId, [PIPPEN_PROOF_SANDBOX.execAccountEnv]: ctx.accountId });
  const bucket = validatePippenStateBucket(aws, ctx);
  const current = currentStateKeys(aws, { bucket, region: ctx.region, prefix });
  if (current.length === 0) throw new StateBucketError("state_not_claimed");
  assertRunMarker(aws, { bucket, prefix, ctx });
  if (current.length === 1 && current[0] === runMarkerKey(prefix)) assertRehearsalNamesUnused(aws, ctx);
  return { bucket, prefix };
}

/**
 * The explicit claim of the stage's state by this run (scripts/rehearsal-state-teardown.ts
 * --claim), before the first capture: the attested caller and the bucket checked; the prefix's
 * VERSION inventory must be empty (zero versions and zero delete markers, so no prior, unowned
 * history can be claimed and later deleted); none of the stage's names may be in use; then this
 * run's marker is written with PutObject --if-none-match "*". The only write in validate-only mode.
 */
export function claimStageState(aws: AwsRun, ctx: RehearsalContext): { bucket: string; marker: string } {
  const prefix = rehearsalStatePrefix(ctx.stage);
  assertLeaseCaller(aws, ctx.accountId, { [PIPPEN_PROOF_SANDBOX.sessionEnv]: ctx.sessionId, [PIPPEN_PROOF_SANDBOX.execSessionEnv]: ctx.sessionId, [PIPPEN_PROOF_SANDBOX.execAccountEnv]: ctx.accountId });
  const bucket = validatePippenStateBucket(aws, ctx);
  if (listStageStateVersions(aws, { bucket, region: ctx.region, stage: ctx.stage }).length !== 0) throw new StateBucketError("state_provenance_mismatch");
  assertRehearsalNamesUnused(aws, ctx);
  writeRunMarker(aws, { bucket, prefix, ctx });
  return { bucket, marker: runMarkerKey(prefix) };
}

/**
 * The stage prefix's current keys (ListObjectsV2, at most two): the reply must be well formed, with
 * KeyCount exactly 0, 1 or 2 matching its Contents, every key under the prefix; anything else refuses.
 */
function currentStateKeys(aws: AwsRun, i: { bucket: string; region: string; prefix: string }): string[] {
  let listed: Record<string, unknown>;
  try {
    const text = aws(["s3api", "list-objects-v2", "--bucket", i.bucket, "--region", i.region, "--prefix", i.prefix, "--max-keys", "2", "--no-paginate", "--output", "json"]);
    if (!text.trim()) throw new Error("blank");
    listed = JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new StateBucketError("state_bucket_unreadable");
  }
  if (listed === null || typeof listed !== "object" || Array.isArray(listed)) throw new StateBucketError("state_bucket_unreadable");
  const contents = listed.Contents ?? [];
  if (
    !(listed.KeyCount === 0 || listed.KeyCount === 1 || listed.KeyCount === 2) ||
    !Array.isArray(contents) || contents.length !== listed.KeyCount ||
    !contents.every((c) => typeof (c as { Key?: unknown })?.Key === "string" && (c as { Key: string }).Key.startsWith(i.prefix) && (c as { Key: string }).Key.length > i.prefix.length) ||
    (listed.Prefix !== undefined && listed.Prefix !== i.prefix)
  )
    throw new StateBucketError("state_bucket_unreadable");
  return (contents as { Key: string }[]).map((c) => c.Key);
}

/** The run marker's key: not `.json`, so Alchemy's state store (which lists `*.json` resources) ignores it. */
export const runMarkerKey = (prefix: string) => `${prefix}__rehearsal_run__.marker`;

/** The run marker's body: this run, this lease, this pin. */
export function runMarkerBody(ctx: RehearsalContext): string {
  return JSON.stringify({ schema: 1, stage: ctx.stage, runId: ctx.runId, sessionId: ctx.sessionId, issuedAt: ctx.issuedAt, pin: ctx.pin });
}

function writeRunMarker(aws: AwsRun, i: { bucket: string; prefix: string; ctx: RehearsalContext }): void {
  const dir = mkdtempSync(join(tmpdir(), "96bulls-marker-"));
  try {
    const file = join(dir, "marker");
    writeFileSync(file, runMarkerBody(i.ctx), { mode: 0o600 });
    // Only if absent: never overwrite another run's marker.
    aws(["s3api", "put-object", "--bucket", i.bucket, "--region", i.ctx.region, "--key", runMarkerKey(i.prefix), "--body", file, "--if-none-match", "*", "--output", "json"]);
  } catch {
    throw new StateBucketError("state_provenance_mismatch");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/** A populated prefix is resumed only when its marker is exactly this run's (run id, lease, pin). */
function assertRunMarker(aws: AwsRun, i: { bucket: string; prefix: string; ctx: RehearsalContext }): void {
  let text: string;
  try {
    text = aws(["s3", "cp", `s3://${i.bucket}/${runMarkerKey(i.prefix)}`, "-", "--region", i.ctx.region]);
  } catch {
    throw new StateBucketError("state_provenance_mismatch");
  }
  if (text.trim() !== runMarkerBody(i.ctx)) throw new StateBucketError("state_provenance_mismatch");
}

/**
 * The one entry both the capture (src/cli/aws-provenance-io.ts) and the stack's state layer
 * (stacks/aws/alchemy.aws.run.ts) use: decide the backend and validate everything that needs no AWS
 * (rehearsalRunContext), then either check the bucket and the names (validate-only, no writes) or
 * run `ensure` (the old path). Returns the backend.
 */
export function prepareStateBackend(
  aws: AwsRun,
  i: { stage: string; inputs: unknown; env: Record<string, string | undefined>; now?: number },
  ensure: (aws: AwsRun, t: { accountId: string; region: string; stage: string }) => unknown,
): StateBackend {
  const backend = stateBackendFor(i.stage, i.inputs);
  const v = i.inputs as { accountId: string; region: string };
  // Read-only in validate-only mode (the claim is a separate, explicit step).
  if (backend === "validate-only") assertValidateOnlyState(aws, rehearsalRunContext(i.stage, i.inputs, i.env, i.now)!);
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
 * are never touched. Run it only after assertManifestGone has passed.
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

const NOT_FOUND = /ResourceNotFoundException|NonExistentQueue|QueueDoesNotExist|NoSuchEntity/;
const DENIED = /AccessDenied|UnauthorizedOperation|not authorized/i;

/**
 * An absence readback: NotFound passes; AccessDenied is never "gone" (a tag-conditioned read of a
 * deleted resource can answer AccessDenied): `state_gone_unproven`; a success means the resource
 * exists: `state_resources_remain`; any other failure: `state_teardown_failed`.
 */
function absent(aws: AwsRun, args: string[]): void {
  try {
    aws(args);
  } catch (e) {
    const text = e instanceof Error ? e.message : String(e);
    if (DENIED.test(text)) throw new StateBucketError("state_gone_unproven");
    if (NOT_FOUND.test(text)) return;
    throw new StateBucketError("state_teardown_failed");
  }
  throw new StateBucketError("state_resources_remain");
}

/** DescribeAlarms (metric and composite) strictly: both arrays, no other field, no NextToken. */
function readAlarms(aws: AwsRun, names: readonly string[], region: string, code: StateBucketErrorCode): { metric: string[]; composite: string[] } {
  let v: Record<string, unknown>;
  try {
    const text = aws(["cloudwatch", "describe-alarms", "--alarm-types", "MetricAlarm", "CompositeAlarm", "--alarm-names", ...names, "--region", region, "--output", "json"]);
    if (!text.trim()) throw new Error("blank");
    v = JSON.parse(text) as Record<string, unknown>;
  } catch (e) {
    if (DENIED.test(e instanceof Error ? e.message : String(e))) throw new StateBucketError("state_gone_unproven");
    throw new StateBucketError(code);
  }
  if (v === null || typeof v !== "object" || Array.isArray(v) || !Array.isArray(v.MetricAlarms) || !Array.isArray(v.CompositeAlarms) || Object.keys(v).some((k) => k !== "MetricAlarms" && k !== "CompositeAlarms"))
    throw new StateBucketError(code);
  const nameOf = (a: unknown) => (typeof (a as { AlarmName?: unknown })?.AlarmName === "string" ? (a as { AlarmName: string }).AlarmName : null);
  const metric = v.MetricAlarms.map(nameOf), composite = v.CompositeAlarms.map(nameOf);
  if ([...metric, ...composite].some((n) => n === null)) throw new StateBucketError(code);
  return { metric: metric as string[], composite: composite as string[] };
}

/** DescribeLogGroups for an exact name strictly: the array, no other field, no nextToken. */
function readLogGroups(aws: AwsRun, name: string, region: string, code: StateBucketErrorCode): string[] {
  let v: Record<string, unknown>;
  try {
    const text = aws(["logs", "describe-log-groups", "--log-group-name-prefix", name, "--region", region, "--output", "json"]);
    if (!text.trim()) throw new Error("blank");
    v = JSON.parse(text) as Record<string, unknown>;
  } catch (e) {
    if (DENIED.test(e instanceof Error ? e.message : String(e))) throw new StateBucketError("state_gone_unproven");
    throw new StateBucketError(code);
  }
  if (v === null || typeof v !== "object" || Array.isArray(v) || !Array.isArray(v.logGroups) || Object.keys(v).some((k) => k !== "logGroups")) throw new StateBucketError(code);
  const out = v.logGroups.map((g: unknown) => (g as { logGroupName?: unknown })?.logGroupName);
  if (out.some((n) => typeof n !== "string")) throw new StateBucketError(code);
  return out as string[];
}
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * The pre-destroy physical manifest: every id the stage's resources have, read from AWS before any
 * destroy, persisted (scripts/rehearsal-state-teardown.ts writes it once under the gitignored
 * .96bulls/<stage>/ and never rewrites it), bound to this run and lease, and checked id by id before
 * the state is discarded, so a partial destroy cannot leave a resource the state no longer names.
 */
export interface PhysicalManifest {
  schema: 2;
  stage: string;
  region: string;
  accountId: string;
  /** The run and lease the manifest belongs to: a stale manifest (another run, lease or pin) is refused. */
  binding: { runId: string; sessionId: string; issuedAt: string; pin: string };
  function: { name: string; arn: string };
  /** The pinned execution role and disposition, with the pinned boundary (checked for a retained role). */
  role: { arn: string; disposition: "deleted" | "retained"; boundaryArn: string };
  queues: { name: string; url: string; arn: string }[];
  mappings: { uuid: string; eventSourceArn: string }[];
  alarms: string[];
  logGroup: string;
  /** sha256 of the canonical manifest without this field: an edited manifest is refused. */
  digest: string;
}

const cliJson = (aws: AwsRun, args: string[]): Record<string, unknown> => {
  let text: string;
  try {
    text = aws([...args, "--output", "json"]);
  } catch {
    throw new StateBucketError("state_manifest_invalid");
  }
  try {
    const v = JSON.parse(text) as unknown;
    if (v === null || typeof v !== "object" || Array.isArray(v)) throw new Error("shape");
    return v as Record<string, unknown>;
  } catch {
    throw new StateBucketError("state_manifest_invalid");
  }
};

const canonical = (v: unknown): string =>
  Array.isArray(v) ? `[${v.map(canonical).join(",")}]` : v !== null && typeof v === "object" ? `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`).join(",")}}` : JSON.stringify(v);

/** The manifest's digest: sha256 of its canonical JSON without `digest`. */
export function manifestDigest(m: Omit<PhysicalManifest, "digest"> & { digest?: string }): string {
  const { digest: _d, ...rest } = m;
  return createHash("sha256").update(canonical(rest)).digest("hex");
}

/**
 * Read the manifest from the live resources (before any destroy), for this run. Every resource must
 * exist and be this stage's: the function, with exactly the pinned execution role; the four queues;
 * exactly two mappings (one on the events queue, one on the search queue, both on the function);
 * the five alarms and the log group.
 */
export function readPhysicalManifest(aws: AwsRun, ctx: RehearsalContext, runId: string): PhysicalManifest {
  rehearsalStatePrefix(ctx.stage);
  if (!UUID.test(runId)) throw new StateBucketError("state_manifest_invalid");
  const names = rehearsalResourceNames(ctx.stage);
  const r = ["--region", ctx.region];
  const fn = cliJson(aws, ["lambda", "get-function-configuration", "--function-name", names.function, ...r]);
  const fnArn = `arn:aws:lambda:${ctx.region}:${ctx.accountId}:function:${names.function}`;
  if (fn.FunctionArn !== fnArn || fn.Role !== ctx.roleArn) throw new StateBucketError("state_manifest_invalid");
  const queues = names.queues.map((name) => {
    const url = cliJson(aws, ["sqs", "get-queue-url", "--queue-name", name, ...r]).QueueUrl;
    if (url !== `https://sqs.${ctx.region}.amazonaws.com/${ctx.accountId}/${name}`) throw new StateBucketError("state_manifest_invalid");
    return { name, url, arn: `arn:aws:sqs:${ctx.region}:${ctx.accountId}:${name}` };
  });
  const listed = cliJson(aws, ["lambda", "list-event-source-mappings", "--function-name", names.function, ...r]).EventSourceMappings;
  if (!Array.isArray(listed)) throw new StateBucketError("state_manifest_invalid");
  const mappings = listed.map((m: Record<string, unknown>) => {
    if (typeof m.UUID !== "string" || !UUID.test(m.UUID) || m.FunctionArn !== fnArn || typeof m.EventSourceArn !== "string") throw new StateBucketError("state_manifest_invalid");
    return { uuid: m.UUID, eventSourceArn: m.EventSourceArn };
  });
  const alarms = readAlarms(aws, names.alarms, ctx.region, "state_manifest_invalid");
  if (alarms.composite.length > 0) throw new StateBucketError("state_manifest_invalid");
  if (!readLogGroups(aws, names.logGroup, ctx.region, "state_manifest_invalid").includes(names.logGroup)) throw new StateBucketError("state_manifest_invalid");
  const body: Omit<PhysicalManifest, "digest"> = {
    schema: 2,
    stage: ctx.stage,
    region: ctx.region,
    accountId: ctx.accountId,
    binding: { runId, sessionId: ctx.sessionId, issuedAt: ctx.issuedAt, pin: ctx.pin },
    function: { name: names.function, arn: fnArn },
    role: { arn: ctx.roleArn, disposition: ctx.roleDisposition, boundaryArn: ctx.boundaryArn },
    queues,
    mappings: mappings.sort((a, b) => (a.eventSourceArn < b.eventSourceArn ? -1 : 1)),
    alarms: [...alarms.metric].sort(),
    logGroup: names.logGroup,
  };
  const manifest: PhysicalManifest = { ...body, digest: manifestDigest(body) };
  assertManifestComplete(manifest, ctx, runId);
  return manifest;
}

/**
 * A manifest is usable only when it is complete and this run's: every expected id (the function, the
 * four queues with URLs and ARNs, exactly two mapping UUIDs one per queue, all five alarms, the log
 * group); the role exactly the pinned ARN, disposition and boundary; the binding equal to this run
 * (run id), this lease (session, issuance time) and this pin; and the digest intact. Anything else refuses.
 */
export function assertManifestComplete(m: unknown, ctx: RehearsalContext, runId: string): asserts m is PhysicalManifest {
  const names = rehearsalResourceNames(ctx.stage);
  const v = m as PhysicalManifest | null | undefined;
  const ok =
    v !== null && typeof v === "object" && v.schema === 2 && v.stage === ctx.stage && v.region === ctx.region && v.accountId === ctx.accountId &&
    v.binding?.runId === runId && UUID.test(runId) && runId === ctx.runId && v.binding.sessionId === ctx.sessionId && v.binding.issuedAt === ctx.issuedAt && v.binding.pin === ctx.pin &&
    v.function?.name === names.function && v.function.arn === `arn:aws:lambda:${ctx.region}:${ctx.accountId}:function:${names.function}` &&
    v.role?.arn === ctx.roleArn && v.role.disposition === ctx.roleDisposition && v.role.boundaryArn === ctx.boundaryArn &&
    Array.isArray(v.queues) && v.queues.length === names.queues.length &&
    names.queues.every((n) => v.queues.some((q) => q?.name === n && q.url === `https://sqs.${ctx.region}.amazonaws.com/${ctx.accountId}/${n}` && q.arn === `arn:aws:sqs:${ctx.region}:${ctx.accountId}:${n}`)) &&
    Array.isArray(v.mappings) && v.mappings.length === 2 &&
    v.mappings.every((x) => typeof x?.uuid === "string" && UUID.test(x.uuid)) && new Set(v.mappings.map((x) => x.uuid)).size === 2 &&
    [`arn:aws:sqs:${ctx.region}:${ctx.accountId}:${names.queues[0]}`, `arn:aws:sqs:${ctx.region}:${ctx.accountId}:${names.queues[2]}`].every((arn) => v.mappings.some((x) => x.eventSourceArn === arn)) &&
    Array.isArray(v.alarms) && v.alarms.length === names.alarms.length && names.alarms.every((a) => v.alarms.includes(a)) &&
    v.logGroup === names.logGroup &&
    typeof v.digest === "string" && v.digest === manifestDigest(v);
  if (!ok) throw new StateBucketError("state_manifest_invalid");
}

/**
 * Before the state is discarded: each id in the (complete, this run's) manifest reads back as gone:
 * the function by ARN, each queue by URL, each mapping by UUID, the alarms list empty, no log group of
 * the exact name; and the role per its pinned disposition: deleted, GetRole on that exact name (in
 * the pinned account) is not found; retained, GetRole shows exactly the pinned ARN, its path, and the
 * pinned permissions boundary. Anything still present (or a retained role not exactly as pinned)
 * refuses (`state_resources_remain`); any other failure refuses (`state_teardown_failed`). Read-only.
 */
export function assertManifestGone(aws: AwsRun, m: unknown, ctx: RehearsalContext, runId: string): void {
  assertManifestComplete(m, ctx, runId);
  const r = ["--region", ctx.region, "--output", "json"];
  absent(aws, ["lambda", "get-function-configuration", "--function-name", m.function.arn, ...r]);
  for (const q of m.queues) absent(aws, ["sqs", "get-queue-attributes", "--queue-url", q.url, "--attribute-names", "QueueArn", ...r]);
  for (const x of m.mappings) absent(aws, ["lambda", "get-event-source-mapping", "--uuid", x.uuid, ...r]);
  const alarms = readAlarms(aws, m.alarms, ctx.region, "state_teardown_failed");
  if (alarms.metric.length > 0 || alarms.composite.length > 0) throw new StateBucketError("state_resources_remain");
  if (readLogGroups(aws, m.logGroup, ctx.region, "state_teardown_failed").includes(m.logGroup)) throw new StateBucketError("state_resources_remain");
  const { name, path } = roleParts(m.role.arn, ctx.accountId);
  if (m.role.disposition === "deleted") absent(aws, ["iam", "get-role", "--role-name", name, ...r]);
  else {
    let role: { Arn?: unknown; Path?: unknown; PermissionsBoundary?: { PermissionsBoundaryArn?: unknown } } | undefined;
    try {
      role = (JSON.parse(aws(["iam", "get-role", "--role-name", name, ...r])) as { Role?: typeof role }).Role;
    } catch {
      throw new StateBucketError("state_teardown_failed");
    }
    if (role?.Arn !== m.role.arn || role.Path !== path || role.PermissionsBoundary?.PermissionsBoundaryArn !== m.role.boundaryArn) throw new StateBucketError("state_resources_remain");
  }
}

/**
 * The independent receipt at execute: the digest the broker recorded when the manifest was written
 * (its receipt), not one recomputed from the local file, must equal the manifest's digest.
 */
export function assertManifestReceipt(m: unknown, expectedDigest: string | undefined): void {
  if (expectedDigest === undefined || !/^[0-9a-f]{64}$/.test(expectedDigest)) throw new StateBucketError("state_receipt_mismatch");
  const v = m as { digest?: unknown } | null;
  if (v === null || typeof v !== "object" || v.digest !== expectedDigest || manifestDigest(v as never) !== expectedDigest) throw new StateBucketError("state_receipt_mismatch");
}

/** A role ARN's name and path, in exactly the given account. */
function roleParts(arn: string, accountId: string): { name: string; path: string } {
  const m = new RegExp(`^arn:aws:iam::${accountId}:role(/(?:[A-Za-z0-9_+=,.@-]+/)*)([A-Za-z0-9_+=,.@-]{1,64})$`).exec(arn);
  if (!m) throw new StateBucketError("state_manifest_invalid");
  return { path: m[1]!, name: m[2]! };
}

/**
 * Before stage A: none of the stage's names is in use yet, so no existing resource can be adopted
 * (tagged into the rehearsal) by the lease's create-time tagging grants.
 */
export function assertRehearsalNamesUnused(aws: AwsRun, i: { region: string; stage: string }): void {
  rehearsalStatePrefix(i.stage);
  const names = rehearsalResourceNames(i.stage);
  const r = ["--region", i.region, "--output", "json"];
  absent(aws, ["lambda", "get-function-configuration", "--function-name", names.function, ...r]);
  for (const q of names.queues) absent(aws, ["sqs", "get-queue-url", "--queue-name", q, ...r]);
  // Metric and composite alarms of these names, and the exact log group.
  const alarms = readAlarms(aws, names.alarms, i.region, "state_teardown_failed");
  if (alarms.metric.length > 0 || alarms.composite.length > 0) throw new StateBucketError("state_resources_remain");
  if (readLogGroups(aws, names.logGroup, i.region, "state_teardown_failed").includes(names.logGroup)) throw new StateBucketError("state_resources_remain");
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
export function rehearsalLeasePolicies(i: { accountId: string; roleArn: string; stage?: string; region?: string; issuedAt?: string; expiresAt?: string; mappingUuids?: readonly string[] }) {
  const stage = i.stage ?? REHEARSAL_STAGE;
  const region = i.region ?? REHEARSAL_REGION;
  if (region !== REHEARSAL_REGION || !/^\d{12}$/.test(i.accountId)) throw new StateBucketError("state_inputs_invalid");
  roleParts(i.roleArn, i.accountId);
  const end = rehearsalLeaseEnd({ ...(i.issuedAt ? { issuedAt: i.issuedAt } : {}), ...(i.expiresAt ? { requested: i.expiresAt } : {}) });
  const prefix = rehearsalStatePrefix(stage);
  const bucket = pippenStateBucket(i.accountId, region);
  const bucketArn = `arn:aws:s3:::${bucket}`;
  const a = i.accountId;
  // Exact resources, no name wildcard: the function, the four queues, the five alarms, the log group.
  const names = rehearsalResourceNames(stage);
  const fnArn = `arn:aws:lambda:${region}:${a}:function:${names.function}`;
  const esmArn = `arn:aws:lambda:${region}:${a}:event-source-mapping:*`;
  const queueArn = names.queues.map((q) => `arn:aws:sqs:${region}:${a}:${q}`);
  const alarmArn = names.alarms.map((x) => `arn:aws:cloudwatch:${region}:${a}:alarm:${x}`);
  const logArn = `arn:aws:logs:${region}:${a}:log-group:${names.logGroup}`;
  // Mapping operations only on mappings whose target is exactly the function.
  const onFunction = { ArnEquals: { "lambda:FunctionArn": fnArn } };
  const before = { DateLessThan: { "aws:CurrentTime": end } };
  const tagged = { StringEquals: { [`aws:ResourceTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value } };
  const tagging = { StringEquals: { [`aws:RequestTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value } };
  // Standalone tag calls only on a resource that already carries rehearsal=p1-1126 (never
  // IfExists: an untagged resource cannot be tagged into the rehearsal), and with that tag in the
  // request. Tags at create come with the create call (its aws:RequestTag); if a service also
  // authorizes the tag action at create against a resource tag that does not exist yet, the create
  // fails closed: a residual for the issuer (docs).
  const retag = { StringEquals: { [`aws:RequestTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value, [`aws:ResourceTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value } };
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
          statement("Queues", ["sqs:CreateQueue"], queueArn, tagging),
          statement("Function", ["lambda:CreateFunction"], fnArn, tagging),
          statement("Mappings", ["lambda:CreateEventSourceMapping"], esmArn, { ...tagging, ...onFunction }),
          statement("Alarms", ["cloudwatch:PutMetricAlarm"], alarmArn, tagging),
          statement("Logs", ["logs:CreateLogGroup"], [logArn, `${logArn}:*`], tagging),
          statement("Tags", ["sqs:TagQueue", "lambda:TagResource", "cloudwatch:TagResource", "logs:TagResource"], [...queueArn, fnArn, ...alarmArn, logArn, `${logArn}:*`], retag),
        ],
      },
    },
    {
      name: `96bulls-rehearse-p1-1126-act`,
      document: {
        Version: "2012-10-17" as const,
        Statement: [
          statement("Queues", ["sqs:GetQueueAttributes", "sqs:SetQueueAttributes", "sqs:ListQueueTags", "sqs:DeleteQueue", "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"], queueArn, tagged),
          statement("Function", [
            "lambda:GetFunction", "lambda:GetFunctionConfiguration", "lambda:UpdateFunctionCode", "lambda:UpdateFunctionConfiguration", "lambda:DeleteFunction", "lambda:PublishVersion",
            "lambda:ListAliases", "lambda:ListVersionsByFunction", "lambda:AddPermission", "lambda:RemovePermission", "lambda:GetPolicy", "lambda:ListTags", "lambda:InvokeFunction",
          ], fnArn, tagged),
          // The mappings by their own tag and the function they target (the UUIDs are not known before
          // creation; teardown checks each recorded UUID: assertManifestGone).
          statement("Mappings", ["lambda:GetEventSourceMapping", "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping"], esmArn, { ...tagged, ...onFunction }),
          statement("Alarms", ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:ListTagsForResource"], alarmArn, tagged),
          statement("Logs", ["logs:PutRetentionPolicy", "logs:DeleteLogGroup", "logs:ListTagsForResource", "logs:StartQuery", "logs:FilterLogEvents"], [logArn, `${logArn}:*`], tagged),
          // Reads narrowed to the stage's names (no tag condition: they must also answer "not found").
          statement("NamedReads", ["sqs:GetQueueUrl", "cloudwatch:DescribeAlarms"], [...queueArn, ...alarmArn], {}),
          // Only the pinned execution role (the role-disposition check).
          statement("RoleRead", ["iam:GetRole"], i.roleArn, {}),
          // Absence readbacks by exact name, without the tag condition, so a deleted resource reads as
          // NotFound rather than AccessDenied (teardown and the stage-A collision check).
          statement("GoneReads", ["lambda:GetFunctionConfiguration", "sqs:GetQueueAttributes"], [fnArn, ...queueArn], {}),
          // The mappings' absence readback needs their UUIDs, known only after creation: the teardown
          // lease is issued with the manifest's UUIDs (mappingUuids); without them it is omitted.
          ...(i.mappingUuids && i.mappingUuids.length
            ? [statement("GoneMappings", ["lambda:GetEventSourceMapping"], i.mappingUuids.map((u) => {
                if (!UUID.test(u)) throw new StateBucketError("state_inputs_invalid");
                return `arn:aws:lambda:${region}:${a}:event-source-mapping:${u}`;
              }), {})]
            : []),
          // Reads IAM cannot narrow to a resource (documented): listing mappings, metric data, query results.
          statement("Reads", ["lambda:ListEventSourceMappings", "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData", "logs:DescribeLogGroups", "logs:GetQueryResults"], "*", {}),
        ],
      },
    },
  ];
  return { expiresAt: end, inline, managed };
}


/**
 * The teardown authorization: the trusted handoff from the lease the run was captured and deployed
 * under (A) to the lease issued for teardown (B, with the mappings' absence reads). The broker writes
 * it at B's issuance and records its digest in its receipt; the code verifies it against that digest,
 * the manifest, the current STS caller (B) and the generated teardown policy.
 */
export interface TeardownAuthorization {
  schema: 1;
  stage: string;
  runId: string;
  /** Lease A: the run's original lease, as the manifest and the marker record it. */
  original: { sessionId: string; issuedAt: string; pin: string };
  manifestDigest: string;
  mappingUuids: string[];
  /** Lease B: the authorized teardown principal. */
  teardown: { sessionId: string; callerArn: string; issuedAt: string; expiresAt: string; policyDigest: string };
}

/** sha256 of the canonical JSON of a teardown authorization (the value the broker's receipt records). */
export function authorizationDigest(a: unknown): string {
  return createHash("sha256").update(canonical(a)).digest("hex");
}

/** sha256 of the canonical teardown lease policies for lease B (issuance time) with the manifest's mapping UUIDs. */
export function teardownPolicyDigest(ctx: RehearsalContext, mappingUuids: readonly string[]): string {
  return createHash("sha256").update(canonical(rehearsalLeasePolicies({ accountId: ctx.accountId, roleArn: ctx.roleArn, issuedAt: ctx.issuedAt, mappingUuids }))).digest("hex");
}

/**
 * Verify a teardown authorization for the current lease (ctx, lease B, already proven by STS) and
 * return the original run's context (lease A) the manifest and the marker are checked against.
 * Refuses (`state_authorization_mismatch`) unless: its digest is the broker's recorded one; it is
 * this stage and run; B is exactly the current session, caller ARN and issuance, not expired and
 * expiring no later than B's lease end; B's policy digest is the generated teardown policy with the
 * authorization's mapping UUIDs; and A's pin is the issuer pin of the same approved account,
 * boundary, role and disposition with A's session and issuance time.
 */
export function assertTeardownAuthorization(ctx: RehearsalContext, auth: unknown, expectedDigest: string | undefined, now: number = Date.now()): RehearsalContext {
  const fail = (): never => {
    throw new StateBucketError("state_authorization_mismatch");
  };
  if (expectedDigest === undefined || !/^[0-9a-f]{64}$/.test(expectedDigest) || authorizationDigest(auth) !== expectedDigest) fail();
  const a = auth as TeardownAuthorization;
  const hex = (v: unknown) => typeof v === "string" && /^[0-9a-f]{64}$/.test(v);
  const ok =
    a !== null && typeof a === "object" && a.schema === 1 && a.stage === ctx.stage && a.runId === ctx.runId &&
    typeof a.original?.sessionId === "string" && PIPPEN_SESSION_ID.test(a.original.sessionId) && typeof a.original.issuedAt === "string" && ISO.test(a.original.issuedAt) && hex(a.original.pin) &&
    hex(a.manifestDigest) && Array.isArray(a.mappingUuids) && a.mappingUuids.length === 2 && a.mappingUuids.every((u) => typeof u === "string" && UUID.test(u)) && new Set(a.mappingUuids).size === 2 &&
    a.teardown?.sessionId === ctx.sessionId && a.teardown.issuedAt === ctx.issuedAt &&
    a.teardown.callerArn === `arn:aws:sts::${ctx.accountId}:assumed-role/${PIPPEN_PROOF_SANDBOX.callerRole.roleName}/${PIPPEN_PROOF_SANDBOX.callerRole.sessionPrefix}${ctx.sessionId}` &&
    typeof a.teardown.expiresAt === "string" && ISO.test(a.teardown.expiresAt) && now < Date.parse(a.teardown.expiresAt) && Date.parse(a.teardown.expiresAt) <= Date.parse(rehearsalLeaseEnd({ issuedAt: ctx.issuedAt })) &&
    a.teardown.policyDigest === teardownPolicyDigest(ctx, a.mappingUuids);
  if (!ok) fail();
  const original: IssuerPinned = { stage: ctx.stage, region: ctx.region, accountId: ctx.accountId, boundaryArn: ctx.boundaryArn, roleArn: ctx.roleArn, roleDisposition: ctx.roleDisposition, sessionId: a.original.sessionId, issuedAt: a.original.issuedAt };
  if (rehearsalIssuerPin(original) !== a.original.pin) fail();
  return { ...original, pin: a.original.pin, runId: a.runId };
}

/**
 * The whole teardown, in order, with no deletion before every check passed: the run context of the
 * current lease (before any AWS call); STS; the bucket; with a teardown authorization (lease B), the
 * authorization (above), else the current lease must be the run's own; the independent manifest
 * receipt (the broker's recorded digest) and, with an authorization, its manifest digest and mapping
 * UUIDs; the manifest against the original run; every recorded id gone; the current marker owned by
 * the original run; and only then the state versions deleted (execute) or counted (dry run).
 */
export function runRehearsalTeardown(
  aws: AwsRun,
  i: {
    inputs: unknown;
    env: Record<string, string | undefined>;
    now?: number;
    execute: boolean;
    manifest?: unknown;
    expectManifestDigest?: string;
    authorization?: unknown;
    expectAuthorizationDigest?: string;
  },
): { listed: number; deleted: number; remaining: number } {
  const ctx = rehearsalRunContext(REHEARSAL_STAGE, i.inputs, i.env, i.now)!;
  assertLeaseCaller(aws, ctx.accountId, i.env);
  const bucket = validatePippenStateBucket(aws, ctx);
  const prefix = rehearsalStatePrefix(ctx.stage);
  if (!i.execute) return teardownStageState(aws, { bucket, region: ctx.region, stage: REHEARSAL_STAGE, execute: false });
  const original = i.authorization === undefined ? ctx : assertTeardownAuthorization(ctx, i.authorization, i.expectAuthorizationDigest, i.now);
  assertManifestReceipt(i.manifest, i.expectManifestDigest);
  if (i.authorization !== undefined) {
    const a = i.authorization as TeardownAuthorization;
    const m = i.manifest as PhysicalManifest;
    if (a.manifestDigest !== m.digest || [...a.mappingUuids].sort().join() !== m.mappings.map((x) => x.uuid).sort().join()) throw new StateBucketError("state_authorization_mismatch");
  }
  assertManifestComplete(i.manifest, original, original.runId);
  assertManifestGone(aws, i.manifest, original, original.runId);
  // The current marker must be the original run's before any version is deleted.
  assertRunMarker(aws, { bucket, prefix, ctx: original });
  return teardownStageState(aws, { bucket, region: ctx.region, stage: REHEARSAL_STAGE, execute: true });
}
