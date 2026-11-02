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
 *   IAM users            writer (the RelayHistory Worker), reader (the indexer), eraser (the purge job),
 *                        srv-writer (the relayhistory-api Worker's live writer: `m=srv_*` sessions,
 *                        live-sessions/, live-batches/, SSE-KMS headers required, no delete, no list).
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
 * object ARN, i.e. per-workspace attribution, not tenant cryptographic isolation.
 * All objects are still protected by one shared CMK. Per-workspace CMKs are not used:
 * see "Why one CMK" in the runbook.
 *
 * Pure functions, unit-tested in stacks/aws/__tests__/transcript-store.test.ts;
 * `transcriptStoreStage` declares the resources.
 */
import * as AWS from "@alchemy.run/aws";
import * as Effect from "effect/Effect";
import { LAYOUT_VERSION } from "../../packages/object-store/src/layout.ts";
import { sha256, type Provenance } from "../../src/cli/plan-provenance.ts";
import { KMS_ADMIN_ACTIONS, denyInsecureTransport, trailKeyPolicy } from "./hardening.ts";
import { alchemyStatePrefix } from "./preflight.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";
import {
  SESSION_PIPELINE_RESOURCES,
  assertSessionPipelineInputs,
  pipelineBucketStatements,
  pipelineKeyStatement,
  sessionPipelineFunction,
  sessionPipelineWiring,
  type SessionPipelineInputs,
} from "./session-pipeline.ts";

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
export const TRANSCRIPT_STAGE = "aws-sessions-prod";
export const TRANSCRIPT_ACCOUNT_ID = "941433935742";
export const TRANSCRIPT_PARTITION = "aws";
export const ACCESS_LOG_PREFIX = "s3-access/transcripts/";

export interface TranscriptStoreInputs {
  /**
   * Log management events too (default true). Set false only when another trail
   * in the account already logs them (a second copy is billed).
   */
  managementEvents?: boolean;
  /** Optional role allowed to decrypt/re-encrypt CloudTrail log files. Omit for no trail-log read path. */
  auditPrincipalArn?: string;
  /**
   * The session pipeline (session-pipeline.ts): S3 manifest events -> SQS -> Lambda -> Supabase
   * pointer + ClickHouse index. Omit for the store alone.
   */
  sessionPipeline?: SessionPipelineInputs;
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
  srvWriter: string;
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
    srvWriter: n("transcript-srv-writer"),
  };
}

export const userArn = (accountId: string, path: string, name: string) => `arn:aws:iam::${accountId}:user${path}${name}`;

/** Every session object, in every org: `<bucket>/96bulls/*\/v1/*<tail>`. */
const sessionObjects = (bucketArn: string, tail: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*${tail}`;
const allObjects = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*`;
/** The relayhistory-api Worker's live writer: server-side machines (`m=srv_<hash>`) and the live prefixes, in every org. */
export const srvWriterObjects = (bucketArn: string) => [
  `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/ws=*/m=srv_*`,
  `${bucketArn}/${TENANT_KEY_BASE}/*/live-sessions/*`,
  `${bucketArn}/${TENANT_KEY_BASE}/*/live-batches/*`,
];

/**
 * Inputs a transcript-store stage must not carry: it is S3 only (decision 0001),
 * production-grade (no purge) and in us-east-1 (regional boundary).
 */
export function assertTranscriptStoreInputs(
  stage: string,
  inputs: Record<string, unknown> & { accountId: string; region: string; partition?: string; transcriptStore?: TranscriptStoreInputs },
) {
  const hasTranscriptStore = Object.prototype.hasOwnProperty.call(inputs, "transcriptStore");
  const t = inputs.transcriptStore;
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${stage} is a transcript-store stage: ${why}. Nothing was created.`);
  };
  if (stage !== TRANSCRIPT_STAGE) {
    if (hasTranscriptStore) refuse(`transcriptStore is reserved for ${TRANSCRIPT_STAGE}`);
    return;
  }
  if (!hasTranscriptStore) refuse("transcriptStore is required; production may not fall through to the legacy AWS baseline");
  if (t === null || typeof t !== "object" || Array.isArray(t) || Object.getPrototypeOf(t) !== Object.prototype)
    refuse("transcriptStore must be a plain object");
  const store = t as TranscriptStoreInputs;
  if (inputs.accountId !== TRANSCRIPT_ACCOUNT_ID) refuse(`accountId must be exactly ${TRANSCRIPT_ACCOUNT_ID}, got ${JSON.stringify(inputs.accountId)}`);
  if (inputs.region !== TRANSCRIPT_REGION) refuse(`transcript content must stay in ${TRANSCRIPT_REGION} (migration plan, regional boundaries), got ${inputs.region}`);
  if (inputs.partition !== TRANSCRIPT_PARTITION) refuse(`partition must be exactly ${TRANSCRIPT_PARTITION}, got ${JSON.stringify(inputs.partition)}`);
  for (const k of Object.keys(store))
    if (k !== "managementEvents" && k !== "auditPrincipalArn" && k !== "sessionPipeline") refuse(`transcriptStore.${k} is not a known setting`);
  if (store.sessionPipeline !== undefined) assertSessionPipelineInputs(store.sessionPipeline, refuse);
  if (store.managementEvents !== undefined && typeof store.managementEvents !== "boolean") refuse("transcriptStore.managementEvents must be a boolean");
  if (store.auditPrincipalArn !== undefined && !new RegExp(`^arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/[A-Za-z0-9_+=,.@/-]+$`).test(store.auditPrincipalArn))
    refuse(`transcriptStore.auditPrincipalArn must be a named IAM role in account ${TRANSCRIPT_ACCOUNT_ID} in partition aws`);
  if (stage.startsWith("aws-sandbox-") || inputs.pippenSandbox) refuse("a sandbox account never holds transcripts");
  // S3 only: production AWS is S3 (decision 0001), and nothing here may empty a bucket.
  for (const k of ["eks", "supabase", "teardown", "accountShared", "lockerNoncurrentVersionDays", "hoopdbSecretArn"] as const)
    if (inputs[k] !== undefined) refuse(`${k} is not allowed (S3 only; never purged on destroy)`);
}

const TRANSCRIPT_RESOURCES = [
  "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
  "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
  "transcripts-key", "transcripts-key-alias",
] as const;

export type TranscriptStorePlanPhase =
  | "initial"
  | "principal-propagation recovery"
  | "post-merge policy remediation"
  | "srv-writer addition"
  | "srv-writer propagation recovery"
  | "post-apply"
  | "session pipeline rollout"
  | "session pipeline update";

/**
 * The session pipeline's own phases (session-pipeline.ts). Rollout: the pipeline's
 * resources are created (all, or the rest after a partial apply), and the store
 * changes only by the two grants it needs: the transcripts key policy (decrypt via
 * S3) and the bucket policy (list, read). Update: an existing pipeline changes in
 * place (code, env, settings) and the store does not change at all. Never a
 * replacement or deletion, and every other store resource stays `noop`.
 */
function sessionPipelinePhase(operations: ReadonlyMap<string, string>): TranscriptStorePlanPhase | undefined {
  if (!SESSION_PIPELINE_RESOURCES.some((r) => operations.has(r))) return undefined;
  const pipeline = SESSION_PIPELINE_RESOURCES.map((r) => operations.get(r));
  const store = (r: string) => operations.get(r);
  const othersNoop = TRANSCRIPT_RESOURCES.filter((r) => r !== "transcripts" && r !== "transcripts-key").every((r) => store(r) === "noop");
  if (!othersNoop) return undefined;
  const grants = ["transcripts", "transcripts-key"].map(store);
  if (pipeline.every((a) => a === "create" || a === "noop") && grants.every((a) => a === "update" || a === "noop")) {
    if (pipeline.some((a) => a === "create")) return "session pipeline rollout";
    if (grants.some((a) => a === "update")) return "session pipeline rollout";
  }
  if (grants.every((a) => a === "noop") && pipeline.every((a) => a === "update" || a === "noop") && pipeline.some((a) => a === "update"))
    return "session pipeline update";
  return undefined;
}

const TRANSCRIPT_PLAN_PHASES: ReadonlyArray<{ name: TranscriptStorePlanPhase; actions: Readonly<Record<string, "create" | "update">> }> = [
  { name: "initial", actions: Object.fromEntries(TRANSCRIPT_RESOURCES.map((resource) => [resource, "create"])) },
  { name: "principal-propagation recovery", actions: { transcripts: "create", "transcripts-key": "create", "transcripts-key-alias": "create" } },
  // One capture-bound exception for the policies hardened after the first production apply.
  // The other resources must be noop, and a subsequent capture must be all-noop.
  { name: "post-merge policy remediation", actions: { "trail-key": "update", "trail-logs": "update" } },
  // Adding the relayhistory-api live writer to the applied store: the new user, and the key policy that names it.
  { name: "srv-writer addition", actions: { "transcript-srv-writer": "create", "transcripts-key": "update" } },
  // The srv-writer addition's own recovery: the user was created (now noop, so it exists in state) but
  // KMS refused the key policy because the new principal had not propagated. Only the key policy update
  // that adds the srv-writer's statement is accepted (assertSrvWriterRecoveryDiff).
  { name: "srv-writer propagation recovery", actions: { "transcripts-key": "update" } },
  { name: "post-apply", actions: {} },
];

export interface TranscriptStorePlanCapture {
  detailedPlanText: string;
  provenanceText: string;
  /**
   * The read-only state snapshot the capture planned against (capture only). The deploy has no
   * snapshot; it is bound to the same state by the capture's version digest (capture-verify.ts).
   */
  stateSnapshotText?: string;
}

/** The resources a plan must list: the store's resources, plus every pipeline resource once any of them appears. */
function expectedResources(names: Iterable<string>): readonly string[] {
  const pipeline = new Set<string>(SESSION_PIPELINE_RESOURCES);
  for (const name of names) if (pipeline.has(name)) return [...TRANSCRIPT_RESOURCES, ...SESSION_PIPELINE_RESOURCES];
  return TRANSCRIPT_RESOURCES;
}

function transcriptPlanOperations(text: string, label: "compact" | "detailed", refuse: (why: string) => never) {
  const summaries = text.split(/\r?\n/).filter((line) => line.startsWith("Plan:"));
  if (summaries.length !== 1) refuse(`${label} plan: expected exactly one Plan: summary, found ${summaries.length}`);
  const operations = new Map<string, string>();
  for (const line of text.split(/\r?\n/)) {
    const m = /^\s*\[([^\]]+)\]\s+(\S+)\s*$/.exec(line);
    if (!m) continue;
    const [, resource, action] = m;
    if (operations.has(resource!)) refuse(`${label} plan: resource ${resource} appears more than once`);
    operations.set(resource!, action!);
  }
  const expected = new Set<string>(expectedResources(operations.keys()));
  for (const resource of operations.keys()) if (!expected.has(resource)) refuse(`${label} plan: unexpected resource ${resource}`);
  for (const resource of expected) if (!operations.has(resource)) refuse(`${label} plan: missing resource ${resource}`);
  for (const [resource, action] of operations)
    if (action !== "create" && action !== "update" && action !== "noop") refuse(`${label} plan: ${resource} has forbidden action ${action}`);

  const actualCounts = new Map<string, number>();
  for (const action of operations.values()) if (action !== "noop") actualCounts.set(action, (actualCounts.get(action) ?? 0) + 1);
  const summary = summaries[0]!;
  const clauses = [...summary.matchAll(/(\d+) to ([a-z-]+)/g)].map((m) => ({ count: Number(m[1]), action: m[2]! }));
  if (clauses.length === 0) {
    if (!/^Plan:\s*(?:0 changes|no changes)\s*$/i.test(summary)) refuse(`${label} plan: unrecognized summary ${JSON.stringify(summary)}`);
    if (actualCounts.size !== 0) refuse(`${label} plan: summary says no changes but resource lines contain changes`);
  } else {
    if (!/^Plan:\s*\d+ to [a-z-]+(?:,\s*\d+ to [a-z-]+)*\s*$/.test(summary))
      refuse(`${label} plan: unrecognized summary ${JSON.stringify(summary)}`);
    const seen = new Set<string>();
    for (const clause of clauses) {
      if (seen.has(clause.action)) refuse(`${label} plan: summary repeats ${clause.action}`);
      seen.add(clause.action);
      const actual = actualCounts.get(clause.action) ?? 0;
      if (clause.count !== actual) refuse(`${label} plan: summary says ${clause.count} ${clause.action} but resource lines contain ${actual}`);
    }
    for (const [action, count] of actualCounts) if (!seen.has(action)) refuse(`${label} plan: summary omits ${count} ${action}`);
  }
  return { summary, operations };
}

/**
 * Exact production contract for initial creation, partial recovery, the one
 * capture-bound post-merge policy repair, and final all-noop evidence. Both
 * compact and detailed plan bytes must be the exact files hashed by the bound
 * provenance record.
 */
export function assertTranscriptStorePlanContract(planText: string, capture: TranscriptStorePlanCapture): TranscriptStorePlanPhase {
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${TRANSCRIPT_STAGE} plan contract refused: ${why}. Nothing was changed.`);
  };
  const compact = transcriptPlanOperations(planText, "compact", refuse);
  const detailed = transcriptPlanOperations(capture.detailedPlanText, "detailed", refuse);
  if (compact.summary !== detailed.summary) refuse("compact and detailed plan summaries differ");
  for (const resource of expectedResources(compact.operations.keys()))
    if (compact.operations.get(resource) !== detailed.operations.get(resource))
      refuse(`compact and detailed plans disagree on ${resource}`);

  let provenance: Provenance;
  try {
    provenance = JSON.parse(capture.provenanceText) as Provenance;
  } catch {
    return refuse("provenance is not valid JSON");
  }
  const planPath = `docs/plans/${TRANSCRIPT_STAGE}.plan.txt`;
  const detailedPath = `docs/plans/${TRANSCRIPT_STAGE}.plan.detailed.txt`;
  if (provenance?.schema !== 1 || provenance.stage !== TRANSCRIPT_STAGE) refuse(`provenance must be schema 1 for ${TRANSCRIPT_STAGE}`);
  if (provenance.outputs?.plan?.path !== planPath) refuse(`provenance compact plan path must be exactly ${planPath}`);
  if (provenance.outputs?.detailed?.path !== detailedPath) refuse(`provenance detailed plan path must be exactly ${detailedPath}`);
  if (provenance.outputs.plan.sha256 !== sha256(planText)) refuse("compact plan does not match its provenance sha256");
  if (provenance.outputs.detailed.sha256 !== sha256(capture.detailedPlanText)) refuse("detailed plan does not match its provenance sha256");
  if (provenance.summary !== compact.summary) refuse("provenance summary does not match the plan summary");

  const withPipeline = SESSION_PIPELINE_RESOURCES.some((r) => compact.operations.has(r));
  if (withPipeline) {
    if (SESSION_PIPELINE_RESOURCES.every((r) => compact.operations.get(r) === "noop") && TRANSCRIPT_RESOURCES.every((r) => compact.operations.get(r) === "noop"))
      return "post-apply";
    const pipelinePhase = sessionPipelinePhase(compact.operations);
    if (pipelinePhase) return pipelinePhase;
    const changes = [...compact.operations].filter(([, action]) => action !== "noop").map(([resource, action]) => `${resource}:${action}`);
    return refuse(`action set does not match the session pipeline rollout, update or post-apply phase: ${changes.join(", ") || "(none)"}`);
  }
  const phase = TRANSCRIPT_PLAN_PHASES.find((candidate) =>
    TRANSCRIPT_RESOURCES.every((resource) => compact.operations.get(resource) === (candidate.actions[resource] ?? "noop")),
  );
  if (!phase) {
    const changes = [...compact.operations].filter(([, action]) => action !== "noop").map(([resource, action]) => `${resource}:${action}`);
    return refuse(`action set does not match initial, recovery, exact two-policy remediation, srv-writer addition, srv-writer recovery, or post-apply phase: ${changes.join(", ") || "(none)"}`);
  }
  if (phase.name === "srv-writer propagation recovery") assertSrvWriterRecoveryDiff(capture.detailedPlanText, capture.stateSnapshotText, refuse);
  return phase.name;
}

const SRV_WRITER_KEY_SID = "SrvWriterEncryptsAndReadsViaS3";

/**
 * A lone transcripts-key update is the srv-writer recovery only in one of two forms:
 *   - the detailed plan shows the live policy without the srv-writer's statement and the planned
 *     policy with exactly one; or
 *   - Alchemy persisted the failed update (state `updating`), so the plan shows "no declared property
 *     changes" and the update re-applies the stored props. Then the capture's state snapshot must show
 *     those props are exactly the previous props plus the one srv-writer statement naming the
 *     srv-writer user. The deploy is bound to that same state by the capture's version digest.
 * Any other lone key-policy update stays refused.
 */
function assertSrvWriterRecoveryDiff(detailedPlanText: string, stateSnapshotText: string | undefined, refuse: (why: string) => never) {
  const lines = detailedPlanText.split(/\r?\n/);
  const start = lines.findIndex((line) => /^\s*\[transcripts-key\]\s+update\s*$/.test(line));
  if (start < 0) refuse("srv-writer recovery: detailed plan has no transcripts-key update");
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((line) => /^\s*\[[^\]]+\]\s+\S+\s*$/.test(line));
  const block = end < 0 ? rest : rest.slice(0, end);
  const body = block.map((line) => line.trim()).filter(Boolean);
  if (body.length === 1 && body[0] === "no declared property changes") {
    if (stateSnapshotText !== undefined) assertPendingSrvWriterKeyUpdate(stateSnapshotText, refuse);
    return;
  }
  const sid = new RegExp(`^\\s*([+-])\\s+Sid:\\s*${SRV_WRITER_KEY_SID}\\s*$`);
  const marks = block.map((line) => sid.exec(line)?.[1]).filter(Boolean);
  if (marks.includes("-")) refuse(`srv-writer recovery: the live key policy already has ${SRV_WRITER_KEY_SID}`);
  if (marks.filter((m) => m === "+").length !== 1) refuse(`srv-writer recovery: the key policy update must add exactly one ${SRV_WRITER_KEY_SID} statement`);
}

type PolicyStatement = { Sid?: string; Principal?: { AWS?: unknown } } & Record<string, unknown>;
type KeyState = { status?: string; props?: { policy?: { Statement?: PolicyStatement[] } & Record<string, unknown> } & Record<string, unknown>; old?: KeyState };

const stableJson = (value: unknown): string =>
  Array.isArray(value)
    ? `[${value.map(stableJson).join(",")}]`
    : value !== null && typeof value === "object"
      ? `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${stableJson((value as Record<string, unknown>)[k])}`).join(",")}}`
      : JSON.stringify(value);

/** The persisted transcripts-key update adds exactly the srv-writer statement and changes nothing else. */
function assertPendingSrvWriterKeyUpdate(stateSnapshotText: string, refuse: (why: string) => never) {
  let objects: Array<{ key: string; text: string }>;
  try {
    objects = (JSON.parse(stateSnapshotText) as { objects: Array<{ key: string; text: string }> }).objects;
  } catch {
    return refuse("srv-writer recovery: state snapshot is not valid JSON");
  }
  const read = <T>(name: string): T => {
    const hits = objects.filter((o) => o.key === `${alchemyStatePrefix(TRANSCRIPT_STAGE)}/${name}.json`);
    if (hits.length !== 1) refuse(`srv-writer recovery: state snapshot has ${hits.length} ${name} records`);
    return JSON.parse(hits[0]!.text) as T;
  };
  const key = read<KeyState>("transcripts-key");
  const user = read<{ status?: string; attr?: { userArn?: string } }>("transcript-srv-writer");
  if (key.status !== "updating" || key.old?.status !== "created") refuse("srv-writer recovery: transcripts-key is not a persisted pending update");
  if (user.status !== "created" || !user.attr?.userArn) refuse("srv-writer recovery: transcript-srv-writer is not created in state");
  const planned = key.props?.policy?.Statement ?? [];
  const previous = key.old?.props?.policy?.Statement ?? [];
  const added = planned.filter((s) => s.Sid === SRV_WRITER_KEY_SID);
  if (added.length !== 1 || previous.some((s) => s.Sid === SRV_WRITER_KEY_SID))
    refuse(`srv-writer recovery: the pending key policy must add exactly one ${SRV_WRITER_KEY_SID} statement`);
  if (stableJson(added[0]!.Principal) !== stableJson({ AWS: user.attr!.userArn }))
    refuse(`srv-writer recovery: ${SRV_WRITER_KEY_SID} must name the srv-writer user`);
  const strip = (k: KeyState | undefined, keep: PolicyStatement[]) => stableJson({ ...k?.props, policy: { ...k?.props?.policy, Statement: keep } });
  if (strip(key, planned.filter((s) => s.Sid !== SRV_WRITER_KEY_SID)) !== strip(key.old, previous))
    refuse("srv-writer recovery: the pending key update changes more than the srv-writer statement");
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
 * TLS (1.2+) only; only the eraser deletes objects or versions; a PUT that sends
 * any encryption header other than SSE-KMS naming this bucket's key is refused,
 * including `aws:kms` without a key id (S3 would use `aws/s3`). SSE-C is blocked
 * separately by the encryption configuration.
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
      // A request that sends `x-amz-server-side-encryption` overrides the bucket default. With
      // `aws:kms` and no key id S3 uses the AWS managed `aws/s3` key, which this key policy does
      // not govern, so any SSE header must also name this key. A PUT with no SSE header gets the
      // bucket default (this key): that is how the Worker writes (PIPELINE_RAW_S3_SSE unset).
      Sid: "SseHeaderMustNameThisKey",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:PutObject"],
      Resource: [`${i.bucketArn}/*`],
      Condition: { Null: { "s3:x-amz-server-side-encryption": "false" }, StringNotEqualsIfExists: { "s3:x-amz-server-side-encryption-aws-kms-key-id": i.keyArn } },
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

/** CloudTrail may inspect and write only for this exact trail and account. */
export function trailLogBucketPolicy(i: { bucketArn: string; trailArn: string; accountId: string }) {
  const source = { "aws:SourceArn": i.trailArn, "aws:SourceAccount": i.accountId };
  return [
    ...denyInsecureTransport(i.bucketArn),
    {
      Sid: "AWSCloudTrailAclCheck",
      Effect: "Allow" as const,
      Principal: { Service: "cloudtrail.amazonaws.com" },
      Action: ["s3:GetBucketAcl"],
      Resource: i.bucketArn,
      Condition: { StringEquals: source },
    },
    {
      Sid: "AWSCloudTrailWrite",
      Effect: "Allow" as const,
      Principal: { Service: "cloudtrail.amazonaws.com" },
      Action: ["s3:PutObject"],
      Resource: `${i.bucketArn}/AWSLogs/${i.accountId}/*`,
      Condition: { StringEquals: { ...source, "s3:x-amz-acl": "bucket-owner-full-control" } },
    },
  ];
}

/**
 * The transcripts CMK's policy. The account root administers the key and cannot
 * use it (no `kms:*`). Only the writer, the srv-writer and the reader may use it, only through S3 in
 * this region, only from this account, and only for objects under `96bulls/` in
 * this bucket (the S3 encryption context, per object since bucket keys are off).
 * The eraser and every operator role have no use of it: deleting needs no key,
 * and nobody reads transcripts with the console.
 */
export function transcriptKeyPolicy(i: { accountId: string; region: string; bucketName: string; writerArn: string; srvWriterArn: string; readerArn: string; pipelineRoleArn?: string }) {
  const via = {
    StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
    StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${TENANT_KEY_BASE}/*` },
  };
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "AccountAdministersKey", Effect: "Allow" as const, Principal: { AWS: `arn:aws:iam::${i.accountId}:root` }, Action: [...KMS_ADMIN_ACTIONS], Resource: "*" },
      { Sid: "WriterEncryptsAndReadsViaS3", Effect: "Allow" as const, Principal: { AWS: i.writerArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition: via },
      { Sid: "SrvWriterEncryptsAndReadsViaS3", Effect: "Allow" as const, Principal: { AWS: i.srvWriterArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition: via },
      { Sid: "ReaderDecryptsViaS3", Effect: "Allow" as const, Principal: { AWS: i.readerArn }, Action: ["kms:Decrypt"], Resource: "*", Condition: via },
      ...(i.pipelineRoleArn ? [pipelineKeyStatement({ accountId: i.accountId, region: i.region, bucketName: i.bucketName, roleArn: i.pipelineRoleArn })] : []),
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

/**
 * The relayhistory-api Worker's live writer: read and write only server-side
 * machine sessions (`96bulls/<org>/v1/ws=<ws>/m=srv_<hash>/...`) and the
 * `live-sessions/` and `live-batches/` prefixes. Every PUT must name SSE-KMS and
 * a key in this account and region; the bucket policy's SseHeaderMustNameThisKey
 * narrows that to exactly the transcripts CMK. (The key ARN cannot appear here:
 * the key policy names this user, so the user exists before the key.) Key use is
 * granted by the key policy; this identity grant is scoped to the key's alias and
 * to S3 in this region. ListBucket on the bucket (missing keys must 404, see
 * below); no delete, no old versions, no bucket changes.
 */
export function transcriptSrvWriterPolicy(i: { bucketArn: string; accountId: string; region: string; keyAlias: string }) {
  const objects = srvWriterObjects(i.bucketArn);
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "ReadLiveObjects", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: objects },
      {
        Sid: "WriteLiveObjectsSseKmsOnly",
        Effect: "Allow" as const,
        Action: ["s3:PutObject"],
        Resource: objects,
        Condition: {
          StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms" },
          StringLike: { "s3:x-amz-server-side-encryption-aws-kms-key-id": `arn:aws:kms:${i.region}:${i.accountId}:key/*` },
        },
      },
      {
        Sid: "UseTranscriptKeyViaS3",
        Effect: "Allow" as const,
        Action: ["kms:GenerateDataKey", "kms:Decrypt"],
        Resource: [`arn:aws:kms:${i.region}:${i.accountId}:key/*`],
        Condition: {
          StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com` },
          "ForAnyValue:StringEquals": { "kms:ResourceAliases": i.keyAlias },
        },
      },
      // A missing key must answer 404, not 403: the Worker's create-only
      // pointer, manifest and batch-marker reads depend on it. S3 decides that
      // with s3:ListBucket on the bucket, and HEAD/GET carry no s3:prefix, so
      // the grant cannot be prefix-scoped (hardening.ts listBucket): it is the
      // same unconditional grant the transcript writer holds. Key names only;
      // object reads stay limited to the srv_ and live prefixes above.
      listBucket(i.bucketArn),
      noDeleteNoHistory(i.bucketArn),
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
    const srvWriter = yield* user(
      "transcript-srv-writer",
      n.srvWriter,
      "relayhistory-api-worker-writes-live-transcripts",
      transcriptSrvWriterPolicy({ bucketArn: n.bucketArn, accountId: i.accountId, region: i.region, keyAlias: n.keyAlias }),
    );

    // CloudTrail into its own bucket and key, as in the baseline (hardening.ts trailKeyPolicy).
    const trailKey = yield* AWS.KMS.Key("trail-key", {
      description: `96bulls ${i.stage}: CloudTrail log files`,
      enableKeyRotation: true,
      deletionWindow: "30 days",
      policy: JSON.stringify(trailKeyPolicy(i.accountId, i.region, n.trail, i.transcriptStore.auditPrincipalArn ?? null)),
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
      policy: trailLogBucketPolicy({ bucketArn: trailArn, trailArn: `arn:aws:cloudtrail:${i.region}:${i.accountId}:trail/${n.trail}`, accountId: i.accountId }) as never,
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

    // The session pipeline's function (and so its Alchemy-owned execution role) comes before the key
    // and the bucket, whose policies grant that role read access (session-pipeline.ts).
    const pipeline = i.transcriptStore.sessionPipeline;
    const pipelineFn = pipeline
      ? yield* sessionPipelineFunction({ stage: i.stage, accountId: i.accountId, region: i.region, p: pipeline, tags: i.tags, bucket: n.bucket })
      : undefined;
    const pipelineRoleArn = pipelineFn ? ((pipelineFn as unknown as { roleArn: string }).roleArn) : undefined;

    const key = yield* AWS.KMS.Key("transcripts-key", {
      description: `96bulls ${i.stage}: transcript objects (SSE-KMS, per-object data keys)`,
      enableKeyRotation: true,
      deletionWindow: "30 days",
      policy: transcriptKeyPolicy({
        accountId: i.accountId,
        region: i.region,
        bucketName: n.bucket,
        writerArn: writer.userArn as unknown as string,
        srvWriterArn: srvWriter.userArn as unknown as string,
        readerArn: reader.userArn as unknown as string,
        ...(pipelineRoleArn ? { pipelineRoleArn } : {}),
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
      policy: [
        ...transcriptBucketPolicy({ bucketArn: n.bucketArn, keyArn: key.keyArn as unknown as string, eraserArn }),
        ...(pipelineRoleArn ? pipelineBucketStatements({ bucketArn: n.bucketArn, roleArn: pipelineRoleArn }) : []),
      ] as never,
      lifecycleRules: transcriptLifecycleRules() as never,
      logging: { targetBucket: accessLogs.bucketName as unknown as string, targetPrefix: ACCESS_LOG_PREFIX, targetObjectKeyFormat: { PartitionedPrefix: { PartitionDateSource: "EventTime" } } as never },
      tags: i.tags("transcripts"),
    });

    const sessionPipeline =
      pipeline && pipelineFn
        ? yield* sessionPipelineWiring({
            stage: i.stage,
            accountId: i.accountId,
            region: i.region,
            p: pipeline,
            tags: i.tags,
            bucketArn: n.bucketArn,
            fn: pipelineFn as unknown as { functionName: unknown; functionArn: unknown; roleArn: unknown },
          })
        : null;

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
      srvWriterUser: srvWriter.userName,
      sessionPipeline,
    };
  });
