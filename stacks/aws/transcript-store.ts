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
 * policy lets the named users and configured pipeline/GC roles use the key through S3, for objects under
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
import { assertBufferedGcRoleArn, bufferedGcBucketStatements, bufferedGcKeyStatements } from "./buffered-gc-policy.ts";
import {
  SESSION_BRIEF_RESOURCES,
  SESSION_ANALYSIS_CACHE_RESOURCES,
  SESSION_SEMANTIC_COPY_RESOURCES,
  SESSION_ANALYSIS_JOB_RESOURCES,
  SESSION_BUFFERED_SEARCH_FEED_RESOURCES,
  SESSION_PIPELINE_RESOURCES,
  assertSessionPipelineInputs,
  pipelineBucketStatements,
  pipelineKeyStatement,
  pipelineAnalysisKeyStatement,
  ANALYSIS_RESULT_OBJECT_GLOB,
  sessionPipelineFunction,
  sessionPipelineWiring,
  type SessionPipelineInputs,
} from "./session-pipeline.ts";
import {
  SESSION_READ_API_RESOURCES,
  assertSessionReadApiInputs,
  sessionReadApi,
  type SessionReadApiInputs,
} from "./session-read-api.ts";

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
export const INBOX_CANDIDATE_KEY_ROOT = "96bulls/upload-inbox/v1";

export interface TranscriptStoreInputs {
  /** Exact, separately managed GC role. Required for this production stage: omission must never revoke active GC access. */
  bufferedGcRoleArn?: string;
  /**
   * Log management events too (default true). Set false only when another trail
   * in the account already logs them (a second copy is billed).
   */
  managementEvents?: boolean;
  /** Optional role allowed to decrypt/re-encrypt CloudTrail log files. Omit for no trail-log read path. */
  auditPrincipalArn?: string;
  /** Default-off immutable inbox authority root. Omit to preserve the existing srv-writer policy exactly. */
  inboxAuthorityKeyRoot?: string;
  /**
   * The session pipeline (session-pipeline.ts): S3 manifest events -> SQS -> Lambda -> Supabase
   * pointer + ClickHouse index. Omit for the store alone.
   */
  sessionPipeline?: SessionPipelineInputs;
  /**
   * The session read API (session-read-api.ts): Lambda + Function URL serving
   * /v1 event reads from ClickHouse with Supabase erasure checks. Omit for none.
   */
  sessionReadApi?: SessionReadApiInputs;
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

function validInboxAuthorityKeyRoot(value: unknown): value is string {
  if (typeof value !== "string" || !/^96bulls\/[A-Za-z0-9][A-Za-z0-9_/-]{0,151}$/.test(value) || value.endsWith("/")) return false;
  const parts = value.split("/");
  if (parts.some((part) => !part || part === "." || part === "..")) return false;
  return !(
    value === INBOX_CANDIDATE_KEY_ROOT ||
    value.startsWith(`${INBOX_CANDIDATE_KEY_ROOT}/`) ||
    INBOX_CANDIDATE_KEY_ROOT.startsWith(`${value}/`)
  );
}

function assertInboxAuthorityKeyRoot(value: unknown, refuse: (why: string) => never): asserts value is string | undefined {
  if (value !== undefined && !validInboxAuthorityKeyRoot(value))
    refuse(`transcriptStore.inboxAuthorityKeyRoot must be an isolated 96bulls/ root distinct from ${INBOX_CANDIDATE_KEY_ROOT}`);
}

/** Every session object, in every org: `<bucket>/96bulls/*\/v1/*<tail>`. */
const sessionObjects = (bucketArn: string, tail: string) => `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*${tail}`;
const allObjects = (bucketArn: string) => `${bucketArn}/${TENANT_KEY_BASE}/*`;
/** The relayhistory-api Worker's live writer: server-side machines (`m=srv_<hash>`) and the live prefixes, in every org. */
export const srvWriterObjects = (bucketArn: string, inboxAuthorityKeyRoot?: string) => {
  assertInboxAuthorityKeyRoot(inboxAuthorityKeyRoot, (why) => { throw new Error(why); });
  return [
    `${bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/ws=*/m=srv_*`,
    `${bucketArn}/${TENANT_KEY_BASE}/*/live-sessions/*`,
    `${bucketArn}/${TENANT_KEY_BASE}/*/live-batches/*`,
    ...(inboxAuthorityKeyRoot ? [
      `${bucketArn}/${inboxAuthorityKeyRoot}/*`,
      `${bucketArn}/${INBOX_CANDIDATE_KEY_ROOT}/*`,
    ] : []),
  ];
};

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
    if (k !== "managementEvents" && k !== "auditPrincipalArn" && k !== "inboxAuthorityKeyRoot" && k !== "sessionPipeline" && k !== "sessionReadApi" && k !== "bufferedGcRoleArn") refuse(`transcriptStore.${k} is not a known setting`);
  assertBufferedGcRoleArn(store.bufferedGcRoleArn, inputs.accountId);
  assertInboxAuthorityKeyRoot(store.inboxAuthorityKeyRoot, refuse);
  if (store.sessionPipeline !== undefined) assertSessionPipelineInputs(store.sessionPipeline, refuse);
  if (store.sessionReadApi !== undefined) assertSessionReadApiInputs(store.sessionReadApi, refuse);
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
  | "srv-writer inbox policy update"
  | "post-apply"
  | "session pipeline rollout"
  | "session pipeline update"
  | "session read api rollout"
  | "session read api update"
  | "session brief rollout"
  | "session brief update"
  | "session analysis cache rollout"
  | "session analysis cache update"
  | "session semantic copy rollout"
  | "session semantic copy update"
  | "session analysis jobs rollout"
  | "session analysis jobs update"
  | "session buffered search feed rollout"
  | "session buffered search feed update"
  | "repo tag rename";

/**
 * The read API's own phases (session-read-api.ts). It touches nothing else: no
 * key, bucket, queue or pipeline resource may change in the same plan. Rollout:
 * its resources are created (all, or the rest after a partial apply). Update:
 * they change in place (code, env such as `origin`, settings). Never a
 * replacement or deletion.
 */
function sessionReadApiPhase(operations: ReadonlyMap<string, string>): TranscriptStorePlanPhase | undefined {
  const api = SESSION_READ_API_RESOURCES.map((r) => operations.get(r));
  const others = [...operations].filter(([r]) => !(SESSION_READ_API_RESOURCES as readonly string[]).includes(r));
  if (!others.every(([, a]) => a === "noop")) return undefined;
  if (api.every((a) => a === "create" || a === "noop") && api.some((a) => a === "create")) return "session read api rollout";
  if (api.every((a) => a === "update" || a === "noop") && api.some((a) => a === "update")) return "session read api update";
  return undefined;
}

/**
 * The read API rollout, while the session pipeline carries an unapplied code-only update: main
 * moved since the pipeline's last deploy, and the read API's failed first apply left its errors
 * alarm in state, so neither can be planned alone. The compact shape: `session-read-api` is
 * created (never updated: the function's first apply did not complete in state), the other read
 * API resources are created or noop, `session-pipeline` and all five ROLE_GRANT_RESOURCES update,
 * and every other resource is noop. The detailed plan must then show `session-pipeline` with no
 * declared property changes and each grant as exactly the role-ARN cascade
 * (assertPipelineRoleCascadeDiff, unchanged).
 */
function isReadApiRolloutWithPipelineCascade(operations: ReadonlyMap<string, string>): boolean {
  const readApi = SESSION_READ_API_RESOURCES as readonly string[];
  const grants = ROLE_GRANT_RESOURCES as readonly string[];
  if (operations.get("session-read-api") !== "create") return false;
  if (!readApi.every((r) => operations.get(r) === "create" || operations.get(r) === "noop")) return false;
  if (operations.get("session-pipeline") !== "update" || !grants.every((r) => operations.get(r) === "update")) return false;
  return [...operations].every(([r, a]) => readApi.includes(r) || grants.includes(r) || r === "session-pipeline" || a === "noop");
}

/**
 * The session pipeline's own phases (session-pipeline.ts). Rollout: the pipeline's
 * resources are created (all, or the rest after a partial apply), and the store
 * changes only by the two grants it needs: the transcripts key policy (decrypt via
 * S3) and the bucket policy (list, read). Update: an existing pipeline changes in
 * place (code, env, settings). The five resources that name the pipeline's role
 * (ROLE_GRANT_RESOURCES) may update only by the role-ARN cascade, checked against
 * the detailed plan in assertPipelineRoleCascadeDiff. Never a replacement or
 * deletion, and every other store resource stays `noop`.
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
  if (grants.every((a) => a === "update" || a === "noop") && pipeline.every((a) => a === "update" || a === "noop") && pipeline.some((a) => a === "update"))
    return "session pipeline update";
  return undefined;
}

/**
 * The brief schedule's own phases (session-pipeline.ts SESSION_BRIEF_RESOURCES). Rollout: its
 * resources are created (all, or the rest after a partial apply). Update: only the rule
 * changes in place (its state when `briefs.enabled` flips); the permission stays noop.
 * In either, the rest of the plan is
 * all noop, or exactly a session pipeline update (the function's env, plus the role-ARN
 * cascade, checked by assertPipelineRoleCascadeDiff); the read API stays noop, or changes by
 * exactly its code-only cascade (assertReadApiCodeOnlyCascade). Undefined
 * when every brief resource is noop: the plan is then judged as before.
 */
function sessionBriefPhase(
  operations: ReadonlyMap<string, string>,
  detailedPlanText: string,
  stateSnapshotText: string | undefined,
  refuse: (why: string) => never,
): TranscriptStorePlanPhase | undefined {
  // Absent (a plan captured before briefs) or all noop: not a brief phase. expectedResources
  // already refused a plan that lists some brief resources but not all.
  const [schedule, permission] = SESSION_BRIEF_RESOURCES.map((r) => operations.get(r) ?? "noop");
  const brief = [schedule, permission];
  if (brief.every((a) => a === "noop")) return undefined;
  if(SESSION_ANALYSIS_CACHE_RESOURCES.some(r=>operations.has(r)&&operations.get(r)!=="noop"))return refuse("session brief and analysis cache schedules require separate reviewed phases");
  const changes = () => [...operations].filter(([, a]) => a !== "noop").map(([r, a]) => `${r}:${a}`).join(", ");
  const phase: TranscriptStorePlanPhase | undefined =
    brief.every((a) => a === "create" || a === "noop")
      ? "session brief rollout"
      : schedule === "update" && permission === "noop"
        ? "session brief update"
        : undefined;
  // The EventBridge permission is create-once. Enabling or disabling briefs changes only
  // the schedule state; an in-place permission edit could widen the invoking principal.
  if (!phase) return refuse(`session brief resources have an unsafe action combination: ${changes()}`);
  // The read API stays noop, except for the code-only cascade a shared-code change on main
  // causes (assertReadApiCodeOnlyCascade): it is then judged on its own and left out of the rest.
  const readApi = SESSION_READ_API_RESOURCES as readonly string[];
  if (!readApi.every((r) => !operations.has(r) || operations.get(r) === "noop")) {
    if (!readApiCodeOnlyShape(operations))
      return refuse(`${phase}: the read api may not change in the same plan: ${changes()}`);
    assertReadApiCodeOnlyCascade(detailedPlanText, stateSnapshotText, (why) => refuse(`${phase}: ${why}`));
  }
  const rest = new Map(
    [...operations].filter(([r]) => !(SESSION_BRIEF_RESOURCES as readonly string[]).includes(r) && !readApi.includes(r)),
  );
  if ([...rest.values()].every((a) => a === "noop")) return phase;
  if (sessionPipelinePhase(rest) !== "session pipeline update")
    return refuse(`${phase}: the rest of the plan must be noop or a session pipeline update: ${changes()}`);
  assertPipelineRoleCascadeDiff(rest, detailedPlanText, stateSnapshotText, refuse);
  return phase;
}

function sessionAnalysisCachePhase(
  operations: ReadonlyMap<string, string>,
  detailedPlanText: string,
  stateSnapshotText: string | undefined,
  refuse: (why: string) => never,
): TranscriptStorePlanPhase | undefined {
  // Absent (a plan captured before briefs) or all noop: not an analysis cache phase. expectedResources
  // already refused a plan that lists some analysis cache resources but not all.
  const [schedule, permission,reconcileSchedule,reconcilePermission] = SESSION_ANALYSIS_CACHE_RESOURCES.map((r) => operations.get(r) ?? "noop");
  const brief = [schedule, permission,reconcileSchedule,reconcilePermission];
  if (brief.every((a) => a === "noop")) return undefined;
  if(SESSION_BRIEF_RESOURCES.some(r=>operations.has(r)&&operations.get(r)!=="noop"))return refuse("brief and analysis cache schedules require separate reviewed phases");
  const changes = () => [...operations].filter(([, a]) => a !== "noop").map(([r, a]) => `${r}:${a}`).join(", ");
  const analysisPhase: TranscriptStorePlanPhase | undefined =
    brief.every((a) => a === "create" || a === "noop")
      ? "session analysis cache rollout"
      : permission === "noop" && reconcilePermission === "noop" && [schedule,reconcileSchedule].every(a=>a==="update"||a==="noop") && [schedule,reconcileSchedule].some(a=>a==="update")
        ? "session analysis cache update"
        : undefined;
  // The EventBridge permission is create-once. Enabling or disabling briefs changes only
  // the schedule state; an in-place permission edit could widen the invoking principal.
  if (!analysisPhase) return refuse(`session analysis cache resources have an unsafe action combination: ${changes()}`);
  // The read API stays noop, except for the code-only cascade a shared-code change on main
  // causes (assertReadApiCodeOnlyCascade): it is then judged on its own and left out of the analysisRest.
  const analysisReadApi = SESSION_READ_API_RESOURCES as readonly string[];
  if (!analysisReadApi.every((r) => !operations.has(r) || operations.get(r) === "noop")) {
    if (!readApiCodeOnlyShape(operations))
      return refuse(`${analysisPhase}: the read api may not change in the same plan: ${changes()}`);
    assertReadApiCodeOnlyCascade(detailedPlanText, stateSnapshotText, (why) => refuse(`${analysisPhase}: ${why}`));
  }
  const analysisRest = new Map(
    [...operations].filter(([r]) => !(SESSION_ANALYSIS_CACHE_RESOURCES as readonly string[]).includes(r) && !analysisReadApi.includes(r)),
  );
  if ([...analysisRest.values()].every((a) => a === "noop")) return analysisPhase;
  if (sessionPipelinePhase(analysisRest) !== "session pipeline update")
    return refuse(`${analysisPhase}: the analysisRest of the plan must be noop or a session pipeline update: ${changes()}`);
  assertPipelineRoleCascadeDiff(analysisRest, detailedPlanText, stateSnapshotText, refuse);
  return analysisPhase;
}

/** The analysis-job schedule (lane F1): its own reviewed phase, exactly as the semantic copier's. */
function sessionAnalysisJobPhase(operations:ReadonlyMap<string,string>,detailedPlanText:string,stateSnapshotText:string|undefined,refuse:(why:string)=>never):TranscriptStorePlanPhase|undefined{
 const [schedule,permission]=SESSION_ANALYSIS_JOB_RESOURCES.map(r=>operations.get(r)??"noop");
 if(schedule==="noop"&&permission==="noop")return undefined;
 const phase:TranscriptStorePlanPhase|undefined=[schedule,permission].every(a=>a==="create"||a==="noop")?"session analysis jobs rollout":schedule==="update"&&permission==="noop"?"session analysis jobs update":undefined;
 if(!phase)return refuse("analysis job resources have an unsafe action combination");
 // Enabling the jobs also adds the pipeline role's analysis-result bucket and key statements:
 // the bucket and key may update in this phase only when the detailed plan names those Sids.
 const storeUpdates=["transcripts","transcripts-key"].filter(r=>operations.get(r)==="update");
 for(const r of storeUpdates){
  const sid=r==="transcripts"?"SessionPipelineWritesAnalysisResults":"SessionPipelineEncryptsAnalysisResultsViaS3";
  if(!detailedPlanText.includes(sid))return refuse(`analysis jobs: ${r} may update only to add ${sid}`);
 }
 const allowed=new Set<string>([...SESSION_ANALYSIS_JOB_RESOURCES,...SESSION_PIPELINE_RESOURCES,...ROLE_GRANT_RESOURCES,...SESSION_READ_API_RESOURCES,...storeUpdates]);
 if([...operations].some(([r,a])=>a!=="noop"&&!allowed.has(r)))return refuse("analysis jobs require a separate reviewed phase");
 const readApi=SESSION_READ_API_RESOURCES as readonly string[];
 if(!readApi.every(r=>!operations.has(r)||operations.get(r)==="noop")){
  if(!readApiCodeOnlyShape(operations))return refuse("analysis jobs: read api may not change in same plan");
  assertReadApiCodeOnlyCascade(detailedPlanText,stateSnapshotText,why=>refuse("analysis jobs: "+why));
 }
 const rest=new Map([...operations].filter(([r])=>!(SESSION_ANALYSIS_JOB_RESOURCES as readonly string[]).includes(r)&&!readApi.includes(r)&&!storeUpdates.includes(r)));
 if([...rest.values()].every(a=>a==="noop"))return phase;
 if(sessionPipelinePhase(rest)!=="session pipeline update")return refuse("analysis jobs: rest must be noop or session pipeline update");
 assertPipelineRoleCascadeDiff(rest,detailedPlanText,stateSnapshotText,refuse);
 return phase;
}

/** The copier schedule is a separately reviewed phase, never a mixed rollout. */
function sessionSemanticCopyPhase(operations:ReadonlyMap<string,string>,detailedPlanText:string,stateSnapshotText:string|undefined,refuse:(why:string)=>never):TranscriptStorePlanPhase|undefined{
 const [semanticSchedule,semanticPermission]=SESSION_SEMANTIC_COPY_RESOURCES.map(r=>operations.get(r)??"noop");
 if(semanticSchedule==="noop"&&semanticPermission==="noop")return undefined;
 const semanticPhase:TranscriptStorePlanPhase|undefined=[semanticSchedule,semanticPermission].every(a=>a==="create"||a==="noop")?"session semantic copy rollout":semanticSchedule==="update"&&semanticPermission==="noop"?"session semantic copy update":undefined;
 if(!semanticPhase)return refuse("semantic copy resources have an unsafe action combination");
 const allowed=new Set<string>([...SESSION_SEMANTIC_COPY_RESOURCES,...SESSION_PIPELINE_RESOURCES,...ROLE_GRANT_RESOURCES,...SESSION_READ_API_RESOURCES]);
 if([...operations].some(([r,a])=>a!=="noop"&&!allowed.has(r)))return refuse("semantic copy requires a separate reviewed phase");
 const semanticReadApi=SESSION_READ_API_RESOURCES as readonly string[];
 if(!semanticReadApi.every(r=>!operations.has(r)||operations.get(r)==="noop")){
  if(!readApiCodeOnlyShape(operations))return refuse("semantic copy: read api may not change in same plan");
  assertReadApiCodeOnlyCascade(detailedPlanText,stateSnapshotText,why=>refuse("semantic copy: "+why));
 }
 const semanticRest=new Map([...operations].filter(([r])=>!(SESSION_SEMANTIC_COPY_RESOURCES as readonly string[]).includes(r)&&!semanticReadApi.includes(r)));
 if([...semanticRest.values()].every(a=>a==="noop"))return semanticPhase;
 if(sessionPipelinePhase(semanticRest)!=="session pipeline update")return refuse("semantic copy: rest must be noop or session pipeline update");
 assertPipelineRoleCascadeDiff(semanticRest,detailedPlanText,stateSnapshotText,refuse);
 return semanticPhase;
}

/** The buffered feed schedule is its own reviewed phase, never a mixed rollout. */
function sessionBufferedSearchFeedPhase(operations: ReadonlyMap<string, string>, detailedPlanText: string, stateSnapshotText: string | undefined, refuse: (why: string) => never): TranscriptStorePlanPhase | undefined {
  const [schedule, permission] = SESSION_BUFFERED_SEARCH_FEED_RESOURCES.map((r) => operations.get(r) ?? "noop");
  if (schedule === "noop" && permission === "noop") return undefined;
  const phase: TranscriptStorePlanPhase | undefined = [schedule, permission].every((a) => a === "create" || a === "noop")
    ? "session buffered search feed rollout"
    : schedule === "update" && permission === "noop"
      ? "session buffered search feed update"
      : undefined;
  if (!phase) return refuse("buffered search feed resources have an unsafe action combination");
  const allowed = new Set<string>([...SESSION_BUFFERED_SEARCH_FEED_RESOURCES, ...SESSION_PIPELINE_RESOURCES, ...ROLE_GRANT_RESOURCES, ...SESSION_READ_API_RESOURCES]);
  if ([...operations].some(([r, a]) => a !== "noop" && !allowed.has(r))) return refuse("buffered search feed requires a separate reviewed phase");
  const readApi = SESSION_READ_API_RESOURCES as readonly string[];
  if (!readApi.every((r) => !operations.has(r) || operations.get(r) === "noop")) {
    if (!readApiCodeOnlyShape(operations)) return refuse("buffered search feed: read api may not change in same plan");
    assertReadApiCodeOnlyCascade(detailedPlanText, stateSnapshotText, (why) => refuse("buffered search feed: " + why));
  }
  const feedRest = new Map([...operations].filter(([r]) => !(SESSION_BUFFERED_SEARCH_FEED_RESOURCES as readonly string[]).includes(r) && !readApi.includes(r)));
  if ([...feedRest.values()].every((a) => a === "noop")) return phase;
  if (sessionPipelinePhase(feedRest) !== "session pipeline update") return refuse("buffered search feed: rest must be noop or session pipeline update");
  assertPipelineRoleCascadeDiff(feedRest, detailedPlanText, stateSnapshotText, refuse);
  return phase;
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
  // Default-off inbox authority activation changes only the existing srv-writer's inline policy.
  // The detailed plan must add exactly the two reviewed object roots to both Get and Put.
  { name: "srv-writer inbox policy update", actions: { "transcript-srv-writer": "update" } },
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

/**
 * The resources a plan must list: the store's resources, plus every pipeline resource
 * once any of them appears, plus every read API resource once any of those does, plus
 * every brief resource once any of those does (and then the pipeline's too).
 */
function expectedResources(names: Iterable<string>): readonly string[] {
  const list = [...names];
  const pipeline = new Set<string>(SESSION_PIPELINE_RESOURCES);
  const readApi = new Set<string>(SESSION_READ_API_RESOURCES);
  const brief = new Set<string>(SESSION_BRIEF_RESOURCES);
  const withBrief = list.some((n) => brief.has(n));
  const analysis=new Set<string>(SESSION_ANALYSIS_CACHE_RESOURCES);const withAnalysis=list.some(n=>analysis.has(n));
  const withSemantic=list.some(n=>(SESSION_SEMANTIC_COPY_RESOURCES as readonly string[]).includes(n));
  const withAnalysisJobs=list.some(n=>(SESSION_ANALYSIS_JOB_RESOURCES as readonly string[]).includes(n));
  const withBufferedFeed = list.some((n) => (SESSION_BUFFERED_SEARCH_FEED_RESOURCES as readonly string[]).includes(n));
  return [
    ...TRANSCRIPT_RESOURCES,
    ...(withBrief || withAnalysis || withSemantic || withAnalysisJobs || withBufferedFeed || list.some((n) => pipeline.has(n)) ? SESSION_PIPELINE_RESOURCES : []),
    ...(withBrief ? SESSION_BRIEF_RESOURCES : []),
    ...(withAnalysis ? SESSION_ANALYSIS_CACHE_RESOURCES : []),
    ...(withSemantic?SESSION_SEMANTIC_COPY_RESOURCES:[]),
    ...(withAnalysisJobs?SESSION_ANALYSIS_JOB_RESOURCES:[]),
    ...(withBufferedFeed ? SESSION_BUFFERED_SEARCH_FEED_RESOURCES : []),
    ...(list.some((n) => readApi.has(n)) ? SESSION_READ_API_RESOURCES : []),
  ];
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

  // The repository rename re-tags the stack: judged on its own (assertRepoTagRenameDiff), before any other phase.
  if (planTouchesRepoTag(capture.detailedPlanText)) {
    assertRepoTagRenameDiff(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse);
    return "repo tag rename";
  }
  // The brief schedule changes alone, or with exactly a session pipeline update.
  const semanticPhase=sessionSemanticCopyPhase(compact.operations,capture.detailedPlanText,capture.stateSnapshotText,refuse);
  if(semanticPhase)return semanticPhase;
  const analysisJobPhase=sessionAnalysisJobPhase(compact.operations,capture.detailedPlanText,capture.stateSnapshotText,refuse);
  if(analysisJobPhase)return analysisJobPhase;
  const bufferedFeedPhase = sessionBufferedSearchFeedPhase(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse);
  if (bufferedFeedPhase) return bufferedFeedPhase;
  const briefPhase = sessionBriefPhase(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse);
  const analysisPhase=sessionAnalysisCachePhase(compact.operations,capture.detailedPlanText,capture.stateSnapshotText,refuse);
  if(analysisPhase)return analysisPhase;
  if (briefPhase) return briefPhase;
  // The read API changes alone (or, in its rollout, with the pipeline's exact code-only cascade);
  // once it is all noop, the rest of the plan is judged as before.
  if (SESSION_READ_API_RESOURCES.some((r) => compact.operations.has(r))) {
    const readApiPhase = sessionReadApiPhase(compact.operations);
    if (readApiPhase) return readApiPhase;
    if (isReadApiRolloutWithPipelineCascade(compact.operations)) {
      const lines = capture.detailedPlanText.split(/\r?\n/);
      if (!pipelineHasNoDeclaredChanges(lines))
        refuse("session read api rollout with the pipeline cascade: session-pipeline may update only with no declared property changes");
      const pipelineOnly = new Map([...compact.operations].filter(([r]) => !(SESSION_READ_API_RESOURCES as readonly string[]).includes(r)));
      assertPipelineRoleCascadeDiff(pipelineOnly, capture.detailedPlanText, capture.stateSnapshotText, refuse);
      return "session read api rollout";
    }
    if (!SESSION_READ_API_RESOURCES.every((r) => compact.operations.get(r) === "noop")) {
      const changes = [...compact.operations].filter(([, action]) => action !== "noop").map(([resource, action]) => `${resource}:${action}`);
      return refuse(`action set does not match the session read api rollout or update phase (the read api changes alone): ${changes.join(", ")}`);
    }
  }
  const withPipeline = SESSION_PIPELINE_RESOURCES.some((r) => compact.operations.has(r));
  const changes = [...compact.operations].filter(([, action]) => action !== "noop");
  if (withPipeline && changes.length === 1 && changes[0]![0] === "transcript-srv-writer" && changes[0]![1] === "update") {
    assertSrvWriterInboxPolicyDiff(capture.detailedPlanText, refuse);
    return "srv-writer inbox policy update";
  }
  if (withPipeline) {
    if (SESSION_PIPELINE_RESOURCES.every((r) => compact.operations.get(r) === "noop") && TRANSCRIPT_RESOURCES.every((r) => compact.operations.get(r) === "noop"))
      return "post-apply";
    const pipelinePhase = sessionPipelinePhase(compact.operations);
    if (pipelinePhase === "session pipeline update")
      assertPipelineRoleCascadeDiff(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse);
    if (pipelinePhase) return pipelinePhase;
    const describedChanges = changes.map(([resource, action]) => `${resource}:${action}`);
    return refuse(`action set does not match the session pipeline rollout, update or post-apply phase: ${describedChanges.join(", ") || "(none)"}`);
  }
  const phase = TRANSCRIPT_PLAN_PHASES.find((candidate) =>
    TRANSCRIPT_RESOURCES.every((resource) => compact.operations.get(resource) === (candidate.actions[resource] ?? "noop")),
  );
  if (!phase) {
    const changes = [...compact.operations].filter(([, action]) => action !== "noop").map(([resource, action]) => `${resource}:${action}`);
    return refuse(`action set does not match initial, recovery, exact two-policy remediation, srv-writer addition/recovery/inbox-policy update, or post-apply phase: ${changes.join(", ") || "(none)"}`);
  }
  if (phase.name === "srv-writer propagation recovery") assertSrvWriterRecoveryDiff(capture.detailedPlanText, capture.stateSnapshotText, refuse);
  if (phase.name === "srv-writer inbox policy update") assertSrvWriterInboxPolicyDiff(capture.detailedPlanText, refuse);
  return phase.name;
}

/**
 * The resources whose policies name the session pipeline's execution role. A pipeline
 * update (any change to the function) re-plans the function's `roleArn` output as
 * `(known after apply)`: Alchemy's Lambda provider keeps `roleName` stable but not
 * `roleArn`. So every one of them shows an update in which the role's ARN becomes
 * `(known after apply)`, though the role, and so the ARN, does not change.
 */
export const ROLE_GRANT_RESOURCES = ["session-events", "session-pipeline-secret", "session-reconcile-continue", "transcripts", "transcripts-key"] as const;
const KNOWN_AFTER_APPLY = "(known after apply)";
/** The two keys that hold the role ARN: a policy statement's `Principal.AWS`, and the permission's `principal`. */
const ROLE_ARN_LINE = /^(\s*(?:AWS|principal): )(.*)$/;

/** One resource's lines in a detailed plan, between its `[resource] action` line and the next. */
function detailedBlock(lines: readonly string[], resource: string): string[] {
  const start = lines.findIndex((line) => line.trim() === `[${resource}] update`);
  if (start < 0) return [];
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((line) => /^\s*\[[^\]]+\]\s+\S+\s*$/.test(line));
  return end < 0 ? rest : rest.slice(0, end);
}

/**
 * The one permitted srv-writer policy expansion. The rendered diff must add the
 * same reviewed authority and candidate object ARNs once to Get and once to
 * Put, without removing or changing any other property/action/condition.
 */
function assertSrvWriterInboxPolicyDiff(detailedPlanText: string, refuse: (why: string) => never): void {
  const label = "srv-writer inbox policy update";
  const block = detailedBlock(detailedPlanText.split(/\r?\n/), "transcript-srv-writer");
  const indent = (line: string) => line.search(/\S|$/);
  const marked = block.map((line, index) => {
    const change = /^  ([+-]) (.*)$/.exec(line);
    return change ? { index, kind: change[1]!, payload: change[2]! } : undefined;
  }).filter((entry): entry is { index: number; kind: string; payload: string } => entry !== undefined);
  const oldLines = marked.filter(({ kind }) => kind === "-").map(({ payload }) => payload);
  const newLines = marked.filter(({ kind }) => kind === "+").map(({ payload }) => payload);
  if (oldLines.length === 0 || newLines.length === 0)
    refuse(`${label}: expected Alchemy's native old/new policy statement diff`);

  type StatementLine = { index: number; value: string };
  type StatementShape = {
    sid: string;
    effect: string;
    actions: string[];
    resources: StatementLine[];
  };
  const parseStatements = (lines: string[], side: "old" | "new"): StatementShape[] => {
    const starts = lines.map((line, index) => {
      const action = /^(\s*)-\s+Action:\s*$/.exec(line);
      return action ? { index, indent: action[1]!.length } : undefined;
    }).filter((entry): entry is { index: number; indent: number } => entry !== undefined);
    const statementIndent = Math.min(...starts.map(({ indent: width }) => width));
    if (!Number.isFinite(statementIndent)) refuse(`${label}: ${side} policy has no native Statement array`);
    if (starts.some(({ indent: width }) => width !== statementIndent))
      refuse(`${label}: ${side} policy has a nested Action masquerading as a statement`);
    const propertyIndent = statementIndent + 2;
    const statements = starts.map((start, ordinal): StatementShape => {
      const end = starts[ordinal + 1]?.index ?? lines.length;
      const body = lines.slice(start.index, end).map((line, offset) => ({ line, index: start.index + offset }));
      const direct = (pattern: RegExp) => body.filter(({ line }) => indent(line) === propertyIndent && pattern.test(line));
      const sidFields = body.filter(({ line }) => /^\s*Sid:\s+/.test(line));
      if (sidFields.some(({ line }) => indent(line) !== propertyIndent))
        refuse(`${label}: ${side} policy has a nested Sid field`);
      const sids = direct(/^\s*Sid:\s+([A-Za-z][A-Za-z0-9]*)\s*$/);
      if (sids.length !== 1) refuse(`${label}: ${side} statement must have exactly one direct Sid`);
      const sid = /^\s*Sid:\s+([A-Za-z][A-Za-z0-9]*)\s*$/.exec(sids[0]!.line)![1]!;
      const effects = direct(/^\s*Effect:\s+(Allow|Deny)\s*$/);
      if (effects.length !== 1) refuse(`${label}: ${side} ${sid} must have exactly one direct Effect`);
      const effect = /^\s*Effect:\s+(Allow|Deny)\s*$/.exec(effects[0]!.line)![1]!;
      const actionEndOffset = lines.slice(start.index + 1, end).findIndex((line) => indent(line) <= propertyIndent);
      const actionEnd = actionEndOffset < 0 ? end : start.index + 1 + actionEndOffset;
      const actions = lines.slice(start.index + 1, actionEnd)
        .filter((line) => indent(line) === propertyIndent + 2)
        .map((line) => /^\s*-\s+(\S+)\s*$/.exec(line)?.[1])
        .filter((value): value is string => value !== undefined);
      if (actions.length === 0) refuse(`${label}: ${side} ${sid} has no direct Action values`);
      const resources = direct(/^\s*Resource:\s*$/);
      if (resources.length !== 1) refuse(`${label}: ${side} ${sid} must have exactly one direct Resource`);
      const resourceStart = resources[0]!.index;
      const resourceEndOffset = lines.slice(resourceStart + 1, end).findIndex((line) => indent(line) <= propertyIndent);
      const resourceEnd = resourceEndOffset < 0 ? end : resourceStart + 1 + resourceEndOffset;
      const resourceValues = lines.slice(resourceStart + 1, resourceEnd).map((line, offset) => ({ line, index: resourceStart + 1 + offset }))
        .filter(({ line }) => indent(line) === propertyIndent + 2)
        .map(({ line, index }) => ({ index, value: /^\s*-\s+(\S+)\s*$/.exec(line)?.[1] }))
        .filter((entry): entry is StatementLine => entry.value !== undefined);
      if (resourceValues.length === 0) refuse(`${label}: ${side} ${sid} has no direct Resource values`);
      return { sid, effect, actions, resources: resourceValues };
    });
    if (new Set(statements.map(({ sid }) => sid)).size !== statements.length)
      refuse(`${label}: ${side} policy statement Sids must be unique`);
    return statements;
  };

  const oldStatements = parseStatements(oldLines, "old");
  const newStatements = parseStatements(newLines, "new");
  const one = (statements: StatementShape[], sid: string, side: "old" | "new") => {
    const matches = statements.filter((statement) => statement.sid === sid);
    if (matches.length !== 1) refuse(`${label}: expected exactly one ${side} ${sid} statement`);
    return matches[0]!;
  };

  const bucketArn = transcriptNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID).bucketArn;
  const baseline = srvWriterObjects(bucketArn);
  const addedResourceIndexes = new Set<number>();
  let authority: string | undefined;
  const verifyTarget = (sid: string, action: string) => {
    const oldStatement = one(oldStatements, sid, "old");
    const newStatement = one(newStatements, sid, "new");
    if (oldStatement.effect !== "Allow" || newStatement.effect !== "Allow")
      refuse(`${label}: ${sid} must remain Effect Allow`);
    if (oldStatement.actions.length !== 1 || oldStatement.actions[0] !== action || newStatement.actions.length !== 1 || newStatement.actions[0] !== action)
      refuse(`${label}: ${sid} must remain exactly action ${action}`);
    const oldResources = oldStatement.resources.map(({ value }) => value);
    const newResources = newStatement.resources.map(({ value }) => value);
    if (JSON.stringify(oldResources) !== JSON.stringify(baseline))
      refuse(`${label}: old ${sid} resources do not match the dormant srv-writer baseline`);
    if (newResources.length !== baseline.length + 2 || JSON.stringify(newResources.slice(0, baseline.length)) !== JSON.stringify(baseline))
      refuse(`${label}: new ${sid} must append exactly authority and candidate resources`);
    const nextAuthority = newResources[baseline.length]!;
    const candidate = newResources[baseline.length + 1]!;
    if (candidate !== `${bucketArn}/${INBOX_CANDIDATE_KEY_ROOT}/*`)
      refuse(`${label}: ${sid} must append the fixed candidate resource last`);
    if (authority !== undefined && authority !== nextAuthority)
      refuse(`${label}: read and write must append the same authority resource`);
    authority = nextAuthority;
    addedResourceIndexes.add(newStatement.resources[baseline.length]!.index);
    addedResourceIndexes.add(newStatement.resources[baseline.length + 1]!.index);
  };
  verifyTarget("ReadLiveObjects", "s3:GetObject");
  verifyTarget("WriteLiveObjectsSseKmsOnly", "s3:PutObject");
  if (addedResourceIndexes.size !== 4)
    refuse(`${label}: expected exactly four appended resource lines`);
  const newWithoutAdditions = newLines.filter((_, index) => !addedResourceIndexes.has(index));
  if (JSON.stringify(newWithoutAdditions) !== JSON.stringify(oldLines))
    refuse(`${label}: the policy changes more than the four reviewed Resource additions`);
  const prefix = `${bucketArn}/`;
  const line = authority ?? "";
  if (!line.startsWith(prefix) || !line.endsWith("/*"))
    refuse(`${label}: the authority resource must be an object ARN in the transcripts bucket`);
  const keyRoot = line.slice(prefix.length, -2);
  if (!validInboxAuthorityKeyRoot(keyRoot))
    refuse(`${label}: the authority resource is not an isolated reviewed key root`);
}

/** Whether the detailed plan's `session-pipeline` update block is exactly "no declared property changes". */
function pipelineHasNoDeclaredChanges(lines: readonly string[]): boolean {
  const body = detailedBlock(lines, "session-pipeline").map((l) => l.trim()).filter(Boolean);
  return body.length === 1 && body[0] === "no declared property changes";
}

/**
 * In a session pipeline update, each ROLE_GRANT_RESOURCES update must be exactly the role-ARN
 * cascade. Alchemy prints a changed key as its whole old value (`  - ` lines) followed by its whole
 * new value (`  + ` lines). Each such run must have as many `-` as `+` lines, and the i-th `-` line
 * must equal the i-th `+` line except where the `-` line is `AWS: <role>` or `principal: <role>` and
 * the `+` line is the same key with `(known after apply)`, at least once per updated resource.
 * <role> is the function's roleArn in the capture's state snapshot. The deploy has no snapshot; it is
 * bound to the captured state by the version digest, and here every replaced ARN must be one role
 * in the production account. The function itself must update, and nothing else may change.
 */
function assertPipelineRoleCascadeDiff(
  operations: ReadonlyMap<string, string>,
  detailedPlanText: string,
  stateSnapshotText: string | undefined,
  refuse: (why: string) => never,
) {
  const updated = ROLE_GRANT_RESOURCES.filter((r) => operations.get(r) === "update");
  if (updated.length === 0) return;
  if (operations.get("session-pipeline") !== "update")
    refuse(`session pipeline update: ${updated.join(", ")} update but session-pipeline does not`);
  const cascade = new Set<string>(["session-pipeline", ...updated]);
  const others = [...operations].filter(([r, a]) => a !== "noop" && !cascade.has(r)).map(([r, a]) => `${r}:${a}`);
  if (others.length) refuse(`session pipeline update: the role-ARN cascade comes with other changes: ${others.join(", ")}`);
  const roleArn = stateSnapshotText === undefined ? undefined : roleArnFromState(stateSnapshotText, "session-pipeline", "session pipeline update", refuse);
  const lines = detailedPlanText.split(/\r?\n/);
  const replaced = new Set<string>();
  for (const resource of updated)
    for (const arn of roleArnCascade(lines, resource, roleArn, "session pipeline update", "the session pipeline's role", refuse)) replaced.add(arn);
  assertOneAccountRole(replaced, "session pipeline update", refuse);
}

/**
 * One updated resource's detailed block must be exactly the role-ARN cascade (see
 * assertPipelineRoleCascadeDiff), at least once. Returns the role ARNs it replaces; each must be
 * `roleArn` when the capture's state snapshot gives it.
 */
function roleArnCascade(
  lines: readonly string[],
  resource: string,
  roleArn: string | undefined,
  label: string,
  roleName: string,
  refuse: (why: string) => never,
): Set<string> {
  const block = detailedBlock(lines, resource);
  const marked = block.map((line) => /^  ([+-]) (.*)$/.exec(line));
  const replaced = new Set<string>();
  let cascades = 0;
  for (let i = 0; i < block.length; ) {
    if (!marked[i]) {
      i++;
      continue;
    }
    let j = i;
    while (j < block.length && marked[j]?.[1] === "-") j++;
    let k = j;
    while (k < block.length && marked[k]?.[1] === "+") k++;
    const minus = marked.slice(i, j).map((m) => m![2]!);
    const plus = marked.slice(j, k).map((m) => m![2]!);
    if (minus.length === 0 || minus.length !== plus.length)
      refuse(`${label}: ${resource} adds or removes lines (${minus.length} - / ${plus.length} +), not only the role-ARN cascade`);
    for (let n = 0; n < minus.length; n++) {
      if (minus[n] === plus[n]) continue;
      const old = ROLE_ARN_LINE.exec(minus[n]!);
      if (!old || plus[n] !== `${old[1]}${KNOWN_AFTER_APPLY}`)
        refuse(`${label}: ${resource} changes ${JSON.stringify(minus[n]!.trim())} to ${JSON.stringify(plus[n]!.trim())}, not only the role-ARN cascade`);
      if (roleArn !== undefined && old[2] !== roleArn)
        refuse(`${label}: ${resource} replaces ${old[2]}, which is not ${roleName} ${roleArn}`);
      replaced.add(old[2]!);
      cascades++;
    }
    i = k;
  }
  if (cascades === 0) refuse(`${label}: ${resource} updates without the role-ARN cascade`);
  return replaced;
}

function assertOneAccountRole(replaced: ReadonlySet<string>, label: string, refuse: (why: string) => never) {
  const role = new RegExp(`^arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/[A-Za-z0-9_+=,.@-]+$`);
  if (replaced.size !== 1 || ![...replaced].every((arn) => role.test(arn)))
    refuse(`${label}: the cascade must replace exactly one role ARN in ${TRANSCRIPT_ACCOUNT_ID}, found ${[...replaced].join(", ")}`);
}

/**
 * The read API's code-only shape: `session-read-api` updates, `session-read-api-secret` updates or
 * is noop, and both alarms are noop. Nothing is created, so a first rollout never rides along.
 */
function readApiCodeOnlyShape(operations: ReadonlyMap<string, string>): boolean {
  if (operations.get("session-read-api") !== "update") return false;
  const secret = operations.get("session-read-api-secret") ?? "noop";
  if (secret !== "update" && secret !== "noop") return false;
  return SESSION_READ_API_RESOURCES.filter((r) => r !== "session-read-api" && r !== "session-read-api-secret").every(
    (r) => (operations.get(r) ?? "noop") === "noop",
  );
}

/**
 * The read API's code-only cascade: shared code changed on main, so its bundle (and Alchemy's
 * view of its role ARN) changes while none of its settings do. `session-read-api` must show
 * exactly "no declared property changes", and a `session-read-api-secret` update must be exactly
 * the role-ARN cascade for the read API's own role (from the state snapshot when there is one),
 * never the pipeline's.
 */
function assertReadApiCodeOnlyCascade(detailedPlanText: string, stateSnapshotText: string | undefined, refuse: (why: string) => never) {
  const label = "read api code-only update";
  const lines = detailedPlanText.split(/\r?\n/);
  const body = detailedBlock(lines, "session-read-api").map((l) => l.trim()).filter(Boolean);
  if (body.length !== 1 || body[0] !== "no declared property changes")
    refuse(`${label}: session-read-api may update only with no declared property changes`);
  if (!lines.some((l) => l.trim() === "[session-read-api-secret] update")) return;
  const roleArn = stateSnapshotText === undefined ? undefined : roleArnFromState(stateSnapshotText, "session-read-api", label, refuse);
  const replaced = roleArnCascade(lines, "session-read-api-secret", roleArn, label, "the read api's role", refuse);
  assertOneAccountRole(replaced, label, refuse);
  const pipelineRole = stateSnapshotText === undefined ? undefined : roleArnFromState(stateSnapshotText, "session-pipeline", label, refuse);
  if (pipelineRole !== undefined && replaced.has(pipelineRole))
    refuse(`${label}: session-read-api-secret replaces the session pipeline's role`);
}

/** A function's execution role ARN, as the capture's state snapshot records it. */
function roleArnFromState(stateSnapshotText: string, resource: string, label: string, refuse: (why: string) => never): string {
  let objects: Array<{ key: string; text: string }>;
  try {
    objects = (JSON.parse(stateSnapshotText) as { objects: Array<{ key: string; text: string }> }).objects;
  } catch {
    return refuse(`${label}: state snapshot is not valid JSON`);
  }
  const hits = (objects ?? []).filter((o) => o.key === `${alchemyStatePrefix(TRANSCRIPT_STAGE)}/${resource}.json`);
  if (hits.length !== 1) refuse(`${label}: state snapshot has ${hits.length} ${resource} records`);
  let fn: { status?: string; attr?: { roleArn?: unknown } };
  try {
    fn = JSON.parse(hits[0]!.text) as typeof fn;
  } catch {
    return refuse(`${label}: the ${resource} state record is not valid JSON`);
  }
  if ((fn.status !== "created" && fn.status !== "updated") || typeof fn.attr?.roleArn !== "string" || !fn.attr.roleArn)
    refuse(`${label}: ${resource} is not created in state with a roleArn`);
  return fn.attr!.roleArn as string;
}

/**
 * The one accepted change of the stack-wide `96bulls:repo` tag: the repository rename to
 * AgentWorkforce/sessions-pipeline (3f8494c). Hard-coded, so no later tag change rides through.
 */
export const REPO_TAG_RENAME = { from: "AgentWorkforce/96BullSessions", to: "AgentWorkforce/sessions-pipeline" } as const;
const REPO_TAG_KEY = "96bulls:repo:";
/** A changed (`  - ` / `  + `) line of the detailed plan whose key is the repo tag. */
const REPO_TAG_DIFF_LINE = /^  [+-] \s*96bulls:repo:/;
/** A repo tag line (without its diff marker): indentation, value. */
const REPO_TAG_LINE = /^(\s*)96bulls:repo: (.*)$/;

/** Whether any resource in the detailed plan changes, adds or removes the repo tag. */
function planTouchesRepoTag(detailedPlanText: string): boolean {
  return detailedPlanText.split(/\r?\n/).some((line) => REPO_TAG_DIFF_LINE.test(line));
}

/**
 * The repo tag rename: every resource re-tagged `96bulls:repo` from REPO_TAG_RENAME.from to
 * REPO_TAG_RENAME.to, and nothing else. The plan may only update in place (no create, so the read
 * API rollout is its own, later capture). Each updated resource's detailed diff is read as runs of
 * `  - ` lines followed by as many `  + ` lines; the i-th `-` line must equal the i-th `+` line,
 * except:
 *   - the repo tag pair, `<indent>96bulls:repo: <from>` to the same `<indent>96bulls:repo: <to>`,
 *     exactly once per resource (a tag-less grant, session-reconcile-continue, may have none); and
 *   - in ROLE_GRANT_RESOURCES only, and only while session-pipeline updates, the role-ARN cascade
 *     pair (assertPipelineRoleCascadeDiff): `AWS:` / `principal:` <the function's role> to
 *     `(known after apply)`, with the role read from the capture's state snapshot when it has one,
 *     and one role ARN in the production account across the plan.
 * session-pipeline itself may update only with "no declared property changes". Every other updated
 * resource must show the tag pair (or, for a grant, the cascade).
 */
function assertRepoTagRenameDiff(
  operations: ReadonlyMap<string, string>,
  detailedPlanText: string,
  stateSnapshotText: string | undefined,
  refuse: (why: string) => never,
) {
  const why = (s: string): never => refuse(`repo tag rename: ${s}`);
  const notUpdate = [...operations].filter(([, a]) => a !== "update" && a !== "noop").map(([r, a]) => `${r}:${a}`);
  if (notUpdate.length) why(`only in-place updates may come with the rename, found ${notUpdate.join(", ")}; apply the rename alone first`);
  const updated = [...operations].filter(([, a]) => a === "update").map(([r]) => r);
  const lines = detailedPlanText.split(/\r?\n/);
  const pipelineUpdates = operations.get("session-pipeline") === "update";
  if (pipelineUpdates) {
    const body = detailedBlock(lines, "session-pipeline").map((l) => l.trim()).filter(Boolean);
    if (body.length !== 1 || body[0] !== "no declared property changes") why("session-pipeline may update only with no declared property changes");
  }
  let stateRole: string | undefined;
  let renamed = 0;
  const cascadeArns = new Set<string>();
  for (const resource of updated) {
    if (resource === "session-pipeline") continue;
    const grant = (ROLE_GRANT_RESOURCES as readonly string[]).includes(resource);
    const block = detailedBlock(lines, resource);
    const diff = block.map((line) => /^  ([+-]) (.*)$/.exec(line));
    let tags = 0;
    let cascade = 0;
    let at = 0;
    while (at < block.length) {
      if (!diff[at]) {
        at++;
        continue;
      }
      const runStart = at;
      while (at < block.length && diff[at]?.[1] === "-") at++;
      const plusStart = at;
      while (at < block.length && diff[at]?.[1] === "+") at++;
      const removed = diff.slice(runStart, plusStart).map((m) => m![2]!);
      const added = diff.slice(plusStart, at).map((m) => m![2]!);
      if (removed.length !== added.length || removed.length === 0)
        why(`${resource} adds or removes lines (${removed.length} - / ${added.length} +), not only the tag rename`);
      removed.forEach((was, n) => {
        const now = added[n]!;
        if (was === now) return;
        const tag = REPO_TAG_LINE.exec(was);
        if (tag) {
          if (tag[2] !== REPO_TAG_RENAME.from || now !== `${tag[1]}${REPO_TAG_KEY} ${REPO_TAG_RENAME.to}`)
            why(`${resource} changes ${JSON.stringify(was.trim())} to ${JSON.stringify(now.trim())}, not exactly ${REPO_TAG_RENAME.from} to ${REPO_TAG_RENAME.to}`);
          tags++;
          return;
        }
        const principal = ROLE_ARN_LINE.exec(was);
        if (grant && pipelineUpdates && principal && now === principal[1] + KNOWN_AFTER_APPLY) {
          if (stateSnapshotText !== undefined) stateRole ??= roleArnFromState(stateSnapshotText, "session-pipeline", "session pipeline update", refuse);
          if (stateRole !== undefined && principal[2] !== stateRole)
            why(`${resource} replaces ${principal[2]}, which is not the session pipeline's role ${stateRole}`);
          cascadeArns.add(principal[2]!);
          cascade++;
          return;
        }
        why(`${resource} changes ${JSON.stringify(was.trim())} to ${JSON.stringify(now.trim())}, not only the tag rename${grant ? " (and the role-ARN cascade)" : ""}`);
      });
    }
    if (tags > 1) why(`${resource} renames the repo tag ${tags} times`);
    renamed += tags;
    if (tags === 0 && !(grant && cascade > 0)) why(`${resource} updates without the repo tag rename`);
  }
  const accountRole = new RegExp(`^arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/[A-Za-z0-9_+=,.@-]+$`);
  if (cascadeArns.size > 1 || [...cascadeArns].some((arn) => !accountRole.test(arn)))
    why(`the role-ARN cascade must replace one role ARN in ${TRANSCRIPT_ACCOUNT_ID}, found ${[...cascadeArns].join(", ")}`);
  // Every changed repo tag line in the plan is one side of a pair checked above (none outside an updated resource).
  const tagLines = lines.filter((line) => REPO_TAG_DIFF_LINE.test(line)).length;
  if (renamed === 0 || tagLines !== 2 * renamed) why(`the plan changes ${tagLines} repo tag lines, but only ${renamed} resources rename the tag`);
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
 * TLS (1.2+) only; the eraser deletes objects/versions and configured GC may delete
 * buffered versions only, preserving its current barrier. A PUT that sends
 * any encryption header other than SSE-KMS naming this bucket's key is refused,
 * including `aws:kms` without a key id (S3 would use `aws/s3`). SSE-C is blocked
 * separately by the encryption configuration.
 */
export function transcriptBucketPolicy(i: { bucketArn: string; keyArn: string; eraserArn: string; accountId?: string; bufferedGcRoleArn?: string; analysisDeleterArn?: string }) {
  if (i.bufferedGcRoleArn !== undefined) assertBufferedGcRoleArn(i.bufferedGcRoleArn, i.accountId ?? "");
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
    // Lane F1 (analysisJobs on): the session-analysis worker (the pipeline role) erases its own
    // result objects, so the eraser-only deny excludes exactly that object root, and a second
    // deny on that root alone exempts the eraser, the buffered GC and the pipeline role.
    i.analysisDeleterArn
      ? {
          Sid: "OnlyTheEraserDeletes",
          Effect: "Deny" as const,
          Principal: "*",
          Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
          NotResource: [`${i.bucketArn}/${ANALYSIS_RESULT_OBJECT_GLOB}`],
          Condition: { ArnNotEquals: { "aws:PrincipalArn": i.bufferedGcRoleArn ? [i.eraserArn, i.bufferedGcRoleArn] : i.eraserArn } },
        }
      : {
          Sid: "OnlyTheEraserDeletes",
          Effect: "Deny" as const,
          Principal: "*",
          Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
          Resource: [`${i.bucketArn}/*`],
          Condition: { ArnNotEquals: { "aws:PrincipalArn": i.bufferedGcRoleArn ? [i.eraserArn, i.bufferedGcRoleArn] : i.eraserArn } },
        },
    ...(i.analysisDeleterArn
      ? [{
          Sid: "OnlyTheEraserOrAnalysisWorkerDeletesAnalysisResults",
          Effect: "Deny" as const,
          Principal: "*",
          Action: ["s3:DeleteObject", "s3:DeleteObjectVersion"],
          Resource: [`${i.bucketArn}/${ANALYSIS_RESULT_OBJECT_GLOB}`],
          Condition: { ArnNotEquals: { "aws:PrincipalArn": [i.eraserArn, ...(i.bufferedGcRoleArn ? [i.bufferedGcRoleArn] : []), i.analysisDeleterArn] } },
        }]
      : []),
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
    ...(i.bufferedGcRoleArn ? bufferedGcBucketStatements({ accountId: i.accountId!, bucketArn: i.bucketArn, roleArn: i.bufferedGcRoleArn }) : []),
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
 * use it (no `kms:*`). The writer, srv-writer, reader and configured pipeline may use it only through S3 in
 * this region, only from this account, and only for objects under `96bulls/` in
 * this bucket (the S3 encryption context, per object since bucket keys are off).
 * Configured GC has the separately bounded buffered-object grant and explicit canonical deny.
 * The eraser and every other operator role have no use of it: deleting needs no key,
 * and nobody reads transcripts with the console.
 */
export function transcriptKeyPolicy(i: { accountId: string; region: string; bucketName: string; writerArn: string; srvWriterArn: string; readerArn: string; pipelineRoleArn?: string; bufferedGcRoleArn?: string; analysisResults?: boolean }) {
  if (i.bufferedGcRoleArn !== undefined) assertBufferedGcRoleArn(i.bufferedGcRoleArn, i.accountId);
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
      ...(i.pipelineRoleArn && i.analysisResults ? [pipelineAnalysisKeyStatement({ accountId: i.accountId, region: i.region, bucketName: i.bucketName, roleArn: i.pipelineRoleArn })] : []),
      ...(i.bufferedGcRoleArn ? bufferedGcKeyStatements({ accountId: i.accountId, region: i.region, bucketName: i.bucketName, roleArn: i.bufferedGcRoleArn }) : []),
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
export function transcriptSrvWriterPolicy(i: { bucketArn: string; accountId: string; region: string; keyAlias: string; inboxAuthorityKeyRoot?: string }) {
  const objects = srvWriterObjects(i.bucketArn, i.inboxAuthorityKeyRoot);
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
    // Defense in depth for direct callers: refuse before declaring the first resource.
    if (i.stage === TRANSCRIPT_STAGE || i.transcriptStore.bufferedGcRoleArn !== undefined)
      assertBufferedGcRoleArn(i.transcriptStore.bufferedGcRoleArn, i.accountId);
    assertInboxAuthorityKeyRoot(i.transcriptStore.inboxAuthorityKeyRoot, (why) => { throw new Error(why); });
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
      transcriptSrvWriterPolicy({
        bucketArn: n.bucketArn,
        accountId: i.accountId,
        region: i.region,
        keyAlias: n.keyAlias,
        inboxAuthorityKeyRoot: i.transcriptStore.inboxAuthorityKeyRoot,
      }),
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
        bufferedGcRoleArn: i.transcriptStore.bufferedGcRoleArn,
        ...(pipelineRoleArn ? { pipelineRoleArn } : {}),
        ...(pipeline?.analysisJobs?.enabled ? { analysisResults: true } : {}),
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
        ...transcriptBucketPolicy({ bucketArn: n.bucketArn, keyArn: key.keyArn as unknown as string, eraserArn, accountId: i.accountId, bufferedGcRoleArn: i.transcriptStore.bufferedGcRoleArn, ...(pipelineRoleArn && pipeline?.analysisJobs?.enabled ? { analysisDeleterArn: pipelineRoleArn } : {}) }),
        ...(pipelineRoleArn ? pipelineBucketStatements({ bucketArn: n.bucketArn, roleArn: pipelineRoleArn, analysisResults: pipeline?.analysisJobs?.enabled === true }) : []),
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

    const readApiInputs = i.transcriptStore.sessionReadApi;
    const readApi = readApiInputs ? yield* sessionReadApi({ stage: i.stage, p: readApiInputs, tags: i.tags }) : null;

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
      sessionReadApi: readApi,
    };
  });
