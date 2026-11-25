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
import { LIVE_UUID, MAPPING_STATES, pipelineLiveFingerprint, type PipelineLive, type PipelineLiveMapping } from "./pipeline-live.ts";
import {
  SESSION_BRIEF_RESOURCES,
  SESSION_PIPELINE_RESOURCES,
  SESSION_SEARCH_QUEUE_RESOURCES,
  ALARM_DEFAULTS,
  POOLER_BUDGET,
  sessionEventsMappingProps,
  assertSessionPipelineInputs,
  pipelineBucketStatements,
  pipelineKeyStatement,
  sessionPipelineFunction,
  sessionPipelineNames,
  sessionPipelineWiring,
  sessionSearchAlarms,
  sessionSearchDlqProps,
  sessionSearchMappingProps,
  sessionSearchQueueProps,
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
    if (k !== "managementEvents" && k !== "auditPrincipalArn" && k !== "sessionPipeline" && k !== "sessionReadApi") refuse(`transcriptStore.${k} is not a known setting`);
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
  | "post-apply"
  | "session pipeline rollout"
  | "session pipeline update"
  | "session search queue rollout"
  | "session read api rollout"
  | "session read api update"
  | "session brief rollout"
  | "session brief update"
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
  // The search-queue split's resources, when the plan lists them, change in place as pipeline resources
  // (their creation is its own phase: sessionSearchQueuePhase, judged first).
  const pipeline = [...SESSION_PIPELINE_RESOURCES, ...SESSION_SEARCH_QUEUE_RESOURCES.filter((r) => operations.has(r))].map((r) => operations.get(r));
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
  /**
   * The live pipeline function's env and event source mappings (pipeline-live.ts readPipelineLive),
   * read by the capture and by the deploy. Required to judge an in-place search-split change at
   * capture: it is the only place a variable set by hand on the function, which a function update
   * would drop, can be seen.
   */
  liveSnapshotText?: string;
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
  const search = new Set<string>(SESSION_SEARCH_QUEUE_RESOURCES);
  const withBrief = list.some((n) => brief.has(n));
  const withSearch = list.some((n) => search.has(n));
  return [
    ...TRANSCRIPT_RESOURCES,
    ...(withBrief || withSearch || list.some((n) => pipeline.has(n)) ? SESSION_PIPELINE_RESOURCES : []),
    ...(withBrief ? SESSION_BRIEF_RESOURCES : []),
    ...(withSearch ? SESSION_SEARCH_QUEUE_RESOURCES : []),
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
  // The search-queue split: its provisioning is a phase of its own (everything else noop, every
  // created resource pinned to its rendered props); its in-place changes are pinned, then judged below.
  const searchPhase = sessionSearchQueuePhase(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse, capture.liveSnapshotText, capture.provenanceText);
  if (searchPhase) return searchPhase;
  const phase = judgePlanPhase(compact, capture, refuse);
  // Every accepted pipeline plan, last: the events mapping changes only its cap, and no function
  // update may drop or overwrite a live variable the plan does not show.
  if (SESSION_PIPELINE_RESOURCES.some((r) => compact.operations.has(r))) assertPipelineInPlace(compact.operations, capture, refuse);
  return phase;
}

/** assertTranscriptStorePlanContract's phases after the capture binding and the search split. */
function judgePlanPhase(
  compact: { operations: ReadonlyMap<string, string> },
  capture: TranscriptStorePlanCapture,
  refuse: (why: string) => never,
): TranscriptStorePlanPhase {
  // The brief schedule changes alone, or with exactly a session pipeline update.
  const briefPhase = sessionBriefPhase(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse);
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
  if (withPipeline) {
    const searchNoop = SESSION_SEARCH_QUEUE_RESOURCES.every((r) => (compact.operations.get(r) ?? "noop") === "noop");
    if (searchNoop && SESSION_PIPELINE_RESOURCES.every((r) => compact.operations.get(r) === "noop") && TRANSCRIPT_RESOURCES.every((r) => compact.operations.get(r) === "noop"))
      return "post-apply";
    const pipelinePhase = sessionPipelinePhase(compact.operations);
    if (pipelinePhase === "session pipeline update")
      assertPipelineRoleCascadeDiff(compact.operations, capture.detailedPlanText, capture.stateSnapshotText, refuse);
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

/** The search-queue split's resource whose policy names the pipeline role: it updates only by the role-ARN cascade. */
const SEARCH_ROLE_GRANT_RESOURCES = ["session-search"] as const;

/**
 * The search-queue split (session-pipeline.ts SESSION_SEARCH_QUEUE_RESOURCES). Undefined when the plan
 * lists none of its resources, or (after the checks below) when none is created: the plan is then
 * judged as a pipeline plan, with the search resources as pipeline resources.
 *
 * Rollout ("session search queue rollout"): provisioning only. Its resources are created (all, or
 * the rest after a partial apply), and every other resource in the plan is noop, so no function,
 * env, mapping or grant change rides along. Each created resource's detailed block must be exactly
 * its rendered props (tags aside): the DLQ, the queue with its policy naming only the pipeline role
 * (the state snapshot's role when there is one, else one role in the production account), the
 * mapping on the search queue for the pipeline function, created disabled, and the two alarms with
 * no actions.
 *
 * In place: assertSearchSplitInPlace (one staged change, pinned old -> new, budget recomputed).
 */
function sessionSearchQueuePhase(
  operations: ReadonlyMap<string, string>,
  detailedPlanText: string,
  stateSnapshotText: string | undefined,
  refuse: (why: string) => never,
  liveSnapshotText: string | undefined,
  provenanceText: string,
): TranscriptStorePlanPhase | undefined {
  const search = SESSION_SEARCH_QUEUE_RESOURCES as readonly string[];
  if (!search.some((r) => operations.has(r))) return undefined;
  const lines = detailedPlanText.split(/\r?\n/);
  const names = sessionPipelineNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION);
  const changes = () => [...operations].filter(([, a]) => a !== "noop").map(([r, a]) => `${r}:${a}`).join(", ");
  const created = search.filter((r) => operations.get(r) === "create");
  if (created.length > 0) {
    const label = "session search queue rollout";
    if (!search.every((r) => operations.get(r) === "create" || operations.get(r) === "noop"))
      refuse(`${label}: the search resources may only be created or noop: ${changes()}`);
    const others = [...operations].filter(([r, a]) => !search.includes(r) && a !== "noop");
    if (others.length) refuse(`${label}: provisioning only, every other resource must be noop: ${changes()}`);
    // The approved starting point, from the verified live readback (capture and deploy alike): the
    // events mapping settled Enabled at exactly 20, the pool exactly 3, and no search mapping yet
    // (or, after a partial apply, the one this stage created: Disabled at exactly 16).
    if (liveSnapshotText === undefined) return refuse(`${label}: needs the live function and mapping readback (the approved baseline)`);
    const live = verifiedLive(parseLive(liveSnapshotText, refuse), names, provenanceText, refuse);
    if (live.pointer.maximumConcurrency !== APPROVED.livePointer || live.env.HOOPDB_POOL_MAX !== String(APPROVED.pool))
      refuse(`${label}: the live baseline is not the approved one (events mapping Enabled at ${APPROVED.livePointer}, HOOPDB_POOL_MAX ${APPROVED.pool})`);
    if (live.search && !(live.search.state === "Disabled" && live.search.maximumConcurrency === APPROVED.search))
      refuse(`${label}: a live search mapping exists that is not this stage's (Disabled at ${APPROVED.search})`);
    if (URL_KEY in live.env) refuse(`${label}: the live function already has ${URL_KEY}`);
    // The queue policy names exactly the function's own role: the live readback's (trusted at deploy,
    // where there is no state snapshot), and, at capture, the same role as in state.
    if (stateSnapshotText !== undefined && roleArnFromState(stateSnapshotText, "session-pipeline", label, refuse) !== live.role)
      refuse(`${label}: the function's role in state is not its live role`);
    const role = ((arn: string) => (v: string) => v === arn)(live.role);
    const known = (value: string) => (v: string) => v === value || v === KNOWN_AFTER_APPLY;
    const concurrency = (v: string) => v === String(APPROVED.search);
    const tags = { "96bulls:component": "session-pipeline", "96bulls:repo": REPO_TAG_RENAME.to, "96bulls:stack": TRANSCRIPT_STAGE };
    const expected: Record<string, unknown> = {
      "session-search-dlq": sessionSearchDlqProps({ names, tags }),
      "session-search": sessionSearchQueueProps({ names, roleArn: role as never, dlqArn: known(names.searchDlqArn) as never, tags }),
      "session-search-mapping": {
        ...sessionSearchMappingProps({ q: { enabled: true }, functionName: names.function, queueArn: known(names.searchQueueArn) as never, tags }),
        scalingConfig: { MaximumConcurrency: concurrency },
        enabled: "false",
      },
      ...Object.fromEntries(sessionSearchAlarms(names).map(({ id, props }) => [id, { ...ALARM_DEFAULTS, ...props, tags }])),
    };
    for (const resource of created) {
      const actual = planProperties(lines, resource, "create");
      if (actual === undefined) refuse(`${label}: the detailed plan has no properties for ${resource}`);
      // Tags included: exactly this stage's ownership tags, nothing foreign.
      const diff = planMismatch(actual, expected[resource], "");
      if (diff !== undefined) refuse(`${label}: ${resource} is not exactly its rendered props (${diff})`);
    }
    return label;
  }
  return assertSearchSplitInPlace(operations, lines, provenanceText, liveSnapshotText, refuse);
}

const SEARCH_LABEL = "session search queue";
/** What a live mapping's MaximumConcurrency may be read as (the account limit for SQS mappings); the stages pin exact values. */
const LIVE_MAX_CONCURRENCY = 1000;
const STAGED_MIN_CONCURRENCY = 2;
const STAGED_MAX_CONCURRENCY = 20;
const URL_KEY = "SESSION_PIPELINE_SEARCH_QUEUE_URL";
/**
 * The approved values of the rollout (docs/handoffs/search-queue-split.md): the live pointer cap
 * before and after the split, the search cap, and the pool. Each stage is pinned to them exactly;
 * budget compliance alone is not enough.
 */
export const APPROVED = Object.freeze({ livePointer: 20, splitPointer: 10, search: 16, pool: 3 });

/**
 * An in-place plan that touches the search split (a search resource, the events mapping or the
 * function) while the search resources exist: exactly one approved stage, recognised from the plan's
 * old -> new values and the verified live state, each value exact:
 *   - B:  events mapping 20 -> 10; live: events Enabled 20, search Disabled 16, pool 3, no URL;
 *   - R3: events mapping 10 -> 20; live: events Enabled 10, search Disabled 16, pool 3, no URL;
 *   - C1: search mapping enabled false -> true at 16; live: events Enabled 10, search Disabled 16, pool 3;
 *   - R2: search mapping enabled true -> false at 16; live: events Enabled 10, search Enabled 16,
 *         pool 3, and no URL (R1 already applied);
 *   - C2: function env adds exactly SESSION_PIPELINE_SEARCH_QUEUE_URL (this stage's) with
 *         HOOPDB_POOL_MAX exactly 3; live: events Enabled 10, search Enabled 16 on this queue and
 *         function, pool 3, no URL;
 *   - R1: function env removes exactly that URL; live: events Enabled 10, search Enabled 16, pool 3, the URL.
 * For C2 and R1 the full intended post-apply env (the planned new env) is compared with the live env:
 * the URL is the only permitted delta. Old values must equal the live ones (no stale plan). Anything
 * else is refused, and the budget is checked on top. A plan whose search resources are all noop and
 * that touches neither the events mapping nor the function is left to the other phases.
 */
function assertSearchSplitInPlace(
  operations: ReadonlyMap<string, string>,
  lines: readonly string[],
  provenanceText: string,
  liveSnapshotText: string | undefined,
  refuse: (why: string) => never,
): undefined {
  const names = sessionPipelineNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION);
  const changed = [...operations].filter(([, a]) => a !== "noop").map(([r]) => r);
  const changes = () => [...operations].filter(([, a]) => a !== "noop").map(([r, a]) => `${r}:${a}`).join(", ");
  if (changed.length === 0) return undefined;
  const searchTouched = (SESSION_SEARCH_QUEUE_RESOURCES as readonly string[]).some((r) => (operations.get(r) ?? "noop") !== "noop");
  if (!searchTouched && !changed.includes("session-events-mapping") && !changed.includes("session-pipeline")) return undefined;
  if (liveSnapshotText === undefined) return refuse(`${SEARCH_LABEL}: an in-place change needs the live function and mapping readback`);
  const live = verifiedLive(parseLive(liveSnapshotText, refuse), names, provenanceText, refuse);
  const grants = new Set<string>([...ROLE_GRANT_RESOURCES, ...SEARCH_ROLE_GRANT_RESOURCES]);
  const only = (r: string) => changed.length === 1 && changed[0] === r && operations.get(r) === "update";
  const liveCap = (m: PipelineLiveMapping | undefined) => (m && m.state === "Enabled" ? m.maximumConcurrency! : 0);
  const before = { pointer: liveCap(live.pointer), search: liveCap(live.search), pool: Number(live.env.HOOPDB_POOL_MAX) };
  const hasUrl = URL_KEY in live.env;
  const searchIs = (state: "Enabled" | "Disabled") => live.search !== undefined && live.search.state === state && live.search.maximumConcurrency === APPROVED.search;
  // The stage's exact live preconditions, checked after the budget (so an over-budget plan is
  // reported as such) but before anything is accepted.
  const pending: (() => void)[] = [];
  const baseline = (pointer: number, search: "Enabled" | "Disabled", url: boolean, stage: string) =>
    pending.push(() => {
      if (live.pointer.maximumConcurrency !== pointer || !searchIs(search) || live.env.HOOPDB_POOL_MAX !== String(APPROVED.pool) || hasUrl !== url)
        refuse(`${SEARCH_LABEL}: ${stage} needs the live state events Enabled ${pointer}, search ${search} ${APPROVED.search}, HOOPDB_POOL_MAX ${APPROVED.pool}, ${url ? "with" : "without"} ${URL_KEY}`);
    });
  const after = { ...before };
  if (only("session-events-mapping")) {
    const cap = assertEventsMappingUpdate(lines, names, refuse);
    if (cap.old !== live.pointer.maximumConcurrency)
      refuse(`${SEARCH_LABEL}: the plan's old events mapping MaximumConcurrency ${cap.old} is not the live ${live.pointer.maximumConcurrency} (a stale plan)`);
    if (cap.old === APPROVED.livePointer && cap.new === APPROVED.splitPointer) baseline(APPROVED.livePointer, "Disabled", false, "B");
    else if (cap.old === APPROVED.splitPointer && cap.new === APPROVED.livePointer) baseline(APPROVED.splitPointer, "Disabled", false, "R3");
    else refuse(`${SEARCH_LABEL}: the events mapping may move only ${APPROVED.livePointer} -> ${APPROVED.splitPointer} (B) or ${APPROVED.splitPointer} -> ${APPROVED.livePointer} (R3), not ${cap.old} -> ${cap.new}`);
    after.pointer = cap.new;
  } else if (only("session-search-mapping")) {
    if (!live.search) return refuse(`${SEARCH_LABEL}: the live function has no search mapping`);
    const sides = planUpdateSides(lines, "session-search-mapping");
    if (sides === undefined || sides === "unchanged") return refuse(`${SEARCH_LABEL}: session-search-mapping updates without changing enabled`);
    const diff = changedLeaves(sides.old, sides.new);
    if (diff.length !== 1 || diff[0] !== "enabled") refuse(`${SEARCH_LABEL}: session-search-mapping may change only enabled (at ${APPROVED.search}), found ${diff.join(", ") || "nothing"}`);
    const side = (v: PlanValue) => {
      const m = v as Record<string, PlanValue>;
      const enabled = m.enabled === "true" ? true : m.enabled === "false" ? false : refuse(`${SEARCH_LABEL}: session-search-mapping enabled is not a boolean`);
      const max = Number((m.scalingConfig as Record<string, PlanValue> | undefined)?.MaximumConcurrency);
      if (max !== APPROVED.search) refuse(`${SEARCH_LABEL}: session-search-mapping MaximumConcurrency must be exactly ${APPROVED.search}`);
      const { tags: _t, ...props } = m;
      const { tags: _e, ...want } = sessionSearchMappingProps({ q: { enabled: true, mappingEnabled: enabled, maxConcurrency: max }, functionName: names.function, queueArn: names.searchQueueArn, tags: {} });
      const d = planMismatch(props, want, "");
      if (d !== undefined) refuse(`${SEARCH_LABEL}: session-search-mapping is not its rendered props on the search queue (${d})`);
      return enabled;
    };
    const oldEnabled = side(sides.old);
    const newEnabled = side(sides.new);
    if (oldEnabled !== (live.search.state === "Enabled")) refuse(`${SEARCH_LABEL}: the plan's old search mapping is not the live one (a stale plan)`);
    if (!oldEnabled && newEnabled) baseline(APPROVED.splitPointer, "Disabled", false, "C1");
    else if (oldEnabled && !newEnabled) {
      if (hasUrl) refuse(`${SEARCH_LABEL}: disabling the search mapping needs R1 first: the live function still has ${URL_KEY}`);
      baseline(APPROVED.splitPointer, "Enabled", false, "R2");
    }
    after.search = newEnabled ? APPROVED.search : 0;
  } else if (operations.get("session-pipeline") === "update" && changed.every((r) => r === "session-pipeline" || grants.has(r))) {
    const sides = planUpdateSides(lines, "session-pipeline");
    // C2 and R1 always declare their env change; a code-only function update is not a split stage.
    if (sides === undefined || sides === "unchanged") return refuse(`${SEARCH_LABEL}: a function update while the search resources exist must be C2 or R1 (a declared env change)`);
    const other = changedLeaves(sides.old, sides.new).filter((k) => !k.startsWith("env."));
    if (other.length) refuse(`${SEARCH_LABEL}: the function update changes more than its env: ${other.join(", ")}`);
    const env = (v: PlanValue) => ((v as Record<string, PlanValue>).env ?? {}) as Record<string, string>;
    const oldEnv = env(sides.old);
    const newEnv = env(sides.new);
    const keys = envDelta(oldEnv, newEnv);
    const adds = keys.length === 1 && keys[0] === URL_KEY && oldEnv[URL_KEY] === undefined && newEnv[URL_KEY] === names.searchQueueUrl;
    const removes = keys.length === 1 && keys[0] === URL_KEY && newEnv[URL_KEY] === undefined && oldEnv[URL_KEY] === names.searchQueueUrl;
    if (!adds && !removes) refuse(`${SEARCH_LABEL}: the function env may change only by adding or removing ${URL_KEY} (this stage's search queue), found ${describeDelta(oldEnv, newEnv, keys)}`);
    // The full intended post-apply env (Alchemy writes the planned env whole) against the live env:
    // the stage's URL is the only permitted delta, in the stage's direction.
    const liveDelta = envDelta(live.env, newEnv);
    if (!(liveDelta.length === 1 && liveDelta[0] === URL_KEY && (adds ? live.env[URL_KEY] === undefined : live.env[URL_KEY] === names.searchQueueUrl)))
      refuse(`${SEARCH_LABEL}: after this apply the function env would differ from the live env by ${describeDelta(live.env, newEnv, liveDelta)}; only ${URL_KEY} may`);
    pending.push(() => {
      if (newEnv.HOOPDB_POOL_MAX !== String(APPROVED.pool)) refuse(`${SEARCH_LABEL}: HOOPDB_POOL_MAX must be exactly ${APPROVED.pool}`);
    });
    if (adds) {
      if (!(live.search && live.search.state === "Enabled")) refuse(`${SEARCH_LABEL}: setting ${URL_KEY} needs the search mapping on this function and queue, enabled (C1) first`);
      baseline(APPROVED.splitPointer, "Enabled", false, "C2");
    } else baseline(APPROVED.splitPointer, "Enabled", true, "R1");
    after.pool = Number(newEnv.HOOPDB_POOL_MAX);
  } else return refuse(`${SEARCH_LABEL}: not one staged change (the pointer mapping's cap, the search mapping, or the function env alone): ${changes()}`);
  const demand = (after.pointer + after.search) * after.pool;
  const worst = (Math.max(before.pointer, after.pointer) + Math.max(before.search, after.search)) * Math.max(before.pool, after.pool);
  if (demand > POOLER_BUDGET || worst > POOLER_BUDGET)
    refuse(`${SEARCH_LABEL}: after this plan (pointer ${after.pointer} + search ${after.search}) x pool ${after.pool} is ${demand} pooler connections and the apply's worst moment ${worst}, over the budget of ${POOLER_BUDGET}`);
  for (const check of pending) check();
  return undefined;
}

/** One resource's declared props in the capture's state snapshot, or undefined. */
function stateProps(stateSnapshotText: string, resource: string): Record<string, unknown> | undefined {
  try {
    const objects = (JSON.parse(stateSnapshotText) as { objects: Array<{ key: string; text: string }> }).objects ?? [];
    const hit = objects.filter((o) => o.key === `${alchemyStatePrefix(TRANSCRIPT_STAGE)}/${resource}.json`);
    return hit.length === 1 ? ((JSON.parse(hit[0]!.text) as { props?: Record<string, unknown> }).props ?? undefined) : undefined;
  } catch {
    return undefined;
  }
}

/** The keys whose values differ between two envs (either side may lack the key). */
function envDelta(a: Record<string, string>, b: Record<string, string>): string[] {
  return [...new Set([...Object.keys(a), ...Object.keys(b)])].filter((k) => a[k] !== b[k]).sort();
}

/** Key names only, never values (the env may hold anything). */
function describeDelta(a: Record<string, string>, b: Record<string, string>, keys: readonly string[]): string {
  return keys.map((k) => `${k}${b[k] === undefined ? " removed" : a[k] === undefined ? " added" : " changed"}`).join(", ") || "nothing";
}

/** The live function and its mappings (pipeline-live.ts PipelineLive, schema 2). */
function parseLive(text: string, refuse: (why: string) => never): PipelineLive {
  let v: PipelineLive;
  try {
    v = JSON.parse(text) as PipelineLive;
  } catch {
    return refuse("the live pipeline snapshot is not valid JSON");
  }
  const env = v?.function?.env;
  if (v?.schema !== 3 || typeof v.function?.name !== "string" || typeof v.function.revisionId !== "string" || typeof v.function.role !== "string" || env === null || typeof env !== "object" || !Array.isArray(v.mappings))
    refuse("the live pipeline snapshot is not schema 3");
  if (Object.values(env).some((x) => typeof x !== "string")) refuse("the live pipeline snapshot has a non-string env value");
  // Every external field that a refusal may name is checked against its allow-list first.
  if (!v.mappings.every((m) => typeof m?.uuid === "string" && LIVE_UUID.test(m.uuid) && (MAPPING_STATES as readonly unknown[]).includes(m.state)))
    refuse("the live pipeline snapshot has a mapping outside its allow-list (UUID or state)");
  return v;
}

/**
 * The live readback, verified: it is this stage's function; it is the readback the capture recorded
 * (provenance `pipelineLive`: RevisionId, env digest, every mapping's UUID, state and settings), so a
 * hand edit or another apply since the capture refuses the deploy; every mapping on the function is
 * stable (Enabled or Disabled, never Creating, Updating, Enabling, Disabling or Deleting), targets
 * this function, and is one of the two this stack declares (an unknown mapping would hold pooler
 * connections the budget cannot see); both carry the rendered settings (batch 10, 20 s window,
 * partial batch responses) and a MaximumConcurrency within the account limit; the events mapping is
 * enabled.
 */
function verifiedLive(
  live: PipelineLive,
  names: ReturnType<typeof sessionPipelineNames>,
  provenanceText: string,
  refuse: (why: string) => never,
): { env: Record<string, string>; role: string; pointer: PipelineLiveMapping; search?: PipelineLiveMapping } {
  const functionArn = `arn:aws:lambda:${TRANSCRIPT_REGION}:${TRANSCRIPT_ACCOUNT_ID}:function:${names.function}`;
  if (live.function.name !== names.function || live.function.functionArn !== functionArn || live.function.state !== "Active" || live.function.lastUpdateStatus !== "Successful")
    refuse(`${SEARCH_LABEL}: the live readback is not this stage's function, Active and Successful`);
  if (!new RegExp(`^arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/[A-Za-z0-9_+=,.@/-]+$`).test(live.function.role)) refuse(`${SEARCH_LABEL}: the live function's role is not a role in the production account`);
  let recorded: unknown;
  try {
    recorded = (JSON.parse(provenanceText) as { pipelineLive?: unknown }).pipelineLive;
  } catch {
    recorded = undefined;
  }
  if (recorded === undefined) refuse(`${SEARCH_LABEL}: the capture did not record the live function (provenance pipelineLive)`);
  if (stableJson(recorded) !== stableJson(pipelineLiveFingerprint(live)))
    refuse(`${SEARCH_LABEL}: the live function or its mappings changed since the capture (RevisionId, env or mapping UUID, state or settings); capture again`);
  let pointer: PipelineLiveMapping | undefined;
  let search: PipelineLiveMapping | undefined;
  for (const m of live.mappings) {
    if (m.state !== "Enabled" && m.state !== "Disabled") refuse(`${SEARCH_LABEL}: live mapping ${m.uuid} is ${m.state}, not stable; wait for it`);
    if (m.functionArn !== functionArn) refuse(`${SEARCH_LABEL}: live mapping ${m.uuid} does not target this function`);
    if (m.eventSourceArn === names.queueArn && !pointer) pointer = m;
    else if (m.eventSourceArn === names.searchQueueArn && !search) search = m;
    else refuse(`${SEARCH_LABEL}: live mapping ${m.uuid} is not one this stack declares (or a duplicate); its connections are outside the budget`);
    if (m.batchSize !== 10 || m.maximumBatchingWindowInSeconds !== 20 || stableJson(m.functionResponseTypes) !== stableJson(["ReportBatchItemFailures"]))
      refuse(`${SEARCH_LABEL}: live mapping ${m.uuid} does not carry the rendered settings`);
    if (!(Number.isInteger(m.maximumConcurrency) && m.maximumConcurrency! >= STAGED_MIN_CONCURRENCY && m.maximumConcurrency! <= LIVE_MAX_CONCURRENCY))
      refuse(`${SEARCH_LABEL}: live mapping ${m.uuid} has no MaximumConcurrency within ${STAGED_MIN_CONCURRENCY}-${LIVE_MAX_CONCURRENCY}`);
  }
  if (!pointer) return refuse(`${SEARCH_LABEL}: the live function has no events mapping`);
  if (pointer.state !== "Enabled") refuse(`${SEARCH_LABEL}: the live events mapping is not enabled`);
  return { env: live.function.env, role: live.function.role, pointer, ...(search ? { search } : {}) };
}

/**
 * An events (pointer) mapping update changes only scalingConfig.MaximumConcurrency; old and new
 * are otherwise exactly its rendered props: the pipeline function, the events queue, batch 10, the
 * 20 s window, partial batch responses, enabled. The old cap may be anything up to the account limit
 * (it is compared with the live one by the caller); the new cap is within the staged 2-20. Returns
 * the old and new caps.
 */
function assertEventsMappingUpdate(lines: readonly string[], names: ReturnType<typeof sessionPipelineNames>, refuse: (why: string) => never): { old: number; new: number } {
  const label = "session pipeline update";
  const sides = planUpdateSides(lines, "session-events-mapping");
  if (sides === undefined || sides === "unchanged") return refuse(`${label}: session-events-mapping updates without changing its MaximumConcurrency`);
  const diff = changedLeaves(sides.old, sides.new);
  if (diff.length !== 1 || diff[0] !== "scalingConfig.MaximumConcurrency")
    refuse(`${label}: session-events-mapping may change only its MaximumConcurrency, found ${diff.join(", ") || "nothing"}`);
  const side = (v: PlanValue, cap: number) => {
    const { tags: _t, ...props } = v as Record<string, PlanValue>;
    const max = Number((props.scalingConfig as Record<string, PlanValue> | undefined)?.MaximumConcurrency);
    if (!(Number.isInteger(max) && max >= STAGED_MIN_CONCURRENCY && max <= cap)) refuse(`${label}: session-events-mapping MaximumConcurrency must be ${STAGED_MIN_CONCURRENCY}-${cap}`);
    const { tags: _e, ...want } = sessionEventsMappingProps({ p: { maxConcurrency: max }, functionName: names.function, queueArn: names.queueArn, tags: {} });
    const d = planMismatch(props, want, "");
    if (d !== undefined) refuse(`${label}: session-events-mapping is not its rendered props on the events queue (${d})`);
    return max;
  };
  return { old: side(sides.old, LIVE_MAX_CONCURRENCY), new: side(sides.new, STAGED_MAX_CONCURRENCY) };
}

/**
 * Every plan with pipeline resources: an events mapping update is pinned (assertEventsMappingUpdate),
 * and, with the live snapshot, a function update's full intended post-apply env is compared with the
 * live env. Alchemy writes the function's whole env on every update (UpdateFunctionConfiguration with
 * exactly the stored props, code-only updates included) and plans against state, so the intended env
 * is the planned new env when the plan declares an env change, and the env in state when it does not.
 * Every key whose value differs between the intended and the live env (in either direction) must be a
 * key the plan itself visibly changes; with no declared change, none may differ. At deploy (no state
 * snapshot) a code-only update relies on the capture's check: the live function must be exactly the
 * one the capture compared (provenance pipelineLive).
 */
function assertPipelineInPlace(operations: ReadonlyMap<string, string>, capture: TranscriptStorePlanCapture, refuse: (why: string) => never) {
  const names = sessionPipelineNames(TRANSCRIPT_STAGE, TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION);
  const lines = capture.detailedPlanText.split(/\r?\n/);
  if (operations.get("session-events-mapping") === "update") assertEventsMappingUpdate(lines, names, refuse);
  if (operations.get("session-pipeline") !== "update" || capture.liveSnapshotText === undefined) return;
  const parsed = parseLive(capture.liveSnapshotText, refuse);
  const live = parsed.function.env;
  const sides = planUpdateSides(lines, "session-pipeline");
  if (sides === undefined) return refuse("session pipeline update: the planned function props cannot be read to compare with the live env");
  const envOf = (v: unknown) => (((v as Record<string, unknown> | undefined)?.env ?? undefined) as Record<string, string> | undefined);
  let oldEnv: Record<string, string>, intended: Record<string, string>;
  if (sides === "unchanged") {
    if (capture.stateSnapshotText === undefined) {
      let recorded: unknown;
      try {
        recorded = (JSON.parse(capture.provenanceText) as { pipelineLive?: unknown }).pipelineLive;
      } catch {
        recorded = undefined;
      }
      if (recorded === undefined || stableJson(recorded) !== stableJson(pipelineLiveFingerprint(parsed)))
        refuse("session pipeline update: the live function is not the one the capture compared with its intended env; capture again");
      return;
    }
    const stateEnv = envOf(stateProps(capture.stateSnapshotText, "session-pipeline"));
    if (stateEnv === undefined) return refuse("session pipeline update: the intended function env (in state) cannot be read to compare with the live env");
    oldEnv = intended = stateEnv;
  } else {
    oldEnv = envOf(sides.old) ?? {};
    intended = envOf(sides.new) ?? {};
  }
  const declared = new Set(envDelta(oldEnv, intended));
  for (const k of envDelta(live, intended)) {
    if (declared.has(k)) continue;
    if (!(k in intended)) refuse(`session pipeline update: the live function has ${k}, which this plan does not render (the update would drop it)`);
    if (!(k in live)) refuse(`session pipeline update: the update would add ${k}, which the live function does not have and the plan does not show`);
    refuse(`session pipeline update: the live ${k} differs from the planned value, and the plan does not show that change`);
  }
}

/**
 * A detailed plan's update block as its old and new props: Alchemy prints a changed key's whole old
 * value as `  - ` lines and its whole new value as `  + ` lines, among the unchanged lines.
 * "unchanged" for "no declared property changes"; undefined when there is no readable block.
 */
export function planUpdateSides(lines: readonly string[], resource: string): { old: PlanValue; new: PlanValue } | "unchanged" | undefined {
  const start = lines.findIndex((line) => line.trim() === `[${resource}] update`);
  if (start < 0) return undefined;
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((line) => /^\s*\[[^\]]+\]\s+\S+\s*$/.test(line));
  const block = (end < 0 ? rest : rest.slice(0, end)).filter((line) => line.trim() !== "");
  if (block.length === 1 && block[0]!.trim() === "no declared property changes") return "unchanged";
  const side = (drop: "+" | "-") => block.filter((l) => !l.startsWith(`  ${drop} `)).map((l) => (/^  [+-] /.test(l) ? `  ${l.slice(4)}` : l));
  const old = parsePlanBlock(side("+"));
  const now = parsePlanBlock(side("-"));
  return old === undefined || now === undefined ? undefined : { old, new: now };
}

/** The dotted leaf paths whose printed values differ between two parsed props. */
function changedLeaves(a: PlanValue, b: PlanValue): string[] {
  const leaves = (v: PlanValue | undefined, path: string, out: Map<string, string>) => {
    if (typeof v === "string" || v === undefined) out.set(path, v ?? "\u0000absent");
    else if (Array.isArray(v)) v.forEach((x, n) => leaves(x, `${path}[${n}]`, out));
    else for (const k of Object.keys(v)) leaves(v[k], path ? `${path}.${k}` : k, out);
    return out;
  };
  const x = leaves(a, "", new Map()), y = leaves(b, "", new Map());
  return [...new Set([...x.keys(), ...y.keys()])].filter((k) => x.get(k) !== y.get(k)).sort();
}

/** A detailed plan's property value: Alchemy prints declared props as block YAML (maps, lists, scalars). */
type PlanValue = string | PlanValue[] | { [key: string]: PlanValue };

/** The `properties:` of one resource's `[resource] <action>` block in a detailed plan, parsed; undefined when absent or not fully parsed. */
export function planProperties(lines: readonly string[], resource: string, action: string): PlanValue | undefined {
  const start = lines.findIndex((line) => line.trim() === `[${resource}] ${action}`);
  if (start < 0) return undefined;
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((line) => /^\s*\[[^\]]+\]\s+\S+\s*$/.test(line));
  return parsePlanBlock((end < 0 ? rest : rest.slice(0, end)).filter((line) => line.trim() !== ""));
}

/** A resource block's lines (`properties:` first) parsed, or undefined when absent or not fully parsed. */
function parsePlanBlock(block: readonly string[]): PlanValue | undefined {
  if (block[0]?.trim() !== "properties:") return undefined;
  const items = block.slice(1).map((line) => ({ indent: line.length - line.trimStart().length, text: line.trim() }));
  if (items.length === 0) return undefined;
  const at = { i: 0 };
  const value = parsePlanYaml(items, at, items[0]!.indent);
  return at.i === items.length ? value : undefined;
}

function planScalar(text: string): string {
  if (text.startsWith('"') && text.endsWith('"') && text.length >= 2) {
    try {
      return JSON.parse(text) as string;
    } catch {
      return text;
    }
  }
  if (text.startsWith("'") && text.endsWith("'") && text.length >= 2) return text.slice(1, -1).replace(/''/g, "'");
  return text;
}

/** Block YAML as Alchemy prints it: `key: value`, `key:` with a deeper block, `- item` lists (of scalars or maps). */
function parsePlanYaml(items: { indent: number; text: string }[], at: { i: number }, indent: number): PlanValue {
  if (items[at.i]!.text.startsWith("- ") || items[at.i]!.text === "-") {
    const list: PlanValue[] = [];
    while (at.i < items.length && items[at.i]!.indent === indent && items[at.i]!.text.startsWith("- ")) {
      const rest = items[at.i]!.text.slice(2);
      if (/^[^"'][^:]*(?::[^ ][^:]*)*:(?: |$)/.test(rest)) {
        // A map item: its first key sits after the dash, the rest at the dash's indent + 2.
        items[at.i] = { indent: indent + 2, text: rest };
        list.push(parsePlanYaml(items, at, indent + 2));
      } else {
        list.push(planScalar(rest));
        at.i++;
      }
    }
    return list;
  }
  const map: { [key: string]: PlanValue } = {};
  while (at.i < items.length && items[at.i]!.indent === indent && !items[at.i]!.text.startsWith("- ")) {
    const text = items[at.i]!.text;
    at.i++;
    const nested = text.endsWith(":") && !text.includes(": ");
    if (nested) {
      const key = text.slice(0, -1);
      const next = items[at.i];
      map[key] = next && (next.indent > indent || (next.indent === indent && next.text.startsWith("- "))) ? parsePlanYaml(items, at, next.indent) : "";
      continue;
    }
    const split = text.indexOf(": ");
    if (split < 0) return map;
    map[text.slice(0, split)] = planScalar(text.slice(split + 2));
  }
  return map;
}

/**
 * Where `actual` (a parsed detailed-plan value) differs from `expected` (rendered props, whose
 * scalars are compared as printed strings; a function is a predicate on the printed value), or
 * undefined when it is exactly that.
 */
function planMismatch(actual: PlanValue | undefined, expected: unknown, path: string): string | undefined {
  if (typeof expected === "function") return typeof actual === "string" && (expected as (v: string) => boolean)(actual) ? undefined : `${path || "value"}`;
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length) return `${path || "value"} length`;
    for (let n = 0; n < expected.length; n++) {
      const d = planMismatch(actual[n], expected[n], `${path}[${n}]`);
      if (d) return d;
    }
    return undefined;
  }
  if (expected !== null && typeof expected === "object") {
    if (actual === undefined || typeof actual !== "object" || Array.isArray(actual)) return `${path || "value"} is not a map`;
    const want = Object.entries(expected as Record<string, unknown>).filter(([, v]) => v !== undefined);
    const keys = new Set(want.map(([k]) => k));
    for (const k of Object.keys(actual)) if (!keys.has(k)) return `${path ? `${path}.` : ""}${k} is not rendered`;
    for (const [k, v] of want) {
      const d = planMismatch(actual[k], v, path ? `${path}.${k}` : k);
      if (d) return d;
    }
    return undefined;
  }
  return actual === String(expected) ? undefined : `${path || "value"}`;
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
  // The search queue's policy names the same role (sessionSearchQueuePolicy), so it cascades too.
  const updated = [...ROLE_GRANT_RESOURCES, ...SEARCH_ROLE_GRANT_RESOURCES].filter((r) => operations.get(r) === "update");
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
