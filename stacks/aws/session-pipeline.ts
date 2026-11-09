/**
 * The sessions-prod session pipeline: a manifest landing in the transcript
 * bucket upserts its Supabase pointer row and indexes its events into
 * ClickHouse (`transcript_events`). Declared inside the `aws-sessions-prod`
 * transcript-store stage, opt-in with `transcriptStore.sessionPipeline`.
 *
 *   transcripts bucket --(S3 event notification, manifest.json only)--> SQS session-events
 *       session-events --(event source mapping, batch 10, partial failures)--> Lambda session-pipeline
 *       session-events --(after maxReceiveCount)--> SQS session-events-dlq (14 days, alarmed)
 *       EventBridge rule (every reconcileMinutes) --> Lambda {"reconcile":true} --> session-events
 *       (a reconcile at its deadline invokes itself asynchronously with the listing cursor)
 *       EventBridge rule (every minute, ENABLED only with briefs.enabled) --> Lambda {"brief":true}
 *       (one session-brief tick: src/session-brief/worker.ts briefTick)
 *       EventBridge rule (every minute, ENABLED only with bufferedSearchFeed.enabled) --> Lambda {"bufferedSearchFeed":true}
 *       (one bounded buffered-stage feed run: src/session-pipeline/buffered-search-feed.ts)
 *       EventBridge rule (every minute, ENABLED only with bufferedErasureDelete.enabled) --> Lambda {"bufferedErasureDelete":true}
 *       (one bounded later-erasure delete pass over buffered rows: src/session-pipeline/buffered-erasure-delete.ts)
 *
 * The Lambda (src/session-pipeline/lambda.ts) runs the pipeline's own
 * object-landed + session-indexed handlers (Supabase `upsert_session_index`,
 * revision-fenced), then the ClickHouse scope reconcile.
 *
 * Permissions are resource-based, because Alchemy owns the function's
 * execution role and attaches no custom policy to it: the transcripts key
 * policy lets the role decrypt via S3, the bucket policy lets it list and read
 * `96bulls/*\/v1/*`, the queue policy lets it consume and re-enqueue, and the
 * secret's resource policy lets it read the one pipeline secret, and the
 * function's own resource policy lets it invoke itself (the reconcile chain). The role gets
 * nothing else (no put, no delete, no old versions, no other secret).
 *
 * The bucket notification itself is not an Alchemy resource in this provider
 * version (only function bindings can declare one), so it is applied with the
 * checked-in `scripts/sessions-prod-bucket-notification.sh`, from
 * `bucketNotificationConfiguration` here; Alchemy's bucket reconcile keeps
 * notification targets it does not manage.
 *
 * Pure functions are unit-tested in stacks/aws/__tests__/session-pipeline.test.ts.
 */
import * as AWS from "@alchemy.run/aws";
import * as Effect from "effect/Effect";
import { LAYOUT_VERSION } from "../../packages/object-store/src/layout.ts";
import { TENANT_KEY_BASE } from "./tenants.ts";
import{derivedSchemaBuildPlugin}from "./derived-schema-build.ts";

export interface SessionPipelineInputs {
  /** Turn the ClickHouse stage on (SEARCH_INDEX_ENABLED=1). Off: pointers only. */
  searchIndex?: boolean;
  /** Default off; source identity must match the immutable CH copier binding. */
  semanticCopy?: {enabled:boolean;sourceId:string};
  /**
   * Default off: the schedule stays DISABLED and neither feed variable is set. `enabled: true` must
   * name the read gate's exact contract in `gateDeclared`, an operator's assertion that the read
   * Worker already runs that gate, and needs `searchIndex: true`. The feeder's login is not wired
   * here: it is read from the pipeline secret at run time, and without a valid one the feed stays off.
   */
  bufferedSearchFeed?: {
    enabled: boolean;
    gateDeclared?: string;
    /** Stages per feed page (INGEST_BUFFERED_SEARCH_FEED_PAGE, 1-200). Absent: not set, so the feeder's default (50) applies. */
    pageSize?: number;
    /** Stages per second (INGEST_BUFFERED_SEARCH_FEED_RATE, 1-50). Absent: not set, so the feeder's default (5) applies. */
    stagesPerSecond?: number;
    /** Per-run time budget in ms (INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS, 5000-240000). Absent: not set, so the feeder's default (50000) applies. */
    budgetMs?: number;
    /**
     * `true` sets INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION=on, the S version encoding the feeder requires
     * before `enabled` can run (otherwise it refuses `version_encoding_not_armed`). Absent or `false`: not set,
     * so the feeder's default (off) applies. Allowed independently of `enabled`: the feeder is its only reader,
     * and with the feed off it only validates the literal (the erasure lane never reads it).
     */
    secondVersion?: boolean;
    /** Settle window in seconds (INGEST_BUFFERED_SEARCH_FEED_SETTLE_S, 180-3600). Absent: not set, so the feeder's default (300) applies. */
    settleSeconds?: number;
  };
  /**
   * The buffered-row later-erasure delete lane (src/session-pipeline/buffered-erasure-delete.ts). Default off:
   * its schedule stays DISABLED and INGEST_BUFFERED_ERASURE_DELETE is not set. `enabled: true` needs
   * `bufferedSearchFeed.gateDeclared` (the runtime applies the feeder's ordering guard) and `searchIndex: true`
   * (it deletes ClickHouse rows); it then also sets BUFFERED_SEARCH_FEED_GATE_DECLARED, even with the feed off.
   * It uses the feeder's login from the pipeline secret; without a valid one it stays off.
   */
  bufferedErasureDelete?: {
    enabled: boolean;
    /** (claim, stage) pairs per ClickHouse page (INGEST_BUFFERED_ERASURE_PAGE, 1-200). Absent: the lane's default (25) applies. */
    pageSize?: number;
    /** Stage checks per second (INGEST_BUFFERED_ERASURE_RATE, 1-20). Absent: the lane's default (2) applies. */
    stagesPerSecond?: number;
    /** Per-run time budget in ms (INGEST_BUFFERED_ERASURE_BUDGET_MS, 5000-240000). Absent: the lane's default (40000) applies. */
    budgetMs?: number;
  };
  /** Default off. `true` is refused until the reviewed normalized-prefix PutObject/KMS grant and publication migration exist. */
  replayPages?: boolean;
  /** Dormant full v1 analysis-cache worker; separate readonly source secret required. */
  analysisCache?: boolean;
  /**
   * Default off. `true` is refused until the forget/reproject fix (migration 20261105031000)
   * lands and is reviewed (docs/session-desktop-catalog.md); SESSION_DESKTOP_CATALOG_ENABLED stays "0".
   */
  desktopCatalog?: boolean;
  /** Lambda concurrency cap from the queue (2-10). Default 4. */
  maxConcurrency?: number;
  /** Reconcile cadence in minutes (15-120). Default 30; lookback is twice this. */
  reconcileMinutes?: number;
  /** Session briefs (src/session-brief). Absent or `enabled: false`: off. */
  briefs?: SessionBriefInputs;
}

/**
 * `enabled: true` sets SESSION_BRIEF_ENABLED=true on the pipeline function (indexing
 * arms brief wakeups) and enables the brief schedule (each tick briefs due sessions).
 * Model calls also need `openai_api_key` in the pipeline secret; without it the tick
 * claims nothing. The caps default to src/session-brief/config.ts.
 */
export interface SessionBriefInputs {
  enabled: boolean;
  /** Distinct sessions briefed per org per UTC day (SESSION_BRIEF_BACKGROUND_DAILY_BUDGET). */
  backgroundDailyBudget?: number;
  /** Model attempts across all orgs per UTC day (SESSION_BRIEF_GLOBAL_DAILY_CAP). */
  globalDailyCap?: number;
  /**
   * How often the brief tick queues indexed sessions that have no brief at all
   * (SESSION_BRIEF_SWEEP_SECONDS; store.ts sweepUnbriefed, 100 per sweep). Absent: not set,
   * so the worker's default (0, off) applies. History that never receives new evidence is
   * briefed only by this sweep; new evidence arms its own wakeup through indexing.
   */
  sweepSeconds?: number;
}

export const SESSION_PIPELINE_RESOURCES = [
  "session-pipeline",
  "session-events-dlq",
  "session-events",
  "session-pipeline-secret",
  "session-events-mapping",
  "session-reconcile-schedule",
  "session-reconcile-permission",
  "session-reconcile-continue",
  "session-events-dlq-alarm",
  "session-events-age-alarm",
  "session-pipeline-errors-alarm",
] as const;

/**
 * The brief schedule's resources. Listed apart from SESSION_PIPELINE_RESOURCES so plans
 * captured before briefs existed still parse; once any appears, all must
 * (transcript-store.ts expectedResources).
 */
export const SESSION_BRIEF_RESOURCES = ["session-brief-schedule", "session-brief-permission"] as const;
export const SESSION_SEMANTIC_COPY_RESOURCES=["session-semantic-copy-schedule","session-semantic-copy-permission"] as const;
export const SESSION_BUFFERED_SEARCH_FEED_RESOURCES = ["session-buffered-search-feed-schedule", "session-buffered-search-feed-permission"] as const;
export const SESSION_BUFFERED_ERASURE_DELETE_RESOURCES = ["session-buffered-erasure-delete-schedule", "session-buffered-erasure-delete-permission"] as const;
/** The read gate's contract id; a test pins it to BUFFERED_SEARCH_GATE_CONTRACT in the gate source. */
export const BUFFERED_SEARCH_FEED_GATE_CONTRACT = "buffered-search-gate/v1";
/** The brief tick runs this often. */
export const BRIEF_SCHEDULE_MINUTES = 1;
export const SESSION_ANALYSIS_CACHE_RESOURCES=["session-analysis-cache-schedule","session-analysis-cache-permission","session-analysis-reconcile-schedule","session-analysis-reconcile-permission"] as const;

/** Function timeout; the queue's visibility timeout is six times this (AWS guidance for SQS sources). */
export const FUNCTION_TIMEOUT_SECONDS = 300;
export const VISIBILITY_TIMEOUT_SECONDS = 6 * FUNCTION_TIMEOUT_SECONDS;
/** Receives before a message moves to the DLQ. */
export const MAX_RECEIVE_COUNT = 8;
/** S3 events wait this long before the first delivery, so a burst of manifest writes coalesces. */
export const DELIVERY_DELAY_SECONDS = 30;

export interface SessionPipelineNames {
  function: string;
  queue: string;
  dlq: string;
  queueArn: string;
  dlqArn: string;
  queueUrl: string;
  secret: string;
  rule: string;
  briefRule: string;
  semanticRule:string;
  bufferedSearchFeedRule: string;
  bufferedErasureDeleteRule: string;
}

export function sessionPipelineNames(stage: string, accountId: string, region: string): SessionPipelineNames {
  const n = (base: string) => `96bulls-${stage}-${base}`;
  return {
    function: n("session-pipeline"),
    queue: n("session-events"),
    dlq: n("session-events-dlq"),
    queueArn: `arn:aws:sqs:${region}:${accountId}:${n("session-events")}`,
    dlqArn: `arn:aws:sqs:${region}:${accountId}:${n("session-events-dlq")}`,
    queueUrl: `https://sqs.${region}.amazonaws.com/${accountId}/${n("session-events")}`,
    secret: `96bulls/${stage}/session-pipeline`,
    rule: n("session-reconcile"),
    briefRule: n("session-brief"),
    semanticRule:n("session-semantic-copy"),
    bufferedSearchFeedRule: n("session-buffered-search-feed"),
    bufferedErasureDeleteRule: n("session-buffered-erasure-delete"),
  };
}

export function assertSessionPipelineInputs(p: unknown, refuse: (why: string) => never): asserts p is SessionPipelineInputs {
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.getPrototypeOf(p) !== Object.prototype)
    refuse("transcriptStore.sessionPipeline must be a plain object");
  const v = p as Record<string, unknown>;
  for (const k of Object.keys(v))
    if (k !== "searchIndex" && k !== "replayPages" && k!=="analysisCache" && k !== "desktopCatalog" && k !== "maxConcurrency" && k !== "reconcileMinutes" && k !== "briefs" && k !== "semanticCopy" && k !== "bufferedSearchFeed" && k !== "bufferedErasureDelete") refuse(`transcriptStore.sessionPipeline.${k} is not a known setting`);
  if (v.searchIndex !== undefined && typeof v.searchIndex !== "boolean") refuse("transcriptStore.sessionPipeline.searchIndex must be a boolean");
  if (v.replayPages !== undefined && typeof v.replayPages !== "boolean") refuse("transcriptStore.sessionPipeline.replayPages must be a boolean");
  if(v.analysisCache!==undefined&&typeof v.analysisCache!=="boolean")refuse("transcriptStore.sessionPipeline.analysisCache must be a boolean");
  // The pipeline role has only s3:ListBucket/GetObject: enabling the stage now would fail every replay put.
  if (v.replayPages === true) refuse("transcriptStore.sessionPipeline.replayPages requires the reviewed normalized-prefix PutObject/KMS grant; not wired yet");
  // Same gap for the analysis cache: publications are normalized-prefix S3 writes the role cannot make yet.
  if (v.analysisCache === true) refuse("transcriptStore.sessionPipeline.analysisCache requires the reviewed normalized-prefix PutObject/KMS grant; not wired yet");
  if (v.desktopCatalog !== undefined && typeof v.desktopCatalog !== "boolean") refuse("transcriptStore.sessionPipeline.desktopCatalog must be a boolean");
  // SEARCH_INDEX_ENABLED=1 must never activate the desktop catalog writer; its own flag waits on 031000.
  if (v.desktopCatalog === true) refuse("transcriptStore.sessionPipeline.desktopCatalog requires the forget/reproject fix (migration 20261105031000) and its review; not wired yet");
  if (v.maxConcurrency !== undefined && !(Number.isInteger(v.maxConcurrency) && (v.maxConcurrency as number) >= 2 && (v.maxConcurrency as number) <= 10))
    refuse("transcriptStore.sessionPipeline.maxConcurrency must be an integer from 2 to 10");
  if (v.reconcileMinutes !== undefined && !(Number.isInteger(v.reconcileMinutes) && (v.reconcileMinutes as number) >= 15 && (v.reconcileMinutes as number) <= 120))
    refuse("transcriptStore.sessionPipeline.reconcileMinutes must be an integer from 15 to 120");
  if(v.semanticCopy!==undefined){const c=v.semanticCopy as Record<string,unknown>|null;
    if(!c||typeof c!=="object"||Array.isArray(c)||Object.getPrototypeOf(c)!==Object.prototype||Object.keys(c).some(k=>k!=="enabled"&&k!=="sourceId")||typeof c.enabled!=="boolean"||typeof c.sourceId!=="string"||!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(c.sourceId))refuse("transcriptStore.sessionPipeline.semanticCopy requires enabled and an explicit sourceId");
  }
  if (v.bufferedSearchFeed !== undefined) {
    const f = v.bufferedSearchFeed as Record<string, unknown> | null;
    if (f === null || typeof f !== "object" || Array.isArray(f) || Object.getPrototypeOf(f) !== Object.prototype)
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed must be a plain object");
    for (const k of Object.keys(f))
      if (k !== "enabled" && k !== "gateDeclared" && k !== "pageSize" && k !== "stagesPerSecond" && k !== "budgetMs" && k !== "secondVersion" && k !== "settleSeconds") refuse(`transcriptStore.sessionPipeline.bufferedSearchFeed.${k} is not a known setting`);
    if (typeof f.enabled !== "boolean") refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.enabled must be a boolean");
    if (f.gateDeclared !== undefined && f.gateDeclared !== BUFFERED_SEARCH_FEED_GATE_CONTRACT)
      refuse(`transcriptStore.sessionPipeline.bufferedSearchFeed.gateDeclared must be exactly ${BUFFERED_SEARCH_FEED_GATE_CONTRACT}`);
    if (f.enabled === true && f.gateDeclared !== BUFFERED_SEARCH_FEED_GATE_CONTRACT)
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.enabled requires gateDeclared: deploy and verify the read gate first");
    if (f.enabled === true && v.searchIndex !== true)
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.enabled requires searchIndex: true");
    // The same bounds as src/session-pipeline/buffered-search-feed.ts, so a value here is never refused at run time.
    if (f.pageSize !== undefined && !(Number.isInteger(f.pageSize) && (f.pageSize as number) >= 1 && (f.pageSize as number) <= 200))
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.pageSize must be an integer from 1 to 200");
    if (f.stagesPerSecond !== undefined && !(Number.isInteger(f.stagesPerSecond) && (f.stagesPerSecond as number) >= 1 && (f.stagesPerSecond as number) <= 50))
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.stagesPerSecond must be an integer from 1 to 50");
    if (f.budgetMs !== undefined && !(Number.isInteger(f.budgetMs) && (f.budgetMs as number) >= 5_000 && (f.budgetMs as number) <= 240_000))
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.budgetMs must be an integer from 5000 to 240000");
    if (f.secondVersion !== undefined && typeof f.secondVersion !== "boolean") refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.secondVersion must be a boolean");
    if (f.settleSeconds !== undefined && !(Number.isInteger(f.settleSeconds) && (f.settleSeconds as number) >= 180 && (f.settleSeconds as number) <= 3_600))
      refuse("transcriptStore.sessionPipeline.bufferedSearchFeed.settleSeconds must be an integer from 180 to 3600");
  }
  if (v.bufferedErasureDelete !== undefined) {
    const e = v.bufferedErasureDelete as Record<string, unknown> | null;
    if (e === null || typeof e !== "object" || Array.isArray(e) || Object.getPrototypeOf(e) !== Object.prototype)
      refuse("transcriptStore.sessionPipeline.bufferedErasureDelete must be a plain object");
    for (const k of Object.keys(e))
      if (k !== "enabled" && k !== "pageSize" && k !== "stagesPerSecond" && k !== "budgetMs") refuse(`transcriptStore.sessionPipeline.bufferedErasureDelete.${k} is not a known setting`);
    if (typeof e.enabled !== "boolean") refuse("transcriptStore.sessionPipeline.bufferedErasureDelete.enabled must be a boolean");
    // The runtime applies the feeder's ordering guard (bufferedFeedGateRefusal) to this lane too.
    if (e.enabled === true && (v.bufferedSearchFeed as { gateDeclared?: unknown } | undefined)?.gateDeclared !== BUFFERED_SEARCH_FEED_GATE_CONTRACT)
      refuse("transcriptStore.sessionPipeline.bufferedErasureDelete.enabled requires bufferedSearchFeed.gateDeclared: deploy and verify the read gate first");
    if (e.enabled === true && v.searchIndex !== true)
      refuse("transcriptStore.sessionPipeline.bufferedErasureDelete.enabled requires searchIndex: true");
    // The same bounds as src/session-pipeline/buffered-erasure-delete.ts, so a value here is never refused at run time.
    if (e.pageSize !== undefined && !(Number.isInteger(e.pageSize) && (e.pageSize as number) >= 1 && (e.pageSize as number) <= 200))
      refuse("transcriptStore.sessionPipeline.bufferedErasureDelete.pageSize must be an integer from 1 to 200");
    if (e.stagesPerSecond !== undefined && !(Number.isInteger(e.stagesPerSecond) && (e.stagesPerSecond as number) >= 1 && (e.stagesPerSecond as number) <= 20))
      refuse("transcriptStore.sessionPipeline.bufferedErasureDelete.stagesPerSecond must be an integer from 1 to 20");
    if (e.budgetMs !== undefined && !(Number.isInteger(e.budgetMs) && (e.budgetMs as number) >= 5_000 && (e.budgetMs as number) <= 240_000))
      refuse("transcriptStore.sessionPipeline.bufferedErasureDelete.budgetMs must be an integer from 5000 to 240000");
  }
  if (v.briefs !== undefined) {
    const b = v.briefs as Record<string, unknown> | null;
    if (b === null || typeof b !== "object" || Array.isArray(b) || Object.getPrototypeOf(b) !== Object.prototype)
      refuse("transcriptStore.sessionPipeline.briefs must be a plain object");
    for (const k of Object.keys(b))
      if (k !== "enabled" && k !== "backgroundDailyBudget" && k !== "globalDailyCap" && k !== "sweepSeconds") refuse(`transcriptStore.sessionPipeline.briefs.${k} is not a known setting`);
    if (typeof b.enabled !== "boolean") refuse("transcriptStore.sessionPipeline.briefs.enabled must be a boolean");
    // The same bounds as src/session-brief/config.ts, so a value here is never silently clamped.
    if (b.backgroundDailyBudget !== undefined && !(Number.isInteger(b.backgroundDailyBudget) && (b.backgroundDailyBudget as number) >= 0 && (b.backgroundDailyBudget as number) <= 1_000_000))
      refuse("transcriptStore.sessionPipeline.briefs.backgroundDailyBudget must be an integer from 0 to 1000000");
    if (b.globalDailyCap !== undefined && !(Number.isInteger(b.globalDailyCap) && (b.globalDailyCap as number) >= 0 && (b.globalDailyCap as number) <= 10_000_000))
      refuse("transcriptStore.sessionPipeline.briefs.globalDailyCap must be an integer from 0 to 10000000");
    // src/session-brief/config.ts bounds SESSION_BRIEF_SWEEP_SECONDS to 0-86400 (0 = off).
    if (b.sweepSeconds !== undefined && !(Number.isInteger(b.sweepSeconds) && (b.sweepSeconds as number) >= 0 && (b.sweepSeconds as number) <= 86_400))
      refuse("transcriptStore.sessionPipeline.briefs.sweepSeconds must be an integer from 0 to 86400");
  }
}

/** The pipeline role reads session objects through S3 only, for objects under `96bulls/` (as the reader user). */
export function pipelineKeyStatement(i: { accountId: string; region: string; bucketName: string; roleArn: string }) {
  return {
    Sid: "SessionPipelineDecryptsViaS3",
    Effect: "Allow" as const,
    Principal: { AWS: i.roleArn },
    Action: ["kms:Decrypt"],
    Resource: "*",
    Condition: {
      StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
      StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${TENANT_KEY_BASE}/*` },
    },
  };
}

/** Bucket-policy grants for the pipeline role: list keys, read session objects. Nothing else. */
export function pipelineBucketStatements(i: { bucketArn: string; roleArn: string }) {
  return [
    {
      Sid: "SessionPipelineLists",
      Effect: "Allow" as const,
      Principal: { AWS: i.roleArn },
      Action: ["s3:ListBucket"],
      // Unconditional, as for the reader user: S3 answers 404 (not 403) for a
      // missing manifest only with ListBucket, and a delete is detected that way.
      Resource: [i.bucketArn],
    },
    {
      Sid: "SessionPipelineReads",
      Effect: "Allow" as const,
      Principal: { AWS: i.roleArn },
      Action: ["s3:GetObject"],
      Resource: [`${i.bucketArn}/${TENANT_KEY_BASE}/*/${LAYOUT_VERSION}/*`],
    },
  ];
}

/** S3 may send this bucket's events; the pipeline role may consume and re-enqueue (reconcile). TLS only. */
export function sessionQueuePolicy(i: { queueArn: string; bucketArn: string; accountId: string; roleArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "DenyInsecureTransport",
        Effect: "Deny" as const,
        Principal: "*",
        Action: "sqs:*",
        Resource: i.queueArn,
        Condition: { Bool: { "aws:SecureTransport": "false" } },
      },
      {
        Sid: "TranscriptBucketEvents",
        Effect: "Allow" as const,
        Principal: { Service: "s3.amazonaws.com" },
        Action: "sqs:SendMessage",
        Resource: i.queueArn,
        Condition: { ArnEquals: { "aws:SourceArn": i.bucketArn }, StringEquals: { "aws:SourceAccount": i.accountId } },
      },
      {
        Sid: "SessionPipelineConsumes",
        Effect: "Allow" as const,
        Principal: { AWS: i.roleArn },
        Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:SendMessage"],
        Resource: i.queueArn,
      },
    ],
  };
}

export function sessionDlqPolicy(i: { dlqArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Sid: "DenyInsecureTransport",
        Effect: "Deny" as const,
        Principal: "*",
        Action: "sqs:*",
        Resource: i.dlqArn,
        Condition: { Bool: { "aws:SecureTransport": "false" } },
      },
    ],
  };
}

/** Only the pipeline role reads the pipeline secret. */
export function sessionSecretPolicy(i: { roleArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "SessionPipelineReads", Effect: "Allow" as const, Principal: { AWS: i.roleArn }, Action: "secretsmanager:GetSecretValue", Resource: "*" },
    ],
  };
}

/**
 * The bucket's notification configuration: every manifest put or delete under
 * `96bulls/` to the queue. Chunks are never notified (the suffix filter), so
 * the queue carries one message per manifest write.
 */
export function bucketNotificationConfiguration(queueArn: string) {
  return {
    QueueConfigurations: [
      {
        Id: "96bulls-session-pipeline-manifests",
        QueueArn: queueArn,
        Events: ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
        Filter: { Key: { FilterRules: [{ Name: "prefix", Value: `${TENANT_KEY_BASE}/` }, { Name: "suffix", Value: "/manifest.json" }] } },
      },
    ],
  };
}

export function sessionPipelineEnv(i: { stage: string; bucket: string; names: SessionPipelineNames; p: SessionPipelineInputs }) {
  return {
    DEPLOYMENT: i.stage,
    DEPLOYMENT_ID: i.stage,
    OTEL_SERVICE_NAME: "96bulls-session-pipeline",
    PIPELINE_RAW_S3_BUCKET: i.bucket,
    PIPELINE_KEY_PREFIX: TENANT_KEY_BASE,
    PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: i.names.secret,
    SESSION_PIPELINE_QUEUE_URL: i.names.queueUrl,
    SEARCH_INDEX_ENABLED: i.p.searchIndex ? "1" : "0",
    SESSION_REPLAY_PAGES_ENABLED: i.p.replayPages ? "1" : "0",
    SESSION_ANALYSIS_CACHE_ENABLED:i.p.analysisCache?"1":"0",
    // Always "0" while desktopCatalog: true is refused; lifting that refusal is a reviewed PR.
    SESSION_DESKTOP_CATALOG_ENABLED: "0",
    SESSION_SEMANTIC_COPY_ENABLED:i.p.semanticCopy?.enabled?"1":"0",
    SESSION_SEARCH_FEATURE_SOURCE_ID:i.p.semanticCopy?.sourceId??"",
    // Only when armed, so a default deploy leaves the feed and the erasure lane at their source default of off.
    ...(i.p.bufferedSearchFeed?.enabled === true ? { INGEST_BUFFERED_SEARCH_FEED: "on" } : {}),
    // Both lanes apply the same ordering guard: declared once when either is armed.
    ...(i.p.bufferedSearchFeed?.enabled === true || i.p.bufferedErasureDelete?.enabled === true ? { BUFFERED_SEARCH_FEED_GATE_DECLARED: i.p.bufferedSearchFeed?.gateDeclared ?? "" } : {}),
    ...(i.p.bufferedSearchFeed?.pageSize !== undefined ? { INGEST_BUFFERED_SEARCH_FEED_PAGE: String(i.p.bufferedSearchFeed.pageSize) } : {}),
    ...(i.p.bufferedSearchFeed?.stagesPerSecond !== undefined ? { INGEST_BUFFERED_SEARCH_FEED_RATE: String(i.p.bufferedSearchFeed.stagesPerSecond) } : {}),
    ...(i.p.bufferedSearchFeed?.budgetMs !== undefined ? { INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS: String(i.p.bufferedSearchFeed.budgetMs) } : {}),
    ...(i.p.bufferedSearchFeed?.secondVersion === true ? { INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION: "on" } : {}),
    ...(i.p.bufferedSearchFeed?.settleSeconds !== undefined ? { INGEST_BUFFERED_SEARCH_FEED_SETTLE_S: String(i.p.bufferedSearchFeed.settleSeconds) } : {}),
    ...(i.p.bufferedErasureDelete?.enabled === true ? { INGEST_BUFFERED_ERASURE_DELETE: "on" } : {}),
    ...(i.p.bufferedErasureDelete?.pageSize !== undefined ? { INGEST_BUFFERED_ERASURE_PAGE: String(i.p.bufferedErasureDelete.pageSize) } : {}),
    ...(i.p.bufferedErasureDelete?.stagesPerSecond !== undefined ? { INGEST_BUFFERED_ERASURE_RATE: String(i.p.bufferedErasureDelete.stagesPerSecond) } : {}),
    ...(i.p.bufferedErasureDelete?.budgetMs !== undefined ? { INGEST_BUFFERED_ERASURE_BUDGET_MS: String(i.p.bufferedErasureDelete.budgetMs) } : {}),
    PIPELINE_FANOUT: "off",
    PIPELINE_RECONCILE_LOOKBACK_MINUTES: String(2 * (i.p.reconcileMinutes ?? 30)),
    HOOPDB_POOL_MAX: "4",
    NODE_OPTIONS: "--enable-source-maps",
    SESSION_BRIEF_ENABLED: i.p.briefs?.enabled ? "true" : "false",
    ...(i.p.briefs?.backgroundDailyBudget !== undefined ? { SESSION_BRIEF_BACKGROUND_DAILY_BUDGET: String(i.p.briefs.backgroundDailyBudget) } : {}),
    ...(i.p.briefs?.globalDailyCap !== undefined ? { SESSION_BRIEF_GLOBAL_DAILY_CAP: String(i.p.briefs.globalDailyCap) } : {}),
    ...(i.p.briefs?.sweepSeconds !== undefined ? { SESSION_BRIEF_SWEEP_SECONDS: String(i.p.briefs.sweepSeconds) } : {}),
  };
}

export interface SessionPipelineStageInputs {
  stage: string;
  accountId: string;
  region: string;
  p: SessionPipelineInputs;
  tags: (component: string) => Record<string, string>;
}

/**
 * The pipeline function's props. `functionUrl: false` is load-bearing: Alchemy's
 * `Lambda.Function` defaults to a public Function URL (`authType: "NONE"`, plus a
 * resource-policy grant of the URL invoke action and `lambda:InvokeFunction`
 * to `*`). The pipeline is invoked only by SQS, EventBridge and its own role
 * (the reconcile chain calls the Lambda Invoke API with SigV4, never a URL), so
 * it has no URL at all.
 */
export function sessionPipelineFunctionProps(i: SessionPipelineStageInputs & { bucket: string }) {
  const names = sessionPipelineNames(i.stage, i.accountId, i.region);
  return {
    functionName: names.function,
    // Relative to the repository root (bin/96bulls runs Alchemy there), so the planned
    // props are the same in every checkout and a capture never diffs on a path.
    main: "src/session-pipeline/lambda.ts",
    handler: "handler",
    runtime: "nodejs24.x",
    architecture: "arm64",
    memorySize: 3008,
    timeout: `${FUNCTION_TIMEOUT_SECONDS} seconds`,
    env: sessionPipelineEnv({ stage: i.stage, bucket: i.bucket, names, p: i.p }),
    logging: { retention: "30 days" },
    functionUrl: false as const,
    build: { tsconfig: "tsconfig.json",plugins:[derivedSchemaBuildPlugin()] },
  };
}

/**
 * Every resource-policy grant on the pipeline function: EventBridge (the reconcile
 * and brief rules, this account) and the function's own role (the reconcile chain),
 * all `lambda:InvokeFunction` only. Nothing public, no Function URL action.
 */
export function sessionPipelinePermissions(i: { accountId: string; functionName: string; ruleArn: string; briefRuleArn: string; analysisRuleArn?:string; analysisReconcileRuleArn?:string; semanticRuleArn?:string; bufferedSearchFeedRuleArn?: string; bufferedErasureDeleteRuleArn?: string; roleArn: string }) {
  return [
    {
      id: "session-reconcile-permission",
      props: {
        action: "lambda:InvokeFunction",
        functionName: i.functionName,
        principal: "events.amazonaws.com",
        sourceArn: i.ruleArn,
        sourceAccount: i.accountId,
      },
    },
    // A reconcile that reaches its deadline hands its cursor to the next link by invoking
    // this function asynchronously (Lambda Invoke API, SigV4); only the function's own role may do that.
    {
      id: "session-reconcile-continue",
      props: {
        action: "lambda:InvokeFunction",
        functionName: i.functionName,
        principal: i.roleArn,
      },
    },
    {
      id: "session-brief-permission",
      props: {
        action: "lambda:InvokeFunction",
        functionName: i.functionName,
        principal: "events.amazonaws.com",
        sourceArn: i.briefRuleArn,
        sourceAccount: i.accountId,
      },
    },
    ...(i.analysisReconcileRuleArn?[{id:"session-analysis-reconcile-permission",props:{action:"lambda:InvokeFunction",functionName:i.functionName,principal:"events.amazonaws.com",sourceArn:i.analysisReconcileRuleArn,sourceAccount:i.accountId}}]:[]),
    ...(i.analysisRuleArn?[{id:"session-analysis-cache-permission",props:{action:"lambda:InvokeFunction",functionName:i.functionName,principal:"events.amazonaws.com",sourceArn:i.analysisRuleArn,sourceAccount:i.accountId}}]:[]),
    ...(i.semanticRuleArn?[{id:"session-semantic-copy-permission",props:{action:"lambda:InvokeFunction",functionName:i.functionName,principal:"events.amazonaws.com",sourceArn:i.semanticRuleArn,sourceAccount:i.accountId}}]:[]),
    ...(i.bufferedSearchFeedRuleArn ? [{ id: "session-buffered-search-feed-permission", props: { action: "lambda:InvokeFunction", functionName: i.functionName, principal: "events.amazonaws.com", sourceArn: i.bufferedSearchFeedRuleArn, sourceAccount: i.accountId } }] : []),
    ...(i.bufferedErasureDeleteRuleArn ? [{ id: "session-buffered-erasure-delete-permission", props: { action: "lambda:InvokeFunction", functionName: i.functionName, principal: "events.amazonaws.com", sourceArn: i.bufferedErasureDeleteRuleArn, sourceAccount: i.accountId } }] : []),
  ] as const;
}

/**
 * The brief schedule: DISABLED unless `briefs.enabled`, so nothing is invoked while briefs
 * are off. The function also returns before any work unless SESSION_BRIEF_ENABLED is "true".
 */
export function briefScheduleProps(i: { names: SessionPipelineNames; functionArn: string; p: SessionPipelineInputs; tags: Record<string, string> }) {
  return {
    name: i.names.briefRule,
    description: "96bulls session pipeline: one session-brief tick (due wakeups)",
    scheduleExpression: `rate(${BRIEF_SCHEDULE_MINUTES} minute)`,
    state: i.p.briefs?.enabled ? ("ENABLED" as const) : ("DISABLED" as const),
    targets: [{ Id: "session-pipeline", Arn: i.functionArn, Input: JSON.stringify({ brief: true }) }],
    tags: i.tags,
  };
}

/** Disabled by default; source definition alone does not enable a running rule. */
export function analysisCacheScheduleProps(i:{names:SessionPipelineNames;functionArn:string;p:SessionPipelineInputs;tags:Record<string,string>}){
 return {name:i.names.rule+"-analysis-cache",description:"96bulls bounded demand analysis-cache refresh",scheduleExpression:"rate(1 minute)",state:i.p.analysisCache?"ENABLED" as const:"DISABLED" as const,targets:[{Id:"session-pipeline",Arn:i.functionArn,Input:JSON.stringify({analysisCache:true})}],tags:i.tags};
}

export function analysisReconcileScheduleProps(i:{names:SessionPipelineNames;functionArn:string;p:SessionPipelineInputs;tags:Record<string,string>}){
 return {name:i.names.rule+"-analysis-backfill",description:"96bulls bounded analysis-cache source discovery",scheduleExpression:"rate(30 minutes)",state:i.p.analysisCache?"ENABLED" as const:"DISABLED" as const,targets:[{Id:"session-pipeline",Arn:i.functionArn,Input:JSON.stringify({analysisCacheReconcile:true})}],tags:i.tags};
}

/** One bounded background feature tick; defaults disabled and carries no credentials. */
export function semanticCopyScheduleProps(i:{names:SessionPipelineNames;functionArn:string;p:SessionPipelineInputs;tags:Record<string,string>}){
 return {name:i.names.semanticRule,description:"96bulls verified global feature copy",scheduleExpression:"rate(1 minute)",state:i.p.semanticCopy?.enabled?("ENABLED" as const):("DISABLED" as const),targets:[{Id:"session-pipeline",Arn:i.functionArn,Input:JSON.stringify({semanticCopy:true})}],tags:i.tags};
}

/** One bounded buffered-stage feed run per minute; DISABLED by default and carries no credentials. */
export function bufferedSearchFeedScheduleProps(i: { names: SessionPipelineNames; functionArn: string; p: SessionPipelineInputs; tags: Record<string, string> }) {
  return {
    name: i.names.bufferedSearchFeedRule,
    description: "96bulls bounded buffered-stage search feed",
    scheduleExpression: "rate(1 minute)",
    state: i.p.bufferedSearchFeed?.enabled === true ? ("ENABLED" as const) : ("DISABLED" as const),
    targets: [{ Id: "session-pipeline", Arn: i.functionArn, Input: JSON.stringify({ bufferedSearchFeed: true }) }],
    tags: i.tags,
  };
}

/** One bounded buffered-row later-erasure delete pass per minute; DISABLED by default and carries no credentials. */
export function bufferedErasureDeleteScheduleProps(i: { names: SessionPipelineNames; functionArn: string; p: SessionPipelineInputs; tags: Record<string, string> }) {
  return {
    name: i.names.bufferedErasureDeleteRule,
    description: "96bulls bounded buffered-row later-erasure delete",
    scheduleExpression: "rate(1 minute)",
    state: i.p.bufferedErasureDelete?.enabled === true ? ("ENABLED" as const) : ("DISABLED" as const),
    targets: [{ Id: "session-pipeline", Arn: i.functionArn, Input: JSON.stringify({ bufferedErasureDelete: true }) }],
    tags: i.tags,
  };
}

/** Step 1, before the transcripts key and bucket: the function (and so its role) must exist first. */
export const sessionPipelineFunction = (i: SessionPipelineStageInputs & { bucket: string }) =>
  Effect.gen(function* () {
    return yield* AWS.Lambda.Function("session-pipeline", sessionPipelineFunctionProps(i) as never);
  });

/** Step 2, after the bucket: queues, secret, mapping, schedule, alarms. */
export const sessionPipelineWiring = (
  i: SessionPipelineStageInputs & { bucketArn: string; fn: { functionName: unknown; functionArn: unknown; roleArn: unknown } },
) =>
  Effect.gen(function* () {
    const names = sessionPipelineNames(i.stage, i.accountId, i.region);
    const roleArn = i.fn.roleArn as unknown as string;
    const dlq = yield* AWS.SQS.Queue("session-events-dlq", {
      queueName: names.dlq,
      messageRetentionPeriod: "14 days",
      sqsManagedSseEnabled: true,
      policy: sessionDlqPolicy({ dlqArn: names.dlqArn }),
      tags: i.tags("session-pipeline"),
    } as never);
    const queue = yield* AWS.SQS.Queue("session-events", {
      queueName: names.queue,
      delay: `${DELIVERY_DELAY_SECONDS} seconds`,
      visibilityTimeout: `${VISIBILITY_TIMEOUT_SECONDS} seconds`,
      messageRetentionPeriod: "4 days",
      receiveMessageWaitTime: "20 seconds",
      sqsManagedSseEnabled: true,
      redrivePolicy: { deadLetterTargetArn: dlq.queueArn as unknown as string, maxReceiveCount: MAX_RECEIVE_COUNT },
      policy: sessionQueuePolicy({ queueArn: names.queueArn, bucketArn: i.bucketArn, accountId: i.accountId, roleArn }),
      tags: i.tags("session-pipeline"),
    } as never);
    // A container only: the lead sets the value from 1Password (runbook). Alchemy state never holds it.
    const secret = yield* AWS.SecretsManager.Secret("session-pipeline-secret", {
      name: names.secret,
      description: "96bulls session pipeline: Supabase HoopDb URL and ClickHouse ingest credentials (JSON)",
      resourcePolicy: sessionSecretPolicy({ roleArn }),
      recoveryWindowInDays: 7,
      tags: i.tags("session-pipeline"),
    } as never);
    const mapping = yield* AWS.Lambda.EventSourceMapping("session-events-mapping", {
      functionName: i.fn.functionName as unknown as string,
      eventSourceArn: queue.queueArn as unknown as string,
      batchSize: 10,
      maximumBatchingWindow: "20 seconds",
      functionResponseTypes: ["ReportBatchItemFailures"],
      scalingConfig: { MaximumConcurrency: i.p.maxConcurrency ?? 4 },
      enabled: true,
      tags: i.tags("session-pipeline"),
    } as never);
    const rule = yield* AWS.EventBridge.Rule("session-reconcile-schedule", {
      name: names.rule,
      description: "96bulls session pipeline: reconcile recent manifests (lost notifications)",
      scheduleExpression: `rate(${i.p.reconcileMinutes ?? 30} minutes)`,
      targets: [{ Id: "session-pipeline", Arn: i.fn.functionArn as unknown as string, Input: JSON.stringify({ reconcile: true }) }],
      tags: i.tags("session-pipeline"),
    } as never);
    const briefRule = yield* AWS.EventBridge.Rule("session-brief-schedule", briefScheduleProps({ names, functionArn: i.fn.functionArn as unknown as string, p: i.p, tags: i.tags("session-pipeline") }) as never);
    const analysisRule=yield* AWS.EventBridge.Rule("session-analysis-cache-schedule",analysisCacheScheduleProps({names,functionArn:i.fn.functionArn as unknown as string,p:i.p,tags:i.tags("session-pipeline")}) as never);
    const analysisReconcileRule=yield* AWS.EventBridge.Rule("session-analysis-reconcile-schedule",analysisReconcileScheduleProps({names,functionArn:i.fn.functionArn as unknown as string,p:i.p,tags:i.tags("session-pipeline")}) as never);
    const semanticRule=yield* AWS.EventBridge.Rule("session-semantic-copy-schedule",semanticCopyScheduleProps({names,functionArn:i.fn.functionArn as unknown as string,p:i.p,tags:i.tags("session-pipeline")}) as never);
    const bufferedSearchFeedRule = yield* AWS.EventBridge.Rule("session-buffered-search-feed-schedule", bufferedSearchFeedScheduleProps({ names, functionArn: i.fn.functionArn as unknown as string, p: i.p, tags: i.tags("session-pipeline") }) as never);
    const bufferedErasureDeleteRule = yield* AWS.EventBridge.Rule("session-buffered-erasure-delete-schedule", bufferedErasureDeleteScheduleProps({ names, functionArn: i.fn.functionArn as unknown as string, p: i.p, tags: i.tags("session-pipeline") }) as never);
    for (const { id, props } of sessionPipelinePermissions({
      accountId: i.accountId,
      functionName: i.fn.functionName as unknown as string,
      ruleArn: (rule as unknown as { ruleArn: string }).ruleArn,
      briefRuleArn: (briefRule as unknown as { ruleArn: string }).ruleArn,
      analysisRuleArn:(analysisRule as unknown as {ruleArn:string}).ruleArn,
      analysisReconcileRuleArn:(analysisReconcileRule as unknown as {ruleArn:string}).ruleArn,
      semanticRuleArn:(semanticRule as unknown as {ruleArn:string}).ruleArn,
      bufferedSearchFeedRuleArn: (bufferedSearchFeedRule as unknown as { ruleArn: string }).ruleArn,
      bufferedErasureDeleteRuleArn: (bufferedErasureDeleteRule as unknown as { ruleArn: string }).ruleArn,
      roleArn,
    }))
      yield* AWS.Lambda.Permission(id, props as never);
    const alarm = (id: string, props: Record<string, unknown>) =>
      AWS.CloudWatch.Alarm(id, { ActionsEnabled: false, TreatMissingData: "notBreaching", ...props, tags: i.tags("session-pipeline") } as never);
    yield* alarm("session-events-dlq-alarm", {
      name: `${names.dlq}-not-empty`,
      AlarmDescription: "Session pipeline messages exhausted their retries (runbooks/sessions-prod-session-pipeline.md, DLQ).",
      Namespace: "AWS/SQS",
      MetricName: "ApproximateNumberOfMessagesVisible",
      Dimensions: [{ Name: "QueueName", Value: names.dlq }],
      Statistic: "Maximum",
      Period: 300,
      EvaluationPeriods: 1,
      Threshold: 0,
      ComparisonOperator: "GreaterThanThreshold",
    });
    yield* alarm("session-events-age-alarm", {
      name: `${names.queue}-backlog-age`,
      AlarmDescription: "Oldest session event is older than 15 minutes: the pipeline is behind or failing.",
      Namespace: "AWS/SQS",
      MetricName: "ApproximateAgeOfOldestMessage",
      Dimensions: [{ Name: "QueueName", Value: names.queue }],
      Statistic: "Maximum",
      Period: 300,
      EvaluationPeriods: 3,
      Threshold: 900,
      ComparisonOperator: "GreaterThanThreshold",
    });
    yield* alarm("session-pipeline-errors-alarm", {
      name: `${names.function}-errors`,
      AlarmDescription: "Session pipeline invocations are failing (init, timeouts, crashes).",
      Namespace: "AWS/Lambda",
      MetricName: "Errors",
      Dimensions: [{ Name: "FunctionName", Value: names.function }],
      Statistic: "Sum",
      Period: 300,
      EvaluationPeriods: 3,
      Threshold: 0,
      ComparisonOperator: "GreaterThanThreshold",
    });
    return {
      queueUrl: names.queueUrl,
      queueArn: queue.queueArn,
      dlqArn: dlq.queueArn,
      secretName: names.secret,
      secretArn: (secret as unknown as { secretArn: string }).secretArn,
      mappingUuid: (mapping as unknown as { uuid: string }).uuid,
      notification: bucketNotificationConfiguration(names.queueArn),
    };
  });
