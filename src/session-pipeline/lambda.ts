/**
 * AWS Lambda entry for the sessions-prod session pipeline
 * (stacks/aws/session-pipeline.ts, runbooks/sessions-prod-session-pipeline.md).
 *
 *   SQS batch (S3 object events, reconcile items) -> processBatch -> partial batch response
 *   EventBridge schedule {"reconcile": true}      -> list recent manifests -> SQS
 *   EventBridge schedule {"brief": true}          -> one session-brief tick (src/session-brief)
 *
 * Configuration: plain env for ids and switches; credentials only from one
 * Secrets Manager secret (`SESSION_PIPELINE_SECRET_ARN`), read once per
 * sandbox with the execution role. The secret holds JSON:
 *
 *   { "hoopdb_url": "postgresql://…",           Supabase, the hoopdb runtime role
 *     "clickhouse": { "url", "user", "password", "database" },   optional
 *     "openai_api_key": "…" }                    optional: session brief model calls
 *
 * Switches (env): SEARCH_INDEX_ENABLED=1 turns the ClickHouse stage on (off by
 * default, so the pointer stage can go live first); PIPELINE_FANOUT=postgres
 * enqueues the index's summary/state fan-out on `sessions.pipeline_jobs`
 * (default off: no consumer runs in production yet, and Bob Costas's sweep
 * rebuilds missing summaries when it does). SESSION_BRIEF_ENABLED=true arms brief
 * wakeups as sessions index and lets the brief schedule run; with no
 * `openai_api_key` a brief tick claims nothing (src/session-brief/config.ts).
 */
import pg from "pg";
import {
  createS3Store,
  objectStoreManifestReader,
  postgresPipelineQueue,
  prefixAllowed,
  prefixOrgResolver,
  objectKey,
  parseSessionKey,
  type ObjectStore,
  type PipelineQueue,
  type SessionLocation,
} from "../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { HoopDb } from "../shared/db.ts";
import { errorFields, log } from "../shared/log.ts";
import { awsJsonCall, invokeAsync, lambdaCredentials } from "./aws-json.ts";
import { createClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { processBatch, type BatchResponse, type ConsumerDeps, type QueueRecord } from "./consumer.ts";
import { droppedFanout, scopeLockKey, type ScopeLock } from "./pointer.ts";
import { SEARCH_INDEX_STREAM_CH_BUDGET, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS, type SearchIndexDeps } from "./search-index.ts";
import { RECONCILE_BODY_KEY } from "./s3-events.ts";
import { SUPABASE_PROD_CA_2021 } from "./supabase-ca.ts";
import { briefConfig, modelKey } from "../session-brief/config.ts";
import { createAnalysisTransport } from "../session-brief/engine/transport.ts";
import { briefTick } from "../session-brief/worker.ts";

interface PipelineSecret {
  hoopdb_url: string;
  clickhouse?: { url: string; user: string; password: string; database: string };
  openai_api_key?: string;
}

export interface LambdaEnv {
  region: string;
  bucket: string;
  keyPrefix: string;
  secretArn: string;
  queueUrl: string;
  searchEnabled: boolean;
  fanout: "off" | "postgres";
  reconcileLookbackMinutes: number;
  dbPoolMax: number;
  /**
   * SEARCH_INDEX_STREAMING_CH=1 (default off): a scope past the whole-read bound is reconciled
   * by the streamed reader (search-index-stream.ts), one per invocation, stopping at the
   * invocation's remaining time less SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS. Absent: such a
   * scope is marked partial exactly as before.
   */
  searchStreamCh?: true;
  /**
   * SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED=true (default off): a scope whose stored pointer
   * sizes and index state already show the size refusal at its current revision returns
   * before any manifest read (search-index.ts overBoundBeforeRead). Absent: as before.
   */
  searchSkipOverBound?: true;
  /**
   * SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true (default off): a scope whose last pass
   * ended incomplete for relayhistory_acceptance_missing alone, over the same pointer set,
   * returns that outcome again before any manifest read (search-index.ts acceptance stamp).
   * Absent: as before.
   */
  searchSkipAcceptanceMissing?: true;
}

export function lambdaEnv(env: NodeJS.ProcessEnv = process.env): LambdaEnv {
  const need = (name: string) => {
    const value = env[name];
    if (!value) throw new Error(`session pipeline: ${name} is not set`);
    return value;
  };
  const fanout = env.PIPELINE_FANOUT ?? "off";
  if (fanout !== "off" && fanout !== "postgres") throw new Error("session pipeline: PIPELINE_FANOUT must be off or postgres");
  if (env.PIPELINE_ORG_FROM_PREFIX !== "true") throw new Error("session pipeline: PIPELINE_ORG_FROM_PREFIX must be true (org-prefix layout)");
  const streamCh = env.SEARCH_INDEX_STREAMING_CH;
  const searchStreamCh = streamCh === "1";
  if (!searchStreamCh && streamCh !== undefined && streamCh !== "" && streamCh !== "0")
    throw new Error("session pipeline: SEARCH_INDEX_STREAMING_CH must be 1 or 0");
  if (searchStreamCh && env.SEARCH_INDEX_ENABLED !== "1")
    throw new Error("session pipeline: SEARCH_INDEX_STREAMING_CH=1 needs SEARCH_INDEX_ENABLED=1");
  const skipOverBound = env.SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED;
  const searchSkipOverBound = skipOverBound === "true";
  if (!searchSkipOverBound && skipOverBound !== undefined && skipOverBound !== "" && skipOverBound !== "false")
    throw new Error("session pipeline: SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED must be true or false");
  if (searchSkipOverBound && env.SEARCH_INDEX_ENABLED !== "1")
    throw new Error("session pipeline: SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
  const skipAcceptance = env.SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED;
  const searchSkipAcceptanceMissing = skipAcceptance === "true";
  if (!searchSkipAcceptanceMissing && skipAcceptance !== undefined && skipAcceptance !== "" && skipAcceptance !== "false")
    throw new Error("session pipeline: SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED must be true or false");
  if (searchSkipAcceptanceMissing && env.SEARCH_INDEX_ENABLED !== "1")
    throw new Error("session pipeline: SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
  return {
    region: need("AWS_REGION"),
    bucket: need("PIPELINE_RAW_S3_BUCKET"),
    keyPrefix: need("PIPELINE_KEY_PREFIX").replace(/^\/+|\/+$/g, ""),
    secretArn: need("SESSION_PIPELINE_SECRET_ARN"),
    queueUrl: need("SESSION_PIPELINE_QUEUE_URL"),
    searchEnabled: env.SEARCH_INDEX_ENABLED === "1",
    fanout,
    reconcileLookbackMinutes: Number(env.PIPELINE_RECONCILE_LOOKBACK_MINUTES ?? "30") || 30,
    dbPoolMax: Number(env.HOOPDB_POOL_MAX ?? "4") || 4,
    ...(searchStreamCh ? { searchStreamCh: true as const } : {}),
    ...(searchSkipOverBound ? { searchSkipOverBound: true as const } : {}),
    ...(searchSkipAcceptanceMissing ? { searchSkipAcceptanceMissing: true as const } : {}),
  };
}

/** Parse and check the secret's shape. Never echoes a value. */
export function parseSecret(text: string): PipelineSecret {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error("session pipeline secret is not JSON");
  }
  const v = value as Partial<PipelineSecret> | null;
  if (!v || typeof v.hoopdb_url !== "string" || !/^postgres(ql)?:\/\//.test(v.hoopdb_url))
    throw new Error("session pipeline secret has no hoopdb_url");
  if (v.clickhouse !== undefined) {
    const c = v.clickhouse;
    for (const k of ["url", "user", "password", "database"] as const)
      if (!c || typeof c[k] !== "string" || !c[k]) throw new Error(`session pipeline secret clickhouse.${k} is missing`);
  }
  if (v.openai_api_key !== undefined && (typeof v.openai_api_key !== "string" || !v.openai_api_key.trim()))
    throw new Error("session pipeline secret openai_api_key must be a non-empty string when present");
  return v as PipelineSecret;
}

/**
 * A pg pool that verifies Supabase's certificate against the pinned root. The
 * URL's own ssl parameters are dropped so they cannot weaken that.
 */
export function supabasePool(url: string, max: number): HoopDb {
  const parsed = new URL(url);
  for (const k of ["sslmode", "sslrootcert", "sslcert", "sslkey", "ssl"]) parsed.searchParams.delete(k);
  const pool = new pg.Pool({
    connectionString: parsed.toString(),
    max,
    application_name: "96bulls-session-pipeline",
    ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true },
    idleTimeoutMillis: 60_000,
  });
  pool.on("error", (err) => log.error("hoopdb pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  return {
    pool,
    async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const r = await pool.query(text, params as unknown[]);
      return { rows: r.rows as R[] };
    },
    end: () => pool.end(),
  };
}

/**
 * One transaction on one checked-out client. `guarded` (SEARCH_INDEX_STREAMING_CH=1, the search
 * stage only): pg-pool drops its idle 'error' listener at checkout, and pg's client emits
 * 'error' when its connection ends, also between statements, so a streamed pass holding the
 * client for minutes would crash the invocation with an uncaught exception. Guarded, a
 * per-checkout listener keeps the error: the next statement (the pass's keepalive, its forced
 * lock check before any delete, COMMIT) rejects with it, the transaction fails as a normal
 * rejected promise, and the broken client is destroyed (`release(err)`), not returned to the
 * pool. Unguarded: exactly as before.
 */
export async function transaction<T>(db: HoopDb, work: (tx: SqlExecutor) => Promise<T>, guarded = false): Promise<T> {
  const client = await db.pool.connect();
  let broken: Error | undefined;
  const onError = (error: Error) => {
    broken ??= error;
  };
  if (guarded) client.on("error", onError);
  const query = async (text: string, params?: unknown[]) => {
    if (broken) throw broken;
    return client.query(text, params as unknown[]);
  };
  try {
    await query("BEGIN");
    await query("SET LOCAL statement_timeout = '60s'");
    const result = await work({
      async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
        const r = await query(text, params);
        return { rows: r.rows as R[] };
      },
    });
    await query("COMMIT");
    return result;
  } catch (error) {
    await query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    if (guarded) client.removeListener("error", onError);
    if (broken) client.release(broken);
    else client.release();
  }
}

/** The search stage's transaction: guarded (transaction() above) exactly when streaming is armed. */
export function searchTransaction(db: HoopDb, env: Pick<LambdaEnv, "searchStreamCh">): SearchIndexDeps["transact"] {
  return (work) => transaction(db, work, env.searchStreamCh === true);
}

/** A search pass that found its session busy runs again this much later. */
export const BUSY_REQUEUE_DELAY_SECONDS = 60;

interface Runtime {
  env: LambdaEnv;
  deps: ConsumerDeps;
  brief: ReturnType<typeof briefTick>;
}

let runtime: Promise<Runtime> | undefined;

async function init(): Promise<Runtime> {
  const env = lambdaEnv();
  const { SecretString } = await awsJsonCall<{ SecretString?: string }>({
    service: "secretsmanager",
    target: "secretsmanager.GetSecretValue",
    region: env.region,
    body: { SecretId: env.secretArn },
    credentials: lambdaCredentials(),
  });
  if (!SecretString) throw new Error("session pipeline secret has no value yet");
  const secret = parseSecret(SecretString);
  const db = supabasePool(secret.hoopdb_url, env.dbPoolMax);
  const store = createS3Store({ bucket: env.bucket, region: env.region, credentials: async () => lambdaCredentials() });
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: env.bucket });
  const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: env.keyPrefix };
  const fanout: PipelineQueue = env.fanout === "postgres" ? postgresPipelineQueue(db) : droppedFanout;
  const lock: ScopeLock = (scope, work) =>
    transaction(db, async (tx) => {
      await tx.query("SET LOCAL lock_timeout = '30s'");
      await tx.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [scopeLockKey(scope)]);
      return work();
    });
  const deps: ConsumerDeps = {
    pointer: {
      db,
      lock,
      reader,
      exists: async (key) => (await store.head(objectKey(key))) !== undefined,
      orgs: prefixOrgResolver,
      allowPrefix: (prefix, orgId) => prefixAllowed(bindings, prefix, orgId),
      fanout,
    },
    requeue: async (manifestKey) => {
      await awsJsonCall({
        service: "sqs",
        target: "AmazonSQS.SendMessage",
        region: env.region,
        body: { QueueUrl: env.queueUrl, MessageBody: JSON.stringify({ [RECONCILE_BODY_KEY]: { key: manifestKey } }), DelaySeconds: BUSY_REQUEUE_DELAY_SECONDS },
        credentials: lambdaCredentials(),
      });
    },
  };
  if (env.searchEnabled) {
    if (!secret.clickhouse) throw new Error("SEARCH_INDEX_ENABLED=1 but the secret has no clickhouse block");
    deps.search = {
      // Guarded only when streaming is armed: unarmed, the search transaction is exactly as before.
      transact: searchTransaction(db, env),
      reader,
      store,
      ch: createClickHouseClient(secret.clickhouse),
      prefixFor: (orgId) => `${env.keyPrefix}/${orgId}`,
      // Default off (SEARCH_INDEX_STREAMING_CH=1); the deadline and the slot are set per invocation.
      ...(env.searchStreamCh ? { stream: { ...SEARCH_INDEX_STREAM_CH_BUDGET } } : {}),
      // Default off (SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED=true).
      ...(env.searchSkipOverBound ? { skipOverBound: true as const } : {}),
      // Default off (SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true).
      ...(env.searchSkipAcceptanceMissing ? { skipAcceptanceMissing: true as const } : {}),
    };
  }
  // The key comes from the secret only; "-disabled" or blank means no model, as in config.ts.
  const key = modelKey({ OPENAI_API_KEY: secret.openai_api_key });
  const brief = briefTick({
    reader,
    store,
    transport: key ? createAnalysisTransport({ apiKey: () => key }) : null,
    config: briefConfig(),
  });
  log.info("session pipeline ready", { stage: "init", ok: true, state: env.searchEnabled ? "search_on" : "search_off" });
  return { env, deps, brief };
}

function ready(): Promise<Runtime> {
  runtime ??= init().catch((error) => {
    // A failed init (secret not set yet, bad URL) is retried on the next invoke.
    runtime = undefined;
    throw error;
  });
  return runtime;
}

export interface ReconcileSummary {
  listed: number;
  recent: number;
  enqueued: number;
  pages: number;
  /** Live pointers indexed in the window whose manifest was checked, and how many were gone. */
  pointersChecked: number;
  pointersGone: number;
  /** Where the listing stopped at the deadline; the next link of the chain starts here. */
  nextCursor?: string;
  complete: boolean;
}

/** Stop listing this long before the function timeout (300 s), leaving time to hand off. */
export const RECONCILE_DEADLINE_MS = 240_000;

export interface ReconcileDeps {
  store: Pick<ObjectStore, "list" | "head">;
  db: SqlExecutor;
  keyPrefix: string;
  send(body: string): Promise<void>;
  /** Lower bound of "recent", fixed for a whole chain. */
  since: Date;
  cursor?: string;
  maxPages?: number;
  deadlineMs?: number;
  now?: () => number;
}

/**
 * The safety net for lost or raced notifications, in two legs.
 *
 * 1. Live pointers indexed since `since` (first link only): HEAD each
 *    manifest; a gone one is enqueued, and the queue path tombstones it under
 *    the session lock. This repairs a delete that was lost or raced.
 * 2. Manifests modified since `since`: enqueue those whose pointer row is
 *    missing, deleted or names another ETag. No manifest body is read here.
 *
 * The listing stops at the deadline and returns `nextCursor`; the caller
 * hands it to the next link (an async self-invocation), so a bucket of any
 * size is covered within one chain instead of being cut off at the timeout.
 */
export async function reconcileRecent(deps: ReconcileDeps): Promise<ReconcileSummary> {
  const now = deps.now ?? Date.now;
  const since = deps.since.getTime();
  const deadline = now() + (deps.deadlineMs ?? RECONCILE_DEADLINE_MS);
  const summary: ReconcileSummary = { listed: 0, recent: 0, enqueued: 0, pages: 0, pointersChecked: 0, pointersGone: 0, complete: false };

  if (deps.cursor === undefined) {
    const { rows } = await deps.db.query<{ manifest_key: string }>(
      `SELECT manifest_key FROM sessions.session_index
        WHERE deleted_at IS NULL AND indexed_at >= $1::timestamptz AND manifest_key LIKE $2
        ORDER BY indexed_at LIMIT 5000`,
      [deps.since.toISOString(), `${deps.keyPrefix}/%`],
    );
    for (const { manifest_key } of rows) {
      if (now() > deadline) break;
      summary.pointersChecked += 1;
      if (await deps.store.head(objectKey(manifest_key))) continue;
      summary.pointersGone += 1;
      await deps.send(JSON.stringify({ [RECONCILE_BODY_KEY]: { key: manifest_key } }));
      summary.enqueued += 1;
    }
  }

  let cursor = deps.cursor;
  do {
    const page = await deps.store.list(objectKey(deps.keyPrefix), cursor);
    summary.pages += 1;
    const recent: { key: string; etag?: string; version?: string; loc: SessionLocation }[] = [];
    for (const item of page.items) {
      const key = String(item.key);
      if (!key.endsWith("/manifest.json")) continue;
      summary.listed += 1;
      if (item.lastModified.getTime() < since) continue;
      const loc = parseSessionKey(key);
      if (!loc || loc.rest !== "manifest.json" || !loc.prefix) continue;
      recent.push({ key, ...(item.version.eTag ? { etag: item.version.eTag.replace(/^"|"$/g, "") } : {}), ...(item.version.version ? { version: item.version.version } : {}), loc });
    }
    summary.recent += recent.length;
    if (recent.length > 0) {
      const { rows } = await deps.db.query<{ manifest_key: string; manifest_etag: string | null; deleted: boolean }>(
        `SELECT s.manifest_key, s.manifest_etag, s.deleted_at IS NOT NULL AS deleted
           FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) AS k(o, w, h, s, m)
           JOIN sessions.session_index s
             ON s.org_id = k.o AND s.workspace_id = k.w AND s.harness = k.h AND s.session_id = k.s AND s.machine_id = k.m`,
        [
          recent.map((r) => r.loc.prefix!.split("/").pop()!),
          recent.map((r) => r.loc.workspaceId),
          recent.map((r) => r.loc.harness),
          recent.map((r) => r.loc.sessionId),
          recent.map((r) => r.loc.machineId),
        ],
      );
      const indexed = new Map(rows.map((r) => [r.manifest_key, r]));
      for (const r of recent) {
        const row = indexed.get(r.key);
        if (row && !row.deleted && row.manifest_etag && row.manifest_etag === r.etag) continue;
        await deps.send(JSON.stringify({ [RECONCILE_BODY_KEY]: { key: r.key, etag: r.etag, version: r.version } }));
        summary.enqueued += 1;
      }
    }
    cursor = page.nextCursor;
  } while (cursor && summary.pages < (deps.maxPages ?? 5_000) && now() < deadline);
  summary.complete = !cursor;
  if (cursor) summary.nextCursor = cursor;
  return summary;
}

interface ReconcileEvent {
  reconcile: true;
  cursor?: string;
  since?: string;
  /** Links already run in this chain. */
  link?: number;
}

/** A chain never outlives two cadences of listing: a runaway is cut off, the next schedule starts over. */
export const MAX_RECONCILE_LINKS = 24;

async function runReconcile(rt: Runtime, event: ReconcileEvent) {
  const { env } = rt;
  const since = event.since ? new Date(event.since) : new Date(Date.now() - env.reconcileLookbackMinutes * 60_000);
  if (!Number.isFinite(since.getTime())) throw new Error("reconcile: invalid since");
  const summary = await reconcileRecent({
    store: createS3Store({ bucket: env.bucket, region: env.region, credentials: async () => lambdaCredentials() }),
    db: rt.deps.pointer.db,
    keyPrefix: env.keyPrefix,
    since,
    ...(event.cursor ? { cursor: event.cursor } : {}),
    send: async (body) => {
      await awsJsonCall({
        service: "sqs",
        target: "AmazonSQS.SendMessage",
        region: env.region,
        body: { QueueUrl: env.queueUrl, MessageBody: body },
        credentials: lambdaCredentials(),
      });
    },
  });
  const link = (event.link ?? 0) + 1;
  if (summary.nextCursor && link < MAX_RECONCILE_LINKS) {
    const next: ReconcileEvent = { reconcile: true, cursor: summary.nextCursor, since: since.toISOString(), link };
    await invokeAsync({
      region: env.region,
      functionName: process.env.AWS_LAMBDA_FUNCTION_NAME ?? "",
      payload: JSON.stringify(next),
      credentials: lambdaCredentials(),
    });
  }
  return { ...summary, link };
}

/** The part of the Lambda context the pipeline reads. */
export interface LambdaInvocationContext {
  getRemainingTimeInMillis(): number;
}

/**
 * The consumer deps for one SQS batch: `deps` itself unless streaming is armed; then the
 * search stage streams at most one scope of the batch (a second is requeued) and, with a
 * Lambda context, stops streaming at the remaining time less the margin.
 */
export function invocationDeps(deps: ConsumerDeps, context?: LambdaInvocationContext, now: () => number = Date.now): ConsumerDeps {
  const search = deps.search;
  if (!search?.stream) return deps;
  const remaining = typeof context?.getRemainingTimeInMillis === "function" ? context.getRemainingTimeInMillis() : NaN;
  return {
    ...deps,
    search: {
      ...search,
      streamSlot: { used: false },
      ...(Number.isFinite(remaining) ? { streamDeadline: { at: now() + remaining - SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS } } : {}),
    },
  };
}

type LambdaEvent = { Records?: { messageId: string; body: string; eventSource?: string }[]; reconcile?: unknown; brief?: unknown; cursor?: string; since?: string; link?: number };

export async function handler(event: LambdaEvent, context?: LambdaInvocationContext): Promise<BatchResponse | { ok: boolean }> {
  if (Array.isArray(event?.Records)) {
    let rt: Runtime;
    try {
      rt = await ready();
    } catch (error) {
      log.error("session pipeline init failed", { stage: "init", ...errorFields(error) });
      // Nothing was processed: every message goes back to the queue.
      return { batchItemFailures: event.Records.map((r) => ({ itemIdentifier: r.messageId })) };
    }
    const records: QueueRecord[] = event.Records.map((r) => ({ messageId: r.messageId, body: r.body }));
    return processBatch(records, invocationDeps(rt.deps, context));
  }
  if (event?.reconcile === true) {
    const started = Date.now();
    try {
      const result = await runReconcile(await ready(), event as ReconcileEvent);
      log.info("reconcile finished", {
        stage: "object-landed",
        listed: result.listed,
        count: result.recent,
        enqueued: result.enqueued,
        skipped: result.pointersChecked,
        failed: result.pointersGone,
        attempt: result.link,
        ok: result.complete,
        duration_ms: Date.now() - started,
      });
      return { ok: true };
    } catch (error) {
      log.error("reconcile failed", { stage: "object-landed", ...errorFields(error), duration_ms: Date.now() - started });
      return { ok: false };
    }
  }
  if (event?.brief === true) {
    // Off: return before init, so a schedule left enabled by mistake reads no secret and opens no pool.
    if (!briefConfig().enabled) return { ok: true };
    const started = Date.now();
    try {
      const rt = await ready();
      await rt.brief(rt.deps.pointer.db, new Date());
      return { ok: true };
    } catch (error) {
      log.error("session brief tick failed", { stage: "session-brief", ...errorFields(error), duration_ms: Date.now() - started });
      return { ok: false };
    }
  }
  log.warn("unknown invocation", { stage: "init", reason: "unknown_event" });
  return { ok: false };
}
