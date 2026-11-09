import {createSemanticClickHouseClient} from "../search-features/semantic-copy-ch.ts";
import {SemanticPostgresExecutor} from "../search-features/semantic-copy-postgres.ts";
import { largeReadJobState, routeLargeRead } from "./large-read-worker.ts";
/**
 * AWS Lambda entry for the sessions-prod session pipeline
 * (stacks/aws/session-pipeline.ts, runbooks/sessions-prod-session-pipeline.md).
 *
 *   SQS batch (S3 object events, reconcile items) -> processBatch -> partial batch response
 *   EventBridge schedule {"reconcile": true}      -> list recent manifests -> SQS
 *   EventBridge schedule {"brief": true}          -> one session-brief tick (src/session-brief)
 *   schedule {"bufferedSearchFeed": true}         -> one bounded buffered-stage feed run (default off)
 *   schedule {"bufferedErasureDelete": true}      -> one bounded later-erasure delete pass over buffered rows (default off)
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
 * SESSION_DESKTOP_CATALOG_ENABLED=1 (only the literal "1"; default off) switches the
 * search stage from the pre-#182 catalog writer to the complete desktop catalog writer
 * (migration 20261105022000 columns); SEARCH_INDEX_ENABLED alone never does
 * (docs/session-desktop-catalog.md, Activation blockers).
 * SEARCH_INDEX_OUTPUT_FACTS=1 (only the literal "1"; default off; needs
 * SEARCH_INDEX_ENABLED=1) also reconciles the Output/digest fact tables in the same
 * pass (output-facts.ts); it needs the owner-applied output-facts DDL first.
 * INGEST_BUFFERED_SEARCH_FEED=on (only the literal "on"; default "off", anything else refuses)
 * feeds accepted buffered stages into transcript_events (buffered-search-feed.ts); it is refused
 * unless BUFFERED_SEARCH_FEED_GATE_DECLARED equals the read gate's contract id
 * (BUFFERED_SEARCH_GATE_CONTRACT, "buffered-search-gate/v1"; the read Worker deploys with
 * READ_API_BUFFERED_SEARCH_GATE=on first), and needs SEARCH_INDEX_ENABLED=1 on the ClickHouse backend, the feed RPCs
 * (migration 20261105043870) and the origin columns (docs/proposals/buffered-search-feed.clickhouse.draft.sql) applied.
 * It also needs its own login: the secret fields buffered_search_feed_database_url and
 * buffered_search_feed_database_ca_pem (the buffered_search_feed_runtime LOGIN, buffered-feed-login.ts). Without
 * them, or if malformed, the feed stays off; every pooled backend passes the live login check before its first feed
 * RPC, and a refusal indexes nothing. Provisioning that LOGIN and credential needs Khaliq's explicit OK.
 * INGEST_BUFFERED_ERASURE_DELETE=on (only the literal "on"; default "off", anything else refuses) runs the
 * later-erasure delete lane (buffered-erasure-delete.ts): it re-checks buffered rows' stages through the same
 * feed RPC (`stage` only) and deletes, through deleteBufferedRows only, buffered rows whose stage is gone or
 * refused and whose records retention no longer keeps; it never matches canonical rows. Same ordering guard
 * (BUFFERED_SEARCH_FEED_GATE_DECLARED), same feeder login and ClickHouse prerequisites as the feed; the flag is
 * checked before any secret lookup, pool or ClickHouse access. Bounds: INGEST_BUFFERED_ERASURE_PAGE (1-200,
 * default 25), INGEST_BUFFERED_ERASURE_RATE (stage checks/s, 1-20, default 2), INGEST_BUFFERED_ERASURE_BUDGET_MS
 * (5000-240000, default 40000).
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
import { searchIndexLimits } from "./search-index.ts";
import { BufferedFeedError, bufferedFeedConfig, bufferedFeedGateRefusal, bufferedFeedVersionRefusal, clickhouseTieLedger, runBufferedFeed, sqlBufferedFeedSource, type BufferedFeedConfig } from "./buffered-search-feed.ts";
import { bufferedErasureConfig, runBufferedErasureDelete, type BufferedErasureConfig, type BufferedErasureDeps } from "./buffered-erasure-delete.ts";
import { RECONCILE_BODY_KEY } from "./s3-events.ts";
import { BUFFERED_FEED_CA_FIELD, BUFFERED_FEED_URL_FIELD, bufferedFeedClientConfig, loginCheckedFeedExecutor } from "./buffered-feed-login.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import { composeSearchService, searchSecret, type SearchComposition } from "../shared/search-composition.ts";
import type { PipelineRuntimeRegistry } from "@rhc/adapters/defaults.ts";
import { dispatchRegisteredLambda, type PipelineLambdaEvent } from "./lambda-runtime.ts";
import { writerTransactionSql } from "./transaction-sql.ts";
import { briefConfig, modelKey } from "../session-brief/config.ts";
import { createAnalysisTransport } from "../session-brief/engine/transport.ts";
import {checkedAnalysisCursor,reconcileAnalysisPage} from "./analysis-reconcile.ts";
import {analysisRefreshTick,enqueueAnalysisScope,type AnalysisRefreshDeps} from "./analysis-refresh.ts";
import {readonlyAnalysisPool,readonlyAnalysisSource} from "./analysis-snapshot.ts";
import { briefTick } from "../session-brief/worker.ts";
import { analysisConfig, analysisTick } from "../session-analysis/worker.ts";
import { zstdDecompressSync } from "node:zlib";
import { createS3Eraser } from "../migration/neon-s3-erasure/eraser.ts";
import{derivedSchemaAssets}from "./derived-schema.ts";
import { validateAdapterEnv } from "@rhc/adapters/registry.ts";
import {runSemanticCopyTick,semanticFailureReason} from "../search-features/semantic-copy-job.ts";
import {semanticReadonlyPool,semanticFeatureReader,semanticMetadataPool} from "../search-features/semantic-copy-source-pool.ts";
import {assertNoNeonEdge,neonExitProfile} from "../shared/neon-exit.ts";

interface PipelineSecret {
  hoopdb_url: string;
  /**
   * Separate least-privilege READ ONLY legacy source, never exposed to Worker.
   * Deliberately unchecked here: only the analysis branch validates it, and only
   * when SESSION_ANALYSIS_CACHE_ENABLED=1, so a staged or malformed field can never
   * stop the shared runtime that serves SQS/search invocations.
   */
  analysis_source_readonly_url?: unknown;
  clickhouse?: { url: string; user: string; password: string; database: string };
  openai_api_key?: string;
  search?: unknown;
  /**
   * The buffered search feeder's own login (buffered_search_feed_runtime) and its CA. Carried raw and
   * validated only when INGEST_BUFFERED_SEARCH_FEED=on (buffered-feed-login.ts); absent leaves the feed
   * off. Provisioning the LOGIN and this credential needs Khaliq's explicit OK; no deploy wiring sets it.
   */
  buffered_search_feed_database_url?: unknown;
  buffered_search_feed_database_ca_pem?: unknown;
}

export interface LambdaEnv {
  searchBackend?: string;
  adapterEnv?: Readonly<Record<string, string | undefined>>;
  postgresTls?: ReturnType<typeof postgresTlsOptions>;
  region: string;
  bucket: string;
  keyPrefix: string;
  secretArn: string;
  queueUrl: string;
  searchEnabled: boolean;
  replayPagesEnabled: boolean;
  largeScopeHandoffEnabled?: boolean;
  analysisCacheEnabled: boolean;
  /** SESSION_DESKTOP_CATALOG_ENABLED=1: the search stage writes the complete desktop catalog; off (default): the pre-#182 writer. */
  desktopCatalogEnabled: boolean;
  /** SEARCH_INDEX_OUTPUT_FACTS=1: the search stage also writes the Output/digest facts. */
  outputFactsEnabled?: boolean;
  semanticCopyEnabled?: boolean;
  semanticSourceId?: string;
  /** SESSIONS_NEON_EXIT=true: the legacy readonly source (analysis_source_readonly_url) is never carried or built. */
  neonExit?: boolean;
  fanout: "off" | "postgres";
  reconcileLookbackMinutes: number;
  dbPoolMax: number;
  /** INGEST_BUFFERED_SEARCH_FEED*: absent or `enabled: false` (default) builds nothing. */
  bufferedSearchFeed?: BufferedFeedConfig;
  /** INGEST_BUFFERED_ERASURE_*: absent or `enabled: false` (default) builds nothing. */
  bufferedErasureDelete?: BufferedErasureConfig;
}

/**
 * The buffered feed's settings without risk to the shared runtime: a malformed value disarms the
 * feed (logged by code) instead of failing init for SQS/search/reconcile. A typo never arms it.
 */
function bufferedFeedEnv(env: NodeJS.ProcessEnv): BufferedFeedConfig | undefined {
  try { return bufferedFeedConfig(env); }
  catch (error) {
    // Refused, not degraded: nothing is written, and every scheduled run reports ok=false.
    log.error("buffered search feed refused", { stage: "buffered-search-feed", reason: error instanceof BufferedFeedError ? error.code : "config_invalid" });
    return undefined;
  }
}

/** The erasure lane's settings, disarmed (logged by code) instead of failing the shared runtime. */
function bufferedErasureEnv(env: NodeJS.ProcessEnv): BufferedErasureConfig | undefined {
  try { return bufferedErasureConfig(env); }
  catch (error) {
    log.error("buffered erasure delete refused", { stage: "buffered-erasure-delete", reason: error instanceof BufferedFeedError ? error.code : "config_invalid" });
    return undefined;
  }
}

export function lambdaEnv(env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition): LambdaEnv {
  const need = (name: string) => {
    const value = env[name];
    if (!value) throw new Error(`session pipeline: ${name} is not set`);
    return value;
  };
  const fanout = env.PIPELINE_FANOUT ?? "off";
  if (fanout !== "off" && fanout !== "postgres") throw new Error("session pipeline: PIPELINE_FANOUT must be off or postgres");
  // This entry point is the lambda runtime with the ClickHouse stage. Unset
  // selection variables (today's stack) resolve to exactly that; anything else
  // is a misconfiguration (docs/search-and-runtime-adapters.md).
  const selected = validateAdapterEnv(env, { runtime: ["lambda"], runtimeDefault: "lambda", search: ["clickhouse", ...(composition?.registry.registered().filter(name => name !== "clickhouse") ?? [])], searchDefault: "clickhouse" });
  if (env.PIPELINE_ORG_FROM_PREFIX !== "true") throw new Error("session pipeline: PIPELINE_ORG_FROM_PREFIX must be true (org-prefix layout)");
  // Both read the legacy readonly source, which has no non-Neon replacement; no host check substitutes.
  const neonExit = neonExitProfile(env);
  assertNoNeonEdge(neonExit, "the session pipeline", {
    "SESSION_ANALYSIS_CACHE_ENABLED=1": env.SESSION_ANALYSIS_CACHE_ENABLED === "1",
    "SESSION_SEMANTIC_COPY_ENABLED=1": env.SESSION_SEMANTIC_COPY_ENABLED === "1",
  });
  if (env.SEARCH_INDEX_OUTPUT_FACTS === "1" && env.SEARCH_INDEX_ENABLED !== "1")
    throw new Error("session pipeline: SEARCH_INDEX_OUTPUT_FACTS=1 needs SEARCH_INDEX_ENABLED=1");
  // The fact tables are ClickHouse DDL: a portable search backend has nowhere to put them.
  if (env.SEARCH_INDEX_OUTPUT_FACTS === "1" && selected.search !== "clickhouse")
    throw new Error("session pipeline: SEARCH_INDEX_OUTPUT_FACTS=1 needs the clickhouse search backend");
  return {
    searchBackend: selected.search,
    adapterEnv: Object.freeze(Object.fromEntries(Object.entries(env).filter(([key]) => key.startsWith("SEARCH_INDEX_") || ["PIPELINE_RUNTIME", "AWS_REGION", "READ_API_COMPLETE_SINCE"].includes(key)))),
    postgresTls: postgresTlsOptions(env),
    region: need("AWS_REGION"),
    bucket: need("PIPELINE_RAW_S3_BUCKET"),
    keyPrefix: need("PIPELINE_KEY_PREFIX").replace(/^\/+|\/+$/g, ""),
    secretArn: need("SESSION_PIPELINE_SECRET_ARN"),
    queueUrl: need("SESSION_PIPELINE_QUEUE_URL"),
    searchEnabled: env.SEARCH_INDEX_ENABLED === "1",
    bufferedSearchFeed: bufferedFeedEnv(env),
    bufferedErasureDelete: bufferedErasureEnv(env),
    replayPagesEnabled: env.SESSION_REPLAY_PAGES_ENABLED === "1",
    largeScopeHandoffEnabled: env.SESSION_LARGE_READ_HANDOFF_ENABLED === "1",
    analysisCacheEnabled: env.SESSION_ANALYSIS_CACHE_ENABLED === "1",
    desktopCatalogEnabled: env.SESSION_DESKTOP_CATALOG_ENABLED === "1",
    outputFactsEnabled: env.SEARCH_INDEX_OUTPUT_FACTS === "1",
    semanticCopyEnabled: env.SESSION_SEMANTIC_COPY_ENABLED === "1",
    semanticSourceId: env.SESSION_SEARCH_FEATURE_SOURCE_ID,
    ...(neonExit.enabled ? { neonExit: true } : {}),
    fanout,
    reconcileLookbackMinutes: Number(env.PIPELINE_RECONCILE_LOOKBACK_MINUTES ?? "30") || 30,
    dbPoolMax: Number(env.HOOPDB_POOL_MAX ?? "4") || 4,
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
 * The feeder's pinned login config, or undefined (feed disarmed, logged by reason, never a value):
 * both secret fields absent is "feeder_login_absent"; anything malformed is "feeder_login_invalid".
 */
function bufferedFeedLogin(secret: PipelineSecret): pg.ClientConfig | undefined {
  const url = secret.buffered_search_feed_database_url, ca = secret.buffered_search_feed_database_ca_pem;
  if (url === undefined && ca === undefined) {
    log.warn("buffered search feed disarmed", { stage: "buffered-search-feed", reason: "feeder_login_absent" });
    return undefined;
  }
  try { return bufferedFeedClientConfig(url, ca); }
  catch (error) {
    const msg = error instanceof Error ? error.message : "";
    const field = msg.includes(BUFFERED_FEED_CA_FIELD) ? BUFFERED_FEED_CA_FIELD : msg.includes(BUFFERED_FEED_URL_FIELD) ? BUFFERED_FEED_URL_FIELD : "unknown";
    log.error("buffered search feed disarmed", { stage: "buffered-search-feed", reason: "feeder_login_invalid", error_code: field });
    return undefined;
  }
}

/**
 * Validate the readonly analysis source field. Called only from the analysis
 * branch with the flag on; never echoes the value.
 */
export function analysisSourceReadonlyUrl(value: unknown): string {
  if (value === undefined || value === null || value === "")
    throw new Error("analysis cache enabled without readonly source configuration (secret analysis_source_readonly_url is missing)");
  if (typeof value !== "string" || !/^postgres(ql)?:\/\//u.test(value))
    throw new Error("session pipeline secret analysis_source_readonly_url must be a postgres URL");
  return value;
}

/**
 * A pg pool that verifies Supabase's certificate against the pinned root. The
 * URL's own ssl parameters are dropped so they cannot weaken that.
 *
 * `options` is pinned non-empty (mirroring owner-pg-config): node-pg resolves
 * a missing `options` from process.env.PGOPTIONS, so without this pin ambient
 * PGOPTIONS could smuggle session GUCs (role, search_path, timeouts) past
 * authentication into every connection this pool opens. A set PGOPTIONS is
 * refused loudly below instead of silently ignored.
 */
/**
 * The environment half of startup-option smuggling: node-pg resolves a
 * missing pool `options` from process.env.PGOPTIONS, so ambient PGOPTIONS can
 * smuggle session GUCs (role, search_path, timeouts) past authentication into
 * every pooled connection. The pools pin a non-empty `options` (see below),
 * which shadows the environment, and pool creation refuses a set PGOPTIONS
 * loudly here instead of silently ignoring it. PGlite never reads this
 * variable. Accepts an env for tests; defaults to the process environment.
 */
export function refuseEnvOptions(env: NodeJS.ProcessEnv = process.env): void {
  if (env.PGOPTIONS !== undefined && env.PGOPTIONS !== "")
    throw new Error("pipeline consumer: refusing PGOPTIONS environment fallback (pool options are pinned)");
}

export function supabasePool(url: string, max: number, tls = postgresTlsOptions()): HoopDb {
  refuseEnvOptions();
  const pool = new pg.Pool({
    connectionString: postgresConnectionString(url),
    max,
    application_name: "96bulls-session-pipeline",
    ssl: tls,
    idleTimeoutMillis: 60_000,
    // Inert by itself (the tick sets tighter per-statement timeouts); its
    // purpose is to shadow the environment: a non-empty config value wins
    // over PGOPTIONS in node-pg's resolution, so the env fallback is dead.
    options: "-c idle_in_transaction_session_timeout=60000",
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

export async function transaction<T>(db: HoopDb, work: (tx: SqlExecutor) => Promise<T>): Promise<T> {
  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SET LOCAL statement_timeout = '60s'");
    const result = await work({
      async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
        const r = await client.query(text, params as unknown[]);
        return { rows: r.rows as R[] };
      },
    });
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/** A search pass that found its session busy runs again this much later. */
export const BUSY_REQUEUE_DELAY_SECONDS = 60;

export interface Runtime {
  env: LambdaEnv;
  deps: ConsumerDeps;
  brief: ReturnType<typeof briefTick>;
  /** Session analysis jobs (lane F1); claims nothing unless enabled AND a model key is set. */
  sessionAnalysis: ReturnType<typeof analysisTick>;
  store: ReturnType<typeof createS3Store>;
  /** Raw, unvalidated secret field; see analysisDeps(). */
  analysisSourceUrl?: unknown;
  /** Built on first analysis invocation with the flag on, never in shared init. */
  analysis?: AnalysisRefreshDeps;
  /** Raw secret fields for the semantic copier, carried only with its flag on; see semanticCopyDeps(). */
  semanticSecret?: { hoopdbUrl: string; clickhouse?: PipelineSecret["clickhouse"]; sourceUrl?: unknown };
  /** Built on first semantic-copy invocation with the flag on, never in shared init. */
  semanticCopy?: () => Promise<SemanticCopyStats>;
  /** INGEST_BUFFERED_ERASURE_DELETE=on only: the later-erasure lane over the feeder login's pool. */
  bufferedErasure?: BufferedErasureDeps;
}

type SemanticCopyStats = Awaited<ReturnType<typeof runSemanticCopyTick>>;

async function init(composition?: SearchComposition): Promise<Runtime> {
  const env = lambdaEnv(process.env, composition);
  const publicationSchema=await derivedSchemaAssets();
  const { SecretString } = await awsJsonCall<{ SecretString?: string }>({
    service: "secretsmanager",
    target: "secretsmanager.GetSecretValue",
    region: env.region,
    body: { SecretId: env.secretArn },
    credentials: lambdaCredentials(),
  });
  if (!SecretString) throw new Error("session pipeline secret has no value yet");
  return runtimeFromSecret(env, SecretString, publicationSchema.expected.generation, composition);
}

/**
 * The shared runtime for every invocation kind. Network-free: pools and clients
 * connect lazily. The analysis source field is carried raw and not validated here.
 */
export function runtimeFromSecret(env: LambdaEnv, secretText: string, schemaGeneration: string, composition?: SearchComposition): Runtime {
  const secret = parseSecret(secretText);
  const db = supabasePool(secret.hoopdb_url, env.dbPoolMax, env.postgresTls ?? postgresTlsOptions());
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
      publication:{schemaGeneration,transact:work=>transaction(db,async tx=>{
        await tx.query("SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s'");return work(tx);
      })},
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
    const portable = env.searchBackend && env.searchBackend !== "clickhouse";
    if (portable && !composition) throw new Error("search service composition is not registered");
    if (!portable && !secret.clickhouse) throw new Error("SEARCH_INDEX_ENABLED=1 but the secret has no clickhouse block");
    // Portable SQL joins the pass's metadata transaction. External indexes
    // retain their own service-native commit/rollback semantics.
    const writerSql = portable ? writerTransactionSql(db) : undefined;
    const binding = portable ? composeSearchService(composition!, { env: { ...env.adapterEnv, SEARCH_INDEX_BACKEND: env.searchBackend }, sql: writerSql!.sql, secrets: searchSecret(secret.search) }, "writer") : undefined;
    deps.search = {
      transact: (work) => transaction(db, tx => writerSql ? writerSql.run(tx, () => work(tx)) : work(tx)),
      reader,
      store,
      ...(binding ? { adapter: binding } : { ch: createClickHouseClient(secret.clickhouse!) }),
      prefixFor: (orgId) => `${env.keyPrefix}/${orgId}`,
      // Raised to the replay producer's source bound only when that stage is on.
      limits: searchIndexLimits(env.replayPagesEnabled),
      // Default off and separate from SEARCH_INDEX_ENABLED: off keeps the pre-#182 catalog
      // writer; the desktop catalog writer waits on migration 20261105031000 (docs/session-desktop-catalog.md).
      ...(env.desktopCatalogEnabled ? { desktopCatalog: true as const } : {}),
      // Default off; the fact tables are owner-applied DDL (output-facts-schema.ts).
      ...(env.outputFactsEnabled ? { outputFacts: true as const } : {}),
    };
  }
  let bufferedErasure: BufferedErasureDeps | undefined;
  if (env.bufferedSearchFeed?.enabled || env.bufferedErasureDelete?.enabled) {
    // Writes the legacy transcript_events table only; a portable search backend has no buffered feed.
    // Missing prerequisites disarm only this feed, never the shared runtime.
    // The feed RPCs admit only the buffered_search_feed_runtime login, so the feed never runs on the shared
    // hoopdb pool: it gets its own pinned single-connection pool, and every backend passes the login check
    // (buffered-feed-login.ts) before its first feed RPC. No login fields: the feed stays off.
    // The erasure lane calls only the `stage` RPC and shares that one pool.
    const feedConfig = deps.search?.ch ? bufferedFeedLogin(secret) : undefined;
    if (!deps.search?.ch) log.warn("buffered search feed disarmed", { stage: "buffered-search-feed", reason: "clickhouse_search_required" });
    else if (feedConfig) {
      const feedPool = new pg.Pool({ ...feedConfig, max: 1, idleTimeoutMillis: 60_000 });
      feedPool.on("error", (err) => log.error("buffered search feed pool error", { stage: "buffered-search-feed", error_code: (err as { code?: string }).code ?? "pool_error" }));
      const feedExecutor = loginCheckedFeedExecutor(feedPool);
      if (env.bufferedSearchFeed?.enabled) {
        const secondVersion = env.bufferedSearchFeed.secondVersion;
        deps.bufferedFeed = { source: sqlBufferedFeedSource(feedExecutor, secondVersion ? { secondVersion } : {}), store, ch: deps.search.ch, keyRoot: env.keyPrefix,
          ...(secondVersion ? { secondVersion: { ledger: clickhouseTieLedger(deps.search.ch) } } : {}) };
      }
      // The erasure lane reads only `stage` status and retention, never sameSecond or createdAt, so it keeps the
      // legacy (non-strict) source: its deletes do not depend on 045010 or the version flag.
      if (env.bufferedErasureDelete?.enabled) bufferedErasure = { source: sqlBufferedFeedSource(feedExecutor), ch: deps.search.ch };
    }
  }
  if (env.replayPagesEnabled) {
    // This stage needs scoped Put/Get on normalized read artifacts in addition to raw reads.
    // Wiring code does not grant that permission or enable this default-off flag.
    deps.replay = { db, store, provider: "s3", bucket: env.bucket, prefix: bindings };
  }
  if(env.largeScopeHandoffEnabled&&(env.searchEnabled||env.replayPagesEnabled))deps.large={route:scope=>routeLargeRead(db,scope),jobState:scope=>largeReadJobState(db,scope)};
  // The key comes from the secret only; "-disabled" or blank means no model, as in config.ts.
  const key = modelKey({ OPENAI_API_KEY: secret.openai_api_key });
  const brief = briefTick({
    reader,
    store,
    transport: key ? createAnalysisTransport({ apiKey: () => key }) : null,
    config: briefConfig(),
  });
  // Session analysis (lane F1): the same secret-only model key; no key = no claim.
  const sessionAnalysis = analysisTick({
    config: analysisConfig(),
    db,
    store,
    canonical: {
      store,
      provider: "s3",
      bucket: env.bucket,
      prefix: bindings,
      decompress: (bytes, maxBytes) => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: maxBytes })),
    },
    transport: key ? createAnalysisTransport({ apiKey: () => key }) : null,
    transact: (work) => transaction(db, work),
    // The bucket is versioned: the sweeper erases, by exact version id and in resumable strict
    // pages, every version of a ticketed result key that no published job owns.
    // Built per call so each step signs with the current Lambda credentials.
    eraser: {
      eraseStep: (prefix, opts) => createS3Eraser({ bucket: env.bucket, region: env.region, credentials: lambdaCredentials() }).eraseStep(prefix, opts),
      verifyErased: (prefix, opts) => createS3Eraser({ bucket: env.bucket, region: env.region, credentials: lambdaCredentials() }).verifyErased(prefix, opts),
    },
  });
  log.info("session pipeline ready", { stage: "init", ok: true, state: env.searchEnabled ? "search_on" : "search_off" });
  // Semantic copier fields are carried raw and validated only in semanticCopyDeps().
  const semanticSecret = env.semanticCopyEnabled ? { hoopdbUrl: secret.hoopdb_url, clickhouse: secret.clickhouse, sourceUrl: secret.analysis_source_readonly_url } : undefined;
  // Neon-exit: the readonly source URL is dropped here, so no later branch can build its pool.
  return { env, deps, brief, sessionAnalysis, store, analysisSourceUrl: env.neonExit ? undefined : secret.analysis_source_readonly_url, semanticSecret, ...(bufferedErasure ? { bufferedErasure } : {}) };
}

/**
 * The semantic copier, validated and built on first use from the semantic-copy
 * branch. Incomplete or malformed config fails only that invocation; the shared
 * runtime (SQS/search/reconcile/brief) is unaffected and not invalidated.
 */
export function semanticCopyDeps(rt: Runtime): () => Promise<SemanticCopyStats> {
  if (!rt.env.semanticCopyEnabled || !rt.semanticSecret) throw new Error("semantic copier unavailable");
  if (!rt.semanticCopy) {
    const { hoopdbUrl, clickhouse, sourceUrl } = rt.semanticSecret, sourceId = rt.env.semanticSourceId;
    // Reason codes only: URL parse errors can carry the URL, so their text is never surfaced.
    if (!clickhouse || !sourceUrl || !sourceId) throw Object.assign(new Error("semantic copier configuration incomplete"), { reason: "config_incomplete" });
    let source: SemanticPostgresExecutor, db: SemanticPostgresExecutor;
    try {
      source = semanticFeatureReader(semanticReadonlyPool(analysisSourceReadonlyUrl(sourceUrl)));
      db = new SemanticPostgresExecutor(semanticMetadataPool(hoopdbUrl));
    } catch { throw Object.assign(new Error("semantic copier configuration invalid"), { reason: "config_invalid" }); }
    const ch = createSemanticClickHouseClient(clickhouse), endpoint = clickhouse.url;
    rt.semanticCopy = () => runSemanticCopyTick({ db, source, ch, sourceId, endpoint });
  }
  return rt.semanticCopy;
}

/**
 * Analysis-only dependencies, validated and built on first use from the analysis
 * branches. A missing or malformed source field fails only that invocation; the
 * shared runtime (SQS/search/reconcile/brief) is unaffected and not invalidated.
 */
export function analysisDeps(rt: Runtime): AnalysisRefreshDeps {
  if (!rt.env.analysisCacheEnabled) throw new Error("analysis cache runtime unavailable");
  if (!rt.analysis) {
    const url = analysisSourceReadonlyUrl(rt.analysisSourceUrl);
    rt.analysis = { db: rt.deps.pointer.db, store: rt.store, source: readonlyAnalysisSource(readonlyAnalysisPool(url, code => log.error("analysis source pool error", { stage: "analysis-cache", error_code: code }))) };
  }
  return rt.analysis;
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

export function semanticTickState(stats:{failed:number;eventComplete:boolean;briefComplete:boolean;reasons:{event:string;brief:string}}):"failed"|"complete"|"idle"|"continuation"{
  if(stats.failed>0)return "failed";
  if(stats.eventComplete&&stats.briefComplete)return "complete";
  const quiet=(r:string)=>r==="idle"||r==="complete";
  return quiet(stats.reasons.event)&&quiet(stats.reasons.brief)?"idle":"continuation";
}



async function dispatchLambdaEvent(event: PipelineLambdaEvent, ready: () => Promise<Runtime>): Promise<BatchResponse | { ok: boolean }> {
  if(event?.semanticCopy===true){
    // Default off before secret lookup, connection pools or source access.
    if(process.env.SESSION_SEMANTIC_COPY_ENABLED!=="1")return {ok:true};
    const started=Date.now();try{const rt=await ready();const stats=await semanticCopyDeps(rt)();
      log.info("semantic copy tick",{stage:"semantic-copy",count:stats.published,claimed:stats.pages,failed:stats.failed,ok:stats.failed===0,state:semanticTickState(stats),reason:`event:${stats.reasons.event};brief:${stats.reasons.brief}`,duration_ms:Date.now()-started});
      return {ok:stats.failed===0};
    }catch(error){log.error("semantic copy tick failed",{stage:"semantic-copy",...errorFields(error),reason:semanticFailureReason(error),duration_ms:Date.now()-started});return {ok:false};}
  }
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
    return processBatch(records, rt.deps);
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
  if (event?.bufferedSearchFeed === true) {
    // Default off before secret lookup, connection pools or object access.
    if (process.env.INGEST_BUFFERED_SEARCH_FEED !== "on") return { ok: true };
    // The ordering guard, also before any secret or connection: no declared read gate, no run.
    // The version guard too: no run without the S encoding armed and its ceiling pinned.
    const refusal = bufferedFeedGateRefusal(process.env) ?? bufferedFeedVersionRefusal(process.env);
    if (refusal) {
      log.error("buffered search feed refused", { stage: "buffered-search-feed", reason: refusal });
      return { ok: false };
    }
    const started = Date.now();
    try {
      const rt = await ready();
      if (!rt.deps.bufferedFeed || !rt.env.bufferedSearchFeed) throw new Error("buffered search feed unavailable");
      const s = await runBufferedFeed(rt.deps.bufferedFeed, rt.env.bufferedSearchFeed);
      // A same-second refusal is never coverage: the run is not ok while any is recorded or outstanding.
      const refusals = (s.refused ?? 0) + (s.refusalsOutstanding ?? 0);
      if (refusals) log.warn("buffered search feed refusals", { stage: "buffered-search-feed", count: s.refused,
        reason: `same_second_tie:${s.refused ?? 0},outstanding:${s.refusalsOutstanding ?? 0}` });
      log.info("buffered search feed run", { stage: "buffered-search-feed", listed: s.stages, inserted: s.indexed, skipped: s.skipped,
        count: s.rows, deleted: s.deleted, ok: !s.blocked && !refusals,
        state: s.caughtUp ? (refusals ? "caught_up_with_refusals" : "caught_up") : s.blocked ? "blocked" : "continuation",
        reason: s.blocked ?? (Object.entries(s.reasons).map(([k, n]) => `${k}:${n}`).join(",") || undefined), duration_ms: Date.now() - started });
      return { ok: true };
    } catch (error) {
      log.error("buffered search feed failed", { stage: "buffered-search-feed", ...errorFields(error), duration_ms: Date.now() - started });
      return { ok: false };
    }
  }
  if (event?.bufferedErasureDelete === true) {
    // Default off before secret lookup, connection pools or ClickHouse access.
    if (process.env.INGEST_BUFFERED_ERASURE_DELETE !== "on") return { ok: true };
    // The feeder's ordering guard, also before any secret or connection.
    const refusal = bufferedFeedGateRefusal(process.env);
    if (refusal) {
      log.error("buffered erasure delete refused", { stage: "buffered-erasure-delete", reason: refusal });
      return { ok: false };
    }
    const started = Date.now();
    try {
      const rt = await ready();
      if (!rt.bufferedErasure || !rt.env.bufferedErasureDelete?.enabled) throw new Error("buffered erasure delete unavailable");
      const s = await runBufferedErasureDelete(rt.bufferedErasure, rt.env.bufferedErasureDelete);
      log.info("buffered erasure delete run", { stage: "buffered-erasure-delete", listed: s.pairs, count: s.checked, deleted: s.deleted,
        skipped: s.skipped, failed: s.failed, ok: s.failed === 0, state: s.caughtUp ? "caught_up" : "continuation",
        reason: Object.entries(s.reasons).map(([k, n]) => `${k}:${n}`).join(",") || undefined, duration_ms: Date.now() - started });
      return { ok: true };
    } catch (error) {
      log.error("buffered erasure delete failed", { stage: "buffered-erasure-delete", ...errorFields(error), duration_ms: Date.now() - started });
      return { ok: false };
    }
  }
  if(event?.analysisCacheReconcile===true){
    if(process.env.SESSION_ANALYSIS_CACHE_ENABLED!=="1")return {ok:true};
    const started=Date.now();try{const rt=await ready();const analysis=analysisDeps(rt);
      const page=await reconcileAnalysisPage({source:analysis.source,enqueue:identity=>enqueueAnalysisScope(analysis,identity)},{cursor:checkedAnalysisCursor(event.analysisCursor)});
      if(page.nextCursor)await invokeAsync({region:rt.env.region,functionName:process.env.AWS_LAMBDA_FUNCTION_NAME??"",payload:JSON.stringify({analysisCacheReconcile:true,analysisCursor:page.nextCursor}),credentials:lambdaCredentials()});
      log.info("analysis cache reconcile page",{stage:"analysis-cache",count:page.scanned,enqueued:page.accepted,skipped:page.unavailable,failed:page.invalidScope+page.refused,ok:page.unavailable===0&&page.invalidScope===0&&page.refused===0,reason:page.complete?"scan_complete":"continuation",duration_ms:Date.now()-started});return {ok:true};
    }catch(error){log.error("analysis cache reconcile failed",{stage:"analysis-cache",...errorFields(error),duration_ms:Date.now()-started});throw new Error("analysis cache reconcile failed");}
  }
  if(event?.analysisCache===true){
    // Default off before secret lookup, connection pools or object access.
    if(process.env.SESSION_ANALYSIS_CACHE_ENABLED!=="1")return {ok:true};
    const started=Date.now();try{const rt=await ready();const analysis=analysisDeps(rt);
      const first=await analysisRefreshTick(analysis,{maxJobs:10,maxMs:24000});
      // One-minute schedule, two bounded passes. Cadence remains a target until
      // actual source/network/load execution is measured; leases make overlap safe.
      if(Date.now()-started<30000)await new Promise(resolve=>setTimeout(resolve,30000-(Date.now()-started)));
      const second=Date.now()-started<50000?await analysisRefreshTick(analysis,{maxJobs:10,maxMs:24000}):{claimed:0,published:0,failed:0,lost:0};
      log.info("analysis cache refresh finished",{stage:"analysis-cache",count:first.published+second.published,failed:first.failed+second.failed+first.lost+second.lost,duration_ms:Date.now()-started});
      return {ok:first.failed+second.failed+first.lost+second.lost===0};
    }catch(error){log.error("analysis cache refresh failed",{stage:"analysis-cache",...errorFields(error),duration_ms:Date.now()-started});return {ok:false};}
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
  if (event?.sessionAnalysis === true) {
    // Off: return before init (no secret read, no pool), as the brief tick does.
    if (!analysisConfig().enabled) return { ok: true };
    const started = Date.now();
    try {
      const rt = await ready();
      const counts = await rt.sessionAnalysis();
      // A job that fails is a recorded job outcome (its code is on the row), not a tick error.
      log.info("session analysis tick", { stage: "session-analysis", count: counts.completed ?? 0, failed: counts.failed ?? 0, skipped: counts["lease-lost"] ?? 0, duration_ms: Date.now() - started });
      return { ok: true };
    } catch (error) {
      log.error("session analysis tick failed", { stage: "session-analysis", ...errorFields(error), duration_ms: Date.now() - started });
      // Surface it: the invocation fails, so the Lambda Errors metric (and its alarm) and the
      // schedule's async retry see it; a swallowed {ok:false} would be invisible.
      throw new Error("session analysis tick failed");
    }
  }
  log.warn("unknown invocation", { stage: "init", reason: "unknown_event" });
  return { ok: false };
}


/** Each composition owns its cold-start cache. Default export retains the
 * legacy service path; an explicit reviewed entrypoint supplies registries. */
export function createPipelineLambdaHandler(options: { search?: SearchComposition; runtime?: PipelineRuntimeRegistry; loadRuntime?: () => Promise<Runtime> } = {}) {
  let pending: Promise<Runtime> | undefined;
  const ready = options.loadRuntime ?? (() => {
    pending ??= init(options.search).catch(error => { pending = undefined; throw error; });
    return pending;
  });
  return async (event: PipelineLambdaEvent): Promise<BatchResponse | { ok: boolean }> => {
    const invoke = (input: PipelineLambdaEvent) => dispatchLambdaEvent(input, ready);
    if (!options.runtime) return invoke(event);
    try {
      if (process.env.PIPELINE_RUNTIME !== "lambda") throw new Error("registered Lambda runtime must be selected explicitly");
      const adapter = options.runtime.create({ PIPELINE_RUNTIME: "lambda" }, { env: { PIPELINE_RUNTIME: "lambda" } }, ["lambda"]);
      return await dispatchRegisteredLambda(adapter, event, invoke);
    } catch (error) {
      if (Array.isArray(event.Records)) {
        log.error("Lambda runtime dispatch failed", { stage: "init", ...errorFields(error) });
        return { batchItemFailures: event.Records.map(record => ({ itemIdentifier: record.messageId })) };
      }
      // A selected platform runtime throws failed schedules for platform retry.
      throw new Error("Lambda runtime schedule failed");
    }
  };
}

export const handler = createPipelineLambdaHandler();
