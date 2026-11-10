/**
 * The 96Bulls session read API as a Cloudflare Worker: the same app as the
 * #135 Lambda (src/session-read-api: routes, auth, cursors, the two-step
 * ClickHouse dedupe, the history floor and erasure), with a Worker adapter in
 * place of lambda.ts. Nothing on the request path runs on Lambda.
 *
 *   ClickHouse  ClickHouse Cloud over HTTPS with the dedicated SELECT-only read
 *               user (Worker secrets CLICKHOUSE_URL / _USER / _PASSWORD), the
 *               relayhistory-cloud search-clickhouse.ts request bounds.
 *   Supabase    the `bulls96_read_api` role through Hyperdrive (binding
 *               SUPABASE, caching disabled, verify-full against Supabase's CA).
 *               One pg client per request; every check is one READ ONLY
 *               transaction that binds the tenant settings the role's RLS
 *               policy reads (liveness.ts TENANT_SETTINGS_SQL).
 *
 * Env (wrangler.jsonc vars, set by the deploy workflow): READ_API_ORIGIN (this
 * Worker's own origin, the only audience accepted; unset = every /v1 request
 * 401), READ_API_COMPLETE_SINCE (ISO timestamp or "epoch"; missing = 503),
 * CLICKHOUSE_DATABASE, READ_API_DIRECTORY_WORKSPACES (the directory list/activity
 * and timing canary allowlist; unset = none), READ_API_TIMING_ASSUME_NO_TURNS,
 * READ_API_OUTPUT_ENABLED ("1" only: GET /v1/sessions/output from the ClickHouse facts,
 * which needs the owner-applied output-facts DDL; default off = 501),
 * READ_API_DIGEST_ENABLED ("1" only: GET /v1/digest/daily; its narrative needs migration
 * 20261105045000 and the DIGEST_NARRATIVE service binding, without which every miss is
 * `provider_not_configured`; default off = 501),
 * READ_API_COORDINATION_OVERLAPS_ENABLED (exactly "true": GET /v1/coordination/overlaps from
 * the existing ClickHouse facts; every other value = 404),
 * READ_API_BRIEF_TITLES_WORKSPACES (the brief-titles allowlist; unset = none), and
 * RELAYAUTH_JWKS_URL / RELAYAUTH_ISSUER (production
 * values in wrangler.jsonc; the verifier's fallback JWKS does not resolve), and
 * RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS with RELAYCONTROL_INTROSPECT_URL /
 * RELAYCONTROL_INTROSPECT_SECRET (relaycontrol `sh_at_` tokens; default off,
 * unset in wrangler.jsonc).
 *
 * Owner-only buffered reads (src/session-read-api/owner-buffered.ts, Option C):
 * RELAYHISTORY_READ_OWNER_BUFFERED (default off; only "1" or "true" enables), with
 * INGEST_S3_BUCKET / INGEST_S3_REGION naming the ingest bucket that holds the
 * buffered-batches/ stage objects, read with the PIPELINE_RAW_S3_ACCESS_KEY_ID /
 * _SECRET_ACCESS_KEY principal (GetObject + kms:Decrypt on buffered-batches/ is a
 * separate IAM grant). Off, or any of those missing: no buffered read, every route
 * exactly as without the flag. Every buffered response passes the privacy authority gate,
 * the HISTORY_CONTROL_HEAD service binding to relayhistory-cloud's named entrypoint
 * SessionReadPrivacyControls (pinned with `entrypoint` in wrangler config; never a public
 * route), R2 control-head contract v5, wire frozen at relayhistory-cloud #328 a06a58f9:
 * GET /v1/session-read/control-head, no query string, no body, identity only in
 * x-relayhistory-org-id / -workspace-id / -user-id from the verified token, 2 s deadline,
 * no-store both ways, no caching. Flag on with the binding absent: every request answers 503
 * backend_unavailable (logged missing_HISTORY_CONTROL_HEAD), the same fail-closed rule as a missing
 * Hyperdrive binding. READ_API_OWNER_BUFFERED_WORKSPACES is the owner-buffered canary allowlist (on
 * top of READ_API_DIRECTORY_WORKSPACES; unset = none; "*" = all-workspaces mode, which the deploy
 * render accepts only with READ_API_DIRECTORY_WORKSPACES="*" and which never bypasses the per-user
 * privacy gate). READ_API_OWNER_BUFFERED_EVENTS=on (default off; any value but unset/empty/"on" answers
 * 503) adds owner-buffered session events on GET /v1/sessions/:sessionId/events with `source`; it
 * has no effect without the owner-buffered flag and allowlists. READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY
 * (default unset = the serial stage fetch; "0"/"1" the same; "2".."32" = that many stage GetObjects
 * in flight per buffered events read; any other value answers 503) and READ_API_OWNER_BUFFERED_PHASE_LOG
 * (default off; exactly "1" adds per-phase *_ms fields to the request log line) apply to the
 * owner-buffered events read only. READ_API_OWNER_BUFFERED_INDEX_VERIFY=on (default off; any value
 * but unset/empty/"on" answers 503) makes every owner-buffered read first prove, through the
 * catalog on the same read connection, that the out-of-band index
 * ingest_buffer_outbox_user_buffered_idx exists and is valid (cached per isolate: valid for its
 * lifetime, a failure re-probed after 60 s); not proven, the buffered read is fenced before any
 * candidate (detail routes answer as without the flag, the list as fenced).
 * READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS (default unset = 2000; "2000".."6000" = the
 * per-transaction statement_timeout of both owner-buffered RPCs; any other value answers 503) never
 * changes the role default or any other read. Flag, allowlist, bucket and binding are
 * all unset in wrangler.jsonc; only the deploy workflow's owner_buffered=on render
 * (scripts/render-deploy-config.ts) adds them.
 */
import { readerMaxRuleset } from "../../pipeline/src/lib/scrub-engine/reader-policy.ts";
import pg from "pg";
import { Decompress, decompress } from "fzstd";
import { canonicalAnalysisHandle } from "./analysis-cache.ts";
import { canonicalHandle, type CanonicalEnv } from "./canonical.ts";
import { endPg, isPgEnding, pgClientErrorLog } from "./pg-close.ts";
import { rawStoreFromEnv } from "../../pipeline/src/lib/pipeline/adapter.ts";
import { canonicalSnapshot, CanonicalReadError } from "../../../src/session-read-api/canonical.ts";
import { sqlCanonicalPointers } from "../../../src/session-read-api/session-timing-source.ts";
import { sqlSessionHarnesses } from "../../../src/session-read-api/session-turns-source.ts";
import type { TimingDeps, TurnsDeps } from "../../../src/session-read-api/app.ts";
import { sqlEvidenceDeps, type EvidenceDeps } from "../../../src/session-read-api/babysitter-evidence.ts";
import { objectKey, readAll } from "../../../src/shared/vendor.ts";
import { logLine, type Level } from "../../../src/shared/log.ts";
import { createClickHouseClient, type ClickHouseClient, type RequestOptions } from "../../../src/session-pipeline/vendor/search-index/clickhouse.ts";
import { createReadApi, parseDirectoryWorkspaces } from "../../../src/session-read-api/app.ts";
import { sqlDirectory } from "../../../src/session-read-api/directory.ts";
import { serviceNarrativeGenerator, sqlNarrativeCache } from "../../../src/session-read-api/digest-narrative.ts";
import { sqlBriefTitles } from "../../../src/session-read-api/brief-titles.ts";
import { readApiVerifier, type AuthConfig } from "../../../src/session-read-api/auth.ts";
import { assertDestinationUrl, destinationPolicy } from "../../../src/shared/destination-url.ts";
import { repeatableReadEnabled, sqlLiveness, TENANT_SETTINGS_SQL, tenantBeginSql, type SqlQuery, type TenantRunner } from "../../../src/session-read-api/liveness.ts";
import { coordinationOverlapsEnabled, parseCompleteSince } from "../../../src/session-read-api/config.ts";
import { AnalyticsConfigError, analyticsConfig, analyticsCounterFields, createAnalyticsSink, type AnalyticsConfig } from "../../../src/shared/analytics-config.ts";
import {
  createBufferedSearchGate,
  parseBufferedSearchGate,
  serviceBindingControlHeads,
  sqlGateStore,
  type BufferedSearchGate,
  type ServiceBinding,
} from "../../../src/session-read-api/buffered-search-gate.ts";
import { createS3Store } from "../../../src/shared/vendor.ts";
import {
  BufferedIndexVerifier,
  boundedStreamDecompress,
  ownerBufferedEnabled,
  ownerBufferedEventsEnabled,
  ownerBufferedIndexVerifyEnabled,
  ownerBufferedPhaseLogEnabled,
  ownerBufferedStageConcurrency,
  ownerBufferedStatementTimeoutMs,
  serviceBindingControlHead,
  sqlBufferedIndexProbe,
  sqlOwnerBufferedReads,
  type OwnerBufferedDeps,
} from "../../../src/session-read-api/owner-buffered.ts";

export interface Env extends CanonicalEnv {
  SUPABASE: Hyperdrive;
  CLICKHOUSE_URL?: string;
  CLICKHOUSE_USER?: string;
  CLICKHOUSE_PASSWORD?: string;
  CLICKHOUSE_DATABASE?: string;
  READ_API_ORIGIN?: string;
  READ_API_COMPLETE_SINCE?: string;
  /** Directory list/activity and timing canary allowlist: unset = none, "*" = all, else comma-separated workspace ids. */
  READ_API_DIRECTORY_WORKSPACES?: string;
  /** Brief-titles allowlist (GET /v1/sessions/titles): unset = none, "*" = all, else comma-separated workspace ids. */
  READ_API_BRIEF_TITLES_WORKSPACES?: string;
  /** "true": time a session without legacy turn evidence as having none (session-timing-source.ts). */
  READ_API_TIMING_ASSUME_NO_TURNS?: string;
  /** "1": serve GET /v1/sessions/output from the ClickHouse facts; anything else = 501 output_not_configured. */
  READ_API_OUTPUT_ENABLED?: string;
  /** "1": serve GET /v1/digest/daily; anything else = 501 digest_not_configured. */
  READ_API_DIGEST_ENABLED?: string;
  /** Exactly "true": serve GET /v1/coordination/overlaps from ClickHouse facts. */
  READ_API_COORDINATION_OVERLAPS_ENABLED?: string;
  /** The digest narrative Worker (wrangler.digest-narrative.jsonc), bound at activation only. */
  DIGEST_NARRATIVE?: Fetcher;
  /** Babysitter evidence reader (GET /v1/delivery/records for its kinds): only "on" enables; default off. */
  READ_API_BABYSITTER_EVIDENCE?: string;
  /** The ingest key root the retention buckets live under (default 96bulls). */
  READ_API_BABYSITTER_KEY_ROOT?: string;
  /**
   * The per-hit R2 privacy gate for buffered-origin search rows: only "on" enables; default off
   * (src/session-read-api/buffered-search-gate.ts). It reads the same privacy authority as
   * owner-buffered reads, the HISTORY_CONTROL_HEAD binding below; on without it, every
   * buffered-origin row is dropped.
   */
  READ_API_BUFFERED_SEARCH_GATE?: string;
  /**
   * Tenant read transactions at REPEATABLE READ: only "1" enables; unset/"0" (default) keeps
   * `BEGIN READ ONLY` at READ COMMITTED (see requestTenantRunner).
   */
  READ_API_REPEATABLE_READ?: string;
  RELAYAUTH_JWKS_URL?: string;
  RELAYAUTH_ISSUER?: string;
  /** Exactly "true" also accepts relaycontrol `sh_at_` tokens; default off. */
  RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS?: string;
  RELAYCONTROL_INTROSPECT_URL?: string;
  RELAYCONTROL_INTROSPECT_SECRET?: string;
  /** Owner-only buffered reads (Option C): only "1" or "true" enables; default off. */
  RELAYHISTORY_READ_OWNER_BUFFERED?: string;
  /**
   * Owner-buffered session events (GET /v1/sessions/:sessionId/events with `source`): unset or
   * empty = off (default), exactly "on" enables, any other value refuses every request (503,
   * logged missing_READ_API_OWNER_BUFFERED_EVENTS). No effect unless
   * RELAYHISTORY_READ_OWNER_BUFFERED is on and the workspace is on both allowlists.
   */
  READ_API_OWNER_BUFFERED_EVENTS?: string;
  /**
   * Owner-buffered events read: the most S3 stage GetObjects in flight. Unset, empty, "0" or "1"
   * = the serial fetch (default); an integer "2".."32" = bounded concurrency; any other value
   * refuses every request (503, logged missing_READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY).
   */
  READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY?: string;
  /**
   * Owner-buffered events read: exactly "1" adds the per-phase fields (session_start_ms ..
   * fold_ms, s3_stage_count, aborted_phase) to the request log line; anything else is off (default).
   */
  READ_API_OWNER_BUFFERED_PHASE_LOG?: string;
  /**
   * Owner-buffered RPCs (list and detail): the per-transaction statement_timeout in ms. Unset or
   * empty = 2000 (default, the read role's own bound); an integer 2000..6000 = that bound; any
   * other value refuses every request (503, logged missing_READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).
   */
  READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS?: string;
  /**
   * Owner-buffered reads: verify the out-of-band index ingest_buffer_outbox_user_buffered_idx
   * (valid, per the catalog) before serving any buffered read. Unset or empty = off (default, no
   * query), exactly "on" enables, any other value refuses every request (503, logged
   * missing_READ_API_OWNER_BUFFERED_INDEX_VERIFY). No effect unless RELAYHISTORY_READ_OWNER_BUFFERED
   * is on and the workspace is on both allowlists.
   */
  READ_API_OWNER_BUFFERED_INDEX_VERIFY?: string;
  /** The owner-buffered canary allowlist (parseDirectoryWorkspaces syntax); unset/empty = none. */
  READ_API_OWNER_BUFFERED_WORKSPACES?: string;
  /** The ingest bucket holding buffered-batches/ stage objects (owner-buffered reads only). */
  INGEST_S3_BUCKET?: string;
  /** Its region; default us-east-1. */
  INGEST_S3_REGION?: string;
  /**
   * The privacy authority for owner-buffered reads: a service binding to relayhistory-cloud's
   * named entrypoint SessionReadPrivacyControls (control-head contract v5, frozen wire
   * #328 a06a58f9, GET /v1/session-read/control-head). Unbound = every buffered read fenced.
   */
  HISTORY_CONTROL_HEAD?: Fetcher;
  /**
   * Product analytics (src/shared/analytics-config.ts, hosted profile): unset, empty or "noop" = off
   * (default); "posthog" sends one session_read.request_completed per verified /v1 request to
   * ANALYTICS_POSTHOG_HOST (default https://i.agentrelay.com) with the ANALYTICS_POSTHOG_KEY secret and
   * ANALYTICS_DEPLOYMENT_ID, flushed in waitUntil after the response (at most 2 subrequests). Any other
   * value, or "collector", refuses every request (503, logged missing_<variable>).
   */
  ANALYTICS_SINK?: string;
  ANALYTICS_DEPLOYMENT_ID?: string;
  ANALYTICS_POSTHOG_HOST?: string;
  ANALYTICS_POSTHOG_KEY?: string;
}

/** The read verifier's config from Worker env; the introspection URL passes the https/loopback rule. */
export function workerAuthConfig(env: Env): AuthConfig {
  const config: AuthConfig = { origin: env.READ_API_ORIGIN?.trim() || undefined, jwksUrl: env.RELAYAUTH_JWKS_URL?.trim() || undefined, issuer: env.RELAYAUTH_ISSUER?.trim() || undefined };
  if (env.RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS === "true") {
    const url = env.RELAYCONTROL_INTROSPECT_URL?.trim() || undefined;
    if (url) assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, destinationPolicy({}));
    config.relaycontrol = { url, secret: env.RELAYCONTROL_INTROSPECT_SECRET?.trim() || undefined };
  }
  return config;
}

const SERVICE = "96bulls-session-read-worker";

export function log(level: Level, message: string, fields: Record<string, unknown> = {}): void {
  const line = logLine(level, message, { service: SERVICE, ...fields });
  if (level === "error" || level === "warn") console.error(line);
  else console.log(line);
}

/**
 * Per-query bounds on top of the read API's READ_SETTINGS, as relayhistory-cloud's
 * search-clickhouse.ts sets them: the server cancels a read-only query whose
 * client went away (our AbortSignal timeout), and threads per query are capped
 * so one request cannot take the whole service.
 */
export const WORKER_CH_SETTINGS = {
  cancel_http_readonly_queries_on_client_close: 1,
  max_threads: 4,
  use_skip_indexes: 1,
} as const;

/** Adds WORKER_CH_SETTINGS to every select; the read path never writes. */
export function boundedClickHouse(inner: ClickHouseClient): ClickHouseClient {
  const withBounds = <O extends RequestOptions>(o: O | undefined): O => ({ ...(o ?? ({} as O)), settings: { ...WORKER_CH_SETTINGS, ...(o?.settings ?? {}) } });
  return {
    database: inner.database,
    select: (query, options) => inner.select(query, withBounds(options)),
    command: () => Promise.reject(new Error("the session read Worker does not write to ClickHouse")),
    insert: () => Promise.reject(new Error("the session read Worker does not write to ClickHouse")),
  };
}

export class ConfigError extends Error {
  constructor(readonly field: string) {
    super(`session read worker: ${field} is not configured`);
    this.name = "ConfigError";
  }
}

/** ClickHouse connection from Worker secrets, trimmed as relayhistory-cloud reads them. Never echoes a value. */
export function clickHouseConfig(env: Env) {
  const url = env.CLICKHOUSE_URL?.trim();
  const user = env.CLICKHOUSE_USER?.trim();
  const password = env.CLICKHOUSE_PASSWORD;
  const database = env.CLICKHOUSE_DATABASE?.trim();
  if (!url) throw new ConfigError("CLICKHOUSE_URL");
  if (!user) throw new ConfigError("CLICKHOUSE_USER");
  if (!password) throw new ConfigError("CLICKHOUSE_PASSWORD");
  if (!database) throw new ConfigError("CLICKHOUSE_DATABASE");
  return { url, user, password, database };
}

/**
 * A TenantRunner over one lazily connected pg client for the request. The
 * Hyperdrive guidance is a client per request (Hyperdrive pools the origin
 * connections), closed after the response. Checks are serialized on the one
 * client, so two transactions never interleave on it.
 *
 * Isolation (READ_API_REPEATABLE_READ, default off): off, each transaction is
 * `BEGIN READ ONLY` at READ COMMITTED, so every statement takes its own
 * snapshot. On ("1"), it is REPEATABLE READ READ ONLY: every statement reads
 * ONE snapshot (taken at its first statement), so a check that issues several
 * statements (the buffered search gate's 500-hit chunks, the owner-buffered
 * scope then list) judges them all against the same database state. Every
 * consumer only reads, and a read-only REPEATABLE READ transaction never fails
 * serialization. It is flagged because Hyperdrive's handling of it is unproven.
 */
export function requestTenantRunner(
  connect: () => Promise<pg.Client>,
  options: { repeatableRead?: boolean } = {},
): { run: TenantRunner; close(): Promise<void> } {
  const begin = tenantBeginSql(options.repeatableRead === true);
  let client: Promise<pg.Client> | undefined;
  let tail: Promise<unknown> = Promise.resolve();
  const run: TenantRunner = (orgId, workspaceId, work) => {
    const next = tail.then(async () => {
      const c = await (client ??= connect());
      await c.query(begin);
      try {
        await c.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
        const q: SqlQuery = {
          async query<R>(text: string, params?: unknown[]) {
            const r = await c.query(text, params);
            return { rows: r.rows as R[] };
          },
        };
        const result = await work(q);
        await c.query("COMMIT");
        return result;
      } catch (error) {
        await c.query("ROLLBACK").catch(() => {});
        throw error;
      }
    });
    tail = next.catch(() => {});
    return next;
  };
  return {
    run,
    async close() {
      if (!client) return;
      const c = await client.catch(() => undefined);
      if (c) await endPg(c).catch(() => {});
    },
  };
}

/**
 * A connected pg client over the Workers socket. pg-cloudflare's read loop
 * emits `error` ("This socket has been closed") on the socket once `end()` has
 * closed it, after pg has removed its own listener, which would surface as an
 * unhandled rejection on every request. The extra listener absorbs only that;
 * an error while a query runs still reaches pg's listener and fails the query.
 */
export async function connectPg(connectionString: string): Promise<pg.Client> {
  const client = new pg.Client({ connectionString, application_name: SERVICE, connectionTimeoutMillis: 2_000 });
  client.on("error", (err) => {
    const line = pgClientErrorLog(isPgEnding(client), err);
    log(line.level, line.message, { error_code: line.error_code });
  });
  await client.connect();
  (client as unknown as { connection?: { stream?: { on(event: "error", fn: () => void): void } } }).connection?.stream?.on("error", () => {});
  return client;
}

export function hyperdriveClient(hyperdrive: Hyperdrive): () => Promise<pg.Client> {
  return () => connectPg(hyperdrive.connectionString);
}

const json = (status: number, code: string, message: string, extra: Record<string, string> = {}) =>
  new Response(JSON.stringify({ error: { code, message } }), {
    status,
    headers: { "content-type": "application/json", "cache-control": "private, no-store", "x-sessions-read-backend": "96bulls", ...extra },
  });

export interface Overrides {
  /** Tests: the pg client factory in place of Hyperdrive's. */
  connect?: () => Promise<pg.Client>;
  fetcher?: typeof fetch;
}

export async function handle(request: Request, env: Env, ctx: ExecutionContext, overrides: Overrides = {}): Promise<Response> {
  if (new URL(request.url).pathname.startsWith("/v1/canonical/analyses/")) return canonicalAnalysisHandle(request, env, overrides.fetcher);
  if (new URL(request.url).pathname.startsWith("/v1/canonical/")) return canonicalHandle(request, env, overrides.fetcher);
  // GET only, as the Lambda's Function URL adapter (HEAD is not a read the contract has).
  if (request.method !== "GET") return json(405, "method_not_allowed", "GET only", { allow: "GET" });
  let ch: ClickHouseClient;
  let completeSinceMs: number;
  let authConfig: AuthConfig;
  let analyticsSelection: AnalyticsConfig;
  try {
    completeSinceMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
    try {
      analyticsSelection = analyticsConfig(env as unknown as Record<string, unknown>, "hosted");
    } catch (error) {
      throw new ConfigError(error instanceof AnalyticsConfigError ? error.variable : "ANALYTICS_SINK");
    }
    authConfig = workerAuthConfig(env);
    ch = boundedClickHouse(createClickHouseClient(clickHouseConfig(env), overrides.fetcher));
    if (!overrides.connect && !env.SUPABASE?.connectionString) throw new ConfigError("SUPABASE");
    // Owner-buffered on needs its privacy authority bound; refuse every request rather than run
    // a deploy whose flag says on and whose gate can only fence.
    if (ownerBufferedEnabled(env.RELAYHISTORY_READ_OWNER_BUFFERED) && typeof env.HISTORY_CONTROL_HEAD?.fetch !== "function") throw new ConfigError("HISTORY_CONTROL_HEAD");
    // The events flag accepts only unset/empty or "on"; anything else is a misconfigured deploy.
    try {
      ownerBufferedEventsEnabled(env.READ_API_OWNER_BUFFERED_EVENTS);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_EVENTS");
    }
    // The stage concurrency accepts only unset/empty, "0", "1" or "2".."32"; anything else is a misconfigured deploy.
    try {
      ownerBufferedStageConcurrency(env.READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY");
    }
    // The buffered statement timeout accepts only unset/empty or 2000..6000; anything else is a misconfigured deploy.
    try {
      ownerBufferedStatementTimeoutMs(env.READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS");
    }
    // The index verify flag accepts only unset/empty or "on"; anything else is a misconfigured deploy.
    try {
      ownerBufferedIndexVerifyEnabled(env.READ_API_OWNER_BUFFERED_INDEX_VERIFY);
    } catch {
      throw new ConfigError("READ_API_OWNER_BUFFERED_INDEX_VERIFY");
    }
  } catch (error) {
    // Configuration errors name a field, never a value.
    log("error", "read worker not configured", { error_code: error instanceof ConfigError ? `missing_${error.field}` : (error as { name?: string })?.name ?? "config" });
    return json(503, "backend_unavailable", "read backend unavailable");
  }
  const tenant = requestTenantRunner(overrides.connect ?? hyperdriveClient(env.SUPABASE), {
    repeatableRead: repeatableReadEnabled(env.READ_API_REPEATABLE_READ),
  });
  // One sink per invocation (no state crosses requests): captured during the request, exported once in waitUntil.
  const analytics = createAnalyticsSink(analyticsSelection, "session-read-api", "worker", {
    ...(overrides.fetcher ? { fetch: overrides.fetcher } : {}),
    report: (counters) => log("warn", "analytics export degraded", analyticsCounterFields(analyticsSelection.sink, counters)),
  });
  const app = createReadApi({
    ...(analyticsSelection.sink !== "noop" ? { analytics } : {}),
    timing: timingDeps(env, tenant.run, overrides.fetcher),
    turns: turnsDeps(env, tenant.run, overrides.fetcher),
    babysitter: babysitterEvidenceDeps(env, tenant.run, overrides.fetcher),
    ownerBuffered: ownerBufferedDeps(env, tenant.run, overrides.fetcher),
    verify: readApiVerifier(authConfig),
    ch,
    liveness: sqlLiveness(tenant.run),
    directory: sqlDirectory(tenant.run),
    directoryWorkspaces: parseDirectoryWorkspaces(env.READ_API_DIRECTORY_WORKSPACES),
    ...(env.READ_API_OUTPUT_ENABLED?.trim() === "1" ? { output: { enabled: true as const } } : {}),
    ...(env.READ_API_DIGEST_ENABLED?.trim() === "1"
      ? { digest: { cache: sqlNarrativeCache(tenant.run), ...(env.DIGEST_NARRATIVE ? { generator: serviceNarrativeGenerator(env.DIGEST_NARRATIVE) } : {}) } }
      : {}),
    ...(coordinationOverlapsEnabled(env.READ_API_COORDINATION_OVERLAPS_ENABLED) ? { coordination: { enabled: true as const } } : {}),
    briefTitles: sqlBriefTitles(tenant.run),
    briefTitleWorkspaces: parseDirectoryWorkspaces(env.READ_API_BRIEF_TITLES_WORKSPACES),
    completeSinceMs,
    bufferedSearchGate: bufferedSearchGate(env, tenant.run),
    log: (fields) => log("info", "session read", fields),
  });
  try {
    return await app.fetch(request);
  } finally {
    ctx.waitUntil(tenant.close());
    // Bounded (WORKER_LIMITS) and never rejecting; the response is already final.
    if (analyticsSelection.sink !== "noop") ctx.waitUntil(analytics.shutdown());
  }
}

/**
 * The buffered search gate, per request (control heads are never cached across requests).
 * Off unless READ_API_BUFFERED_SEARCH_GATE=on.
 */
export function bufferedSearchGate(env: Env, run: TenantRunner): BufferedSearchGate | undefined {
  if (!parseBufferedSearchGate(env.READ_API_BUFFERED_SEARCH_GATE)) return undefined;
  const binding: ServiceBinding | undefined = typeof env.HISTORY_CONTROL_HEAD?.fetch === "function" ? env.HISTORY_CONTROL_HEAD : undefined;
  if (!binding) log("warn", "buffered search gate on without HISTORY_CONTROL_HEAD: buffered rows are dropped", {});
  return createBufferedSearchGate({ controlHeads: binding ? serviceBindingControlHeads(binding) : undefined, store: sqlGateStore(run) });
}

/**
 * Session timing from canonical S3: pointers through the read role (tenant-bound), objects
 * with the raw-store secrets the /v1/canonical route uses (PIPELINE_RAW_S3_*). Without
 * those secrets the route answers 501 `timing_not_configured`.
 */
export function timingDeps(env: Env, run: Parameters<typeof sqlCanonicalPointers>[0], fetcher: typeof fetch = (input, init) => fetch(input, init)): TimingDeps | undefined {
  if (!rawStoreFromEnv(env, fetcher)) return undefined;
  const maximum = readerMaxRuleset(env);
  const pointers = sqlCanonicalPointers(run);
  return {
    state: pointers.state,
    assumeNoTurns: env.READ_API_TIMING_ASSUME_NO_TURNS?.trim() === "true",
    async source(identity, signal) {
      const bounded: typeof fetch = (input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal });
      const raw = rawStoreFromEnv(env, bounded);
      if (!raw) throw new CanonicalReadError("not_configured");
      const snapshot = await canonicalSnapshot(
        {
          ...raw,
          readerMaxRuleset: maximum,
          prefix: env,
          pointers: pointers.pointers,
          decompress: (bytes, max) => {
            const output = decompress(bytes, new Uint8Array(max));
            if (output.byteLength > max) throw new CanonicalReadError("chunk_bound");
            return output;
          },
        },
        identity,
      );
      return { rows: snapshot.rows, legacyCoverage: snapshot.legacyCoverage };
    },
  };
}

/**
 * Relay session turns and metadata from canonical S3, on the same pointers and raw-store
 * secrets as timing. Without the secrets the routes answer 501 `turns_not_configured`.
 */
export function turnsDeps(env: Env, run: Parameters<typeof sqlCanonicalPointers>[0], fetcher: typeof fetch = (input, init) => fetch(input, init)): TurnsDeps | undefined {
  const timing = timingDeps(env, run, fetcher);
  if (!timing) return undefined;
  return { harnesses: sqlSessionHarnesses(run), source: timing.source };
}

/**
 * Babysitter evidence from the retention buckets (src/session-read-api/babysitter-evidence.ts):
 * buckets and revisions through the read role (tenant-bound), objects with the raw-store
 * secrets. Off unless READ_API_BABYSITTER_EVIDENCE=on and the raw store is configured.
 */
export function babysitterEvidenceDeps(env: Env, run: TenantRunner, fetcher: typeof fetch = (input, init) => fetch(input, init)): EvidenceDeps | undefined {
  if ((env.READ_API_BABYSITTER_EVIDENCE ?? "").trim().toLowerCase() !== "on") return undefined;
  if (!rawStoreFromEnv(env, fetcher)) return undefined;
  const keyRoot = env.READ_API_BABYSITTER_KEY_ROOT?.trim() || "96bulls";
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot)) return undefined;
  // A store per request, its fetcher bound to the request's deadline: list and get both abort.
  const storeFor = (signal: AbortSignal) =>
    rawStoreFromEnv(env, ((input, init) => fetcher(input, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal })) as typeof fetch)?.store;
  return sqlEvidenceDeps(run as never, storeFor as never, (body) => readAll(body as never) /* the bound fetch aborts the body stream too */, (bytes) => decompress(bytes), (key) => objectKey(key) as never, keyRoot);
}

/**
 * The isolate's index verifier (READ_API_OWNER_BUFFERED_INDEX_VERIFY=on): module scope, so a valid
 * answer is kept for the isolate's lifetime and a failure is re-probed after 60 s, across
 * requests. Each request's probe runs on that request's own pg client.
 */
const ISOLATE_INDEX_VERIFIER = new BufferedIndexVerifier();

/**
 * Owner-only buffered reads (Option C): the two SECURITY DEFINER readers through the read role
 * (tenant-bound, user bound per call) and GetObject on the ingest bucket's buffered-batches/
 * stage objects, gated by the HISTORY_CONTROL_HEAD binding (absent with the flag on: handle()
 * answers 503 before this runs; the gate still fences if it ever is). Served only to
 * workspaces on READ_API_OWNER_BUFFERED_WORKSPACES (and the directory allowlist). Undefined unless RELAYHISTORY_READ_OWNER_BUFFERED is "1"/"true", the bucket
 * is named and the raw-store keys exist.
 */
export function ownerBufferedDeps(
  env: Env,
  run: TenantRunner,
  fetcher: typeof fetch = (input, init) => fetch(input, init),
  indexVerifier: BufferedIndexVerifier = ISOLATE_INDEX_VERIFIER,
): OwnerBufferedDeps | undefined {
  if (!ownerBufferedEnabled(env.RELAYHISTORY_READ_OWNER_BUFFERED)) return undefined;
  const bucket = env.INGEST_S3_BUCKET?.trim();
  const accessKeyId = env.PIPELINE_RAW_S3_ACCESS_KEY_ID;
  const secretAccessKey = env.PIPELINE_RAW_S3_SECRET_ACCESS_KEY;
  if (!bucket || !accessKeyId || !secretAccessKey) return undefined;
  return {
    enabled: true,
    workspaces: parseDirectoryWorkspaces(env.READ_API_OWNER_BUFFERED_WORKSPACES),
    reads: sqlOwnerBufferedReads(run, { statementTimeoutMs: ownerBufferedStatementTimeoutFlag(env) }),
    gate: serviceBindingControlHead(env.HISTORY_CONTROL_HEAD),
    store: createS3Store({ bucket, region: env.INGEST_S3_REGION?.trim() || "us-east-1", credentials: { accessKeyId, secretAccessKey }, fetch: fetcher }),
    decompress: boundedStreamDecompress(Decompress) /* stops at the bound; fzstd's one-shot form with a fixed buffer silently truncates */,
    // Session events from buffered stages only with READ_API_OWNER_BUFFERED_EVENTS=on (handle() refused any other value).
    ...(ownerBufferedEventsFlag(env) ? { events: true } : {}),
    // Bounded-concurrency stage fetch only with READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY=2..32; otherwise serial.
    ...(ownerBufferedStageConcurrencyFlag(env) > 1 ? { stageConcurrency: ownerBufferedStageConcurrencyFlag(env) } : {}),
    // Per-phase log fields only with READ_API_OWNER_BUFFERED_PHASE_LOG=1.
    ...(ownerBufferedPhaseLogEnabled(env.READ_API_OWNER_BUFFERED_PHASE_LOG) ? { phaseLog: true } : {}),
    // Out-of-band index check only with READ_API_OWNER_BUFFERED_INDEX_VERIFY=on: this request's
    // read connection probes; the isolate's verifier caches the answer.
    ...(ownerBufferedIndexVerifyFlag(env) ? { indexCheck: indexVerifier.check(sqlBufferedIndexProbe(run)) } : {}),
  };
}

/** READ_API_OWNER_BUFFERED_INDEX_VERIFY as a boolean; a refused value is off here (handle() already answered 503 for it). */
function ownerBufferedIndexVerifyFlag(env: Env): boolean {
  try {
    return ownerBufferedIndexVerifyEnabled(env.READ_API_OWNER_BUFFERED_INDEX_VERIFY);
  } catch {
    return false;
  }
}

/** READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS as ms; a refused value is 2000 (handle() already answered 503 for it). */
function ownerBufferedStatementTimeoutFlag(env: Env): number {
  try {
    return ownerBufferedStatementTimeoutMs(env.READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS);
  } catch {
    return 2000;
  }
}

/** READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY as a number; a refused value is 1, serial (handle() already answered 503 for it). */
function ownerBufferedStageConcurrencyFlag(env: Env): number {
  try {
    return ownerBufferedStageConcurrency(env.READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY);
  } catch {
    return 1;
  }
}

/** READ_API_OWNER_BUFFERED_EVENTS as a boolean; a refused value is off here (handle() already answered 503 for it). */
function ownerBufferedEventsFlag(env: Env): boolean {
  try {
    return ownerBufferedEventsEnabled(env.READ_API_OWNER_BUFFERED_EVENTS);
  } catch {
    return false;
  }
}

export default {
  fetch(request, env, ctx) {
    return handle(request, env, ctx);
  },
} satisfies ExportedHandler<Env>;
