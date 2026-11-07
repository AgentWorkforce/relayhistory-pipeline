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
 * and timing canary allowlist; unset = none), READ_API_BRIEF_TITLES_WORKSPACES (the
 * brief-titles allowlist; unset = none), READ_API_TIMING_ASSUME_NO_TURNS, and
 * RELAYAUTH_JWKS_URL / RELAYAUTH_ISSUER (production
 * values in wrangler.jsonc; the verifier's fallback JWKS does not resolve).
 */
import pg from "pg";
import { decompress } from "fzstd";
import { canonicalAnalysisHandle } from "./analysis-cache.ts";
import { canonicalHandle, type CanonicalEnv } from "./canonical.ts";
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
import { sqlBriefTitles } from "../../../src/session-read-api/brief-titles.ts";
import { relayAuthVerifier } from "../../../src/session-read-api/auth.ts";
import { sqlLiveness, TENANT_SETTINGS_SQL, type SqlQuery, type TenantRunner } from "../../../src/session-read-api/liveness.ts";
import { parseCompleteSince } from "../../../src/session-read-api/config.ts";

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
  /** Babysitter evidence reader (GET /v1/delivery/records for its kinds): only "on" enables; default off. */
  READ_API_BABYSITTER_EVIDENCE?: string;
  /** The ingest key root the retention buckets live under (default 96bulls). */
  READ_API_BABYSITTER_KEY_ROOT?: string;
  RELAYAUTH_JWKS_URL?: string;
  RELAYAUTH_ISSUER?: string;
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
 */
export function requestTenantRunner(connect: () => Promise<pg.Client>): { run: TenantRunner; close(): Promise<void> } {
  let client: Promise<pg.Client> | undefined;
  let tail: Promise<unknown> = Promise.resolve();
  const run: TenantRunner = (orgId, workspaceId, work) => {
    const next = tail.then(async () => {
      const c = await (client ??= connect());
      await c.query("BEGIN READ ONLY");
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
      await c?.end().catch(() => {});
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
  client.on("error", (err) => log("error", "read worker pg error", { error_code: (err as { code?: string }).code ?? "pg_error" }));
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
  try {
    completeSinceMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
    ch = boundedClickHouse(createClickHouseClient(clickHouseConfig(env), overrides.fetcher));
    if (!overrides.connect && !env.SUPABASE?.connectionString) throw new ConfigError("SUPABASE");
  } catch (error) {
    // Configuration errors name a field, never a value.
    log("error", "read worker not configured", { error_code: error instanceof ConfigError ? `missing_${error.field}` : (error as { name?: string })?.name ?? "config" });
    return json(503, "backend_unavailable", "read backend unavailable");
  }
  const tenant = requestTenantRunner(overrides.connect ?? hyperdriveClient(env.SUPABASE));
  const app = createReadApi({
    timing: timingDeps(env, tenant.run, overrides.fetcher),
    turns: turnsDeps(env, tenant.run, overrides.fetcher),
    babysitter: babysitterEvidenceDeps(env, tenant.run, overrides.fetcher),
    verify: relayAuthVerifier({ origin: env.READ_API_ORIGIN?.trim() || undefined, jwksUrl: env.RELAYAUTH_JWKS_URL?.trim() || undefined, issuer: env.RELAYAUTH_ISSUER?.trim() || undefined }),
    ch,
    liveness: sqlLiveness(tenant.run),
    directory: sqlDirectory(tenant.run),
    directoryWorkspaces: parseDirectoryWorkspaces(env.READ_API_DIRECTORY_WORKSPACES),
    briefTitles: sqlBriefTitles(tenant.run),
    briefTitleWorkspaces: parseDirectoryWorkspaces(env.READ_API_BRIEF_TITLES_WORKSPACES),
    completeSinceMs,
    log: (fields) => log("info", "session read", fields),
  });
  try {
    return await app.fetch(request);
  } finally {
    ctx.waitUntil(tenant.close());
  }
}

/**
 * Session timing from canonical S3: pointers through the read role (tenant-bound), objects
 * with the raw-store secrets the /v1/canonical route uses (PIPELINE_RAW_S3_*). Without
 * those secrets the route answers 501 `timing_not_configured`.
 */
export function timingDeps(env: Env, run: Parameters<typeof sqlCanonicalPointers>[0], fetcher: typeof fetch = (input, init) => fetch(input, init)): TimingDeps | undefined {
  if (!rawStoreFromEnv(env, fetcher)) return undefined;
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

export default {
  fetch(request, env, ctx) {
    return handle(request, env, ctx);
  },
} satisfies ExportedHandler<Env>;
