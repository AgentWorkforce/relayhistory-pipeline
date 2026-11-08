/**
 * AWS Lambda entry for the session read API (Function URL, payload v2;
 * stacks/aws/session-read-api.ts, runbooks/sessions-prod-session-read-api.md).
 *
 * Configuration: plain env for ids and switches; credentials only from one
 * Secrets Manager secret (`SESSION_READ_API_SECRET_ARN`), read once per sandbox
 * with the execution role. The secret holds JSON:
 *
 *   { "clickhouse": { "url", "user", "password", "database" },   a SELECT-only user
 *     "supabase_url": "postgresql://bulls96_read_api:…@…" }        the read role
 *
 * Env: READ_API_ORIGIN (this function's URL origin; unset = every request 401),
 * READ_API_COMPLETE_SINCE (ISO timestamp from which ClickHouse history is
 * complete, or "epoch"; required), RELAYAUTH_JWKS_URL / RELAYAUTH_ISSUER
 * (optional overrides), RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS (exactly "true"
 * also accepts relaycontrol `sh_at_` tokens; default off) with
 * RELAYCONTROL_INTROSPECT_URL / RELAYCONTROL_INTROSPECT_SECRET,
 * RELAYHISTORY_SELFHOST_READ_API (exactly "true" registers POST /v1/cli/login and
 * GET /v1/sessions/:id/catalog; unset/""/"false" off; anything else fails startup.
 * Login needs the relaycontrol introspection above, else 503; this entry wires no
 * canonical source, so catalog answers 501 `catalog_not_configured`, and the
 * Function URL handler is GET-only, so login is 405 here; the self-host service
 * entry, `entry: "service"`, wires the canonical source from LOCKER_*, see
 * service-canonical.ts). READ_API_AUTH_MODE (unset/"" = the default dual path;
 * exactly "relaycontrol_only" = no RelayAuth verifier at all, see
 * readApiAuthConfig).
 *
 * Owner-only buffered reads (RELAYHISTORY_READ_OWNER_BUFFERED, owner-buffered.ts) are
 * Worker-only: they need the HISTORY_CONTROL_HEAD service binding. This entry never wires
 * them, so the flag has no effect here.
 */
import pg from "pg";
import { log } from "../shared/log.ts";
import { awsJsonCall, lambdaCredentials } from "../session-pipeline/aws-json.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import { composeSearchService, searchSecret, type SearchComposition } from "../shared/search-composition.ts";
import { PIPELINE_RUNTIMES, validateAdapterEnv } from "@rhc/adapters/registry.ts";
import { createClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { createReadApi, parseDirectoryWorkspaces, type ReadApiDeps } from "./app.ts";
import { sqlDirectory } from "./directory.ts";
import { sqlBriefTitles } from "./brief-titles.ts";
import { readApiVerifier, relaycontrolLogin, type AuthConfig } from "./auth.ts";
import { parseSelfhostReadApi } from "./selfhost.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { parseCompleteSince } from "./config.ts";
import { poolTenantRunner, sqlLiveness, type SqlQuery } from "./liveness.ts";
import { serviceCanonicalDeps } from "./service-canonical.ts";

export interface ReadApiSecret {
  clickhouse?: { url: string; user: string; password: string; database: string };
  supabase_url: string;
  search?: unknown;
}

/** Parse and check the secret's shape. Never echoes a value. */
export function parseReadApiSecret(text: string, requireClickHouse = true): ReadApiSecret {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error("session read api secret is not JSON");
  }
  const v = value as Partial<ReadApiSecret> | null;
  if (!v || typeof v.supabase_url !== "string" || !/^postgres(ql)?:\/\//.test(v.supabase_url))
    throw new Error("session read api secret has no supabase_url");
  const c = v.clickhouse;
  if (requireClickHouse || c !== undefined) for (const k of ["url", "user", "password", "database"] as const)
    if (!c || typeof c[k] !== "string" || !c[k]) throw new Error(`session read api secret clickhouse.${k} is missing`);
  return v as ReadApiSecret;
}

export { parseCompleteSince };

export const READ_API_AUTH_MODE_VAR = "READ_API_AUTH_MODE";

/**
 * READ_API_AUTH_MODE: unset or "" is the default (RelayAuth, plus relaycontrol
 * when RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true); exactly "relaycontrol_only"
 * takes relaycontrol `sh_at_` tokens only. Anything else refuses startup. Never
 * echoes the value.
 */
export function readApiAuthMode(env: NodeJS.ProcessEnv = process.env): "default" | "relaycontrol_only" {
  const raw = env[READ_API_AUTH_MODE_VAR];
  if (raw === undefined || raw === "") return "default";
  if (raw === "relaycontrol_only") return "relaycontrol_only";
  throw new Error(`session read api: ${READ_API_AUTH_MODE_VAR} must be unset, "" or "relaycontrol_only"`);
}

/**
 * relaycontrol_only (a self-host without RelayAuth): the verifier never builds
 * the RelayAuth path, so no JWKS is ever fetched and a bearer without the
 * `sh_at_` prefix is 401 before any outbound call. A RelayAuth setting next to
 * it is a contradiction and refuses startup, as does a missing piece of the
 * relaycontrol introspection (so the mode can never silently fall back to 401
 * not_configured on every request). Errors name variables, never values.
 */
function relaycontrolOnlyAuthConfig(env: NodeJS.ProcessEnv, policy: ReturnType<typeof destinationPolicy>): AuthConfig {
  const mode = `${READ_API_AUTH_MODE_VAR}=relaycontrol_only`;
  for (const name of ["RELAYAUTH_JWKS_URL", "RELAYAUTH_ISSUER"] as const)
    if (env[name]?.trim()) throw new Error(`session read api: ${mode} refuses ${name}; unset it`);
  if (env.RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS !== "true")
    throw new Error(`session read api: ${mode} needs RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true`);
  const origin = env.READ_API_ORIGIN?.trim() || undefined;
  const url = env.RELAYCONTROL_INTROSPECT_URL?.trim() || undefined;
  const secret = env.RELAYCONTROL_INTROSPECT_SECRET?.trim() || undefined;
  for (const [name, value] of [["READ_API_ORIGIN", origin], ["RELAYCONTROL_INTROSPECT_URL", url], ["RELAYCONTROL_INTROSPECT_SECRET", secret]] as const)
    if (!value) throw new Error(`session read api: ${mode} needs ${name}`);
  assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url!, policy);
  return { origin: env.READ_API_ORIGIN || undefined, mode: "relaycontrol_only", relaycontrol: { url, secret } };
}

/**
 * The verifier's destinations from env, validated with the shared transport
 * rule. Unset JWKS/issuer fall back to the hosted RelayAuth defaults in
 * packages/pipeline/src/env.ts; a self-host sets both, or sets
 * READ_API_AUTH_MODE=relaycontrol_only and neither.
 */
export function readApiAuthConfig(env: NodeJS.ProcessEnv = process.env): AuthConfig {
  const policy = destinationPolicy(env);
  if (readApiAuthMode(env) === "relaycontrol_only") return relaycontrolOnlyAuthConfig(env, policy);
  const jwksUrl = env.RELAYAUTH_JWKS_URL?.trim() || undefined;
  if (jwksUrl) assertDestinationUrl("RELAYAUTH_JWKS_URL", jwksUrl, policy);
  const config: AuthConfig = { origin: env.READ_API_ORIGIN || undefined, jwksUrl, issuer: env.RELAYAUTH_ISSUER };
  // Relaycontrol introspection (default off): only exactly "true" enables. The
  // URL is configuration only and passes the same transport rule.
  if (env.RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS === "true") {
    const url = env.RELAYCONTROL_INTROSPECT_URL?.trim() || undefined;
    if (url) assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, policy);
    config.relaycontrol = { url, secret: env.RELAYCONTROL_INTROSPECT_SECRET?.trim() || undefined };
  }
  return config;
}

type App = ReturnType<typeof createReadApi>;

/**
 * Which entry assembles the app. "lambda" (the default, hosted): PIPELINE_RUNTIME
 * must be lambda, SEARCH_INDEX_BACKEND defaults to clickhouse, and the read role's
 * URL reaches pg as a sanitized connectionString. "service" (the self-host
 * SERVICE, server.ts): PIPELINE_RUNTIME is not this process's concern (any known
 * value), SEARCH_INDEX_BACKEND defaults to postgres, SESSION_POSTGRES_TLS_PROFILE
 * must be set explicitly, and the pool gets discrete host/port/database/user/
 * password fields, so nothing in the URL's query string reaches pg.
 */
export type ReadAppEntry = "lambda" | "service";
export interface ReadAppOptions { entry?: ReadAppEntry }

function searchBackend(env: NodeJS.ProcessEnv, composition?: SearchComposition, entry: ReadAppEntry = "lambda") {
  const search = ["clickhouse" as const, ...(composition?.registry.registered().filter(name => name !== "clickhouse") ?? [])];
  if (entry === "service")
    return validateAdapterEnv(env, { runtime: PIPELINE_RUNTIMES, runtimeDefault: "workers", search, searchDefault: "postgres" }).search;
  return validateAdapterEnv(env, { runtime: ["lambda"], runtimeDefault: "lambda", search, searchDefault: "clickhouse" }).search;
}

/**
 * The self-host service's search selection before any composition exists:
 * SEARCH_INDEX_BACKEND unset or "" is postgres; otherwise exactly a name this
 * entry can run (postgres, clickhouse). Anything else is an AdapterConfigError
 * that names the variable and the allowed names, never the value.
 */
export function serviceSearchBackend(env: NodeJS.ProcessEnv): "postgres" | "clickhouse" {
  return validateAdapterEnv(env, { runtime: PIPELINE_RUNTIMES, runtimeDefault: "workers", search: ["postgres", "clickhouse"], searchDefault: "postgres" }).search as "postgres" | "clickhouse";
}

/**
 * The read role's pool target. Lambda: the existing sanitized connectionString
 * (TLS/options/target overrides removed by postgresConnectionString). Service:
 * discrete fields from the same sanitized URL; its query string is never
 * applied, and a missing user, password or database refuses startup (pg would
 * otherwise fall back to PG* env or a .pgpass file). Never echoes a value.
 */
function readerPoolTarget(url: string, entry: ReadAppEntry): pg.PoolConfig {
  const target = new URL(postgresConnectionString(url));
  if (entry === "service") {
    let user: string, password: string, database: string;
    try {
      user = decodeURIComponent(target.username);
      password = decodeURIComponent(target.password);
      database = decodeURIComponent(target.pathname.replace(/^\//u, ""));
    } catch { throw new Error("session read api secret supabase_url is invalid"); }
    if (!user || !password || !database || database.includes("/") || target.hash)
      throw new Error("session read api secret supabase_url needs a user, password and database");
    return { host: target.hostname.replace(/^\[(.*)\]$/u, "$1"), port: target.port ? Number(target.port) : 5432, user, password, database };
  }
  // pg lets URL parameters override pool configuration. A client-side query
  // timer does not cancel SQL, and neither URL nor ambient PGOPTIONS may
  // disable this reader's server-side bound. TLS/target sanitization stays in
  // postgresConnectionString; options from the URL are already removed there.
  for (const key of [...target.searchParams.keys()])
    if (["statement_timeout", "query_timeout"].includes(key.toLowerCase())) target.searchParams.delete(key);
  return { connectionString: target.toString() };
}

/** Closed refusal codes for a read login that row-level security would not bind. */
export type ReaderRoleRefusal = "reader_role_unknown" | "reader_role_superuser" | "reader_role_bypassrls";

/** Names the role and the failed property only; never the URL or a credential. */
export class ReaderRoleError extends Error {
  constructor(readonly code: ReaderRoleRefusal, role?: string) {
    super(`session read api: ${code}${role ? ` (role ${JSON.stringify(role.slice(0, 63))})` : ""}`);
    this.name = "ReaderRoleError";
  }
}

/**
 * Self-host reader admission. Tenant isolation on search_index.projection_state
 * and transcript_events is forced row-level security (bulls96_read.* settings,
 * `tenant_only`), which a superuser or BYPASSRLS login skips without any error:
 * every tenant's rows would become visible. Refuse such a login at startup
 * rather than serve with isolation resting on WHERE clauses alone. Any failure
 * to read the role (no row, a query error) also refuses.
 */
export async function assertReaderRole(sql: SqlQuery): Promise<void> {
  const { rows } = await sql.query<{ role: unknown; rolsuper: unknown; rolbypassrls: unknown }>(
    "SELECT current_user AS role, r.rolsuper, r.rolbypassrls FROM pg_roles r WHERE r.rolname = current_user",
  );
  const me = rows[0];
  if (rows.length !== 1 || !me || typeof me.role !== "string") throw new ReaderRoleError("reader_role_unknown");
  if (me.rolsuper !== false) throw new ReaderRoleError("reader_role_superuser", me.role);
  if (me.rolbypassrls !== false) throw new ReaderRoleError("reader_role_bypassrls", me.role);
}

async function buildApp(env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition): Promise<App> {
  const region = env.AWS_REGION;
  const secretArn = env.SESSION_READ_API_SECRET_ARN;
  if (!region || !secretArn) throw new Error("session read api: AWS_REGION and SESSION_READ_API_SECRET_ARN are required");
  parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  parseSelfhostReadApi(env);
  postgresTlsOptions(env);
  searchBackend(env, composition); // reject unsupported selections before secret lookup
  const { SecretString } = await awsJsonCall<{ SecretString?: string }>({
    service: "secretsmanager",
    target: "secretsmanager.GetSecretValue",
    region,
    body: { SecretId: secretArn },
    credentials: lambdaCredentials(),
  });
  if (!SecretString) throw new Error("session read api secret has no value yet");
  return (await readAppFromSecret(SecretString, env, composition)).app;
}

/** The read API's dependencies except its verifier: the read-role pool, the
 * selected search binding (the ClickHouse client or the composition's adapter,
 * never both), directory/liveness SQL and the self-host login. `close` releases
 * the pool. readAppFromSecret adds the verifier from `authConfig`. */
export async function readApiDepsFromSecret(secretText: string, env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition, options: ReadAppOptions = {}): Promise<{ deps: Omit<ReadApiDeps, "verify">; authConfig: AuthConfig; close(): Promise<void> }> {
  const entry = options.entry ?? "lambda";
  const backend = searchBackend(env, composition, entry);
  const completeSinceMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  const selfhostReadApi = parseSelfhostReadApi(env);
  // The self-host service never inherits the hosted Supabase pin by omission.
  if (entry === "service" && !env.SESSION_POSTGRES_TLS_PROFILE)
    throw new Error("session read api: the self-host service needs SESSION_POSTGRES_TLS_PROFILE set explicitly");
  const tls = postgresTlsOptions(env);
  const secret = parseReadApiSecret(secretText, backend === "clickhouse");
  const target = readerPoolTarget(secret.supabase_url, entry);
  // Env only: a bad auth setting (including READ_API_AUTH_MODE) refuses before the pool opens.
  const authConfig = readApiAuthConfig(env);
  const pool = new pg.Pool({
    ...target,
    max: 2,
    application_name: "96bulls-session-read-api",
    ssl: tls,
    idleTimeoutMillis: 60_000,
    connectionTimeoutMillis: 2_000,
    // Covers index queries and tenant completeness/liveness/directory SQL on
    // this pool. Queued work may outlive the HTTP deadline: these are limits
    // on acquisition and each server statement, not AbortSignal cancellation.
    statement_timeout: 2_000, // Matches the existing reader-role bootstrap bound.
    options: "-c statement_timeout=2000", // Nonempty: pg otherwise uses PGOPTIONS.
  });
  pool.on("error", (err) => log.error("read api pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  try {
  // Directory, liveness and Postgres search isolation is forced RLS. Checked on
  // the service always and on any portable (Postgres) search binding; the hosted
  // ClickHouse Lambda's read role is verified by its own bootstrap
  // (supabase/bootstrap/session_read_api_role_verify.sql).
  if (entry === "service" || backend !== "clickhouse") await assertReaderRole(pool);
  const adapter = backend === "clickhouse" ? undefined : composeSearchService(composition!, { env, sql: pool, secrets: searchSecret(secret.search) }, "reader", poolTenantRunner(pool as never));
  if (adapter && await adapter.state.fromMs(new Date()) > completeSinceMs) throw new Error("read history floor is below search coverage");
  // The self-host service only: canonical S3 (LOCKER_*) behind timing, turns,
  // metadata and catalog. The hosted Lambda never builds it (those routes keep
  // their 501 `*_not_configured` there).
  const canonical = entry === "service" ? serviceCanonicalDeps(env, poolTenantRunner(pool as never)) : undefined;
  const deps: Omit<ReadApiDeps, "verify"> = {
    ...(adapter ? { adapter } : { ch: createClickHouseClient(secret.clickhouse!) }),
    liveness: sqlLiveness(poolTenantRunner(pool as never)),
    directory: sqlDirectory(poolTenantRunner(pool as never)),
    directoryWorkspaces: parseDirectoryWorkspaces(env.READ_API_DIRECTORY_WORKSPACES),
    briefTitles: sqlBriefTitles(poolTenantRunner(pool as never)),
    briefTitleWorkspaces: parseDirectoryWorkspaces(env.READ_API_BRIEF_TITLES_WORKSPACES),
    completeSinceMs,
    ...(canonical ? { timing: canonical.timing, turns: canonical.turns } : {}),
    ...(selfhostReadApi ? { selfhost: { login: relaycontrolLogin(authConfig), ...(canonical ? { catalog: canonical.catalog } : {}) } } : {}),
    log: (fields) => log.info("session read", fields),
  };
  return { deps, authConfig, close: () => pool.end() };
  } catch (error) { await pool.end(); throw error; }
}

/** The same service assembly used by the Lambda loader, without credential
 * discovery. A reviewed artifact composition can provide registered adapters;
 * `options.entry` selects the self-host service's rules (see ReadAppEntry). */
export async function readAppFromSecret(secretText: string, env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition, options: ReadAppOptions = {}): Promise<{ app: App; close(): Promise<void> }> {
  const { deps, authConfig, close } = await readApiDepsFromSecret(secretText, env, composition, options);
  try {
    return { app: createReadApi({ verify: readApiVerifier(authConfig), ...deps }), close };
  } catch (error) { await close(); throw error; }
}

export interface FunctionUrlEvent {
  rawPath: string;
  rawQueryString?: string;
  headers?: Record<string, string | undefined>;
  requestContext: { http: { method: string } };
  body?: string;
  isBase64Encoded?: boolean;
}

export interface FunctionUrlResult {
  statusCode: number;
  headers: Record<string, string>;
  body: string;
}

/** Function URL event → fetch Request → app → Function URL result. GET only. */
export async function handleUrlEvent(app: { fetch(req: Request): Response | Promise<Response> }, event: FunctionUrlEvent): Promise<FunctionUrlResult> {
  const method = event.requestContext.http.method.toUpperCase();
  if (method !== "GET") return { statusCode: 405, headers: { "content-type": "application/json", allow: "GET" }, body: JSON.stringify({ error: { code: "method_not_allowed", message: "GET only" } }) };
  const headers = new Headers();
  for (const [k, v] of Object.entries(event.headers ?? {})) if (v !== undefined) headers.set(k, v);
  const qs = event.rawQueryString ? `?${event.rawQueryString}` : "";
  const response = await app.fetch(new Request(`https://read-api.internal${event.rawPath}${qs}`, { method, headers }));
  const out: Record<string, string> = {};
  response.headers.forEach((value, key) => {
    out[key] = value;
  });
  return { statusCode: response.status, headers: out, body: await response.text() };
}

export function createReadApiLambdaHandler(composition?: SearchComposition) {
let appPromise: Promise<App> | undefined;
return async function readApiHandler(event: FunctionUrlEvent): Promise<FunctionUrlResult> {
  let app: Awaited<NonNullable<typeof appPromise>>;
  try {
    appPromise ??= buildApp(process.env, composition);
    app = await appPromise;
  } catch (error) {
    // A failed init is retried on the next request rather than cached forever.
    appPromise = undefined;
    log.error("read api init failed", { error_code: (error as { name?: string })?.name ?? "error" });
    return { statusCode: 503, headers: { "content-type": "application/json", "cache-control": "private, no-store" }, body: JSON.stringify({ error: { code: "backend_unavailable", message: "read backend unavailable" } }) };
  }
  // Route errors are answered inside the app (Hono); a throw here is not an init failure.
  return await handleUrlEvent(app, event);
};
}

export const handler = createReadApiLambdaHandler();
