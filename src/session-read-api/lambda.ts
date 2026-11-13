import { sqlBriefSummary } from "./brief-summary.ts";
/**
 * AWS Lambda entry for the session read API (Function URL, payload v2;
 * stacks/aws/session-read-api.ts, sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-session-read-api.md).
 *
 * Configuration: plain env for ids and switches; credentials only from one
 * Secrets Manager secret (`SESSION_READ_API_SECRET_ARN`), read once per sandbox
 * with the execution role. The secret holds JSON:
 *
 *   { "clickhouse": { "url", "user", "password", "database" },   a SELECT-only user
 *     "supabase_url": "postgresql://bulls96_read_api:…@…" }        the read role
 *
 * Session analysis (lane F1), off unless configured:
 *   READ_API_ANALYSIS_BUCKET   the canonical bucket holding analysis result objects (the
 *                              read role's GET is served; unset = 501 analysis_not_configured).
 *   "analysis_admit_url"       optional secret field: postgresql://bulls96_analysis_admit:…,
 *                              the separate admission principal; absent = POST 501.
 * POST is accepted for exactly /v1/sessions/:id/analysis; every other method/path is 405.
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
 * readApiAuthConfig). READ_API_INTROSPECT_POSITIVE_TTL_MS (unset = 60000; an
 * integer 0..60000, 0 = introspect every request; anything else fails startup,
 * see readApiIntrospectPositiveTtlMs). READ_API_SESSION_QUERY (exactly "true" serves
 * `q` on GET /v1/sessions and /v1/sessions/activity from the directory's query functions,
 * migration 20261105046010; unset/""/"false" off = 501 `param_q` as before; anything else
 * fails startup, see sessionQueryEnabled). READ_API_COORDINATION_OVERLAPS_ENABLED (exactly
 * "true") exposes GET /v1/coordination/overlaps; READ_API_COORDINATION_OVERLAPS_BACKEND selects
 * clickhouse (default) or postgres (self-host service only). READ_API_BRIEF_SUMMARY_ENABLED (exactly
 * "true") wires the brief-summary read (GET /v1/sessions/:id/summary and
 * /v1/sessions/summaries/activity) on this entry too; unset/any other value = 501
 * `summary_not_configured` as before (the self-host service always wires it).
 * READ_API_LIST_SHORT_SUMMARY_ENABLED (exactly "true", either entry) serves the opt-in
 * includeShortSummary=recorded on GET /v1/sessions; otherwise that param is 501
 * `param_includeShortSummary` and the list shape is unchanged.
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
import {
  READ_API_INTROSPECT_POSITIVE_TTL_MS_DEFAULT,
  READ_API_INTROSPECT_POSITIVE_TTL_MS_MAX,
  readApiVerifier,
  relaycontrolLogin,
  selfIssuedVerifier,
  type AuthConfig,
} from "./auth.ts";
import { parseSelfhostLoginSync, parseSelfhostReadApi } from "./selfhost.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import {
  coordinationOverlapsEnabled, exactTrueFlag, parseCompleteSince, parseFactsBackend, readApiRouteEnabled,
  READ_API_BRIEF_SUMMARY_ENABLED, READ_API_COORDINATION_OVERLAPS_BACKEND, READ_API_COORDINATION_OVERLAPS_ENABLED,
  READ_API_LIST_SHORT_SUMMARY_ENABLED,
  READ_API_DIGEST_BACKEND, READ_API_OUTPUT_BACKEND,
} from "./config.ts";
import { poolTenantRunner, sqlLiveness, sqlOperatorErased, type SqlQuery } from "./liveness.ts";
import { ERASE_FENCE_FLAG, eraseRuntimeFlag } from "../session-erase/flags.ts";
import { createS3Store } from "../shared/vendor.ts";
import { sqlAnalysisSource } from "./analysis-source.ts";
import { sqlNarrativeCache, type DigestNarrativeDeps } from "./digest-narrative.ts";
import { postgresOutputBackend } from "./postgres/output-query.ts";
import { postgresDigestBackend } from "./postgres/digest-query.ts";
import { postgresCoordinationBackend } from "./postgres/coordination-query.ts";
import { ANALYTICS_OFF, ANALYTICS_SINK_VAR, analyticsConfig, analyticsCounterFields, createAnalyticsSink } from "../shared/analytics-config.ts";
import { serviceCanonicalDeps } from "./service-canonical.ts";
import { digestEnabled, digestNarrativeModel, selfhostNarrativeFromEnv } from "./digest-narrative-selfhost.ts";
import {
  ACCESS_TTL_SECONDS_DEFAULT,
  ACCESS_TTL_SECONDS_MAX,
  ACCESS_TTL_SECONDS_MIN,
  INTERNAL_SECRET_MIN_LENGTH,
  normalizedOrigin,
  sqlTokenStore,
} from "./self-issued.ts";

export interface ReadApiSecret {
  clickhouse?: { url: string; user: string; password: string; database: string };
  supabase_url: string;
  search?: unknown;
  /** The session-analysis admission principal (bulls96_analysis_admit); optional. */
  analysis_admit_url?: string;
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
  if (v.analysis_admit_url !== undefined && (typeof v.analysis_admit_url !== "string" || !/^postgres(ql)?:\/\//.test(v.analysis_admit_url)))
    throw new Error("session read api secret analysis_admit_url is not a postgres URL");
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
export function readApiAuthMode(env: NodeJS.ProcessEnv = process.env): "default" | "relaycontrol_only" | "self_issued" {
  const raw = env[READ_API_AUTH_MODE_VAR];
  if (raw === undefined || raw === "") return "default";
  if (raw === "relaycontrol_only" || raw === "self_issued") return raw;
  throw new Error(`session read api: ${READ_API_AUTH_MODE_VAR} must be unset, "", "relaycontrol_only" or "self_issued"`);
}

export const READ_API_INTERNAL_SECRET_VAR = "READ_API_INTERNAL_SECRET";
export const READ_API_INTERNAL_SECRET_PREVIOUS_VAR = "READ_API_INTERNAL_SECRET_PREVIOUS";
export const READ_API_ACCESS_TOKEN_TTL_VAR = "READ_API_ACCESS_TOKEN_TTL_SECONDS";
export const READ_API_CORS_ORIGINS_VAR = "READ_API_CORS_ALLOWED_ORIGINS";
export const READ_API_EVENT_MESSAGE_IDS_VAR = "READ_API_EVENT_MESSAGE_IDS";

/**
 * READ_API_EVENT_MESSAGE_IDS (default off): exactly "true" serves includeMessageId on session events
 * from the canonical store. Unset, "" or "false" leaves /events as without it; anything else refuses startup.
 */
export function eventMessageIdDeps<T>(env: NodeJS.ProcessEnv, canonicalTurns: T | undefined): { eventMessageIds?: T } {
  const raw = env[READ_API_EVENT_MESSAGE_IDS_VAR];
  if (raw === undefined || raw === "" || raw === "false") return {};
  if (raw !== "true") throw new Error(`session read api: ${READ_API_EVENT_MESSAGE_IDS_VAR} must be unset, "", "true" or "false"`);
  return canonicalTurns ? { eventMessageIds: canonicalTurns } : {};
}

export const READ_API_SESSION_QUERY_VAR = "READ_API_SESSION_QUERY";

/**
 * READ_API_SESSION_QUERY (default off): exactly "true" serves the session list/activity `q`
 * (list-query.ts). Unset, "" or "false" leaves both routes' 501 `param_q`; anything else refuses
 * startup, naming the variable and never its value.
 */
export function sessionQueryEnabled(env: NodeJS.ProcessEnv): boolean {
  const raw = env[READ_API_SESSION_QUERY_VAR];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw !== "true") throw new Error(`session read api: ${READ_API_SESSION_QUERY_VAR} must be unset, "", "true" or "false"`);
  return true;
}

function internalSecret(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const raw = env[name];
  if (raw === undefined || raw === "") return undefined;
  // The route compares the exact bearer, so a padded secret could never match.
  if (raw !== raw.trim()) throw new Error(`session read api: ${name} must not have leading or trailing whitespace`);
  if (raw.length < INTERNAL_SECRET_MIN_LENGTH) throw new Error(`session read api: ${name} must be at least ${INTERNAL_SECRET_MIN_LENGTH} characters`);
  return raw;
}

/** READ_API_ACCESS_TOKEN_TTL_SECONDS: unset is 300; otherwise a decimal integer within the bounds. */
function accessTtlSeconds(env: NodeJS.ProcessEnv): number {
  const raw = env[READ_API_ACCESS_TOKEN_TTL_VAR];
  if (raw === undefined) return ACCESS_TTL_SECONDS_DEFAULT;
  const value = /^[1-9][0-9]{0,4}$/u.test(raw) ? Number(raw) : Number.NaN;
  if (!(value >= ACCESS_TTL_SECONDS_MIN && value <= ACCESS_TTL_SECONDS_MAX))
    throw new Error(`session read api: ${READ_API_ACCESS_TOKEN_TTL_VAR} must be an integer from ${ACCESS_TTL_SECONDS_MIN} to ${ACCESS_TTL_SECONDS_MAX}`);
  return value;
}

/**
 * self_issued: this service's own tokens only. Anything that would leave it serving another token
 * family or unable to bind audience refuses startup. Errors name variables, never values.
 */
function selfIssuedAuthConfig(env: NodeJS.ProcessEnv): AuthConfig {
  const mode = `${READ_API_AUTH_MODE_VAR}=self_issued`;
  for (const name of ["RELAYAUTH_JWKS_URL", "RELAYAUTH_ISSUER", "RELAYCONTROL_INTROSPECT_URL", "RELAYCONTROL_INTROSPECT_SECRET"] as const)
    if (env[name]?.trim()) throw new Error(`session read api: ${mode} refuses ${name}; unset it`);
  for (const name of ["RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS", "RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC"] as const)
    if (env[name] === "true") throw new Error(`session read api: ${mode} refuses ${name}=true`);
  const raw = env.READ_API_ORIGIN?.trim();
  const audience = raw ? normalizedOrigin(raw) : null;
  if (!audience) throw new Error(`session read api: ${mode} needs READ_API_ORIGIN set to an origin`);
  const current = internalSecret(env, READ_API_INTERNAL_SECRET_VAR);
  if (!current) throw new Error(`session read api: ${mode} needs ${READ_API_INTERNAL_SECRET_VAR}`);
  const previous = internalSecret(env, READ_API_INTERNAL_SECRET_PREVIOUS_VAR);
  if (previous === current) throw new Error(`session read api: ${READ_API_INTERNAL_SECRET_PREVIOUS_VAR} must differ from ${READ_API_INTERNAL_SECRET_VAR}`);
  return {
    origin: raw,
    mode: "self_issued",
    selfIssued: { audience, internalSecrets: previous ? [current, previous] : [current], accessTtlSeconds: accessTtlSeconds(env) },
  };
}

const CORS_LOOPBACK = /^http:\/\/(localhost|127\.0\.0\.1)(:[0-9]+)?$/u;

/**
 * READ_API_CORS_ALLOWED_ORIGINS: comma-separated exact browser origins. Unset or blank is no CORS.
 * A wildcard, a path, or plain http off loopback refuses startup. Never echoes the value.
 */
export function readApiCorsOrigins(env: NodeJS.ProcessEnv = process.env): ReadonlySet<string> {
  const values = (env[READ_API_CORS_ORIGINS_VAR] ?? "").split(",").map((v) => v.trim()).filter(Boolean);
  for (const value of values) {
    if (value.includes("*") || normalizedOrigin(value) !== value || !(value.startsWith("https://") || CORS_LOOPBACK.test(value)))
      throw new Error(`session read api: ${READ_API_CORS_ORIGINS_VAR} must list exact https origins (http only on loopback), with no wildcard or path`);
  }
  return new Set(values);
}

export const READ_API_INTROSPECT_POSITIVE_TTL_MS_VAR = "READ_API_INTROSPECT_POSITIVE_TTL_MS";

/**
 * READ_API_INTROSPECT_POSITIVE_TTL_MS: how long (ms) an admitted relaycontrol
 * token is reused before it is introspected again, so the read path's
 * revocation lag. Unset keeps 60000 (the hosted behaviour). Otherwise exactly
 * a decimal integer 0..60000 (no sign, spaces, exponent or leading zero); 0
 * introspects every request. Anything else, "" included, refuses startup.
 * Parsed whether or not relaycontrol tokens are enabled. Never echoes the value.
 */
export function readApiIntrospectPositiveTtlMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env[READ_API_INTROSPECT_POSITIVE_TTL_MS_VAR];
  if (raw === undefined) return READ_API_INTROSPECT_POSITIVE_TTL_MS_DEFAULT;
  const value = /^(0|[1-9][0-9]{0,4})$/.test(raw) ? Number(raw) : NaN;
  if (!Number.isSafeInteger(value) || value < 0 || value > READ_API_INTROSPECT_POSITIVE_TTL_MS_MAX)
    throw new Error(
      `session read api: ${READ_API_INTROSPECT_POSITIVE_TTL_MS_VAR} must be unset or an integer from 0 to ${READ_API_INTROSPECT_POSITIVE_TTL_MS_MAX}`,
    );
  return value;
}

/**
 * relaycontrol_only (a self-host without RelayAuth): the verifier never builds
 * the RelayAuth path, so no JWKS is ever fetched and a bearer without the
 * `sh_at_` prefix is 401 before any outbound call. A RelayAuth setting next to
 * it is a contradiction and refuses startup, as does a missing piece of the
 * relaycontrol introspection (so the mode can never silently fall back to 401
 * not_configured on every request). Errors name variables, never values.
 */
function relaycontrolOnlyAuthConfig(
  env: NodeJS.ProcessEnv,
  policy: ReturnType<typeof destinationPolicy>,
  positiveTtlMs: number,
): AuthConfig {
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
  return { origin: env.READ_API_ORIGIN || undefined, mode: "relaycontrol_only", relaycontrol: { url, secret, positiveTtlMs } };
}

/**
 * The verifier's destinations from env, validated with the shared transport
 * rule. Unset JWKS/issuer fall back to the hosted RelayAuth defaults in
 * packages/pipeline/src/env.ts; a self-host sets both, or sets
 * READ_API_AUTH_MODE=relaycontrol_only and neither.
 */
export function readApiAuthConfig(env: NodeJS.ProcessEnv = process.env): AuthConfig {
  const policy = destinationPolicy(env);
  const positiveTtlMs = readApiIntrospectPositiveTtlMs(env);
  if (readApiAuthMode(env) === "self_issued") return selfIssuedAuthConfig(env);
  if (readApiAuthMode(env) === "relaycontrol_only") return relaycontrolOnlyAuthConfig(env, policy, positiveTtlMs);
  const jwksUrl = env.RELAYAUTH_JWKS_URL?.trim() || undefined;
  if (jwksUrl) assertDestinationUrl("RELAYAUTH_JWKS_URL", jwksUrl, policy);
  const config: AuthConfig = { origin: env.READ_API_ORIGIN || undefined, jwksUrl, issuer: env.RELAYAUTH_ISSUER };
  // Relaycontrol introspection (default off): only exactly "true" enables. The
  // URL is configuration only and passes the same transport rule.
  if (env.RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS === "true") {
    const url = env.RELAYCONTROL_INTROSPECT_URL?.trim() || undefined;
    if (url) assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, policy);
    config.relaycontrol = { url, secret: env.RELAYCONTROL_INTROSPECT_SECRET?.trim() || undefined, positiveTtlMs };
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
 * entry can run (postgres, clickhouse, opensearch). Anything else is an AdapterConfigError
 * that names the variable and the allowed names, never the value.
 */
export function serviceSearchBackend(env: NodeJS.ProcessEnv): "postgres" | "clickhouse" | "opensearch" {
  return validateAdapterEnv(env, { runtime: PIPELINE_RUNTIMES, runtimeDefault: "workers", search: ["postgres", "clickhouse", "opensearch"], searchDefault: "postgres" }).search as "postgres" | "clickhouse" | "opensearch";
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

/** The read login the bootstrap provisions (supabase/bootstrap/session_read_api_role.sql); the RLS policies name it. */
export const READ_API_ROLE = "bulls96_read_api";

/**
 * The role's pinned settings (session_read_api_role.sql `ALTER ROLE ... SET`; session_read_api_role_verify.sql
 * requires rolconfig to contain both). The pool also sends statement_timeout=2000 as a startup option, so
 * the live session must show exactly these values.
 */
export const READER_SETTINGS = Object.freeze({ statement_timeout: "2s", default_transaction_read_only: "on" });

/** Closed refusal codes for a read login that row-level security would not bind. */
export type ReaderRoleRefusal =
  | "reader_role_unknown" | "reader_role_session_user" | "reader_role_current_user" | "reader_role_set_role" | "reader_role_backend_user"
  | "reader_role_system_user" | "reader_role_superuser" | "reader_role_bypassrls" | "reader_role_attributes"
  | "reader_role_membership" | "reader_role_members" | "reader_role_settings";

/** Names the failed property only; never an observed role name, the URL or a credential. */
export class ReaderRoleError extends Error {
  constructor(readonly code: ReaderRoleRefusal) {
    super(`session read api: ${code} (the read login must be ${READ_API_ROLE})`);
    this.name = "ReaderRoleError";
  }
}

/** The live session and the role's shape in one row (pattern: src/session-ingest/selfhost-server.ts INGEST_LOGIN_SQL). */
export const READER_LOGIN_SQL = `SELECT session_user::text AS session_user, current_user::text AS current_user,
  current_setting('role') AS role_setting, current_setting('server_version_num')::int AS server_version,
  current_setting('statement_timeout') AS statement_timeout,
  current_setting('default_transaction_read_only') AS default_transaction_read_only,
  r.rolsuper, r.rolbypassrls, r.rolcreaterole, r.rolcreatedb, r.rolreplication, r.rolinherit, r.rolcanlogin, r.rolconfig,
  (SELECT count(*)::int FROM pg_auth_members m WHERE m.member = r.oid) AS memberships,
  (SELECT count(*)::int FROM pg_auth_members m WHERE m.roleid = r.oid) AS members,
  (SELECT a.usename::text FROM pg_catalog.pg_stat_activity a WHERE a.pid = pg_catalog.pg_backend_pid()) AS backend_user
  FROM pg_roles r WHERE r.rolname = current_user`;
/**
 * PG16+ only: system_user, inherit_option and set_option do not exist earlier. LEFT JOINs, so an edge
 * whose member or grantor cannot be named is still listed, and then refused.
 */
export const READER_PG16_SQL = `SELECT system_user::text AS system_user,
  (SELECT coalesce(json_agg(json_build_object('member', u.rolname, 'grantor', g.rolname, 'grantor_super', g.rolsuper,
     'admin_option', a.admin_option, 'inherit_option', a.inherit_option, 'set_option', a.set_option)), '[]'::json)
   FROM pg_auth_members a LEFT JOIN pg_roles u ON u.oid = a.member LEFT JOIN pg_roles g ON g.oid = a.grantor
   WHERE a.roleid = (SELECT oid FROM pg_roles WHERE rolname = current_user)) AS member_edges`;

export interface ReaderProvider {
  /** SESSION_POSTGRES_TLS_PROFILE is supabase (the Lambda's implicit default included). */
  supabase: boolean;
}

/**
 * The one reverse membership edge permitted, on PG16+ with a Supabase provider only: Supabase's
 * non-superuser `postgres` (CREATEROLE) created the role (session_read_api_roles.sql allows exactly
 * that creator), so PostgreSQL 16 recorded `postgres` as a member granted by `supabase_admin`, WITH
 * ADMIN, INHERIT FALSE, SET FALSE: it may administer the role's grants but never assume or inherit it.
 * The same edge src/session-ingest/selfhost-server.ts permits for the ingest login.
 */
function supabaseCreatorEdge(edge: unknown): boolean {
  if (!edge || typeof edge !== "object") return false;
  const e = edge as Record<string, unknown>;
  return e.member === "postgres" && e.grantor === "supabase_admin" && e.grantor_super === true
    && e.admin_option === true && e.inherit_option === false && e.set_option === false;
}

/**
 * The admission decision over the live rows (pure, so a real-login row can be tested where PGlite has
 * no login). `pg16` is the READER_PG16_SQL row, required when server_version >= 160000. null admits.
 */
export function readerLoginRefusal(login: Record<string, unknown> | undefined, pg16: Record<string, unknown> | undefined, provider: ReaderProvider): ReaderRoleRefusal | null {
  if (!login) return "reader_role_unknown";
  // Identity: a real login as the role, never another login that SET ROLE into it.
  if (login.session_user !== READ_API_ROLE) return "reader_role_session_user";
  if (login.current_user !== READ_API_ROLE) return "reader_role_current_user";
  if (login.role_setting !== "none") return "reader_role_set_role";
  const version = login.server_version;
  if (typeof version !== "number" || !Number.isInteger(version) || version < 100000) return "reader_role_unknown";
  // The login this backend authenticated as (fixed at authentication, every version). A superuser's
  // SET SESSION AUTHORIZATION makes session_user and current_user the role with role 'none'; on PG15,
  // which has no system_user, only usename shows the real login (#366's recorded 15.19/16.15/17.11 rows).
  if (login.backend_user !== READ_API_ROLE) return "reader_role_backend_user";
  const modern = version >= 160000;
  if (modern) {
    if (!pg16) return "reader_role_unknown";
    // "<auth method>:<authenticated identity>"; NULL means trust auth, which proves nothing.
    const su = pg16.system_user;
    const colon = typeof su === "string" ? su.indexOf(":") : -1;
    if (typeof su !== "string" || colon < 1 || su.slice(colon + 1) !== READ_API_ROLE) return "reader_role_system_user";
  }
  // Shape: the bootstrap's LOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION.
  if (login.rolsuper !== false) return "reader_role_superuser";
  if (login.rolbypassrls !== false) return "reader_role_bypassrls";
  if (login.rolcreaterole !== false || login.rolcreatedb !== false || login.rolreplication !== false || login.rolinherit !== false || login.rolcanlogin !== true)
    return "reader_role_attributes";
  // Membership in both directions: a member of no role (the verifier's rule), and no role a member
  // of it but the Supabase creator edge.
  if (login.memberships !== 0) return "reader_role_membership";
  if (typeof login.members !== "number") return "reader_role_unknown";
  if (modern) {
    const edges = pg16!.member_edges;
    if (!Array.isArray(edges) || edges.length !== login.members || !edges.every((edge) => provider.supabase && supabaseCreatorEdge(edge))) return "reader_role_members";
  } else if (login.members !== 0) return "reader_role_members";
  // Settings: the role pins both (the verifier's `rolconfig @>`; other entries are allowed, as there),
  // and the live session shows exactly those values.
  const config = login.rolconfig;
  if (!Array.isArray(config)) return "reader_role_settings";
  for (const [k, v] of Object.entries(READER_SETTINGS)) {
    if (!config.includes(`${k}=${v}`)) return "reader_role_settings";
    if (login[k] !== v) return "reader_role_settings";
  }
  return null;
}

/**
 * Reader admission (the self-host service, and any portable Postgres search binding). Tenant isolation
 * on sessions.session_index and the search tables is row-level security, which a superuser, BYPASSRLS
 * or otherwise different login skips or changes without any error. So the pool's session must be a
 * real login as exactly bulls96_read_api (session_user, current_user, no SET ROLE, this backend's
 * pg_stat_activity usename so no SET SESSION AUTHORIZATION, and on PG16+
 * system_user), with the bootstrap's role shape, no membership edge in either direction (but the
 * Supabase creator edge) and the pinned settings. Any mismatch or failure to read the rows refuses
 * startup; the error names the failed property only. A query error propagates unchanged.
 */
export async function assertReaderRole(sql: SqlQuery, provider: ReaderProvider = { supabase: false }): Promise<void> {
  const { rows } = await sql.query<Record<string, unknown>>(READER_LOGIN_SQL);
  if (rows.length !== 1) throw new ReaderRoleError("reader_role_unknown");
  const login = rows[0];
  let pg16: Record<string, unknown> | undefined;
  if (typeof login?.server_version === "number" && login.server_version >= 160000) {
    const extra = await sql.query<Record<string, unknown>>(READER_PG16_SQL);
    if (extra.rows.length !== 1) throw new ReaderRoleError("reader_role_unknown");
    pg16 = extra.rows[0];
  }
  const refusal = readerLoginRefusal(login, pg16, provider);
  if (refusal) throw new ReaderRoleError(refusal);
}

async function buildApp(env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition): Promise<App> {
  const region = env.AWS_REGION;
  const secretArn = env.SESSION_READ_API_SECRET_ARN;
  if (!region || !secretArn) throw new Error("session read api: AWS_REGION and SESSION_READ_API_SECRET_ARN are required");
  parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  parseSelfhostReadApi(env);
  parseSelfhostLoginSync(env);
  factsBackends(env, "lambda");
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

/**
 * READ_API_OUTPUT_BACKEND / READ_API_DIGEST_BACKEND / READ_API_COORDINATION_OVERLAPS_BACKEND
 * (config.ts parseFactsBackend): which facts store serves these read routes. Unset = "clickhouse",
 * today's behaviour. "postgres" (the
 * self-host Postgres facts, migration 045040) is served only by the self-host service: the hosted
 * Lambda refuses it rather than ignore it. Any other value refuses to start.
 */
export function factsBackends(env: NodeJS.ProcessEnv, entry: ReadAppEntry) {
  const output = parseFactsBackend(env, READ_API_OUTPUT_BACKEND);
  const digest = parseFactsBackend(env, READ_API_DIGEST_BACKEND);
  const coordination = parseFactsBackend(env, READ_API_COORDINATION_OVERLAPS_BACKEND);
  if (entry !== "service" && (output === "postgres" || digest === "postgres" || coordination === "postgres"))
    throw new Error("session read api: a facts backend set to postgres is served only by the self-host service");
  return { output, digest, coordination };
}

/** The read API's dependencies except its verifier: the read-role pool, the
 * selected search binding (the ClickHouse client or the composition's adapter,
 * never both), directory/liveness SQL and the self-host login. `close` releases
 * the pool. readAppFromSecret adds the verifier from `authConfig`. */
export async function readApiDepsFromSecret(secretText: string, env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition, options: ReadAppOptions = {}): Promise<{ deps: Omit<ReadApiDeps, "verify">; authConfig: AuthConfig; close(): Promise<void>; digestNarrative?: DigestNarrativeDeps }> {
  const entry = options.entry ?? "lambda";
  const backend = searchBackend(env, composition, entry);
  const completeSinceMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  const selfhostReadApi = parseSelfhostReadApi(env);
  const sessionQuery = sessionQueryEnabled(env);
  const loginSync = parseSelfhostLoginSync(env);
  const facts = factsBackends(env, entry);
  const coordinationEnabled = coordinationOverlapsEnabled(env[READ_API_COORDINATION_OVERLAPS_ENABLED]);
  // The service always wires the brief-summary read; the hosted Lambda only with the exact flag.
  const briefSummaryWired = entry === "service" || exactTrueFlag(env[READ_API_BRIEF_SUMMARY_ENABLED]);
  const listShortSummary = exactTrueFlag(env[READ_API_LIST_SHORT_SUMMARY_ENABLED]);
  // The self-host service never inherits the hosted Supabase pin by omission.
  if (entry === "service" && !env.SESSION_POSTGRES_TLS_PROFILE)
    throw new Error("session read api: the self-host service needs SESSION_POSTGRES_TLS_PROFILE set explicitly");
  const tls = postgresTlsOptions(env);
  const secret = parseReadApiSecret(secretText, backend === "clickhouse");
  const target = readerPoolTarget(secret.supabase_url, entry);
  // Env only: a bad auth setting (including READ_API_AUTH_MODE) refuses before the pool opens.
  const authConfig = readApiAuthConfig(env);
  // The Function URL handler is GET-only and could never serve the token routes.
  if (authConfig.mode === "self_issued" && entry !== "service")
    throw new Error("session read api: READ_API_AUTH_MODE=self_issued is served only by the self-host service");
  const corsAllowedOrigins = entry === "service" ? readApiCorsOrigins(env) : undefined;
  // Analytics (src/shared/analytics-config.ts), before the pool opens: the service takes the self-host
  // profile (noop or collector, never PostHog). The Lambda has no analytics wiring, so a selection there
  // is refused rather than silently ignored.
  const analyticsSelection = entry === "service" ? analyticsConfig(env, "selfhost") : ANALYTICS_OFF;
  if (entry !== "service" && !["", "noop", "none"].includes(env[ANALYTICS_SINK_VAR] ?? ""))
    throw new Error(`session read api: ${ANALYTICS_SINK_VAR} is served only by the self-host service`);
  // Env only, before any pool opens: with READ_API_DIGEST_ENABLED=1 an invalid narrative model
  // id refuses the service. With the flag off no narrative variable is read.
  if (entry === "service" && digestEnabled(env)) digestNarrativeModel(env);
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
  // The admission principal: its own pool, never the read role's.
  const admitPool = secret.analysis_admit_url
    ? new pg.Pool({
        connectionString: postgresConnectionString(secret.analysis_admit_url),
        max: 1,
        application_name: "96bulls-session-analysis-admit",
        ssl: tls,
        idleTimeoutMillis: 60_000,
        connectionTimeoutMillis: 2_000,
        statement_timeout: 2_000,
        options: "-c statement_timeout=2000",
      })
    : undefined;
  admitPool?.on("error", (err) => log.error("analysis admit pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  const analysisBucket = env.READ_API_ANALYSIS_BUCKET?.trim();
  const analysis = analysisBucket
    ? sqlAnalysisSource(
        poolTenantRunner(pool as never),
        createS3Store({ bucket: analysisBucket, region: env.AWS_REGION ?? "", credentials: async () => lambdaCredentials() }),
        admitPool as never,
      )
    : undefined;
  let narrative: Awaited<ReturnType<typeof selfhostNarrativeFromEnv>>;
  try {
  // Directory, liveness and Postgres search isolation is forced RLS. Checked on
  // the service always and on any portable (Postgres) search binding; the hosted
  // ClickHouse Lambda's read role is verified by its own bootstrap
  // (supabase/bootstrap/session_read_api_role_verify.sql).
  // Supabase provider: the TLS profile, whose unset default (the Lambda's) is supabase too.
  if (entry === "service" || backend !== "clickhouse") await assertReaderRole(pool, { supabase: (env.SESSION_POSTGRES_TLS_PROFILE ?? "supabase") === "supabase" });
  const adapter = backend === "clickhouse" ? undefined : composeSearchService(composition!, { env, sql: pool, secrets: searchSecret(secret.search) }, "reader", poolTenantRunner(pool as never));
  if (adapter && await adapter.state.fromMs(new Date()) > completeSinceMs) throw new Error("read history floor is below search coverage");
  // The self-host service only: canonical S3 (LOCKER_*) behind timing, turns,
  // metadata and catalog. The hosted Lambda never builds it (those routes keep
  // their 501 `*_not_configured` there).
  const canonical = entry === "service" ? await serviceCanonicalDeps(env, poolTenantRunner(pool as never)) : undefined;
  const liveness = sqlLiveness(poolTenantRunner(pool as never));
  const operatorErased = entry === "service" && eraseRuntimeFlag(env, ERASE_FENCE_FLAG)
    ? sqlOperatorErased(poolTenantRunner(pool as never)) : undefined;
  // The self-host service only, READ_API_DIGEST_ENABLED=1 (digest-narrative-selfhost.ts): the
  // narrative cache on this read pool and, with a model, key and READ_API_DIGEST_NARRATIVE_DB_URL,
  // in-process generation through a separate writer pool. Also returned beside `deps`; it serves
  // GET /v1/digest/daily only through the Postgres digest backend below (deps.digest).
  narrative = entry === "service" ? await selfhostNarrativeFromEnv(env, poolTenantRunner(pool as never)) : undefined;
  // The self-host service only (factsBackends refuses postgres elsewhere): Output and the digest
  // from the Postgres facts through the read role, no ClickHouse. "clickhouse" there wires
  // nothing, as before (the service has no ClickHouse client: 501 *_not_configured).
  const factsDeps = { run: poolTenantRunner(pool as never), liveness };
  const outputDeps = facts.output === "postgres" && readApiRouteEnabled(env.READ_API_OUTPUT_ENABLED)
    ? { output: { enabled: true as const, backend: postgresOutputBackend(factsDeps) } } : {};
  // Narrative: the self-host narrative deps above (the cache, plus the in-process generator when
  // configured). READ_API_DIGEST_ENABLED=1 always builds them here; the cache-only fallback is
  // defensive only (a miss is then 200 `provider_not_configured`).
  const digestDeps = facts.digest === "postgres" && readApiRouteEnabled(env.READ_API_DIGEST_ENABLED)
    ? { digest: { ...(narrative?.deps ?? { cache: sqlNarrativeCache(poolTenantRunner(pool as never)) }), backend: postgresDigestBackend(factsDeps) } } : {};
  const coordinationDeps = coordinationEnabled
    ? { coordination: {
        enabled: true as const,
        ...(facts.coordination === "postgres" ? { backend: postgresCoordinationBackend(factsDeps) } : {}),
      } }
    : {};
  const deps: Omit<ReadApiDeps, "verify"> = {
    ...(adapter ? { adapter } : { ch: createClickHouseClient(secret.clickhouse!) }),
    liveness,
    ...(operatorErased ? { operatorErased } : {}),
    directory: sqlDirectory(poolTenantRunner(pool as never)),
    directoryWorkspaces: parseDirectoryWorkspaces(env.READ_API_DIRECTORY_WORKSPACES),
    ...(analysis ? { analysis } : {}),
    ...(briefSummaryWired ? { briefSummary: sqlBriefSummary(poolTenantRunner(pool as never)) } : {}),
    ...(listShortSummary ? { listShortSummary: true } : {}),
    briefTitles: sqlBriefTitles(poolTenantRunner(pool as never)),
    briefTitleWorkspaces: parseDirectoryWorkspaces(env.READ_API_BRIEF_TITLES_WORKSPACES),
    completeSinceMs,
    ...(canonical ? { timing: canonical.timing, turns: canonical.turns, canonicalEvents: canonical.events } : {}),
    ...eventMessageIdDeps(env, canonical?.turns),
    ...(sessionQuery ? { sessionQuery: true } : {}),
    ...outputDeps,
    ...digestDeps,
    ...coordinationDeps,
    ...(authConfig.selfIssued ? { selfIssued: { ...authConfig.selfIssued, store: sqlTokenStore(pool as never) } } : {}),
    ...(corsAllowedOrigins?.size ? { corsAllowedOrigins } : {}),
    ...(selfhostReadApi ? { selfhost: { ...(authConfig.mode === "self_issued" ? {} : { login: relaycontrolLogin(authConfig) }), ...(canonical ? { catalog: canonical.catalog } : {}), ...(loginSync ? { syncServed: true as const } : {}) } } : {}),
    log: (fields) => log.info("session read", fields),
  };
  const analytics = createAnalyticsSink(analyticsSelection, "session-read-api", "service", {
    report: (counters) => log.warn("analytics export degraded", analyticsCounterFields(analyticsSelection.sink, counters)),
  });
  if (analyticsSelection.sink !== "noop") deps.analytics = analytics;
  const writer = narrative;
  return {
    deps, authConfig,
    // The analytics shutdown flush first (bounded), then the pools and the narrative writer.
    close: async () => {
      await analytics.shutdown();
      await admitPool?.end();
      await (writer ? Promise.all([pool.end(), writer.close()]) : pool.end());
    },
    ...(writer ? { digestNarrative: writer.deps } : {}),
  };
  } catch (error) { await narrative?.close().catch(() => {}); await admitPool?.end(); await pool.end(); throw error; }
}

/** The same service assembly used by the Lambda loader, without credential
 * discovery. A reviewed artifact composition can provide registered adapters;
 * `options.entry` selects the self-host service's rules (see ReadAppEntry). */
export async function readAppFromSecret(secretText: string, env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition, options: ReadAppOptions = {}): Promise<{ app: App; close(): Promise<void> }> {
  const { deps, authConfig, close } = await readApiDepsFromSecret(secretText, env, composition, options);
  try {
    const verify = deps.selfIssued ? selfIssuedVerifier(deps.selfIssued.store, deps.selfIssued.audience) : readApiVerifier(authConfig);
    return { app: createReadApi({ verify, ...deps }), close };
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

/** The one write route (lane F1): POST /v1/sessions/:id/analysis, empty body. */
export const ANALYSIS_POST_PATH = /^\/v1\/sessions\/[^/]+\/analysis$/u;

/** Function URL event → fetch Request → app → Function URL result. GET, plus the analysis POST. */
export async function handleUrlEvent(app: { fetch(req: Request): Response | Promise<Response> }, event: FunctionUrlEvent): Promise<FunctionUrlResult> {
  const method = event.requestContext.http.method.toUpperCase();
  const post = method === "POST" && ANALYSIS_POST_PATH.test(event.rawPath);
  if (method !== "GET" && !post) return { statusCode: 405, headers: { "content-type": "application/json", allow: "GET" }, body: JSON.stringify({ error: { code: "method_not_allowed", message: "GET only" } }) };
  // No caller-controlled transcript, tenant, model or version enters an admission (Cloud's rule).
  if (post && event.body) return { statusCode: 400, headers: { "content-type": "application/json" }, body: JSON.stringify({ error: { code: "bad_request", message: "Request body is not accepted" } }) };
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
