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
 * (optional overrides).
 */
import pg from "pg";
import { log } from "../shared/log.ts";
import { awsJsonCall, lambdaCredentials } from "../session-pipeline/aws-json.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import { composeSearchService, searchSecret, type SearchComposition } from "../shared/search-composition.ts";
import { validateAdapterEnv } from "@rhc/adapters/registry.ts";
import { createClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { createReadApi, parseDirectoryWorkspaces } from "./app.ts";
import { sqlDirectory } from "./directory.ts";
import { relayAuthVerifier, type AuthConfig } from "./auth.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { parseCompleteSince } from "./config.ts";
import { poolTenantRunner, sqlLiveness } from "./liveness.ts";

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

/**
 * The verifier's destinations from env, validated with the shared transport
 * rule. Unset JWKS/issuer fall back to the hosted RelayAuth defaults in
 * packages/pipeline/src/env.ts; a self-host sets both.
 */
export function readApiAuthConfig(env: NodeJS.ProcessEnv = process.env): AuthConfig {
  const policy = destinationPolicy(env);
  const jwksUrl = env.RELAYAUTH_JWKS_URL?.trim() || undefined;
  if (jwksUrl) assertDestinationUrl("RELAYAUTH_JWKS_URL", jwksUrl, policy);
  return { origin: env.READ_API_ORIGIN || undefined, jwksUrl, issuer: env.RELAYAUTH_ISSUER };
}

type App = ReturnType<typeof createReadApi>;

function searchBackend(env: NodeJS.ProcessEnv, composition?: SearchComposition) {
  return validateAdapterEnv(env, { runtime: ["lambda"], runtimeDefault: "lambda", search: ["clickhouse", ...(composition?.registry.registered().filter(name => name !== "clickhouse") ?? [])], searchDefault: "clickhouse" }).search;
}

async function buildApp(env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition): Promise<App> {
  const region = env.AWS_REGION;
  const secretArn = env.SESSION_READ_API_SECRET_ARN;
  if (!region || !secretArn) throw new Error("session read api: AWS_REGION and SESSION_READ_API_SECRET_ARN are required");
  parseCompleteSince(env.READ_API_COMPLETE_SINCE);
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

/** The same service assembly used by the Lambda loader, without credential
 * discovery. A reviewed artifact composition can provide registered adapters. */
export async function readAppFromSecret(secretText: string, env: NodeJS.ProcessEnv = process.env, composition?: SearchComposition): Promise<{ app: App; close(): Promise<void> }> {
  const backend = searchBackend(env, composition);
  const completeSinceMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  const tls = postgresTlsOptions(env);
  const secret = parseReadApiSecret(secretText, backend === "clickhouse");
  const connectionString = new URL(postgresConnectionString(secret.supabase_url));
  // pg lets URL parameters override pool configuration. A client-side query
  // timer does not cancel SQL, and neither URL nor ambient PGOPTIONS may
  // disable this reader's server-side bound. TLS/target sanitization stays in
  // postgresConnectionString; options from the URL are already removed there.
  for (const key of [...connectionString.searchParams.keys()])
    if (["statement_timeout", "query_timeout"].includes(key.toLowerCase())) connectionString.searchParams.delete(key);
  const pool = new pg.Pool({
    connectionString: connectionString.toString(),
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
  const adapter = backend === "clickhouse" ? undefined : composeSearchService(composition!, { env, sql: pool, secrets: searchSecret(secret.search) }, "reader", poolTenantRunner(pool as never));
  if (adapter && await adapter.state.fromMs(new Date()) > completeSinceMs) throw new Error("read history floor is below search coverage");
  const app = createReadApi({
    verify: relayAuthVerifier(readApiAuthConfig(env)),
    ...(adapter ? { adapter } : { ch: createClickHouseClient(secret.clickhouse!) }),
    liveness: sqlLiveness(poolTenantRunner(pool as never)),
    directory: sqlDirectory(poolTenantRunner(pool as never)),
    directoryWorkspaces: parseDirectoryWorkspaces(env.READ_API_DIRECTORY_WORKSPACES),
    completeSinceMs,
    log: (fields) => log.info("session read", fields),
  });
  return { app, close: () => pool.end() };
  } catch (error) { await pool.end(); throw error; }
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
