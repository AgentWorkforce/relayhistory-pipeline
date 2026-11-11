/**
 * The Node ingest edge on AWS (skeleton): step 6 of the direct-to-S3 / RHC-off plan
 * (docs/decisions/0004-direct-upload-inbox.md; the scoping doc's sections 2.4, 3.2 and 4.1). It is a
 * Node edge, not a Cloudflare Worker: the hosted Worker (worker.ts), the read Worker and the RHC
 * forward are untouched, and nothing here is routed or deployed yet (no stack change).
 *
 * Default off: INGEST_EDGE_ENABLED (unset, "" or "false" = off; exactly "true" = on; anything else
 * refuses startup). Off, `mountIngestEdge` returns null and reads no other variable: the edge is not
 * mounted at all, and every existing server (selfhost-server.ts, worker.ts) is unchanged.
 *
 * Per request, in this order, before any body byte is read:
 *   1. Route: only the rth:sync routes in INGEST_EDGE_ROUTES. Any other /v1 path is 501
 *      unsupported_by_backend; anything else (/internal/*, /healthz, ...) is 404. Neither introspects.
 *      A wrong method is 405.
 *   2. Identify: the bearer must be a relay-control user access token (`sh_at_`), checked by
 *      relay-control introspection (packages/pipeline/src/auth/relaycontrol.ts, contract v1.2) with
 *      a bounded timeout, no retry and no cache (positive and negative TTL 0; only concurrent identical
 *      calls share one in-flight request). Caching is a later step and gets its own default-off flag.
 *        no / malformed Authorization            401 missing_authorization
 *        an opaque relayhistory token (`rth_*`)  401 unsupported_token, never sent anywhere; except,
 *                                                with INGEST_EDGE_RTH_VERIFIER=supabase (default off),
 *                                                an `rth_at_` bearer goes to rth-verifier.ts instead
 *        any other non-`sh_at_` bearer           401 invalid_token
 *        inactive, wrong audience/principal      401 invalid_token (reason to the log only)
 *        expired                                 401 token_expired
 *        no rth:sync                             403 forbidden
 *        relay-control unreachable, timeout, 5xx, 401 to our secret, malformed answer
 *                                                503 relaycontrol_unavailable + jittered Retry-After
 *                                                (never a 401, which would log the Desktop out)
 *   3. Header strip: every client copy of a trust or credential header (EDGE_STRIPPED_HEADERS and any
 *      `x-relay-ingest-*`) is removed. The edge then sets `x-relay-ingest-identity` itself, from the
 *      introspection answer only.
 *   4. Pass-through: the request (method, path, query, remaining headers, body stream and
 *      Content-Encoding unchanged) goes to the in-process ingest handler (app.ts createIngestApp, no
 *      second network hop). Its status, headers and body come back unchanged.
 *
 * The handler's `identify` trusts only the identity bound to that exact Request object (a WeakMap the
 * edge fills), and refuses the request unless the identity header still equals it. A request that did
 * not come through the edge, or whose header was altered, is 401; no header alone can assert a tenant.
 *
 * Environment (enabled only; each missing or malformed value refuses startup, naming the variable,
 * never the value; there are no defaults pointing at any host):
 *   RELAYHISTORY_INGEST_ORIGIN      this edge's public origin, sent to relay-control as `aud`
 *   RELAYCONTROL_INTROSPECT_URL     relay-control's introspection base URL (destination policy: https,
 *                                   loopback, or an in-cluster Service name with ALLOW_IN_CLUSTER_HTTP=true)
 *   RELAYCONTROL_INTROSPECT_SECRET  the introspection service credential (injected, never on disk)
 *   INGEST_EDGE_RTH_VERIFIER        default off; "supabase" adds the `rth_at_` verifier and its own
 *                                   INGEST_EDGE_RTH_VERIFIER_* settings (rth-verifier.ts)
 * Bodies, tokens and header values are never logged.
 */
import {
  createRelaycontrolIntrospector,
  introspectEndpoint,
  normalizedAudience,
  type RelaycontrolLog,
} from "@rhc/auth/relaycontrol.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { log } from "../shared/log.ts";
import { AuthError } from "../session-read-api/app.ts";
import { createIngestApp, encodeIdentity, IDENTITY_HEADER, type IngestAppDeps, type IngestIdentity } from "./app.ts";
import { createRthVerifier, RTH_ACCESS_TOKEN_PREFIX, rthVerifierConfig, type RthVerifierOptions, type RthVerifierSettings } from "./rth-verifier.ts";
import { bearerOf, relaycontrolIngestIdentity } from "./selfhost-auth.ts";

export const INGEST_EDGE_FLAG = "INGEST_EDGE_ENABLED";
export const INGEST_EDGE_ORIGIN_VAR = "RELAYHISTORY_INGEST_ORIGIN";
export const INGEST_EDGE_INTROSPECT_URL_VAR = "RELAYCONTROL_INTROSPECT_URL";
export const INGEST_EDGE_INTROSPECT_SECRET_VAR = "RELAYCONTROL_INTROSPECT_SECRET";

/** The introspection call's bound (request and body read). No retry follows a failure. */
export const INGEST_EDGE_INTROSPECT_TIMEOUT_MS = 2_000;
/** Retry-After on the edge's own 503: a jittered 5..10 s, never a 1 s floor (scoping doc 4.3). */
export const INGEST_EDGE_RETRY_AFTER_MIN_S = 5;
export const INGEST_EDGE_RETRY_AFTER_MAX_S = 10;

/** The only routes this skeleton serves, by exact raw pathname. All need rth:sync. */
export const INGEST_EDGE_ROUTES: Readonly<Record<string, "GET" | "POST">> = Object.freeze({
  "/v1/delivery/batches": "POST",
  "/v1/turns/batches": "POST",
  "/v1/delivery/limits": "GET",
});

/**
 * The opaque relayhistory token classes (rth_at_, rth_rt_, rth_st_): refused and never sent anywhere,
 * except `rth_at_` when the default-off INGEST_EDGE_RTH_VERIFIER is on (rth-verifier.ts).
 */
export const RELAYHISTORY_TOKEN_PREFIX = "rth_";
export const UNSUPPORTED_TOKEN_CODE = "unsupported_token";

/**
 * Removed from every inbound request before pass-through. `x-relay-ingest-identity` and
 * `x-relay-ingest-signature` are what app.ts trusts for tenancy (docs/selfhost-composition.md, router
 * requirements); every other `x-relay-ingest-*` (the async opt-in name included) goes by prefix.
 * The bearer and other credential carriers never reach the handler either.
 */
export const EDGE_STRIPPED_HEADERS: readonly string[] = Object.freeze(["authorization", "proxy-authorization", "cookie"]);
export const EDGE_STRIPPED_PREFIX = "x-relay-ingest-";

export interface IngestEdgeSettings {
  /** Normalized origin, sent as `aud`. */
  origin: string;
  introspectUrl: string;
  introspectSecret: string;
  /** Present only with INGEST_EDGE_RTH_VERIFIER=supabase (rth-verifier.ts); absent = step 6 behaviour. */
  rthVerifier?: RthVerifierSettings;
}

export type IngestEdgeConfig = { enabled: false } | ({ enabled: true } & IngestEdgeSettings);

/** Exactly "true" enables; unset, empty or "false" is off; anything else throws (a typo never silently means either). */
export function ingestEdgeEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[INGEST_EDGE_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  throw new Error(`96bulls: ${INGEST_EDGE_FLAG} must be true or false`);
}

function need(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`96bulls: ${INGEST_EDGE_FLAG}=true needs ${name}`);
  return value;
}

/** Off: `{ enabled: false }` and nothing else is read. On: every value is checked, or startup is refused. */
export function ingestEdgeConfig(env: NodeJS.ProcessEnv = process.env): IngestEdgeConfig {
  if (!ingestEdgeEnabled(env)) return { enabled: false };
  const origin = normalizedAudience(need(env, INGEST_EDGE_ORIGIN_VAR));
  if (!origin) throw new Error(`96bulls: ${INGEST_EDGE_ORIGIN_VAR} must be an absolute http(s) origin`);
  const introspectUrl = need(env, INGEST_EDGE_INTROSPECT_URL_VAR);
  assertDestinationUrl(INGEST_EDGE_INTROSPECT_URL_VAR, introspectUrl, destinationPolicy(env));
  try {
    introspectEndpoint(introspectUrl);
  } catch {
    throw new Error(`96bulls: ${INGEST_EDGE_INTROSPECT_URL_VAR} must be an absolute http(s) URL without credentials, query or fragment`);
  }
  const introspectSecret = need(env, INGEST_EDGE_INTROSPECT_SECRET_VAR);
  // Read only with the edge on; off (the default), no INGEST_EDGE_RTH_VERIFIER* variable is read.
  const rth = rthVerifierConfig(env);
  if (!rth.enabled) return { enabled: true, origin, introspectUrl, introspectSecret };
  const { enabled: _on, ...rthVerifier } = rth;
  return { enabled: true, origin, introspectUrl, introspectSecret, rthVerifier };
}

export interface IngestEdgeOptions {
  /** Test seam: the introspection transport. Production: the global fetch. */
  fetch?: typeof fetch;
  /** Test seam: the introspection bound. Default INGEST_EDGE_INTROSPECT_TIMEOUT_MS. */
  timeoutMs?: number;
  /** Test seam: [0, 1) for the Retry-After jitter. */
  random?: () => number;
  /** Test seams for the `rth_at_` verifier (used only when settings.rthVerifier is present). */
  rthVerifier?: RthVerifierOptions;
}

const introspectionLog: RelaycontrolLog = (level, message, fields) =>
  log[level](message, { route: "ingest_edge.auth", reason: typeof fields.reason === "string" ? fields.reason : undefined, status: fields.status });

/** Verifies one Authorization header value; throws AuthError (401 / 403 / 503) to refuse. */
export type EdgeVerify = (authorization: string | null | undefined) => Promise<IngestIdentity>;

/**
 * The edge's bearer check: `rth_*` refused before anything is sent, then relay-control introspection
 * with no cache and no retry, mapped to the identity app.ts uses ({ userId, orgId, workspaceId, scopes };
 * never `control` or `admission`).
 */
export function edgeIdentify(settings: IngestEdgeSettings, env: NodeJS.ProcessEnv = process.env, options: IngestEdgeOptions = {}): EdgeVerify {
  const policy = destinationPolicy(env);
  const introspector = createRelaycontrolIntrospector({
    url: settings.introspectUrl,
    secret: settings.introspectSecret,
    validateUrl: (url) => void assertDestinationUrl(INGEST_EDGE_INTROSPECT_URL_VAR, url, policy),
    timeoutMs: options.timeoutMs ?? INGEST_EDGE_INTROSPECT_TIMEOUT_MS,
    positiveTtlMs: 0,
    negativeTtlMs: 0,
    log: introspectionLog,
    ...(options.fetch ? { fetch: options.fetch } : {}),
  });
  const introspected = relaycontrolIngestIdentity({ origin: settings.origin, introspectUrl: settings.introspectUrl, introspectSecret: settings.introspectSecret }, introspector);
  const rthVerify = settings.rthVerifier ? createRthVerifier(settings.rthVerifier, env, options.rthVerifier) : null;
  return async (authorization) => {
    const bearer = bearerOf(authorization);
    if (rthVerify && bearer?.startsWith(RTH_ACCESS_TOKEN_PREFIX)) return rthVerify(bearer);
    if (bearer?.startsWith(RELAYHISTORY_TOKEN_PREFIX))
      throw new AuthError(401, UNSUPPORTED_TOKEN_CODE, "opaque relayhistory tokens are not supported by this deployment");
    const identity = await introspected(authorization);
    return { userId: identity.userId, orgId: identity.orgId, workspaceId: identity.workspaceId, scopes: [...identity.scopes] };
  };
}

/** True for a header the edge never forwards from the client. */
export function isStrippedHeader(name: string): boolean {
  const k = name.toLowerCase();
  return k.startsWith(EDGE_STRIPPED_PREFIX) || EDGE_STRIPPED_HEADERS.includes(k);
}

/** A copy of `headers` without any trust or credential header, then the edge's own identity header. */
export function edgeHeaders(headers: Headers, identity: IngestIdentity): Headers {
  const out = new Headers();
  headers.forEach((value, key) => {
    if (!isStrippedHeader(key)) out.append(key, value);
  });
  out.set(IDENTITY_HEADER, encodeIdentity(identity));
  return out;
}

type FetchApp = { fetch(request: Request): Response | Promise<Response> };
/** What the in-process handler uses to resolve the identity of a request the edge handed it. */
export type EdgeHandlerIdentify = (request: Request) => IngestIdentity;

export interface IngestEdge {
  fetch(request: Request): Promise<Response>;
}

const correlationOf = (request: Request) => request.headers.get("x-correlation-id")?.trim().slice(0, 128) || crypto.randomUUID();

function refusal(status: number, code: string, message: string, correlationId: string, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify({ error: { code, message }, correlationId }), {
    status,
    headers: { "content-type": "application/json", "cache-control": "private, no-store", "x-correlation-id": correlationId, ...headers },
  });
}

/**
 * The edge over an in-process handler. `makeHandler` receives the only identity source the handler
 * may use; production passes `ingestEdgeHandler(deps)` (app.ts), tests may pass a recorder.
 */
export function createIngestEdge(verify: EdgeVerify, makeHandler: (identify: EdgeHandlerIdentify) => FetchApp, options: Pick<IngestEdgeOptions, "random"> = {}): IngestEdge {
  const bound = new WeakMap<Request, IngestIdentity>();
  const identify: EdgeHandlerIdentify = (request) => {
    const identity = bound.get(request);
    // Not handed over by this edge, or the identity header no longer says what the edge set: refuse.
    if (!identity || request.headers.get(IDENTITY_HEADER) !== encodeIdentity(identity))
      throw new AuthError(401, "missing_authorization", "missing Authorization header");
    return identity;
  };
  const handler = makeHandler(identify);
  const random = options.random ?? Math.random;
  const retryAfter = () => String(INGEST_EDGE_RETRY_AFTER_MIN_S + Math.floor(random() * (INGEST_EDGE_RETRY_AFTER_MAX_S - INGEST_EDGE_RETRY_AFTER_MIN_S + 1)));

  return {
    async fetch(request) {
      const correlationId = correlationOf(request);
      const url = new URL(request.url);
      const method = INGEST_EDGE_ROUTES[url.pathname];
      if (!method) {
        if (url.pathname.startsWith("/v1/"))
          return refusal(501, "unsupported_by_backend", "this route is not served by the ingest edge", correlationId);
        return refusal(404, "not_found", "not found", correlationId);
      }
      if (request.method !== method) return refusal(405, "method_not_allowed", `${method} only`, correlationId, { allow: method });

      // Identity before any body byte is read.
      let identity: IngestIdentity;
      try {
        identity = await verify(request.headers.get("authorization"));
      } catch (error) {
        const e = error instanceof AuthError ? error : new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
        log.info("ingest edge", { route: "ingest_edge.auth", outcome: e.code, status: e.status, correlation_id: correlationId });
        return refusal(e.status, e.code, e.message, correlationId, {
          ...(e.status === 503 ? { "retry-after": retryAfter() } : {}),
          ...(e.status === 401 ? { "www-authenticate": "Bearer" } : {}),
        });
      }

      const headers = edgeHeaders(request.headers, identity);
      headers.set("x-correlation-id", correlationId);
      const post = method === "POST";
      const forwarded = new Request(url.toString(), {
        method,
        headers,
        ...(post && request.body ? { body: request.body, duplex: "half" } : {}),
        signal: request.signal,
      } as RequestInit);
      bound.set(forwarded, identity);
      return handler.fetch(forwarded);
    },
  };
}

/** The handler deps the edge passes through; identity is the edge's alone. */
export type IngestEdgeAppDeps = Omit<IngestAppDeps, "identify" | "signingKey" | "asyncAdmission" | "syncAdmissionFrozenReceipt" | "sessionErase">;

/** The existing ingest app (app.ts), unchanged, with the edge's identity as its only identity source. */
export function ingestEdgeHandler(deps: IngestEdgeAppDeps): (identify: EdgeHandlerIdentify) => FetchApp {
  return (identify) => createIngestApp({ ...deps, identify: (request) => identify(request) });
}

/**
 * The mount point. Off (the default): null, and no other variable is read; nothing is mounted.
 * On: the edge over the in-process ingest app.
 */
export function mountIngestEdge(env: NodeJS.ProcessEnv, deps: IngestEdgeAppDeps, options: IngestEdgeOptions = {}): IngestEdge | null {
  const config = ingestEdgeConfig(env);
  if (!config.enabled) return null;
  return createIngestEdge(edgeIdentify(config, env, options), ingestEdgeHandler(deps), options);
}
