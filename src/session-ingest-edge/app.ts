/**
 * The 96Bulls session-ingest edge: the self-host Desktop-upload front of the
 * session-ingest Worker (src/session-ingest/app.ts), in place of
 * relayhistory-cloud's signing edge (lib/delivery-forward.ts there), with no
 * RelayAuth. docs/session-ingest-edge.md.
 *
 *   Desktop --Bearer sh_at_...--> edge --introspect {token, aud}--> relaycontrol
 *                                   \--x-relay-ingest-identity + signature (signRequest)--> Worker
 *
 * Per request, in this order (each refusal forwards nothing):
 *   1. Route allowlist (EDGE_ROUTES, the single source of truth): anything else
 *      is 404. /v1/delivery/recovery and every /v1/custody/* route are never
 *      forwarded: they carry cloud-issuer custody witnesses authenticated only
 *      by the outer HMAC, which a user token must not be able to mint.
 *   2. Media type: a POST must be exactly application/json (parameters
 *      stripped); any other Content-Type, the custody and recovery wires
 *      included, is 415. Content-Encoding identity, gzip or zstd only (415).
 *      A declared Content-Length over MAX_DELIVERY_BYTES is 413.
 *   3. Bearer: only a relaycontrol user access token (`sh_at_`), else 401.
 *   4. Introspection through the shared client (packages/pipeline/src/auth/
 *      relaycontrol.ts) with `aud` = SESSION_INGEST_EDGE_ORIGIN, no workspace
 *      and no machine (contract pin 28e9d95). Inactive / refused / relaycontrol
 *      400: 401 invalid_token. Timeout, network, 404 (disabled), 5xx, a
 *      malformed answer, or relaycontrol's 401 (our secret is wrong: a
 *      deployment error): 503 relaycontrol_unavailable.
 *   5. Scope: rth:sync for POST routes, rth:read for GET routes, else 403.
 *   6. Body read bounded by MAX_DELIVERY_BYTES on the wire (413), unmodified.
 *   7. originIdGate (no-op today; the one place a future origin_id machine gate goes).
 *   8. Identity ONLY from the introspection answer (sub, org, workspace, mapped
 *      scopes; never `control`), signed exactly as the Worker verifies
 *      (signRequest / encodeIdentity, imported), and forwarded with a header
 *      set built from scratch: no client header but Content-Encoding reaches
 *      the Worker, so a client-supplied x-relay-ingest-identity, signature or
 *      Authorization can never pass. The query string is never forwarded (the
 *      signature covers the path only). The Worker's answer is streamed back.
 *
 * Never logged: the bearer, the introspection secret, the signing key, the
 * identity header or the body.
 */
import { Hono, type Context } from "hono";
import {
  isRelaycontrolSecretRefusal,
  isRelaycontrolToken,
  RelaycontrolRejectedError,
  type RelaycontrolIntrospector,
} from "@rhc/auth/relaycontrol.ts";
import { log } from "../shared/log.ts";
import {
  encodeIdentity,
  IDENTITY_HEADER,
  READ_SCOPE,
  readBounded,
  SIGNATURE_HEADER,
  signRequest,
  SYNC_SCOPE,
  type IngestIdentity,
} from "../session-ingest/app.ts";
import { DeliveryError, MAX_DELIVERY_BYTES } from "../session-ingest/vendor/delivery/delivery-contracts.ts";

export interface EdgeRoute {
  readonly method: "GET" | "POST";
  readonly path: string;
  readonly scope: typeof READ_SCOPE | typeof SYNC_SCOPE;
}

/**
 * Every route the edge forwards (Amendment 4). Exact method and path; nothing
 * else is ever forwarded. Mutating routes need rth:sync, reads rth:read. (The
 * Worker applies its own scope rule on top: it still wants rth:sync for
 * GET /v1/delivery/limits.)
 */
export const EDGE_ROUTES: readonly EdgeRoute[] = Object.freeze([
  { method: "POST", path: "/v1/delivery/batches", scope: SYNC_SCOPE },
  { method: "POST", path: "/v1/turns/batches", scope: SYNC_SCOPE },
  { method: "POST", path: "/v1/delivery/sessions/selection", scope: SYNC_SCOPE },
  { method: "POST", path: "/v1/delivery/sessions/reset", scope: SYNC_SCOPE },
  { method: "POST", path: "/v1/onboarding/heartbeat", scope: SYNC_SCOPE },
  { method: "GET", path: "/v1/onboarding/connection", scope: READ_SCOPE },
  { method: "GET", path: "/v1/delivery/limits", scope: READ_SCOPE },
].map((route) => Object.freeze(route as EdgeRoute)));

const ROUTE_INDEX = new Map(EDGE_ROUTES.map((route) => [`${route.method} ${route.path}`, route]));

/** The allowlisted route for this exact method and (already normalized) path, or undefined. */
export function edgeRoute(method: string, path: string): EdgeRoute | undefined {
  return ROUTE_INDEX.get(`${method} ${path}`);
}

export const JSON_MEDIA_TYPE = "application/json";
const FORWARDED_ENCODINGS = new Set(["identity", "gzip", "zstd"]);
const CORRELATION_ID = /^[A-Za-z0-9._:-]{1,128}$/;
/** How long the Worker may take to answer one forwarded request. */
export const DEFAULT_UPSTREAM_TIMEOUT_MS = 120_000;
const RESPONSE_HEADERS_DROPPED = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
  "transfer-encoding", "upgrade", "content-length", "set-cookie",
  // fetch() has already decoded any encoded answer; passing the header on would make the client decode twice.
  "content-encoding",
]);

/** What a future origin_id gate would see. Read-only; the body is the exact forwarded bytes. */
export interface OriginIdGateInput {
  readonly route: EdgeRoute;
  readonly identity: Readonly<IngestIdentity>;
  readonly body: Uint8Array;
}

/**
 * THE extension point for verifying the Desktop install's `origin_id` (the
 * body field that names the install; Desktop sends no X-Machine-Id). Today it
 * deliberately does nothing: the edge neither adds a machine gate on origin_id
 * nor assumes that one is absent, and origin_id is forwarded untouched in the
 * body. A future gate plugs in here, and only here, by throwing an
 * EdgeRefusal; it runs after authentication and the scope check and before
 * anything is signed or forwarded.
 */
export async function originIdGate(_input: OriginIdGateInput): Promise<void> {
  // Intentionally empty: no origin_id verification, no behaviour either way.
}

/** A refusal with its HTTP status and code; nothing has been forwarded. */
export class EdgeRefusal extends Error {
  constructor(readonly status: 401 | 403 | 404 | 413 | 415 | 503, readonly code: string, message: string) {
    super(message);
    this.name = "EdgeRefusal";
  }
}

export type EdgeDeps =
  | { enabled: false; log?: (fields: Record<string, unknown>) => void }
  | {
      enabled: true;
      /** SESSION_INGEST_EDGE_ORIGIN: the introspection `aud`. */
      origin: string;
      /** SESSION_INGEST_UPSTREAM_URL: the Worker's bare origin. */
      upstreamUrl: string;
      /** INGEST_SIGNING_KEY. */
      signingKey: string;
      /** The shared relaycontrol introspection client (one cache per process). */
      introspector: RelaycontrolIntrospector;
      /** Upstream fetch (tests: the in-process Worker). */
      fetch?: typeof fetch;
      /** Epoch seconds for the signature. */
      nowSeconds?: () => number;
      upstreamTimeoutMs?: number;
      log?: (fields: Record<string, unknown>) => void;
    };

type Env = { Variables: { correlationId: string; requestId: string } };

function errorBody(c: Context<Env>, code: string, message: string) {
  return { error: { code, message }, correlationId: c.get("correlationId") };
}

/** The media type without parameters, lowercased; "" when absent. */
export function mediaType(header: string | undefined): string {
  return (header ?? "").split(";")[0]!.trim().toLowerCase();
}

function bearer(header: string | undefined): string | null {
  const match = /^Bearer[ \t]+(\S+)[ \t]*$/i.exec(header ?? "");
  return match ? match[1]! : null;
}

export function createIngestEdgeApp(deps: EdgeDeps) {
  const app = new Hono<Env>();
  const emit = deps.log ?? ((fields) => log.info("session ingest edge", fields));

  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    const given = c.req.header("X-Correlation-Id")?.trim();
    c.set("requestId", requestId);
    c.set("correlationId", given && CORRELATION_ID.test(given) ? given : requestId);
    await next();
    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", c.get("correlationId"));
  });

  app.get("/healthz", (c) => c.json({ ok: true, enabled: deps.enabled }));

  if (!deps.enabled) {
    // Dormant: every route but liveness is 503, nothing is read or forwarded.
    app.all("*", (c) => {
      c.header("Retry-After", "60");
      return c.json(errorBody(c, "not_configured", "the session ingest edge is not enabled"), 503);
    });
    return app;
  }

  const live = deps;
  const upstream = new URL(live.upstreamUrl);
  const doFetch = live.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const nowSeconds = live.nowSeconds ?? (() => Math.floor(Date.now() / 1000));
  const timeoutMs = live.upstreamTimeoutMs ?? DEFAULT_UPSTREAM_TIMEOUT_MS;

  /** Steps 3-5: the identity relaycontrol vouches for, or a refusal. */
  async function authenticate(c: Context<Env>, route: EdgeRoute): Promise<IngestIdentity> {
    const header = c.req.header("Authorization");
    const token = bearer(header);
    if (!token) throw new EdgeRefusal(401, "missing_authorization", "missing Authorization header");
    if (!isRelaycontrolToken(token)) throw new EdgeRefusal(401, "invalid_token", "invalid or expired token");
    let auth;
    try {
      auth = await live.introspector.verifyRelaycontrolToken(token, { aud: live.origin });
    } catch (error) {
      if (error instanceof RelaycontrolRejectedError && !isRelaycontrolSecretRefusal(error))
        throw new EdgeRefusal(401, "invalid_token", "invalid or expired token");
      // Unavailable, our secret refused, or anything unexpected: fail closed, retryable.
      throw new EdgeRefusal(503, "relaycontrol_unavailable", "token introspection is unavailable");
    }
    if (auth.historyOrigin !== live.origin || !auth.workspaceId || auth.migration)
      throw new EdgeRefusal(401, "invalid_token", "invalid or expired token");
    // Only these four fields, only from relaycontrol's answer; never `control`.
    const identity: IngestIdentity = {
      userId: auth.userId,
      orgId: auth.orgId,
      workspaceId: auth.workspaceId,
      scopes: [...auth.scopes],
    };
    if (!identity.scopes.includes(route.scope))
      throw new EdgeRefusal(403, "forbidden", `missing required scope: ${route.scope}`);
    return identity;
  }

  app.all("*", async (c) => {
    const started = Date.now();
    const path = new URL(c.req.url).pathname;
    const route = edgeRoute(c.req.method, path);
    const fields: Record<string, unknown> = { route: route?.path ?? "unrouted", correlation_id: c.get("correlationId") };
    const refuse = (refusal: EdgeRefusal) => {
      emit({ ...fields, outcome: refusal.code, status: refusal.status, duration_ms: Date.now() - started });
      if (refusal.status === 503) c.header("Retry-After", "1");
      return c.json(errorBody(c, refusal.code, refusal.message), refusal.status);
    };
    try {
      // 1. Allowlist: before anything else is looked at.
      if (!route) throw new EdgeRefusal(404, "not_found", "No such route");
      // 2. Wire shape, before introspection: only plain JSON is ever forwarded.
      const contentType = c.req.header("Content-Type");
      if (route.method === "POST" ? mediaType(contentType) !== JSON_MEDIA_TYPE : contentType !== undefined && mediaType(contentType) !== JSON_MEDIA_TYPE)
        throw new EdgeRefusal(415, "unsupported_media_type", "Content-Type must be application/json");
      const encoding = (c.req.header("Content-Encoding") ?? "").trim().toLowerCase() || "identity";
      if (!FORWARDED_ENCODINGS.has(encoding))
        throw new EdgeRefusal(415, "unsupported_encoding", "Delivery request Content-Encoding is unsupported");
      const declared = c.req.header("Content-Length");
      if (declared !== undefined && !(Number(declared) <= MAX_DELIVERY_BYTES))
        throw new EdgeRefusal(413, "delivery_too_large", `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes`);
      // 3-5.
      const identity = await authenticate(c, route);
      fields.org_id = identity.orgId;
      fields.workspace_id = identity.workspaceId;
      // 6. The exact wire bytes, bounded; Content-Length is never trusted.
      let body: Uint8Array = new Uint8Array();
      if (route.method === "POST" && c.req.raw.body) {
        try {
          body = await readBounded(c.req.raw.body, MAX_DELIVERY_BYTES);
        } catch (error) {
          if (error instanceof DeliveryError && error.status === 413)
            throw new EdgeRefusal(413, "delivery_too_large", `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes`);
          throw error;
        }
      }
      // 7. No-op today.
      await originIdGate({ route, identity, body });
      // 8. Sign and forward; the header set is built from scratch.
      const encoded = encodeIdentity(identity);
      const headers = new Headers({
        [IDENTITY_HEADER]: encoded,
        [SIGNATURE_HEADER]: signRequest(live.signingKey, nowSeconds(), route.method, route.path, encoded, body),
        "X-Correlation-Id": c.get("correlationId"),
        accept: JSON_MEDIA_TYPE,
        "accept-encoding": "identity",
      });
      if (route.method === "POST") {
        headers.set("content-type", JSON_MEDIA_TYPE);
        if (encoding !== "identity") headers.set("content-encoding", encoding);
      }
      let response: Response;
      try {
        response = await doFetch(new URL(route.path, upstream).toString(), {
          method: route.method,
          headers,
          ...(route.method === "POST" ? { body } : {}),
          redirect: "error",
          signal: AbortSignal.timeout(timeoutMs),
        });
      } catch {
        throw new EdgeRefusal(503, "delivery_unavailable", "Durable delivery is temporarily unavailable");
      }
      emit({ ...fields, outcome: "forwarded", status: response.status, duration_ms: Date.now() - started, bytes_raw: body.byteLength });
      const out = new Headers();
      response.headers.forEach((value, key) => {
        if (!RESPONSE_HEADERS_DROPPED.has(key.toLowerCase())) out.set(key, value);
      });
      out.set("X-Request-Id", c.get("requestId"));
      out.set("X-Correlation-Id", c.get("correlationId"));
      return new Response(response.body, { status: response.status, headers: out });
    } catch (error) {
      if (error instanceof EdgeRefusal) return refuse(error);
      emit({ ...fields, outcome: "error", error_code: "edge_failed", duration_ms: Date.now() - started });
      return c.json(errorBody(c, "internal_error", "Internal server error"), 500);
    }
  });

  return app;
}
