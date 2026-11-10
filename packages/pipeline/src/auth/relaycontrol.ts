/**
 * 96BullSessions: relaycontrol user access tokens (opaque, `sh_at_`), verified by
 * relaycontrol's in-cluster introspection endpoint. Self-host deployments drop
 * RelayAuth; this is the second accepted token family next to the RelayAuth JWT
 * paths in auth/relayauth.ts, behind its own default-off flag
 * (`RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS`, middleware/auth.ts and
 * src/session-read-api/auth.ts).
 *
 * Contract v1.2 (relaycontrol, owned by layered-control; pinned to
 * relay-control PR #20, src/auth/introspect.ts):
 *   POST {RELAYCONTROL_INTROSPECT_URL}/internal/v1/auth/introspect
 *   Authorization: Bearer <RELAYCONTROL_INTROSPECT_SECRET>   (never the user token)
 *   body {token, aud, workspace?, machine?}
 *   200 {active:true, sub, email, principal_type, machine_id: string|null, org,
 *        workspace, scopes, aud, iat, exp, token_id} | {active:false, reason}
 *   400 = malformed body (History validates token, workspace and machine
 *   first, so a caller cannot cause one: contract drift or a History bug);
 *   401 = our secret is wrong or missing; 404 = introspection disabled.
 *
 * Fail closed: a timeout (of the call or of the body read), network error, a
 * 3xx (never followed: the fetch uses `redirect: "manual"`, the only
 * non-following mode workerd supports), relaycontrol's 400, 401 (our
 * introspection secret is wrong: a deployment fault, not the caller's), 404,
 * 5xx, a body over 16 KiB, or a body that is not the
 * contract shape is RelaycontrolUnavailableError (the caller answers 503
 * `relaycontrol_unavailable`, retryable); `active:false` (any reason), a
 * principal_type other than "desktop-user", an audience, workspace or machine
 * mismatch is RelaycontrolRejectedError (401, generic
 * code; the reason goes to the log only). The audience is compared as a
 * normalized origin on both sides (lowercased scheme and host, no path or
 * trailing slash, an explicit non-default port kept), as relaycontrol matches
 * it, because relaycontrol echoes its configured spelling. The response's
 * org, workspace and scopes are the only source of identity: `workspace` in
 * the request is optional, and when it is sent the response must echo it
 * exactly; either way the AuthContext's workspace is the response's. Token
 * material is never logged, cached in clear or echoed: the cache key is
 * sha256(token), and logs carry `token_id` only.
 *
 * Machine verification (v1.1, AlleyOop courier surface only): the caller's
 * claimed machine id (the request's X-Machine-Id) is forwarded as `machine`,
 * a claim for relaycontrol to verify, never identity. It must match
 * NODE_ID_PATTERN or it is refused here without a call. The AuthContext's
 * machine claim (`claims.meta.machineId`, read by the ingest's
 * liveWriteMachineGate) is set ONLY from a response `machine_id` string equal
 * to the claim; with no claim, or `machine_id: null`, there is no machine
 * claim and the gate refuses every live write (machine_claim_refused).
 */
import {
  analyticsIdentity,
  type AnalyticsIdentity,
} from "./analytics-identity.js";
import type { AuthContext } from "../env.js";

export const RELAYCONTROL_TOKEN_PREFIX = "sh_at_";
export const RELAYCONTROL_INTROSPECT_PATH = "/internal/v1/auth/introspect";
/** The only relaycontrol scopes that grant anything here, and what they grant. */
export const RELAYCONTROL_SCOPE_ALLOWLIST: Readonly<Record<string, string>> =
  Object.freeze({ "rth:read": "rth:read", "rth:sync": "rth:sync" });
export const RELAYCONTROL_TOKEN_SUBJECT = "relaycontrol_user_access_token";
/** relaycontrol's NODE_ID_PATTERN: a claimed machine id it would not answer 400 to. */
export const RELAYCONTROL_MACHINE_ID_PATTERN = /^[A-Za-z0-9._:-]{1,200}$/;
/** The only principal this service admits; relaycontrol also issues e.g. "agent". */
export const RELAYCONTROL_PRINCIPAL_TYPE = "desktop-user";
/** Introspection responses larger than this are refused unread (503). */
export const RELAYCONTROL_MAX_RESPONSE_BYTES = 16 * 1024;

const DEFAULT_TIMEOUT_MS = 5_000;
const DEFAULT_POSITIVE_TTL_MS = 60_000;
const DEFAULT_NEGATIVE_TTL_MS = 5_000;
const DEFAULT_MAX_ENTRIES = 1_000;

export function isRelaycontrolToken(token: string): boolean {
  return token.startsWith(RELAYCONTROL_TOKEN_PREFIX);
}

/** Relaycontrol could not answer: the caller answers 503, retryable. */
export class RelaycontrolUnavailableError extends Error {
  readonly code = "relaycontrol_unavailable";
  constructor(readonly reason: string) {
    super("token introspection is unavailable");
    this.name = "RelaycontrolUnavailableError";
  }
}

/** The token is not accepted: the caller answers 401. `reason` is for server logs only. */
export class RelaycontrolRejectedError extends Error {
  readonly code = "unauthorized";
  constructor(readonly reason: string) {
    super("token is not valid");
    this.name = "RelaycontrolRejectedError";
  }
}

export type RelaycontrolLog = (
  level: "info" | "warn" | "error",
  message: string,
  fields: Record<string, unknown>,
) => void;

/** One JSON line on the console, never exported anywhere else. */
const consoleLog: RelaycontrolLog = (level, message, fields) => {
  const line = JSON.stringify({ level, message, component: "relaycontrol-auth", ...fields });
  if (level === "info") console.log(line);
  else console.warn(line);
};

export interface RelaycontrolIntrospectorOptions {
  /** `RELAYCONTROL_INTROSPECT_URL`, from configuration only, never from a request. */
  url: string;
  /** `RELAYCONTROL_INTROSPECT_SECRET`, the dedicated service secret. */
  secret: string;
  /**
   * The deployment's destination policy (src/shared/destination-url.ts), run
   * once on the URL; throws to refuse it. The built-in check always runs too.
   */
  validateUrl?: (url: string) => void;
  fetch?: typeof fetch;
  /** Epoch ms. */
  now?: () => number;
  timeoutMs?: number;
  positiveTtlMs?: number;
  negativeTtlMs?: number;
  maxEntries?: number;
  log?: RelaycontrolLog;
}

export interface RelaycontrolRequest {
  /** This service's configured origin, sent as `aud`; the echo must be the same normalized origin. */
  aud: string;
  /**
   * The workspace the request claims to target, when the caller knows it.
   * Omitted from the introspection body when undefined; relaycontrol's answer
   * is authoritative either way.
   */
  workspace?: string;
  /**
   * The caller-claimed machine id (the request's X-Machine-Id), forwarded
   * verbatim for relaycontrol to verify. Never identity on its own.
   */
  machine?: string;
}

export interface RelaycontrolIntrospector {
  verifyRelaycontrolToken(
    token: string,
    request: RelaycontrolRequest,
  ): Promise<AuthContext>;
}

type Outcome =
  | { ok: true; auth: AuthContext }
  | { ok: false; reason: string; tokenId?: string };

interface CacheEntry {
  outcome: Outcome;
  expiresAt: number;
}

/**
 * Shape check the introspection base URL: absolute http(s), no credentials,
 * query or fragment. Transport policy (https, loopback, in-cluster Service
 * names) is the composition's `validateUrl`. Never echoes the value.
 */
export function introspectEndpoint(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new Error("RELAYCONTROL_INTROSPECT_URL must be an absolute http(s) URL");
  }
  if (url.protocol !== "https:" && url.protocol !== "http:")
    throw new Error("RELAYCONTROL_INTROSPECT_URL must be an absolute http(s) URL");
  if (url.username || url.password || url.search || url.hash)
    throw new Error(
      "RELAYCONTROL_INTROSPECT_URL must not contain credentials, query or fragment",
    );
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}${RELAYCONTROL_INTROSPECT_PATH}`;
}

const WORKSPACE_SHAPE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$/;

/** The request-side workspace a caller may name: a bounded id, nothing else. */
export function isWorkspaceId(value: unknown): value is string {
  return typeof value === "string" && WORKSPACE_SHAPE.test(value);
}

/**
 * The audience as relaycontrol compares it (its `normalizedOrigin`): the URL
 * origin, so scheme and host are lowercased, any path or trailing slash is
 * dropped, a default port is dropped and an explicit non-default port kept.
 * Null for anything that is not a credential-free http(s) URL.
 */
export function normalizedAudience(value: string): string | null {
  try {
    const url = new URL(value.trim());
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    if (url.username || url.password) return null;
    return url.origin;
  } catch {
    return null;
  }
}

/**
 * Read at most `limit` bytes of a response body; null when it is larger (the
 * rest is cancelled, never buffered). A declared Content-Length over the limit
 * is refused before reading.
 */
async function readCapped(response: Response, limit: number): Promise<string | null> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > limit) {
    await response.body?.cancel().catch(() => {});
    return null;
  }
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(text),
  );
  return Array.from(new Uint8Array(digest), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}

function nonEmpty(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256;
}

function cloneAuth(auth: AuthContext): AuthContext {
  return {
    ...auth,
    scopes: [...auth.scopes],
    claims: {
      ...auth.claims,
      scopes: [...(auth.claims.scopes as string[])],
      ...(auth.claims.meta
        ? { meta: { ...(auth.claims.meta as Record<string, unknown>) } }
        : {}),
    },
    ...(auth.analyticsIdentity
      ? { analyticsIdentity: { ...auth.analyticsIdentity } }
      : {}),
  };
}

export function createRelaycontrolIntrospector(
  opts: RelaycontrolIntrospectorOptions,
): RelaycontrolIntrospector {
  if (!opts.secret?.trim())
    throw new Error("RELAYCONTROL_INTROSPECT_SECRET is required");
  const endpoint = introspectEndpoint(opts.url);
  opts.validateUrl?.(opts.url.trim());
  const secret = opts.secret.trim();
  const doFetch = opts.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const now = opts.now ?? (() => Date.now());
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const positiveTtlMs = Math.min(opts.positiveTtlMs ?? DEFAULT_POSITIVE_TTL_MS, DEFAULT_POSITIVE_TTL_MS);
  const negativeTtlMs = Math.min(opts.negativeTtlMs ?? DEFAULT_NEGATIVE_TTL_MS, DEFAULT_NEGATIVE_TTL_MS);
  const maxEntries = Math.max(1, opts.maxEntries ?? DEFAULT_MAX_ENTRIES);
  const log = opts.log ?? consoleLog;
  const cache = new Map<string, CacheEntry>();
  const inflight = new Map<string, Promise<Outcome>>();

  const remember = (key: string, outcome: Outcome, ttlMs: number) => {
    if (ttlMs <= 0) return;
    cache.delete(key);
    while (cache.size >= maxEntries) {
      const oldest = cache.keys().next().value;
      if (oldest === undefined) break;
      cache.delete(oldest);
    }
    cache.set(key, { outcome, expiresAt: now() + ttlMs });
  };

  const fetchFailureReason = (error: unknown): string => {
    const name = (error as { name?: string } | null)?.name;
    return name === "TimeoutError" || name === "AbortError" ? "timeout" : "network_error";
  };

  async function introspect(
    token: string,
    request: RelaycontrolRequest,
  ): Promise<{ outcome: Outcome; ttlMs: number }> {
    let response: Response;
    try {
      response = await doFetch(endpoint, {
        method: "POST",
        headers: {
          authorization: `Bearer ${secret}`,
          "content-type": "application/json",
          accept: "application/json",
        },
        body: JSON.stringify({
          token,
          aud: request.aud,
          ...(request.workspace !== undefined ? { workspace: request.workspace } : {}),
          ...(request.machine !== undefined ? { machine: request.machine } : {}),
        }),
        // workerd rejects "error"; "manual" never follows, and a 3xx is a
        // non-200, so it maps to 503 below.
        redirect: "manual",
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      throw new RelaycontrolUnavailableError(fetchFailureReason(error));
    }
    if (response.status === 401) {
      // Our introspection secret is wrong or missing: a deployment fault that
      // says nothing about the caller's token, so an outage (503), never a 401
      // that would tell clients to discard a valid token.
      await response.body?.cancel().catch(() => {});
      log("error", "relaycontrol introspection refused our service secret", {
        status: 401,
      });
      throw new RelaycontrolUnavailableError("introspect_unauthorized");
    }
    if (response.status === 400) {
      // History validates token, workspace and machine before calling, so a
      // caller cannot cause a 400: it means contract drift or a History bug.
      // An outage (503), never a 401, and never cached.
      await response.body?.cancel().catch(() => {});
      log("error", "relaycontrol introspection refused our request", {
        status: 400,
      });
      throw new RelaycontrolUnavailableError("introspect_bad_request");
    }
    if (response.status !== 200) {
      // 404 is introspection disabled on relaycontrol: an outage, not a refusal.
      await response.body?.cancel().catch(() => {});
      throw new RelaycontrolUnavailableError(`status_${response.status}`);
    }
    let text: string | null;
    try {
      text = await readCapped(response, RELAYCONTROL_MAX_RESPONSE_BYTES);
    } catch (error) {
      // The fetch's AbortSignal also bounds the body read.
      throw new RelaycontrolUnavailableError(fetchFailureReason(error));
    }
    if (text === null) throw new RelaycontrolUnavailableError("response_too_large");
    let body: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(text);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        throw new Error("not an object");
      body = parsed as Record<string, unknown>;
    } catch {
      throw new RelaycontrolUnavailableError("malformed_json");
    }
    if (typeof body.active !== "boolean")
      throw new RelaycontrolUnavailableError("malformed_response");
    const tokenId = nonEmpty(body.token_id) ? body.token_id : undefined;
    if (!body.active) {
      const reason = typeof body.reason === "string" ? body.reason.slice(0, 64) : "inactive";
      return {
        outcome: { ok: false, reason: `inactive:${reason}`, tokenId },
        ttlMs: negativeTtlMs,
      };
    }
    const scopes = body.scopes;
    if (
      !nonEmpty(body.sub) ||
      !nonEmpty(body.org) ||
      !nonEmpty(body.workspace) ||
      !nonEmpty(body.aud) ||
      !tokenId ||
      !Array.isArray(scopes) ||
      !scopes.every((s) => typeof s === "string") ||
      typeof body.exp !== "number" ||
      !Number.isFinite(body.exp) ||
      typeof body.iat !== "number" ||
      !Number.isFinite(body.iat) ||
      !(body.machine_id === null || body.machine_id === undefined || typeof body.machine_id === "string") ||
      !(body.email === undefined || body.email === null || typeof body.email === "string")
    )
      throw new RelaycontrolUnavailableError("malformed_response");
    // Only desktop users; an "agent" (or any new or absent) principal is refused.
    if (body.principal_type !== RELAYCONTROL_PRINCIPAL_TYPE)
      return {
        outcome: { ok: false, reason: "wrong-principal", tokenId },
        ttlMs: negativeTtlMs,
      };
    // relaycontrol echoes its configured spelling of the matched audience:
    // compare normalized origins, as it does.
    const echoed = normalizedAudience(body.aud);
    if (echoed === null || echoed !== normalizedAudience(request.aud))
      return {
        outcome: { ok: false, reason: "aud_mismatch", tokenId },
        ttlMs: negativeTtlMs,
      };
    if (request.workspace !== undefined && body.workspace !== request.workspace)
      return {
        outcome: { ok: false, reason: "workspace_mismatch", tokenId },
        ttlMs: negativeTtlMs,
      };
    const remainingMs = body.exp * 1000 - now();
    if (remainingMs <= 0)
      return {
        outcome: { ok: false, reason: "expired", tokenId },
        ttlMs: negativeTtlMs,
      };
    // The machine claim comes only from relaycontrol's verified machine_id, and
    // only when it is exactly the id this request claimed.
    let machineId: string | undefined;
    if (request.machine !== undefined && body.machine_id !== null && body.machine_id !== undefined) {
      if (body.machine_id !== request.machine)
        return {
          outcome: { ok: false, reason: "machine_mismatch", tokenId },
          ttlMs: negativeTtlMs,
        };
      machineId = body.machine_id as string;
    }
    const granted = [
      ...new Set(
        (scopes as string[])
          .filter((s) => Object.hasOwn(RELAYCONTROL_SCOPE_ALLOWLIST, s))
          .map((s) => RELAYCONTROL_SCOPE_ALLOWLIST[s]!),
      ),
    ];
    const userId = body.sub;
    const orgId = body.org;
    const workspaceId = body.workspace;
    const auth: AuthContext = {
      userId,
      orgId,
      workspaceId,
      tokenSubject: RELAYCONTROL_TOKEN_SUBJECT,
      scopes: granted,
      // Identifiers only; `meta.machineId` only for a verified machine claim.
      claims: {
        sub: userId,
        org: orgId,
        wks: workspaceId,
        aud: body.aud,
        iat: body.iat,
        exp: body.exp,
        jti: tokenId,
        scopes: granted,
        token_class: "relaycontrol_user_access_token",
        ...(machineId !== undefined ? { meta: { machineId } } : {}),
      },
      historyOrigin: request.aud,
    };
    const analytics: AnalyticsIdentity | undefined = analyticsIdentity({
      version: 2,
      userId,
      organizationId: orgId,
      workspaceId,
      actorKind: "human",
      actorId: userId,
      source: "cloud_whoami",
    });
    if (analytics) auth.analyticsIdentity = analytics;
    return {
      outcome: { ok: true, auth },
      ttlMs: Math.min(remainingMs, positiveTtlMs),
    };
  }

  return {
    async verifyRelaycontrolToken(token, request) {
      // Never send anything but a relaycontrol token, and never parse one as a JWT.
      if (!isRelaycontrolToken(token) || token.length > 4096 || /\s/.test(token))
        throw new RelaycontrolRejectedError("not_a_relaycontrol_token");
      if (!nonEmpty(request.aud))
        throw new RelaycontrolRejectedError("no_audience_configured");
      if (request.workspace !== undefined && !isWorkspaceId(request.workspace))
        throw new RelaycontrolRejectedError("malformed_request_workspace");
      if (request.machine !== undefined && !RELAYCONTROL_MACHINE_ID_PATTERN.test(request.machine))
        throw new RelaycontrolRejectedError("malformed_machine_claim");
      // One entry per (token, audience, workspace sent, machine claimed): a
      // result is never reused across claimed machines or workspaces.
      const key = JSON.stringify([
        await sha256Hex(token),
        request.aud,
        request.workspace ?? null,
        request.machine ?? null,
      ]);
      const hit = cache.get(key);
      let outcome: Outcome | undefined;
      if (hit && hit.expiresAt > now()) {
        // LRU: a hit moves to the back.
        cache.delete(key);
        cache.set(key, hit);
        outcome = hit.outcome;
      } else {
        // An expired entry is never reused, not even when relaycontrol is down.
        if (hit) cache.delete(key);
        let pending = inflight.get(key);
        if (!pending) {
          pending = introspect(token, request)
            .then(({ outcome: fresh, ttlMs }) => {
              remember(key, fresh, ttlMs);
              return fresh;
            })
            .finally(() => inflight.delete(key));
          inflight.set(key, pending);
        }
        try {
          outcome = await pending;
        } catch (error) {
          if (error instanceof RelaycontrolUnavailableError)
            log("warn", "relaycontrol introspection unavailable", {
              reason: error.reason,
            });
          throw error;
        }
      }
      if (!outcome.ok) {
        log("info", "relaycontrol token refused", {
          reason: outcome.reason,
          ...(outcome.tokenId ? { token_id: outcome.tokenId } : {}),
        });
        throw new RelaycontrolRejectedError(outcome.reason);
      }
      return cloneAuth(outcome.auth);
    },
  };
}

const shared = new Map<string, RelaycontrolIntrospector>();

/**
 * One introspector (and so one cache) per configured URL, secret and
 * effective cache TTLs for the life of the process, for callers that read
 * configuration per request (the Hono middleware reads `c.env`). The map key is a digest, never the secret.
 */
export async function sharedRelaycontrolIntrospector(
  opts: RelaycontrolIntrospectorOptions,
): Promise<RelaycontrolIntrospector> {
  // The effective cache TTLs are part of the key: a caller asking for a
  // shorter positive (or negative) window never reuses an introspector built
  // with a longer one.
  const positive = Math.min(opts.positiveTtlMs ?? DEFAULT_POSITIVE_TTL_MS, DEFAULT_POSITIVE_TTL_MS);
  const negative = Math.min(opts.negativeTtlMs ?? DEFAULT_NEGATIVE_TTL_MS, DEFAULT_NEGATIVE_TTL_MS);
  const key = await sha256Hex(`${opts.url.trim()}\n${opts.secret.trim()}\n${positive}\n${negative}`);
  let introspector = shared.get(key);
  if (!introspector) {
    introspector = createRelaycontrolIntrospector(opts);
    if (shared.size >= 8) shared.clear();
    shared.set(key, introspector);
  }
  return introspector;
}

/** Test seam: drop every shared introspector and its cache. */
export function resetSharedRelaycontrolIntrospectors(): void {
  shared.clear();
}
