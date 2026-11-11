/**
 * Self-host ingest identity: the Desktop's own relaycontrol bearer (`sh_at_`),
 * introspected on EVERY request. This replaces the relayhistory HMAC
 * (app.ts signedBindingIdentity) only in the self-host SERVICE
 * (selfhost-server.ts); the hosted Worker never imports this file.
 *
 * Pattern A: POST /v1/cli/login on the self-host read API advertises
 * `rth:sync` only when introspection granted it, but this edge never trusts
 * that login reply or any request header for identity. By default the
 * introspector is built with no positive cache (positiveTtlMs 0), so a revoked
 * token or a removed `rth:sync` is refused on the very next request. Only
 * concurrent identical requests share one in-flight call. Refusals are cached
 * for relaycontrol's short negative TTL (at most 5 s, fail closed); outages
 * are never cached.
 *
 * RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS (selfhost-config.ts, default
 * 0 = off, an integer 0..60000) opts into the shared introspector's positive
 * cache: an admit is reused for min(TTL, the token's remaining exp) per
 * process, keyed by sha256(token) with this edge's one `aud` (no workspace or
 * machine is sent here), at most 1000 entries, LRU. The introspection answer
 * carries no revocation epoch, so revocation lag (a revoked token, or a removed
 * `rth:sync`, still admitted) is bounded only by that TTL.
 *
 *   no / malformed Authorization            401 missing_authorization
 *   not `sh_at_`, rejected, wrong audience  401 invalid_token (reason to the log only)
 *   expired                                 401 token_expired
 *   no org or workspace                     401 invalid_token
 *   granted scopes lack rth:sync            403 forbidden
 *   relaycontrol unreachable / 5xx / 401 to our secret / malformed answer
 *                                           503 relaycontrol_unavailable (retryable; never a 401 that
 *                                           would tell the Desktop to discard a good token)
 *
 * Identity is only what relaycontrol answers (org, workspace, sub, allowlisted
 * scopes, and when sent the live role and the organization's capture policy,
 * enforced by capture-policy.ts). No `control` (ordered-control) field is ever
 * produced here.
 */
import { CONSENT_SCOPE } from "./upload-consent.ts";
import {
  createRelaycontrolIntrospector,
  isRelaycontrolToken,
  RelaycontrolRejectedError,
  RelaycontrolUnavailableError,
  type RelaycontrolIntrospector,
  type RelaycontrolLog,
} from "@rhc/auth/relaycontrol.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { log } from "../shared/log.ts";
import { AuthError } from "../session-read-api/app.ts";
import { SYNC_SCOPE, type IngestIdentity } from "./app.ts";

export interface SelfhostAuthConfig {
  /** Normalized origin, sent as `aud`. */
  origin: string;
  introspectUrl: string;
  introspectSecret: string;
  /**
   * RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS: how long (ms) an admitted
   * token is reused before it is introspected again (the revocation lag).
   * Absent or 0: no positive cache, every request introspects.
   */
  introspectPositiveTtlMs?: number;
}

const relaycontrolLog: RelaycontrolLog = (level, message, fields) =>
  log[level](message, { route: "ingest.auth", reason: typeof fields.reason === "string" ? fields.reason : undefined, status: fields.status });

/**
 * The production introspector: the deployment's destination policy on the URL,
 * and no positive cache unless the operator set introspectPositiveTtlMs
 * (default 0). Negative TTL and the entry bound stay relaycontrol.ts's
 * defaults (5 s, 1000 entries LRU).
 */
export function selfhostIntrospector(config: SelfhostAuthConfig, env: NodeJS.ProcessEnv = process.env, fetcher?: typeof fetch): RelaycontrolIntrospector {
  const policy = destinationPolicy(env);
  return createRelaycontrolIntrospector({
    url: config.introspectUrl,
    secret: config.introspectSecret,
    validateUrl: (url) => void assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, policy),
    positiveTtlMs: config.introspectPositiveTtlMs ?? 0,
    log: relaycontrolLog,
    ...(fetcher ? { fetch: fetcher } : {}),
  });
}

/** The bearer, or null for an absent or malformed Authorization header. */
export function bearerOf(header: string | null | undefined): string | null {
  const m = /^Bearer ([^\s]+)$/i.exec(header ?? "");
  return m ? m[1]! : null;
}

/**
 * Verify one request's bearer. Throws AuthError (401 / 403 / 503) to refuse;
 * on success the identity carries `rth:sync` or `rth:consent` (callers gate every route but the
 * upload-consent write on `rth:sync`).
 */
export function relaycontrolIngestIdentity(config: SelfhostAuthConfig, introspector: RelaycontrolIntrospector) {
  return async (authorization: string | null | undefined): Promise<IngestIdentity> => {
    const bearer = bearerOf(authorization);
    if (!bearer) throw new AuthError(401, "missing_authorization", "missing Authorization header");
    if (!isRelaycontrolToken(bearer)) throw new AuthError(401, "invalid_token", "invalid or expired token");
    let auth;
    try {
      auth = await introspector.verifyRelaycontrolToken(bearer, { aud: config.origin });
    } catch (error) {
      if (error instanceof RelaycontrolUnavailableError) throw new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
      if (error instanceof RelaycontrolRejectedError) {
        if (error.reason === "expired" || error.reason === "inactive:expired") throw new AuthError(401, "token_expired", "token has expired");
        throw new AuthError(401, "invalid_token", "invalid or expired token");
      }
      // Anything unexpected is an outage, never a refusal of the caller's token.
      throw new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
    }
    if (auth.historyOrigin !== config.origin || auth.migration) throw new AuthError(401, "invalid_token", "invalid or expired token");
    if (!auth.userId || !auth.orgId || !auth.workspaceId) throw new AuthError(401, "invalid_token", "invalid or expired token");
    const scopes = [...auth.scopes];
    // rth:sync, or rth:consent alone (a person's confirmed upload-consent write, upload-consent.ts, which
    // relaycontrol mints without rth:sync). Callers serving any other route must still require rth:sync.
    if (!scopes.includes(SYNC_SCOPE) && !scopes.includes(CONSENT_SCOPE)) throw new AuthError(403, "forbidden", `missing required scope: ${SYNC_SCOPE}`);
    // The capture policy and role (introspection v1.5) only when relaycontrol sent them (capture-policy.ts).
    return {
      userId: auth.userId, orgId: auth.orgId, workspaceId: auth.workspaceId, scopes,
      ...(auth.role !== undefined ? { role: auth.role } : {}),
      ...(auth.capturePolicy ? { capturePolicy: auth.capturePolicy } : {}),
    };
  };
}
