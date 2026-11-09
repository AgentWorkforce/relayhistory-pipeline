/**
 * Self-host ingest identity: the Desktop's own relaycontrol bearer (`sh_at_`),
 * introspected on EVERY request. This replaces the relayhistory HMAC
 * (app.ts signedBindingIdentity) only in the self-host SERVICE
 * (selfhost-server.ts); the hosted Worker never imports this file.
 *
 * Pattern A: POST /v1/cli/login on the self-host read API advertises
 * `rth:sync` only when introspection granted it, but this edge never trusts
 * that login reply, a cached grant or any request header for identity. The
 * introspector is built with no positive cache (positiveTtlMs 0), so a revoked
 * token or a removed `rth:sync` is refused on the very next request. Only
 * concurrent identical requests share one in-flight call. Refusals are cached
 * for relaycontrol's short negative TTL (fail closed).
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
 * scopes). No `control` (ordered-control) field is ever produced here.
 */
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
}

const relaycontrolLog: RelaycontrolLog = (level, message, fields) =>
  log[level](message, { route: "ingest.auth", reason: typeof fields.reason === "string" ? fields.reason : undefined, status: fields.status });

/** The production introspector: no positive cache, the deployment's destination policy on the URL. */
export function selfhostIntrospector(config: SelfhostAuthConfig, env: NodeJS.ProcessEnv = process.env, fetcher?: typeof fetch): RelaycontrolIntrospector {
  const policy = destinationPolicy(env);
  return createRelaycontrolIntrospector({
    url: config.introspectUrl,
    secret: config.introspectSecret,
    validateUrl: (url) => void assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, policy),
    positiveTtlMs: 0,
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
 * on success the identity carries `rth:sync`.
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
    if (!scopes.includes(SYNC_SCOPE)) throw new AuthError(403, "forbidden", `missing required scope: ${SYNC_SCOPE}`);
    return { userId: auth.userId, orgId: auth.orgId, workspaceId: auth.workspaceId, scopes };
  };
}
