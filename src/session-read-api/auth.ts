/**
 * Token verification for the session read API: RelayAuth's relayhistory
 * assertion (POST /v1/tokens/relayhistory-assertion), minted by Cloud for this
 * service's own origin. The shared verifier checks RS256 against the JWKS, the
 * issuer and an exact single audience equal to READ_API_ORIGIN; the legacy
 * shared audience and migration assertions are never accepted here.
 */
import { IdentityError, verifyAgentRelayToken } from "@rhc/auth/relayauth.ts";
import {
  isRelaycontrolToken,
  RelaycontrolRejectedError,
  RelaycontrolUnavailableError,
  sharedRelaycontrolIntrospector,
  type RelaycontrolIntrospector,
  type RelaycontrolIntrospectorOptions,
} from "@rhc/auth/relaycontrol.ts";
import { AuthError, type Verifier } from "./app.ts";
import type { LoginExchange } from "./selfhost.ts";

export interface AuthConfig {
  /** This service's origin, e.g. https://xyz.lambda-url.us-east-1.on.aws. Unset: every token is refused. */
  origin: string | undefined;
  jwksUrl?: string;
  issuer?: string;
  /**
   * Present only with RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true: relaycontrol
   * user access tokens (`sh_at_`) are introspected. A missing URL or secret
   * refuses those tokens (401 not_configured); RelayAuth tokens are unaffected.
   */
  relaycontrol?: { url?: string; secret?: string };
}

export function relayAuthVerifier(config: AuthConfig, verify = verifyAgentRelayToken): Verifier {
  return async (bearer) => {
    if (!config.origin) throw new AuthError(401, "not_configured", "this read API has no origin configured");
    let auth;
    try {
      auth = await verify(bearer, {
        RELAYAUTH_JWKS_URL: config.jwksUrl,
        RELAYAUTH_ISSUER: config.issuer,
        RELAYHISTORY_ORIGIN: config.origin,
      });
    } catch (error) {
      if (error instanceof IdentityError) throw new AuthError(401, "invalid_token", "token is not valid");
      throw error;
    }
    // Only an assertion bound to this origin: the verifier sets historyOrigin then.
    if (auth.historyOrigin !== config.origin) throw new AuthError(401, "invalid_token", "token is not bound to this service");
    if (auth.migration) throw new AuthError(401, "invalid_token", "migration assertions are not accepted");
    // Tenancy is exactly the asserted org, workspace and sponsor: the shared
    // extractor also accepts alternative claim names, which this service never
    // honours.
    const claims = auth.claims ?? {};
    const strip = (v: unknown) => (typeof v === "string" && v.startsWith("relay_") ? v.slice("relay_".length) : v);
    if (
      typeof claims.org !== "string" || typeof claims.wks !== "string" || typeof claims.sponsorId !== "string" ||
      strip(claims.org) !== auth.orgId || strip(claims.wks) !== auth.workspaceId || strip(claims.sponsorId) !== auth.userId
    )
      throw new AuthError(401, "invalid_token", "token tenancy is ambiguous");
    return { userId: auth.userId, orgId: auth.orgId, workspaceId: auth.workspaceId ?? "", scopes: auth.scopes ?? [] };
  };
}

/**
 * A relaycontrol user access token (`sh_at_`) through relaycontrol's
 * introspection, bound to READ_API_ORIGIN as `aud`. A single `workspace=` is
 * sent along and relaycontrol must return exactly that workspace; without one
 * the response's workspace is authoritative (the routes still require
 * `workspace=` to equal the token's). Identity is only what relaycontrol
 * answers; reads send no machine claim. Unreachable, disabled or malformed:
 * 503 relaycontrol_unavailable.
 */
export function relaycontrolVerifier(
  config: AuthConfig,
  introspector: (opts: RelaycontrolIntrospectorOptions) => Promise<RelaycontrolIntrospector> = sharedRelaycontrolIntrospector,
): Verifier {
  return async (bearer, request) => {
    if (!isRelaycontrolToken(bearer)) throw new AuthError(401, "invalid_token", "token is not valid");
    const url = config.relaycontrol?.url?.trim();
    const secret = config.relaycontrol?.secret?.trim();
    if (!config.origin || !url || !secret)
      throw new AuthError(401, "not_configured", "this read API has no relaycontrol introspection configured");
    let auth;
    try {
      const verifier = await introspector({ url, secret });
      auth = await verifier.verifyRelaycontrolToken(bearer, {
        aud: config.origin,
        ...(request?.workspace ? { workspace: request.workspace } : {}),
      });
    } catch (error) {
      if (error instanceof RelaycontrolUnavailableError) throw new AuthError(503, error.code, error.message);
      if (error instanceof RelaycontrolRejectedError) throw new AuthError(401, error.code, error.message);
      throw new AuthError(401, "not_configured", "this read API has no relaycontrol introspection configured");
    }
    if (auth.historyOrigin !== config.origin || auth.migration)
      throw new AuthError(401, "unauthorized", "token is not bound to this service");
    if (!auth.workspaceId || (request?.workspace && auth.workspaceId !== request.workspace))
      throw new AuthError(401, "unauthorized", "token is not valid");
    return { userId: auth.userId, orgId: auth.orgId, workspaceId: auth.workspaceId ?? "", scopes: auth.scopes };
  };
}

/**
 * The read API's verifier: with relaycontrol configured (the flag on), a
 * `sh_at_` bearer goes to relaycontrol and nothing else does; every other
 * bearer, and every bearer with the flag off, takes relayAuthVerifier
 * unchanged (which refuses a `sh_at_` bearer as invalid_token, as before).
 */
export function readApiVerifier(
  config: AuthConfig,
  deps: { verify?: typeof verifyAgentRelayToken; introspector?: Parameters<typeof relaycontrolVerifier>[1] } = {},
): Verifier {
  const relayAuth = relayAuthVerifier(config, deps.verify);
  if (!config.relaycontrol) return relayAuth;
  const relaycontrol = relaycontrolVerifier(config, deps.introspector);
  return (bearer, request) => (isRelaycontrolToken(bearer) ? relaycontrol(bearer, request) : relayAuth(bearer, request));
}

/**
 * POST /v1/cli/login's exchange (RELAYHISTORY_SELFHOST_READ_API): the presented
 * `sh_at_` token through the same introspection as every relaycontrol read, with
 * `aud` = READ_API_ORIGIN and no workspace (the response's is authoritative).
 * Fail closed: the relaycontrol path off, unconfigured, unreachable or answering
 * malformed data is a 503, never a 401 that would tell a client to drop a good
 * token. Refusals are 401 with a fixed code mapped from the introspector's
 * reason; nothing from the token or relaycontrol's reason text is echoed.
 */
export function relaycontrolLogin(
  config: AuthConfig,
  introspector: (opts: RelaycontrolIntrospectorOptions) => Promise<RelaycontrolIntrospector> = sharedRelaycontrolIntrospector,
): LoginExchange {
  return async (token) => {
    if (!config.relaycontrol)
      throw new AuthError(503, "relaycontrol_not_enabled", "relaycontrol token introspection is not enabled");
    const url = config.relaycontrol.url?.trim();
    const secret = config.relaycontrol.secret?.trim();
    if (!config.origin || !url || !secret)
      throw new AuthError(503, "relaycontrol_not_configured", "relaycontrol token introspection is not configured");
    if (!isRelaycontrolToken(token)) throw new AuthError(401, "invalid_token", "token is not valid");
    let auth;
    try {
      const verifier = await introspector({ url, secret });
      auth = await verifier.verifyRelaycontrolToken(token, { aud: config.origin });
    } catch (error) {
      if (error instanceof RelaycontrolUnavailableError) throw new AuthError(503, error.code, error.message);
      if (error instanceof RelaycontrolRejectedError) {
        const reason = error.reason;
        if (reason === "expired" || reason === "inactive:expired") throw new AuthError(401, "token_expired", "token has expired");
        if (reason.startsWith("inactive:")) throw new AuthError(401, "token_inactive", "token is not active");
        if (reason === "aud_mismatch") throw new AuthError(401, "wrong_audience", "token is not bound to this service");
        throw new AuthError(401, "invalid_token", "token is not valid");
      }
      // A URL or secret the introspector refuses to be built with: configuration, not the caller.
      throw new AuthError(503, "relaycontrol_not_configured", "relaycontrol token introspection is not configured");
    }
    if (auth.historyOrigin !== config.origin || auth.migration)
      throw new AuthError(401, "wrong_audience", "token is not bound to this service");
    const exp = auth.claims?.exp;
    if (typeof exp !== "number" || !Number.isFinite(exp))
      throw new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
    if (!auth.orgId || !auth.workspaceId) throw new AuthError(401, "invalid_token", "token carries no workspace");
    if (!auth.scopes.includes("rth:read")) throw new AuthError(403, "forbidden", "missing required scope: rth:read");
    return {
      identity: { userId: auth.userId, orgId: auth.orgId, workspaceId: auth.workspaceId, scopes: auth.scopes },
      expiresAtMs: exp * 1000,
    };
  };
}
