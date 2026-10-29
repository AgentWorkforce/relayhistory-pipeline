/**
 * Stand-in for relayhistory-cloud packages/relayhistory/src/middleware/auth.ts.
 *
 * routes/pipeline.ts imports `getAuth` and `requireScope`. Those two are copied
 * from the upstream file at 8603c141bf7dfcd67687db1e5f02e48c1eb2ef61 unchanged.
 * There is NO token verification here (upstream `requireAuth` resolves rth_at_/
 * rth_st_ tokens and RelayAuth assertions against the database): the host app
 * authenticates the request and sets `auth` on the Hono context before these run.
 * `AuthContext` is re-exported from env.ts, where upstream declares it.
 */
import { createMiddleware } from "hono/factory";
import type { AppContext, AppEnv, AuthContext } from "../env.js";

export type { AuthContext };

export function getAuth(c: AppContext): AuthContext {
  const auth = c.get("auth");
  if (!auth) {
    throw new Error("auth context is not available");
  }
  return auth;
}

export function requireScope(scope: string) {
  return createMiddleware<AppEnv>(async (c, next) => {
    const auth = getAuth(c);
    if (!auth.scopes.includes(scope)) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: `missing required scope: ${scope}`,
          },
          correlationId: c.get("correlationId") ?? "",
        },
        403,
      );
    }

    await next();
  });
}

/**
 * 96BullSessions `requireAuth`: the RelayAuth JWT path only (upstream also
 * resolves `rth_at_`/`rth_st_` tokens against Neon, which this repo does not
 * have). Enabled by `ALLOW_INTERIM_RELAYAUTH_BEARER=true`; verifies an RS256
 * JWT against `RELAYAUTH_JWKS_URL` and sets `auth` on the context.
 */
import { IdentityError, verifyAgentRelayToken } from "../auth/relayauth.js";

export const requireAuth = createMiddleware<AppEnv>(async (c, next) => {
  const header = c.req.header("Authorization") ?? "";
  const token = header.startsWith("Bearer ") ? header.slice(7).trim() : "";
  const deny = (status: 401 | 403, code: string, message: string) =>
    c.json({ error: { code, message }, correlationId: c.get("correlationId") ?? "" }, status);
  if (!token) return deny(401, "missing_authorization", "missing Authorization header");
  if (token.startsWith("rth_at_") || token.startsWith("rth_st_"))
    return deny(401, "unsupported_token", "opaque relayhistory tokens are not supported by this deployment");
  if (c.env?.ALLOW_INTERIM_RELAYAUTH_BEARER !== "true")
    return deny(401, "auth_not_configured", "bearer auth is not enabled (ALLOW_INTERIM_RELAYAUTH_BEARER)");
  try {
    const auth = await verifyAgentRelayToken(token, c.env);
    c.set("auth", auth);
  } catch (err) {
    if (err instanceof IdentityError) return deny(401, err.code, err.message);
    throw err;
  }
  await next();
});
