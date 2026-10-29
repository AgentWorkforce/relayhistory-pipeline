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
