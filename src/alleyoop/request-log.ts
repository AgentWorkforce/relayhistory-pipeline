/** The request line of the ingest API: the source of `pipeline.ingest.requests`. */
import type { Context, MiddlewareHandler } from "hono";
import { routePath } from "hono/route";
import { log } from "../shared/log.ts";

/**
 * The registered template of a request (`/v1/pipeline/traces/:traceId`), never its
 * URL: raw paths carry ids and query values. The last match, because an auth
 * middleware that answers first still belongs to the route it guarded.
 */
export const requestRoute = (c: Context) => routePath(c, -1);

export const requestLog = (): MiddlewareHandler<{ Variables: { traceId?: string } }> => async (c, next) => {
  const started = Date.now();
  await next();
  log.info("request completed", {
    route: requestRoute(c),
    status: c.res.status,
    duration_ms: Date.now() - started,
    trace_id: c.get("traceId"),
  });
};
