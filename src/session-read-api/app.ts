/**
 * The session read API: an API-compatible subset of relayhistory's /v1 read
 * contract (packages/relayhistory/src/routes/recall.ts), served from ClickHouse
 * `transcript_events` with Supabase for erasure and the history floor.
 *
 * Phase A routes (runbooks/sessions-prod-session-read-api.md):
 *   GET /v1/sessions/:id/events     session replay page
 *   GET /v1/events?session=…        in-session reads (q, since, until, kind)
 *   GET /v1/events?q=…              workspace transcript search
 *
 * Anything this backend cannot answer exactly is `501 unsupported_by_backend`
 * with a `reason`, never a best-effort answer: the caller (Cloud) falls back
 * to relayhistory for it during the transition.
 *
 * Auth: a RelayAuth assertion JWT minted for THIS service's origin with scope
 * `rth:read` (verifyAgentRelayToken: RS256, JWKS, issuer, exact audience).
 * Org and workspace come only from the verified claims.
 */
import { Hono, type Context } from "hono";
import { ClickHouseError, type ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { CursorError, decodeCursor } from "./cursor.ts";
import { TenantMismatchError, readEventPage, type Liveness } from "./page.ts";
import type { EventQuery } from "./queries.ts";

export interface VerifiedIdentity {
  userId: string;
  orgId: string;
  workspaceId: string;
  scopes: string[];
}

export type Verifier = (bearer: string) => Promise<VerifiedIdentity>;

export class AuthError extends Error {
  constructor(readonly status: 401 | 403, readonly code: string, message: string) {
    super(message);
    this.name = "AuthError";
  }
}

export interface ReadApiDeps {
  verify: Verifier;
  ch: ClickHouseClient;
  liveness: Liveness;
  /** Epoch ms from which ClickHouse holds complete history; 0 = complete. */
  completeSinceMs: number;
  log?: (fields: Record<string, unknown>) => void;
  timeoutMs?: number;
}

/** relayhistory lib/recall.ts limits. */
export const EVENTS_DEFAULT_LIMIT = 200;
export const EVENTS_MAX_LIMIT = 1000;
/** Search pages above this are not served here (Cloud asks for at most 50). */
export const SEARCH_MAX_LIMIT = 100;
export const MAX_Q_CHARS = 500;
const MAX_ID_CHARS = 512;

class HttpError extends Error {
  constructor(readonly status: number, readonly code: string, message: string, readonly reason?: string) {
    super(message);
  }
}
const unsupported = (reason: string) => new HttpError(501, "unsupported_by_backend", `not served by this backend: ${reason}`, reason);
const bad = (message: string) => new HttpError(400, "bad_request", message);

/** relayhistory clampLimit: truncate, clamp to [1, max]; junk gives the default. */
export function clampLimit(raw: string | undefined, fallback: number, max: number): number {
  if (raw === undefined) return fallback;
  const n = Math.trunc(Number(raw));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(1, n));
}

function positiveInt(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

function parseTime(raw: string | undefined, name: string): number | undefined {
  if (raw === undefined || raw === "") return undefined;
  const t = Date.parse(raw);
  if (Number.isNaN(t)) throw bad(`${name} must be a date`);
  return t;
}

/** Exactly one value or none; a repeated parameter is a 400 like relayhistory's. */
function single(c: Context, name: string): string | undefined {
  const all = c.req.queries(name);
  if (!all || all.length === 0) return undefined;
  if (all.length > 1) throw bad(`${name} must be given at most once`);
  return all[0];
}

/** Parameters this phase never serves: their presence routes the call back to relayhistory. */
const UNSUPPORTED_PARAMS = ["taskRef", "noTaskRef", "tag", "agent", "noProject", "projectContains", "includeMessageId", "search", "kinds"];

function rejectUnsupported(c: Context): void {
  for (const p of UNSUPPORTED_PARAMS) if (c.req.queries(p)?.length) throw unsupported(`param_${p}`);
}

function attestWorkspace(c: Context, identity: VerifiedIdentity): void {
  const all = c.req.queries("workspace");
  // relayhistory serves an org-wide read without `workspace`; this backend is
  // workspace-scoped only, so that read stays there.
  if (!all || all.length === 0) throw unsupported("org_scope");
  if (all.length > 1 || !all[0]) throw bad("workspace must be given exactly once");
  if (all[0] !== identity.workspaceId) throw new HttpError(403, "forbidden", "workspace does not match the token");
}

export function createReadApi(deps: ReadApiDeps) {
  const app = new Hono<{ Variables: { identity: VerifiedIdentity; correlationId: string } }>();
  const log = deps.log ?? (() => {});

  app.use("*", async (c, next) => {
    const incoming = c.req.header("x-correlation-id");
    const correlationId = incoming && /^[A-Za-z0-9._:-]{1,128}$/.test(incoming) ? incoming : crypto.randomUUID();
    c.set("correlationId", correlationId);
    const started = Date.now();
    await next();
    c.header("Cache-Control", "private, no-store");
    c.header("X-Sessions-Read-Backend", "96bulls");
    c.header("X-Correlation-Id", correlationId);
    log({ route: c.req.routePath, status: c.res.status, duration_ms: Date.now() - started, correlation_id: correlationId });
  });

  app.get("/health", (c) => c.json({ ok: true }));

  app.use("/v1/*", async (c, next) => {
    const header = c.req.header("authorization") ?? "";
    const m = /^Bearer\s+(\S+)$/i.exec(header);
    if (!m) return c.json({ error: { code: "missing_authorization", message: "Bearer token required" }, correlationId: c.get("correlationId") }, 401);
    let identity: VerifiedIdentity;
    try {
      identity = await deps.verify(m[1]!);
    } catch (error) {
      const e = error instanceof AuthError ? error : new AuthError(401, "invalid_token", "token is not valid");
      return c.json({ error: { code: e.code, message: e.message }, correlationId: c.get("correlationId") }, e.status);
    }
    if (!identity.orgId || !identity.workspaceId)
      return c.json({ error: { code: "invalid_token", message: "token carries no workspace" }, correlationId: c.get("correlationId") }, 401);
    if (!identity.scopes.includes("rth:read"))
      return c.json({ error: { code: "forbidden", message: "missing required scope: rth:read" }, correlationId: c.get("correlationId") }, 403);
    c.set("identity", identity);
    await next();
    c.header("X-Relayhistory-Workspace-Id", identity.workspaceId);
  });

  const fail = (c: Context, error: unknown) => {
    const correlationId = c.get("correlationId") as string;
    const identity = c.get("identity") as VerifiedIdentity | undefined;
    if (error instanceof HttpError) {
      if (error.status === 501) log({ route: c.req.routePath, reason: error.reason, status: 501, correlation_id: correlationId });
      return c.json({ error: { code: error.code, message: error.message, ...(error.reason ? { reason: error.reason } : {}) }, correlationId }, error.status as never);
    }
    if (error instanceof CursorError) {
      return error.code === "foreign_cursor"
        ? c.json({ error: { code: "unsupported_by_backend", message: error.message, reason: "foreign_cursor" }, correlationId }, 501)
        : c.json({ error: { code: "bad_request", message: error.message }, correlationId }, 400);
    }
    if (error instanceof TenantMismatchError) {
      log({ route: c.req.routePath, reason: "cross_tenant_rows", status: 500, org_id: identity?.orgId, workspace_id: identity?.workspaceId, correlation_id: correlationId });
      return c.json({ error: { code: "internal", message: "read refused" }, correlationId }, 500);
    }
    if (error instanceof ClickHouseError) {
      // 159 TIMEOUT_EXCEEDED: the same answer relayhistory gives a cancelled search.
      const timeout = error.code === "159";
      log({ route: c.req.routePath, reason: timeout ? "clickhouse_timeout" : "clickhouse_error", error_code: error.code ?? String(error.status), status: 503, correlation_id: correlationId });
      return c.json({ error: { code: timeout ? "search_timeout" : "backend_unavailable", message: timeout ? "search took too long" : "read backend unavailable" }, correlationId }, 503);
    }
    log({ route: c.req.routePath, reason: "unavailable", error_code: (error as { name?: string })?.name ?? "error", status: 503, correlation_id: correlationId });
    return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId }, 503);
  };

  /** Session reads: erased → empty page; history older than the floor → 501. */
  async function sessionGate(identity: VerifiedIdentity, sessionId: string, source: string | undefined): Promise<"empty" | "ok"> {
    const start = await deps.liveness.sessionStart(identity.orgId, identity.workspaceId, sessionId, source);
    if (start.pointers > 0 && start.live === 0) return "empty";
    if (deps.completeSinceMs > 0) {
      if (start.createdAtMs === null) throw unsupported("history_floor_unknown_session");
      if (start.createdAtMs < deps.completeSinceMs) throw unsupported("history_floor");
    }
    return "ok";
  }

  app.get("/v1/sessions/:sessionId/events", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      rejectUnsupported(c);
      for (const p of ["q", "since", "until", "kind", "session", "project"]) if (c.req.queries(p)?.length) throw unsupported(`param_${p}`);
      const sessionId = c.req.param("sessionId");
      if (!sessionId || sessionId.length > MAX_ID_CHARS) throw bad("sessionId is not valid");
      const source = single(c, "source") || undefined;
      const order = single(c, "order") === "desc" ? "desc" : "asc";
      const limit = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
      const maxContent = positiveInt(single(c, "maxContent"));
      const cursor = decodeCursor(single(c, "cursor"));
      if ((await sessionGate(identity, sessionId, source)) === "empty")
        return c.json({ sessionId, events: [], nextCursor: null, correlationId: c.get("correlationId") });
      const page = await readEventPage(
        { ch: deps.ch, liveness: deps.liveness, timeoutMs: deps.timeoutMs },
        { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, order, limit, maxContent, cursor },
      );
      return c.json({ sessionId, events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return fail(c, error);
    }
  });

  app.get("/v1/events", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      rejectUnsupported(c);
      const sessionId = single(c, "session") || undefined;
      const qRaw = single(c, "q");
      const q = qRaw === undefined || qRaw === "" ? undefined : qRaw;
      if (q !== undefined && q.length > MAX_Q_CHARS) throw bad(`q must be at most ${MAX_Q_CHARS} characters`);
      if (sessionId === undefined && q === undefined) throw unsupported("unfiltered_events");
      if (sessionId !== undefined && sessionId.length > MAX_ID_CHARS) throw bad("session is not valid");
      const source = single(c, "source") || undefined;
      const project = single(c, "project") || undefined;
      const kind = single(c, "kind") || undefined;
      const order: "asc" | "desc" = single(c, "order") === "asc" ? "asc" : "desc";
      const sinceMs = parseTime(single(c, "since"), "since");
      const untilMs = parseTime(single(c, "until"), "until");
      const maxContent = positiveInt(single(c, "maxContent"));
      const cursor = decodeCursor(single(c, "cursor"));
      const base: EventQuery = { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, kind, projectId: project, q, sinceMs, untilMs, order, limit: 0, maxContent, cursor };

      if (sessionId !== undefined) {
        base.limit = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
        if ((await sessionGate(identity, sessionId, source)) === "empty")
          return c.json({ events: [], nextCursor: null, correlationId: c.get("correlationId") });
        const page = await readEventPage({ ch: deps.ch, liveness: deps.liveness, timeoutMs: deps.timeoutMs }, base);
        return c.json({ events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
      }

      // Workspace search: newest first only.
      if (order === "asc") throw unsupported("search_ascending");
      const requested = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
      if (requested > SEARCH_MAX_LIMIT) throw unsupported("search_limit");
      base.limit = requested;
      // History floor: search only where ClickHouse is complete; a page that
      // reaches the floor without filling would be missing older rows.
      const belowFloor = deps.completeSinceMs > 0 && (sinceMs === undefined || sinceMs < deps.completeSinceMs);
      if (belowFloor) base.sinceMs = deps.completeSinceMs;
      if (untilMs !== undefined && base.sinceMs !== undefined && untilMs < base.sinceMs) throw unsupported("history_floor");
      const page = await readEventPage({ ch: deps.ch, liveness: deps.liveness, timeoutMs: deps.timeoutMs }, base);
      if (belowFloor && page.nextCursor === null) throw unsupported("history_floor");
      return c.json({ events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return fail(c, error);
    }
  });

  app.notFound((c) => c.json({ error: { code: "unsupported_by_backend", message: "route not served by this backend", reason: "route" }, correlationId: c.get("correlationId") }, 501));

  return app;
}
