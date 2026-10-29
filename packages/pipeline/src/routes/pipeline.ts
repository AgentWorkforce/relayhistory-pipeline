import { Hono } from "hono";
import type { AppContext, AppEnv, Bindings } from "../env.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import { loggerEnvFromBindings } from "../lib/logger.js";
import {
  deadLetterStoreFromEnv,
  errorReporterFromEnv,
  isPipelineStage,
  isSafeIdentifier,
  isTraceId,
  telemetryIdHash,
  PipelineRecorder,
  PipelineStoreUnavailable,
  RESERVED_ATTR_KEYS,
  pipelineEventSinkFromEnv,
  replayDeadLetters,
  sendToStage,
  summarizeTrace,
  type DeadLetterStore,
  type PipelineEventSink,
  type PipelineStage,
  type PipelineStatus,
  type RecordOptions,
  type TraceContext,
  type TraceScope,
  type StageMessage,
} from "../lib/pipeline/index.js";

/** Stages a client may report about itself. Server stages cannot be forged. */
const CLIENT_STAGES: readonly PipelineStage[] = [
  "ingest",
  "deliver.put",
  "fetch",
];
const CLIENT_STATUSES: readonly PipelineStatus[] = [
  "started",
  "ok",
  "error",
  "retrying",
  "skipped",
];
const MAX_REPORTED_EVENTS = 100;
const MAX_TELEMETRY_LOOKUPS = 100;
/** Client attrs that say which destination and which unit of work an event is for. */
const KEY_ATTRS = new Set(["destination", "chunk", "seq", "offset"]);
/** Never accepted as an attr key or a destination name: they name prototype slots. */
const PROTOTYPE_KEYS = new Set(["__proto__", "constructor", "prototype"]);

export interface PipelineRouteDeps {
  sink(env: Bindings): PipelineEventSink;
  deadLetters(env: Bindings): DeadLetterStore;
  send(
    env: Bindings,
    stage: PipelineStage,
    message: StageMessage,
  ): Promise<void>;
}

const defaultDeps: PipelineRouteDeps = {
  sink: (env) => pipelineEventSinkFromEnv(env),
  deadLetters: (env) => deadLetterStoreFromEnv(env),
  send: sendToStage,
};

function error(
  c: AppContext,
  status: 400 | 401 | 403 | 404 | 503,
  code: string,
  message: string,
) {
  return c.json(
    { error: { code, message }, correlationId: c.get("correlationId") ?? "" },
    status,
  );
}

function waitUntil(c: AppContext, work: Promise<unknown>): void {
  try {
    c.executionCtx.waitUntil(work);
  } catch {
    // No execution context (tests, `app.request`): the promise still runs.
  }
}

/**
 * The trace scope for resolved auth. A token with no workspace claim uses
 * `default`, the same workspace `applyIngest` stores its uploads under, so the
 * trace and the upload it describes stay in one workspace.
 */
function pipelineScope(c: AppContext): TraceScope {
  const auth = getAuth(c);
  return { orgId: auth.orgId, workspaceId: auth.workspaceId || "default" };
}

/** How far a client clock may sit from ours before its `at` is replaced by now. */
const MAX_CLIENT_LAG_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_CLIENT_LEAD_MS = 5 * 60 * 1000;

/**
 * A client-reported time, or now when it is missing or implausible. Accepting a
 * far-future `at` would park the row in the default partition, outside retention.
 */
function clientEventTime(value: unknown, now = Date.now()): Date | undefined {
  if (typeof value !== "string") return undefined;
  const at = Date.parse(value);
  if (
    Number.isNaN(at) ||
    at > now + MAX_CLIENT_LEAD_MS ||
    at < now - MAX_CLIENT_LAG_MS
  )
    return undefined;
  return new Date(at);
}

/**
 * Distinguishes recordings of one stage/status/attempt that are different work:
 * the destination plus the chunk, seq or offset the event is about.
 */
function clientEventKey(attrs: Record<string, unknown>) {
  const unit = attrs.chunk ?? attrs.seq ?? attrs.offset;
  return [
    typeof attrs.destination === "string" ? attrs.destination : "",
    unit === undefined ? "" : String(unit),
  ].join("/");
}

/**
 * Record a server-observed stage for this request's trace, after the response.
 * No trace id on the request means no pipeline row; the request log line still
 * carries machine_id and correlation_id.
 */
export function recordRequestStage(
  c: AppContext,
  stage: PipelineStage,
  status: PipelineStatus,
  context: { machineId?: string; sessionId?: string },
  options: RecordOptions = {},
  sink: PipelineEventSink = pipelineEventSinkFromEnv(c.env ?? {}),
) {
  const traceId = c.get("traceId");
  if (!traceId) return;
  const env = c.env ?? {};
  const recorder = new PipelineRecorder(
    {
      traceId,
      ...pipelineScope(c),
      ...(context.machineId ? { machineId: context.machineId } : {}),
      ...(context.sessionId ? { sessionId: context.sessionId } : {}),
    },
    {
      sink,
      reporter: errorReporterFromEnv(env),
      logger: loggerEnvFromBindings(env),
    },
  );
  waitUntil(
    c,
    recorder.record(stage, status, options).then(() => recorder.flush()),
  );
}

export function createPipelineRoutes(deps: PipelineRouteDeps = defaultDeps) {
  const routes = new Hono<AppEnv>();

  /** "Did my upload land?": every recorded stage of one trace, plus a verdict. */
  routes.get(
    "/pipeline/traces/:traceId",
    requireScope("rth:read"),
    async (c) => {
      const traceId = c.req.param("traceId").toLowerCase();
      if (!isTraceId(traceId))
        return error(c, 400, "bad_request", "traceId must be a UUID");
      try {
        const { events, truncated } = await deps
          .sink(c.env ?? {})
          .byTrace(pipelineScope(c), traceId);
        const summary = summarizeTrace(traceId, events, truncated);
        return c.json(
          { ...summary, correlationId: c.get("correlationId") ?? "" },
          summary.verdict === "not_found" ? 404 : 200,
        );
      } catch (cause) {
        if (cause instanceof PipelineStoreUnavailable)
          return error(
            c,
            503,
            "not_configured",
            "Pipeline event store is not configured; stage events are in logs only",
          );
        return error(
          c,
          503,
          "pipeline_store_unavailable",
          "Pipeline event store is temporarily unavailable",
        );
      }
    },
  );

  /**
   * Client-side stages (local ingest, object storage put, fetch), reported by the
   * delivery coordinator. Workspace comes from auth; machine and session ids are
   * the client's own and are validated as identifiers only.
   */
  routes.post("/pipeline/events", requireScope("rth:sync"), async (c) => {
    let body: { machineId?: unknown; events?: unknown };
    try {
      body = await c.req.json();
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    const machineId = body?.machineId;
    if (typeof machineId !== "string" || !isSafeIdentifier(machineId))
      return error(c, 400, "bad_request", "machineId is required");
    if (
      !Array.isArray(body.events) ||
      !body.events.length ||
      body.events.length > MAX_REPORTED_EVENTS
    )
      return error(
        c,
        400,
        "bad_request",
        `events must include 1 to ${MAX_REPORTED_EVENTS} items`,
      );
    const scope = pipelineScope(c);
    const sink = deps.sink(c.env ?? {});
    const env = c.env ?? {};
    // Validate the whole batch before writing any of it, so a rejected request
    // leaves no partial trace behind.
    const accepted: {
      ctx: TraceContext;
      stage: PipelineStage;
      status: PipelineStatus;
      options: RecordOptions;
    }[] = [];
    for (const [index, raw] of (body.events as unknown[]).entries()) {
      const event = raw as Record<string, unknown> | null;
      if (
        !event ||
        !isTraceId(event.traceId) ||
        !isPipelineStage(event.stage) ||
        !CLIENT_STAGES.includes(event.stage) ||
        typeof event.status !== "string" ||
        !CLIENT_STATUSES.includes(event.status as PipelineStatus) ||
        !validDestination(event.attrs)
      )
        return error(
          c,
          400,
          "bad_request",
          `events[${index}] needs a traceId, a client stage (${CLIENT_STAGES.join(", ")}), a status, and an identifier-shaped attrs.destination if one is given`,
        );
      const sessionId = isSafeIdentifier(event.sessionId)
        ? event.sessionId
        : undefined;
      const attrs = numericAndIdAttrs(event.attrs);
      accepted.push({
        ctx: {
          traceId: event.traceId.toLowerCase(),
          ...scope,
          machineId,
          ...(sessionId ? { sessionId } : {}),
        },
        stage: event.stage,
        status: event.status as PipelineStatus,
        options: {
          attempt:
            Number.isInteger(event.attempt) && (event.attempt as number) > 0
              ? (event.attempt as number)
              : 1,
          attrs,
          errorCode:
            typeof event.errorCode === "string" &&
            /^[A-Za-z0-9_.:-]{1,64}$/.test(event.errorCode)
              ? event.errorCode
              : undefined,
          key: clientEventKey(attrs),
          now: clientEventTime(event.at),
        },
      });
    }
    const recorders = accepted.map(
      ({ ctx }) =>
        new PipelineRecorder(ctx, { sink, logger: loggerEnvFromBindings(env) }),
    );
    await Promise.all(
      accepted.map(({ stage, status, options }, index) =>
        recorders[index]!.record(stage, status, options),
      ),
    );
    // A configured log export must finish after the response, not be cancelled.
    waitUntil(c, Promise.allSettled(recorders.map((r) => r.flush())));
    return c.json({ recorded: recorders.length });
  });

  return routes;
}

/**
 * A destination, when given, must survive the attr filter. Otherwise the event
 * would silently fall back to object storage (`destinationOf`) and count as a
 * put that never happened, so it is rejected instead.
 */
function validDestination(attrs: unknown): boolean {
  if (!attrs || typeof attrs !== "object" || Array.isArray(attrs)) return true;
  if (!("destination" in attrs)) return true;
  const destination = (attrs as Record<string, unknown>).destination;
  return (
    typeof destination === "string" &&
    isSafeIdentifier(destination) &&
    !PROTOTYPE_KEYS.has(destination)
  );
}

/**
 * Client attrs keep only numbers, booleans and identifier-shaped strings, so an
 * event can carry sizes, seqs, offsets, hashes and destination names but never
 * prose or content.
 */
function numericAndIdAttrs(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const attrs: Record<string, unknown> = {};
  // The keys that identify the destination and the work go first, so the cap
  // can never drop them and turn a legacy put into an object storage one.
  const entries = Object.entries(value).sort(
    ([a], [b]) => Number(KEY_ATTRS.has(b)) - Number(KEY_ATTRS.has(a)),
  );
  for (const [key, item] of entries.slice(0, 32)) {
    if (
      !/^[a-z0-9_.]{1,64}$/.test(key) ||
      RESERVED_ATTR_KEYS.has(key) ||
      PROTOTYPE_KEYS.has(key)
    )
      continue;
    if (typeof item === "number" && Number.isFinite(item)) attrs[key] = item;
    else if (typeof item === "boolean") attrs[key] = item;
    else if (
      typeof item === "string" &&
      isSafeIdentifier(item) &&
      !(key === "destination" && PROTOTYPE_KEYS.has(item))
    )
      attrs[key] = item;
  }
  return attrs;
}

/**
 * Operator-only dead letter list and replay. Not under `/v1` auth: it is gated by
 * `PIPELINE_OPS_SECRET` like the other internal routes, and spans workspaces.
 * Replay is a dry run unless the body says `"dryRun": false`.
 */
export function createPipelineOpsRoutes(deps: PipelineRouteDeps = defaultDeps) {
  const routes = new Hono<AppEnv>();

  routes.use("/internal/pipeline/*", async (c, next) => {
    const secret = c.env?.PIPELINE_OPS_SECRET;
    if (!secret)
      return error(c, 503, "not_configured", "PIPELINE_OPS_SECRET is not set");
    if (!(await sameSecret(c.req.header("Authorization"), `Bearer ${secret}`)))
      return error(c, 401, "unauthorized", "Unauthorized");
    await next();
  });

  routes.get("/internal/pipeline/dead-letters", async (c) => {
    const stage = c.req.query("stage");
    if (stage && !isPipelineStage(stage))
      return error(c, 400, "bad_request", "unknown stage");
    const traceId = c.req.query("traceId");
    const letters = await deps.deadLetters(c.env).list({
      ...(stage && isPipelineStage(stage) ? { stage } : {}),
      ...(traceId ? { traceId } : {}),
      states: ["dead", "replaying"],
      limit: Math.min(Number(c.req.query("limit")) || 100, 1000),
    });
    return c.json({
      deadLetters: letters.map(({ message: _message, ...letter }) => letter),
    });
  });

  /**
   * Keyed lookup, raw → telemetry: the tokens that stand for these machine and
   * session ids in logs, OTLP and Sentry, so their telemetry can be searched.
   */
  routes.post("/internal/pipeline/telemetry-ids", async (c) => {
    const key = loggerEnvFromBindings(c.env).telemetryIdKey;
    if (!key)
      return error(
        c,
        503,
        "not_configured",
        "PIPELINE_TELEMETRY_ID_KEY is not set; telemetry carries no machine or session ids",
      );
    let body: { machineIds?: unknown; sessionIds?: unknown };
    try {
      body = await c.req.json();
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    const ids = (value: unknown) =>
      Array.isArray(value)
        ? value.filter((id): id is string => typeof id === "string")
        : [];
    const machineIds = ids(body?.machineIds);
    const sessionIds = ids(body?.sessionIds);
    if (!machineIds.length && !sessionIds.length)
      return error(c, 400, "bad_request", "machineIds or sessionIds required");
    if ([...machineIds, ...sessionIds].some((id) => !id || id.length > 256))
      return error(c, 400, "bad_request", "ids must be 1 to 256 characters");
    if (machineIds.length + sessionIds.length > MAX_TELEMETRY_LOOKUPS)
      return error(
        c,
        400,
        "bad_request",
        `at most ${MAX_TELEMETRY_LOOKUPS} ids per request`,
      );
    const tokens = (kind: "machine" | "session", list: string[]) =>
      Object.fromEntries(
        list.map((id) => [id, telemetryIdHash(key, kind, id)]),
      );
    return c.json({
      machines: tokens("machine", machineIds),
      sessions: tokens("session", sessionIds),
    });
  });

  /**
   * Keyed lookup, telemetry → raw: a hashed log line or error report still
   * carries `trace_id`, `org_id` and `workspace_id` raw; this returns that
   * trace's events from service-role storage, raw machine and session ids
   * included. Operators only (the ops secret), any tenant.
   */
  routes.get("/internal/pipeline/traces/:traceId", async (c) => {
    const traceId = c.req.param("traceId").toLowerCase();
    if (!isTraceId(traceId))
      return error(c, 400, "bad_request", "traceId must be a UUID");
    const orgId = c.req.query("orgId");
    const workspaceId = c.req.query("workspaceId");
    if (!orgId || !workspaceId)
      return error(c, 400, "bad_request", "orgId and workspaceId are required");
    try {
      const { events, truncated } = await deps
        .sink(c.env ?? {})
        .byTrace({ orgId, workspaceId }, traceId);
      return c.json(summarizeTrace(traceId, events, truncated));
    } catch (failure) {
      return failure instanceof PipelineStoreUnavailable
        ? error(
            c,
            503,
            "not_configured",
            "Pipeline event store is not configured",
          )
        : error(
            c,
            503,
            "pipeline_store_unavailable",
            "Pipeline event store is temporarily unavailable",
          );
    }
  });

  routes.post("/internal/pipeline/dead-letters/replay", async (c) => {
    let body: Record<string, unknown> = {};
    try {
      const text = await c.req.text();
      body = text ? JSON.parse(text) : {};
    } catch {
      return error(c, 400, "bad_request", "Request body must be valid JSON");
    }
    if (body.stage !== undefined && !isPipelineStage(body.stage))
      return error(c, 400, "bad_request", "unknown stage");
    if (body.traceId !== undefined && !isTraceId(body.traceId))
      return error(c, 400, "bad_request", "traceId must be a UUID");
    const env = c.env;
    const result = await replayDeadLetters(
      {
        deadLetters: deps.deadLetters(env),
        sink: deps.sink(env),
        send: (stage, message) => deps.send(env, stage, message),
        logger: loggerEnvFromBindings(env),
      },
      {
        stage: body.stage as PipelineStage | undefined,
        traceId: body.traceId as string | undefined,
        workspaceId:
          typeof body.workspaceId === "string" ? body.workspaceId : undefined,
        limit: typeof body.limit === "number" ? body.limit : undefined,
        dryRun: body.dryRun !== false,
        force: body.force === true,
      },
    );
    return c.json(result);
  });

  return routes;
}

async function sameSecret(given: string | undefined, expected: string) {
  if (!given) return false;
  const encoder = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(given)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const left = new Uint8Array(a);
  const right = new Uint8Array(b);
  let diff = 0;
  for (let index = 0; index < left.length; index += 1)
    diff |= left[index]! ^ right[index]!;
  return diff === 0;
}

export const pipelineRoutes = createPipelineRoutes();
export const pipelineOpsRoutes = createPipelineOpsRoutes();
