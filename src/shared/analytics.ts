/**
 * Product analytics port for the sessions pipeline (helm-charts
 * docs/analytics-adapter-contract.md, docs/analytics-collector-interface.md).
 *
 * Business logic emits typed domain events through `AnalyticsPort.capture`; the
 * deployment supplies the destination (analytics-config.ts): NoopSink (the
 * default), the self-host collector, or the hosted PostHog. Business logic never
 * selects a destination and never sees one fail: `capture` only enqueues, never
 * awaits export and never throws.
 *
 * Every event is projected here into the versioned export envelope through an
 * explicit per-kind allowlist. Properties are closed vocabularies, bucketed
 * latencies and clamped counts; never session content, raw requests, paths,
 * session ids, user ids, error text or tokens. A field the projector does not
 * name never leaves, whatever a caller puts on the event object.
 *
 * Runtime-neutral: this file, analytics-export.ts and analytics-config.ts run in
 * the Cloudflare Workers (read API, ingest) and in the Node self-host services,
 * so they use only fetch, timers, TextEncoder and crypto.randomUUID.
 */

/** Envelope version (collector interface: initially 1). */
export const ANALYTICS_ENVELOPE_VERSION = 1;

/** Producer components (proposed wire ids; registration pending owner review in the producer registry). */
export type AnalyticsComponent = "session-ingest" | "session-read-api" | "session-pipeline-consumer";
export const ANALYTICS_COMPONENTS: readonly AnalyticsComponent[] = ["session-ingest", "session-read-api", "session-pipeline-consumer"];

/** Closed latency buckets: a duration is never exported as a raw number. */
export type LatencyBucket = "lt_100ms" | "lt_250ms" | "lt_500ms" | "lt_1s" | "lt_2500ms" | "lt_5s" | "lt_30s" | "ge_30s";

export function latencyBucket(ms: unknown): LatencyBucket {
  const v = typeof ms === "number" && Number.isFinite(ms) && ms >= 0 ? ms : Number.POSITIVE_INFINITY;
  if (v < 100) return "lt_100ms";
  if (v < 250) return "lt_250ms";
  if (v < 500) return "lt_500ms";
  if (v < 1_000) return "lt_1s";
  if (v < 2_500) return "lt_2500ms";
  if (v < 5_000) return "lt_5s";
  if (v < 30_000) return "lt_30s";
  return "ge_30s";
}

/** The ingest outcome classes; every native outcome and refusal code maps to one. */
export type IngestOutcome = "accepted" | "replay" | "rejected" | "conflict" | "too_large" | "unauthorized" | "busy" | "unavailable";
const INGEST_OUTCOMES: ReadonlySet<string> = new Set<IngestOutcome>(["accepted", "replay", "rejected", "conflict", "too_large", "unauthorized", "busy", "unavailable"]);

/**
 * A batch's native outcome (deliver.ts / app.ts `outcome`) or its refusal status, as a class.
 * A success (no status, or 2xx): any `*replay*` outcome is a replay, every other one accepted.
 * A refusal: by HTTP status.
 */
export function ingestOutcome(native: { outcome?: string; status?: number }): IngestOutcome {
  if (native.status === undefined || native.status < 300) return (native.outcome ?? "").includes("replay") ? "replay" : "accepted";
  const s = native.status;
  if (s === 409) return "conflict";
  if (s === 413) return "too_large";
  if (s === 401 || s === 403) return "unauthorized";
  if (s === 429) return "busy";
  if (s >= 400 && s < 500) return "rejected";
  return "unavailable";
}

/** The read routes analytics may name (Hono route patterns, never a concrete path); anything else is "other". */
export const READ_ROUTES: ReadonlySet<string> = new Set([
  "/v1/events",
  "/v1/sessions",
  "/v1/sessions/activity",
  "/v1/sessions/output",
  "/v1/sessions/titles",
  "/v1/sessions/:sessionId/events",
  "/v1/sessions/:sessionId/timing",
  "/v1/sessions/:sessionId/turns",
  "/v1/sessions/:sessionId/metadata",
  "/v1/sessions/:sessionId/summary",
  "/v1/sessions/:sessionId/catalog",
  "/v1/digest/daily",
  "/v1/delivery/records",
]);

export type ReadOutcome = "served" | "invalid" | "unauthorized" | "forbidden" | "not_found" | "busy" | "unsupported" | "unavailable" | "other";

export function readOutcome(status: number): ReadOutcome {
  if (status >= 200 && status < 300) return "served";
  if (status === 400) return "invalid";
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 429) return "busy";
  if (status === 501) return "unsupported";
  if (status >= 500 && status < 600) return "unavailable";
  return "other";
}

/**
 * The typed domain events. `traceId` is the request's correlation id; `orgId` and
 * `workspaceId` are the VERIFIED identity's (never a request field). Workspace events
 * use the "workspace in a tenant model" scope: both ids are required, or the event is
 * not exported. The index tick is installation-only: it carries neither.
 */
export type AnalyticsEvent =
  | {
      kind: "session_ingest.batch_completed";
      traceId?: string;
      orgId: string;
      workspaceId: string;
      route: "delivery" | "turns";
      outcome: IngestOutcome;
      records: number;
      durationMs: number;
    }
  | {
      kind: "session_read.request_completed";
      traceId?: string;
      orgId: string;
      workspaceId: string;
      route: string;
      status: number;
      durationMs: number;
    }
  | {
      kind: "session_pipeline.index_tick_completed";
      collected: number;
      failed: number;
      requeued: number;
      stale: number;
      tombstoned: number;
      complete: boolean;
      durationMs: number;
    };

export type AnalyticsEventKind = AnalyticsEvent["kind"];

/** Registered scope of each kind (collector interface table): fixed here, never chosen by a caller. */
export const EVENT_SCOPE: Readonly<Record<AnalyticsEventKind, "workspace" | "installation">> = {
  "session_ingest.batch_completed": "workspace",
  "session_read.request_completed": "workspace",
  "session_pipeline.index_tick_completed": "installation",
};

/** Which component may emit which kind; a sink bound to one component refuses the others. */
export const EVENT_COMPONENT: Readonly<Record<AnalyticsEventKind, AnalyticsComponent>> = {
  "session_ingest.batch_completed": "session-ingest",
  "session_read.request_completed": "session-read-api",
  "session_pipeline.index_tick_completed": "session-pipeline-consumer",
};

/** Property values: closed strings, clamped integers, booleans. */
export type PropertyValue = string | number | boolean;

/** The versioned export envelope (collector interface "Internal batch endpoint"). */
export interface AnalyticsEnvelope {
  version: typeof ANALYTICS_ENVELOPE_VERSION;
  event_id: string;
  occurred_at: string;
  component: AnalyticsComponent;
  deployment_id: string;
  event: AnalyticsEventKind;
  trace_id: string;
  tenant_id?: string;
  workspace_id?: string;
  properties: Record<string, PropertyValue>;
}

/** The analytics port business logic depends on. `capture` never throws and never awaits export. */
export interface AnalyticsPort {
  capture(event: AnalyticsEvent): void;
}

/** Producer identity, bound by configuration (the future producer registration), never by an event. */
export interface ProducerIdentity {
  component: AnalyticsComponent;
  deploymentId: string;
}

/** Opaque ids: a fixed alphabet and length, so no free text, path, email or URL can pass. */
const OPAQUE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
/** The read API's own correlation-id rule (app.ts), the strictest in the repo. */
const TRACE_ID = /^[A-Za-z0-9._:-]{1,128}$/;
const MAX_COUNT = 1_000_000;

export function opaqueId(value: unknown): string | undefined {
  return typeof value === "string" && OPAQUE_ID.test(value) ? value : undefined;
}

function count(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.min(Math.floor(value), MAX_COUNT) : 0;
}

function oneOf<T extends string>(value: unknown, allowed: ReadonlySet<string>, fallback: T): T {
  return typeof value === "string" && allowed.has(value) ? (value as T) : fallback;
}

/** Why an event was not projected (counted, never exported). */
export type ProjectionRefusal = "unknown_kind" | "wrong_component" | "missing_attribution";

export interface ProjectorClock {
  now(): Date;
  newId(): string;
}

const DEFAULT_CLOCK: ProjectorClock = { now: () => new Date(), newId: () => crypto.randomUUID() };

/**
 * The event as an envelope, or the refusal. A fresh random event id and the UTC
 * occurrence time are fixed here, once, and travel with the queued event across
 * retries. Properties are built from named fields only.
 */
export function projectEvent(event: AnalyticsEvent, producer: ProducerIdentity, clock: ProjectorClock = DEFAULT_CLOCK): AnalyticsEnvelope | ProjectionRefusal {
  const kind = (event as { kind?: unknown })?.kind;
  if (typeof kind !== "string" || !Object.hasOwn(EVENT_SCOPE, kind)) return "unknown_kind";
  const k = kind as AnalyticsEventKind;
  if (EVENT_COMPONENT[k] !== producer.component) return "wrong_component";
  let tenant: string | undefined;
  let workspace: string | undefined;
  if (EVENT_SCOPE[k] === "workspace") {
    const e = event as { orgId?: unknown; workspaceId?: unknown };
    tenant = opaqueId(e.orgId);
    workspace = opaqueId(e.workspaceId);
    // Never invent attribution or weaken the scope: an event without both ids is not exported.
    if (!tenant || !workspace) return "missing_attribution";
  }
  const rawTrace = (event as { traceId?: unknown }).traceId;
  const trace = typeof rawTrace === "string" && TRACE_ID.test(rawTrace) ? rawTrace : clock.newId();
  return {
    version: ANALYTICS_ENVELOPE_VERSION,
    event_id: clock.newId(),
    occurred_at: clock.now().toISOString(),
    component: producer.component,
    deployment_id: producer.deploymentId,
    event: k,
    trace_id: trace,
    ...(tenant ? { tenant_id: tenant } : {}),
    ...(workspace ? { workspace_id: workspace } : {}),
    properties: projectProperties(event, k),
  };
}

/** The per-kind property allowlist. Adding a field here is a registry (privacy) change. */
function projectProperties(event: AnalyticsEvent, kind: AnalyticsEventKind): Record<string, PropertyValue> {
  const e = event as Record<string, unknown>;
  switch (kind) {
    case "session_ingest.batch_completed":
      return {
        route: e.route === "turns" ? "turns" : "delivery",
        outcome: oneOf<IngestOutcome>(e.outcome, INGEST_OUTCOMES, "unavailable"),
        record_count: count(e.records),
        latency_bucket: latencyBucket(e.durationMs),
      };
    case "session_read.request_completed": {
      const status = typeof e.status === "number" && Number.isInteger(e.status) ? e.status : 0;
      return {
        route: oneOf(e.route, READ_ROUTES, "other"),
        outcome: readOutcome(status),
        status_class: status >= 100 && status < 600 ? `${Math.floor(status / 100)}xx` : "other",
        latency_bucket: latencyBucket(e.durationMs),
      };
    }
    case "session_pipeline.index_tick_completed":
      return {
        collected: count(e.collected),
        failed: count(e.failed),
        requeued: count(e.requeued),
        stale: count(e.stale),
        tombstoned: count(e.tombstoned),
        complete: e.complete === true,
        latency_bucket: latencyBucket(e.durationMs),
      };
  }
}

/** Producer-side counters: visible failure and drop accounting (never fed back as analytics). */
export interface AnalyticsCounters {
  captured: number;
  enqueued: number;
  accepted: number;
  dropped_invalid: number;
  dropped_oversize: number;
  dropped_queue_full: number;
  dropped_rejected: number;
  dropped_auth: number;
  dropped_exhausted: number;
  dropped_shutdown: number;
  export_requests: number;
  export_failures: number;
}

export function zeroCounters(): AnalyticsCounters {
  return {
    captured: 0, enqueued: 0, accepted: 0,
    dropped_invalid: 0, dropped_oversize: 0, dropped_queue_full: 0, dropped_rejected: 0, dropped_auth: 0, dropped_exhausted: 0, dropped_shutdown: 0,
    export_requests: 0, export_failures: 0,
  };
}

/** A port with lifecycle: flush bounded by a deadline (tests, Worker waitUntil), shutdown, counters. */
export interface AnalyticsSink extends AnalyticsPort {
  readonly destination: "noop" | "collector" | "posthog";
  /** Exports what is queued now, within `deadlineMs`; never rejects. */
  flush(deadlineMs?: number): Promise<void>;
  /** Stops the timer, flushes within the shutdown deadline, drops (and counts) the rest. Never rejects. */
  shutdown(deadlineMs?: number): Promise<void>;
  stats(): Readonly<AnalyticsCounters>;
}

/** The default destination: nothing leaves, nothing is held. Satisfies tests, not analytics acceptance. */
export class NoopSink implements AnalyticsSink {
  readonly destination = "noop" as const;
  private readonly counters = zeroCounters();
  capture(_event: AnalyticsEvent): void {
    this.counters.captured++;
  }
  flush(): Promise<void> {
    return Promise.resolve();
  }
  shutdown(): Promise<void> {
    return Promise.resolve();
  }
  stats(): Readonly<AnalyticsCounters> {
    return { ...this.counters };
  }
}

export const NOOP_ANALYTICS: AnalyticsSink = new NoopSink();

/** Calls `port.capture`, swallowing anything a misbehaving port throws: analytics never fails the caller. */
export function captureSafely(port: AnalyticsPort | undefined, event: AnalyticsEvent): void {
  if (!port) return;
  try {
    port.capture(event);
  } catch {
    // Never surfaces: the product operation's answer does not depend on analytics.
  }
}
