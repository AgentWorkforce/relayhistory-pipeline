/**
 * Bounded asynchronous export for the analytics port (analytics.ts), and its two
 * destinations: the self-host collector (POST /v1/events, helm-charts
 * docs/analytics-collector-interface.md) and the hosted PostHog batch capture API.
 *
 * Bounds (every one finite; the defaults are below):
 *   - queue: at most maxQueueEvents events and maxQueueBytes serialized bytes. A full
 *     queue drops the NEWEST event and counts dropped_queue_full; memory never grows
 *     with an unavailable destination. (A batch in flight is outside the queue; one whose
 *     drain budget ran out before its first attempt is put back at the head, so the
 *     total held is at most the queue bound plus one batch.)
 *   - an event over maxEventBytes is dropped (dropped_oversize) at capture.
 *   - a batch: at most maxBatchEvents events and maxBatchBytes bytes.
 *   - delivery: each event's total deadline (deliveryDeadlineMs) starts at enqueue and
 *     covers queued time, every attempt and every wait. A batch uses its earliest
 *     event's deadline; retries and 413 splits never reset it. Each attempt's timeout is
 *     clipped to what remains. At most maxAttempts attempts per batch, and at most
 *     maxRequestsPerDrain requests per drain (the Worker's subrequest budget).
 *   - Retry-After: unsigned delta-seconds only, capped at retryAfterMaxMs and the
 *     remaining deadline; a date or malformed value falls back to local backoff.
 *   - shutdown: flush within shutdownMs, then abort and drop (counted) the rest.
 *
 * Failure isolation: `capture` never throws and never awaits; a transport that
 * throws, hangs or answers garbage only moves counters. Counters are reported to an
 * operator hook (a log line), throttled, and never emitted back into analytics.
 * Runtime-neutral: fetch, AbortController, timers and TextEncoder only.
 */
import {
  projectEvent,
  zeroCounters,
  type AnalyticsCounters,
  type AnalyticsEnvelope,
  type AnalyticsEvent,
  type AnalyticsSink,
  type ProducerIdentity,
  type ProjectorClock,
} from "./analytics.ts";

export const COLLECTOR_BATCH_SCHEMA = "agent-relay.analytics.batch/v1";

export interface AnalyticsLimits {
  maxQueueEvents: number;
  maxQueueBytes: number;
  maxEventBytes: number;
  maxBatchEvents: number;
  maxBatchBytes: number;
  /** Timer flush cadence; 0 = no timer, export only on flush() (the Worker: waitUntil). */
  flushIntervalMs: number;
  /** flush()'s default deadline. */
  flushDeadlineMs: number;
  attemptTimeoutMs: number;
  deliveryDeadlineMs: number;
  maxAttempts: number;
  maxRequestsPerDrain: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  retryAfterMaxMs: number;
  shutdownMs: number;
  /** The least time between two operator reports of failures/drops. */
  reportIntervalMs: number;
}

/** Long-lived Node services (self-host ingest, read API, consumer). */
export const SERVICE_LIMITS: Readonly<AnalyticsLimits> = Object.freeze({
  maxQueueEvents: 1_000,
  maxQueueBytes: 1024 * 1024,
  maxEventBytes: 8 * 1024,
  maxBatchEvents: 100,
  maxBatchBytes: 256 * 1024,
  flushIntervalMs: 5_000,
  flushDeadlineMs: 5_000,
  attemptTimeoutMs: 5_000,
  deliveryDeadlineMs: 60_000,
  maxAttempts: 5,
  maxRequestsPerDrain: 50,
  backoffBaseMs: 250,
  backoffMaxMs: 10_000,
  retryAfterMaxMs: 30_000,
  shutdownMs: 5_000,
  reportIntervalMs: 60_000,
});

/**
 * One Worker invocation: no timer, one flush in waitUntil after the response, at most
 * two requests (the subrequests the ingest Worker reserves for it), all inside 5 s.
 */
export const WORKER_LIMITS: Readonly<AnalyticsLimits> = Object.freeze({
  ...SERVICE_LIMITS,
  maxQueueEvents: 50,
  maxQueueBytes: 128 * 1024,
  flushIntervalMs: 0,
  flushDeadlineMs: 5_000,
  attemptTimeoutMs: 2_000,
  deliveryDeadlineMs: 5_000,
  maxAttempts: 2,
  maxRequestsPerDrain: 2,
  backoffMaxMs: 1_000,
  retryAfterMaxMs: 1_000,
  shutdownMs: 5_000,
});

/** What one request attempt told us. */
export type SendOutcome =
  | { kind: "accepted" }
  | { kind: "retry"; retryAfterMs?: number }
  | { kind: "rejected" }
  | { kind: "auth" }
  | { kind: "too_large" };

export interface AnalyticsTransport {
  readonly destination: "collector" | "posthog";
  /** Never rejects: a network error, a timeout or an abort is `retry`. */
  send(events: readonly AnalyticsEnvelope[], signal: AbortSignal): Promise<SendOutcome>;
}

export type Fetcher = (input: string, init: RequestInit) => Promise<Response>;

/** `Retry-After` as unsigned delta-seconds only; an HTTP-date or anything malformed is undefined. */
export function retryAfterMs(header: string | null): number | undefined {
  const v = header?.trim();
  if (!v || !/^\d{1,9}$/.test(v)) return undefined;
  return Number(v) * 1000;
}

async function discard(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Nothing to release.
  }
}

/** `redirect: "manual"` answers: a 3xx (Node, Bun, workerd) or an opaque redirect (status 0, browsers). */
const isRedirect = (r: Response) => (r as { type?: string }).type === "opaqueredirect" || r.status === 0 || (r.status >= 300 && r.status < 400);

/**
 * The self-host collector: `POST <ANALYTICS_COLLECTOR_URL>` (the full URL, path
 * included), exactly one `Authorization: Bearer <producer token>`, body
 * `{ schema, events }`. Only 204 acknowledges; other 2xx are protocol errors (retried,
 * then lost); redirects are never followed (the token is never forwarded).
 */
export function collectorTransport(options: { url: string; token: string; fetch?: Fetcher }): AnalyticsTransport {
  const doFetch: Fetcher = options.fetch ?? ((input, init) => fetch(input, init));
  return {
    destination: "collector",
    async send(events, signal) {
      let response: Response;
      try {
        response = await doFetch(options.url, {
          method: "POST",
          headers: { "content-type": "application/json", authorization: `Bearer ${options.token}` },
          body: JSON.stringify({ schema: COLLECTOR_BATCH_SCHEMA, events }),
          redirect: "manual",
          signal,
        });
      } catch {
        return { kind: "retry" };
      }
      const status = response.status;
      const after = retryAfterMs(response.headers.get("retry-after"));
      await discard(response);
      if (status === 204) return { kind: "accepted" };
      if (isRedirect(response)) return { kind: "rejected" };
      if (status >= 200 && status < 300) return { kind: "retry" };
      if (status === 401 || status === 403) return { kind: "auth" };
      if (status === 413) return { kind: "too_large" };
      if (status === 429 || status === 503 || status === 507) return after === undefined ? { kind: "retry" } : { kind: "retry", retryAfterMs: after };
      if (status >= 500) return { kind: "retry" };
      return { kind: "rejected" };
    },
  };
}

/** One envelope as a PostHog capture event; reserved envelope fields are set after (and so win over) properties. */
export function postHogEvent(e: AnalyticsEnvelope): Record<string, unknown> {
  return {
    event: e.event,
    uuid: e.event_id,
    timestamp: e.occurred_at,
    distinct_id: e.workspace_id ?? `deployment:${e.deployment_id}`,
    properties: {
      ...e.properties,
      envelope_version: e.version,
      component: e.component,
      deployment_id: e.deployment_id,
      trace_id: e.trace_id,
      ...(e.tenant_id ? { tenant_id: e.tenant_id } : {}),
      ...(e.workspace_id ? { workspace_id: e.workspace_id } : {}),
      // Analytics about workspaces, not people: never create a person profile.
      $process_person_profile: false,
      $lib: "sessions-pipeline",
    },
  };
}

/**
 * The hosted PostHog batch capture API (`POST <host>/batch/`). The project key is
 * PostHog's body field `api_key`; it is read from configuration only and never logged.
 */
export function postHogTransport(options: { host: string; apiKey: string; fetch?: Fetcher }): AnalyticsTransport {
  const doFetch: Fetcher = options.fetch ?? ((input, init) => fetch(input, init));
  const url = `${options.host.replace(/\/+$/, "")}/batch/`;
  return {
    destination: "posthog",
    async send(events, signal) {
      let response: Response;
      try {
        response = await doFetch(url, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ api_key: options.apiKey, batch: events.map(postHogEvent) }),
          redirect: "manual",
          signal,
        });
      } catch {
        return { kind: "retry" };
      }
      const status = response.status;
      const after = retryAfterMs(response.headers.get("retry-after"));
      await discard(response);
      if (isRedirect(response)) return { kind: "rejected" };
      if (status >= 200 && status < 300) return { kind: "accepted" };
      if (status === 401 || status === 403) return { kind: "auth" };
      if (status === 413) return { kind: "too_large" };
      if (status === 429 || status >= 500) return after === undefined ? { kind: "retry" } : { kind: "retry", retryAfterMs: after };
      return { kind: "rejected" };
    },
  };
}

interface Queued {
  envelope: AnalyticsEnvelope;
  bytes: number;
  /** Epoch ms: the event's total delivery deadline, fixed at enqueue. */
  deadlineAt: number;
}

export interface ExporterOptions {
  limits?: Partial<AnalyticsLimits>;
  clock?: ProjectorClock;
  nowMs?: () => number;
  /** Waits `ms`, resolving early (never rejecting) when `signal` aborts. */
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  /** Operator visibility: called (throttled) after failures/drops with the counters. Never analytics. */
  report?: (counters: Readonly<AnalyticsCounters>) => void;
  random?: () => number;
}

function defaultSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted || ms <= 0) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });
}

const encoder = new TextEncoder();

type Delivery = "done" | "deferred";

/** The batching sink behind CollectorSink and PostHogSink. */
export class BatchingSink implements AnalyticsSink {
  readonly destination: "collector" | "posthog";
  private readonly limits: AnalyticsLimits;
  private readonly counters = zeroCounters();
  private readonly queue: Queued[] = [];
  private queueBytes = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private running: Promise<void> | undefined;
  private closed = false;
  private readonly stop = new AbortController();
  private readonly nowMs: () => number;
  private readonly sleep: (ms: number, signal: AbortSignal) => Promise<void>;
  private readonly random: () => number;
  private lastReport = Number.NEGATIVE_INFINITY;
  private unreported = false;

  constructor(
    private readonly producer: ProducerIdentity,
    private readonly transport: AnalyticsTransport,
    private readonly options: ExporterOptions = {},
  ) {
    this.destination = transport.destination;
    this.limits = { ...SERVICE_LIMITS, ...options.limits };
    this.nowMs = options.nowMs ?? Date.now;
    this.sleep = options.sleep ?? defaultSleep;
    this.random = options.random ?? Math.random;
  }

  capture(event: AnalyticsEvent): void {
    try {
      this.counters.captured++;
      if (this.closed) return this.drop("dropped_shutdown", 1);
      const projected = projectEvent(event, this.producer, this.options.clock);
      if (typeof projected === "string") return this.drop("dropped_invalid", 1);
      const bytes = encoder.encode(JSON.stringify(projected)).byteLength;
      if (bytes > this.limits.maxEventBytes) return this.drop("dropped_oversize", 1);
      // Queue full: drop the newest (this one), never an unbounded backlog.
      if (this.queue.length >= this.limits.maxQueueEvents || this.queueBytes + bytes > this.limits.maxQueueBytes) return this.drop("dropped_queue_full", 1);
      this.queue.push({ envelope: projected, bytes, deadlineAt: this.nowMs() + this.limits.deliveryDeadlineMs });
      this.queueBytes += bytes;
      this.counters.enqueued++;
      this.schedule();
    } catch {
      // A capture never fails its caller.
    }
  }

  stats(): Readonly<AnalyticsCounters> {
    return { ...this.counters };
  }

  /** Events waiting (not in flight). Tests and health only. */
  queued(): number {
    return this.queue.length;
  }

  async flush(deadlineMs: number = this.limits.flushDeadlineMs): Promise<void> {
    const deadlineAt = this.nowMs() + Math.max(0, deadlineMs);
    try {
      for (;;) {
        if (this.running) {
          await this.until(this.running, deadlineAt);
          if (this.nowMs() >= deadlineAt || this.stop.signal.aborted) return;
          continue;
        }
        if (this.queue.length === 0 || this.nowMs() >= deadlineAt) return;
        const before = { queued: this.queue.length, requests: this.counters.export_requests };
        await this.drain(deadlineAt);
        // A drain that sent nothing and moved nothing (its request budget is spent) ends this flush.
        if (this.counters.export_requests === before.requests && this.queue.length >= before.queued) return;
      }
    } catch {
      // flush never rejects.
    }
  }

  async shutdown(deadlineMs: number = this.limits.shutdownMs): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    await this.flush(deadlineMs);
    // Past the deadline: abort any attempt or wait in flight, then count what is left.
    this.stop.abort();
    try {
      await this.running;
    } catch {
      // drain never rejects; belt and braces.
    }
    if (this.queue.length > 0) this.drop("dropped_shutdown", this.queue.length);
    this.queue.length = 0;
    this.queueBytes = 0;
    this.report(true);
  }

  /** Awaits `work` or the deadline, whichever is first; the deadline's timer never outlives the call. */
  private async until(work: Promise<void>, deadlineAt: number): Promise<void> {
    const local = new AbortController();
    const onStop = () => local.abort();
    this.stop.signal.addEventListener("abort", onStop, { once: true });
    try {
      await Promise.race([work, this.sleep(deadlineAt - this.nowMs(), local.signal)]);
    } finally {
      local.abort();
      this.stop.signal.removeEventListener("abort", onStop);
    }
  }

  private schedule(): void {
    // Worker mode (no interval): export only on flush(), inside waitUntil.
    if (this.limits.flushIntervalMs <= 0 || this.closed) return;
    if (this.queue.length >= this.limits.maxBatchEvents) {
      if (!this.running) void this.drain(this.nowMs() + this.limits.deliveryDeadlineMs);
      return;
    }
    if (this.timer !== undefined) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      if (!this.running && !this.closed) void this.drain(this.nowMs() + this.limits.deliveryDeadlineMs);
    }, this.limits.flushIntervalMs);
    // A pending flush never keeps a Node process alive (no-op where timers are plain numbers).
    (this.timer as unknown as { unref?: () => void }).unref?.();
  }

  /** One serialized export loop, bounded by `deadlineAt` and maxRequestsPerDrain. Never rejects. */
  private drain(deadlineAt: number): Promise<void> {
    if (this.running) return this.running;
    const budget = { requests: this.limits.maxRequestsPerDrain, deadlineAt };
    // Deferred a microtask, so `running` is set before the loop can finish and clear it.
    const run = Promise.resolve().then(async () => {
      try {
        while (this.queue.length > 0 && budget.requests > 0 && this.nowMs() < budget.deadlineAt && !this.stop.signal.aborted) {
          const batch = this.take();
          if (batch.length === 0) continue;
          const result = await this.deliver(batch, Math.min(...batch.map((q) => q.deadlineAt)), budget, 0);
          if (result === "deferred") break;
        }
      } catch {
        // Unreachable by construction; an export loop never surfaces.
      } finally {
        this.running = undefined;
        this.report(false);
        // Events captured while this drain ran get their own timer.
        if (this.queue.length > 0) this.schedule();
      }
    });
    this.running = run;
    return run;
  }

  /** The next batch off the head of the queue; events already past their deadline are dropped. */
  private take(): Queued[] {
    const now = this.nowMs();
    const batch: Queued[] = [];
    let bytes = 0;
    while (this.queue.length > 0 && batch.length < this.limits.maxBatchEvents) {
      const head = this.queue[0]!;
      if (head.deadlineAt <= now) {
        this.queue.shift();
        this.queueBytes -= head.bytes;
        this.drop("dropped_exhausted", 1);
        continue;
      }
      if (batch.length > 0 && bytes + head.bytes > this.limits.maxBatchBytes) break;
      this.queue.shift();
      this.queueBytes -= head.bytes;
      batch.push(head);
      bytes += head.bytes;
    }
    return batch;
  }

  /** Puts an undelivered batch back at the head (it keeps its deadlines). */
  private requeue(batch: Queued[]): void {
    this.queue.unshift(...batch);
    for (const q of batch) this.queueBytes += q.bytes;
  }

  private async deliver(batch: Queued[], deadlineAt: number, budget: { requests: number; deadlineAt: number }, depth: number): Promise<Delivery> {
    for (let attempt = 1; ; attempt++) {
      const now = this.nowMs();
      if (deadlineAt - now <= 0) return this.lose(batch, "dropped_exhausted");
      // This drain's own budget (a Worker flush's deadline or request cap) is spent, but the events'
      // deadline is not: keep them for the next drain rather than drop them.
      if (budget.requests <= 0 || budget.deadlineAt - now <= 0 || this.stop.signal.aborted) {
        if (attempt === 1 && !this.stop.signal.aborted) {
          this.requeue(batch);
          return "deferred";
        }
        return this.lose(batch, "dropped_exhausted");
      }
      budget.requests--;
      this.counters.export_requests++;
      const timeout = Math.max(1, Math.min(this.limits.attemptTimeoutMs, deadlineAt - now, budget.deadlineAt - now));
      const outcome = await this.attempt(batch, timeout);
      if (outcome.kind === "accepted") {
        this.counters.accepted += batch.length;
        return "done";
      }
      this.counters.export_failures++;
      this.unreported = true;
      if (outcome.kind === "rejected") return this.lose(batch, "dropped_rejected");
      if (outcome.kind === "auth") return this.lose(batch, "dropped_auth");
      if (outcome.kind === "too_large") {
        // Bounded splitting: halves, each with the same deadline; a lone event that is still too large is lost.
        if (batch.length === 1 || depth >= 16) return this.lose(batch, "dropped_oversize");
        const mid = Math.ceil(batch.length / 2);
        const first = await this.deliver(batch.slice(0, mid), deadlineAt, budget, depth + 1);
        if (first === "deferred") {
          this.requeue(batch.slice(mid));
          return "deferred";
        }
        return this.deliver(batch.slice(mid), deadlineAt, budget, depth + 1);
      }
      // Retryable.
      if (attempt >= this.limits.maxAttempts) return this.lose(batch, "dropped_exhausted");
      const backoff = Math.min(this.limits.backoffMaxMs, this.limits.backoffBaseMs * 2 ** (attempt - 1)) * (0.5 + this.random() / 2);
      const wait = outcome.retryAfterMs !== undefined ? Math.min(outcome.retryAfterMs, this.limits.retryAfterMaxMs) : backoff;
      // Waiting consumes the same budget; a wait that would end past the deadline is not started.
      if (this.nowMs() + wait >= deadlineAt) return this.lose(batch, "dropped_exhausted");
      await this.sleep(wait, this.stop.signal);
    }
  }

  private async attempt(batch: Queued[], timeoutMs: number): Promise<SendOutcome> {
    const controller = new AbortController();
    const onStop = () => controller.abort();
    this.stop.signal.addEventListener("abort", onStop, { once: true });
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await this.transport.send(batch.map((q) => q.envelope), controller.signal);
    } catch {
      return { kind: "retry" };
    } finally {
      clearTimeout(timer);
      this.stop.signal.removeEventListener("abort", onStop);
    }
  }

  private lose(batch: Queued[], counter: keyof AnalyticsCounters): Delivery {
    this.drop(counter, batch.length);
    return "done";
  }

  private drop(counter: keyof AnalyticsCounters, n: number): void {
    this.counters[counter] += n;
    this.unreported = true;
  }

  private report(final: boolean): void {
    if (!this.options.report || !this.unreported) return;
    const now = this.nowMs();
    if (!final && now - this.lastReport < this.limits.reportIntervalMs) return;
    this.lastReport = now;
    this.unreported = false;
    try {
      this.options.report(this.stats());
    } catch {
      // A failing reporter is not the caller's problem either.
    }
  }
}

/** The self-host collector sink. */
export function collectorSink(producer: ProducerIdentity, options: { url: string; token: string; fetch?: Fetcher } & ExporterOptions): BatchingSink {
  return new BatchingSink(producer, collectorTransport(options), options);
}

/** The hosted PostHog sink. */
export function postHogSink(producer: ProducerIdentity, options: { host: string; apiKey: string; fetch?: Fetcher } & ExporterOptions): BatchingSink {
  return new BatchingSink(producer, postHogTransport(options), options);
}
