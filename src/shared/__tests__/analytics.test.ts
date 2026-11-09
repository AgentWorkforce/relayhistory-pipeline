import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  NOOP_ANALYTICS,
  captureSafely,
  ingestOutcome,
  latencyBucket,
  projectEvent,
  readOutcome,
  type AnalyticsEnvelope,
  type AnalyticsEvent,
  type ProducerIdentity,
} from "../analytics.ts";
import { BatchingSink, COLLECTOR_BATCH_SCHEMA, WORKER_LIMITS, collectorSink, postHogSink, retryAfterMs, type AnalyticsTransport, type SendOutcome } from "../analytics-export.ts";
import { AnalyticsConfigError, analyticsConfig, createAnalyticsSink, DEFAULT_POSTHOG_HOST } from "../analytics-config.ts";
import { logLine } from "../log.ts";

const READER: ProducerIdentity = { component: "session-read-api", deploymentId: "install-1" };
const INGEST: ProducerIdentity = { component: "session-ingest", deploymentId: "install-1" };

const readEvent = (over: Partial<Extract<AnalyticsEvent, { kind: "session_read.request_completed" }>> = {}): AnalyticsEvent => ({
  kind: "session_read.request_completed", traceId: "corr-1", orgId: "org_1", workspaceId: "ws_1", route: "/v1/sessions/:sessionId/events", status: 200, durationMs: 42, ...over,
});

/** A fake clock whose sleep advances time, so retry and deadline paths run instantly. */
function clock(start = 1_000_000) {
  let t = start;
  const sleeps: number[] = [];
  return {
    nowMs: () => t,
    advance: (ms: number) => { t += ms; },
    sleeps,
    sleep: async (ms: number) => { sleeps.push(ms); t += Math.max(0, ms); },
  };
}

/** A scripted transport: answers in order (the last repeats); records every batch. */
function scripted(...answers: (SendOutcome | "throw")[]) {
  const batches: AnalyticsEnvelope[][] = [];
  const transport: AnalyticsTransport = {
    destination: "collector",
    async send(events) {
      batches.push([...events]);
      const a = answers[Math.min(batches.length - 1, answers.length - 1)]!;
      if (a === "throw") throw new Error("boom");
      return a;
    },
  };
  return { transport, batches };
}

const fixedClock = { now: () => new Date("2026-11-09T12:00:00.000Z"), newId: () => "00000000-0000-4000-8000-000000000001" };

describe("envelope projector", () => {
  test("a forbidden field never escapes, whatever the caller puts on the event", () => {
    const planted = {
      content: "PLANTED-CONTENT", prompt: "PLANTED-PROMPT", token: "sh_at_PLANTEDTOKEN", authorization: "Bearer PLANTED", sessionId: "sess-PLANTED",
      userId: "user-PLANTED", path: "/v1/sessions/sess-PLANTED/events", error: new Error("PLANTED-ERROR"), credential_hash: "PLANTED-HASH",
    };
    const projected = projectEvent({ ...readEvent(), ...planted } as AnalyticsEvent, READER, fixedClock);
    if (typeof projected === "string") throw new Error(projected);
    const text = JSON.stringify(projected);
    expect(text).not.toContain("PLANTED");
    expect(Object.keys(projected).sort()).toEqual(["component", "deployment_id", "event", "event_id", "occurred_at", "properties", "tenant_id", "trace_id", "version", "workspace_id"]);
    expect(projected.properties).toEqual({ route: "/v1/sessions/:sessionId/events", outcome: "served", status_class: "2xx", latency_bucket: "lt_100ms" });
    expect(projected).toMatchObject({ version: 1, event_id: fixedClock.newId(), occurred_at: "2026-11-09T12:00:00.000Z", component: "session-read-api", deployment_id: "install-1", event: "session_read.request_completed", trace_id: "corr-1", tenant_id: "org_1", workspace_id: "ws_1" });
  });

  test("ingest properties are counts and closed classes only", () => {
    const projected = projectEvent({
      kind: "session_ingest.batch_completed", traceId: "c", orgId: "org_1", workspaceId: "ws_1", route: "turns", outcome: "accepted", records: 12, durationMs: 300,
      batchId: "PLANTED-BATCH", records_body: ["PLANTED"],
    } as AnalyticsEvent, INGEST, fixedClock);
    if (typeof projected === "string") throw new Error(projected);
    expect(projected.properties).toEqual({ route: "turns", outcome: "accepted", record_count: 12, latency_bucket: "lt_500ms" });
    expect(JSON.stringify(projected)).not.toContain("PLANTED");
  });

  test("an unregistered route, outcome or count shape becomes a fixed class, never the raw value", () => {
    const p = projectEvent(readEvent({ route: "/v1/sessions/sess-123/events", status: 999 }), READER, fixedClock) as AnalyticsEnvelope;
    expect(p.properties).toMatchObject({ route: "other", outcome: "other", status_class: "other" });
    const i = projectEvent({ kind: "session_ingest.batch_completed", orgId: "o", workspaceId: "w", route: "x" as never, outcome: "secret text" as never, records: -5, durationMs: Number.NaN }, INGEST, fixedClock) as AnalyticsEnvelope;
    expect(i.properties).toEqual({ route: "delivery", outcome: "unavailable", record_count: 0, latency_bucket: "ge_30s" });
  });

  test("workspace scope requires both verified ids, opaque-shaped; never invented", () => {
    expect(projectEvent(readEvent({ workspaceId: "" }), READER)).toBe("missing_attribution");
    expect(projectEvent(readEvent({ orgId: undefined as never }), READER)).toBe("missing_attribution");
    expect(projectEvent(readEvent({ orgId: "user@example.com" }), READER)).toBe("missing_attribution");
    expect(projectEvent(readEvent({ workspaceId: "ws 1; drop" }), READER)).toBe("missing_attribution");
  });

  test("installation scope carries neither tenant nor workspace", () => {
    const p = projectEvent({ kind: "session_pipeline.index_tick_completed", collected: 3, failed: 1, requeued: 0, stale: 2, tombstoned: 0, complete: true, durationMs: 1200, orgId: "org_PLANTED" } as AnalyticsEvent,
      { component: "session-pipeline-consumer", deploymentId: "d" }, fixedClock) as AnalyticsEnvelope;
    expect(p.tenant_id).toBeUndefined();
    expect(p.workspace_id).toBeUndefined();
    expect(p.properties).toEqual({ collected: 3, failed: 1, requeued: 0, stale: 2, tombstoned: 0, complete: true, latency_bucket: "lt_2500ms" });
  });

  test("component is bound by the producer; an unknown kind or another component's kind is refused", () => {
    expect(projectEvent(readEvent(), INGEST)).toBe("wrong_component");
    expect(projectEvent({ kind: "session_read.anything" } as never, READER)).toBe("unknown_kind");
    expect(projectEvent({ kind: "__proto__" } as never, READER)).toBe("unknown_kind");
  });

  test("a trace id outside the opaque alphabet is replaced, never exported", () => {
    const p = projectEvent(readEvent({ traceId: "hello world <script>" }), READER) as AnalyticsEnvelope;
    expect(p.trace_id).toMatch(/^[0-9a-f-]{36}$/);
    const ids = new Set([p.event_id, (projectEvent(readEvent(), READER) as AnalyticsEnvelope).event_id]);
    expect(ids.size).toBe(2);
  });

  test("outcome classes", () => {
    expect(["accepted", "accepted_staged", "accepted_queued", "accepted_buffered", "accepted_all_deleted"].map((o) => ingestOutcome({ outcome: o }))).toEqual(Array(5).fill("accepted"));
    expect(["replay", "replay_fast", "legacy_replay"].map((o) => ingestOutcome({ outcome: o }))).toEqual(["replay", "replay", "replay"]);
    expect([400, 401, 403, 409, 413, 415, 422, 429, 503, 500].map((status) => ingestOutcome({ status }))).toEqual(
      ["rejected", "unauthorized", "unauthorized", "conflict", "too_large", "rejected", "rejected", "busy", "unavailable", "unavailable"]);
    expect([200, 400, 401, 403, 404, 429, 501, 503, 302].map(readOutcome)).toEqual(["served", "invalid", "unauthorized", "forbidden", "not_found", "busy", "unsupported", "unavailable", "other"]);
    expect([0, 99, 100, 499, 999, 4_999, 29_999, 30_000].map(latencyBucket)).toEqual(["lt_100ms", "lt_100ms", "lt_250ms", "lt_500ms", "lt_1s", "lt_5s", "lt_30s", "ge_30s"]);
  });
});

describe("bounded queue and counters", () => {
  test("a full queue drops the newest event and counts it; memory stays bounded", async () => {
    const { transport } = scripted({ kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { limits: { maxQueueEvents: 3, flushIntervalMs: 0 } });
    for (let i = 0; i < 10; i++) sink.capture(readEvent());
    expect(sink.queued()).toBe(3);
    expect(sink.stats()).toMatchObject({ captured: 10, enqueued: 3, dropped_queue_full: 7 });
    await sink.flush();
    expect(sink.stats()).toMatchObject({ accepted: 3, export_requests: 1 });
  });

  test("the byte bound applies too, and an oversized event is dropped at capture", () => {
    const { transport } = scripted({ kind: "accepted" });
    const one = new TextEncoder().encode(JSON.stringify(projectEvent(readEvent(), READER))).byteLength;
    const sink = new BatchingSink(READER, transport, { limits: { maxQueueBytes: one * 2 + 10, flushIntervalMs: 0 } });
    for (let i = 0; i < 5; i++) sink.capture(readEvent());
    expect(sink.stats()).toMatchObject({ enqueued: 2, dropped_queue_full: 3 });
    const tiny = new BatchingSink(READER, transport, { limits: { maxEventBytes: 10, flushIntervalMs: 0 } });
    tiny.capture(readEvent());
    expect(tiny.stats()).toMatchObject({ enqueued: 0, dropped_oversize: 1 });
  });

  test("an unattributable event is counted, not queued", () => {
    const { transport } = scripted({ kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { limits: { flushIntervalMs: 0 } });
    sink.capture(readEvent({ workspaceId: "" }));
    sink.capture({ kind: "nope" } as never);
    sink.capture(null as never);
    expect(sink.stats()).toMatchObject({ captured: 3, enqueued: 0, dropped_invalid: 3 });
  });

  test("batches respect the event and byte limits", async () => {
    const { transport, batches } = scripted({ kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { limits: { maxBatchEvents: 4, flushIntervalMs: 0 } });
    for (let i = 0; i < 10; i++) sink.capture(readEvent());
    await sink.flush();
    expect(batches.map((b) => b.length)).toEqual([4, 4, 2]);
    expect(sink.stats().accepted).toBe(10);
  });

  test("the timer exports on its own in a service, and shutdown drops (and counts) what it cannot deliver", async () => {
    const { transport, batches } = scripted({ kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { limits: { flushIntervalMs: 5 } });
    sink.capture(readEvent());
    await new Promise((r) => setTimeout(r, 40));
    expect(batches.length).toBe(1);
    const stuck = new BatchingSink(READER, { destination: "collector", send: (_e, signal) => new Promise((resolve) => signal.addEventListener("abort", () => resolve({ kind: "retry" }))) }, { limits: { flushIntervalMs: 0, maxAttempts: 1 } });
    stuck.capture(readEvent());
    stuck.capture(readEvent());
    const started = Date.now();
    await stuck.shutdown(30);
    expect(Date.now() - started).toBeLessThan(2_000);
    const s = stuck.stats();
    expect(s.accepted).toBe(0);
    expect(s.dropped_exhausted + s.dropped_shutdown).toBe(2);
    stuck.capture(readEvent());
    expect(stuck.stats().dropped_shutdown).toBe(s.dropped_shutdown + 1);
  });
});

describe("a failing sink never fails the operation", () => {
  test("a throwing, hanging or failing transport only moves counters", async () => {
    const c = clock();
    const { transport } = scripted("throw");
    const sink = new BatchingSink(READER, transport, { nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, maxAttempts: 3 } });
    expect(() => sink.capture(readEvent())).not.toThrow();
    await expect(sink.flush()).resolves.toBeUndefined();
    expect(sink.stats()).toMatchObject({ accepted: 0, export_requests: 3, export_failures: 3, dropped_exhausted: 1 });
  });

  test("captureSafely swallows a port that throws", () => {
    expect(() => captureSafely({ capture: () => { throw new Error("broken port"); } }, readEvent())).not.toThrow();
    expect(() => captureSafely(undefined, readEvent())).not.toThrow();
    expect(() => NOOP_ANALYTICS.capture(readEvent())).not.toThrow();
  });

  test("a throwing reporter is contained too", async () => {
    const { transport } = scripted({ kind: "rejected" });
    const sink = new BatchingSink(READER, transport, { limits: { flushIntervalMs: 0 }, report: () => { throw new Error("log down"); } });
    sink.capture(readEvent());
    await expect(sink.flush()).resolves.toBeUndefined();
    expect(sink.stats().dropped_rejected).toBe(1);
  });

  test("reports are throttled and carry counters only", async () => {
    const c = clock();
    const reports: unknown[] = [];
    const { transport } = scripted({ kind: "rejected" });
    const sink = new BatchingSink(READER, transport, { nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, reportIntervalMs: 60_000 }, report: (r) => reports.push(r) });
    sink.capture(readEvent());
    await sink.flush();
    sink.capture(readEvent());
    await sink.flush();
    expect(reports.length).toBe(1);
    c.advance(60_001);
    sink.capture(readEvent());
    await sink.flush();
    expect(reports.length).toBe(2);
    expect(JSON.stringify(reports)).not.toContain("ws_1");
  });
});

describe("delivery: retries, deadlines and splitting", () => {
  test("retryable answers back off within maxAttempts, then succeed", async () => {
    const c = clock();
    const { transport, batches } = scripted({ kind: "retry" }, { kind: "retry" }, { kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { nowMs: c.nowMs, sleep: c.sleep, random: () => 1, limits: { flushIntervalMs: 0, backoffBaseMs: 100 } });
    sink.capture(readEvent());
    await sink.flush();
    expect(batches.length).toBe(3);
    expect(c.sleeps).toEqual([100, 200]);
    expect(batches[0]![0]!.event_id).toBe(batches[2]![0]!.event_id);
    expect(sink.stats()).toMatchObject({ accepted: 1, export_failures: 2 });
  });

  test("Retry-After is honored, capped, and never starts a wait past the deadline", async () => {
    const c = clock();
    const { transport } = scripted({ kind: "retry", retryAfterMs: 120_000 }, { kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, deliveryDeadlineMs: 60_000, retryAfterMaxMs: 30_000 } });
    sink.capture(readEvent());
    await sink.flush(120_000);
    expect(c.sleeps).toEqual([30_000]);
    expect(sink.stats().accepted).toBe(1);
    const late = clock();
    const { transport: t2 } = scripted({ kind: "retry", retryAfterMs: 20_000 });
    const s2 = new BatchingSink(READER, t2, { nowMs: late.nowMs, sleep: late.sleep, limits: { flushIntervalMs: 0, deliveryDeadlineMs: 10_000 } });
    s2.capture(readEvent());
    await s2.flush(60_000);
    expect(late.sleeps).toEqual([]);
    expect(s2.stats()).toMatchObject({ dropped_exhausted: 1, export_requests: 1 });
  });

  test("an event past its total deadline is dropped before any request", async () => {
    const c = clock();
    const { transport, batches } = scripted({ kind: "accepted" });
    const sink = new BatchingSink(READER, transport, { nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, deliveryDeadlineMs: 1_000 } });
    sink.capture(readEvent());
    c.advance(1_001);
    await sink.flush();
    expect(batches.length).toBe(0);
    expect(sink.stats().dropped_exhausted).toBe(1);
  });

  test("terminal answers are not retried", async () => {
    for (const [answer, counter] of [[{ kind: "rejected" }, "dropped_rejected"], [{ kind: "auth" }, "dropped_auth"]] as const) {
      const { transport, batches } = scripted(answer);
      const sink = new BatchingSink(READER, transport, { limits: { flushIntervalMs: 0 } });
      sink.capture(readEvent());
      await sink.flush();
      expect(batches.length).toBe(1);
      expect(sink.stats()[counter]).toBe(1);
    }
  });

  test("413 splits a batch in halves (same event ids) until it fits; a lone oversized event is dropped", async () => {
    const seen: string[][] = [];
    const transport: AnalyticsTransport = {
      destination: "collector",
      async send(events) {
        seen.push(events.map((e) => e.event_id));
        return events.length > 2 ? { kind: "too_large" } : { kind: "accepted" };
      },
    };
    const sink = new BatchingSink(READER, transport, { limits: { flushIntervalMs: 0 } });
    for (let i = 0; i < 5; i++) sink.capture(readEvent());
    await sink.flush();
    expect(seen.map((b) => b.length)).toEqual([5, 3, 2, 1, 2]);
    expect(new Set(seen.slice(1).flat())).toEqual(new Set(seen[0]));
    expect(sink.stats()).toMatchObject({ accepted: 5, dropped_oversize: 0 });
    const { transport: always } = scripted({ kind: "too_large" });
    const lone = new BatchingSink(READER, always, { limits: { flushIntervalMs: 0 } });
    lone.capture(readEvent());
    await lone.flush();
    expect(lone.stats()).toMatchObject({ export_requests: 1, dropped_oversize: 1 });
  });

  test("the Worker profile never exceeds its request budget (the ingest Worker's subrequest reservation)", async () => {
    const { transport, batches } = scripted({ kind: "retry" });
    const c = clock();
    const worker = new BatchingSink(READER, transport, { nowMs: c.nowMs, sleep: c.sleep, limits: WORKER_LIMITS });
    for (let i = 0; i < 20; i++) worker.capture(readEvent());
    await worker.shutdown();
    expect(batches.length).toBeLessThanOrEqual(WORKER_LIMITS.maxRequestsPerDrain);
    expect(worker.stats().export_requests).toBeLessThanOrEqual(WORKER_LIMITS.maxRequestsPerDrain);
    const s = worker.stats();
    expect(s.dropped_exhausted + s.dropped_shutdown).toBe(20);
  });
});

describe("collector transport", () => {
  const TOKEN = "producer-token-0123456789abcdef";
  const URL_ = "http://analytics-collector.relay.svc:8080/v1/events";
  function recording(...responses: (() => Response)[]) {
    const calls: { url: string; init: RequestInit }[] = [];
    const fetcher = async (url: string, init: RequestInit) => {
      calls.push({ url, init });
      return responses[Math.min(calls.length - 1, responses.length - 1)]!();
    };
    return { calls, fetcher };
  }

  test("POSTs the batch schema to the configured URL exactly, with one bearer and no redirect following", async () => {
    const { calls, fetcher } = recording(() => new Response(null, { status: 204 }));
    const sink = collectorSink(READER, { url: URL_, token: TOKEN, fetch: fetcher, limits: { flushIntervalMs: 0 } });
    sink.capture(readEvent());
    await sink.flush();
    expect(calls.length).toBe(1);
    expect(calls[0]!.url).toBe(URL_);
    expect(calls[0]!.init.method).toBe("POST");
    expect(calls[0]!.init.redirect).toBe("manual");
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBe(`Bearer ${TOKEN}`);
    const body = JSON.parse(String(calls[0]!.init.body));
    expect(body.schema).toBe(COLLECTOR_BATCH_SCHEMA);
    expect(body.events.length).toBe(1);
    expect(JSON.stringify(body)).not.toContain(TOKEN);
    expect(sink.stats().accepted).toBe(1);
  });

  test("only 204 acknowledges: 200/202 are protocol errors, retried then lost", async () => {
    const c = clock();
    const { calls, fetcher } = recording(() => new Response("{}", { status: 200 }), () => new Response(null, { status: 202 }));
    const sink = collectorSink(READER, { url: URL_, token: TOKEN, fetch: fetcher, nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, maxAttempts: 3 } });
    sink.capture(readEvent());
    await sink.flush();
    expect(calls.length).toBe(3);
    expect(sink.stats()).toMatchObject({ accepted: 0, dropped_exhausted: 1 });
  });

  test("status table: 400 rejected, 401/403 auth, redirect rejected, 503/507/5xx/network retried", async () => {
    const run = async (make: () => Response | Promise<Response>) => {
      const c = clock();
      let n = 0;
      const sink = collectorSink(READER, { url: URL_, token: TOKEN, fetch: async () => { n++; return make(); }, nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, maxAttempts: 2 } });
      sink.capture(readEvent());
      await sink.flush();
      return { n, s: sink.stats() };
    };
    expect(await run(() => new Response(null, { status: 400 }))).toMatchObject({ n: 1, s: { dropped_rejected: 1 } });
    expect(await run(() => new Response(null, { status: 401, headers: { "www-authenticate": "Bearer" } }))).toMatchObject({ n: 1, s: { dropped_auth: 1 } });
    expect(await run(() => new Response(null, { status: 403 }))).toMatchObject({ n: 1, s: { dropped_auth: 1 } });
    expect(await run(() => new Response(null, { status: 307, headers: { location: "https://elsewhere.example/v1/events" } }))).toMatchObject({ n: 1, s: { dropped_rejected: 1 } });
    for (const status of [429, 503, 507, 500, 502]) expect(await run(() => new Response(null, { status }))).toMatchObject({ n: 2, s: { dropped_exhausted: 1 } });
    expect(await run(() => Promise.reject(new TypeError("connection refused")))).toMatchObject({ n: 2, s: { dropped_exhausted: 1 } });
  });

  test("Retry-After: delta-seconds only; date, malformed and negative values fall back to backoff", () => {
    expect(retryAfterMs("2")).toBe(2_000);
    expect(retryAfterMs(" 0 ")).toBe(0);
    for (const bad of [null, "", "Wed, 21 Oct 2026 07:28:00 GMT", "-1", "1.5", "abc", "1e3", "9999999999"]) expect(retryAfterMs(bad)).toBeUndefined();
  });

  test("a 503 Retry-After is honored and clipped", async () => {
    const c = clock();
    const { fetcher } = recording(() => new Response(null, { status: 503, headers: { "retry-after": "3" } }), () => new Response(null, { status: 204 }));
    const sink = collectorSink(READER, { url: URL_, token: TOKEN, fetch: fetcher, nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0 } });
    sink.capture(readEvent());
    await sink.flush(60_000);
    expect(c.sleeps).toEqual([3_000]);
    expect(sink.stats().accepted).toBe(1);
  });

  test("a hung collector is cut off by the per-attempt timeout", async () => {
    const fetcher = (_url: string, init: RequestInit) => new Promise<Response>((_resolve, reject) => init.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError"))));
    const sink = collectorSink(READER, { url: URL_, token: TOKEN, fetch: fetcher, limits: { flushIntervalMs: 0, attemptTimeoutMs: 20, maxAttempts: 1 } });
    sink.capture(readEvent());
    const started = Date.now();
    await sink.flush(5_000);
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(sink.stats().dropped_exhausted).toBe(1);
  });
});

describe("posthog transport", () => {
  test("posts the batch capture body to <host>/batch/ with the project key in the body only", async () => {
    const calls: { url: string; init: RequestInit }[] = [];
    const sink = postHogSink(READER, {
      host: DEFAULT_POSTHOG_HOST, apiKey: "phc_testkey123", limits: { flushIntervalMs: 0 },
      fetch: async (url, init) => { calls.push({ url, init }); return new Response("{\"status\":1}", { status: 200 }); },
    });
    sink.capture(readEvent());
    await sink.flush();
    expect(calls[0]!.url).toBe("https://i.agentrelay.com/batch/");
    expect(calls[0]!.init.redirect).toBe("manual");
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBeUndefined();
    const body = JSON.parse(String(calls[0]!.init.body));
    expect(body.api_key).toBe("phc_testkey123");
    expect(body.batch).toHaveLength(1);
    const e = body.batch[0];
    expect(e).toMatchObject({ event: "session_read.request_completed", distinct_id: "ws_1" });
    expect(e.uuid).toMatch(/^[0-9a-f-]{36}$/);
    expect(e.properties).toMatchObject({ route: "/v1/sessions/:sessionId/events", component: "session-read-api", deployment_id: "install-1", trace_id: "corr-1", tenant_id: "org_1", workspace_id: "ws_1", $process_person_profile: false });
    expect(sink.stats().accepted).toBe(1);
  });

  test("4xx is terminal, 429/5xx retried, 413 split", async () => {
    const run = async (status: number) => {
      const c = clock();
      let n = 0;
      const sink = postHogSink(READER, { host: DEFAULT_POSTHOG_HOST, apiKey: "phc_testkey123", nowMs: c.nowMs, sleep: c.sleep, limits: { flushIntervalMs: 0, maxAttempts: 2 }, fetch: async () => { n++; return new Response(null, { status }); } });
      sink.capture(readEvent());
      await sink.flush();
      return { n, s: sink.stats() };
    };
    expect(await run(400)).toMatchObject({ n: 1, s: { dropped_rejected: 1 } });
    expect(await run(401)).toMatchObject({ n: 1, s: { dropped_auth: 1 } });
    expect(await run(429)).toMatchObject({ n: 2, s: { dropped_exhausted: 1 } });
    expect(await run(502)).toMatchObject({ n: 2, s: { dropped_exhausted: 1 } });
    expect(await run(413)).toMatchObject({ n: 1, s: { dropped_oversize: 1 } });
  });
});

describe("configuration", () => {
  const collector = { ANALYTICS_SINK: "collector", ANALYTICS_DEPLOYMENT_ID: "install-1", ANALYTICS_COLLECTOR_URL: "https://collector.example/v1/events", ANALYTICS_COLLECTOR_TOKEN: "producer-token-0123456789abcdef" };
  const posthog = { ANALYTICS_SINK: "posthog", ANALYTICS_DEPLOYMENT_ID: "hosted-prod", ANALYTICS_POSTHOG_KEY: "phc_testkey123" };
  const refused = (env: Record<string, unknown>, profile: "selfhost" | "hosted") => {
    try {
      analyticsConfig(env, profile);
    } catch (error) {
      expect(error).toBeInstanceOf(AnalyticsConfigError);
      return { variable: (error as AnalyticsConfigError).variable, message: (error as Error).message };
    }
    throw new Error("expected a refusal");
  };

  test("default off: unset, empty and noop are the NoopSink in both profiles", () => {
    for (const profile of ["selfhost", "hosted"] as const)
      for (const env of [{}, { ANALYTICS_SINK: "" }, { ANALYTICS_SINK: "noop" }, { ANALYTICS_SINK: "none" }]) {
        expect(analyticsConfig(env, profile)).toEqual({ sink: "noop" });
        expect(createAnalyticsSink(analyticsConfig(env, profile), "session-read-api", "service").destination).toBe("noop");
      }
  });

  test("a typo or other case is refused, naming the variable only", () => {
    for (const v of ["Collector", "posthog ", "file", "None", "NOOP", "true", "1"]) expect(refused({ ANALYTICS_SINK: v }, "selfhost").variable).toBe("ANALYTICS_SINK");
  });

  test("self-host never selects PostHog, and never carries a hosted key or endpoint", () => {
    expect(refused(posthog, "selfhost").variable).toBe("ANALYTICS_POSTHOG_KEY");
    expect(refused({ ANALYTICS_SINK: "posthog" }, "selfhost").variable).toBe("ANALYTICS_SINK");
    expect(refused({ ANALYTICS_POSTHOG_HOST: DEFAULT_POSTHOG_HOST }, "selfhost").variable).toBe("ANALYTICS_POSTHOG_HOST");
    expect(refused({ ...collector, ANALYTICS_POSTHOG_KEY: "phc_testkey123" }, "selfhost").variable).toBe("ANALYTICS_POSTHOG_KEY");
  });

  test("hosted never selects the collector", () => {
    expect(refused({ ANALYTICS_SINK: "collector" }, "hosted").variable).toBe("ANALYTICS_SINK");
    expect(refused({ ...posthog, ANALYTICS_COLLECTOR_URL: collector.ANALYTICS_COLLECTOR_URL }, "hosted").variable).toBe("ANALYTICS_COLLECTOR_URL");
  });

  test("collector: deployment id, full /v1/events URL under the destination policy, and a bearer token", () => {
    expect(analyticsConfig(collector, "selfhost")).toEqual({ sink: "collector", deploymentId: "install-1", url: "https://collector.example/v1/events", token: collector.ANALYTICS_COLLECTOR_TOKEN });
    expect(refused({ ...collector, ANALYTICS_DEPLOYMENT_ID: "" }, "selfhost").variable).toBe("ANALYTICS_DEPLOYMENT_ID");
    expect(refused({ ...collector, ANALYTICS_DEPLOYMENT_ID: "has space" }, "selfhost").variable).toBe("ANALYTICS_DEPLOYMENT_ID");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_URL: undefined }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_URL");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_URL: "https://collector.example/" }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_URL");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_URL: "https://collector.example/v1/events?token=x" }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_URL");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_URL: "https://u:p@collector.example/v1/events" }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_URL");
    // Plain http to an in-cluster Service only with the operator's flag.
    const inCluster = { ...collector, ANALYTICS_COLLECTOR_URL: "http://analytics-collector.relay.svc.cluster.local:8080/v1/events" };
    expect(refused(inCluster, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_URL");
    expect(analyticsConfig({ ...inCluster, ALLOW_IN_CLUSTER_HTTP: "true" }, "selfhost")).toMatchObject({ sink: "collector" });
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_URL: "http://collector.example/v1/events", ALLOW_IN_CLUSTER_HTTP: "true" }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_URL");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_TOKEN: undefined }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_TOKEN");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_TOKEN: "short" }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_TOKEN");
    expect(refused({ ...collector, ANALYTICS_COLLECTOR_TOKEN: "has a space in it 0123456789" }, "selfhost").variable).toBe("ANALYTICS_COLLECTOR_TOKEN");
  });

  test("posthog: defaults to i.agentrelay.com, https origin only, key required", () => {
    expect(analyticsConfig(posthog, "hosted")).toEqual({ sink: "posthog", deploymentId: "hosted-prod", host: "https://i.agentrelay.com", apiKey: "phc_testkey123" });
    expect(analyticsConfig({ ...posthog, ANALYTICS_POSTHOG_HOST: "https://eu.example.com" }, "hosted")).toMatchObject({ host: "https://eu.example.com" });
    for (const host of ["http://i.agentrelay.com", "https://i.agentrelay.com/batch", "https://i.agentrelay.com?x=1", "not a url"]) expect(refused({ ...posthog, ANALYTICS_POSTHOG_HOST: host }, "hosted").variable).toBe("ANALYTICS_POSTHOG_HOST");
    expect(refused({ ...posthog, ANALYTICS_POSTHOG_KEY: undefined }, "hosted").variable).toBe("ANALYTICS_POSTHOG_KEY");
    expect(refused({ ...posthog, ANALYTICS_POSTHOG_KEY: { get: async () => "phc_x" } }, "hosted").variable).toBe("ANALYTICS_POSTHOG_KEY");
  });

  test("refusals name the variable, never the value", () => {
    const secretish = "phc_SECRETVALUE0001";
    const { message } = refused({ ANALYTICS_SINK: "collector", ANALYTICS_POSTHOG_KEY: secretish }, "selfhost");
    expect(message).not.toContain(secretish);
    const r = refused({ ...collector, ANALYTICS_COLLECTOR_URL: "https://user:SECRETPASS@collector.example/v1/events" }, "selfhost");
    expect(r.message).not.toContain("SECRETPASS");
  });

  test("operator counter lines survive the log allowlist as numbers", () => {
    const line = JSON.parse(logLine("warn", "analytics export degraded", { state: "collector", analytics_dropped_queue_full: 7, analytics_export_failures: 2, analytics_accepted: "x" }));
    expect(line).toMatchObject({ state: "collector", analytics_dropped_queue_full: 7, analytics_export_failures: 2 });
    expect(line.analytics_accepted).toBeUndefined();
  });
});

describe("Cloudflare Worker compatibility", () => {
  test("the analytics modules use no Node-only API", () => {
    for (const file of ["analytics.ts", "analytics-export.ts", "analytics-config.ts"]) {
      const source = readFileSync(new URL(`../${file}`, import.meta.url), "utf8");
      const code = source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:])\/\/.*$/gm, "$1");
      expect(code).not.toMatch(/from\s+["']node:/);
      expect(code).not.toMatch(/\brequire\s*\(/);
      expect(code).not.toMatch(/\bprocess\./);
      expect(code).not.toMatch(/\bBuffer\b/);
      expect(code).not.toMatch(/\bNodeJS\./);
      // Only each other and the destination policy the read Worker already imports.
      for (const m of code.matchAll(/from\s+["']([^"']+)["']/g)) expect(["./analytics.ts", "./analytics-export.ts", "./destination-url.ts"]).toContain(m[1]!);
    }
  });

  test("a Worker-style invocation: capture during the request, one bounded flush in waitUntil", async () => {
    const calls: string[] = [];
    const waitUntil: Promise<unknown>[] = [];
    const sink = createAnalyticsSink({ sink: "posthog", deploymentId: "hosted-prod", host: DEFAULT_POSTHOG_HOST, apiKey: "phc_testkey123" }, "session-read-api", "worker", {
      fetch: async (url) => { calls.push(url); return new Response(null, { status: 200 }); },
    });
    sink.capture(readEvent());
    // Nothing leaves before the flush: no timer in the Worker profile.
    await new Promise((r) => setTimeout(r, 10));
    expect(calls.length).toBe(0);
    waitUntil.push(sink.shutdown());
    await Promise.all(waitUntil);
    expect(calls).toEqual(["https://i.agentrelay.com/batch/"]);
  });
});
