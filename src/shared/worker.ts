/**
 * The stage worker runtime every consumer shares: one poll loop per stage over
 * `sessions.pipeline_jobs`, each message handled inside a span that continues
 * the message's traceparent. Upstream ships `pollPostgresStage` but no runner.
 */
import {
  pipelineQueueDeps,
  pollPostgresStage,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  type PipelineImplementations,
  type QueuedStage,
  type StageHandler,
  type StageMessage,
  type StageQueueDeps,
} from "./vendor.ts";
import { openHoopDb, waitForHoopDb, waitForSchema, type HoopDb } from "./db.ts";
import { ctxOf, enrichMessage } from "./enrich.ts";
import { vendorBindings, integer } from "./env.ts";
import { liveDataPathVerdict, workloadReadyLive } from "./data-path.ts";
import { errorFields, log } from "./log.ts";
import { initTelemetry, shutdownTelemetry, withSpan } from "./telemetry.ts";
import { createServer } from "node:http";
import { sqlPipelineEventSink } from "../hoopdb/sink.ts";
import { expectedMigrationNames } from "../hoopdb/migrate.ts";

export interface WorkerOptions {
  service: string;
  stages: QueuedStage[];
  /** Builds the handlers once the database is open. */
  handlers: (db: HoopDb, current: () => StageMessage | undefined) => Partial<Record<QueuedStage, StageHandler>>;
  /** Extra vendored implementations (e.g. the session index). */
  impl?: (db: HoopDb, current: () => StageMessage | undefined) => Omit<PipelineImplementations, "handlers" | "queue" | "deadLetters" | "sink">;
  /** Runs once per loop tick, for side loops such as reconcile. */
  tick?: (db: HoopDb, now: Date) => Promise<void>;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Wraps a handler so its work runs in a span on the message's trace, with allowlisted attrs. */
export function traced(stage: QueuedStage, handler: StageHandler): StageHandler {
  return async (message, recorder, attempt, job) => {
    const ctx = ctxOf(message);
    return withSpan(`stage ${stage}`, {
      traceparent: ctx.traceparent,
      attrs: {
        stage,
        attempt,
        trace_id: message.ctx.traceId,
        session_uuid: ctx.session_uuid,
        tenant_id: ctx.tenant_id ?? message.ctx.orgId,
        workspace_id: message.ctx.workspaceId,
        harness: message.ctx.harness,
        revision: message.revision,
      },
    }, async (span) => {
      const started = Date.now();
      try {
        const out = await handler(message, recorder, attempt, job);
        span.setAttribute("outcome", out && typeof out === "object" && out.status === "skipped" ? "skipped" : "ok");
        log.info("stage handled", {
          stage, attempt, outcome: out && typeof out === "object" ? out.status : "ok",
          trace_id: message.ctx.traceId, session_uuid: ctx.session_uuid, duration_ms: Date.now() - started,
        });
        return out;
      } catch (err) {
        const fields = errorFields(err);
        span.setAttribute("outcome", fields.retryable === false ? "quarantined" : "retry");
        log.warn("stage failed", { stage, attempt, trace_id: message.ctx.traceId, session_uuid: ctx.session_uuid, ...fields });
        throw err;
      }
    });
  };
}

/**
 * Worker health, for container and Kubernetes probes (workers have no other port).
 *   GET /healthz  200 while the poll loop is ticking and HoopDb answered within
 *                 the last WORKER_UNHEALTHY_AFTER_FAILURES ticks; 503 otherwise.
 *   GET /readyz   200 once HoopDb answers and the schema ledger is complete.
 *                 A GCS workload (LOCKER_PROVIDER=gcs or WORKER_DATA_PATH_PROBE=1)
 *                 also requires the data-path settings and refuses HMAC keys.
 *   GET /datapathz  the data-path verdict alone. Unarmed on the S3 default.
 * A rotated HoopDb password therefore turns the worker unhealthy and lets the
 * orchestrator restart it with the new secret, instead of "Running" forever.
 */
export interface WorkerHealth {
  ready: boolean;
  lastTickAt: number;
  consecutiveDbFailures: number;
}

/**
 * Liveness. `lastTickAt` is progress, not just loop turns: it moves on every
 * tick, at the start and end of every handled message, and while the worker
 * waits for HoopDb or the schema, so a long batch, reconcile or migration is
 * never mistaken for a stall.
 */
export function healthVerdict(h: WorkerHealth, now: number, opts: { maxFailures: number; maxTickAgeMs: number }) {
  if (h.consecutiveDbFailures >= opts.maxFailures) return { ok: false, reason: "hoopdb_unreachable" };
  if (now - h.lastTickAt > opts.maxTickAgeMs) return { ok: false, reason: "loop_stalled" };
  return { ok: true, reason: h.ready ? "ok" : "starting" };
}

/** Readiness: the schema is complete and HoopDb is answering now. */
export function readyVerdict(h: WorkerHealth, opts: { maxFailures: number }) {
  return h.ready && h.consecutiveDbFailures < opts.maxFailures;
}

function serveHealth(health: WorkerHealth, port: number) {
  const maxFailures = integer("WORKER_UNHEALTHY_AFTER_FAILURES", 5);
  const maxTickAgeMs = integer("WORKER_MAX_TICK_AGE_MS", 120_000);
  const server = createServer((req, res) => {
    if (req.url === "/readyz") {
      void workloadReadyLive(readyVerdict(health, { maxFailures }), process.env)
        .then((verdict) => {
          res.writeHead(verdict.ok ? 200 : 503, { "content-type": "application/json" });
          res.end(JSON.stringify(verdict));
        })
        .catch(() => {
          res.writeHead(503, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: false, missing: ["REDIS_TLS_ROUND_TRIP"] }));
        });
      return;
    }
    if (req.url === "/datapathz") {
      void liveDataPathVerdict(process.env)
        .then((verdict) => {
          res.writeHead(verdict.ok ? 200 : 503, { "content-type": "application/json" });
          res.end(JSON.stringify(verdict.ok ? { ok: true, armed: verdict.armed } : { ok: false, missing: verdict.missing }));
        })
        .catch(() => {
          res.writeHead(503, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: false, missing: ["REDIS_TLS_ROUND_TRIP"] }));
        });
      return;
    }
    if (req.url === "/healthz") {
      const v = healthVerdict(health, Date.now(), { maxFailures, maxTickAgeMs });
      res.writeHead(v.ok ? 200 : 503, { "content-type": "application/json" });
      res.end(JSON.stringify(v));
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(port, "0.0.0.0", () => log.info("worker health listening", { port }));
}

export async function runWorker(opts: WorkerOptions): Promise<never> {
  process.env.OTEL_SERVICE_NAME ||= opts.service;
  initTelemetry(opts.service);
  const health: WorkerHealth = { ready: false, lastTickAt: Date.now(), consecutiveDbFailures: 0 };
  serveHealth(health, integer("WORKER_HEALTH_PORT", 8081));
  const db = openHoopDb();
  // Waiting for HoopDb or a long migration is progress, not a stall.
  const startupBeat = setInterval(() => (health.lastTickAt = Date.now()), 10_000);
  await waitForHoopDb(db);
  await waitForSchema(db, await expectedMigrationNames());
  clearInterval(startupBeat);
  health.ready = true;
  health.lastTickAt = Date.now();
  const env = vendorBindings() as Parameters<typeof pipelineQueueDeps>[0];
  const rawQueue = postgresPipelineQueue(db);
  let current: StageMessage | undefined;
  const currentRef = () => current;
  // Everything sent from inside a stage carries the ids of the message being handled.
  const queue = {
    backend: rawQueue.backend,
    send: async (m: StageMessage, options?: Parameters<typeof rawQueue.send>[1]) => rawQueue.send(await enrichMessage(m), options),
    sendBatch: async (ms: readonly StageMessage[], options?: Parameters<typeof rawQueue.sendBatch>[1]) =>
      rawQueue.sendBatch(await Promise.all(ms.map((m) => enrichMessage(m))), options),
  };
  const impl = opts.impl?.(db, currentRef) ?? {};
  const handlers = opts.handlers(db, currentRef);
  const deps: StageQueueDeps = pipelineQueueDeps(env, {
    ...impl,
    queue,
    deadLetters: postgresDeadLetterStore(db),
    sink: sqlPipelineEventSink(db),
    handlers,
  });
  // Every handler, ours or vendored, runs in a span on the message's trace, and
  // the message in flight is tracked so fan-out continues that trace.
  for (const stage of Object.keys(deps.handlers) as QueuedStage[]) {
    const h = traced(stage, deps.handlers[stage]!);
    deps.handlers[stage] = async (message, recorder, attempt) => {
      current = message;
      health.lastTickAt = Date.now(); // progress inside a batch
      try {
        return await h(message, recorder, attempt);
      } finally {
        current = undefined;
        health.lastTickAt = Date.now();
      }
    };
  }
  const idleMs = integer("WORKER_IDLE_MS", 1_000);
  const batchSize = integer("WORKER_BATCH_SIZE", 10);
  const leaseSeconds = integer("WORKER_LEASE_SECONDS", 300);
  log.info("worker started", { count: opts.stages.length });
  let stopping = false;
  const stop = async () => {
    stopping = true;
    await shutdownTelemetry();
    await db.end();
    process.exit(0);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  for (;;) {
    if (stopping) await sleep(idleMs);
    let claimed = 0;
    // One cheap probe per tick: a failing HoopDb (e.g. rotated password) marks the worker unhealthy.
    try {
      await db.query("SELECT 1");
      health.consecutiveDbFailures = 0;
    } catch (err) {
      health.consecutiveDbFailures += 1;
      log.warn("hoopdb probe failed", { count: health.consecutiveDbFailures, ...errorFields(err) });
    }
    health.lastTickAt = Date.now();
    for (const stage of opts.stages) {
      try {
        const r = await pollPostgresStage({ sql: db, stage, deps, batchSize, leaseSeconds });
        claimed += r.claimed;
        if (r.claimed) log.info("poll", { stage, claimed: r.claimed, acked: r.acked, retried: r.retried, exhausted: r.exhausted });
      } catch (err) {
        log.error("poll failed", { stage, ...errorFields(err) });
      }
    }
    if (opts.tick) {
      // Long side work (reconcile) keeps the progress clock moving while it runs.
      const tickBeat = setInterval(() => (health.lastTickAt = Date.now()), 10_000);
      try {
        await opts.tick(db, new Date());
      } catch (err) {
        log.error("tick failed", errorFields(err));
      } finally {
        clearInterval(tickBeat);
      }
    }
    if (claimed === 0) await sleep(idleMs);
  }
}

/** The object-store reader and raw store for this deployment, from env. */
export function rawStoreOrThrow() {
  const env = vendorBindings();
  return env;
}
