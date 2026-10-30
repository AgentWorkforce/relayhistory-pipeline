/**
 * AlleyOop ingest: the HTTP side of local-to-Locker. The vendored Hono app's
 * pipeline routes (`/v1/pipeline/chunks` = Technical Foul scrub-before-storage,
 * `/v1/pipeline/manifests`, `/v1/pipeline/events`, `/v1/pipeline/traces/:id`)
 * run on Node/Bun with Postgres deps instead of Cloudflare queues. Only the
 * pipeline routes are mounted: the rest of the Worker needs Neon.
 */
import { createServer } from "node:http";
import { Hono } from "hono";
import {
  createPipelineRoutes,
  pipelineOpsRoutes,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  requireAuth,
  SCRUB_REFUSAL_CODES,
  TRACE_ID_HEADER,
  traceIdFromHeaders,
  type AppEnv,
  type Bindings,
  type PipelineStage,
  type StageMessage,
} from "../shared/vendor.ts";
import { openHoopDb, waitForHoopDb, waitForSchema } from "../shared/db.ts";
import { enrichMessage, requestIdentity } from "../shared/enrich.ts";
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import { traceparentFor } from "../shared/ids.ts";
import { integer, optional, required, vendorBindings } from "../shared/env.ts";
import { liveDataPathVerdict } from "../shared/data-path.ts";
import { healthVerdict, type WorkerHealth } from "../shared/worker.ts";
import { errorFields, log } from "../shared/log.ts";
import { initTelemetry, withSpan } from "../shared/telemetry.ts";
import { sqlPipelineEventSink } from "../hoopdb/sink.ts";
import { expectedMigrationNames } from "../hoopdb/migrate.ts";
import { jwksFromPrivateKey } from "./auth.ts";
import { createCourierManifestRoutes } from "./manifests.ts";
import { requestLog } from "./request-log.ts";

export async function buildIngestApp() {
  const port = integer("INGEST_PORT", 8080);
  const jwks = process.env.JWKS_JSON
    ? (JSON.parse(process.env.JWKS_JSON) as Record<string, unknown>)
    : process.env.JWT_PRIVATE_KEY_PEM
      ? jwksFromPrivateKey(process.env.JWT_PRIVATE_KEY_PEM, required("JWT_KID"))
      : undefined;
  const env = vendorBindings({
    ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
    RELAYAUTH_ISSUER: optional("AUTH_ISSUER", "https://96bulls.local"),
    RELAYAUTH_AUDIENCE: optional("AUTH_AUDIENCE", "relayhistory"),
    RELAYAUTH_JWKS_URL: optional("RELAYAUTH_JWKS_URL", `http://127.0.0.1:${port}/.well-known/jwks.json`),
    ...(process.env.PIPELINE_OPS_SECRET ? { PIPELINE_OPS_SECRET: process.env.PIPELINE_OPS_SECRET } : {}),
    // Hosted/customer installs on a shared RelayAuth issuer: set RELAYHISTORY_ORIGIN so only
    // assertions minted for this deployment's origin are accepted (legacy shared audience refused).
    ...(process.env.RELAYHISTORY_ORIGIN ? { RELAYHISTORY_ORIGIN: process.env.RELAYHISTORY_ORIGIN } : {}),
    ...(process.env.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS ? { RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS: process.env.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS } : {}),
  }) as unknown as Bindings;
  if (!jwks && !process.env.RELAYAUTH_JWKS_URL) throw new Error("96bulls: ingest needs JWKS_JSON, JWT_PRIVATE_KEY_PEM or RELAYAUTH_JWKS_URL");

  const db = openHoopDb();
  await waitForHoopDb(db);
  await waitForSchema(db, await expectedMigrationNames());
  const queue = postgresPipelineQueue(db);
  const reader = lockerManifestReader();
  const sink = sqlPipelineEventSink(db);
  const deadLetters = postgresDeadLetterStore(db);
  const send = async (_env: Bindings, stage: PipelineStage, message: StageMessage) =>
    withSpan("enqueue", { traceparent: traceparentFor(message.ctx.traceId), attrs: { stage, trace_id: message.ctx.traceId } }, async () => {
      await queue.send(await enrichMessage({ ...message, stage }, reader));
    });
  const pipeline = createPipelineRoutes({
    manifests: () => reader,
    rawStore: () => lockerStore(),
    sink: () => sink,
    deadLetters: () => deadLetters,
    send,
    onChunkRefused: (code) => {
      if (SCRUB_REFUSAL_CODES.has(code)) log.warn("scrub refused", { kind: "scrub_refusal", error_code: code, route: "/v1/pipeline/chunks" });
    },
  });
  const courierManifests = createCourierManifestRoutes({ manifests: reader, raw: lockerStore(), send });

  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    const correlationId = c.req.header("X-Correlation-Id")?.trim() || requestId;
    const traceId = traceIdFromHeaders((name) => c.req.header(name));
    c.set("requestId", requestId);
    c.set("correlationId", correlationId);
    if (traceId) c.set("traceId", traceId);
    await next();
    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", correlationId);
    if (traceId) c.header(TRACE_ID_HEADER, traceId);
  });
  // One allowlisted request line per request (method, route status, duration; never bodies or query values).
  app.use("*", requestLog());
  if (jwks) app.get("/.well-known/jwks.json", (c) => c.json(jwks));
  app.get("/health", (c) => c.json({ ok: true, service: "alleyoop-ingest" }));
  // Liveness: same rule as the workers. A background SELECT 1 every 10s; after
  // WORKER_UNHEALTHY_AFTER_FAILURES consecutive failures (e.g. a rotated HoopDb
  // password once sessions reconnect) /healthz turns 503 so the orchestrator restarts us.
  const health: WorkerHealth = { ready: true, lastTickAt: Date.now(), consecutiveDbFailures: 0 };
  const maxFailures = integer("WORKER_UNHEALTHY_AFTER_FAILURES", 5);
  const maxTickAgeMs = integer("WORKER_MAX_TICK_AGE_MS", 120_000);
  // One probe at a time, so results apply in order (a late success can't mask newer failures).
  let probing = false;
  setInterval(() => {
    if (probing) return;
    probing = true;
    db.query("SELECT 1")
      .then(() => (health.consecutiveDbFailures = 0))
      .catch(() => (health.consecutiveDbFailures += 1))
      .finally(() => {
        health.lastTickAt = Date.now();
        probing = false;
      });
  }, 10_000).unref();
  app.get("/healthz", (c) => {
    const v = healthVerdict(health, Date.now(), { maxFailures, maxTickAgeMs });
    return c.json(v, v.ok ? 200 : 503);
  });
  // Ready = the schema this image was built for is fully migrated, not just "DB answers".
  const expected = await expectedMigrationNames();
  app.get("/readyz", async (c) => {
    try {
      const { rows } = await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations");
      const have = new Set(rows.map((r) => r.name));
      const missing = expected.filter((n) => !n.startsWith("shim:") && !have.has(n));
      if (missing.length) return c.json({ ok: false, reason: "schema_pending", count: missing.length }, 503);
      const data = await liveDataPathVerdict(process.env);
      if (!data.ok) return c.json({ ok: false, missing: data.missing }, 503);
      return c.json({ ok: true });
    } catch {
      return c.json({ ok: false, reason: "hoopdb_unreachable" }, 503);
    }
  });
  const v1 = new Hono<AppEnv>();
  v1.use("*", async (c, next) => requireAuth(c as never, next));
  // Every send made while handling this request carries the authenticated user.
  v1.use("*", async (c, next) => {
    const auth = c.get("auth") as { userId?: string } | undefined;
    return requestIdentity.run({ userId: auth?.userId }, () => next());
  });
  // AlleyOop owns authenticated manifest writes and their compare-and-swap
  // fence. Mount before the provenance-tracked pipeline routes it shadows.
  v1.route("/", courierManifests);
  v1.route("/", pipeline);
  app.route("/v1", pipelineOpsRoutes);
  app.route("/v1", v1);
  app.notFound((c) => c.json({ error: { code: "not_found", message: "no route" } }, 404));
  app.onError((err, c) => {
    log.error("request failed", errorFields(err));
    if (process.env.DEBUG === "1") console.error(err);
    return c.json({ error: { code: "internal_error", message: "Internal server error" } }, 500);
  });
  return { app, env, port, db };
}

export async function serve() {
  process.env.OTEL_SERVICE_NAME ||= "alleyoop-ingest";
  initTelemetry();
  const { app, env, port } = await buildIngestApp();
  const executionContext = { waitUntil: (p: Promise<unknown>) => void p.catch(() => {}), passThroughOnException: () => {}, props: {} };
  const server = createServer((req, res) => {
    void (async () => {
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(chunk as Buffer);
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined) continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        const url = `http://${req.headers.host ?? "127.0.0.1"}${req.url ?? "/"}`;
        const body = chunks.length ? Buffer.concat(chunks) : undefined;
        const request = new Request(url, { method: req.method, headers, body: req.method === "GET" || req.method === "HEAD" ? undefined : body });
        const response = await app.fetch(request, env, executionContext as unknown as ExecutionContext);
        const out: Record<string, string> = {};
        response.headers.forEach((v, k) => (out[k] = v));
        res.writeHead(response.status, out);
        if (response.body) for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) res.write(Buffer.from(chunk));
        res.end();
      } catch (err) {
        log.error("request crashed", errorFields(err));
        if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { code: "internal_error" } }));
      }
    })();
  });
  await new Promise<void>((resolve) => server.listen(port, "0.0.0.0", resolve));
  log.info("alleyoop ingest listening", { port });
  const stop = () => server.close(() => process.exit(0));
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
