/**
 * SERVICE=session-ingest: the session ingest Hono app (app.ts) served over
 * node:http in the self-host services image, with no Cloudflare binding, no
 * RelayAuth and no legacy relayhistory-cloud. Default off
 * (RELAYHISTORY_SELFHOST_INGEST, selfhost-config.ts); the hosted Worker
 * (worker.ts) is unchanged and never imports this file.
 *
 *   GET  /healthz              liveness, always 200 { ok, enabled }
 *   GET  /readyz               enabled and the ledger answers SELECT 1: 200; otherwise 503
 *   POST /v1/delivery/batches  rth:sync   (app.ts, unchanged handler)
 *   POST /v1/turns/batches     rth:sync   (app.ts, unchanged handler)
 *   GET  /v1/delivery/limits   rth:sync   (app.ts, unchanged handler)
 *   any other /v1 path         501 unsupported_by_backend (selection, reset, recovery, custody,
 *                              onboarding are relayhistory-internal and not served self-host)
 *
 * Bindings, Node instead of Cloudflare:
 *   HYPERDRIVE     -> pg.Pool as bulls96_session_ingest (discrete fields, SESSION_POSTGRES_TLS_PROFILE),
 *                     admitted at startup only if current_user is exactly that role, not superuser,
 *                     not BYPASSRLS
 *   S3 secrets     -> the Locker store (src/shared/locker.ts: MinIO static keys, EKS Pod Identity, or GCS)
 *   INGEST_FANOUT  -> none. The queue only exists because a Worker invocation has a subrequest budget
 *                     (staging.ts); a Node process does not, so `inlineMaxSessions` is unset and every
 *                     batch is written session by session BEFORE the receipt. Nothing is ever staged, so
 *                     there is nothing to fan out and no outbox to lose on a crash: an interrupted request
 *                     leaves a pending claim the Desktop's retry resumes (ledger `resume`), as on the Worker.
 *   NEON_REPLICA   -> none (refused by config)
 *   INGEST_SIGNING_KEY (relayhistory HMAC) -> replaced by relaycontrol introspection (selfhost-auth.ts)
 *
 * Auth runs in this wrapper BEFORE the request body is read: an unauthenticated
 * or unscoped caller never gets the edge to buffer MAX_DELIVERY_BYTES. The
 * verified identity reaches the app only through a WeakMap keyed by the exact
 * Request object handed to app.fetch; no header (x-relay-ingest-identity
 * included, which is stripped) can assert identity. Bodies, tokens and header
 * values are never logged.
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";
import pg from "pg";
import { errorFields, log } from "../shared/log.ts";
import { buildLockerStore } from "../shared/locker.ts";
import type { ObjectStore } from "../shared/vendor.ts";
import { AuthError } from "../session-read-api/app.ts";
import { createIngestApp, IDENTITY_HEADER, SIGNATURE_HEADER, type IngestIdentity } from "./app.ts";
import { sqlLedger } from "./ledger-sql.ts";
import type { SqlRun } from "./ledger-sql.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector } from "./selfhost-auth.ts";
import { SELFHOST_INGEST_ROLE, selfhostIngestConfig, type SelfhostIngestSettings } from "./selfhost-config.ts";

type FetchApp = { fetch(req: Request): Response | Promise<Response> };

/** The only routes served, by exact raw pathname. */
export const SELFHOST_INGEST_ROUTES: Readonly<Record<string, "GET" | "POST">> = Object.freeze({
  "/v1/delivery/batches": "POST",
  "/v1/turns/batches": "POST",
  "/v1/delivery/limits": "GET",
});

/** One assembled ingest: the app, the per-request verifier, readiness and teardown. */
export interface SelfhostIngestRuntime {
  app: FetchApp;
  /** Verifies one Authorization header value; throws AuthError to refuse. */
  verify(authorization: string | undefined): Promise<IngestIdentity>;
  /** Hands the verified identity to the app for exactly this Request object. */
  bind(request: Request, identity: IngestIdentity): void;
  ready(): Promise<boolean>;
  close(): Promise<void>;
}

export interface RuntimeParts {
  run: SqlRun;
  store: ObjectStore;
  verify: (authorization: string | undefined) => Promise<IngestIdentity>;
  ready: () => Promise<boolean>;
  close: () => Promise<void>;
}

/** The ingest app over the given parts. Canonical acceptance, inline writes, no fan-out, no replica, no custody. */
export function selfhostIngestRuntime(settings: Pick<SelfhostIngestSettings, "keyRoot" | "maxPerUser" | "maxPerOrg">, parts: RuntimeParts): SelfhostIngestRuntime {
  const verified = new WeakMap<Request, IngestIdentity>();
  const app = createIngestApp({
    identify: (request) => {
      const identity = verified.get(request);
      // Unreachable through the wrapper; a direct app.fetch without bind() is refused, never trusted.
      if (!identity) throw new AuthError(401, "missing_authorization", "missing Authorization header");
      return identity;
    },
    deliver: {
      ledger: sqlLedger(parts.run, { acceptanceMode: "canonical" }),
      acceptanceMode: "canonical",
      target: { store: parts.store, keyRoot: settings.keyRoot },
      // Slot TTL past any request this process serves, so a lost release frees itself (Worker: 90).
      slots: { maxPerUser: settings.maxPerUser, maxPerOrg: settings.maxPerOrg, ttlSeconds: 90 },
      // inlineMaxSessions unset: always inline, so `fanout` is never needed (see the header).
    },
    log: (fields) => log.info("session ingest", fields),
  });
  return { app, verify: parts.verify, bind: (request, identity) => void verified.set(request, identity), ready: parts.ready, close: parts.close };
}

/** Startup admission: the pool's login is exactly the ingest role, not superuser, not BYPASSRLS. Names the property only. */
export async function assertIngestRole(run: SqlRun): Promise<void> {
  const { rows } = await run("SELECT current_user AS role, r.rolsuper, r.rolbypassrls FROM pg_roles r WHERE r.rolname = current_user", []);
  const me = rows[0];
  if (rows.length !== 1 || !me || me.role !== SELFHOST_INGEST_ROLE) throw new Error(`96bulls: the session ingest login must be ${SELFHOST_INGEST_ROLE}`);
  if (me.rolsuper !== false || me.rolbypassrls !== false) throw new Error(`96bulls: ${SELFHOST_INGEST_ROLE} must not be superuser or BYPASSRLS`);
}

export type RuntimeBuilder = (settings: SelfhostIngestSettings, env: NodeJS.ProcessEnv) => Promise<SelfhostIngestRuntime>;

/** Production: pg pool, Locker store, relaycontrol introspection with no positive cache. */
export const productionRuntime: RuntimeBuilder = async (settings, env) => {
  const pool = new pg.Pool({
    ...settings.pool,
    application_name: "96bulls-session-ingest",
    idleTimeoutMillis: 60_000,
    connectionTimeoutMillis: 2_000,
    // Nonempty, so pg never applies PGOPTIONS; the same bounds the role itself sets.
    options: "-c statement_timeout=5000 -c lock_timeout=2000",
  });
  pool.on("error", (err) => log.error("session ingest pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  const run: SqlRun = async (text, params) => (await pool.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
  try {
    await assertIngestRole(run);
    const store = buildLockerStore(env).store as unknown as ObjectStore;
    const verify = relaycontrolIngestIdentity(settings, selfhostIntrospector(settings, env));
    return selfhostIngestRuntime(settings, {
      run,
      store,
      verify,
      ready: async () => {
        try {
          await run("SELECT 1", []);
          return true;
        } catch {
          return false;
        }
      },
      close: () => pool.end(),
    });
  } catch (error) {
    await pool.end().catch(() => {});
    throw error;
  }
};

const correlationOf = (req: IncomingMessage) => {
  const raw = req.headers["x-correlation-id"];
  return (typeof raw === "string" ? raw.trim().slice(0, 128) : "") || crypto.randomUUID();
};

function sendJson(req: IncomingMessage, res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) {
  // A refused request's body is never read: close the connection once the answer is out.
  const unread = !req.complete && req.method !== "GET" && req.method !== "HEAD";
  res.writeHead(status, { "content-type": "application/json", "cache-control": "private, no-store", ...(unread ? { connection: "close" } : {}), ...headers });
  res.end(JSON.stringify(body), () => {
    if (unread) req.socket?.destroy();
  });
}

const envelope = (code: string, message: string, correlationId: string, extra: Record<string, unknown> = {}) => ({ error: { code, message, ...extra }, correlationId });

/** node:http in front of the runtime (null: dormant). */
export function selfhostIngestHttpServer(runtime: SelfhostIngestRuntime | null): Server {
  const server = createServer((req, res) => {
    void (async () => {
      const correlationId = correlationOf(req);
      try {
        const url = new URL(`http://localhost${req.url?.startsWith("/") ? req.url : "/"}`);
        const path = url.pathname;
        if (req.method === "GET" && path === "/healthz") return sendJson(req, res, 200, { ok: true, enabled: runtime !== null });
        if (req.method === "GET" && path === "/readyz") {
          const ok = runtime ? await runtime.ready() : false;
          return sendJson(req, res, ok ? 200 : 503, { ok, enabled: runtime !== null });
        }
        if (!runtime)
          return sendJson(req, res, 503, envelope("not_configured", "the session ingest is not enabled", correlationId), { "retry-after": "60", "x-correlation-id": correlationId });
        const method = SELFHOST_INGEST_ROUTES[path];
        if (!method) {
          if (path.startsWith("/v1/"))
            return sendJson(req, res, 501, envelope("unsupported_by_backend", "this route is not served by the self-host session ingest", correlationId, { reason: "route_not_served_selfhost" }), { "x-correlation-id": correlationId });
          return sendJson(req, res, 404, envelope("not_found", "not found", correlationId), { "x-correlation-id": correlationId });
        }
        if (req.method !== method)
          return sendJson(req, res, 405, envelope("method_not_allowed", `${method} only`, correlationId), { allow: method, "x-correlation-id": correlationId });

        // Identity before any body byte is read.
        let identity: IngestIdentity;
        try {
          identity = await runtime.verify(req.headers.authorization);
        } catch (error) {
          const e = error instanceof AuthError ? error : new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
          log.info("session ingest", { route: "ingest.auth", outcome: e.code, status: e.status, correlation_id: correlationId });
          return sendJson(req, res, e.status, envelope(e.code, e.message, correlationId), {
            "x-correlation-id": correlationId,
            ...(e.status === 503 ? { "retry-after": "1" } : {}),
            ...(e.status === 401 ? { "www-authenticate": "Bearer" } : {}),
          });
        }

        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined || k === IDENTITY_HEADER || k === SIGNATURE_HEADER || k === "authorization") continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        headers.set("x-correlation-id", correlationId);
        const post = method === "POST";
        if (!post) req.resume();
        const request = new Request(url.toString(), {
          method,
          headers,
          ...(post ? { body: Readable.toWeb(req) as unknown as ReadableStream<Uint8Array>, duplex: "half" } : {}),
        } as RequestInit);
        runtime.bind(request, identity);
        const response = await runtime.app.fetch(request);
        const out: Record<string, string> = {};
        response.headers.forEach((v, k) => (out[k] = v));
        // The app stopped reading at its cap (413) or never needed the rest: do not keep the upload's socket.
        const unread = post && !req.complete;
        if (unread) out.connection = "close";
        res.writeHead(response.status, out);
        if (response.body) for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) res.write(Buffer.from(chunk));
        res.end(() => {
          if (unread) req.socket?.destroy();
        });
      } catch (err) {
        log.error("session ingest request crashed", errorFields(err));
        if (!res.headersSent) sendJson(req, res, 500, envelope("internal_error", "internal error", correlationId));
        else res.destroy();
      }
    })();
  });
  // A slow or stalled upload cannot hold a connection (and its slot) forever.
  server.requestTimeout = 60_000; // inside the 90 s slot TTL
  server.headersTimeout = 30_000;
  return server;
}

export interface SelfhostIngestService {
  server: Server;
  port: number;
  enabled: boolean;
  close(): Promise<void>;
}

/** Parse the config, assemble (enabled) and listen. `host`/`port` override the bind for tests; `build` replaces the production runtime. */
export async function startSelfhostIngestService(
  env: NodeJS.ProcessEnv = process.env,
  options: { host?: string; port?: number; build?: RuntimeBuilder } = {},
): Promise<SelfhostIngestService> {
  const config = selfhostIngestConfig(env);
  const runtime = config.enabled ? await (options.build ?? productionRuntime)(config, env) : null;
  const server = selfhostIngestHttpServer(runtime);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.port ?? config.port, options.host ?? "0.0.0.0", () => {
        server.off("error", reject);
        resolve();
      });
    });
  } catch (error) {
    await runtime?.close();
    throw error;
  }
  const port = (server.address() as AddressInfo).port;
  log.info("session ingest listening", { port, state: config.enabled ? "enabled" : "dormant" });
  let closing: Promise<void> | undefined;
  return {
    server,
    port,
    enabled: config.enabled,
    close: () =>
      (closing ??= new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeIdleConnections?.();
      }).then(() => runtime?.close())),
  };
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const service = await startSelfhostIngestService(env);
  const stop = () => {
    setTimeout(() => process.exit(0), 10_000).unref();
    service.close().then(
      () => process.exit(0),
      (err) => {
        log.error("session ingest shutdown failed", errorFields(err));
        process.exit(1);
      },
    );
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
