/**
 * SERVICE=session-read-api: the session read API (app.ts) served over HTTP in
 * the self-host services image. Default off (RELAYHISTORY_SELFHOST_READ_API);
 * see service-config.ts and docs/session-read-api-design.md ("Self-host
 * SERVICE").
 *
 * Enabled, the app is assembled by the Lambda's own assembly (lambda.ts
 * readAppFromSecret: same secret shape, verifier, read-role pool and env) under
 * the service entry's rules, once at startup, so a bad configuration fails the
 * process instead of the first request. Search is selected by
 * SEARCH_INDEX_BACKEND (default postgres here, so no ClickHouse or Lambda is
 * needed; clickhouse keeps the hosted binding; opensearch is the managed AWS
 * index); see selfhostSearchComposition.
 * With LOCKER_BUCKET set, timing, turns, metadata and catalog read canonical
 * S3 (service-canonical.ts); READ_API_AUTH_MODE=relaycontrol_only removes the
 * RelayAuth verifier entirely (lambda.ts readApiAuthConfig, auth.ts).
 * Like the Lambda and the Worker it answers GET, plus exactly one POST route: `POST /v1/cli/login`, whose JSON
 * body is the only request body ever read. It is buffered here with a hard
 * cap (LOGIN_BODY_CAP_BYTES, 413 beyond it) before the app sees it; every
 * other request body is drained, never read.
 */
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { Hono } from "hono";
import { errorFields, log } from "../shared/log.ts";
import { createPostgresSearchComposition, type SearchComposition } from "../shared/search-composition.ts";
import { createOpenSearchSearchComposition } from "../shared/opensearch-composition.ts";
import { parseCompleteSince } from "./config.ts";
import { readAppFromSecret, serviceSearchBackend } from "./lambda.ts";
import { readApiServiceConfig, SEARCH_BACKEND_ID_VAR } from "./service-config.ts";

type FetchApp = { fetch(req: Request): Response | Promise<Response> };
export type ReadAppBuilder = (secret: string, env: NodeJS.ProcessEnv) => Promise<{ app: FetchApp; close(): Promise<void> }>;

/**
 * The service's search composition, chosen by SEARCH_INDEX_BACKEND through the
 * adapter registry (never hard-wired): postgres (the default here) composes the
 * registry's Postgres SearchIndexAdapter with epoch-fenced Postgres
 * completeness state for SEARCH_INDEX_POSTGRES_BACKEND_ID, covering history from
 * READ_API_COMPLETE_SINCE; clickhouse returns none (the Lambda's ClickHouse
 * client, from the secret). Unknown or unrunnable names refuse startup, naming
 * the variable only.
 */
export function selfhostSearchComposition(env: NodeJS.ProcessEnv): SearchComposition | undefined {
  const backend = serviceSearchBackend(env);
  if (backend === "clickhouse") return undefined;
  const backendId = env[SEARCH_BACKEND_ID_VAR]?.trim();
  if (!backendId) throw new Error(`96bulls: SEARCH_INDEX_BACKEND=${backend} needs ${SEARCH_BACKEND_ID_VAR}`);
  const fromMs = parseCompleteSince(env.READ_API_COMPLETE_SINCE);
  if (backend === "opensearch") return createOpenSearchSearchComposition({ env, backendId, fromMs });
  return createPostgresSearchComposition({ backendId, fromMs });
}

/** The production builder: the Lambda's assembly under the service entry's rules, with the selected search composition. */
export const selfhostReadApp: ReadAppBuilder = async (secret, env) =>
  await readAppFromSecret(secret, env, selfhostSearchComposition(env), { entry: "service" });

const errorJson = (code: string, message: string) => ({ error: { code, message } });

/** The only non-GET route the service lets through (exact path match, POST only). */
const LOGIN_PATH = "/v1/cli/login";
/** Mirrors the app's bodyLimit on POST /v1/cli/login (8 KiB); the entry enforces it before the app runs. */
const LOGIN_BODY_CAP_BYTES = 8 * 1024;

/** The routes the service answers: `/healthz` always, then the read API (enabled) or 503 (dormant). */
export function readApiServiceApp(app: FetchApp | null): Hono {
  const service = new Hono();
  service.get("/healthz", (c) => c.json({ ok: true, enabled: app !== null }));
  if (!app) {
    // Dormant: every route but liveness is 503, nothing is read.
    service.all("*", (c) => {
      c.header("Retry-After", "60");
      c.header("Cache-Control", "private, no-store");
      return c.json(errorJson("not_configured", "the session read API is not enabled"), 503);
    });
    return service;
  }
  service.all("*", (c) => {
    // c.req.path is percent-decoded; the raw pathname must match too, the same check readApiHttpServer uses to read the body.
    const login = c.req.path === LOGIN_PATH && new URL(c.req.url).pathname === LOGIN_PATH;
    if (c.req.method === "GET" || (login && c.req.method === "POST")) return app.fetch(c.req.raw);
    return login
      ? c.json(errorJson("method_not_allowed", "GET or POST only"), 405, { allow: "GET, POST" })
      : c.json(errorJson("method_not_allowed", "GET only"), 405, { allow: "GET" });
  });
  return service;
}

/**
 * Buffers a request body up to `cap` bytes. Resolves "too_large" as soon as the
 * declared Content-Length or the bytes actually received exceed the cap; the
 * rest of the upload is left unread (the caller closes the connection).
 */
function readCappedBody(req: IncomingMessage, cap: number): Promise<Buffer | "too_large"> {
  const declared = req.headers["content-length"];
  if (declared !== undefined && Number(declared) > cap) return Promise.resolve("too_large");
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let settled = false;
    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      req.off("data", onData).off("end", onEnd).off("error", onError).off("close", onClose);
      fn();
    };
    const onData = (chunk: Buffer) => {
      size += chunk.length;
      if (size > cap) {
        req.pause();
        settle(() => resolve("too_large"));
        return;
      }
      chunks.push(chunk);
    };
    const onEnd = () => settle(() => resolve(Buffer.concat(chunks, size)));
    const onError = (err: Error) => settle(() => reject(err));
    const onClose = () => settle(() => reject(new Error("request closed before its body was complete")));
    req.on("data", onData).on("end", onEnd).on("error", onError).on("close", onClose);
  });
}

/**
 * node:http in front of a fetch handler. With `readLoginBody` (the enabled
 * service), only `POST /v1/cli/login` has its body read (buffered, capped at
 * LOGIN_BODY_CAP_BYTES); without it (dormant, the default) and for every other
 * request the body is drained, never read. Bodies and header values are never
 * logged.
 */
export function readApiHttpServer(handler: FetchApp, options: { readLoginBody?: boolean } = {}): Server {
  return createServer((req, res) => {
    void (async () => {
      try {
        const url = `http://localhost${req.url?.startsWith("/") ? req.url : "/"}`;
        let body: Buffer | undefined;
        if (options.readLoginBody === true && req.method === "POST" && new URL(url).pathname === LOGIN_PATH) {
          const read = await readCappedBody(req, LOGIN_BODY_CAP_BYTES);
          if (read === "too_large") {
            // The app is never called; the connection is closed once the 413 is out so the rest of the upload is not consumed.
            const payload = JSON.stringify(errorJson("body_too_large", `request body exceeds ${LOGIN_BODY_CAP_BYTES} bytes`));
            res.writeHead(413, { "content-type": "application/json", "cache-control": "private, no-store", connection: "close" });
            res.end(payload, () => req.socket.destroy());
            return;
          }
          body = read;
        } else {
          req.resume();
        }
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined) continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        const request = new Request(url, { method: req.method, headers, body });
        const response = await handler.fetch(request);
        const out: Record<string, string> = {};
        response.headers.forEach((v, k) => (out[k] = v));
        res.writeHead(response.status, out);
        if (response.body && req.method !== "HEAD")
          for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) res.write(Buffer.from(chunk));
        res.end();
      } catch (err) {
        log.error("session read api request crashed", errorFields(err));
        if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify(errorJson("internal_error", "internal error")));
      }
    })();
  });
}

export interface ReadApiService {
  server: Server;
  port: number;
  enabled: boolean;
  /** Stops accepting connections, waits for in-flight requests, then releases the read pool. */
  close(): Promise<void>;
}

/**
 * Parse the config, assemble the app (enabled) and listen. `host`/`port`
 * override the bind for tests (loopback, port 0); `build` replaces the Lambda
 * assembly with test adapters.
 */
export async function startReadApiService(
  env: NodeJS.ProcessEnv = process.env,
  options: { host?: string; port?: number; build?: ReadAppBuilder } = {},
): Promise<ReadApiService> {
  const config = readApiServiceConfig(env);
  const built = config.enabled ? await (options.build ?? selfhostReadApp)(config.secret, env) : null;
  const server = readApiHttpServer(readApiServiceApp(built?.app ?? null), { readLoginBody: built !== null });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.port ?? config.port, options.host ?? "0.0.0.0", () => {
        server.off("error", reject);
        resolve();
      });
    });
  } catch (error) {
    await built?.close();
    throw error;
  }
  const port = (server.address() as AddressInfo).port;
  log.info("session read api listening", { port, state: config.enabled ? "enabled" : "dormant" });
  let closing: Promise<void> | undefined;
  return {
    server,
    port,
    enabled: config.enabled,
    close: () =>
      (closing ??= new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeIdleConnections?.();
      }).then(() => built?.close())),
  };
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const service = await startReadApiService(env);
  const stop = () => {
    // Bounded: a client holding a connection open cannot block the pod's termination.
    setTimeout(() => process.exit(0), 10_000).unref();
    service.close().then(
      () => process.exit(0),
      (err) => {
        log.error("session read api shutdown failed", errorFields(err));
        process.exit(1);
      },
    );
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
