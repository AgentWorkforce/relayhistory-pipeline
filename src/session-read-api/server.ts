/**
 * SERVICE=session-read-api: the session read API (app.ts) served over HTTP in
 * the self-host services image. Default off (RELAYHISTORY_SELFHOST_READ_API);
 * see service-config.ts and docs/session-read-api-design.md ("Self-host
 * SERVICE").
 *
 * Enabled, the app is assembled exactly as the Lambda assembles it
 * (lambda.ts readAppFromSecret: same secret shape, verifier, search binding,
 * read-role pool and env), once at startup, so a bad configuration fails the
 * process instead of the first request. Like the Lambda and the Worker it
 * answers GET only. Request bodies are never read.
 */
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { Hono } from "hono";
import { errorFields, log } from "../shared/log.ts";
import { readAppFromSecret } from "./lambda.ts";
import { readApiServiceConfig } from "./service-config.ts";

type FetchApp = { fetch(req: Request): Response | Promise<Response> };
export type ReadAppBuilder = (secret: string, env: NodeJS.ProcessEnv) => Promise<{ app: FetchApp; close(): Promise<void> }>;

const errorJson = (code: string, message: string) => ({ error: { code, message } });

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
    if (c.req.method !== "GET")
      return c.json(errorJson("method_not_allowed", "GET only"), 405, { allow: "GET" });
    return app.fetch(c.req.raw);
  });
  return service;
}

/** node:http in front of a fetch handler; the request body is drained, never read. */
export function readApiHttpServer(handler: FetchApp): Server {
  return createServer((req, res) => {
    void (async () => {
      try {
        req.resume();
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined) continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        const request = new Request(`http://localhost${req.url?.startsWith("/") ? req.url : "/"}`, { method: req.method, headers });
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
  const built = config.enabled ? await (options.build ?? readAppFromSecret)(config.secret, env) : null;
  const server = readApiHttpServer(readApiServiceApp(built?.app ?? null));
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
