/**
 * SERVICE=acquisition: the acquisition exchange's verify-and-forward
 * operations port. Verify-only: nothing here mints, signs or stores
 * acquisition credentials; relaycontrol is the sole issuer. Default off
 * (ACQUISITION_ENABLED); see config.ts and docs/acquisition-exchange.md.
 * Two listeners: ACQUISITION_OPS_PORT serves only the ops routes, bound to
 * ACQUISITION_OPS_HOST (default 127.0.0.1); ACQUISITION_PORT is always a
 * dormant listener (/healthz, every other route 503 acquisition_disabled).
 */
import { createServer, type IncomingMessage, type Server } from "node:http";
import pg from "pg";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { errorFields, log } from "../shared/log.ts";
import { createAcquisitionApp, type AcquisitionGroup } from "./app.ts";
import { acquisitionConfig } from "./config.ts";
import { assertInstallerLogin } from "./db-login.ts";
import { AcquisitionSql, Installer, pgAcquisitionExecutor } from "./installer.ts";
import { registryOfClass } from "./key-registry.ts";
import { createAcquisitionVerifier } from "./verifier.ts";

/** The dormant listener: all interfaces; ops binds config.opsHost. */
export const NATIVE_HOST = "0.0.0.0";

function bodyStream(req: IncomingMessage): ReadableStream<Uint8Array> {
  const iterator = req[Symbol.asyncIterator]();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const next = await iterator.next();
      if (next.done) controller.close();
      else controller.enqueue(new Uint8Array(next.value as Buffer));
    },
    cancel() {
      req.resume();
    },
  });
}

function listen(app: { fetch: (request: Request) => Response | Promise<Response> }, port: number, host: string, group: AcquisitionGroup, state: string): Promise<Server> {
  const server = createServer((req, res) => {
    void (async () => {
      try {
        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined) continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        const hasBody = req.method !== "GET" && req.method !== "HEAD";
        const request = new Request(`http://localhost${req.url?.startsWith("/") ? req.url : "/"}`, {
          method: req.method,
          headers,
          ...(hasBody ? { body: bodyStream(req), duplex: "half" } : {}),
        } as RequestInit);
        if (!hasBody) req.resume();
        const response = await app.fetch(request);
        const out: Record<string, string> = {};
        response.headers.forEach((v, k) => (out[k] = v));
        res.writeHead(response.status, out);
        if (response.body)
          for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) res.write(Buffer.from(chunk));
        res.end();
      } catch (err) {
        log.error("acquisition request crashed", { kind: group, ...errorFields(err) });
        if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { code: "internal_error" } }));
      }
    })();
  });
  return new Promise<Server>((resolve) => server.listen(port, host, () => {
    log.info("acquisition listening", { port, kind: group, state });
    resolve(server);
  }));
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const config = acquisitionConfig(env);
  const servers: Server[] = [];
  let pool: pg.Pool | null = null;
  if (!config.enabled) {
    servers.push(await listen(createAcquisitionApp({ enabled: false, group: "native" }), config.port, NATIVE_HOST, "native", "dormant"));
    servers.push(await listen(createAcquisitionApp({ enabled: false, group: "ops" }), config.opsPort, config.opsHost, "ops", "dormant"));
  } else {
    // Discrete pinned fields only (db-login.ts): never a connectionString, so no URL parameter,
    // PG* variable or .pgpass can change TLS, the login or the role.
    pool = new pg.Pool({ ...config.database, max: 5 });
    pool.on("error", (err) => log.warn("acquisition pool error", errorFields(err)));
    // The ops listener only starts (and reports healthy) once the live session is exactly the
    // installer login with the expected role shape; otherwise startup fails.
    if (config.opsEnabled) {
      try {
        await assertInstallerLogin(pool);
      } catch (err) {
        await pool.end().catch(() => undefined);
        throw err;
      }
    }
    const sql = new AcquisitionSql(pgAcquisitionExecutor(pool));
    const authenticator = createAcquisitionVerifier({
      url: config.introspectUrl,
      secret: config.introspectSecret,
      origin: config.origin,
      validateUrl: (url) => assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, destinationPolicy(env)),
    });
    // Cloud-class keys only on the ops port. parsePinnedKeys already refuses native-class
    // entries at startup; registryOfClass stays as a defensive filter.
    const installer = config.opsEnabled ? new Installer(sql, registryOfClass(config.registry, "cloud")) : null;
    servers.push(await listen(createAcquisitionApp({ enabled: false, group: "native" }), config.port, NATIVE_HOST, "native", "dormant"));
    servers.push(await listen(createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: config.opsEnabled, authenticator, installer }), config.opsPort, config.opsHost, "ops", config.opsEnabled ? "enabled" : "dormant"));
  }
  const stop = () => {
    let open = servers.length;
    for (const s of servers) s.close(() => { if (--open === 0) void (pool?.end() ?? Promise.resolve()).finally(() => process.exit(0)); });
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
