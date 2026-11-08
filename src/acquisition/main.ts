/**
 * SERVICE=acquisition: the acquisition exchange (native issuer + operations
 * port). Default off (ACQUISITION_ENABLED); see config.ts and
 * docs/acquisition-exchange.md. Two listeners: ACQUISITION_PORT serves only the
 * native routes (Desktop ingress), ACQUISITION_OPS_PORT serves only the ops
 * routes (private network only). Modelled on src/session-ingest-edge/main.ts.
 */
import { createServer, type IncomingMessage, type Server } from "node:http";
import pg from "pg";
import { createRelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { errorFields, log } from "../shared/log.ts";
import { createAcquisitionApp, relaycontrolAuthenticator, type AcquisitionGroup } from "./app.ts";
import { acquisitionConfig } from "./config.ts";
import { AcquisitionSql, Installer, pgAcquisitionExecutor } from "./installer.ts";
import { PgAcquisitionStore, pgSqlClient } from "./intent-store.ts";
import { assertKeysRetained, registryOfClass } from "./key-registry.ts";
import { NativeIssuer } from "./native-issuer.ts";

export const RECONCILE_INTERVAL_MS = 30_000;

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

function listen(app: { fetch: (request: Request) => Response | Promise<Response> }, port: number, group: AcquisitionGroup, state: string): Promise<Server> {
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
  return new Promise<Server>((resolve) => server.listen(port, "0.0.0.0", () => {
    log.info("acquisition listening", { port, kind: group, state });
    resolve(server);
  }));
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const config = acquisitionConfig(env);
  const servers: Server[] = [];
  let timer: ReturnType<typeof setInterval> | null = null;
  let pool: pg.Pool | null = null;
  if (!config.enabled) {
    servers.push(await listen(createAcquisitionApp({ enabled: false, group: "native" }), config.port, "native", "dormant"));
    servers.push(await listen(createAcquisitionApp({ enabled: false, group: "ops" }), config.opsPort, "ops", "dormant"));
  } else {
    pool = new pg.Pool({ connectionString: config.databaseUrl, max: 5 });
    pool.on("error", (err) => log.warn("acquisition pool error", errorFields(err)));
    const sql = new AcquisitionSql(pgAcquisitionExecutor(pool));
    const authenticator = relaycontrolAuthenticator(
      createRelaycontrolIntrospector({
        url: config.introspectUrl,
        secret: config.introspectSecret,
        validateUrl: (url) => assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, destinationPolicy(env)),
      }),
      config.origin,
    );
    const store = new PgAcquisitionStore(pgSqlClient(pool));
    const issuer = config.nativeEnabled && config.signer && config.intentKey
      ? new NativeIssuer({ store, sql, signer: config.signer, intentKey: config.intentKey })
      : null;
    // §9 key retention: refuse to start if a rotation dropped a native key that a
    // reserved revoke or pending intent still carries (fail closed, before listening).
    if (issuer) assertKeysRetained(config.registry, await store.outstandingIssuerKeyIds(), "native");
    // Cloud-class keys only on the ops port; a native-class key is refused there.
    const installer = config.opsEnabled ? new Installer(sql, registryOfClass(config.registry, "cloud")) : null;
    servers.push(await listen(createAcquisitionApp({ enabled: true, group: "native", groupEnabled: config.nativeEnabled, authenticator, issuer }), config.port, "native", config.nativeEnabled ? "enabled" : "dormant"));
    servers.push(await listen(createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: config.opsEnabled, authenticator, installer }), config.opsPort, "ops", config.opsEnabled ? "enabled" : "dormant"));
    if (issuer) {
      let running = false;
      timer = setInterval(() => {
        if (running) return;
        running = true;
        issuer.reconcile().then(
          (r) => { if (r.sent || r.retry) log.info("acquisition reconcile", { count: r.sent, reason: `retry=${r.retry}` }); },
          (err) => log.warn("acquisition reconcile failed", errorFields(err)),
        ).finally(() => { running = false; });
      }, RECONCILE_INTERVAL_MS);
    }
  }
  const stop = () => {
    if (timer) clearInterval(timer);
    let open = servers.length;
    for (const s of servers) s.close(() => { if (--open === 0) void (pool?.end() ?? Promise.resolve()).finally(() => process.exit(0)); });
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
