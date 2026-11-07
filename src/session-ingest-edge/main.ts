/**
 * SERVICE=session-ingest-edge: the self-host Desktop-upload edge in front of
 * the session-ingest Worker. Default off (SESSION_INGEST_EDGE_ENABLED); see
 * config.ts and docs/session-ingest-edge.md.
 *
 * The request body is streamed into the app (not buffered here), so the app's
 * MAX_DELIVERY_BYTES bound is the only buffer, and only after authentication.
 */
import { createServer, type IncomingMessage } from "node:http";
import { createRelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { errorFields, log } from "../shared/log.ts";
import { createIngestEdgeApp } from "./app.ts";
import { edgeConfig } from "./config.ts";

/** The request body as a web stream; cancelling drains the rest so the response can still be written. */
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

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const config = edgeConfig(env);
  const app = config.enabled
    ? createIngestEdgeApp({
        enabled: true,
        origin: config.origin,
        upstreamUrl: config.upstreamUrl,
        signingKey: config.signingKey,
        introspector: createRelaycontrolIntrospector({
          url: config.introspectUrl,
          secret: config.introspectSecret,
          validateUrl: (url) => assertDestinationUrl("RELAYCONTROL_INTROSPECT_URL", url, destinationPolicy(env)),
        }),
      })
    : createIngestEdgeApp({ enabled: false });

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
        log.error("session ingest edge request crashed", errorFields(err));
        if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { code: "internal_error" } }));
      }
    })();
  });
  await new Promise<void>((resolve) => server.listen(config.port, "0.0.0.0", resolve));
  log.info("session ingest edge listening", { port: config.port, state: config.enabled ? "enabled" : "dormant" });
  const stop = () => server.close(() => process.exit(0));
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

if (import.meta.main) await serve();
