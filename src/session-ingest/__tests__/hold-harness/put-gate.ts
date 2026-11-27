/**
 * A loopback HTTP proxy in front of an S3-compatible store (MinIO) that can pause one specific request and deliver it
 * later, so a test can land an old PUT after a later read or a later writer: the "delayed PUT after a timeout" and
 * "paused marker PUT" interleavings of docs/design/org-admission-hold.md.
 *
 *   const gate = await startPutGate({ upstream: minio.endpoint });
 *   const store = createS3Store({ endpoint: gate.url, ... });         // the code under test, unchanged
 *   const held = gate.hold({ method: "PUT", key: "org/marker.json" }); // arm: the next matching request is held
 *   const put = store.put(key, body, { ifNoneMatch: true });           // reaches the gate, is NOT forwarded
 *   await held.arrived;                                                // buffered: method, path, headers, body
 *   held.abandon();                                                    // optional: the client sees a dropped socket
 *   ...later reads and writes pass straight through...
 *   const late = await held.deliver();                                 // forwarded now; MinIO's status returned
 *
 * Everything not matched by an armed hold passes through unchanged, including the Host header, so SigV4 signatures
 * the client computed for the gate's address still verify upstream (MinIO checks the received Host). A held request is
 * forwarded byte for byte whenever `deliver` is called, whether or not its client is still waiting: that is the point.
 * Each hold matches exactly one request. The gate only listens on 127.0.0.1 and only forwards to a loopback upstream.
 *
 * Every response carries `Connection: close`, so each client request arrives on a fresh connection. That matters for
 * `abandon`: fetch (Bun's, undici's) silently re-sends an idempotent request whose REUSED keep-alive socket is reset,
 * which would turn a dropped PUT into a second, fresh one; on a fresh socket the reset fails the client instead.
 * `log` records every arrival, so a test can assert that no retry reached the store.
 */
import http from "node:http";
import type { AddressInfo } from "node:net";

export interface HeldRequest {
  method: string;
  /** request path as sent (path-style: /bucket/key), with query */
  path: string;
  /** the object key: the path after /bucket/, URL-decoded, without query */
  key: string;
  headers: http.IncomingHttpHeaders;
  body: Uint8Array;
}

export interface UpstreamResponse { status: number; headers: http.IncomingHttpHeaders; body: string }

export interface Hold {
  /** resolves when a matching request has arrived and its body is buffered (it has not been forwarded) */
  readonly arrived: Promise<HeldRequest>;
  /** forward the held request now (once); resolves with the upstream's response, also relayed to a waiting client */
  deliver(): Promise<UpstreamResponse>;
  /** drop the client's socket without forwarding (its fetch fails like a timeout); `deliver` still works afterwards */
  abandon(): void;
  /** answer the client with `status` without forwarding; `deliver` still works afterwards */
  respond(status: number, body?: string): void;
  /** disarm a hold that never matched */
  cancel(): void;
}

export interface Match {
  method?: string;
  /** exact object key, or a predicate on it */
  key?: string | ((key: string) => boolean);
  /** a predicate on the whole request, applied after method and key */
  where?: (r: Omit<HeldRequest, "body">) => boolean;
}

export interface PutGate {
  /** the endpoint to hand to createS3Store (http://127.0.0.1:<port>) */
  readonly url: string;
  hold(match: Match): Hold;
  /** every request seen, in arrival order (method, key, and the upstream status once forwarded) */
  readonly log: Array<{ method: string; key: string; held: boolean; status?: number }>;
  close(): Promise<void>;
}

const LOOPBACK = ["127.0.0.1", "localhost", "[::1]"];
const HOP = new Set(["connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "te", "trailer"]);

function keyOf(path: string): string {
  const p = path.split("?")[0]!;
  const rest = p.replace(/^\/[^/]*\/?/, "");
  return rest.split("/").map(decodeURIComponent).join("/");
}

function forward(upstream: URL, r: HeldRequest): Promise<UpstreamResponse> {
  const headers: http.OutgoingHttpHeaders = {};
  for (const [k, v] of Object.entries(r.headers)) if (v !== undefined && !HOP.has(k)) headers[k] = v;
  headers["content-length"] = String(r.body.byteLength);
  return new Promise((resolve, reject) => {
    const req = http.request({ host: upstream.hostname.replace(/^\[|\]$/g, ""), port: upstream.port || 80, method: r.method, path: r.path, headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (c: Buffer) => chunks.push(c));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks).toString("utf8") }));
      res.on("error", reject);
    });
    req.on("error", reject);
    req.end(r.body);
  });
}

function relay(res: http.ServerResponse, up: UpstreamResponse) {
  if (res.headersSent || res.destroyed || res.writableEnded) return;
  const headers: http.OutgoingHttpHeaders = {};
  for (const [k, v] of Object.entries(up.headers)) if (v !== undefined && !HOP.has(k) && k !== "content-length") headers[k] = v;
  headers.connection = "close";
  res.writeHead(up.status, headers);
  res.end(up.body);
}

export async function startPutGate(opts: { upstream: string }): Promise<PutGate> {
  const upstream = new URL(opts.upstream);
  if (upstream.protocol !== "http:" || !LOOPBACK.includes(upstream.hostname) || upstream.username || upstream.password)
    throw Error("put_gate_loopback_upstream_required");
  const armed: Array<{ match: Match; take: (r: HeldRequest, res: http.ServerResponse) => void; reject: (e: Error) => void }> = [];
  const log: PutGate["log"] = [];
  const sockets = new Set<import("node:net").Socket>();

  const matches = (m: Match, r: Omit<HeldRequest, "body">) =>
    (!m.method || m.method.toUpperCase() === r.method) &&
    (m.key === undefined || (typeof m.key === "string" ? m.key === r.key : m.key(r.key))) &&
    (!m.where || m.where(r));

  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const r: HeldRequest = { method: req.method ?? "GET", path: req.url ?? "/", key: keyOf(req.url ?? "/"), headers: req.headers, body: new Uint8Array(Buffer.concat(chunks)) };
      const i = armed.findIndex((a) => matches(a.match, r));
      const entry: PutGate["log"][number] = { method: r.method, key: r.key, held: i >= 0 };
      log.push(entry);
      if (i >= 0) { const [a] = armed.splice(i, 1); a!.take(r, res); return; }
      forward(upstream, r).then((up) => { entry.status = up.status; relay(res, up); },
        () => { if (!res.headersSent) { res.writeHead(502, { connection: "close" }); res.end(); } });
    });
  });
  server.on("connection", (s) => { sockets.add(s); s.on("close", () => sockets.delete(s)); });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}`,
    log,
    hold(match) {
      let req: HeldRequest | undefined, res: http.ServerResponse | undefined, delivered: Promise<UpstreamResponse> | undefined;
      let resolveArrived!: (r: HeldRequest) => void, rejectArrived!: (e: Error) => void;
      const arrived = new Promise<HeldRequest>((y, n) => { resolveArrived = y; rejectArrived = n; });
      arrived.catch(() => {});
      const slot = { match, take: (r: HeldRequest, s: http.ServerResponse) => { req = r; res = s; resolveArrived(r); }, reject: rejectArrived };
      armed.push(slot);
      const need = () => { if (!req) throw Error("put_gate_nothing_held"); return req; };
      return {
        arrived,
        deliver() {
          const r = need();
          delivered ??= forward(upstream, r).then((up) => {
            const e = [...log].reverse().find((x) => x.held && x.key === r.key && x.status === undefined);
            if (e) e.status = up.status;
            if (res) relay(res, up);
            return up;
          });
          return delivered;
        },
        abandon() { need(); res?.destroy(); res = undefined; },
        respond(status, body = "") { need(); if (res && !res.headersSent) { res.writeHead(status, { connection: "close" }); res.end(body); } res = undefined; },
        cancel() { const i = armed.indexOf(slot); if (i >= 0) armed.splice(i, 1); rejectArrived(Error("put_gate_hold_cancelled")); },
      };
    },
    async close() {
      for (const a of armed.splice(0)) a.reject(Error("put_gate_closed"));
      for (const s of sockets) s.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
