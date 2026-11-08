/** Offline fakes for the portable publication runtime (test-only).
 *
 * pgliteTestPool: a pg.Pool-shaped port over one PGlite instance. Connections are
 *   serialized (PGlite has one backend). Each acquired client runs as the given
 *   role via SET SESSION AUTHORIZATION, so current_user AND session_user are that
 *   role, as a real LOGIN would be; release rolls back and returns to postgres
 *   (PGlite single-user mode does not honour RESET SESSION AUTHORIZATION).
 * MinioShapedS3: a path-style S3-compatible fetch (PUT with If-None-Match / If-Match, GET,
 *   HEAD) that only answers its own bucket and refuses any request whose SigV4
 *   Credential scope is not exactly its access key and region.
 */
import pg from "pg";
import { createHash } from "node:crypto";
import type { PGlite } from "@electric-sql/pglite";

export function pgliteTestPool(db: PGlite, role: string, options: pg.PoolConfig = {}) {
  if (!/^[a-z0-9_]{1,63}$/.test(role)) throw new Error("fixture role");
  let lock: Promise<void> = Promise.resolve();
  let ended = false;
  const releasing = new Set<Promise<void>>();
  const acquire = async () => {
    let release!: () => void;
    const prior = lock;
    lock = new Promise<void>((r) => (release = r));
    await prior;
    return release;
  };
  const pool: any = {
    options: { connectionTimeoutMillis: 1000, ...options },
    ended: () => ended,
    on() { return pool; },
    async end() { ended = true; await Promise.all(releasing); await lock; },
    connect(cb: (e: Error | undefined, c?: any) => void) {
      if (ended) return void cb(new Error("pool ended"));
      acquire().then(async (unlock) => {
        await db.exec(`SET SESSION AUTHORIZATION ${role}`);
        const listeners = new Map<string, Function>();
        const client: any = {
          on(name: string, f: Function) { listeners.set(name, f); return client; },
          removeListener(name: string) { listeners.delete(name); return client; },
          query(q: pg.Query) {
            const { text, values } = q as unknown as { text: string; values?: unknown[] };
            (async () => {
              try {
                let rows: unknown[];
                if (values && values.length) rows = (await db.query(text, values as never)).rows;
                else { const all = await db.exec(text); rows = all.at(-1)?.rows ?? []; }
                for (const row of rows) q.emit("row", row);
                q.emit("end");
              } catch (error) { q.emit("error", error); }
            })();
          },
          release() {
            const done = (async () => {
              try { await db.exec("ROLLBACK"); } catch {}
              await db.exec("SET SESSION AUTHORIZATION postgres");
            })().finally(() => { releasing.delete(done); unlock(); });
            releasing.add(done);
          },
        };
        cb(undefined, client);
      }, (e) => cb(e));
    },
  };
  return pool as pg.Pool;
}

interface StoredObject { body: Uint8Array; etag: string; version: string; metadata: Record<string, string>; contentType: string }

export class MinioShapedS3 {
  readonly objects = new Map<string, StoredObject>();
  readonly requests: { method: string; key: string; host: string; scope: string }[] = [];
  #n = 0;
  constructor(readonly origin: string, readonly bucket: string, readonly region: string, readonly accessKeyId: string) {}
  readonly fetch = (async (input: Parameters<typeof fetch>[0], init: RequestInit = {}) => {
    const url = new URL(String(input instanceof Request ? input.url : input));
    const method = (init.method ?? "GET").toUpperCase();
    const headers = new Headers(init.headers);
    const auth = headers.get("authorization") ?? "";
    const scope = /Credential=([^,]+)/.exec(auth)?.[1] ?? "";
    const [, bucket, ...rest] = url.pathname.split("/");
    const key = rest.map(decodeURIComponent).join("/");
    this.requests.push({ method, key, host: url.host, scope });
    if (url.origin !== this.origin) return new Response("wrong endpoint", { status: 421 });
    if (!scope.startsWith(`${this.accessKeyId}/`) || !scope.endsWith(`/${this.region}/s3/aws4_request`) || !headers.get("x-amz-content-sha256"))
      return new Response("<Error><Code>SignatureDoesNotMatch</Code></Error>", { status: 403 });
    if (bucket !== this.bucket) return new Response("<Error><Code>NoSuchBucket</Code></Error>", { status: 404 });
    if (!key || url.search) return new Response("<Error><Code>NotImplemented</Code></Error>", { status: 501 });
    const existing = this.objects.get(key);
    const respond = (o: StoredObject, body: boolean) => {
      const h = new Headers({ etag: o.etag, "x-amz-version-id": o.version, "content-length": String(o.body.byteLength), "content-type": o.contentType, "last-modified": new Date(0).toUTCString() });
      for (const [k, v] of Object.entries(o.metadata)) h.set(`x-amz-meta-${k}`, v);
      return new Response(body ? (o.body as Uint8Array<ArrayBuffer>) : null, { status: 200, headers: h });
    };
    if (method === "PUT") {
      if (headers.get("if-none-match") === "*" && existing) return new Response("<Error><Code>PreconditionFailed</Code></Error>", { status: 412 });
      const ifMatch = headers.get("if-match");
      if (ifMatch !== null && existing?.etag !== ifMatch) return new Response("<Error><Code>PreconditionFailed</Code></Error>", { status: 412 });
      if (headers.get("x-amz-server-side-encryption")) return new Response("<Error><Code>NotImplemented</Code></Error>", { status: 501 });
      const body = new Uint8Array(await new Response(init.body as BodyInit).arrayBuffer());
      const metadata: Record<string, string> = {};
      headers.forEach((v, k) => { if (k.startsWith("x-amz-meta-")) metadata[k.slice(11)] = v; });
      const o = { body, etag: `"${createHash("md5").update(body).digest("hex")}"`, version: `v${++this.#n}`, metadata, contentType: headers.get("content-type") ?? "application/octet-stream" };
      this.objects.set(key, o);
      return new Response(null, { status: 200, headers: { etag: o.etag, "x-amz-version-id": o.version } });
    }
    if (method === "GET" || method === "HEAD") {
      if (!existing) return new Response(method === "HEAD" ? null : "<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
      return respond(existing, method === "GET");
    }
    return new Response("<Error><Code>MethodNotAllowed</Code></Error>", { status: 405 });
  }) as typeof fetch;
}
