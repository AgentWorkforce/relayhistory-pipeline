/**
 * The self-host SERVICE entry (server.ts) over real loopback HTTP on a random
 * port: dormant answers /healthz only; enabled serves the read API, both with
 * the test adapters and with the Lambda's own assembly (no database is touched:
 * pg and ClickHouse connect lazily and the routes hit here never query).
 */
import { connect } from "node:net";
import { afterEach, describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { startReadApiService, type ReadApiService } from "../server.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const LOOPBACK = { host: "127.0.0.1", port: 0 };

const running: ReadApiService[] = [];
afterEach(async () => {
  for (const s of running.splice(0)) await s.close();
});

async function start(env: NodeJS.ProcessEnv, build?: Parameters<typeof startReadApiService>[1]["build"]) {
  const service = await startReadApiService(env, { ...LOOPBACK, build });
  running.push(service);
  const url = (path: string) => `http://127.0.0.1:${service.port}${path}`;
  return { service, url };
}

/** Raw HTTP/1.1 over a socket: full control over Content-Length vs chunked and over how much body is sent. */
function rawRequest(port: number, head: string, body: string | Buffer = ""): Promise<{ status: number; headers: string; text: string }> {
  return new Promise((resolve, reject) => {
    const socket = connect(port, "127.0.0.1");
    const chunks: Buffer[] = [];
    let done = false;
    const parse = () => {
      const raw = Buffer.concat(chunks).toString("latin1");
      const end = raw.indexOf("\r\n\r\n");
      const headers = end < 0 ? raw : raw.slice(0, end);
      const text = end < 0 ? "" : raw.slice(end + 4);
      return {
        status: Number(headers.split(" ")[1]),
        headers: headers.toLowerCase(),
        text,
        complete:
          end >= 0 &&
          (/transfer-encoding: *chunked/i.test(headers) ? text.endsWith("0\r\n\r\n") : text.length >= Number(/content-length: *(\d+)/i.exec(headers)?.[1] ?? Infinity)),
      };
    };
    const finish = () => {
      if (done) return;
      done = true;
      const { complete: _, ...r } = parse();
      socket.destroy();
      resolve(r);
    };
    socket.on("data", (c) => {
      chunks.push(c);
      if (parse().complete) finish();
    });
    socket.on("error", (err) => (chunks.length ? finish() : reject(err)));
    socket.on("close", finish);
    socket.write(head);
    if (body.length) socket.write(body);
    // Never finish the upload: a server that waits for the whole body would hang here.
    setTimeout(() => socket.destroy(), 2_000).unref();
  });
}

type Seen = { method: string; path: string; search: string; contentType: string | null; body: Uint8Array };

/** A stub app (behind the real service entry) that records every request it is handed. */
function recordingBuild(seen: Seen[]): NonNullable<Parameters<typeof startReadApiService>[1]["build"]> {
  return async () => ({
    app: {
      async fetch(req: Request) {
        const u = new URL(req.url);
        seen.push({ method: req.method, path: u.pathname, search: u.search, contentType: req.headers.get("content-type"), body: new Uint8Array(await req.arrayBuffer()) });
        return Response.json({ ok: true });
      },
    },
    close: async () => {},
  });
}

function stubCh(): ClickHouseClient {
  return {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>() {
      return [] as T[];
    },
  };
}

describe("dormant (flag unset, empty or false)", () => {
  test.each([undefined, "", "false"])("%p: /healthz 200 enabled:false, every other route 503 not_configured", async (flag) => {
    let built = false;
    const { service, url } = await start({ RELAYHISTORY_SELFHOST_READ_API: flag }, async () => {
      built = true;
      throw new Error("must not build");
    });
    expect(service.enabled).toBe(false);
    expect(built).toBe(false);
    const health = await fetch(url("/healthz"));
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true, enabled: false });
    for (const [path, method] of [["/v1/events?q=x", "GET"], ["/health", "GET"], ["/", "GET"], ["/v1/sessions", "POST"]] as const) {
      const r = await fetch(url(path), { method, headers: { authorization: "Bearer good" } });
      expect(r.status).toBe(503);
      expect(r.headers.get("retry-after")).toBe("60");
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe("not_configured");
    }
  });

  test("POST /v1/cli/login is 503 without the body being read or awaited", async () => {
    const { service } = await start({});
    // Declares 100000 bytes, sends 10 and never finishes: the 503 must still arrive.
    const r = await rawRequest(
      service.port,
      "POST /v1/cli/login HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: 100000\r\n\r\n",
      '{"agentRe',
    );
    expect(r.status).toBe(503);
    expect(r.text).toContain("not_configured");
  });

  test("a bad flag value refuses to start", async () => {
    await expect(startReadApiService({ RELAYHISTORY_SELFHOST_READ_API: "yes" }, LOOPBACK)).rejects.toThrow(
      "RELAYHISTORY_SELFHOST_READ_API must be true or false",
    );
  });
});

describe("enabled", () => {
  const env = { RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: "{}" };

  test("serves the read API through the server entry with the test adapters", async () => {
    let closed = false;
    const { service, url } = await start(env, async (secret) => {
      expect(secret).toBe("{}");
      const app = createReadApi({
        verify: async (t) => {
          if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
          return ID;
        },
        ch: stubCh(),
        liveness: memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } }).liveness,
        completeSinceMs: 0,
      });
      return { app, close: async () => void (closed = true) };
    });
    expect(service.enabled).toBe(true);

    const health = await fetch(url("/healthz"));
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true, enabled: true });

    const ok = await fetch(url("/v1/sessions/s-1/events?workspace=ws-a"), { headers: { authorization: "Bearer good" } });
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-sessions-read-backend")).toBe("96bulls");
    expect(ok.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(((await ok.json()) as { events: unknown[] }).events).toEqual([]);

    const denied = await fetch(url("/v1/sessions/s-1/events?workspace=ws-a"), { headers: { authorization: "Bearer bad" } });
    expect(denied.status).toBe(401);
    expect(((await denied.json()) as { error: { code: string } }).error.code).toBe("invalid_token");

    // GET only (bar POST /v1/cli/login), like the Lambda and the Worker.
    const post = await fetch(url("/v1/sessions/s-1/events?workspace=ws-a"), { method: "POST", body: "x", headers: { authorization: "Bearer good" } });
    expect(post.status).toBe(405);
    expect(post.headers.get("allow")).toBe("GET");

    await service.close();
    running.splice(0);
    expect(closed).toBe(true);
  });

  test("POST /v1/cli/login reaches the app with the exact body bytes and content-type", async () => {
    const seen: Seen[] = [];
    const { url } = await start(env, recordingBuild(seen));
    const body = JSON.stringify({ agentRelayToken: "tok-\u00e9\u2713", label: "laptop" });
    const r = await fetch(url("/v1/cli/login"), { method: "POST", body, headers: { "content-type": "application/json; charset=utf-8" } });
    expect(r.status).toBe(200);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.method).toBe("POST");
    expect(seen[0]!.path).toBe("/v1/cli/login");
    expect(seen[0]!.contentType).toBe("application/json; charset=utf-8");
    expect(Buffer.from(seen[0]!.body).equals(Buffer.from(body, "utf8"))).toBe(true);

    // Exactly at the cap is accepted.
    const atCap = Buffer.alloc(8 * 1024, 0x61);
    expect((await fetch(url("/v1/cli/login"), { method: "POST", body: atCap, headers: { "content-type": "application/json" } })).status).toBe(200);
    expect(seen).toHaveLength(2);
    expect(Buffer.from(seen[1]!.body).equals(atCap)).toBe(true);
  });

  test("POST /v1/cli/login?x=1 passes (query is not part of the path) with its body", async () => {
    const seen: Seen[] = [];
    const { url } = await start(env, recordingBuild(seen));
    const r = await fetch(url("/v1/cli/login?x=1"), { method: "POST", body: '{"agentRelayToken":"t"}', headers: { "content-type": "application/json" } });
    expect(r.status).toBe(200);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.path).toBe("/v1/cli/login");
    expect(seen[0]!.search).toBe("?x=1");
    expect(new TextDecoder().decode(seen[0]!.body)).toBe('{"agentRelayToken":"t"}');
  });

  test("POST anywhere else is 405 Allow GET and never reaches the app", async () => {
    const seen: Seen[] = [];
    const { url } = await start(env, recordingBuild(seen));
    for (const path of ["/v1/sessions/x/catalog", "/v1/cli/login/", "/v1/cli/login/x", "/v1/cli/loginx", "/v1/cli", "/healthz/", "/healthz/x", "/healthzx", "/v1/cli/%6cogin", "/V1/cli/login"]) {
      const r = await fetch(url(path), { method: "POST", body: "{}", headers: { "content-type": "application/json" } });
      expect([path, r.status]).toEqual([path, 405]);
      expect([path, r.headers.get("allow")]).toEqual([path, "GET"]);
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe("method_not_allowed");
    }
    expect(seen).toEqual([]);
  });

  test("PUT, DELETE and PATCH on /v1/cli/login are 405 Allow GET, POST", async () => {
    const seen: Seen[] = [];
    const { url } = await start(env, recordingBuild(seen));
    for (const method of ["PUT", "DELETE", "PATCH"]) {
      const r = await fetch(url("/v1/cli/login"), { method, body: method === "DELETE" ? undefined : "{}" });
      expect([method, r.status]).toEqual([method, 405]);
      expect([method, r.headers.get("allow")]).toEqual([method, "GET, POST"]);
      expect(((await r.json()) as { error: { code: string } }).error.code).toBe("method_not_allowed");
    }
    expect(seen).toEqual([]);
  });

  test("an over-cap login body is 413 body_too_large and the app is never called (Content-Length and chunked)", async () => {
    const seen: Seen[] = [];
    const { service, url } = await start(env, recordingBuild(seen));

    // Declared over the cap: answered from the header alone, before the (unfinished) upload.
    const declared = await rawRequest(
      service.port,
      `POST /v1/cli/login HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: ${8 * 1024 + 1}\r\n\r\n`,
      "{",
    );
    expect(declared.status).toBe(413);
    expect(declared.text).toContain("body_too_large");
    expect(declared.headers).toContain("connection: close");

    // Chunked, no Content-Length: counted while reading; the upload is never finished.
    const chunk = Buffer.alloc(4 * 1024, 0x61);
    const frame = Buffer.concat([Buffer.from(`${chunk.length.toString(16)}\r\n`), chunk, Buffer.from("\r\n")]);
    const chunked = await rawRequest(
      service.port,
      "POST /v1/cli/login HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
      Buffer.concat([frame, frame, frame]),
    );
    expect(chunked.status).toBe(413);
    expect(chunked.text).toContain("body_too_large");

    // The same via fetch with a body one byte over the cap.
    const over = await fetch(url("/v1/cli/login"), { method: "POST", body: Buffer.alloc(8 * 1024 + 1, 0x61), headers: { "content-type": "application/json" } });
    expect(over.status).toBe(413);
    expect(((await over.json()) as { error: { code: string } }).error.code).toBe("body_too_large");

    expect(seen).toEqual([]);
    // The service still serves after closing those connections.
    expect((await fetch(url("/healthz"))).status).toBe(200);
  });

  test("the Lambda's own assembly starts without a database and authenticates first", async () => {
    const { url } = await start({
      ...env,
      RELAYHISTORY_READ_API_SECRET: JSON.stringify({
        supabase_url: "postgresql://reader@db.example.invalid:5432/postgres",
        clickhouse: { url: "https://ch.example.invalid:8443", user: "reader", password: "p", database: "relay_search" },
      }),
      READ_API_COMPLETE_SINCE: "epoch",
    });
    expect(await (await fetch(url("/healthz"))).json()).toEqual({ ok: true, enabled: true });
    expect((await fetch(url("/health"))).status).toBe(200);
    const r = await fetch(url("/v1/events?q=x"));
    expect(r.status).toBe(401);
    expect(((await r.json()) as { error: { code: string } }).error.code).toBe("missing_authorization");
  });

  test("startup fails on the Lambda's own checks (bad secret, missing history floor)", async () => {
    await expect(startReadApiService({ ...env, READ_API_COMPLETE_SINCE: "epoch", RELAYHISTORY_READ_API_SECRET: "not json" }, LOOPBACK)).rejects.toThrow(
      "session read api secret is not JSON",
    );
    await expect(
      startReadApiService({ ...env, RELAYHISTORY_READ_API_SECRET: JSON.stringify({ supabase_url: "postgresql://r@h/p" }) }, LOOPBACK),
    ).rejects.toThrow("READ_API_COMPLETE_SINCE");
  });
});
