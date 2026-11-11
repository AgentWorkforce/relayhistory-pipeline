/**
 * Operator per-session erase, PR 1: the 501 stub route (session-erase.ts; docs/operator-session-erase-contract.md).
 * Default off; with the flag and the secret set the request is authenticated and validated, and still 501.
 * No database: the self-host case runs the real node:http wrapper over a runtime whose SQL and
 * introspection throw if they are ever reached.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { createIngestApp } from "../app.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import { selfhostIngestHttpServer, selfhostIngestRuntime } from "../selfhost-server.ts";
import {
  NOT_IMPLEMENTED_BODY, SESSION_ERASE_CONFIG_INVALID, SESSION_ERASE_MAX_BODY_BYTES, UNAVAILABLE_BODY, parseSessionErasePath, sessionEraseConfig, type SessionEraseConfig,
} from "../session-erase.ts";
import { ingestSettings } from "../worker.ts";
import { MemoryLedger, scratchStore } from "./support.ts";

const SECRET = "erase-secret-current-0123456789abcdef";
const PREVIOUS = "erase-secret-previous-0123456789abcdef";
const PATH = "/internal/v1/session-erase/organizations/org-1/workspaces/ws-1/sources/claude/sessions/sess-1";
const BODY = JSON.stringify({ requestId: "req-1", actor: { kind: "user", id: "user-1" } });

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0)) await f();
});

function app(config: SessionEraseConfig | undefined) {
  const lines: Record<string, unknown>[] = [];
  const s = scratchStore();
  cleanups.push(s.done);
  const a = createIngestApp({
    identify: () => { throw new Error("identity must never be asked for on the erase route"); },
    deliver: { ledger: new MemoryLedger(), target: { store: s.store, keyRoot: "96bulls" } },
    log: (fields) => lines.push(fields),
    ...(config ? { sessionErase: config } : {}),
  });
  return { a, lines };
}

const enabled = (env: Record<string, string> = {}) =>
  sessionEraseConfig({ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, ...env });
const auth = (token = SECRET) => ({ authorization: `Bearer ${token}` });
const post = (a: ReturnType<typeof app>["a"], body: string | Uint8Array = BODY, headers: Record<string, string> = auth(), path = PATH) =>
  a.request(path, { method: "POST", headers: { "content-type": "application/json", ...headers }, body });

describe("disabled (default)", () => {
  test("flag off: POST and GET are 501 with the documented body, and nothing is read", async () => {
    for (const config of [undefined, sessionEraseConfig({}), sessionEraseConfig({ SESSION_ERASE_API_ENABLED: "0", SESSION_ERASE_API_SECRET: "short" })]) {
      const { a, lines } = app(config);
      const p = await post(a, "not json", {});
      expect([p.status, await p.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
      const g = await a.request(PATH);
      expect([g.status, await g.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
      expect(lines.map((l) => l.outcome)).toEqual(["flag_off", "flag_off"]);
    }
  });

  test("flag on without a secret: 501, even with a bearer", async () => {
    const config = sessionEraseConfig({ SESSION_ERASE_API_ENABLED: "1" });
    expect(config).toEqual({ enabled: false, reason: "secret_unset" });
    const { a, lines } = app(config);
    const p = await post(a);
    expect([p.status, await p.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
    expect(lines[0]).toMatchObject({ route: "operator.session_erase", status: 501, outcome: "secret_unset" });
  });

  test("config_invalid (hosted Worker, invalid flag or secret): 503 unavailable with Retry-After, before auth", async () => {
    const { a, lines } = app(SESSION_ERASE_CONFIG_INVALID);
    for (const res of [await post(a), await a.request(PATH), await a.request(PATH, { method: "DELETE" })]) {
      expect([res.status, await res.json(), res.headers.get("retry-after")]).toEqual([503, UNAVAILABLE_BODY, "60"]);
    }
    expect(lines.map((l) => [l.status, l.outcome])).toEqual([[503, "config_invalid"], [503, "config_invalid"], [503, "config_invalid"]]);
    expect((await a.request("/healthz")).status).toBe(200);
  });

  test("the rest of the app is unchanged: /healthz and the /v1 fallbacks", async () => {
    const { a } = app(undefined);
    expect((await a.request("/healthz")).status).toBe(200);
    expect((await a.request("/internal/v1/other")).status).toBe(404);
  });
});

describe("enabled with a secret", () => {
  test("no bearer, a malformed bearer and a wrong bearer are 401 invalid_token", async () => {
    const { a } = app(enabled());
    for (const headers of [{}, { authorization: SECRET }, { authorization: `Basic ${SECRET}` }, auth(`${SECRET}x`), auth(PREVIOUS)]) {
      const res = await post(a, BODY, headers);
      expect([res.status, await res.json(), res.headers.get("www-authenticate")]).toEqual([401, { error: "invalid_token", feature: "session_erase" }, "Bearer"]);
    }
    const get = await a.request(PATH);
    expect(get.status).toBe(401);
  });

  test("auth runs before the path shape: an unauthenticated caller cannot probe it", async () => {
    const { a } = app(enabled());
    expect((await a.request("/internal/v1/session-erase/nope")).status).toBe(401);
    const res = await a.request("/internal/v1/session-erase/nope", { headers: auth() });
    expect([res.status, await res.json()]).toEqual([404, { error: "not_found", feature: "session_erase" }]);
  });

  test("the previous secret is accepted during a rotation", async () => {
    const { a } = app(enabled({ SESSION_ERASE_API_SECRET_PREVIOUS: PREVIOUS }));
    for (const token of [SECRET, PREVIOUS]) {
      const res = await post(a, BODY, auth(token));
      expect([res.status, await res.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
    }
  });

  test("an oversize body is 413, declared or streamed", async () => {
    const { a } = app(enabled());
    const big = JSON.stringify({ requestId: "r", actor: { kind: "user", id: "x".repeat(SESSION_ERASE_MAX_BODY_BYTES) } });
    const declared = await post(a, big);
    expect([declared.status, await declared.json()]).toEqual([413, { error: "body_too_large", feature: "session_erase" }]);
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        for (let i = 0; i < 3; i++) c.enqueue(new Uint8Array(4096).fill(0x20));
        c.close();
      },
    });
    const streamed = await a.request(PATH, { method: "POST", headers: auth(), body: stream, duplex: "half" } as RequestInit);
    expect(streamed.status).toBe(413);
  });

  test("a malformed POST is 400 invalid_request", async () => {
    const { a } = app(enabled());
    const bad = [
      "", "not json", "[]", "null",
      JSON.stringify({ requestId: "r" }),
      JSON.stringify({ requestId: "r", actor: { kind: "admin", id: "u" } }),
      JSON.stringify({ requestId: "r", actor: { kind: "user" } }),
      JSON.stringify({ requestId: "r", actor: { kind: "user", id: "u", extra: 1 } }),
      JSON.stringify({ requestId: "r", actor: { kind: "user", id: "u" }, orgId: "o" }),
      JSON.stringify({ requestId: "", actor: { kind: "user", id: "u" } }),
      JSON.stringify({ requestId: "r".repeat(129), actor: { kind: "user", id: "u" } }),
      JSON.stringify({ requestId: "r\n", actor: { kind: "bootstrap", id: "u" } }),
      JSON.stringify({ requestId: 1, actor: { kind: "user", id: "u" } }),
    ];
    for (const body of [...bad, new Uint8Array([0x7b, 0xff, 0x7d])]) {
      const res = await post(a, body);
      expect([String(body).slice(0, 40), res.status, await res.json()]).toEqual([String(body).slice(0, 40), 400, { error: "invalid_request", feature: "session_erase" }]);
    }
    const badId = await post(a, BODY, auth(), PATH.replace("sess-1", "%E0%A4%A"));
    expect(badId.status).toBe(400);
    const tab = await post(a, BODY, auth(), PATH.replace("sess-1", "a%09b"));
    expect(tab.status).toBe(400);
    // A decoded newline matches no Hono route at all (app-wide): the app's generic 404, never a 501.
    const nl = await post(a, BODY, auth(), PATH.replace("sess-1", "a%0Ab"));
    expect(nl.status).toBe(404);
  });

  test("the bootstrap bearer's actor {kind: bootstrap, id: bootstrap} is a valid actor", async () => {
    const { a } = app(enabled());
    const res = await post(a, JSON.stringify({ requestId: "req-1", actor: { kind: "bootstrap", id: "bootstrap" } }));
    expect([res.status, await res.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
  });

  test("a valid POST and a valid GET are 501 not_implemented", async () => {
    const { a, lines } = app(enabled());
    const p = await post(a, JSON.stringify({ requestId: "req-1", actor: { kind: "bootstrap", id: "boot-1" } }));
    expect([p.status, await p.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
    const g = await a.request(PATH, { headers: auth() });
    expect([g.status, await g.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
    expect(lines.map((l) => [l.kind, l.status, l.outcome])).toEqual([["POST", 501, "not_implemented"], ["GET", 501, "not_implemented"]]);
  });

  test("other methods are 405 with Allow: GET, POST", async () => {
    const { a } = app(enabled());
    const res = await a.request(PATH, { method: "DELETE", headers: auth() });
    expect([res.status, res.headers.get("allow")]).toEqual([405, "GET, POST"]);
  });

  test("the path parser decodes the erase key", () => {
    expect(parseSessionErasePath(PATH.replace("sess-1", "a%2Fb"))).toEqual({ orgId: "org-1", workspaceId: "ws-1", source: "claude", sessionId: "a/b" });
    expect(parseSessionErasePath("/internal/v1/session-erase/organizations/o/workspaces/w/sources/s/sessions")).toBeNull();
    expect(parseSessionErasePath(`${PATH}/extra`)).toBeNull();
  });
});

describe("configuration", () => {
  test("rejects a short, padded or reused secret and an unknown flag value; errors name the variable, never the value", () => {
    const cases: [Record<string, string>, RegExp][] = [
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: "too-short" }, /SESSION_ERASE_API_SECRET must be at least 32/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: ` ${SECRET}` }, /whitespace/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: `${SECRET}\n` }, /whitespace/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: `erase-secret ${SECRET}` }, /SESSION_ERASE_API_SECRET must not contain whitespace/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, SESSION_ERASE_API_SECRET_PREVIOUS: `erase-secret\t${PREVIOUS}` }, /SESSION_ERASE_API_SECRET_PREVIOUS must not contain whitespace/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, SESSION_ERASE_API_SECRET_PREVIOUS: "short" }, /SESSION_ERASE_API_SECRET_PREVIOUS must be at least 32/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, SESSION_ERASE_API_SECRET_PREVIOUS: SECRET }, /must differ/],
      [{ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET_PREVIOUS: PREVIOUS }, /needs SESSION_ERASE_API_SECRET/],
      [{ SESSION_ERASE_API_ENABLED: "true", SESSION_ERASE_API_SECRET: SECRET }, /must be unset, "0" or "1"/],
    ];
    for (const [env, message] of cases) {
      let error: unknown;
      try {
        sessionEraseConfig(env);
      } catch (e) {
        error = e;
      }
      expect(String(error)).toMatch(message);
      for (const v of Object.values(env)) if (v.length > 2) expect(String(error)).not.toContain(v.trim());
    }
  });

  test("the Worker settings resolve the secret only with the flag on; an invalid erase setting disables erase only", async () => {
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    expect((await ingestSettings({ ...full, SESSION_ERASE_API_SECRET: "short" }))?.sessionErase).toBeUndefined();
    expect((await ingestSettings({ ...full, SESSION_ERASE_API_ENABLED: "1" }))?.sessionErase).toEqual({ enabled: false, reason: "secret_unset" });
    expect((await ingestSettings({ ...full, SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET }))?.sessionErase?.enabled).toBe(true);
  });

  test("Worker: flag 1 with a short secret (or an unknown flag value) never rejects ingestSettings; erase is disabled, logged by code only", async () => {
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const shortSecret = "erase-short-secret-value";
    const written: string[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    process.stdout.write = ((chunk: unknown, ...rest: unknown[]) => (written.push(String(chunk)), (out as (...a: unknown[]) => boolean)(chunk, ...rest))) as typeof process.stdout.write;
    process.stderr.write = ((chunk: unknown, ...rest: unknown[]) => (written.push(String(chunk)), (err as (...a: unknown[]) => boolean)(chunk, ...rest))) as typeof process.stderr.write;
    const results = [];
    try {
      for (const env of [
        { SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: shortSecret },
        { SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: `${SECRET}\n` },
        { SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, SESSION_ERASE_API_SECRET_PREVIOUS: SECRET },
        { SESSION_ERASE_API_ENABLED: "true", SESSION_ERASE_API_SECRET: SECRET },
      ]) results.push(await ingestSettings({ ...full, ...env }));
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    for (const settings of results) {
      expect(settings).not.toBeNull();
      expect(settings!.sessionErase?.enabled).toBe(false);
      expect(settings!.sessionErase).toEqual({ enabled: false, reason: "config_invalid" });
      // Delivery settings are intact.
      expect(settings!.bucket).toBe("b");
    }
    const codes = written.filter((l) => l.includes("session_erase_config_invalid"));
    expect(codes.length).toBe(4);
    for (const line of written) {
      expect(line).not.toContain(shortSecret);
      expect(line).not.toContain(SECRET);
    }
  });

  test("self-host config carries the erase config; default disabled", () => {
    const env = {
      RELAYHISTORY_SELFHOST_INGEST: "true", RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
      RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test/introspect", RELAYCONTROL_INTROSPECT_SECRET: "introspect-secret-test",
      RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres?sslmode=disable",
      SESSION_POSTGRES_TLS_PROFILE: "supabase", LOCKER_BUCKET: "transcripts",
    };
    const off = selfhostIngestConfig(env);
    expect(off.enabled && off.sessionErase).toEqual({ enabled: false, reason: "flag_off" });
    expect(() => selfhostIngestConfig({ ...env, SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET })).toThrow(/SESSION_ERASE_FENCE_ENFORCED/);
    const on = selfhostIngestConfig({ ...env, SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: SECRET, SESSION_ERASE_FENCE_ENFORCED: "1" });
    expect(on.enabled && on.sessionErase.enabled).toBe(true);
    expect(() => selfhostIngestConfig({ ...env, SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: "short" })).toThrow(/at least 32/);
  });
});

describe("self-host node:http wrapper", () => {
  async function serve(config: SessionEraseConfig) {
    const s = scratchStore();
    cleanups.push(s.done);
    let verifyCalls = 0;
    let sqlCalls = 0;
    const runtime = selfhostIngestRuntime({ keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 16, sessionErase: config }, {
      run: (async () => { sqlCalls++; throw new Error("no database on the erase route"); }) as never,
      store: s.store,
      verify: async () => { verifyCalls++; throw new Error("no introspection on the erase route"); },
      ready: async () => true,
      close: async () => {},
    });
    const server = selfhostIngestHttpServer(runtime);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    cleanups.push(() => new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); }));
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}${PATH}`;
    return { url, counts: () => ({ verifyCalls, sqlCalls }) };
  }

  test("forwards the route with its own bearer, without introspection or SQL; no log line carries the secret", async () => {
    const written: string[] = [];
    const out = process.stdout.write.bind(process.stdout);
    const err = process.stderr.write.bind(process.stderr);
    process.stdout.write = ((chunk: unknown, ...rest: unknown[]) => (written.push(String(chunk)), (out as (...a: unknown[]) => boolean)(chunk, ...rest))) as typeof process.stdout.write;
    process.stderr.write = ((chunk: unknown, ...rest: unknown[]) => (written.push(String(chunk)), (err as (...a: unknown[]) => boolean)(chunk, ...rest))) as typeof process.stderr.write;
    try {
      const { url, counts } = await serve(enabled({ SESSION_ERASE_API_SECRET_PREVIOUS: PREVIOUS }));
      const ok = await fetch(url, { method: "POST", headers: { "content-type": "application/json", ...auth() }, body: BODY });
      expect([ok.status, await ok.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
      const get = await fetch(url, { headers: auth(PREVIOUS) });
      expect(get.status).toBe(501);
      const wrong = await fetch(url, { method: "POST", headers: auth(`${SECRET}x`), body: BODY });
      expect(wrong.status).toBe(401);
      const big = await fetch(url, { method: "POST", headers: auth(), body: "x".repeat(SESSION_ERASE_MAX_BODY_BYTES + 1) });
      expect(big.status).toBe(413);
      const bad = await fetch(url, { method: "POST", headers: auth(), body: "{}" });
      expect(bad.status).toBe(400);
      expect(counts()).toEqual({ verifyCalls: 0, sqlCalls: 0 });
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    const erase = written.filter((l) => l.includes("operator.session_erase"));
    expect(erase.length).toBeGreaterThanOrEqual(5);
    for (const line of written) {
      expect(line).not.toContain(SECRET);
      expect(line).not.toContain(PREVIOUS);
      expect(line).not.toContain("sess-1");
    }
  });

  test("an encoded-dot traversal out of the erase prefix takes the identity-bound path (verify is called)", async () => {
    const { url, counts } = await serve(enabled());
    const { port } = new URL(url);
    // Raw request line: fetch() would normalise the dots client-side before sending.
    const status = await new Promise<number>((resolve, reject) => {
      const r = httpRequest({ host: "127.0.0.1", port: Number(port), method: "POST", path: "/internal/v1/session-erase/%2e%2e/%2e%2e/%2e%2e/v1/delivery/batches", headers: auth() }, (res) => {
        res.resume();
        resolve(res.statusCode ?? 0);
      });
      r.on("error", reject);
      r.end("{}");
    });
    expect(status).toBe(503); // verify threw: relaycontrol_unavailable, never the erase handler
    expect(counts()).toEqual({ verifyCalls: 1, sqlCalls: 0 });
  });

  test("Authorization is still stripped before the app on a /v1 path", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const base = selfhostIngestRuntime({ keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 16, sessionErase: enabled() }, {
      run: (async () => { throw new Error("no database"); }) as never,
      store: s.store,
      verify: async () => { throw new Error("replaced below"); },
      ready: async () => true,
      close: async () => {},
    });
    const seen: (string | null)[] = [];
    const verified: (string | undefined)[] = [];
    const runtime = {
      ...base,
      verify: async (authorization: string | undefined) => { verified.push(authorization); return { userId: "u", orgId: "o", scopes: ["rth:sync"] } as never; },
      app: { fetch: (req: Request) => { seen.push(req.headers.get("authorization")); return Response.json({ ok: true }); } },
    };
    const server = selfhostIngestHttpServer(runtime);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    cleanups.push(() => new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); }));
    const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/v1/delivery/batches`, { method: "POST", headers: auth("tenant-token"), body: "{}" });
    expect(res.status).toBe(200);
    expect(verified).toEqual(["Bearer tenant-token"]);
    expect(seen).toEqual([null]);
  });

  test("an unsupported method on the erase path is 405 Allow: GET, POST at the wrapper", async () => {
    for (const config of [enabled(), sessionEraseConfig({})]) {
      const { url, counts } = await serve(config);
      for (const method of ["PUT", "DELETE", "PATCH"]) {
        const res = await fetch(url, { method, headers: auth(), body: "x" });
        expect([method, res.status, res.headers.get("allow")]).toEqual([method, 405, "GET, POST"]);
      }
      expect(counts()).toEqual({ verifyCalls: 0, sqlCalls: 0 });
    }
  });

  test("disabled: 501 through the wrapper too", async () => {
    const { url, counts } = await serve(sessionEraseConfig({}));
    const res = await fetch(url, { method: "POST", body: "x" });
    expect([res.status, await res.json()]).toEqual([501, NOT_IMPLEMENTED_BODY]);
    expect(counts()).toEqual({ verifyCalls: 0, sqlCalls: 0 });
  });
});
