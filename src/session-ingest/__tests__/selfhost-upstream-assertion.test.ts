/**
 * The relaygate v1 upstream assertion on the self-host ingest edge (selfhost-server.ts phase 1, app.ts phase 2):
 * off is unchanged, log continues and logs a closed reason, enforce answers the generic 401 before introspection
 * and, for phase 2, before any SQL or store write. No database: SQL is a counter that throws. Keys are generated
 * in memory; the signer is test-only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { gzipSync } from "node:zlib";
import type { AddressInfo } from "node:net";
import type { Server } from "node:http";
import { logLine } from "../../shared/log.ts";
import {
  INGEST_UPSTREAM_ASSERTION_PREFIX, UPSTREAM_ASSERTION_LOG_MESSAGE, upstreamAssertionConfig, upstreamAssertionLogFields,
} from "../../shared/upstream-assertion.ts";
import { signAssertion, testKey, type SignInput } from "../../shared/__tests__/upstream-assertion-signer.ts";
import { sessionEraseConfig } from "../session-erase.ts";
import {
  SELFHOST_INGEST_ROUTES, selfhostIngestHttpServer, selfhostIngestRuntime, startSelfhostIngestService,
  type RuntimeBuilder, type SelfhostIngestRuntime, type SelfhostIngestService,
} from "../selfhost-server.ts";
import { scratchStore } from "./support.ts";

const KEY = testKey("ingest-k1");
const OLD = testKey("ingest-k0");
const AUTH = "Bearer rh_tenant-secret-token";
const SESSION = "sess-private-0001";
const ERASE_SECRET = "erase-secret-current-0123456789abcdef";
const ERASE_PATH = "/internal/v1/session-erase/organizations/org-1/workspaces/ws-1/sources/claude/sessions/sess-1";
const IDENTITY = { userId: "user-1", orgId: "org-1", workspaceId: "ws-1", scopes: ["rth:sync"] };

const ENABLED = {
  RELAYHISTORY_SELFHOST_INGEST: "true", RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
  RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test/introspect", RELAYCONTROL_INTROSPECT_SECRET: "introspect-secret-test",
  RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres?sslmode=disable",
  SESSION_POSTGRES_TLS_PROFILE: "supabase", LOCKER_BUCKET: "transcripts",
};
const keyed = (mode: string | undefined, extra: Record<string, string> = {}) => ({
  ...ENABLED,
  ...(mode === undefined ? {} : { INGEST_UPSTREAM_ASSERTION_MODE: mode }),
  INGEST_UPSTREAM_ASSERTION_PUBLIC_KEY: KEY.publicB64,
  INGEST_UPSTREAM_ASSERTION_KID: KEY.kid,
  INGEST_UPSTREAM_ASSERTION_PUBLIC_KEY_PREVIOUS: OLD.publicB64,
  INGEST_UPSTREAM_ASSERTION_KID_PREVIOUS: OLD.kid,
  ...extra,
});

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0)) await f();
});

type Seen = { method: string; path: string; body: Uint8Array };

/** A runtime whose app records what reached it; verify counts introspection calls. */
function recording(seen: Seen[], counts: { verify: number }): RuntimeBuilder {
  return async () => ({
    app: {
      async fetch(req: Request) {
        seen.push({ method: req.method, path: new URL(req.url).pathname, body: new Uint8Array(await req.arrayBuffer()) });
        return Response.json({ ok: true });
      },
    },
    routes: SELFHOST_INGEST_ROUTES,
    verify: async () => { counts.verify++; return IDENTITY as never; },
    bind: () => {},
    ready: async () => true,
    close: async () => {},
  });
}

const services: SelfhostIngestService[] = [];
afterEach(async () => {
  for (const s of services.splice(0)) await s.close();
});
async function start(env: NodeJS.ProcessEnv, build: RuntimeBuilder) {
  const service = await startSelfhostIngestService(env, { host: "127.0.0.1", port: 0, build });
  services.push(service);
  return (path: string) => `http://127.0.0.1:${service.port}${path}`;
}

async function listen(server: Server) {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  cleanups.push(() => new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); }));
  return (path: string) => `http://127.0.0.1:${(server.address() as AddressInfo).port}${path}`;
}

/** Captures stderr (warn/error lines) for the duration of `fn`. */
async function capture<T>(fn: () => Promise<T>): Promise<{ result: T; lines: string[] }> {
  const lines: string[] = [];
  const original = process.stderr.write.bind(process.stderr);
  process.stderr.write = ((chunk: string | Uint8Array) => {
    lines.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
    return true;
  }) as typeof process.stderr.write;
  try {
    return { result: await fn(), lines };
  } finally {
    process.stderr.write = original;
  }
}

const BATCHES = "/v1/delivery/batches";
const LIMITS = "/v1/delivery/limits";
const SELECTION = "/v1/delivery/sessions/selection";
const batchBody = (ids: string[]) => JSON.stringify({ batch: { records: ids.map((session_id) => ({ session_id })) } });

const sign = (path: string, over: Partial<SignInput> = {}) =>
  signAssertion({ key: KEY, aud: "ingest-edge", method: "POST", path, authorization: AUTH, access: "write", sessions: [SESSION], body: batchBody([SESSION]), ...over });
const send = (url: string, init: { method?: string; body?: string | Uint8Array; assertion?: string; authorization?: string | null; headers?: Record<string, string> } = {}) =>
  fetch(url, {
    method: init.method ?? "POST",
    headers: {
      ...(init.authorization === null ? {} : { authorization: init.authorization ?? AUTH }),
      ...(init.assertion === undefined ? {} : { "x-relaygate-assertion": init.assertion }),
      ...init.headers,
    },
    ...(init.body === undefined ? {} : { body: init.body }),
  });
const code = async (r: Response) => ((await r.json()) as { error?: { code?: string } }).error?.code;

function assertNoSecrets(lines: string[], tokens: string[]) {
  for (const line of lines) {
    for (const token of tokens) {
      expect(line).not.toContain(token);
      for (const segment of token.split(".").slice(1)) expect(line).not.toContain(segment);
    }
    expect(line).not.toContain(AUTH);
    expect(line).not.toContain("rh_tenant-secret-token");
    expect(line).not.toContain(SESSION);
    expect(line).not.toContain("test-jti-1");
    expect(line).not.toContain("agent-test");
  }
}

describe("off", () => {
  test.each([undefined, "", "off"])("mode %p: unchanged with and without the header; key variables are not read", async (mode) => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const env = { ...ENABLED, ...(mode === undefined ? {} : { INGEST_UPSTREAM_ASSERTION_MODE: mode }), INGEST_UPSTREAM_ASSERTION_PUBLIC_KEY: "garbage", INGEST_UPSTREAM_ASSERTION_KID: "bad kid" };
    const url = await start(env, recording(seen, counts));
    const { result, lines } = await capture(async () => [
      await send(url(BATCHES), { body: batchBody([SESSION]) }),
      await send(url(BATCHES), { body: batchBody([SESSION]), assertion: "v1.junk.junk" }),
      await send(url("/v1/unknown"), { method: "GET" }),
    ]);
    expect(result.map((r) => r.status)).toEqual([200, 200, 501]);
    expect(seen.map((s) => s.path)).toEqual([BATCHES, BATCHES]);
    expect(counts.verify).toBe(2);
    expect(lines).toEqual([]);
  });

  test("a bad mode refuses startup naming the variable only; the dormant service reads nothing", async () => {
    await expect(startSelfhostIngestService({ ...ENABLED, INGEST_UPSTREAM_ASSERTION_MODE: "strict" }, { host: "127.0.0.1", port: 0, build: recording([], { verify: 0 }) }))
      .rejects.toThrow("INGEST_UPSTREAM_ASSERTION_MODE must be off, log or enforce");
    let message = "";
    try {
      await startSelfhostIngestService(keyed("enforce", { INGEST_UPSTREAM_ASSERTION_PUBLIC_KEY: "not-a-key-value-123" }), { host: "127.0.0.1", port: 0, build: recording([], { verify: 0 }) });
    } catch (error) {
      message = String(error);
    }
    expect(message).toContain("INGEST_UPSTREAM_ASSERTION_PUBLIC_KEY");
    expect(message).not.toContain("not-a-key-value-123");
    const url = await start({ INGEST_UPSTREAM_ASSERTION_MODE: "strict" }, recording([], { verify: 0 }));
    expect((await send(url(BATCHES), { body: "{}" })).status).toBe(503);
  });
});

describe("log", () => {
  test("missing / invalid continue unchanged and log one closed reason with the correlation id", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("log"), recording(seen, counts));
    const bad = sign(BATCHES, { aud: "session-read-api" });
    const { result, lines } = await capture(async () => [
      await send(url(BATCHES), { body: batchBody([SESSION]), headers: { "x-correlation-id": "corr-log-1" } }),
      await send(url(BATCHES), { body: batchBody([SESSION]), assertion: bad }),
      await send(url(BATCHES), { body: batchBody([SESSION]), assertion: sign(BATCHES) }),
    ]);
    expect(result.map((r) => r.status)).toEqual([200, 200, 200]);
    expect(seen).toHaveLength(3);
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(parsed.map((l) => [l.message, l.route, l.outcome, l.reason, l.state])).toEqual([
      [UPSTREAM_ASSERTION_LOG_MESSAGE, "upstream_assertion", "would_refuse", "missing", "log"],
      [UPSTREAM_ASSERTION_LOG_MESSAGE, "upstream_assertion", "would_refuse", "aud", "log"],
    ]);
    expect(parsed[0]!.correlation_id).toBe("corr-log-1");
    assertNoSecrets(lines, [bad]);
  });
  test("a valid assertion on a route not in the table continues to its answer and logs would_refuse route", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("log"), recording(seen, counts));
    const turnsBatches = "/v1/turns/batches";
    const body = JSON.stringify({ sessions: [SESSION] });
    const { result, lines } = await capture(async () => [
      await send(url(turnsBatches), { body, assertion: sign(turnsBatches, { access: "passthrough", sessions: [], body }) }),
      await send(url("/v1/unknown"), { method: "GET", assertion: sign("/v1/unknown", { method: "GET", access: "passthrough", sessions: [], body: "" }) }),
    ]);
    expect(result.map((r) => r.status)).toEqual([200, 501]);
    expect(seen.map((s) => [s.method, s.path])).toEqual([["POST", turnsBatches]]);
    expect(Buffer.from(seen[0]!.body).toString("utf8")).toBe(body);
    expect(counts.verify).toBe(1);
    expect(lines.map((l) => (JSON.parse(l) as { outcome: string; reason: string })).map((l) => [l.outcome, l.reason])).toEqual([["would_refuse", "route"], ["would_refuse", "route"]]);
  });
});

describe("enforce (wrapper, phase 1)", () => {
  test("missing / invalid on a table route and an unknown route: generic 401 before introspection and the app", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("enforce"), recording(seen, counts));
    const tokens = [sign(BATCHES, { aud: "session-read-api" }), sign("/v1/unknown", { method: "GET", sessions: [SESSION], access: "passthrough", body: "" })];
    const { result, lines } = await capture(async () => [
      await send(url(BATCHES), { body: batchBody([SESSION]) }),
      await send(url(BATCHES), { body: batchBody([SESSION]), assertion: tokens[0] }),
      await send(url(BATCHES), { body: batchBody([SESSION]), assertion: sign(BATCHES), authorization: "Bearer other" }),
      await send(url("/v1/unknown"), { method: "GET" }),
      await send(url("/v1/unknown"), { method: "GET", assertion: tokens[1] }),
      await send(url("/not-v1"), { method: "GET" }),
    ]);
    const codes = await Promise.all(result.map(async (r) => [r.status, await code(r)]));
    expect(codes).toEqual([
      [401, "missing_upstream_assertion"],
      [401, "invalid_upstream_assertion"],
      [401, "invalid_upstream_assertion"],
      [401, "missing_upstream_assertion"],
      [401, "invalid_upstream_assertion"],
      [401, "missing_upstream_assertion"],
    ]);
    expect(seen).toEqual([]);
    expect(counts.verify).toBe(0);
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(parsed.map((l) => [l.outcome, l.reason, l.state])).toEqual([
      ["refused", "missing", "enforce"], ["refused", "aud", "enforce"], ["refused", "credential", "enforce"],
      ["refused", "missing", "enforce"], ["refused", "route", "enforce"], ["refused", "missing", "enforce"],
    ]);
    assertNoSecrets(lines, tokens);
  });

  test("a valid assertion (current or previous key) passes; on an unknown route it is refused (route), not the 501", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("enforce"), recording(seen, counts));
    const limits = (key = KEY) => sign(LIMITS, { key, method: "GET", access: "passthrough", sessions: [], body: "" });
    const a = await send(url(BATCHES), { body: batchBody([SESSION]), assertion: sign(BATCHES) });
    const b = await send(url(LIMITS), { method: "GET", assertion: limits(OLD) });
    const c = await send(url("/v1/unknown?x=1"), { method: "GET", assertion: sign("/v1/unknown?x=1", { method: "GET", access: "passthrough", sessions: [], body: "" }) });
    expect([a.status, b.status, c.status]).toEqual([200, 200, 401]);
    expect(await code(c)).toBe("invalid_upstream_assertion");
    expect(seen.map((s) => s.path)).toEqual([BATCHES, LIMITS]);
    expect(counts.verify).toBe(2);
  });

  test("POST /v1/turns/batches is not in the table: route for any access, passthrough with an empty scope included; missing without one", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("enforce"), recording(seen, counts));
    const turnsBatches = "/v1/turns/batches";
    const body = JSON.stringify({ sessions: [SESSION] });
    const { result, lines } = await capture(async () => [
      await send(url(turnsBatches), { body, assertion: sign(turnsBatches, { access: "write", sessions: [], body }) }),
      await send(url(turnsBatches), { body, assertion: sign(turnsBatches, { access: "aggregate", sessions: [], body }) }),
      await send(url(turnsBatches), { body, assertion: sign(turnsBatches, { access: "passthrough", sessions: [], body }) }),
      await send(url(turnsBatches), { body }),
    ]);
    expect(await Promise.all(result.map(async (r) => [r.status, await code(r)]))).toEqual([
      [401, "invalid_upstream_assertion"], [401, "invalid_upstream_assertion"], [401, "invalid_upstream_assertion"], [401, "missing_upstream_assertion"],
    ]);
    expect(seen).toEqual([]);
    expect(counts.verify).toBe(0);
    expect(lines.map((l) => (JSON.parse(l) as { reason: string }).reason)).toEqual(["route", "route", "route", "missing"]);
  });

  test("HEAD is not aliased to GET: a valid assertion for HEAD on a GET table path is refused (route)", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("enforce"), recording(seen, counts));
    const { result, lines } = await capture(async () => [
      await send(url(LIMITS), { method: "HEAD", assertion: sign(LIMITS, { method: "HEAD", access: "passthrough", sessions: [], body: "" }) }),
      await send(url(LIMITS), { method: "GET", assertion: sign(LIMITS, { method: "GET", access: "passthrough", sessions: [], body: "" }) }),
    ]);
    expect(result.map((r) => r.status)).toEqual([401, 200]);
    expect(seen.map((s) => [s.method, s.path])).toEqual([["GET", LIMITS]]);
    expect(counts.verify).toBe(1);
    expect(lines.map((l) => (JSON.parse(l) as { reason: string }).reason)).toEqual(["route"]);
  });

  test("HEAD and GET on a table route without an assertion are the 401 missing_upstream_assertion, before introspection and the app", async () => {
    const seen: Seen[] = [];
    const counts = { verify: 0 };
    const url = await start(keyed("enforce"), recording(seen, counts));
    const { result, lines } = await capture(async () => [
      await send(url(LIMITS), { method: "HEAD" }),
      await send(url(LIMITS), { method: "GET" }),
      await send(url("/v1/delivery/admission"), { method: "HEAD" }),
    ]);
    expect(result.map((r) => r.status)).toEqual([401, 401, 401]);
    // A HEAD answer carries no body: its reason is read from the log line; the GET's code from the body.
    expect(await code(result[1]!)).toBe("missing_upstream_assertion");
    expect(seen).toEqual([]);
    expect(counts.verify).toBe(0);
    expect(lines.map((l) => (JSON.parse(l) as { outcome: string; reason: string })).map((l) => [l.outcome, l.reason])).toEqual([["refused", "missing"], ["refused", "missing"], ["refused", "missing"]]);
  });

  test("bypass: the probes and OPTIONS never see assertion logic", async () => {
    const url = await start(keyed("enforce"), recording([], { verify: 0 }));
    const { result, lines } = await capture(async () => [
      await fetch(url("/healthz")),
      await fetch(url("/readyz")),
      await fetch(url(BATCHES), { method: "OPTIONS" }),
    ]);
    expect(result.map((r) => r.status)).toEqual([200, 200, 405]);
    expect(lines).toEqual([]);
  });

  test("exempt: POST refresh without an assertion keeps its answer; a presented one is validated; GET is not exempt", async () => {
    const url = await start(keyed("enforce"), recording([], { verify: 0 }));
    const refresh = "/v1/auth/token/refresh";
    const none = await send(url(refresh), { body: "{}" });
    expect([none.status, await code(none)]).toEqual([401, "refresh_not_supported"]);
    const bad = await send(url(refresh), { body: "{}", assertion: sign(refresh, { access: "write", sessions: [], body: "{}" }) });
    expect([bad.status, await code(bad)]).toEqual([401, "invalid_upstream_assertion"]);
    const good = await send(url(refresh), { body: "{}", assertion: sign(refresh, { access: "passthrough", sessions: [], body: "{}" }) });
    expect([good.status, await code(good)]).toEqual([401, "refresh_not_supported"]);
    const get = await send(url(refresh), { method: "GET" });
    expect([get.status, await code(get)]).toEqual([401, "missing_upstream_assertion"]);
    // A valid assertion for a method or path next to the exempt one is no route.
    const getSigned = await send(url(refresh), { method: "GET", assertion: sign(refresh, { method: "GET", access: "passthrough", sessions: [], body: "" }) });
    expect([getSigned.status, await code(getSigned)]).toEqual([401, "invalid_upstream_assertion"]);
    const slash = await send(url(`${refresh}/`), { body: "{}", assertion: sign(`${refresh}/`, { access: "passthrough", sessions: [], body: "{}" }) });
    expect([slash.status, await code(slash)]).toEqual([401, "invalid_upstream_assertion"]);
  });

  test("the operator erase route: its own bearer decides, no assertion needed; a wrong secret is still refused", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    let verifyCalls = 0;
    let sqlCalls = 0;
    const runtime = selfhostIngestRuntime({ keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 16, sessionErase: sessionEraseConfig({ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: ERASE_SECRET }) }, {
      run: (async () => { sqlCalls++; throw new Error("no database"); }) as never,
      store: s.store,
      verify: async () => { verifyCalls++; throw new Error("no introspection on the erase route"); },
      ready: async () => true,
      close: async () => {},
    });
    const config = upstreamAssertionConfig(keyed("enforce"), INGEST_UPSTREAM_ASSERTION_PREFIX)!;
    const url = await listen(selfhostIngestHttpServer(runtime, { upstreamAssertion: config }));
    const body = JSON.stringify({ requestId: "req-1", actor: { kind: "user", id: "user-1" } });
    const ok = await send(url(ERASE_PATH), { body, authorization: `Bearer ${ERASE_SECRET}`, headers: { "content-type": "application/json" } });
    expect(ok.status).toBe(501);
    const wrong = await send(url(ERASE_PATH), { body, authorization: `Bearer ${ERASE_SECRET}x` });
    const none = await send(url(ERASE_PATH), { body, authorization: null });
    for (const r of [wrong, none]) {
      const text = await r.text();
      expect(r.status).toBe(401);
      expect(text).toContain("invalid_token");
      expect(text).not.toContain("upstream_assertion");
    }
    expect({ verifyCalls, sqlCalls }).toEqual({ verifyCalls: 0, sqlCalls: 0 });
  });
});

describe("enforce (real app, phase 2)", () => {
  function serveReal(mode: "log" | "enforce", extra: { sessionSelection?: true } = {}) {
    const s = scratchStore();
    cleanups.push(s.done);
    const counts = { sql: 0, verify: 0 };
    const runtime: SelfhostIngestRuntime = selfhostIngestRuntime({ keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 16, ...extra }, {
      run: (async () => { counts.sql++; throw new Error("no database in this test"); }) as never,
      store: s.store,
      verify: async () => { counts.verify++; return IDENTITY as never; },
      ready: async () => true,
      close: async () => {},
    });
    const config = upstreamAssertionConfig(keyed(mode), INGEST_UPSTREAM_ASSERTION_PREFIX)!;
    return { url: listen(selfhostIngestHttpServer(runtime, { upstreamAssertion: config })), counts };
  }

  test("body_sha256 mismatch: 401 after the bounded read, before identity use, parse or any SQL", async () => {
    const { url, counts } = serveReal("enforce");
    const u = await url;
    const { result, lines } = await capture(async () => send(u(BATCHES), { body: batchBody([SESSION]), assertion: sign(BATCHES, { body: batchBody([SESSION]) + " " }) }));
    expect([result.status, await code(result)]).toEqual([401, "invalid_upstream_assertion"]);
    expect(counts.sql).toBe(0);
    expect(lines.map((l) => (JSON.parse(l) as Record<string, unknown>).reason)).toEqual(["body"]);
  });

  test("delivery batch scope mismatch: 401 before any SQL; a matching scope passes the gate", async () => {
    const { url, counts } = serveReal("enforce");
    const u = await url;
    const other = batchBody([SESSION, "sess-other"]);
    const refused = await send(u(BATCHES), { body: other, assertion: sign(BATCHES, { body: other }) });
    expect([refused.status, await code(refused)]).toEqual([401, "invalid_upstream_assertion"]);
    expect(counts.sql).toBe(0);
    const passed = await send(u(BATCHES), { body: other, assertion: sign(BATCHES, { body: other, sessions: ["sess-other", SESSION] }) });
    expect(passed.status).not.toBe(401);
  });

  test("gzip delivery batch: body_sha256 over the compressed wire bytes, scope from the decoded records", async () => {
    const { url, counts } = serveReal("enforce");
    const u = await url;
    const plain = batchBody([SESSION]);
    const wire = gzipSync(Buffer.from(plain));
    const headers = { "content-encoding": "gzip", "content-type": "application/json" };
    const overDecoded = await send(u(BATCHES), { body: wire, headers, assertion: sign(BATCHES, { body: plain }) });
    expect([overDecoded.status, await code(overDecoded)]).toEqual([401, "invalid_upstream_assertion"]);
    const wrongScope = await send(u(BATCHES), { body: wire, headers, assertion: sign(BATCHES, { body: wire, sessions: ["sess-other"] }) });
    expect([wrongScope.status, await code(wrongScope)]).toEqual([401, "invalid_upstream_assertion"]);
    expect(counts.sql).toBe(0);
    const ok = await send(u(BATCHES), { body: wire, headers, assertion: sign(BATCHES, { body: wire }) });
    expect(ok.status).not.toBe(401);
  });

  test("session selection scope mismatch: 401 before any SQL", async () => {
    const { url, counts } = serveReal("enforce", { sessionSelection: true });
    const u = await url;
    const body = JSON.stringify({ account_id: "acct-1", origin_id: "origin-1", enabled: false, sessions: [{ source: "claude", session_id: "sess-other" }] });
    const refused = await send(u(SELECTION), { body, assertion: sign(SELECTION, { body }) });
    expect([refused.status, await code(refused)]).toEqual([401, "invalid_upstream_assertion"]);
    expect(counts.sql).toBe(0);
  });

  test("log mode: a phase-2 failure logs and continues", async () => {
    const { url } = serveReal("log");
    const u = await url;
    const { result, lines } = await capture(async () => send(u(BATCHES), { body: batchBody([SESSION]), assertion: sign(BATCHES, { body: "{}" }) }));
    expect(result.status).not.toBe(401);
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>).filter((l) => l.route === "upstream_assertion");
    expect(parsed.map((l) => [l.outcome, l.reason])).toEqual([["would_refuse", "body"]]);
    assertNoSecrets(lines, []);
  });

  test("the real logLine keeps every field of the refusal line", () => {
    const line = JSON.parse(logLine("warn", UPSTREAM_ASSERTION_LOG_MESSAGE, upstreamAssertionLogFields("enforce", "scope", "corr-2"))) as Record<string, unknown>;
    expect(line).toMatchObject({ route: "upstream_assertion", outcome: "refused", reason: "scope", state: "enforce", correlation_id: "corr-2" });
  });
});
