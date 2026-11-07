/**
 * The session-ingest edge end to end: a relaycontrol fake (the shared
 * introspection client over a stub fetch that plays relaycontrol's
 * /internal/v1/auth/introspect) in front, the real session-ingest Worker
 * (createIngestApp over MemoryLedger + a scratch store) behind. No network;
 * synthetic identifiers and secrets only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { gzipSync, zstdCompressSync } from "node:zlib";
import { createRelaycontrolIntrospector, type RelaycontrolIntrospector } from "@rhc/auth/relaycontrol.ts";
import {
  createIngestApp,
  IDENTITY_HEADER,
  READ_SCOPE,
  SIGNATURE_HEADER,
  signedBindingIdentity,
  SYNC_SCOPE,
} from "../../session-ingest/app.ts";
import { CURRENT_CUSTODY_CONTENT_TYPE } from "../../session-ingest/custody-protocol.ts";
import { RECOVERY_CONTENT_TYPE } from "../../session-ingest/recovery-protocol.ts";
import { DELIVERY_LIMITS, MAX_DELIVERY_BYTES } from "../../session-ingest/vendor/delivery/delivery-contracts.ts";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, record, scratchStore } from "../../session-ingest/__tests__/support.ts";
import { createIngestEdgeApp, EDGE_ROUTES, edgeRoute, originIdGate } from "../app.ts";

const ORIGIN = "https://edge.example.test";
const OTHER_ORIGIN = "https://other-edge.example.test";
const TOKEN = "sh_at_synthetic-edge-token-1";
const TOKEN_2 = "sh_at_synthetic-edge-token-2";
const INTROSPECT_SECRET = "synthetic-introspect-secret";
const UPSTREAM = "http://session-ingest.history.svc:8080";
const enc = (s: string) => new TextEncoder().encode(s);
const noSleep = async () => {};
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

type IntrospectBody = { token: string; aud: string; workspace?: string; machine?: string };
type Answer = (body: IntrospectBody, signal: AbortSignal | undefined) => Response | Promise<Response>;

function active(over: Record<string, unknown> = {}, nowMs = Date.now()) {
  const now = Math.floor(nowMs / 1000);
  return {
    active: true, sub: TENANT.userId, email: "user@example.test", principal_type: "desktop-user", agent_name: null, machine_id: null,
    org: TENANT.orgId, workspace: TENANT.workspaceId, scopes: ["cli:auth", READ_SCOPE, SYNC_SCOPE], aud: ORIGIN, iat: now, exp: now + 600, token_id: "tokid_1", ...over,
  };
}

/** relaycontrol as the contract pin answers: echo the matched aud, or inactive `audience`. */
const relaycontrol = (over: Record<string, unknown> = {}): Answer => (body) =>
  Response.json(body.aud === ORIGIN ? active({ ...over }) : { active: false, reason: "audience" });

interface Forwarded { url: URL; method: string; headers: Headers; body: Uint8Array }

function harness(opts: { answer?: Answer; origin?: string; introspector?: RelaycontrolIntrospector; clock?: { ms: number }; upstream?: (req: Request) => Promise<Response> } = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const worker = createIngestApp({ deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: noSleep } }, log: () => {}, signingKey: SIGNING_KEY });
  const forwarded: Forwarded[] = [];
  const rcCalls: { url: string; authorization: string | null; body: IntrospectBody }[] = [];
  const rcLogs: unknown[] = [];
  const answer = opts.answer ?? relaycontrol();
  const introspector = opts.introspector ?? createRelaycontrolIntrospector({
    url: "http://relaycontrol.control.svc:8080",
    secret: INTROSPECT_SECRET,
    timeoutMs: 50,
    log: (...args) => void rcLogs.push(args),
    ...(opts.clock ? { now: () => opts.clock!.ms } : {}),
    fetch: (async (url: RequestInfo | URL, init: RequestInit = {}) => {
      const body = JSON.parse(String(init.body)) as IntrospectBody;
      rcCalls.push({ url: String(url), authorization: new Headers(init.headers).get("authorization"), body });
      return answer(body, init.signal ?? undefined);
    }) as typeof fetch,
  });
  const logs: Record<string, unknown>[] = [];
  const edge = createIngestEdgeApp({
    enabled: true,
    origin: opts.origin ?? ORIGIN,
    upstreamUrl: UPSTREAM,
    signingKey: SIGNING_KEY,
    introspector,
    log: (fields) => void logs.push(fields),
    fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
      const req = new Request(input, init);
      forwarded.push({ url: new URL(req.url), method: req.method, headers: new Headers(req.headers), body: new Uint8Array(await req.clone().arrayBuffer()) });
      return (opts.upstream ?? ((r) => Promise.resolve(worker.fetch(r))))(req);
    }) as typeof fetch,
  });
  const send = (path: string, init: { method?: string; body?: BodyInit; headers?: Record<string, string>; token?: string | null } = {}) => {
    const headers: Record<string, string> = { ...(init.method === "GET" ? {} : { "content-type": "application/json" }), ...init.headers };
    if (init.token !== null) headers.authorization ??= `Bearer ${init.token ?? TOKEN}`;
    return edge.fetch(new Request(`${ORIGIN}${path}`, { method: init.method ?? "POST", headers, ...(init.body !== undefined ? { body: init.body } : {}) }));
  };
  return { edge, worker, ledger, forwarded, rcCalls, rcLogs, logs, send, introspector };
}

async function deliveryBody(over: Record<string, unknown> = {}, account = TENANT) {
  const b = await batch([record({ record_id: "e1" })], { account_id: await deliveryAccount(account) });
  return JSON.stringify({ protocolVersion: 1, batch: { ...b, ...over } });
}

const decodeIdentity = (h: Headers) => JSON.parse(Buffer.from(h.get(IDENTITY_HEADER)!, "base64url").toString("utf8"));
const code = async (res: Response) => ((await res.clone().json()) as { error?: { code: string } }).error?.code;

describe("the allowlist is the single source of truth (Amendment 4)", () => {
  test("exactly the seven routes, with their scopes", () => {
    expect(EDGE_ROUTES.map((r) => `${r.method} ${r.path} ${r.scope}`)).toEqual([
      "POST /v1/delivery/batches rth:sync",
      "POST /v1/turns/batches rth:sync",
      "POST /v1/delivery/sessions/selection rth:sync",
      "POST /v1/delivery/sessions/reset rth:sync",
      "POST /v1/onboarding/heartbeat rth:sync",
      "GET /v1/onboarding/connection rth:read",
      "GET /v1/delivery/limits rth:read",
    ]);
    expect(Object.isFrozen(EDGE_ROUTES) && EDGE_ROUTES.every((r) => Object.isFrozen(r))).toBe(true);
    for (const r of EDGE_ROUTES) expect(edgeRoute(r.method, r.path)).toBe(r);
  });

  test.each(EDGE_ROUTES.map((r) => [r.method, r.path] as const))("%s %s with a full-scope token reaches the Worker at exactly that path", async (method, path) => {
    const h = harness();
    const res = await h.send(path, { method, ...(method === "POST" ? { body: "{}" } : {}) });
    expect(res.status).not.toBe(404);
    expect(h.forwarded.map((f) => [f.method, f.url.origin, f.url.pathname, f.url.search])).toEqual([[method, UPSTREAM, path, ""]]);
  });

  const refused: [string, string][] = [
    ["POST", "/v1/delivery/recovery"],
    ["POST", "/v1/custody/batches"],
    ["POST", "/v1/custody/selection"],
    ["GET", "/v1/custody/witness"],
    ["POST", "/v1/custody"],
    ["GET", "/v1/delivery/batches"],
    ["PUT", "/v1/delivery/batches"],
    ["DELETE", "/v1/delivery/batches"],
    ["HEAD", "/v1/delivery/limits"],
    ["POST", "/v1/delivery/limits"],
    ["POST", "/v1/onboarding/connection"],
    ["GET", "/v1/onboarding/heartbeat"],
    ["POST", "/v1/delivery/batches/"],
    ["POST", "/V1/delivery/batches"],
    ["POST", "/v1/delivery/%62atches"],
    ["POST", "/v1/delivery/sessions"],
    ["POST", "/v1/delivery/sessions/delete"],
    ["GET", "/v1/sessions"],
    ["POST", "/internal/v1/auth/introspect"],
    ["GET", "/"],
  ];
  test.each(refused)("%s %s is 404 with a valid token: never introspected, never forwarded", async (method, path) => {
    const h = harness();
    const res = await h.send(path, { method, ...(["GET", "HEAD"].includes(method) ? {} : { body: "{}" }) });
    expect(res.status).toBe(404);
    if (method !== "HEAD") expect(await code(res)).toBe("not_found");
    expect(h.rcCalls).toEqual([]);
    expect(h.forwarded).toEqual([]);
  });

  test("recovery is 404 even with its own content type; dot segments resolve before the lookup", async () => {
    const h = harness();
    expect((await h.send("/v1/delivery/recovery", { body: "{}", headers: { "content-type": RECOVERY_CONTENT_TYPE } })).status).toBe(404);
    const res = await h.send("/v1/custody/../delivery/limits", { method: "GET" });
    expect(res.status).toBe(200);
    expect(h.forwarded.map((f) => f.url.pathname)).toEqual(["/v1/delivery/limits"]);
  });
});

describe("media type (Amendment 4): only application/json, checked before introspection", () => {
  test.each([
    [CURRENT_CUSTODY_CONTENT_TYPE],
    [`${CURRENT_CUSTODY_CONTENT_TYPE}; charset=utf-8`],
    [RECOVERY_CONTENT_TYPE],
    ["application/json-seq"],
    ["application/jsonx"],
    ["text/plain"],
    ["application/x-www-form-urlencoded"],
    ["multipart/form-data; boundary=x"],
    [""],
  ])("POST with Content-Type %p is 415, nothing introspected or forwarded", async (type) => {
    const h = harness();
    const res = await h.send("/v1/delivery/batches", { body: "{}", headers: { "content-type": type } });
    expect([res.status, await code(res)]).toEqual([415, "unsupported_media_type"]);
    expect(h.rcCalls).toEqual([]);
    expect(h.forwarded).toEqual([]);
  });

  test("a POST without Content-Type is 415", async () => {
    const h = harness();
    const res = await h.edge.fetch(new Request(`${ORIGIN}/v1/delivery/batches`, { method: "POST", body: new Uint8Array([123, 125]), headers: { authorization: `Bearer ${TOKEN}` } }));
    expect(res.status).toBe(415);
    expect(h.rcCalls.length + h.forwarded.length).toBe(0);
  });

  test("a GET carrying a custody content type is 415", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/limits", { method: "GET", headers: { "content-type": CURRENT_CUSTODY_CONTENT_TYPE } });
    expect(res.status).toBe(415);
    expect(h.rcCalls.length + h.forwarded.length).toBe(0);
  });

  test("parameters and case are ignored, and the Worker gets the canonical application/json", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/batches", { body: await deliveryBody(), headers: { "content-type": "Application/JSON ; charset=utf-8" } });
    expect(res.status).toBe(200);
    expect(h.forwarded[0]!.headers.get("content-type")).toBe("application/json");
  });

  test("an unsupported Content-Encoding is 415 before introspection", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/batches", { body: "{}", headers: { "content-encoding": "br" } });
    expect([res.status, await code(res)]).toEqual([415, "unsupported_encoding"]);
    expect(h.rcCalls.length + h.forwarded.length).toBe(0);
  });
});

describe("happy path end to end", () => {
  test("identity is exactly relaycontrol's answer, the signature verifies, the body bytes are unchanged", async () => {
    const h = harness();
    const body = await deliveryBody();
    const res = await h.send("/v1/delivery/batches", { body });
    expect(res.status).toBe(200);
    // Introspection: {token, aud} only, our secret, the configured endpoint.
    expect(h.rcCalls).toEqual([{ url: "http://relaycontrol.control.svc:8080/internal/v1/auth/introspect", authorization: `Bearer ${INTROSPECT_SECRET}`, body: { token: TOKEN, aud: ORIGIN } }]);
    const f = h.forwarded[0]!;
    expect(decodeIdentity(f.headers)).toEqual({ userId: TENANT.userId, orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, scopes: [READ_SCOPE, SYNC_SCOPE] });
    expect(new TextDecoder().decode(f.body)).toBe(body);
    // The signature verifies independently of the Worker's answer.
    expect(signedBindingIdentity(SIGNING_KEY)(new Request(f.url, { method: f.method, headers: f.headers }), f.body)).toEqual({ ...TENANT, scopes: [READ_SCOPE, SYNC_SCOPE] });
    // The Worker wrote it for that tenant.
    expect(h.ledger.claims.map((c) => c.tenant)).toEqual([TENANT]);
    expect(((await res.json()) as { limits: { maxRequestBytes: number } }).limits.maxRequestBytes).toBe(MAX_DELIVERY_BYTES);
  });

  test.each([
    ["gzip", (b: string) => new Uint8Array(gzipSync(b))],
    ["zstd", (b: string) => new Uint8Array(zstdCompressSync(Buffer.from(b)))],
  ] as const)("%s content-encoding passes through: same wire bytes, same header, the Worker decodes it", async (encoding, compress) => {
    const h = harness();
    const wire = compress(await deliveryBody());
    const res = await h.send("/v1/delivery/batches", { body: wire, headers: { "content-encoding": encoding } });
    expect(res.status).toBe(200);
    expect(h.forwarded[0]!.headers.get("content-encoding")).toBe(encoding);
    expect(Buffer.from(h.forwarded[0]!.body).equals(Buffer.from(wire))).toBe(true);
    expect(h.ledger.claims.length).toBe(1);
  });

  test("turns, selection and reset reach the Worker for the introspected tenant", async () => {
    const h = harness();
    const account_id = await deliveryAccount(TENANT);
    const sel = await h.send("/v1/delivery/sessions/selection", { body: JSON.stringify({ account_id, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "s1" }], enabled: false }) });
    expect([sel.status, await sel.json()]).toEqual([200, { updated: true }]);
    expect(h.ledger.selections).toEqual([{ tenant: TENANT, originId: "origin-1", sessions: [["claude", "s1"]], enabled: false }]);
    const reset = await h.send("/v1/delivery/sessions/reset", { body: JSON.stringify({ account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" }) });
    expect(reset.status).toBe(200);
    // One introspection: the positive answer is cached for this (token, aud).
    expect(h.rcCalls.length).toBe(1);
  });

  test("GET /v1/delivery/limits answers DELIVERY_LIMITS, no body forwarded, no content headers", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/limits", { method: "GET" });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ...DELIVERY_LIMITS });
    const f = h.forwarded[0]!;
    expect([f.body.byteLength, f.headers.get("content-type"), f.headers.get("content-encoding")]).toEqual([0, null, null]);
  });

  test("the Worker's status and body stream back; hop-by-hop and Set-Cookie do not", async () => {
    const h = harness({ upstream: async () => new Response('{"x":1}', { status: 202, headers: { "content-type": "application/json", "set-cookie": "a=b", connection: "close", "x-upstream": "kept" } }) });
    const res = await h.send("/v1/delivery/limits", { method: "GET" });
    expect([res.status, await res.text(), res.headers.get("set-cookie"), res.headers.get("connection"), res.headers.get("x-upstream")]).toEqual([202, '{"x":1}', null, null, "kept"]);
  });
});

describe("only relaycontrol's answer is identity", () => {
  test("client identity, signature, workspace, machine, cookie and query never reach the Worker", async () => {
    const h = harness();
    const body = await deliveryBody();
    const forgedIdentity = Buffer.from(JSON.stringify({ userId: "user-victim", orgId: "org-victim", workspaceId: "ws-victim", scopes: [SYNC_SCOPE] })).toString("base64url");
    const res = await h.send("/v1/delivery/batches?workspace=ws-victim&org=org-victim", {
      body,
      headers: {
        [IDENTITY_HEADER]: forgedIdentity,
        [SIGNATURE_HEADER]: `t=${Math.floor(Date.now() / 1000)},v1=${"0".repeat(64)}`,
        "x-relayhistory-workspace-id": "ws-victim",
        "x-machine-id": "machine-victim",
        cookie: "session=victim",
        "x-forwarded-for": "203.0.113.9",
        "x-correlation-id": "corr-1",
      },
    });
    expect(res.status).toBe(200);
    const f = h.forwarded[0]!;
    expect(f.headers.get(IDENTITY_HEADER)).not.toBe(forgedIdentity);
    expect(decodeIdentity(f.headers)).toEqual({ userId: TENANT.userId, orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, scopes: [READ_SCOPE, SYNC_SCOPE] });
    expect([...f.headers.keys()].sort()).toEqual(["accept", "accept-encoding", "content-type", "x-correlation-id", IDENTITY_HEADER, SIGNATURE_HEADER].sort());
    expect(f.url.search).toBe("");
    // Nothing from the request reached the introspection body either.
    expect(h.rcCalls.map((c) => c.body)).toEqual([{ token: TOKEN, aud: ORIGIN }]);
  });

  test("a client identity header alone (no bearer) is 401: never trusted as identity", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/batches", { token: null, body: "{}", headers: { [IDENTITY_HEADER]: "e30" } });
    expect([res.status, await code(res)]).toEqual([401, "missing_authorization"]);
    expect(h.forwarded).toEqual([]);
  });

  test("workspace comes from the response even when the body names another", async () => {
    const h = harness();
    const other = { ...TENANT, workspaceId: "ws-b" };
    const res = await h.send("/v1/delivery/batches", { body: await deliveryBody({}, other), headers: { "x-relayhistory-workspace-id": "ws-b" } });
    // Signed as ws-a (the response); the Worker then refuses the ws-b account.
    expect(decodeIdentity(h.forwarded[0]!.headers).workspaceId).toBe(TENANT.workspaceId);
    expect(res.status).toBe(403);
    expect(h.ledger.claims).toEqual([]);
    expect("workspace" in h.rcCalls[0]!.body).toBe(false);
  });

  test("relaycontrol scopes outside rth:read/rth:sync are never signed", async () => {
    const h = harness({ answer: relaycontrol({ scopes: ["admin", "rth:sync", "rth:*", "cli:auth"] }) });
    expect((await h.send("/v1/delivery/batches", { body: await deliveryBody() })).status).toBe(200);
    expect(decodeIdentity(h.forwarded[0]!.headers).scopes).toEqual([SYNC_SCOPE]);
  });

  test("originIdGate is a no-op and origin_id stays untouched in the body", async () => {
    await expect(originIdGate({ route: EDGE_ROUTES[0]!, identity: { ...TENANT, scopes: [] }, body: enc("{}") })).resolves.toBeUndefined();
    const h = harness();
    const body = await deliveryBody({ origin_id: "origin-untouched-1" });
    await h.send("/v1/delivery/batches", { body });
    expect(new TextDecoder().decode(h.forwarded[0]!.body)).toBe(body);
    expect(h.rcCalls.every((c) => !("machine" in c.body))).toBe(true);
  });
});

describe("401: not a usable relaycontrol token; nothing forwarded", () => {
  const cases: [string, Partial<{ authorization: string; answer: Answer }>, string][] = [
    ["no Authorization", {}, "missing_authorization"],
    ["Basic auth", { authorization: "Basic dXNlcjpwYXNz" }, "missing_authorization"],
    ["an empty bearer", { authorization: "Bearer " }, "missing_authorization"],
    ["a RelayAuth JWT bearer", { authorization: "Bearer eyJhbGciOiJSUzI1NiJ9.e30.sig" }, "invalid_token"],
    ["a refresh token", { authorization: "Bearer sh_rt_synthetic" }, "invalid_token"],
    ["a workspace token", { authorization: "Bearer sh_wt_synthetic" }, "invalid_token"],
  ];
  test.each(cases)("%s is 401 without calling relaycontrol", async (_name, over, expected) => {
    const h = harness();
    const res = await h.send("/v1/delivery/batches", { token: null, body: "{}", headers: over.authorization ? { authorization: over.authorization } : {} });
    expect([res.status, await code(res)]).toEqual([401, expected]);
    expect(h.rcCalls).toEqual([]);
    expect(h.forwarded).toEqual([]);
  });

  test.each([
    ["active:false revoked", () => Response.json({ active: false, reason: "revoked" })],
    ["active:false expired", () => Response.json({ active: false, reason: "expired" })],
    ["active:false audience", () => Response.json({ active: false, reason: "audience" })],
    ["an active answer for another aud", () => Response.json(active({ aud: OTHER_ORIGIN }))],
    ["relaycontrol 400 (our request malformed)", () => Response.json({ error: "invalid_request" }, { status: 400 })],
  ] as [string, Answer][])("%s is 401 invalid_token", async (_name, answer) => {
    const h = harness({ answer });
    const res = await h.send("/v1/delivery/batches", { body: await deliveryBody() });
    expect([res.status, await code(res)]).toEqual([401, "invalid_token"]);
    expect(h.rcCalls.length).toBe(1);
    expect(h.forwarded).toEqual([]);
  });

  test("audience mismatch: an edge whose origin relaycontrol does not list is refused", async () => {
    const h = harness({ origin: OTHER_ORIGIN });
    const res = await h.send("/v1/delivery/batches", { body: await deliveryBody() });
    expect(res.status).toBe(401);
    expect(h.rcCalls.map((c) => c.body.aud)).toEqual([OTHER_ORIGIN]);
    expect(h.forwarded).toEqual([]);
  });
});

describe("503 relaycontrol_unavailable: fail closed, retryable, nothing forwarded", () => {
  const hang: Answer = (_b, signal) => new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(signal.reason)));
  test.each([
    ["a timeout", hang],
    ["500", () => new Response("boom", { status: 500 })],
    ["502", () => new Response("bad gateway", { status: 502 })],
    ["503", () => new Response("", { status: 503 })],
    ["404 (introspection disabled)", () => Response.json({ error: "not_found" }, { status: 404 })],
    ["401 (our secret refused: a deployment error)", () => Response.json({ error: "unauthorized" }, { status: 401 })],
    ["a network error", () => Promise.reject(new TypeError("fetch failed"))],
    ["malformed JSON", () => new Response("not json", { status: 200 })],
    ["an active answer missing fields", () => Response.json({ active: true, sub: "u" })],
  ] as [string, Answer][])("%s", async (_name, answer) => {
    const h = harness({ answer });
    const res = await h.send("/v1/delivery/batches", { body: await deliveryBody() });
    expect([res.status, await code(res), res.headers.get("retry-after")]).toEqual([503, "relaycontrol_unavailable", "1"]);
    expect(h.forwarded).toEqual([]);
  });

  test("an unexpected introspector failure is also 503, never admitted", async () => {
    const h = harness({ introspector: { verifyRelaycontrolToken: async () => { throw new Error("surprise"); } } });
    expect((await h.send("/v1/delivery/batches", { body: "{}" })).status).toBe(503);
    expect(h.forwarded).toEqual([]);
  });

  test("an unreachable Worker is 503 delivery_unavailable", async () => {
    const h = harness({ upstream: () => Promise.reject(new TypeError("connect refused")) });
    const res = await h.send("/v1/delivery/limits", { method: "GET" });
    expect([res.status, await code(res)]).toEqual([503, "delivery_unavailable"]);
  });
});

describe("403: scope per route", () => {
  test.each(EDGE_ROUTES.filter((r) => r.method === "POST").map((r) => [r.path]))("an rth:read-only token on POST %s is 403, nothing forwarded", async (path) => {
    const h = harness({ answer: relaycontrol({ scopes: [READ_SCOPE] }) });
    const res = await h.send(path, { body: "{}" });
    expect([res.status, await code(res)]).toEqual([403, "forbidden"]);
    expect(h.forwarded).toEqual([]);
  });

  test("an rth:sync-only token on the GETs is 403; a token with no rth scope is 403 everywhere", async () => {
    const h = harness({ answer: relaycontrol({ scopes: [SYNC_SCOPE] }) });
    expect((await h.send("/v1/onboarding/connection", { method: "GET" })).status).toBe(403);
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(403);
    const none = harness({ answer: relaycontrol({ scopes: ["cli:auth"] }) });
    expect((await none.send("/v1/delivery/limits", { method: "GET" })).status).toBe(403);
    expect((await none.send("/v1/delivery/batches", { body: "{}" })).status).toBe(403);
    expect(h.forwarded.length + none.forwarded.length).toBe(0);
  });

  test("an rth:read-only token reads the limits at the edge, but the Worker still wants rth:sync there", async () => {
    const h = harness({ answer: relaycontrol({ scopes: [READ_SCOPE] }) });
    const res = await h.send("/v1/delivery/limits", { method: "GET" });
    expect(h.forwarded.length).toBe(1);
    expect(res.status).toBe(403);
  });
});

describe("caching follows the shared client (#313)", () => {
  test("a cached active answer is not reused after exp", async () => {
    const clock = { ms: Date.now() };
    let revoked = false;
    const h = harness({ clock, answer: (b) => Response.json(revoked ? { active: false, reason: "expired" } : active({ exp: Math.floor(clock.ms / 1000) + 2 }, clock.ms)) });
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(200);
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(200);
    expect(h.rcCalls.length).toBe(1);
    clock.ms += 3_000;
    revoked = true;
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(401);
    expect(h.rcCalls.length).toBe(2);
    expect(h.forwarded.length).toBe(2);
  });

  test("a cached active answer is not reused across aud (one introspector, two edges)", async () => {
    const first = harness();
    expect((await first.send("/v1/delivery/limits", { method: "GET" })).status).toBe(200);
    const second = harness({ introspector: first.introspector, origin: OTHER_ORIGIN });
    expect((await second.send("/v1/delivery/limits", { method: "GET" })).status).toBe(401);
    expect(first.rcCalls.map((c) => c.body.aud)).toEqual([ORIGIN, OTHER_ORIGIN]);
    expect(second.forwarded).toEqual([]);
  });

  test("a cached active answer is not replayed for a different token", async () => {
    const h = harness({ answer: (b) => Response.json(b.token === TOKEN ? active() : { active: false, reason: "unknown" }) });
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(200);
    expect((await h.send("/v1/delivery/limits", { method: "GET", token: TOKEN_2 })).status).toBe(401);
    expect(h.rcCalls.map((c) => c.body.token)).toEqual([TOKEN, TOKEN_2]);
    expect(h.forwarded.length).toBe(1);
  });

  test("unavailability is not cached: the next request introspects again", async () => {
    let up = false;
    const h = harness({ answer: () => (up ? Response.json(active()) : new Response("", { status: 503 })) });
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(503);
    up = true;
    expect((await h.send("/v1/delivery/limits", { method: "GET" })).status).toBe(200);
  });
});

describe("413: DELIVERY_LIMITS.maxRequestBytes", () => {
  test("a body of exactly the limit is forwarded whole", async () => {
    const h = harness();
    const json = await deliveryBody();
    const body = json + " ".repeat(DELIVERY_LIMITS.maxRequestBytes - enc(json).byteLength);
    const res = await h.send("/v1/delivery/batches", { body });
    expect(res.status).toBe(200);
    expect(h.forwarded[0]!.body.byteLength).toBe(DELIVERY_LIMITS.maxRequestBytes);
  });

  test("a declared Content-Length over the limit is 413 before introspection", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/batches", { body: "{}", headers: { "content-length": String(MAX_DELIVERY_BYTES + 1) } });
    expect([res.status, await code(res)]).toEqual([413, "delivery_too_large"]);
    expect(h.rcCalls.length + h.forwarded.length).toBe(0);
  });

  test("a streamed body (no Content-Length) one byte over the limit is 413, nothing forwarded", async () => {
    const h = harness();
    const chunk = new Uint8Array(256 * 1024).fill(0x20);
    let sent = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (sent > MAX_DELIVERY_BYTES) return controller.close();
        const next = sent + chunk.byteLength > MAX_DELIVERY_BYTES + 1 ? chunk.subarray(0, MAX_DELIVERY_BYTES + 1 - sent) : chunk;
        sent += next.byteLength;
        controller.enqueue(next);
      },
    });
    const res = await h.edge.fetch(new Request(`${ORIGIN}/v1/delivery/batches`, { method: "POST", body: stream, headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` }, duplex: "half" } as RequestInit));
    expect([res.status, await code(res)]).toEqual([413, "delivery_too_large"]);
    expect(h.forwarded).toEqual([]);
  });
});

describe("request ids and logs", () => {
  test("a client correlation id is propagated to the Worker and echoed; every answer carries X-Request-Id", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/limits", { method: "GET", headers: { "x-correlation-id": "desktop-corr-42" } });
    expect(res.headers.get("x-correlation-id")).toBe("desktop-corr-42");
    expect(res.headers.get("x-request-id")).toMatch(/^[0-9a-f-]{36}$/);
    expect(h.forwarded[0]!.headers.get("x-correlation-id")).toBe("desktop-corr-42");
    const refused = await h.send("/v1/delivery/recovery", { body: "{}", headers: { "x-correlation-id": "desktop-corr-43" } });
    expect([refused.headers.get("x-correlation-id"), ((await refused.json()) as { correlationId: string }).correlationId]).toEqual(["desktop-corr-43", "desktop-corr-43"]);
    expect(refused.headers.get("x-request-id")).toMatch(/^[0-9a-f-]{36}$/);
  });

  test("an unsafe correlation id is replaced by the request id", async () => {
    const h = harness();
    const res = await h.send("/v1/delivery/limits", { method: "GET", headers: { "x-correlation-id": "a b<script>" } });
    expect(res.headers.get("x-correlation-id")).toBe(res.headers.get("x-request-id"));
    expect(h.forwarded[0]!.headers.get("x-correlation-id")).toBe(res.headers.get("x-request-id"));
  });

  test("no token, secret, signing key or identity header in any log line", async () => {
    const h = harness();
    await h.send("/v1/delivery/batches", { body: await deliveryBody() });
    await h.send("/v1/delivery/batches", { body: "{}", token: "sh_at_another-secret-token" });
    const failing = harness({ answer: () => new Response("", { status: 401 }) });
    await failing.send("/v1/delivery/batches", { body: "{}" });
    const all = JSON.stringify([h.logs, h.rcLogs, failing.logs, failing.rcLogs]);
    for (const secret of [TOKEN, "sh_at_another-secret-token", INTROSPECT_SECRET, SIGNING_KEY, h.forwarded[0]!.headers.get(IDENTITY_HEADER)!])
      expect(all.includes(secret)).toBe(false);
    expect(h.logs.map((l) => l.outcome)).toContain("forwarded");
  });

  test("the default logger writes only allowlisted fields and never the token", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const introspector = createRelaycontrolIntrospector({ url: "http://relaycontrol.test", secret: INTROSPECT_SECRET, log: () => {}, fetch: (async () => Response.json({ active: false, reason: "revoked" })) as typeof fetch });
    const edge = createIngestEdgeApp({ enabled: true, origin: ORIGIN, upstreamUrl: UPSTREAM, signingKey: SIGNING_KEY, introspector, fetch: (async () => new Response("")) as typeof fetch });
    const lines: string[] = [];
    const write = process.stdout.write.bind(process.stdout);
    process.stdout.write = ((chunk: string | Uint8Array) => (lines.push(String(chunk)), true)) as typeof process.stdout.write;
    try {
      expect((await edge.fetch(new Request(`${ORIGIN}/v1/delivery/batches`, { method: "POST", body: "{}", headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` } }))).status).toBe(401);
    } finally {
      process.stdout.write = write;
    }
    expect(lines.join("")).toContain("invalid_token");
    expect(lines.join("")).not.toContain(TOKEN);
  });
});

describe("dormant (SESSION_INGEST_EDGE_ENABLED off)", () => {
  test("liveness answers; every other route is 503 not_configured and nothing is called", async () => {
    const edge = createIngestEdgeApp({ enabled: false, log: () => {} });
    const health = await edge.fetch(new Request(`${ORIGIN}/healthz`));
    expect([health.status, await health.json()]).toEqual([200, { ok: true, enabled: false }]);
    for (const r of [...EDGE_ROUTES, { method: "POST", path: "/v1/delivery/recovery" }]) {
      const res = await edge.fetch(new Request(`${ORIGIN}${r.path}`, { method: r.method, headers: { authorization: `Bearer ${TOKEN}` } }));
      expect([res.status, await code(res)]).toEqual([503, "not_configured"]);
    }
  });

  test("enabled liveness reports enabled without introspecting", async () => {
    const h = harness();
    expect(await (await h.edge.fetch(new Request(`${ORIGIN}/healthz`))).json()).toEqual({ ok: true, enabled: true });
    expect(h.rcCalls).toEqual([]);
  });
});
