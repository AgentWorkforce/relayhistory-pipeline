/**
 * The relaygate v1 upstream assertion on the self-host read API service (server.ts): off is unchanged, log
 * continues and logs a closed reason, enforce answers the generic 401 before the app (and before the body is used).
 * Keys are generated in memory; the signer is test-only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createReadApi, type ReadApiDeps } from "../app.ts";
import { selfIssuedVerifier } from "../auth.ts";
import { ACCESS_TTL_SECONDS_DEFAULT, type TokenStore } from "../self-issued.ts";
import { startReadApiService, type ReadApiService } from "../server.ts";
import { memoryLiveness } from "./support.ts";
import { logLine } from "../../shared/log.ts";
import { UPSTREAM_ASSERTION_LOG_MESSAGE, upstreamAssertionLogFields } from "../../shared/upstream-assertion.ts";
import { signAssertion, testKey, type SignInput } from "../../shared/__tests__/upstream-assertion-signer.ts";

const LOOPBACK = { host: "127.0.0.1", port: 0 };
const KEY = testKey("read-k1");
const AUTH = "Bearer sh_at_reader-secret-token";
const SESSION = "sess-private-0001";
const INTERNAL_SECRET = "i".repeat(40);
const ORIGIN = "https://read.example.test";

const running: ReadApiService[] = [];
afterEach(async () => {
  for (const s of running.splice(0)) await s.close();
});

const base = { RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: "{}" };
const keyed = (mode: string | undefined) => ({
  ...base,
  ...(mode === undefined ? {} : { READ_API_UPSTREAM_ASSERTION_MODE: mode }),
  READ_API_UPSTREAM_ASSERTION_PUBLIC_KEY: KEY.publicB64,
  READ_API_UPSTREAM_ASSERTION_KID: KEY.kid,
});

type Seen = { method: string; path: string; body: Uint8Array };
type Build = NonNullable<NonNullable<Parameters<typeof startReadApiService>[1]>["build"]>;
function recording(seen: Seen[]): Build {
  return async () => ({
    app: {
      async fetch(req: Request) {
        seen.push({ method: req.method, path: new URL(req.url).pathname, body: new Uint8Array(await req.arrayBuffer()) });
        return Response.json({ ok: true });
      },
    },
    close: async () => {},
  });
}

/** The real read app with self-issued tokens (internal secret), over an in-memory token store. */
const realSelfIssued: Build = async () => {
  const store: TokenStore = {
    async verify() { return null; },
    async issue() {},
    async refresh() { return null; },
    async deleteByHash() { return false; },
    async deleteFor() { return 0; },
  };
  const app = createReadApi({
    verify: selfIssuedVerifier(store, ORIGIN),
    ch: { database: "t", async command() {}, async insert() {}, async select() { return [] as never[]; } } as unknown as ReadApiDeps["ch"],
    liveness: memoryLiveness({}).liveness,
    completeSinceMs: 0,
    selfIssued: { store, audience: ORIGIN, internalSecrets: [INTERNAL_SECRET], accessTtlSeconds: ACCESS_TTL_SECONDS_DEFAULT },
  });
  return { app, close: async () => {} };
};

async function start(env: NodeJS.ProcessEnv, build: Build) {
  const service = await startReadApiService(env, { ...LOOPBACK, build });
  running.push(service);
  return (path: string) => `http://127.0.0.1:${service.port}${path}`;
}

/** Captures the service's stderr log lines (warn/error) for the duration of `fn`. */
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

const sign = (path: string, over: Partial<SignInput> = {}) =>
  signAssertion({ key: KEY, aud: "session-read-api", method: "GET", path, authorization: AUTH, access: "read", sessions: [SESSION], ...over });
const get = (url: string, assertion?: string, authorization: string | null = AUTH) =>
  fetch(url, { headers: { ...(authorization === null ? {} : { authorization }), ...(assertion === undefined ? {} : { "x-relaygate-assertion": assertion }) } });
const code = async (r: Response) => ((await r.json()) as { error?: { code?: string } }).error?.code;

const EVENTS = `/v1/sessions/${SESSION}/events`;

describe("off", () => {
  test.each([undefined, "", "off"])("mode %p: unchanged with and without the header; key variables are not read", async (mode) => {
    const seen: Seen[] = [];
    const url = await start({ ...base, ...(mode === undefined ? {} : { READ_API_UPSTREAM_ASSERTION_MODE: mode }), READ_API_UPSTREAM_ASSERTION_PUBLIC_KEY: "garbage", READ_API_UPSTREAM_ASSERTION_KID: "bad kid" }, recording(seen));
    const { result, lines } = await capture(async () => [await get(url(EVENTS)), await get(url(EVENTS), "v1.junk.junk"), await get(url("/v1/whatever"), undefined, null)]);
    for (const r of result) {
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual({ ok: true });
    }
    expect(seen.map((s) => s.path)).toEqual([EVENTS, EVENTS, "/v1/whatever"]);
    expect(lines).toEqual([]);
  });
  test("a bad mode refuses startup naming the variable only; the dormant service reads nothing", async () => {
    await expect(startReadApiService({ ...base, READ_API_UPSTREAM_ASSERTION_MODE: "strict" }, { ...LOOPBACK, build: recording([]) })).rejects.toThrow("READ_API_UPSTREAM_ASSERTION_MODE must be off, log or enforce");
    await expect(startReadApiService({ ...keyed("enforce"), READ_API_UPSTREAM_ASSERTION_KID: undefined }, { ...LOOPBACK, build: recording([]) })).rejects.toThrow("READ_API_UPSTREAM_ASSERTION_KID");
    const url = await start({ READ_API_UPSTREAM_ASSERTION_MODE: "strict" }, recording([]));
    expect((await fetch(url("/v1/sessions"))).status).toBe(503);
  });
});

describe("log", () => {
  test("a missing or invalid assertion continues unchanged and logs one closed reason", async () => {
    const seen: Seen[] = [];
    const url = await start(keyed("log"), recording(seen));
    const bad = sign(EVENTS, { aud: "ingest-edge" });
    const { result, lines } = await capture(async () => [await get(url(EVENTS)), await get(url(EVENTS), bad), await get(url(EVENTS), sign(EVENTS))]);
    expect(result.map((r) => r.status)).toEqual([200, 200, 200]);
    expect(seen).toHaveLength(3);
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(parsed.map((l) => [l.message, l.route, l.outcome, l.reason, l.state])).toEqual([
      [UPSTREAM_ASSERTION_LOG_MESSAGE, "upstream_assertion", "would_refuse", "missing", "log"],
      [UPSTREAM_ASSERTION_LOG_MESSAGE, "upstream_assertion", "would_refuse", "aud", "log"],
    ]);
    for (const line of lines) {
      expect(line).not.toContain(bad);
      for (const segment of bad.split(".").slice(1)) expect(line).not.toContain(segment);
      expect(line).not.toContain(AUTH);
      expect(line).not.toContain("sh_at_reader-secret-token");
      expect(line).not.toContain(SESSION);
      expect(line).not.toContain("test-jti-1");
      expect(line).not.toContain("agent-test");
    }
  });
});

describe("enforce", () => {
  test("missing / invalid on a table route and an unknown route are the generic 401, the app is not called", async () => {
    const seen: Seen[] = [];
    const url = await start(keyed("enforce"), recording(seen));
    const { result, lines } = await capture(async () => [
      await get(url(EVENTS)),
      await get(url(EVENTS), sign(EVENTS, { sessions: ["other"] })),
      await get(url("/v1/unknown/thing")),
      await get(url("/v1/unknown/thing"), sign("/v1/unknown/thing", { sessions: [SESSION] })),
      await get(url(EVENTS), sign(EVENTS), null),
    ]);
    expect(result.map((r) => r.status)).toEqual([401, 401, 401, 401, 401]);
    expect(await Promise.all(result.map(code))).toEqual(["missing_upstream_assertion", "invalid_upstream_assertion", "missing_upstream_assertion", "invalid_upstream_assertion", "invalid_upstream_assertion"]);
    expect(seen).toEqual([]);
    expect(lines.map((l) => (JSON.parse(l) as { outcome: string; reason: string }).reason)).toEqual(["missing", "scope", "missing", "scope", "credential"]);
    expect(lines.every((l) => (JSON.parse(l) as { outcome: string }).outcome === "refused")).toBe(true);
    for (const line of lines) expect(line).not.toContain(SESSION);
  });
  test("a valid assertion passes on a table route and on an unknown route (empty scope); a previous key works", async () => {
    const seen: Seen[] = [];
    const previous = testKey("read-k0");
    const url = await start({ ...keyed("enforce"), READ_API_UPSTREAM_ASSERTION_PUBLIC_KEY_PREVIOUS: previous.publicB64, READ_API_UPSTREAM_ASSERTION_KID_PREVIOUS: previous.kid }, recording(seen));
    const listing = "/v1/sessions?limit=5&workspace=ws-a";
    expect((await get(url(EVENTS), sign(EVENTS))).status).toBe(200);
    expect((await get(url(listing), sign(listing, { access: "filter", sessions: [] }))).status).toBe(200);
    expect((await get(url("/v1/unknown/thing"), sign("/v1/unknown/thing", { access: "aggregate", sessions: [] }))).status).toBe(200);
    expect((await get(url(EVENTS), sign(EVENTS, { key: previous }))).status).toBe(200);
    expect(seen.map((s) => s.path)).toEqual([EVENTS, "/v1/sessions", "/v1/unknown/thing", EVENTS]);
  });
  test("bypass: /healthz and OPTIONS need no assertion", async () => {
    const seen: Seen[] = [];
    const url = await start({ ...keyed("enforce"), READ_API_CORS_ALLOWED_ORIGINS: "https://app.example.test" }, recording(seen));
    expect((await fetch(url("/healthz"))).status).toBe(200);
    expect((await fetch(url(EVENTS), { method: "OPTIONS" })).status).toBe(200);
    expect(seen.map((s) => s.method)).toEqual(["OPTIONS"]);
  });
  test("phase 2: a POST body that does not match body_sha256 is refused before the app sees it", async () => {
    const seen: Seen[] = [];
    const url = await start(keyed("enforce"), recording(seen));
    const login = "/v1/cli/login";
    const body = JSON.stringify({ agentRelayToken: "t" });
    const assertion = sign(login, { method: "POST", access: "passthrough", sessions: [], body });
    const post = (b: string) => fetch(url(login), { method: "POST", body: b, headers: { authorization: AUTH, "content-type": "application/json", "x-relaygate-assertion": assertion } });
    const refused = await capture(() => post(`${body} `));
    expect(refused.result.status).toBe(401);
    expect(await code(refused.result)).toBe("invalid_upstream_assertion");
    expect(refused.lines.map((l) => (JSON.parse(l) as { reason: string }).reason)).toEqual(["body"]);
    expect(seen).toEqual([]);
    expect((await post(body)).status).toBe(200);
    expect(Buffer.from(seen[0]!.body).toString("utf8")).toBe(body);
    // A GET's body is drained unread: its hash binds zero bytes.
    expect((await get(url(EVENTS), sign(EVENTS, { body: "x" }))).status).toBe(401);
  });
  test("exempt POST /v1/auth/token/revoke: passes without an assertion, a presented bad one is refused", async () => {
    const seen: Seen[] = [];
    const url = await start({ ...keyed("enforce"), READ_API_AUTH_MODE: "self_issued" }, recording(seen));
    const revoke = (headers: Record<string, string>) => fetch(url("/v1/auth/token/revoke"), { method: "POST", body: "{}", headers: { "content-type": "application/json", ...headers } });
    expect((await revoke({})).status).toBe(200);
    expect(seen.map((s) => s.path)).toEqual(["/v1/auth/token/revoke"]);
    const bad = await capture(() => revoke({ authorization: AUTH, "x-relaygate-assertion": sign("/v1/auth/token/revoke", { method: "POST", access: "passthrough", sessions: [], body: "{ }" }) }));
    expect(bad.result.status).toBe(401);
    expect(seen).toHaveLength(1);
    // Only the exact method + path is exempt.
    expect(await code(await fetch(url("/v1/auth/token/revoke")))).toBe("missing_upstream_assertion");
    expect(await code(await fetch(url("/v1/auth/token/revoke/"), { method: "POST", body: "{}" }))).toBe("missing_upstream_assertion");
  });
  test("internal token routes: no assertion needed, the internal secret decides as today", async () => {
    const url = await start({ ...keyed("enforce"), READ_API_AUTH_MODE: "self_issued" }, realSelfIssued);
    const issue = (secret: string | null) =>
      fetch(url("/internal/v1/session-read/sessions"), {
        method: "POST",
        headers: { "content-type": "application/json", ...(secret ? { authorization: `Bearer ${secret}` } : {}) },
        body: JSON.stringify({ userId: "u-a", orgId: "org-a", workspaceId: "ws-a", audience: ORIGIN, scopes: ["rth:read"] }),
      });
    expect((await issue(INTERNAL_SECRET)).status).toBe(201);
    const wrong = await issue("w".repeat(40));
    expect(wrong.status).toBe(401);
    expect(await code(wrong)).not.toMatch(/upstream_assertion/);
    expect((await issue(null)).status).toBe(401);
    // No prefix exemption: another internal path needs an assertion.
    expect(await code(await fetch(url("/internal/v1/session-read/other"), { method: "POST", body: "{}" }))).toBe("missing_upstream_assertion");
    // Without self_issued the internal paths are not token routes: an assertion is required.
    const plain = await start(keyed("enforce"), recording([]));
    expect(await code(await fetch(plain("/internal/v1/session-read/sessions"), { method: "POST", body: "{}", headers: { authorization: `Bearer ${INTERNAL_SECRET}` } }))).toBe("missing_upstream_assertion");
  });
});

describe("log line", () => {
  test("the real logLine keeps every field of the refusal line, closed values only", () => {
    for (const mode of ["log", "enforce"] as const) {
      const line = JSON.parse(logLine("warn", UPSTREAM_ASSERTION_LOG_MESSAGE, upstreamAssertionLogFields(mode, "bad_signature", "corr-1"))) as Record<string, unknown>;
      expect(line).toMatchObject({ level: "warn", message: UPSTREAM_ASSERTION_LOG_MESSAGE, route: "upstream_assertion", outcome: mode === "enforce" ? "refused" : "would_refuse", reason: "bad_signature", state: mode, correlation_id: "corr-1" });
    }
  });
});
