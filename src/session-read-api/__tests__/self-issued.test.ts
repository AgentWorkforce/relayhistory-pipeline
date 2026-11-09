/**
 * READ_API_AUTH_MODE=self_issued: the token routes, the verifier, audience isolation, tenant
 * isolation on every read route, CORS and fail-closed configuration. The store is an in-memory
 * stand-in for the SQL functions (self-issued.pglite.test.ts runs the real ones).
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type ReadApiDeps } from "../app.ts";
import { selfIssuedVerifier } from "../auth.ts";
import { eventMessageIdDeps, readApiAuthConfig, readApiAuthMode, readApiCorsOrigins } from "../lambda.ts";
import { ACCESS_PREFIX, ACCESS_TTL_SECONDS_DEFAULT, REFRESH_PREFIX, SERVICE_PREFIX, generateToken, hashToken, type NewTokenRow, type RefreshInput, type TokenRow, type TokenStore } from "../self-issued.ts";
import { memoryLiveness } from "./support.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";

const ORIGIN = "https://read.example.test";
const SECRET = "s".repeat(40);
const PREVIOUS = "p".repeat(40);
const A = { userId: "u-a", orgId: "org-a", workspaceId: "ws-a" };
const B = { userId: "u-b", orgId: "org-b", workspaceId: "ws-b" };

function memoryStore() {
  const rows = new Map<string, NewTokenRow>();
  const row = (r: NewTokenRow): TokenRow => ({ id: r.id, subjectType: r.subjectType, audience: r.audience, userId: r.userId, orgId: r.orgId, workspaceId: r.workspaceId, scopes: ["rth:read"] });
  let fail = false;
  const store: TokenStore = {
    async verify(hash, now) {
      if (fail) throw new Error("connection timeout SECRETVALUE");
      for (const r of rows.values()) if (r.accessHash === hash && r.accessExpires > now) return row(r);
      return null;
    },
    async issue(r) {
      if (fail) throw new Error("down");
      rows.set(r.id, r);
    },
    async refresh(i: RefreshInput) {
      if (fail) throw new Error("down");
      for (const r of rows.values())
        if (r.refreshHash === i.refreshHash && r.subjectType === "cli" && r.userId === i.userId && r.orgId === i.orgId && r.workspaceId === i.workspaceId && r.audience === i.audience && r.refreshExpires > i.now) {
          r.accessHash = i.nextAccessHash; r.accessExpires = i.accessExpires; r.refreshHash = i.nextRefreshHash; r.refreshExpires = i.refreshExpires;
          return row(r);
        }
      return null;
    },
    async deleteByHash(kind, hash) {
      for (const [id, r] of rows) if ((kind === "access" ? r.accessHash : r.refreshHash) === hash) return rows.delete(id);
      return false;
    },
    async deleteFor(orgId, f) {
      let n = 0;
      for (const [id, r] of rows) if (r.orgId === orgId && (!f.id || r.id === f.id) && (!f.userId || r.userId === f.userId) && (!f.workspaceId || r.workspaceId === f.workspaceId)) { rows.delete(id); n += 1; }
      return n;
    },
  };
  return { store, rows, setFail: (v: boolean) => { fail = v; } };
}

const stubCh: ClickHouseClient = { database: "t", async command() {}, async insert() {}, async select() { return [] as never[]; } };

function api(over: Partial<ReadApiDeps> & { secrets?: string[] } = {}) {
  const mem = memoryStore();
  const logs: Record<string, unknown>[] = [];
  const seen: Array<{ route: string; orgId: string; workspaceId: string }> = [];
  const directory = {
    async list(orgId: string, workspaceId: string) { seen.push({ route: "list", orgId, workspaceId }); return []; },
    async activity(orgId: string, workspaceId: string) { seen.push({ route: "activity", orgId, workspaceId }); return []; },
    async query(orgId: string, workspaceId: string) { seen.push({ route: "query", orgId, workspaceId }); return []; },
    async queryActivity(orgId: string, workspaceId: string) { seen.push({ route: "queryActivity", orgId, workspaceId }); return []; },
  } as unknown as ReadApiDeps["directory"];
  const app = createReadApi({
    verify: selfIssuedVerifier(mem.store, ORIGIN),
    ch: stubCh,
    liveness: memoryLiveness({}).liveness,
    completeSinceMs: 0,
    directory,
    directoryWorkspaces: "*",
    turns: { async harnesses(scope) { seen.push({ route: "turns", orgId: scope.orgId, workspaceId: scope.workspaceId }); return []; }, async source() { throw new Error("unused"); } },
    selfIssued: { store: mem.store, audience: ORIGIN, internalSecrets: over.secrets ?? [SECRET], accessTtlSeconds: ACCESS_TTL_SECONDS_DEFAULT },
    log: (f) => logs.push(f),
    ...over,
  });
  const post = (path: string, body: unknown, bearer: string | null = SECRET) =>
    app.request(path, { method: "POST", headers: { "content-type": "application/json", ...(bearer ? { authorization: `Bearer ${bearer}` } : {}) }, body: JSON.stringify(body) });
  const get = (path: string, token: string, headers: Record<string, string> = {}) => app.request(path, { headers: { authorization: `Bearer ${token}`, ...headers } });
  return { app, post, get, mem, logs, seen };
}

const json = async (r: Response) => (await r.json()) as Record<string, any>;
const issueBody = (who = A) => ({ ...who, audience: ORIGIN, scopes: ["rth:read"] });
const ISSUE = "/internal/v1/session-read/sessions";
const REFRESH = "/internal/v1/session-read/sessions/refresh";
const SERVICE = "/internal/v1/session-read/service-tokens";
const REVOKE = "/internal/v1/session-read/revoke";

describe("token shape", () => {
  test("256 bits after a distinct prefix, and only the hash reaches the store", async () => {
    const t = generateToken(ACCESS_PREFIX);
    expect(t.startsWith("rth_at_read_")).toBe(true);
    expect(t.slice(ACCESS_PREFIX.length)).toMatch(/^[A-Za-z0-9_-]{43}$/);
    const { post, mem } = api();
    const pair = await json(await post(ISSUE, issueBody()));
    const stored = [...mem.rows.values()][0]!;
    expect(stored.accessHash).toBe(await hashToken(pair.accessToken));
    expect(JSON.stringify(stored)).not.toContain(pair.accessToken);
    expect(JSON.stringify(stored)).not.toContain(pair.refreshToken);
    expect(new Date(pair.accessTokenExpiresAt).getTime() - Date.now()).toBeLessThanOrEqual(300_000);
    expect(new Date(pair.accessTokenExpiresAt).getTime() - Date.now()).toBeGreaterThan(290_000);
  });

  test("the frozen issue reply; a token is in no log line", async () => {
    const { post, logs } = api();
    const r = await post(ISSUE, { ...issueBody(), label: "desktop" });
    expect(r.status).toBe(201);
    const pair = await json(r);
    expect(Object.keys(pair).sort()).toEqual(["accessToken", "accessTokenExpiresAt", "audience", "orgId", "refreshToken", "refreshTokenExpiresAt", "scopes", "sessionId", "tokenType", "userId", "workspaceId"]);
    expect(pair).toMatchObject({ ...A, audience: ORIGIN, scopes: ["rth:read"], tokenType: "Bearer" });
    expect(JSON.stringify(logs)).not.toContain(pair.accessToken);
    expect(JSON.stringify(logs)).not.toContain(pair.refreshToken);
  });
});

describe("internal secret", () => {
  test("missing or wrong is 401; the secret is not echoed", async () => {
    const { post } = api();
    expect((await post(ISSUE, issueBody(), null)).status).toBe(401);
    const wrong = await post(ISSUE, issueBody(), "x".repeat(40));
    expect(wrong.status).toBe(401);
    expect(await wrong.text()).not.toContain("x".repeat(40));
  });

  test("rotation overlap: both accepted, and the old one stops once removed", async () => {
    const overlap = api({ secrets: [SECRET, PREVIOUS] });
    expect((await overlap.post(ISSUE, issueBody(), SECRET)).status).toBe(201);
    expect((await overlap.post(ISSUE, issueBody(), PREVIOUS)).status).toBe(201);
    const after = api({ secrets: [SECRET] });
    expect((await after.post(ISSUE, issueBody(), PREVIOUS)).status).toBe(401);
  });

  test("scope and audience are fixed", async () => {
    const { post } = api();
    expect((await post(ISSUE, { ...issueBody(), scopes: ["rth:read", "rth:sync"] })).status).toBe(400);
    expect((await post(ISSUE, { ...issueBody(), scopes: [] })).status).toBe(400);
    const r = await post(ISSUE, { ...issueBody(), audience: "https://other.example.test" });
    expect(r.status).toBe(403);
    expect((await json(r)).error.code).toBe("audience_mismatch");
    expect((await post(ISSUE, { ...issueBody(), orgId: "bad id" })).status).toBe(400);
  });
});

describe("refresh", () => {
  test("rotates; the old pair stops working", async () => {
    const { post, get } = api();
    const first = await json(await post(ISSUE, issueBody()));
    const r = await post(REFRESH, { ...issueBody(), refreshToken: first.refreshToken });
    expect(r.status).toBe(200);
    const next = await json(r);
    expect(next.sessionId).toBe(first.sessionId);
    expect(next.refreshToken).not.toBe(first.refreshToken);
    expect((await post(REFRESH, { ...issueBody(), refreshToken: first.refreshToken })).status).toBe(401);
    expect((await get("/v1/sessions?workspace=ws-a", first.accessToken)).status).toBe(401);
    expect((await get("/v1/sessions?workspace=ws-a", next.accessToken)).status).toBe(200);
  });

  test("bound to the exact identity and audience", async () => {
    const { post } = api();
    const pair = await json(await post(ISSUE, issueBody()));
    for (const other of [{ userId: "u-x" }, { orgId: "org-x" }, { workspaceId: "ws-x" }])
      expect((await post(REFRESH, { ...issueBody(), ...other, refreshToken: pair.refreshToken })).status).toBe(401);
    expect((await post(REFRESH, { ...issueBody(), audience: "https://other.example.test", refreshToken: pair.refreshToken })).status).toBe(403);
    expect((await post(REFRESH, { ...issueBody(), refreshToken: pair.refreshToken })).status).toBe(200);
  });

  test("an access token, a service token and a foreign token are not refresh tokens", async () => {
    const { post } = api();
    const pair = await json(await post(ISSUE, issueBody()));
    const service = await json(await post(SERVICE, { ...issueBody(), label: "ci" }));
    for (const refreshToken of [pair.accessToken, service.token, "rth_rt_foreign", "sh_at_x"])
      expect((await post(REFRESH, { ...issueBody(), refreshToken })).status).toBe(401);
  });
});

describe("revocation", () => {
  test("offboarding a user is immediate and ends the refresh family; another org is untouched", async () => {
    const { post, get } = api();
    const a = await json(await post(ISSUE, issueBody()));
    const b = await json(await post(ISSUE, issueBody(B)));
    expect(await json(await post(REVOKE, { orgId: "org-a", userId: "u-a" }))).toEqual({ revoked: 1 });
    expect((await get("/v1/sessions?workspace=ws-a", a.accessToken)).status).toBe(401);
    expect((await post(REFRESH, { ...issueBody(), refreshToken: a.refreshToken })).status).toBe(401);
    expect((await get("/v1/sessions?workspace=ws-b", b.accessToken)).status).toBe(200);
  });

  test("an id from another org matches nothing", async () => {
    const { post, get } = api();
    const a = await json(await post(ISSUE, issueBody()));
    expect(await json(await post(REVOKE, { orgId: "org-b", id: a.sessionId }))).toEqual({ revoked: 0 });
    expect((await get("/v1/sessions?workspace=ws-a", a.accessToken)).status).toBe(200);
  });

  test("a holder revokes its own session; a foreign token never reaches the store", async () => {
    const { post, get } = api();
    const a = await json(await post(ISSUE, issueBody()));
    expect(await json(await post("/v1/auth/token/revoke", { token: a.refreshToken }, null))).toEqual({ revoked: true });
    expect((await get("/v1/sessions?workspace=ws-a", a.accessToken)).status).toBe(401);
    expect(await json(await post("/v1/auth/token/revoke", { token: "sh_at_x" }, null))).toEqual({ revoked: false });
  });
});

describe("audience isolation", () => {
  test("the read API refuses every other family, without a store lookup", async () => {
    const { get, mem } = api();
    mem.setFail(true); // a lookup would answer 503, not 401
    for (const foreign of ["sh_at_abc", "rth_at_notours", "rth_st_notours", "rth_rt_notours", "at_live_abc", "rk_live_abc", "header.payload.signature"]) {
      const r = await get("/v1/sessions?workspace=ws-a", foreign);
      expect(r.status).toBe(401);
      expect((await json(r)).error.code).toBe("invalid_token");
    }
  });

  test("a row for another audience is refused", async () => {
    const { store } = memoryStore();
    const token = generateToken(ACCESS_PREFIX);
    const now = new Date();
    await store.issue({ id: "1", family: "f", subjectType: "cli", audience: "https://other.example.test", ...A, accessHash: await hashToken(token), accessExpires: new Date(now.getTime() + 60_000), refreshHash: "r", refreshExpires: new Date(now.getTime() + 60_000), label: null, now });
    await expect(selfIssuedVerifier(store, ORIGIN)(token)).rejects.toMatchObject({ status: 401, code: "invalid_token" });
  });

  test("read API tokens carry none of the prefixes ingest, Control or Relaycast accept", async () => {
    const { isRelaycontrolToken } = await import("@rhc/auth/relaycontrol.ts");
    for (const prefix of [ACCESS_PREFIX, REFRESH_PREFIX, SERVICE_PREFIX]) {
      const token = generateToken(prefix);
      expect(isRelaycontrolToken(token)).toBe(false);
      for (const relaycast of ["rk_live_", "at_live_", "nt_live_", "ot_live_", "wh_live_", "ar_"]) expect(token.startsWith(relaycast)).toBe(false);
    }
  });

  test("the ingest edge refuses a read API token without introspecting it", async () => {
    const { relaycontrolIngestIdentity } = await import("../../session-ingest/selfhost-auth.ts");
    let introspected = 0;
    const verify = relaycontrolIngestIdentity({ origin: ORIGIN } as never, { verifyRelaycontrolToken: async () => { introspected += 1; throw new Error("unused"); } } as never);
    await expect(verify(`Bearer ${generateToken(ACCESS_PREFIX)}`)).rejects.toMatchObject({ status: 401 });
    expect(introspected).toBe(0);
  });
});

describe("tenant isolation", () => {
  const routes = ["/v1/sessions", "/v1/sessions/activity", "/v1/sessions/s-1/turns", "/v1/events"];
  const query = (route: string, workspace: string) =>
    `${route}?workspace=${workspace}${route === "/v1/sessions/activity" ? "&since=2026-01-01&until=2026-01-02" : route === "/v1/events" ? "&q=x" : ""}`;

  test("a token naming another workspace is 403 on list, turns, search and activity, and reads nothing", async () => {
    const { post, get, seen } = api();
    const a = await json(await post(ISSUE, issueBody()));
    for (const route of routes) {
      const r = await get(query(route, "ws-b"), a.accessToken);
      expect(r.status).toBe(403);
      expect((await json(r)).error.code).toBe("forbidden");
    }
    expect(seen).toEqual([]);
  });

  test("a read binds org and workspace from the token row only", async () => {
    const { post, get, seen } = api();
    const a = await json(await post(ISSUE, issueBody()));
    for (const route of ["/v1/sessions", "/v1/sessions/activity", "/v1/sessions/s-1/turns"]) await get(`${query(route, "ws-a")}&orgId=org-b&org=org-b`, a.accessToken);
    expect(seen.length).toBe(3);
    for (const call of seen) expect(call).toMatchObject({ orgId: "org-a", workspaceId: "ws-a" });
  });
});

describe("fail closed", () => {
  test("a store failure on lookup denies with 503, no data and no detail", async () => {
    const { post, get, mem, seen } = api();
    const a = await json(await post(ISSUE, issueBody()));
    mem.setFail(true);
    const r = await get("/v1/sessions?workspace=ws-a", a.accessToken);
    expect(r.status).toBe(503);
    expect((await json(r)).error.code).toBe("token_store_unavailable");
    expect(seen).toEqual([]);
  });

  test("a store failure on issue is 503 without the error text", async () => {
    const { post, mem } = api();
    mem.setFail(true);
    const r = await post(ISSUE, issueBody());
    expect(r.status).toBe(503);
    expect(await r.text()).not.toContain("down");
  });

  test("an expired access token is refused", async () => {
    const { store } = memoryStore();
    const token = generateToken(ACCESS_PREFIX);
    const now = new Date();
    await store.issue({ id: "1", family: "f", subjectType: "cli", audience: ORIGIN, ...A, accessHash: await hashToken(token), accessExpires: new Date(now.getTime() - 1), refreshHash: "r", refreshExpires: new Date(now.getTime() + 60_000), label: null, now });
    await expect(selfIssuedVerifier(store, ORIGIN)(token)).rejects.toBeInstanceOf(AuthError);
  });

  const env = { READ_API_AUTH_MODE: "self_issued", READ_API_ORIGIN: ORIGIN, READ_API_INTERNAL_SECRET: SECRET };
  test("a valid configuration, with the 5 minute default", () => {
    expect(readApiAuthMode(env)).toBe("self_issued");
    expect(readApiAuthConfig(env).selfIssued).toEqual({ audience: ORIGIN, internalSecrets: [SECRET], accessTtlSeconds: 300 });
    expect(readApiAuthConfig({ ...env, READ_API_ACCESS_TOKEN_TTL_SECONDS: "120", READ_API_INTERNAL_SECRET_PREVIOUS: PREVIOUS }).selfIssued).toEqual({ audience: ORIGIN, internalSecrets: [SECRET, PREVIOUS], accessTtlSeconds: 120 });
  });

  test.each([
    ["no secret", { READ_API_INTERNAL_SECRET: undefined }],
    ["a short secret", { READ_API_INTERNAL_SECRET: "short" }],
    ["a padded secret", { READ_API_INTERNAL_SECRET: ` ${SECRET}` }],
    ["a previous equal to current", { READ_API_INTERNAL_SECRET_PREVIOUS: SECRET }],
    ["no origin", { READ_API_ORIGIN: undefined }],
    ["an origin that is not one", { READ_API_ORIGIN: "not a url" }],
    ["a RelayAuth setting", { RELAYAUTH_JWKS_URL: "https://x.example.test/jwks" }],
    ["relaycontrol introspection", { RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: "true" }],
    ["a TTL of zero", { READ_API_ACCESS_TOKEN_TTL_SECONDS: "0" }],
    ["a TTL over the 5 minute cap", { READ_API_ACCESS_TOKEN_TTL_SECONDS: "301" }],
    ["a TTL that is not a number", { READ_API_ACCESS_TOKEN_TTL_SECONDS: "5m" }],
  ] as Array<[string, Record<string, string | undefined>]>)("refuses to start with %s", (_name, over) => {
    expect(() => readApiAuthConfig({ ...env, ...over })).toThrow();
  });

  test("an unknown mode refuses, and errors never echo a secret", () => {
    expect(() => readApiAuthMode({ READ_API_AUTH_MODE: "self-issued" })).toThrow();
    try {
      readApiAuthConfig({ ...env, READ_API_INTERNAL_SECRET: "tooshortsecretvalue" });
      throw new Error("unreachable");
    } catch (error) {
      expect(String(error)).not.toContain("tooshortsecretvalue");
    }
  });
});

describe("cors", () => {
  const app = () => api({ corsAllowedOrigins: new Set(["https://app.example.test"]) });
  const origin = { origin: "https://app.example.test" };

  test("an allowlisted origin reads with no credentials allowed", async () => {
    const { post, get } = app();
    const a = await json(await post(ISSUE, issueBody()));
    const r = await get("/v1/sessions?workspace=ws-a", a.accessToken, origin);
    expect(r.status).toBe(200);
    expect(r.headers.get("access-control-allow-origin")).toBe("https://app.example.test");
    expect(r.headers.get("access-control-allow-credentials")).toBeNull();
    expect(r.headers.get("vary")).toContain("Origin");
    expect(r.headers.get("access-control-expose-headers")).toContain("X-Relayhistory-Workspace-Id");
  });

  test("preflight skips auth; a foreign origin gets no allow headers", async () => {
    const { app: a } = app();
    const ok = await a.request("/v1/sessions", { method: "OPTIONS", headers: origin });
    expect(ok.status).toBe(204);
    expect(ok.headers.get("access-control-allow-methods")).toBe("GET");
    expect(ok.headers.get("access-control-allow-headers")).toContain("authorization");
    const foreign = await a.request("/v1/sessions", { method: "OPTIONS", headers: { origin: "https://evil.example.test" } });
    expect(foreign.headers.get("access-control-allow-origin")).toBeNull();
  });

  test("token routes are never CORS-enabled", async () => {
    const { post, app: a } = app();
    const pre = await a.request("/v1/auth/token/revoke", { method: "OPTIONS", headers: origin });
    expect(pre.headers.get("access-control-allow-origin")).toBeNull();
    const internal = await post(ISSUE, issueBody());
    expect(internal.headers.get("access-control-allow-origin")).toBeNull();
    const revoke = await a.request("/v1/auth/token/revoke", { method: "POST", headers: { ...origin, "content-type": "application/json" }, body: JSON.stringify({ token: "sh_at_x" }) });
    expect(revoke.headers.get("access-control-allow-origin")).toBeNull();
  });

  test("the allowlist is exact", () => {
    expect([...readApiCorsOrigins({ READ_API_CORS_ALLOWED_ORIGINS: "https://a.example.test, http://localhost:3000" })]).toEqual(["https://a.example.test", "http://localhost:3000"]);
    expect(readApiCorsOrigins({}).size).toBe(0);
    for (const bad of ["*", "https://a.example.test/app", "http://a.example.test", "https://*.example.test", "ftp://a.example.test"])
      expect(() => readApiCorsOrigins({ READ_API_CORS_ALLOWED_ORIGINS: bad })).toThrow();
  });
});

describe("includeMessageId", () => {
  const cand = { ts_ms: Date.parse("2026-09-02T00:00:00Z"), session_id: "s-1", source: "codex", machine_id: "m-1", kind: "message", event_id: "e-1" };
  const full = { ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-a", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "text", in_budget: 1, origin: "canonical", origin_claim_token: "" };
  const ch: ClickHouseClient = {
    database: "t", async command() {}, async insert() {},
    async select<T>(query: string) { return (query.includes("search_ingest_session_state") ? [] : query.includes("bytes_before") ? [full] : [cand]) as T[]; },
  };
  const liveness = memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-09-01T00:00:00Z") } }).liveness;
  const row = (messageId: string | null, over: Record<string, unknown> = {}) => ({ table: "convergence_events", row: { event_id: "e-1", machine_id: "m-1", kind: "message", record: { payload: { message_id: messageId } }, ...over } });
  const URL = "/v1/sessions/s-1/events?workspace=ws-a&source=codex&includeMessageId=true";
  const session = async (a: ReturnType<typeof api>) => (await json(await a.post(ISSUE, issueBody()))).accessToken as string;

  test("fills messageId from canonical rows, bound to the token's tenant", async () => {
    const seen: unknown[] = [];
    const a = api({ ch, liveness, eventMessageIds: { async source(identity) { seen.push(identity); return { legacyCoverage: null, rows: [row("msg-1")] }; } } });
    const r = await a.get(URL, await session(a));
    expect(r.status).toBe(200);
    expect((await json(r)).events[0].messageId).toBe("msg-1");
    expect(seen).toEqual([{ orgId: "org-a", workspaceId: "ws-a", sessionId: "s-1", source: "codex" }]);
  });

  test("null when the canonical record has no id", async () => {
    const a = api({ ch, liveness, eventMessageIds: { async source() { return { legacyCoverage: null, rows: [row(null)] }; } } });
    expect((await json(await a.get(URL, await session(a)))).events[0].messageId).toBeNull();
  });

  test("an event the canonical objects do not hold is a 501, not a null id", async () => {
    const a = api({ ch, liveness, eventMessageIds: { async source() { return { legacyCoverage: null, rows: [row("other", { event_id: "e-2" })] }; } } });
    const r = await a.get(URL, await session(a));
    expect(r.status).toBe(501);
    expect((await json(r)).error.reason).toBe("message_id_no_canonical_event");
  });

  test("a refused canonical read is a 501, never a page of nulls; source is required", async () => {
    const { CanonicalReadError } = await import("../canonical.ts");
    const a = api({ ch, liveness, eventMessageIds: { async source() { throw new CanonicalReadError("too_large"); } } });
    const token = await session(a);
    const r = await a.get(URL, token);
    expect(r.status).toBe(501);
    expect((await json(r)).error.reason).toBe("message_id_canonical_too_large");
    const noSource = await a.get("/v1/sessions/s-1/events?workspace=ws-a&includeMessageId=true", token);
    expect((await json(noSource)).error.reason).toBe("message_id_source_required");
  });

  test("READ_API_EVENT_MESSAGE_IDS is default off and strict", () => {
    const turns = { source: async () => ({ legacyCoverage: null, rows: [] }) };
    for (const value of [undefined, "", "false"]) expect(eventMessageIdDeps({ READ_API_EVENT_MESSAGE_IDS: value }, turns)).toEqual({});
    expect(eventMessageIdDeps({ READ_API_EVENT_MESSAGE_IDS: "true" }, turns)).toEqual({ eventMessageIds: turns });
    expect(eventMessageIdDeps({ READ_API_EVENT_MESSAGE_IDS: "true" }, undefined)).toEqual({});
    for (const value of ["TRUE", " true", "1"]) expect(() => eventMessageIdDeps({ READ_API_EVENT_MESSAGE_IDS: value }, turns)).toThrow(/READ_API_EVENT_MESSAGE_IDS/);
  });

  test("flag off: /events is exactly as before, the parameter 501s and the response has no messageId", async () => {
    const turns = { async source() { throw new Error("must not be read"); } };
    const off = api({ ch, liveness, ...eventMessageIdDeps({}, turns) });
    const token = await session(off);
    expect((await json(await off.get(URL, token))).error.reason).toBe("param_includeMessageId");
    const plain = await off.get("/v1/sessions/s-1/events?workspace=ws-a&source=codex", token);
    expect(plain.status).toBe(200);
    expect("messageId" in (await json(plain)).events[0]).toBe(false);
  });

  test("absent on the Lambda and Worker: the 501 is unchanged; omitted, no read happens", async () => {
    const off = api({ ch, liveness });
    expect((await json(await off.get(URL, await session(off)))).error.reason).toBe("param_includeMessageId");
    let reads = 0;
    const a = api({ ch, liveness, eventMessageIds: { async source() { reads += 1; return { legacyCoverage: null, rows: [] }; } } });
    const r = await a.get("/v1/sessions/s-1/events?workspace=ws-a&source=codex", await session(a));
    expect(r.status).toBe(200);
    expect((await json(r)).events[0].messageId).toBeUndefined();
    expect(reads).toBe(0);
  });
});

describe("default off", () => {
  test("without selfIssued, none of the token routes exist", async () => {
    const app = createReadApi({ verify: async () => { throw new AuthError(401, "invalid_token", "x"); }, ch: stubCh, liveness: memoryLiveness({}).liveness, completeSinceMs: 0 });
    for (const path of [ISSUE, REFRESH, SERVICE, REVOKE]) {
      const r = await app.request(path, { method: "POST" });
      expect(r.status).toBe(501);
      expect((await json(r)).error.code).toBe("unsupported_by_backend");
    }
  });
  test("unset READ_API_AUTH_MODE is unchanged", () => {
    expect(readApiAuthMode({})).toBe("default");
    expect(readApiAuthConfig({ READ_API_ORIGIN: ORIGIN }).mode).toBeUndefined();
  });
});
