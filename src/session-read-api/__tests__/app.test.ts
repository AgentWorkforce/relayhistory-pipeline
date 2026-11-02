/**
 * The HTTP surface: auth, workspace attestation, the 501 fallback contract,
 * the history floor, erasure gating and headers. A stub ClickHouse records the
 * queries it is asked; result-level behaviour is in page.clickhouse.test.ts.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { ClickHouseError } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, continuationUntil, createReadApi, type VerifiedIdentity } from "../app.ts";
import { relayAuthVerifier } from "../auth.ts";
import { encodeCursor } from "../cursor.ts";
import { decodeDirectoryCursor, encodeDirectoryCursor, likePattern, normalizeDirectoryQuery, parseDirectoryWorkspaces, type DirectoryRow, type DirectorySearch, type DirectorySearchInput } from "../directory.ts";
import { handleUrlEvent, parseCompleteSince, parseReadApiSecret } from "../lambda.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

function stubCh(rows: { candidates?: unknown[]; newest?: unknown[]; partial?: string[]; fail?: Error } = {}) {
  const queries: string[] = [];
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      queries.push(query);
      if (rows.fail) throw rows.fail;
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return (rows.partial ?? []).filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      return ((query.includes("bytes_before") ? rows.newest : rows.candidates) ?? []) as T[];
    },
  };
  /** Queries against transcript_events only (the partial-session checks read the state table). */
  const eventQueries = () => queries.filter((q) => !q.includes("search_ingest_session_state"));
  return { ch, queries, eventQueries };
}

function api(over: { identity?: VerifiedIdentity; completeSinceMs?: number; ch?: ClickHouseClient; liveness?: ReturnType<typeof memoryLiveness>["liveness"]; verify?: (t: string) => Promise<VerifiedIdentity> } = {}) {
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({
    verify: over.verify ?? (async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return over.identity ?? ID;
    }),
    ch: over.ch ?? stubCh().ch,
    liveness: over.liveness ?? memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } }).liveness,
    completeSinceMs: over.completeSinceMs ?? 0,
    log: (f) => logs.push(f),
  });
  const get = (path: string, token: string | null = "good") =>
    app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { app, get, logs };
}

const body = async (r: Response) => (await r.json()) as Record<string, any>;

describe("auth and attestation", () => {
  test("no token, a bad token, a missing scope", async () => {
    const { get } = api();
    expect((await get("/v1/events?workspace=ws-a&q=x", null)).status).toBe(401);
    expect((await get("/v1/events?workspace=ws-a&q=x", "bad")).status).toBe(401);
    const noScope = api({ identity: { ...ID, scopes: ["rth:sync"] } });
    const r = await noScope.get("/v1/events?workspace=ws-a&q=x");
    expect(r.status).toBe(403);
    expect((await body(r)).error.code).toBe("forbidden");
  });

  test("workspace must equal the token's; omitting it is the org-wide read relayhistory keeps", async () => {
    const { get } = api();
    expect((await get("/v1/events?workspace=ws-b&q=x")).status).toBe(403);
    expect((await get("/v1/events?workspace=ws-a&workspace=ws-a&q=x")).status).toBe(400);
    const orgWide = await get("/v1/events?q=x");
    expect(orgWide.status).toBe(501);
    expect((await body(orgWide)).error.reason).toBe("org_scope");
  });

  test("tenant predicates come from the token, never from parameters", async () => {
    const { ch, eventQueries } = stubCh();
    const { get } = api({ ch });
    await get("/v1/events?workspace=ws-a&q=hello&limit=20&org=org-b&org_id=org-b");
    expect(eventQueries()[0]).toContain("org_id = {org:String}");
    expect(eventQueries()[0]).toContain("workspace_id = {ws:String}");
  });
});

describe("the 501 fallback contract", () => {
  test.each([
    ["/v1/events?workspace=ws-a&q=x&taskRef=pull/", "param_taskRef"],
    ["/v1/events?workspace=ws-a&q=x&tag=t", "param_tag"],
    ["/v1/events?workspace=ws-a&q=x&agent=a", "param_agent"],
    ["/v1/events?workspace=ws-a&q=x&projectContains=p", "param_projectContains"],
    ["/v1/events?workspace=ws-a&q=x&noProject=true", "param_noProject"],
    ["/v1/sessions/s-1/events?workspace=ws-a&includeMessageId=true", "param_includeMessageId"],
    ["/v1/events?workspace=ws-a", "unfiltered_events"],
    ["/v1/events?workspace=ws-a&q=x&order=asc", "search_ascending"],
    ["/v1/events?workspace=ws-a&q=x&limit=101", "search_limit"],
    ["/v1/events?workspace=ws-a&q=x&cursor=events-v2:abc", "foreign_cursor"],
    ["/v1/sessions?workspace=ws-a", "route"],
    ["/v1/sessions/s-1/timing?workspace=ws-a&source=codex", "route"],
  ])("%s → 501 %s", async (path, reason) => {
    const { get } = api();
    const r = await get(path);
    expect(r.status).toBe(501);
    const b = await body(r);
    expect(b.error.code).toBe("unsupported_by_backend");
    expect(b.error.reason).toBe(reason);
  });

  test("a malformed own cursor is a 400, not a silent first page", async () => {
    const { get } = api();
    expect((await get("/v1/events?workspace=ws-a&q=x&cursor=b96e1:%%%")).status).toBe(400);
  });
});

describe("history floor and erasure", () => {
  const floor = Date.parse("2026-09-01T00:00:00Z");

  test("a session older than the floor, or without a pointer, is not served", async () => {
    const old = api({ completeSinceMs: floor, liveness: memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-08-01T00:00:00Z") } }).liveness });
    expect((await body(await old.get("/v1/sessions/s-1/events?workspace=ws-a"))).error.reason).toBe("history_floor");
    const unknown = api({ completeSinceMs: floor, liveness: memoryLiveness().liveness });
    expect((await body(await unknown.get("/v1/sessions/s-1/events?workspace=ws-a"))).error.reason).toBe("history_floor_unknown_session");
    // Inside the floor it is served.
    const ok = api({ completeSinceMs: floor });
    expect((await ok.get("/v1/sessions/s-1/events?workspace=ws-a")).status).toBe(200);
  });

  test("a search that reaches the floor without filling its page falls back", async () => {
    const { ch, eventQueries } = stubCh({ candidates: [] });
    const { get } = api({ ch, completeSinceMs: floor });
    const r = await get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("history_floor");
    // It searched only from the floor.
    expect(eventQueries()[0]).toContain("{since:Int64}");
    // A search bounded inside the floor is served even when short.
    expect((await get("/v1/events?workspace=ws-a&q=needle&limit=20&since=2026-10-01T00:00:00Z")).status).toBe(200);
    // Without a limit, relayhistory's default (200) is above this backend's search cap.
    expect((await body(await get("/v1/events?workspace=ws-a&q=needle"))).error.reason).toBe("search_limit");
  });

  test("a search cursor page that reaches the floor ends cleanly with a relayhistory continuation, never a 501", async () => {
    const cand = { ts_ms: Date.parse("2026-09-02T00:00:00Z"), session_id: "s-1", source: "codex", machine_id: "m-1", kind: "message", event_id: "e-9" };
    const newest = [{ ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-1", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "needle", in_budget: 1 }];
    const { ch, eventQueries } = stubCh({ candidates: [cand], newest });
    const { get, logs } = api({ ch, completeSinceMs: floor });
    const cursor = encodeCursor({ tsMs: Date.parse("2026-09-03T00:00:00Z"), sessionId: "s-1", source: "codex", machineId: "m-1", kind: "message", eventId: "e-10" });
    const r = await get(`/v1/events?workspace=ws-a&q=needle&limit=20&cursor=${cursor}`);
    expect(r.status).toBe(200);
    const b = await body(r);
    // The rows from the floor up, a terminal page, and where relayhistory takes over.
    expect(b.events.map((e: { eventId: string }) => e.eventId)).toEqual(["e-9"]);
    expect(b.nextCursor).toBeNull();
    expect(b.continuation).toEqual({ backend: "relayhistory", reason: "history_floor", until: "2026-08-31T23:59:59.999999Z" });
    expect(eventQueries()[0]).toContain("{since:Int64}");
    expect(logs.some((l) => l.reason === "history_floor_continuation")).toBe(true);
    expect(r.headers.get("x-sessions-read-continuation")).toBe("history_floor");
    // A full cursor page below no floor keeps its own cursor and no continuation.
    const full = api({ ch: stubCh({ candidates: [cand], newest }).ch, completeSinceMs: floor });
    const f = await body(await full.get(`/v1/events?workspace=ws-a&q=needle&limit=1&cursor=${cursor}`));
    expect(f.nextCursor).toStartWith("b96e1:");
    expect(f.continuation).toBeUndefined();
    expect((await full.get(`/v1/events?workspace=ws-a&q=needle&limit=1&cursor=${cursor}`)).headers.get("x-sessions-read-continuation")).toBeNull();
  });

  test("continuationUntil is one microsecond below the floor, in relayhistory's accepted format", () => {
    expect(continuationUntil(Date.parse("2026-09-01T00:00:00Z"))).toBe("2026-08-31T23:59:59.999999Z");
    expect(continuationUntil(Date.parse("2026-09-01T00:00:00.124Z"))).toBe("2026-09-01T00:00:00.123999Z");
  });

  test("an erased session reads as empty without touching ClickHouse", async () => {
    const live = memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } });
    live.erase({ source: "codex", sessionId: "s-1" });
    const { ch, queries } = stubCh({ candidates: [{ ts_ms: 1, session_id: "s-1", source: "codex", machine_id: "m", kind: "k", event_id: "e" }] });
    const { get } = api({ ch, liveness: live.liveness });
    const r = await get("/v1/sessions/s-1/events?workspace=ws-a&source=codex");
    expect(r.status).toBe(200);
    expect(await body(r)).toMatchObject({ sessionId: "s-1", events: [], nextCursor: null });
    expect(queries).toHaveLength(0);
  });
});

describe("partial sessions (search_ingest_session_state.needs_full = 1)", () => {
  test("a partial session's first page is 501 session_partial, so the caller reads it from relayhistory", async () => {
    const { ch, eventQueries } = stubCh({ partial: ["s-1"] });
    const { get } = api({ ch });
    for (const path of ["/v1/sessions/s-1/events?workspace=ws-a", "/v1/events?workspace=ws-a&session=s-1"]) {
      const r = await get(path);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe("session_partial");
    }
    // Its stale rows were never read.
    expect(eventQueries()).toHaveLength(0);
    // Another, complete session is served.
    const other = api({ ch: stubCh({ partial: ["s-2"] }).ch });
    expect((await other.get("/v1/sessions/s-1/events?workspace=ws-a")).status).toBe(200);
  });

  test("a workspace search starts at relayhistory while any session of the org is partial", async () => {
    const { ch, eventQueries } = stubCh({ partial: ["s-elsewhere"] });
    const { get } = api({ ch });
    const r = await get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial");
    expect(eventQueries()).toHaveLength(0);
    expect((await api({ ch: stubCh().ch }).get("/v1/events?workspace=ws-a&q=needle&limit=20")).status).toBe(200);
  });

  test("the partial check is bound to the token's org and checked when pagination starts, not on cursor pages", async () => {
    const { ch, queries } = stubCh({ partial: ["s-1"] });
    const { get } = api({ ch });
    const cursor = encodeCursor({ tsMs: 5, sessionId: "s-1", source: "codex", machineId: "m-1", kind: "message", eventId: "e-1" });
    // A cursor continues the read the first page admitted; a 501 here would end the caller's pagination in an error.
    expect((await get(`/v1/sessions/s-1/events?workspace=ws-a&cursor=${cursor}`)).status).toBe(200);
    expect((await get(`/v1/events?workspace=ws-a&q=needle&limit=20&cursor=${cursor}`)).status).toBe(200);
    expect(queries.some((q) => q.includes("search_ingest_session_state"))).toBe(false);
    await get("/v1/events?workspace=ws-a&q=needle&limit=20&org=org-b");
    const state = queries.find((q) => q.includes("search_ingest_session_state"))!;
    expect(state).toContain("org_id = {org:String}");
    expect(state).toContain("argMax(needs_full, synced_at) = 1");
  });
});

describe("responses", () => {
  test("headers: workspace attestation, no-store, backend marker, correlation id", async () => {
    const { get } = api();
    const r = await get("/v1/sessions/s-1/events?workspace=ws-a");
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
    expect(r.headers.get("cache-control")).toBe("private, no-store");
    expect(r.headers.get("x-sessions-read-backend")).toBe("96bulls");
    const b = await body(r);
    expect(b).toEqual({ sessionId: "s-1", events: [], nextCursor: null, correlationId: r.headers.get("x-correlation-id") });
  });

  test("a ClickHouse timeout is relayhistory's 503 search_timeout; other failures 503", async () => {
    const t = api({ ch: stubCh({ fail: new ClickHouseError(500, "159", "TIMEOUT_EXCEEDED") }).ch });
    const r = await t.get("/v1/events?workspace=ws-a&q=x&limit=20");
    expect(r.status).toBe(503);
    expect((await body(r)).error.code).toBe("search_timeout");
    const e = api({ ch: stubCh({ fail: new Error("boom") }).ch });
    expect((await e.get("/v1/events?workspace=ws-a&q=x&limit=20")).status).toBe(503);
  });

  test("logs carry ids and codes only", async () => {
    const { get, logs } = api();
    await get("/v1/events?workspace=ws-a&q=secret%20text&limit=20");
    for (const l of logs) expect(JSON.stringify(l)).not.toContain("secret text");
  });
});

describe("relayAuthVerifier", () => {
  const ORIGIN = "https://read.example.test";
  const JWKS = "https://relayauth.test/.well-known/jwks.json";
  const ISSUER = "https://relayauth.test";
  // One key for the suite: the shared verifier caches the JWKS and refetches an
  // unknown kid only after a minimum interval, so a fresh key per token would make
  // every later token fail for "unknown kid" rather than the reason under test.
  const KID = `k-${crypto.randomUUID()}`;
  const realFetch = globalThis.fetch;
  let privateKey: CryptoKey;
  let jwksFetches = 0;
  beforeAll(async () => {
    const pair = (await crypto.subtle.generateKey(
      { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
      true,
      ["sign", "verify"],
    )) as CryptoKeyPair;
    privateKey = pair.privateKey;
    const jwk = { ...(await crypto.subtle.exportKey("jwk", pair.publicKey)), kid: KID, alg: "RS256", use: "sig" };
    globalThis.fetch = (async (url: string) => {
      if (String(url) !== JWKS) throw new Error(`unexpected fetch ${url}`);
      jwksFetches++;
      return new Response(JSON.stringify({ keys: [jwk] }), { headers: { "content-type": "application/json" } });
    }) as typeof fetch;
  });
  afterAll(() => {
    globalThis.fetch = realFetch;
  });

  async function mint(claims: Record<string, unknown>) {
    const enc = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
    const now = Math.floor(Date.now() / 1000);
    const head = enc({ alg: "RS256", kid: KID, typ: "JWT" });
    const payload = enc({ iss: ISSUER, iat: now, exp: now + 60, ...claims });
    const sig = Buffer.from(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", privateKey, new TextEncoder().encode(`${head}.${payload}`))).toString("base64url");
    return `${head}.${payload}.${sig}`;
  }
  const assertion = (over: Record<string, unknown> = {}) => ({
    sub: "agent_relayhistory_assertion",
    meta: { tokenClass: "relayhistory_assertion" },
    aud: [ORIGIN],
    org: "org-a",
    wks: "ws-a",
    sponsorId: "u-1",
    scopes: ["rth:read"],
    ...over,
  });
  const verifier = () => relayAuthVerifier({ origin: ORIGIN, jwksUrl: JWKS, issuer: ISSUER });
  const refusal = async (token: string) => {
    try {
      await verifier()(token);
    } catch (e) {
      return (e as AuthError).message;
    }
    return "accepted";
  };

  test("an assertion minted for this origin is accepted with its exact tenancy", async () => {
    expect(await verifier()(await mint(assertion()))).toEqual({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] });
    // Proof the key resolves for every later case: each refusal below is for its own reason.
    expect(await refusal(await mint(assertion()))).toBe("accepted");
  });

  test("a token minted for History (another origin) or the legacy audience is refused", async () => {
    for (const aud of [["https://history.agentrelay.com"], ["relayhistory"], [ORIGIN, "https://history.agentrelay.com"]])
      expect(await refusal(await mint(assertion({ aud })))).toBe("token is not valid");
  });

  test("the origin binding is checked here too, whatever the shared verifier returns", async () => {
    const stub = (historyOrigin: string | undefined) =>
      relayAuthVerifier({ origin: ORIGIN }, (async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", tokenSubject: "s", scopes: ["rth:read"], claims: { org: "org-a", wks: "ws-a", sponsorId: "u-1" }, historyOrigin })) as never);
    await expect(stub(undefined)("t")).rejects.toThrow("not bound to this service");
    await expect(stub("https://history.agentrelay.com")("t")).rejects.toThrow("not bound to this service");
    expect((await stub(ORIGIN)("t")).orgId).toBe("org-a");
  });

  test("a non-assertion token for this audience, an expired one, or ambiguous tenancy is refused", async () => {
    expect(await refusal(await mint(assertion({ sub: "agent_x", meta: {} })))).toBe("token is not valid");
    const now = Math.floor(Date.now() / 1000);
    expect(await refusal(await mint(assertion({ exp: now - 120 })))).toBe("token is not valid");
    // An alternative claim name the shared extractor would prefer over `org` / `wks`.
    expect(await refusal(await mint(assertion({ relay_org_id: "org-b" })))).toBe("token tenancy is ambiguous");
    expect(await refusal(await mint(assertion({ relay_workspace_id: "ws-b" })))).toBe("token tenancy is ambiguous");
    expect(await refusal(await mint(assertion({ wks: undefined, workspace_id: "ws-a" })))).toBe("token tenancy is ambiguous");
    expect(jwksFetches).toBeGreaterThan(0);
  });

  test("no configured origin refuses every token", async () => {
    await expect(relayAuthVerifier({ origin: undefined, jwksUrl: JWKS, issuer: ISSUER })(await mint(assertion()))).rejects.toThrow("no origin configured");
  });
});

describe("lambda adapter and configuration", () => {
  test("Function URL events map to GET requests; other methods are 405", async () => {
    const { app } = api();
    const ok = await handleUrlEvent(app, { rawPath: "/v1/sessions/s-1/events", rawQueryString: "workspace=ws-a", headers: { authorization: "Bearer good" }, requestContext: { http: { method: "GET" } } });
    expect(ok.statusCode).toBe(200);
    expect(ok.headers["x-relayhistory-workspace-id"]).toBe("ws-a");
    expect(JSON.parse(ok.body).sessionId).toBe("s-1");
    const post = await handleUrlEvent(app, { rawPath: "/v1/events", headers: {}, requestContext: { http: { method: "POST" } } });
    expect(post.statusCode).toBe(405);
  });

  test("secret and floor parsing never echo values and refuse junk", () => {
    expect(() => parseReadApiSecret("not json")).toThrow("not JSON");
    expect(() => parseReadApiSecret(JSON.stringify({ supabase_url: "postgresql://x", clickhouse: { url: "u", user: "u" } }))).toThrow("clickhouse.password");
    try {
      parseReadApiSecret(JSON.stringify({ supabase_url: "mysql://hunter2", clickhouse: {} }));
    } catch (e) {
      expect(String(e)).not.toContain("hunter2");
    }
    expect(parseCompleteSince("epoch")).toBe(0);
    expect(parseCompleteSince("2026-09-01T00:00:00Z")).toBe(Date.parse("2026-09-01T00:00:00Z"));
    expect(() => parseCompleteSince(undefined)).toThrow();
    expect(() => parseCompleteSince("soon")).toThrow();
  });

  test("own cursors round-trip; the prefix keeps them out of relayhistory", () => {
    const c = encodeCursor({ tsMs: 5, sessionId: "s", source: "codex", machineId: "m", kind: "k", eventId: "e" });
    expect(c.startsWith("b96e1:")).toBe(true);
  });
});

describe("directory search (GET /v1/directory/search)", () => {
  const dirRow = (i: number, over: Partial<DirectoryRow> = {}): DirectoryRow & Record<string, unknown> => ({
    org_id: "org-a",
    workspace_id: "ws-a",
    harness: "codex",
    session_id: `s-${i}`,
    title: `widget ${i}`,
    git_repo: "acme/widgets",
    last_activity_at: `2026-10-0${9 - i}T00:00:00.000001Z`,
    state: "done",
    ...over,
  });
  function stubDirectory(rows: (DirectoryRow & Record<string, unknown>)[] | Error = []) {
    const calls: DirectorySearchInput[] = [];
    const directory: DirectorySearch = {
      async search(input) {
        calls.push(input);
        if (rows instanceof Error) throw rows;
        return rows.slice(0, input.fetch);
      },
    };
    return { directory, calls };
  }
  function dirApi(over: { identity?: VerifiedIdentity; rows?: (DirectoryRow & Record<string, unknown>)[] | Error; allow?: string[] | null } = {}) {
    const { directory, calls } = stubDirectory(over.rows);
    const logs: Record<string, unknown>[] = [];
    const app = createReadApi({
      verify: async (t) => {
        if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
        return over.identity ?? ID;
      },
      ch: stubCh().ch,
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      ...(over.allow === null ? {} : { directory, directoryWorkspaces: new Set(over.allow ?? ["ws-a"]) }),
      log: (f) => logs.push(f),
    });
    const get = async (query: string, token: string | null = "good") => {
      const r = await app.request(`/v1/directory/search?${query}`, { headers: token ? { authorization: `Bearer ${token}` } : {} });
      return { status: r.status, body: await body(r), headers: r.headers };
    };
    return { get, calls, logs };
  }

  test("directory auth: no token or a bad token is 401, a missing rth:read scope 403", async () => {
    const { get, calls } = dirApi();
    expect((await get("workspace=ws-a&q=widget", null)).status).toBe(401);
    expect((await get("workspace=ws-a&q=widget", "bad")).status).toBe(401);
    const noScope = dirApi({ identity: { ...ID, scopes: ["rth:sync"] } });
    expect((await noScope.get("workspace=ws-a&q=widget")).status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  test("directory workspace must be given once and equal the token's", async () => {
    const { get, calls } = dirApi();
    expect((await get("q=widget")).status).toBe(400);
    expect((await get("workspace=ws-a&workspace=ws-a&q=widget")).status).toBe(400);
    const other = await get("workspace=ws-b&q=widget");
    expect(other.status).toBe(403);
    expect(other.body.error.code).toBe("forbidden");
    expect(calls).toHaveLength(0);
  });

  test("directory allowlist: a workspace not on it, or no directory configured, is 403 directory_not_enabled", async () => {
    const notListed = dirApi({ allow: ["ws-other"] });
    const r = await notListed.get("workspace=ws-a&q=widget");
    expect(r.status).toBe(403);
    expect(r.body.error.code).toBe("directory_not_enabled");
    expect(notListed.calls).toHaveLength(0);
    const empty = dirApi({ allow: [] });
    expect((await empty.get("workspace=ws-a&q=widget")).status).toBe(403);
    const unconfigured = dirApi({ allow: null });
    expect((await unconfigured.get("workspace=ws-a&q=widget")).body.error.code).toBe("directory_not_enabled");
  });

  test("directory tenant comes from the token, never from parameters; unknown parameters are refused", async () => {
    const { get, calls } = dirApi();
    expect((await get("workspace=ws-a&q=widget&org=org-b")).status).toBe(400);
    expect((await get("workspace=ws-a&q=widget&workspace_id=ws-b")).status).toBe(400);
    expect(calls).toHaveLength(0);
    expect((await get("workspace=ws-a&q=%20%20Widget%20%20login%20")).status).toBe(200);
    expect(calls).toEqual([{ orgId: "org-a", workspaceId: "ws-a", q: "Widget login", fetch: 21, cursor: undefined }]);
  });

  test("directory q must be 3 to 200 characters after trimming", async () => {
    const { get, calls } = dirApi();
    for (const q of ["", "ab", "%20%20ab%20%20", encodeURIComponent("x".repeat(201))]) expect((await get(`workspace=ws-a&q=${q}`)).status).toBe(400);
    expect((await get("workspace=ws-a")).status).toBe(400);
    expect((await get("workspace=ws-a&q=abc&q=def")).status).toBe(400);
    expect(calls).toHaveLength(0);
    expect((await get(`workspace=ws-a&q=${"x".repeat(200)}`)).status).toBe(200);
  });

  test("directory pointer-only: extra row columns never reach the response", async () => {
    const { get } = dirApi({ rows: [dirRow(1, { search_text: "the summary", short_summary: "secret", task_title: "secret task", user_ids: ["u-9"] })] });
    const r = await get("workspace=ws-a&q=widget");
    expect(r.status).toBe(200);
    expect(r.body.sessions).toEqual([{ session_id: "s-1", workspace_id: "ws-a", title: "widget 1", git_repo: "acme/widgets", last_activity_at: "2026-10-08T00:00:00.000001Z", state: "done" }]);
    expect(JSON.stringify(r.body)).not.toMatch(/secret|summary|u-9|codex|org-a/);
    expect(r.headers.get("cache-control")).toBe("private, no-store");
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
  });

  test("directory pagination: limit+1 is fetched, the cursor carries the last row's exact key, limit is clamped", async () => {
    const rows = [1, 2, 3].map((i) => dirRow(i));
    const { get, calls } = dirApi({ rows });
    const first = await get("workspace=ws-a&q=widget&limit=2");
    expect(first.body.sessions.map((s: { session_id: string }) => s.session_id)).toEqual(["s-1", "s-2"]);
    expect(calls[0]!.fetch).toBe(3);
    expect(decodeDirectoryCursor(first.body.nextCursor)).toEqual({ lastActivityAt: "2026-10-07T00:00:00.000001Z", sessionId: "s-2", harness: "codex" });
    await get(`workspace=ws-a&q=widget&limit=2&cursor=${encodeURIComponent(first.body.nextCursor)}`);
    expect(calls[1]!.cursor).toEqual({ lastActivityAt: "2026-10-07T00:00:00.000001Z", sessionId: "s-2", harness: "codex" });
    const last = await get("workspace=ws-a&q=widget&limit=3");
    expect(last.body.nextCursor).toBeNull();
    await get("workspace=ws-a&q=widget&limit=500");
    expect(calls.at(-1)!.fetch).toBe(51);
  });

  test("directory cursors: an event cursor, junk or a forged key is a 400", async () => {
    const { get, calls } = dirApi();
    const event = encodeCursor({ tsMs: 5, sessionId: "s", source: "codex", machineId: "m", kind: "k", eventId: "e" });
    const forged = encodeDirectoryCursor({ lastActivityAt: "2026-10-01T00:00:00Z", sessionId: "s", harness: "codex" });
    const badHarness = encodeDirectoryCursor({ lastActivityAt: "2026-10-01T00:00:00.000001Z", sessionId: "s", harness: "Bad Harness" });
    for (const c of [event, "b96d1:!!!", "b96d1:" + Buffer.from("[1,2]").toString("base64url"), forged, badHarness])
      expect((await get(`workspace=ws-a&q=widget&cursor=${encodeURIComponent(c)}`)).status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  test("directory: a row of another tenant is refused, not served", async () => {
    for (const bad of [dirRow(2, { workspace_id: "ws-b" }), dirRow(2, { org_id: "org-b" })]) {
      const { get, logs } = dirApi({ rows: [dirRow(1), bad] });
      const r = await get("workspace=ws-a&q=widget");
      expect(r.status).toBe(500);
      expect(JSON.stringify(r.body)).not.toContain("s-1");
      expect(logs.some((l) => l.reason === "cross_tenant_rows")).toBe(true);
    }
  });

  test("directory: a database failure is a 503 with no detail, and logs never carry the query", async () => {
    const { get, logs } = dirApi({ rows: Object.assign(new Error("canceling statement due to statement timeout: widget"), { code: "57014" }) });
    const r = await get("workspace=ws-a&q=widget");
    expect(r.status).toBe(503);
    expect(r.body.error.code).toBe("backend_unavailable");
    expect(JSON.stringify(r.body)).not.toContain("widget");
    expect(JSON.stringify(logs)).not.toContain("widget");
  });

  test("directory helpers: query normalization, literal LIKE patterns, the allowlist parser", () => {
    expect(normalizeDirectoryQuery("  a\tb\n c ")).toBe("a b c");
    expect(normalizeDirectoryQuery("ab")).toBeUndefined();
    expect(likePattern("50%_off\\x")).toBe("%50\\%\\_off\\\\x%");
    expect([...parseDirectoryWorkspaces(" ws-a , ws-b ").workspaces]).toEqual(["ws-a", "ws-b"]);
    expect(parseDirectoryWorkspaces(undefined)).toEqual({ workspaces: new Set(), invalid: false });
    expect(parseDirectoryWorkspaces("")).toEqual({ workspaces: new Set(), invalid: false });
    // One malformed entry closes the route for everyone rather than admitting the rest.
    for (const raw of ["ws-a,,ws-b", "ws-a,bad id", `ws-a,${"x".repeat(129)}`]) expect(parseDirectoryWorkspaces(raw)).toEqual({ workspaces: new Set(), invalid: true });
  });
});
