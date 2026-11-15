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
import { handleUrlEvent, parseCompleteSince, parseReadApiSecret } from "../lambda.ts";
import { PARTIAL_SESSIONS_CAP, buildCandidateQuery } from "../queries.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

const EXCLUSION = "session_id NOT IN [";

/**
 * Every inlined `session_id NOT IN [...]` list of a query, decoded with ClickHouse string
 * quoting (a backslash escapes the next character). Throws when a literal is unterminated or
 * anything other than `,` or `]` follows one, so an id that ended its literal early fails here.
 */
function inlinedExclusions(query: string): string[][] {
  const lists: string[][] = [];
  for (let at = query.indexOf(EXCLUSION); at >= 0; at = query.indexOf(EXCLUSION, at + 1)) {
    const ids: string[] = [];
    let i = at + EXCLUSION.length;
    for (;;) {
      if (query[i] !== "'") throw new Error(`expected a string literal at ${i}`);
      let id = "";
      for (i++; ; ) {
        const ch = query[i++];
        if (ch === undefined) throw new Error("unterminated string literal");
        if (ch === "'") break;
        if (ch === "\\") {
          if (query[i] === undefined) throw new Error("unterminated escape");
          id += query[i++];
        } else id += ch;
      }
      ids.push(id);
      if (query[i] === ",") { i++; continue; }
      if (query[i] === "]") break;
      throw new Error(`unexpected ${JSON.stringify(query[i])} after a literal`);
    }
    lists.push(ids);
  }
  return lists;
}

/**
 * `partial`: the org's partial sessions; `partialInWorkspace`: those of them
 * with a row in the token's workspace (the search check's `in_workspace`).
 */
function stubCh(rows: { candidates?: unknown[]; newest?: unknown[]; partial?: string[]; partialInWorkspace?: string[]; fail?: Error; ignoreExclusion?: boolean } = {}) {
  const queries: string[] = [];
  /** Each query's parameters, in the order asked. */
  const params: Record<string, unknown>[] = [];
  /** The candidate query's inlined exclusion list (applied to its rows and the newest rows, as the SQL does unless ignoreExclusion). */
  let excluded = new Set<string>();
  const keep = (list: unknown[] | undefined) => (list ?? []).filter((r) => rows.ignoreExclusion || !excluded.has((r as { session_id: string }).session_id));
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      queries.push(query);
      params.push({ ...(options?.params ?? {}) });
      if (rows.fail) throw rows.fail;
      if (query.includes("in_workspace")) {
        const inWs = new Set(rows.partialInWorkspace ?? []);
        return (rows.partial ?? []).map((session_id) => ({ session_id, in_workspace: inWs.has(session_id) ? 1 : 0 })) as T[];
      }
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return (rows.partial ?? []).filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      if (query.includes("bytes_before")) return keep(rows.newest) as T[];
      excluded = new Set(inlinedExclusions(query)[0] ?? []);
      return keep(rows.candidates) as T[];
    },
  };
  /** Queries against transcript_events only (the partial-session checks read the state table). */
  const eventQueries = () => queries.filter((q) => !q.includes("search_ingest_session_state"));
  /** The parameters of those queries, in the same order. */
  const eventParams = () => params.filter((_, i) => !queries[i]!.includes("search_ingest_session_state"));
  return { ch, queries, eventQueries, params, eventParams };
}

function api(over: { identity?: VerifiedIdentity; completeSinceMs?: number; ch?: ClickHouseClient; liveness?: ReturnType<typeof memoryLiveness>["liveness"]; verify?: (t: string) => Promise<VerifiedIdentity>; searchSkipsPartialSessions?: boolean; corsAllowedOrigins?: ReadonlySet<string> } = {}) {
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({
    verify: over.verify ?? (async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return over.identity ?? ID;
    }),
    ch: over.ch ?? stubCh().ch,
    liveness: over.liveness ?? memoryLiveness({ createdAt: { "codex\u0000s-1": Date.parse("2026-10-01T00:00:00Z") } }).liveness,
    completeSinceMs: over.completeSinceMs ?? 0,
    ...(over.searchSkipsPartialSessions ? { searchSkipsPartialSessions: true } : {}),
    ...(over.corsAllowedOrigins ? { corsAllowedOrigins: over.corsAllowedOrigins } : {}),
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
    // P3b: registered directory routes; off the canary allowlist (none here) they fall back.
    ["/v1/sessions?workspace=ws-a", "directory_workspace_not_enabled"],
    ["/v1/sessions/s-1/timing?workspace=ws-a&source=codex", "timing_workspace_not_enabled"],
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
    const newest = [{ ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-1", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "needle", in_budget: 1, origin: "canonical", origin_claim_token: "" }];
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

  test("a workspace search starts at relayhistory while a session of its workspace is partial", async () => {
    // A row in this workspace: decided in ClickHouse, Supabase not asked.
    const rowHere = stubCh({ partial: ["s-row", "s-elsewhere"], partialInWorkspace: ["s-row"] });
    const noPointers = memoryLiveness();
    let r = await api({ ch: rowHere.ch, liveness: noPointers.liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial");
    expect(rowHere.eventQueries()).toHaveLength(0);
    expect(noPointers.calls.livePointerSessions).toEqual([]);
    // No row anywhere yet (every pass was partial), but a live pointer here: still this workspace's.
    const pointerHere = stubCh({ partial: ["s-1", "s-elsewhere"] });
    const pointers = memoryLiveness({ createdAt: { "codex\u0000s-1": 1 } });
    r = await api({ ch: pointerHere.ch, liveness: pointers.liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial");
    expect(pointerHere.eventQueries()).toHaveLength(0);
    expect(pointers.calls.livePointerSessions).toEqual([["s-1", "s-elsewhere"]]);
  });

  test("with searchSkipsPartialSessions a workspace search answers and names the partial sessions", async () => {
    const rowHere = stubCh({ partial: ["s-row", "s-elsewhere"], partialInWorkspace: ["s-row"] });
    let r = await api({ ch: rowHere.ch, liveness: memoryLiveness().liveness, searchSkipsPartialSessions: true }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(200);
    expect(r.headers.get("X-Sessions-Read-Incomplete")).toBe("session_partial");
    expect((await body(r)).incomplete).toEqual({ reason: "session_partial", sessions: ["s-row"] });
    expect(rowHere.eventQueries()).toHaveLength(1);
    // More partial sessions than one check lists: answered, and said to be unbounded.
    const many = Array.from({ length: PARTIAL_SESSIONS_CAP + 1 }, (_, i) => `s-other-${i}`);
    r = await api({ ch: stubCh({ partial: many }).ch, liveness: memoryLiveness().liveness, searchSkipsPartialSessions: true }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(200);
    expect((await body(r)).incomplete).toEqual({ reason: "session_partial_unbounded" });
    // No partial session: the reply is exactly as without the setting.
    r = await api({ ch: stubCh().ch, searchSkipsPartialSessions: true }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(200);
    expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
    expect("incomplete" in (await body(r))).toBe(false);
    // A session-scoped read of a partial session still refuses.
    r = await api({ ch: stubCh({ partial: ["s-1"] }).ch, searchSkipsPartialSessions: true }).get("/v1/sessions/s-1/events?workspace=ws-a");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial");
  });

  describe("READ_API_SEARCH_PARTIAL_SESSIONS=skip (searchSkipsPartialSessions)", () => {
    const SEARCH = "/v1/events?workspace=ws-a&q=needle&limit=20";
    const skip = (ch: ClickHouseClient, extra: Parameters<typeof api>[0] = {}) => api({ ch, liveness: memoryLiveness().liveness, searchSkipsPartialSessions: true, ...extra });

    test("unset and false keep the 501, with neither the header nor the field", async () => {
      for (const over of [{}, { searchSkipsPartialSessions: false }]) {
        for (const [partial, reason] of [[["s-row"], "session_partial"], [Array.from({ length: PARTIAL_SESSIONS_CAP + 1 }, (_, i) => `s-${i}`), "session_partial_unbounded"]] as const) {
          const stub = stubCh({ partial: [...partial], partialInWorkspace: ["s-row"] });
          const { get, logs } = api({ ch: stub.ch, liveness: memoryLiveness().liveness, ...over });
          const r = await get(SEARCH);
          expect(r.status).toBe(501);
          expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
          const b = await body(r);
          expect(b.error.reason).toBe(reason);
          expect("incomplete" in b).toBe(false);
          expect(stub.eventQueries()).toHaveLength(0);
          expect(logs.some((l) => String(l.reason).endsWith("_skipped"))).toBe(false);
        }
      }
    });

    test("the answered first page logs the skip with a count, never the session ids", async () => {
      const { get, logs } = skip(stubCh({ partial: ["s-row", "s-two"], partialInWorkspace: ["s-row", "s-two"] }).ch);
      const r = await get(SEARCH);
      expect(r.status).toBe(200);
      expect((await body(r)).incomplete).toEqual({ reason: "session_partial", sessions: ["s-row", "s-two"] });
      const entry = logs.find((l) => l.reason === "session_partial_skipped");
      expect(entry).toMatchObject({ status: 200, partial_sessions: 2 });
      expect(JSON.stringify(logs)).not.toContain("s-row");
    });

    test("overflow answers session_partial_unbounded with no sessions list", async () => {
      const many = Array.from({ length: PARTIAL_SESSIONS_CAP + 1 }, (_, i) => `s-other-${i}`);
      const { get, logs } = skip(stubCh({ partial: many, partialInWorkspace: many }).ch);
      const r = await get(SEARCH);
      expect(r.status).toBe(200);
      expect(r.headers.get("X-Sessions-Read-Incomplete")).toBe("session_partial_unbounded");
      const b = await body(r);
      expect(b.incomplete).toEqual({ reason: "session_partial_unbounded" });
      expect("sessions" in b.incomplete).toBe(false);
      const entry = logs.find((l) => l.reason === "session_partial_unbounded_skipped");
      expect(entry).toMatchObject({ status: 200 });
      expect("partial_sessions" in entry!).toBe(false);
    });

    // Rows of a session indexed while small, then partial (grown past the bound): stale.
    const row = (session_id: string, event_id: string, ts: string) => {
      const cand = { ts_ms: Date.parse(ts), session_id, source: "codex", machine_id: "m-1", kind: "message", event_id };
      return { cand, newest: { ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-1", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "needle", in_budget: 1, origin: "canonical", origin_claim_token: "" } };
    };
    const rows = [row("s-stale", "e-3", "2026-10-03T00:00:00Z"), row("s-ok", "e-2", "2026-10-02T00:00:00Z"), row("s-stale", "e-1", "2026-10-01T00:00:00Z")];
    const fixture = (extra: Parameters<typeof stubCh>[0] = {}) => stubCh({ candidates: rows.map((r) => r.cand), newest: rows.map((r) => r.newest), partial: ["s-stale"], partialInWorkspace: ["s-stale"], ...extra });
    const cursor = encodeCursor({ tsMs: Date.parse("2026-10-04T00:00:00Z"), sessionId: "s-ok", source: "codex", machineId: "m-1", kind: "message", eventId: "e-9" });

    test("every page is checked again, leaves out the partial sessions' rows and carries the field and header", async () => {
      for (const page of [SEARCH, `${SEARCH}&cursor=${cursor}`]) {
        const stub = fixture();
        const { get, logs } = skip(stub.ch);
        const r = await get(page);
        expect(r.status).toBe(200);
        expect(r.headers.get("X-Sessions-Read-Incomplete")).toBe("session_partial");
        const b = await body(r);
        expect(b.events.map((e: { sessionId: string; eventId: string }) => `${e.sessionId}/${e.eventId}`)).toEqual(["s-ok/e-2"]);
        expect(b.incomplete).toEqual({ reason: "session_partial", sessions: ["s-stale"] });
        expect(stub.queries.some((q) => q.includes("search_ingest_session_state"))).toBe(true);
        // An escaped literal in the candidate query's body, never a URL parameter.
        expect(stub.eventQueries()[0]).toContain("session_id NOT IN ['s-stale']");
        expect(stub.eventParams().some((p) => "excluded" in p)).toBe(false);
        // Counts only, never the ids.
        expect(logs.find((l) => l.reason === "session_partial_skipped")).toMatchObject({ status: 200, partial_sessions: 1, excluded_sessions: 1 });
        expect(JSON.stringify(logs)).not.toContain("s-stale");
      }
    });

    test("the exclusion list is inlined as escaped literals: no id ends its literal, no URL parameter", () => {
      const ids = ["s-ok", "s'2", "s-stale'; --", "back\\", "mix\\'] OR 1=1 --", "\\'"];
      // The escaped literals as ClickHouse reads them: every quote and backslash is escaped.
      const literal = String.raw`['s-ok','s\'2','s-stale\'; --','back\\','mix\\\'] OR 1=1 --','\\\'']`;
      const input = { orgId: "org-a", workspaceId: "ws-a", order: "desc" as const, limit: 20, q: "needle" };
      for (const sessionId of [undefined, "s-1"]) {
        const plain = buildCandidateQuery("relay_search", { ...input, sessionId });
        const built = buildCandidateQuery("relay_search", { ...input, sessionId, excludeSessionIds: ids });
        // One list per searched branch, each decoding back to exactly the ids.
        const lists = inlinedExclusions(built.query);
        expect(lists.length).toBeGreaterThan(0);
        expect(lists.length).toBe(built.query.split("NOT IN").length - 1);
        for (const list of lists) expect(list).toEqual(ids);
        expect(built.query).toContain(literal);
        // Removing the lists gives back the plain query: nothing else changed.
        expect(built.query.split(` AND session_id NOT IN ${literal}`).join("")).toBe(plain.query);
        // Parameters are unchanged: no `excluded`, nothing carried in the request URL.
        expect("excluded" in built.params).toBe(false);
        expect(built.params).toEqual(plain.params);
      }
      // Absent or empty adds nothing.
      expect(buildCandidateQuery("relay_search", { ...input, excludeSessionIds: [] })).toEqual(buildCandidateQuery("relay_search", input));
    });

    test("a cursor page whose partial-sessions lookup throws is a 503 with the standard body, no detail and no header", async () => {
      const stub = fixture();
      const select = stub.ch.select.bind(stub.ch);
      stub.ch.select = (async (q: string, o?: { params?: Record<string, unknown> }) => {
        if (q.includes("search_ingest_session_state")) throw new Error("state lookup exploded s-stale secret-detail");
        return select(q, o);
      }) as ClickHouseClient["select"];
      const { get, logs } = skip(stub.ch);
      const r = await get(`${SEARCH}&cursor=${cursor}`);
      expect(r.status).toBe(503);
      expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
      const text = await r.text();
      expect(text).not.toContain("exploded");
      expect(text).not.toContain("secret-detail");
      expect(text).not.toContain("s-stale");
      const b = JSON.parse(text) as Record<string, any>;
      expect(b).toEqual({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId: r.headers.get("x-correlation-id") });
      // The page was never read, and nothing claims a skip.
      expect(stub.eventQueries()).toHaveLength(0);
      expect(logs.some((l) => String(l.reason).endsWith("_skipped"))).toBe(false);
      expect(JSON.stringify(logs)).not.toContain("exploded");
    });

    test("a backend that returns an excluded session's row fails the page rather than serving it", async () => {
      const { get } = skip(fixture({ ignoreExclusion: true }).ch);
      for (const page of [SEARCH, `${SEARCH}&cursor=${cursor}`]) {
        const r = await get(page);
        expect(r.status).toBe(500);
        expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
        expect(JSON.stringify(await body(r))).not.toContain("s-stale");
      }
    });

    test("unbounded: every page says so, with no list and no exclusion", async () => {
      const many = Array.from({ length: PARTIAL_SESSIONS_CAP + 1 }, (_, i) => `s-other-${i}`);
      for (const page of [SEARCH, `${SEARCH}&cursor=${cursor}`]) {
        const stub = fixture({ partial: many, partialInWorkspace: many });
        const { get, logs } = skip(stub.ch);
        const r = await get(page);
        expect(r.status).toBe(200);
        expect(r.headers.get("X-Sessions-Read-Incomplete")).toBe("session_partial_unbounded");
        const b = await body(r);
        expect(b.incomplete).toEqual({ reason: "session_partial_unbounded" });
        // Nothing listed, so nothing excluded: rows of a partial session may remain.
        expect(b.events).toHaveLength(3);
        expect(stub.eventQueries().some((q) => q.includes("NOT IN"))).toBe(false);
        expect(stub.eventParams().some((p) => "excluded" in p)).toBe(false);
        expect(logs.find((l) => l.reason === "session_partial_unbounded_skipped")).toMatchObject({ status: 200, excluded_sessions: 0 });
      }
    });

    test("with no partial session every page's queries and reply equal the setting off", async () => {
      for (const page of [SEARCH, `${SEARCH}&cursor=${cursor}`]) {
        const on = fixture({ partial: [], partialInWorkspace: [] });
        const off = fixture({ partial: [], partialInWorkspace: [] });
        const a = await skip(on.ch).get(page);
        const b = await api({ ch: off.ch, liveness: memoryLiveness().liveness }).get(page);
        expect(a.status).toBe(200);
        expect(a.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
        const strip = (t: string) => t.replace(/"correlationId":"[^"]*"/, "");
        expect(strip(await a.text())).toBe(strip(await b.text()));
        expect(on.eventQueries()).toEqual(off.eventQueries());
        expect(on.eventParams()).toEqual(off.eventParams());
      }
    });

    test("a cursor page that ends at the history floor carries the continuation and the field", async () => {
      const stub = fixture();
      const { get } = skip(stub.ch, { completeSinceMs: Date.parse("2026-09-01T00:00:00Z") });
      const r = await get(`${SEARCH}&cursor=${cursor}`);
      expect(r.status).toBe(200);
      expect(r.headers.get("X-Sessions-Read-Continuation")).toBe("history_floor");
      expect(r.headers.get("X-Sessions-Read-Incomplete")).toBe("session_partial");
      const b = await body(r);
      expect(b.continuation).toMatchObject({ backend: "relayhistory", reason: "history_floor" });
      expect(b.incomplete).toEqual({ reason: "session_partial", sessions: ["s-stale"] });
      expect(b.events.map((e: { sessionId: string }) => e.sessionId)).toEqual(["s-ok"]);
    });

    test("with the setting off a cursor page is unchanged: no check, no exclusion, no field, no header", async () => {
      for (const over of [{}, { searchSkipsPartialSessions: false }]) {
        const stub = fixture();
        const { get, logs } = api({ ch: stub.ch, liveness: memoryLiveness().liveness, ...over });
        const r = await get(`${SEARCH}&cursor=${cursor}`);
        expect(r.status).toBe(200);
        expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
        const b = await body(r);
        expect("incomplete" in b).toBe(false);
        expect(b.events).toHaveLength(3);
        expect(stub.queries.some((q) => q.includes("search_ingest_session_state"))).toBe(false);
        expect(stub.eventParams().some((p) => "excluded" in p)).toBe(false);
        expect(logs.some((l) => String(l.reason).endsWith("_skipped"))).toBe(false);
      }
    });

    test("a session-scoped read of a partial session is still 501", async () => {
      for (const path of ["/v1/sessions/s-1/events?workspace=ws-a", "/v1/events?workspace=ws-a&session=s-1", "/v1/events?workspace=ws-a&session=s-1&q=needle&limit=20"]) {
        const stub = stubCh({ partial: ["s-1"], partialInWorkspace: ["s-1"] });
        const r = await api({ ch: stub.ch, searchSkipsPartialSessions: true }).get(path);
        expect(r.status).toBe(501);
        expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
        expect((await body(r)).error.reason).toBe("session_partial");
        expect(stub.eventQueries()).toHaveLength(0);
      }
    });

    test("an error after the check carries neither the header nor the skip log", async () => {
      // First page that reaches the history floor without filling: 501 history_floor, not an incomplete 200.
      const { get, logs } = skip(stubCh({ partial: ["s-row"], partialInWorkspace: ["s-row"], candidates: [] }).ch, { completeSinceMs: Date.parse("2026-09-01T00:00:00Z") });
      let r = await get(SEARCH);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe("history_floor");
      expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
      expect(logs.some((l) => String(l.reason).endsWith("_skipped"))).toBe(false);
      // A backend failure while reading the page.
      let calls = 0;
      const flaky = stubCh({ partial: ["s-row"], partialInWorkspace: ["s-row"] });
      const select = flaky.ch.select.bind(flaky.ch);
      flaky.ch.select = (async (q: string, o?: { params?: Record<string, unknown> }) => {
        if (!q.includes("search_ingest_session_state") && calls++ === 0) throw new Error("backend down");
        return select(q, o);
      }) as ClickHouseClient["select"];
      const failing = skip(flaky.ch);
      r = await failing.get(SEARCH);
      expect(r.status).toBeGreaterThanOrEqual(500);
      expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
      expect(failing.logs.some((l) => String(l.reason).endsWith("_skipped"))).toBe(false);
    });

    test("CORS exposes X-Sessions-Read-Incomplete only with the setting on", async () => {
      const origins = new Set(["https://app.example.test"]);
      const headers = { authorization: "Bearer good", origin: "https://app.example.test" };
      const off = await api({ corsAllowedOrigins: origins }).app.request("/v1/sessions/s-1/events?workspace=ws-a", { headers });
      expect(off.headers.get("access-control-expose-headers")).toBe("X-Correlation-Id, X-Relayhistory-Workspace-Id, X-Sessions-Read-Continuation");
      const on = await api({ corsAllowedOrigins: origins, searchSkipsPartialSessions: true }).app.request("/v1/sessions/s-1/events?workspace=ws-a", { headers });
      expect(on.headers.get("access-control-expose-headers")).toBe("X-Correlation-Id, X-Relayhistory-Workspace-Id, X-Sessions-Read-Continuation, X-Sessions-Read-Incomplete");
    });
  });

  test("partial sessions of other workspaces of the org do not stop this workspace's search", async () => {
    const { ch, eventQueries } = stubCh({ partial: ["s-elsewhere", "s-erased-here"] });
    // s-erased-here: every pointer here tombstoned, so the erasure filter drops its rows anyway.
    const { liveness, calls } = memoryLiveness({ erased: [{ source: "codex", sessionId: "s-erased-here" }] });
    const r = await api({ ch, liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(200);
    expect(eventQueries()).toHaveLength(1);
    expect(calls.livePointerSessions).toEqual([["s-elsewhere", "s-erased-here"]]);
    expect((await api({ ch: stubCh().ch }).get("/v1/events?workspace=ws-a&q=needle&limit=20")).status).toBe(200);
  });

  test("more partial sessions than one check lists is not decided: the search falls back whole", async () => {
    const many = Array.from({ length: PARTIAL_SESSIONS_CAP + 1 }, (_, i) => `s-other-${i}`);
    const { ch, eventQueries } = stubCh({ partial: many });
    const { liveness, calls } = memoryLiveness();
    const r = await api({ ch, liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20");
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("session_partial_unbounded");
    expect(eventQueries()).toHaveLength(0);
    expect(calls.livePointerSessions).toEqual([]);
    // Exactly the cap is decided.
    const atCap = stubCh({ partial: many.slice(1) });
    expect((await api({ ch: atCap.ch, liveness }).get("/v1/events?workspace=ws-a&q=needle&limit=20")).status).toBe(200);
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
    expect(state).toContain("workspace_id = {ws:String}");
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
