import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createPostgresSearchIndex, TENANT_SETTINGS_SQL, type SqlClient } from "@rhc/search-index/postgres.ts";
import { comparePositions, type SearchHit, type SearchIndexAdapter, type SearchQuery } from "@rhc/search-index/contracts.ts";
import { createReadApi } from "../app.ts";
import { encodeCursor } from "../cursor.ts";
import { readEventPage, type Liveness } from "../page.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";

const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const liveness: Liveness = { async erased() { return new Set(); }, async sessionStart() { return { pointers: 1, live: 1, createdAtMs: 1 }; }, async livePointerSessions(_o, _w, ids) { return new Set(ids); } };
const hit = (n: number): SearchHit => ({ ...tenant, tsMs: n, sessionId: "s", source: "codex", machineId: "m", eventId: `e${n}`, kind: "history", type: "history", userId: "u", actorName: "", actorRole: "", subagentId: "", projectId: "", taskTitle: "", taskRef: "{}", tags: [], filesTouched: [], toolName: "", toolStatus: "", content: "content" });
function binding(query: SearchIndexAdapter["query"]): SearchServiceBinding {
  return { index: { query } as SearchIndexAdapter, state: { async fromMs() { return 0; }, async record() {}, async partials() { return { sessions: [], overflow: false }; } } };
}
function api(adapter: SearchServiceBinding) {
  const logs: unknown[] = [];
  const app = createReadApi({ adapter, liveness, completeSinceMs: 0, verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), log: fields => logs.push(fields) });
  return { logs, get: (path: string) => app.request(path, { headers: { authorization: "Bearer synthetic" } }) };
}

test("portable reads reject another tenant and invalid ordering/cursors without leaking result content", async () => {
  for (const [status, result] of [
    [500, { hits: [{ ...hit(1), workspaceId: "other", content: "sensitive-search-content" }] }],
    [503, { hits: [hit(2), hit(1)] }],
    [503, { hits: [hit(1), hit(1)] }],
    [503, { hits: [hit(1)], next: { ...hit(0) } }],
  ] as const) {
    const { get, logs } = api(binding(async () => result));
    const response = await get("/v1/sessions/s/events?workspace=ws-a");
    expect(response.status).toBe(status);
    expect(JSON.stringify(await response.json()) + JSON.stringify(logs)).not.toContain("sensitive-search-content");
  }
});

test("portable byte-budget cut resumes at the last kept event without losing later hits", async () => {
  const rows = [1, 2, 3, 4].map(n => ({ ...hit(n), content: "x".repeat(1024 * 1024) }));
  const adapter = binding(async (q: SearchQuery) => ({ hits: rows.filter(row => !q.cursor || comparePositions(row, q.cursor) > 0) }));
  const first = await readEventPage({ adapter, liveness }, { ...tenant, order: "asc", limit: 4 });
  expect(first.events.map(row => row.eventId)).toEqual(["e1", "e2"]);
  expect(first.nextCursor).toBe(encodeCursor(hit(2)));
  const { decodeCursor } = await import("../cursor.ts");
  const second = await readEventPage({ adapter, liveness }, { ...tenant, order: "asc", limit: 4, cursor: decodeCursor(first.nextCursor) });
  expect(second.events.map(row => row.eventId)).toEqual(["e3", "e4"]);
  expect(second.nextCursor).toBeNull();
});

test("unknown completeness, bounded partial overflow and changed coverage refuse complete answers", async () => {
  let queried = false;
  const adapter = binding(async () => { queried = true; return { hits: [] }; });
  adapter.state.partials = async () => ({ sessions: [], overflow: true });
  const { get } = api(adapter);
  const overflow = await get("/v1/events?workspace=ws-a&q=hello&limit=10");
  expect(overflow.status).toBe(501);
  expect((await overflow.json() as any).error.reason).toBe("session_partial_unbounded");
  expect(queried).toBe(false);
  adapter.state.fromMs = async () => 1;
  const cursor = encodeCursor(hit(10));
  expect((await get(`/v1/sessions/s/events?workspace=ws-a&cursor=${encodeURIComponent(cursor)}`)).status).toBe(503);
  expect(queried).toBe(false);
});

test("a stuck provider query is bounded and receives an abort signal", async () => {
  let signal: AbortSignal | undefined;
  const adapter = binding(async (_q, context) => { signal = context?.signal; return new Promise(() => {}); });
  await expect(readEventPage({ adapter, liveness, timeoutMs: 5 }, { ...tenant, order: "asc", limit: 1 })).rejects.toThrow("timeout");
  expect(signal?.aborted).toBe(true);
});

test("READ_API_SEARCH_PARTIAL_SESSIONS=skip on the adapter path: overflow answers unbounded with no sessions list", async () => {
  let queried = 0;
  const adapter = binding(async () => { queried++; return { hits: [hit(1)] }; });
  const skip = createReadApi({ adapter, liveness, completeSinceMs: 0, searchSkipsPartialSessions: true, verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), log: () => {} });
  const get = (path: string) => skip.request(path, { headers: { authorization: "Bearer synthetic" } });
  // Overflow with a (truncated) list still names none: the list is not the whole set.
  adapter.state.partials = async () => ({ sessions: ["s-a", "s-b"], overflow: true });
  let r = await get("/v1/events?workspace=ws-a&q=hello&limit=10");
  expect(r.status).toBe(200);
  expect(r.headers.get("X-Sessions-Read-Incomplete")).toBe("session_partial_unbounded");
  expect((await r.json() as any).incomplete).toEqual({ reason: "session_partial_unbounded" });
  expect(queried).toBe(1);
  // A bounded list is named.
  adapter.state.partials = async () => ({ sessions: ["s-a"], overflow: false });
  r = await get("/v1/events?workspace=ws-a&q=hello&limit=10");
  expect((await r.json() as any).incomplete).toEqual({ reason: "session_partial", sessions: ["s-a"] });
  // A session-scoped read of a partial session still refuses.
  r = await get("/v1/sessions/s-a/events?workspace=ws-a");
  expect(r.status).toBe(501);
  expect((await r.json() as any).error.reason).toBe("session_partial");
  expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
});

test("READ_API_SEARCH_PARTIAL_SESSIONS=skip over postgres: a stale partial session's rows are left out of every page", async () => {
  const db = new PGlite({ extensions: { pg_trgm } });
  await db.exec("CREATE EXTENSION pg_trgm");
  const index = createPostgresSearchIndex({ client: db as unknown as SqlClient, tenantRunner: (o, w, work) => db.transaction(async tx => { await tx.query(TENANT_SETTINGS_SQL, [o, w]); return work(tx as unknown as SqlClient); }) });
  await index.ensureSchema();
  // "s-stale" was indexed while small and has since grown past the bound: partial, its rows stale.
  const doc = (sessionId: string, n: number) => ({ ...hit(n), sessionId, eventId: `${sessionId}-${n}`, content: "needle", taskRefId: "", deliveryRecordId: "", fingerprint: `fp-${n}`, version: 1 });
  await index.upsert([doc("s-ok", 1), doc("s-stale", 2), doc("s-ok", 3), doc("s-stale", 4), doc("s-ok", 5), doc("s-stale", 6)]);
  const queries: SearchQuery[] = [];
  let partials: { sessions: string[]; overflow: boolean } = { sessions: ["s-stale"], overflow: false };
  const adapter: SearchServiceBinding = {
    index: { ...index, query: (q, context) => { queries.push(q); return index.query(q, context); } },
    state: { async fromMs() { return 0; }, async record() {}, async partials() { return partials; } },
  };
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({ adapter, liveness, completeSinceMs: 0, searchSkipsPartialSessions: true, verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), log: fields => logs.push(fields) });
  const get = (path: string) => app.request(path, { headers: { authorization: "Bearer synthetic" } });
  const pages = async () => {
    const seen: { events: string[]; header: string | null; incomplete: unknown }[] = [];
    let cursor: string | null = null;
    do {
      const r = await get(`/v1/events?workspace=ws-a&q=needle&limit=2${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
      expect(r.status).toBe(200);
      const b = await r.json() as any;
      seen.push({ events: b.events.map((e: { eventId: string }) => e.eventId), header: r.headers.get("X-Sessions-Read-Incomplete"), incomplete: b.incomplete });
      cursor = b.nextCursor;
    } while (cursor && seen.length < 10);
    return seen;
  };
  const listed = await pages();
  expect(listed.flatMap(p => p.events)).toEqual(["s-ok-5", "s-ok-3", "s-ok-1"]);
  expect(listed.length).toBeGreaterThanOrEqual(2);
  for (const page of listed) {
    expect(page.header).toBe("session_partial");
    expect(page.incomplete).toEqual({ reason: "session_partial", sessions: ["s-stale"] });
  }
  expect(queries.every(q => JSON.stringify(q.excludeSessionIds) === JSON.stringify(["s-stale"]))).toBe(true);
  expect(logs.filter(l => l.reason === "session_partial_skipped").every(l => l.excluded_sessions === 1)).toBe(true);
  expect(JSON.stringify(logs)).not.toContain("s-stale");
  // The partial set is recomputed per page: once the session completes, the next pages hold its rows again.
  partials = { sessions: [], overflow: false };
  queries.length = 0;
  const done = await pages();
  expect(done.flatMap(p => p.events)).toEqual(["s-stale-6", "s-ok-5", "s-stale-4", "s-ok-3", "s-stale-2", "s-ok-1"]);
  expect(done.every(p => p.header === null && p.incomplete === undefined)).toBe(true);
  expect(queries.every(q => !("excludeSessionIds" in q))).toBe(true);
  // Unbounded: nothing is listed, so nothing is excluded; every page says so.
  partials = { sessions: ["s-stale"], overflow: true };
  queries.length = 0;
  const unbounded = await pages();
  expect(unbounded.flatMap(p => p.events)).toHaveLength(6);
  expect(unbounded.every(p => p.header === "session_partial_unbounded" && JSON.stringify(p.incomplete) === JSON.stringify({ reason: "session_partial_unbounded" }))).toBe(true);
  expect(queries.every(q => !("excludeSessionIds" in q))).toBe(true);
  // A search scoped to the excluded session keeps its 501 (the session check runs first).
  partials = { sessions: ["s-stale"], overflow: false };
  const scoped = await get("/v1/events?workspace=ws-a&session=s-stale&q=needle&limit=2");
  expect(scoped.status).toBe(501);
  expect((await scoped.json() as any).error.reason).toBe("session_partial");
  await db.close();
});

test("an adapter that ignores excludeSessionIds fails the page instead of serving the excluded session's rows", async () => {
  const adapter = binding(async () => ({ hits: [{ ...hit(2), sessionId: "s-stale", content: "stale-content" }, hit(1)] }));
  adapter.state.partials = async () => ({ sessions: ["s-stale"], overflow: false });
  const app = createReadApi({ adapter, liveness, completeSinceMs: 0, searchSkipsPartialSessions: true, verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), log: () => {} });
  const r = await app.request("/v1/events?workspace=ws-a&q=hello&limit=10", { headers: { authorization: "Bearer synthetic" } });
  expect(r.status).toBe(503);
  expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
  expect(await r.text()).not.toContain("stale-content");
});

test("READ_API_SEARCH_PARTIAL_SESSIONS=skip on the adapter path: a cursor page whose state.partials throws is a 503 with no detail and no header", async () => {
  let queried = 0;
  const adapter = binding(async () => { queried++; return { hits: [hit(1)] }; });
  adapter.state.partials = async () => { throw new Error("partials exploded s-stale secret-detail"); };
  const logs: Record<string, unknown>[] = [];
  const app = createReadApi({ adapter, liveness, completeSinceMs: 0, searchSkipsPartialSessions: true, verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), log: fields => logs.push(fields) });
  const cursor = encodeCursor({ tsMs: 5, sessionId: "s", source: "codex", machineId: "m", kind: "history", eventId: "e5" });
  const r = await app.request(`/v1/events?workspace=ws-a&q=hello&limit=10&cursor=${cursor}`, { headers: { authorization: "Bearer synthetic" } });
  expect(r.status).toBe(503);
  expect(r.headers.get("X-Sessions-Read-Incomplete")).toBeNull();
  const text = await r.text();
  for (const leak of ["exploded", "secret-detail", "s-stale"]) expect(text).not.toContain(leak);
  expect(JSON.parse(text)).toEqual({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId: r.headers.get("x-correlation-id") });
  expect(queried).toBe(0);
  expect(logs.some(l => String(l.reason).endsWith("_skipped"))).toBe(false);
  expect(JSON.stringify(logs)).not.toContain("exploded");
});
