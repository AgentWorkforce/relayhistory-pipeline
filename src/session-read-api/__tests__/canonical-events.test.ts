import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { createReadApi, type ReadApiDeps } from "../app.ts";
import { CanonicalReadError } from "../canonical.ts";
import { CANONICAL_CURSOR_PREFIX } from "../canonical-events.ts";
import { postgresSearchState } from "../../session-pipeline/postgres-search-state.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import type { Liveness } from "../page.ts";
import { encodeCursor } from "../cursor.ts";

const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const floor = Date.parse("2026-11-08T22:02:00Z");
const row = (n: number) => ({ table: "convergence_events", row: { session_id: "old", source: "codex", event_id: `original-${n}`, machine_id: "machine", kind: "history", type: n === 1 ? "user" : "assistant", ts: `2026-11-07T10:00:0${n}.000Z`, content: `actual-old-message-${n}`, user_id: "u" } });
function fixture() {
  let reads = 0, queries = 0, partials = 0;
  const state = { async fromMs() { return floor; }, async record() {}, async partials() { partials++; return { sessions: [] as string[], overflow: false }; } };
  const adapter: SearchServiceBinding = { index: { backend: "postgres", async query() { queries++; return { hits: [] }; } } as unknown as SearchIndexAdapter, state };
  const liveness: Liveness = { async erased() { return new Set(); }, async sessionStart() { return { pointers: 1, live: 1, createdAtMs: floor - 86400000 }; }, async livePointerSessions(_o, _w, ids) { return new Set(ids); } };
  const deps: ReadApiDeps = { verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }), adapter, liveness, completeSinceMs: floor, selfhost: { login: async () => { throw Error("not used"); } }, directoryWorkspaces: new Set(["ws-a"]), canonicalEvents: { source: async (identity) => { reads++; expect(identity).toEqual({ ...tenant, sessionId: "old", source: "codex" }); return { rows: [row(2), row(1)], legacyCoverage: null, exists: true, generation: "a".repeat(64) }; } } };
  const get = (suffix = "", path = "/v1/sessions/old/events") => createReadApi(deps).request(`${path}?workspace=ws-a&source=codex${suffix}`, { headers: { authorization: "Bearer synthetic" } });
  return { deps, state, liveness, get, counts: () => ({ reads, queries, partials }) };
}

test("old exact canonical replay uses full source, original identities/content and generation-bound pages despite empty retention index", async () => {
  const f = fixture();
  const first = await f.get("&limit=1&maxContent=100"); expect(first.status).toBe(200);
  const a = await first.json() as any;
  expect(a.events.map((r: any) => [r.eventId, r.content])).toEqual([["original-1", "actual-old-message-1"]]);
  expect(a.nextCursor.startsWith(CANONICAL_CURSOR_PREFIX)).toBe(true);
  const second = await f.get(`&limit=1&cursor=${encodeURIComponent(a.nextCursor)}`); expect(second.status).toBe(200);
  const b = await second.json() as any;
  expect(b.events.map((r: any) => [r.eventId, r.content])).toEqual([["original-2", "actual-old-message-2"]]); expect(b.nextCursor).toBeNull();
  expect(f.counts()).toEqual({ reads: 2, queries: 0, partials: 4 });
  f.deps.canonicalEvents!.source = async () => ({ rows: [row(1)], exists: true, generation: "b".repeat(64), legacyCoverage: null });
  expect((await f.get(`&cursor=${encodeURIComponent(a.nextCursor)}`)).status).toBe(501);
});

test("partial, overflow, missing source, foreign scope, changed source and erasure never admit old content", async () => {
  for (const mode of ["partial", "overflow", "missing", "foreign", "corrupt", "erased-before", "erased-after", "foreign-workspace", "no-pointer"] as const) {
    const f = fixture();
    if (mode === "partial") f.state.partials = async () => ({ sessions: ["old"], overflow: false });
    if (mode === "overflow") f.state.partials = async () => ({ sessions: [], overflow: true });
    if (mode === "missing") f.deps.canonicalEvents!.source = async () => ({ rows: [], exists: false, generation: "a".repeat(64), legacyCoverage: null });
    if (mode === "foreign") f.deps.canonicalEvents!.source = async () => ({ rows: [{ ...row(1), row: { ...row(1).row, workspace_id: "other" } }], exists: true, generation: "a".repeat(64), legacyCoverage: null });
    if (mode === "corrupt") f.deps.canonicalEvents!.source = async () => { throw new CanonicalReadError("manifest_mismatch"); };
    if (mode === "erased-before") f.liveness.sessionStart = async () => ({ pointers: 1, live: 0, createdAtMs: null });
    if (mode === "erased-after") f.liveness.erased = async () => new Set(["codex\u0000old"]);
    if (mode === "foreign-workspace") f.deps.verify = async () => ({ ...tenant, workspaceId: "other", userId: "u", scopes: ["rth:read"] });
    if (mode === "no-pointer") f.liveness.sessionStart = async () => ({ pointers: 0, live: 0, createdAtMs: null });
    const response = await f.get(); const body = await response.json() as any;
    expect(body.events ?? []).toEqual([]); expect(JSON.stringify(body)).not.toContain("actual-old-message");
    expect(response.status, mode).toBe(mode.startsWith("erased") ? 200 : mode === "overflow" ? 503 : mode === "foreign-workspace" ? 403 : 501);
    expect(f.counts().queries).toBe(0);
  }
});

test("canonical admission stays service/postgres/allowlist/exact-source only; global search floor remains unchanged", async () => {
  for (const mode of ["worker", "other-backend", "allowlist", "no-source"] as const) {
    const f = fixture();
    if (mode === "worker") delete f.deps.selfhost;
    if (mode === "other-backend") f.deps.adapter = { ...f.deps.adapter!, index: { ...f.deps.adapter!.index, backend: "other" } as unknown as SearchIndexAdapter };
    if (mode === "allowlist") f.deps.directoryWorkspaces = new Set();
    const response = mode === "no-source" ? await createReadApi(f.deps).request("/v1/sessions/old/events?workspace=ws-a", { headers: { authorization: "Bearer synthetic" } }) : await f.get();
    expect(response.status).toBe(501); expect(f.counts().reads).toBe(0);
  }
  const f = fixture(); let since: number | undefined;
  f.deps.adapter!.index.query = async q => { since = q.sinceMs; return { hits: [] }; };
  const search = await f.get("&q=old-message&limit=10", "/v1/events");
  expect(search.status).toBe(501); expect(since).toBe(floor); expect(f.counts().reads).toBe(0);
  const foreignCursor = encodeCursor({ tsMs: floor, sessionId: "other", source: "codex", machineId: "m", kind: "history", eventId: "e" });
  expect((await f.get(`&cursor=${encodeURIComponent(foreignCursor)}`)).status).toBe(501);
});

test("real Postgres completeness query refuses stale epoch, missing state/directory and foreign canonical binding before S3", async () => {
  const db = new PGlite();
  try {
    await db.exec(`CREATE SCHEMA sessions;
      CREATE TABLE sessions.session_index(org_id text,workspace_id text,harness text,session_id text,canonical_session_id text,deleted_at timestamptz);
      CREATE TABLE sessions.session_directory(org_id text,workspace_id text,harness text,session_id text,event_projection_epoch bigint,event_projection_ready boolean);
      INSERT INTO sessions.session_index VALUES('org-a','ws-a','codex','old',NULL,NULL);
      INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','old',3,true);`);
    const state = postgresSearchState({ reader: (_o, _w, work) => work(db), backendId: "test", fromMs: floor }); await state.ensureSchema(db);
    const f = fixture(); f.deps.adapter = { ...f.deps.adapter!, state };
    expect((await f.get()).status).toBe(501); expect(f.counts().reads).toBe(0);
    await db.transaction(tx => state.record(tx, { ...tenant, harness: "codex", sessionId: "old" }, { revision: 1, rows: 0, complete: true, at: new Date(), canonicalSessionId: "old" }));
    expect((await f.get()).status).toBe(200); expect(f.counts().reads).toBe(1);
    for (const sql of ["UPDATE sessions.session_directory SET event_projection_epoch=4", "UPDATE sessions.session_directory SET event_projection_epoch=3,event_projection_ready=false", "UPDATE sessions.session_directory SET event_projection_ready=true; UPDATE search_index.projection_state SET canonical_session_id='other'", "DELETE FROM sessions.session_directory"]) {
      await db.exec(sql); expect((await f.get()).status).toBe(501); expect(f.counts().reads).toBe(1);
    }
  } finally { await db.close(); }
}, 30000);

test("canonical continuation refuses different session/source, partial transition and never silently switches to search", async () => {
  const f = fixture(); const first = await (await f.get("&limit=1")).json() as any;
  for (const replacement of ["other-session", "other-source"] as const) {
    const parts = JSON.parse(Buffer.from(first.nextCursor.slice(CANONICAL_CURSOR_PREFIX.length), "base64url").toString());
    const { decodeCursor } = await import("../cursor.ts"); const position = decodeCursor(parts[1])!;
    parts[1] = encodeCursor({ ...position, ...(replacement === "other-session" ? { sessionId: "other" } : { source: "claude" }) });
    const cursor = CANONICAL_CURSOR_PREFIX + Buffer.from(JSON.stringify(parts)).toString("base64url");
    expect((await f.get(`&cursor=${encodeURIComponent(cursor)}`)).status).toBe(400);
  }
  f.state.partials = async () => ({ sessions: ["old"], overflow: false });
  expect((await f.get(`&cursor=${encodeURIComponent(first.nextCursor)}`)).status).toBe(501);
  expect(f.counts().queries).toBe(0);
  const changed = fixture(); let checks = 0;
  changed.state.partials = async () => ({ sessions: ++checks > 1 ? ["old"] : [], overflow: false });
  const refused = await changed.get(); expect(refused.status).toBe(501);
  expect(JSON.stringify(await refused.json())).not.toContain("actual-old-message");
});

test("canonical replay preserves the existing explicitly selected message-id enrichment", async () => {
  const f = fixture(); let enrichments = 0;
  f.deps.eventMessageIds = { source: async identity => {
    enrichments++; expect(identity).toEqual({ ...tenant, sessionId: "old", source: "codex" });
    return { rows: [1, 2].map(n => ({ ...row(n), row: { ...row(n).row, record: { payload: { message_id: `message-${n}` } } } })), legacyCoverage: null };
  } };
  const ordinary = await (await f.get()).json() as any;
  expect(enrichments).toBe(0); expect(ordinary.events[0].messageId).toBeUndefined();
  const enriched = await f.get("&includeMessageId=true"); expect(enriched.status).toBe(200);
  expect((await enriched.json() as any).events.map((r: any) => [r.eventId, r.messageId])).toEqual([["original-1", "message-1"], ["original-2", "message-2"]]);
  expect(enrichments).toBe(1); expect(f.counts().queries).toBe(0);
});
