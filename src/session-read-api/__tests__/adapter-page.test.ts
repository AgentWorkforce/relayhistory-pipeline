import { expect, test } from "bun:test";
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
