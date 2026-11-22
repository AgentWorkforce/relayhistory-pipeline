/**
 * Hosted canonical events (ReadApiDeps.canonicalEventsHosted, READ_API_CANONICAL_EVENTS_HOSTED on
 * the read Worker): GET /v1/sessions/:sessionId/events serves a session older than the retention
 * floor from its canonical objects, for an allowlisted workspace and one resolved source. Without
 * the dep, off the allowlist, or inside the floor, every answer is byte-identical to today's
 * (status, headers, body). The self-host path (canonical-events.test.ts) keeps its own codes.
 */
import { expect, test } from "bun:test";
import { createReadApi, type ReadApiDeps } from "../app.ts";
import { CANONICAL_CURSOR_PREFIX } from "../canonical-events.ts";
import { encodeCursor } from "../cursor.ts";
import type { SessionSourceRow } from "../session-sources.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import type { SearchIndexAdapter } from "@rhc/search-index/contracts.ts";
import { memoryLiveness } from "./support.ts";

const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const floor = Date.parse("2026-11-08T22:02:00Z");
const OLD = floor - 86_400_000;
const NEW = floor + 3_600_000;
const GENERATION = "a".repeat(64);

const objectRow = (n: number, over: Record<string, unknown> = {}) => ({
  table: "convergence_events",
  row: { session_id: "old", source: "codex", event_id: `original-${n}`, machine_id: "machine", kind: "history", type: n === 1 ? "user" : "assistant", ts: `2026-11-07T10:00:0${n}.000Z`, content: `actual-old-message-${n}`, user_id: "u", ...over },
});
const sourceRow = (harness: string, over: Partial<SessionSourceRow> = {}): SessionSourceRow =>
  ({ org_id: "org-a", workspace_id: "ws-a", harness, session_ids: ["old"], canonical_ids: [], ...over });

/** A stub ClickHouse: one indexed event for the session, and the partial-session state rows. */
function stubCh() {
  const asked: string[] = [];
  let partial: string[] = [];
  const cand = { ts_ms: NEW + 1000, session_id: "old", source: "codex", machine_id: "m-1", kind: "message", event_id: "indexed-1" };
  const newest = [{ ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-1", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "indexed content", in_budget: 1, origin: "canonical", origin_claim_token: "" }];
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return partial.filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      asked.push(query);
      return (query.includes("bytes_before") ? newest : [cand]) as T[];
    },
  };
  return { ch, asked, setPartial: (ids: string[]) => { partial = ids; } };
}

interface Options {
  /** Created-at per harness of session "old"; default codex older than the floor. */
  created?: Record<string, number>;
  /** Off: no hosted dep and no canonicalEvents (the Worker today). */
  hosted?: false | { workspaces: ReadonlySet<string> | "*"; sources?: SessionSourceRow[] };
}

function fixture(o: Options = {}) {
  const ch = stubCh();
  const created = o.created ?? { codex: OLD };
  const mem = memoryLiveness({ createdAt: Object.fromEntries(Object.entries(created).map(([h, t]) => [`${h}\u0000old`, t])) });
  let reads = 0, sourceCalls = 0;
  // Identities the object reader was asked with; asserted after the request, never inside the stub
  // (an expect thrown there would be caught by the route's error handling).
  const identities: unknown[] = [];
  let objectRows = [objectRow(2), objectRow(1)];
  let generation = GENERATION;
  const deps: ReadApiDeps = {
    verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }),
    ch: ch.ch,
    liveness: mem.liveness,
    completeSinceMs: floor,
    log: () => {},
  };
  if (o.hosted) {
    const sources = o.hosted.sources ?? [sourceRow("codex")];
    deps.canonicalEvents = { source: async (identity) => { reads++; identities.push(identity); return { rows: objectRows, legacyCoverage: null, exists: true, generation }; } };
    deps.canonicalEventsHosted = { workspaces: o.hosted.workspaces, sources: async () => { sourceCalls++; return sources; } };
  }
  const get = (query: string) => createReadApi(deps).request(`/v1/sessions/old/events?workspace=ws-a${query}`, { headers: { authorization: "Bearer synthetic", "x-correlation-id": "corr-1" } });
  return {
    deps, mem, ch, get,
    setRows: (rows: typeof objectRows) => { objectRows = rows; },
    setGeneration: (g: string) => { generation = g; },
    counts: () => ({ reads, sourceCalls, indexQueries: ch.asked.length }),
    identities,
  };
}

const snapshot = async (r: Response) => ({ status: r.status, headers: [...r.headers.entries()].sort(), body: await r.text() });
const json = { status: 0, headers: [["cache-control", "private, no-store"], ["content-type", "application/json"], ["x-correlation-id", "corr-1"], ["x-relayhistory-workspace-id", "ws-a"], ["x-sessions-read-backend", "96bulls"]] };
const unsupportedBody = (reason: string) => `{"error":{"code":"unsupported_by_backend","message":"not served by this backend: ${reason}","reason":"${reason}"},"correlationId":"corr-1"}`;
const transitionBody = `{"error":{"code":"cursor_backend_transition","message":"the cursor was issued by another backend for this session; restart pagination without a cursor","reason":"cursor_backend_transition"},"correlationId":"corr-1"}`;
const indexedPage = `{"sessionId":"old","events":[{"eventId":"indexed-1","userId":"u-1","sessionId":"old","source":"codex","kind":"message","type":"assistant","ts":"2026-11-08T23:02:01.000Z","actorName":null,"actorRole":null,"subagentId":null,"machineId":"m-1","projectId":null,"trajectoryId":null,"taskRef":{},"taskTitle":null,"content":"indexed content","contentTruncated":false,"tags":[],"toolName":null,"toolStatus":null,"filesTouched":[]}],"nextCursor":null,"correlationId":"corr-1"}`;
const searchCursor = encodeURIComponent(encodeCursor({ tsMs: NEW, sessionId: "old", source: "codex", machineId: "m-1", kind: "message", eventId: "e-0" }));
const objectCursor = (generation = GENERATION) => encodeURIComponent(CANONICAL_CURSOR_PREFIX + Buffer.from(JSON.stringify([generation, encodeCursor({ tsMs: Date.parse("2026-11-07T10:00:01.000Z"), sessionId: "old", source: "codex", machineId: "machine", kind: "history", eventId: "original-1" })])).toString("base64url"));

// Golden answers with the flag off. Each pins status, headers and body exactly.

test("flag off golden: an old session is the 501 history_floor", async () => {
  const f = fixture({ hosted: false });
  expect(await snapshot(await f.get("&source=codex"))).toEqual({ ...json, status: 501, body: unsupportedBody("history_floor") });
  expect(await snapshot(await f.get(""))).toEqual({ ...json, status: 501, body: unsupportedBody("history_floor") });
});

test("flag off golden: a session inside the floor is a ClickHouse page", async () => {
  const f = fixture({ hosted: false, created: { codex: NEW } });
  expect(await snapshot(await f.get("&source=codex"))).toEqual({ ...json, status: 200, body: indexedPage });
});

test("flag off golden: an object cursor is the 501 canonical_cursor_scope", async () => {
  const f = fixture({ hosted: false });
  expect(await snapshot(await f.get(`&source=codex&cursor=${objectCursor()}`))).toEqual({ ...json, status: 501, body: unsupportedBody("canonical_cursor_scope") });
  const inside = fixture({ hosted: false, created: { codex: NEW } });
  expect(await snapshot(await inside.get(`&source=codex&cursor=${objectCursor()}`))).toEqual({ ...json, status: 501, body: unsupportedBody("canonical_cursor_scope") });
});

test("flag off golden: the self-host service keeps the 501 canonical_cursor_required, with or without the hosted dep", async () => {
  for (const withHosted of [false, true]) {
    const f = fixture(withHosted ? { hosted: { workspaces: "*" } } : { hosted: false });
    const state = { async fromMs() { return floor; }, async record() {}, async partials() { return { sessions: [] as string[], overflow: false }; } };
    f.deps.adapter = { index: { backend: "postgres", async query() { return { hits: [] }; } } as unknown as SearchIndexAdapter, state } as SearchServiceBinding;
    delete f.deps.ch;
    f.deps.selfhost = { login: async () => { throw Error("not used"); } };
    f.deps.directoryWorkspaces = new Set(["ws-a"]);
    f.deps.canonicalEvents ??= { source: async () => { throw Error("not read"); } };
    expect([withHosted, await snapshot(await f.get(`&source=codex&cursor=${searchCursor}`))]).toEqual([withHosted, { ...json, status: 501, body: unsupportedBody("canonical_cursor_required") }]);
    expect(f.counts().sourceCalls).toBe(0);
  }
});

// The hosted dep present.

test("hosted: one resolved source is served from the canonical objects, paged by object cursor", async () => {
  const f = fixture({ hosted: { workspaces: new Set(["ws-a"]) } });
  const first = await f.get("&limit=1");
  expect(first.status).toBe(200);
  const a = await first.json() as { events: Array<{ eventId: string; content: string; source: string }>; nextCursor: string };
  expect(a.events.map((e) => [e.eventId, e.source, e.content])).toEqual([["original-1", "codex", "actual-old-message-1"]]);
  expect(a.nextCursor.startsWith(CANONICAL_CURSOR_PREFIX)).toBe(true);
  const second = await (await f.get(`&limit=1&cursor=${encodeURIComponent(a.nextCursor)}`)).json() as { events: Array<{ eventId: string }>; nextCursor: string | null };
  expect(second.events.map((e) => e.eventId)).toEqual(["original-2"]);
  expect(second.nextCursor).toBeNull();
  // With `source` given the sources reader is not asked.
  expect((await f.get("&source=codex")).status).toBe(200);
  expect(f.counts()).toEqual({ reads: 3, sourceCalls: 2, indexQueries: 0 });
  const codex = { ...tenant, sessionId: "old", source: "codex" };
  expect(f.identities).toEqual([codex, codex, codex]);
  const star = fixture({ hosted: { workspaces: "*" } });
  expect((await star.get("")).status).toBe(200);
  expect(star.identities).toEqual([codex]);
});

test("hosted: more than one live source is a 501 events_canonical_source_ambiguous", async () => {
  const f = fixture({ hosted: { workspaces: new Set(["ws-a"]), sources: [sourceRow("claude"), sourceRow("codex")] }, created: { codex: OLD, claude: OLD } });
  expect(await snapshot(await f.get(""))).toEqual({ ...json, status: 501, body: unsupportedBody("events_canonical_source_ambiguous") });
  expect(f.counts()).toEqual({ reads: 0, sourceCalls: 1, indexQueries: 0 });
  // An explicit source is not ambiguous.
  expect((await f.get("&source=codex")).status).toBe(200);
  expect(f.identities).toEqual([{ ...tenant, sessionId: "old", source: "codex" }]);
});

/** An operator erase fence over (source, id) keys; `after` keys turn fenced once the object read returns. */
function fence(f: ReturnType<typeof fixture>, keys: string[], after: string[] = []) {
  const fenced = new Set(keys);
  const asked: string[] = [];
  f.deps.operatorErased = async (key) => {
    asked.push(`${key.orgId}/${key.workspaceId}/${key.source}/${key.sessionId}`);
    return fenced.has(`${key.source}/${key.sessionId}`);
  };
  if (after.length) {
    const read = f.deps.canonicalEvents!.source;
    f.deps.canonicalEvents!.source = async (identity) => {
      const got = await read(identity);
      for (const key of after) fenced.add(key);
      return got;
    };
  }
  return asked;
}

test("hosted: an erase fence that commits during the object read answers 404 with no content", async () => {
  for (const query of ["", "&source=codex"]) {
    const f = fixture({ hosted: { workspaces: "*" } });
    fence(f, [], ["codex/old"]);
    const r = await f.get(query);
    const body = await r.text();
    expect([query, r.status, body.includes("actual-old-message")]).toEqual([query, 404, false]);
    expect([query, f.counts().reads]).toEqual([query, 1]);
  }
  // A fence under another form of the resolved key (a source id its rows hold) is rechecked too.
  const alias = fixture({ hosted: { workspaces: "*", sources: [sourceRow("codex", { canonical_ids: ["old-source-id"] })] } });
  const asked = fence(alias, [], ["codex/old-source-id"]);
  const r = await alias.get("");
  expect([r.status, (await r.text()).includes("actual-old-message")]).toEqual([404, false]);
  expect(asked).toEqual(["codex/old", "codex/old-source-id", "codex/old", "codex/old-source-id"].map((k) => `org-a/ws-a/${k}`));
  // No fence at all: served.
  const open = fixture({ hosted: { workspaces: "*" } });
  fence(open, []);
  expect((await open.get("")).status).toBe(200);
});

test("hosted: a fenced source is dropped before the source is resolved", async () => {
  // Two live sources, one fenced: the other is served, not a 501.
  const two = fixture({ hosted: { workspaces: "*", sources: [sourceRow("claude"), sourceRow("codex")] }, created: { codex: OLD, claude: OLD } });
  fence(two, ["claude/old"]);
  const r = await two.get("");
  expect(r.status).toBe(200);
  const page = await r.json() as { events: Array<{ source: string; content: string }> };
  expect(page.events.map((e) => [e.source, e.content])).toEqual([["codex", "actual-old-message-1"], ["codex", "actual-old-message-2"]]);
  expect(two.identities).toEqual([{ ...tenant, sessionId: "old", source: "codex" }]);
  // Fenced under another form of its key only (a physical id its rows hold): dropped as well.
  const physical = fixture({ hosted: { workspaces: "*", sources: [sourceRow("claude", { session_ids: ["old", "old-physical"] }), sourceRow("codex")] }, created: { codex: OLD, claude: OLD } });
  fence(physical, ["claude/old-physical"]);
  expect((await physical.get("")).status).toBe(200);
  expect(physical.identities).toEqual([{ ...tenant, sessionId: "old", source: "codex" }]);
  // A single fenced source: 404, nothing read.
  for (const key of ["codex/old", "codex/old-source-id"]) {
    const one = fixture({ hosted: { workspaces: "*", sources: [sourceRow("codex", { canonical_ids: ["old-source-id"] })] } });
    fence(one, [key]);
    const n = await one.get("");
    expect([key, n.status, (await n.text()).includes("actual-old-message")]).toEqual([key, 404, false]);
    expect([key, one.counts().reads]).toEqual([key, 0]);
  }
  // Both fenced: 404, not ambiguous.
  const both = fixture({ hosted: { workspaces: "*", sources: [sourceRow("claude"), sourceRow("codex")] }, created: { codex: OLD, claude: OLD } });
  fence(both, ["claude/old", "codex/old"]);
  expect((await both.get("")).status).toBe(404);
  expect(both.counts().reads).toBe(0);
});

test("hosted: an id that is also an alias of another source's session stays a 501 ambiguous (known limit)", async () => {
  const f = fixture({ hosted: { workspaces: "*", sources: [sourceRow("claude", { session_ids: ["other"], canonical_ids: ["old"] }), sourceRow("codex")] }, created: { codex: OLD, claude: OLD } });
  fence(f, []);
  expect(await snapshot(await f.get(""))).toEqual({ ...json, status: 501, body: unsupportedBody("events_canonical_source_ambiguous") });
  expect(f.counts().reads).toBe(0);
  // The caller can pass `source`.
  expect((await f.get("&source=codex")).status).toBe(200);
});

test("hosted: no live source resolved is today's answer byte for byte", async () => {
  const off = await snapshot(await fixture({ hosted: false }).get(""));
  const f = fixture({ hosted: { workspaces: "*", sources: [] } });
  expect(await snapshot(await f.get(""))).toEqual(off);
  expect(f.counts().reads).toBe(0);
});

test("hosted: a workspace off the hosted allowlist gets today's answer byte for byte", async () => {
  for (const workspaces of [new Set<string>(), new Set(["ws-other"])]) {
    for (const query of ["&source=codex", "", `&source=codex&cursor=${objectCursor()}`, `&source=codex&cursor=${searchCursor}`]) {
      const off = await snapshot(await fixture({ hosted: false }).get(query));
      const f = fixture({ hosted: { workspaces } });
      expect([query, await snapshot(await f.get(query))]).toEqual([query, off]);
      expect([query, f.counts()]).toEqual([query, { reads: 0, sourceCalls: 0, indexQueries: 0 }]);
    }
  }
});

test("hosted: a session inside the floor gets today's ClickHouse answer byte for byte", async () => {
  for (const created of [{ codex: NEW }, { codex: NEW, claude: NEW }]) {
    for (const query of ["&source=codex", ""]) {
      const off = await snapshot(await fixture({ hosted: false, created }).get(query));
      const f = fixture({ hosted: { workspaces: "*", sources: Object.keys(created).map((h) => sourceRow(h)) }, created });
      const on = await snapshot(await f.get(query));
      expect(on).toEqual(off);
      expect(on.status).toBe(200);
      expect(f.counts().reads).toBe(0);
    }
  }
  // An old session whose resolved source is itself inside the floor: today's answer too.
  const mixed = { codex: NEW, claude: OLD };
  const f = fixture({ hosted: { workspaces: "*", sources: [sourceRow("codex")] }, created: mixed });
  expect(await snapshot(await f.get(""))).toEqual(await snapshot(await fixture({ hosted: false, created: mixed }).get("")));
  expect(f.counts().reads).toBe(0);
});

test("hosted: a search cursor on a session served from objects is a 409 cursor_backend_transition", async () => {
  const f = fixture({ hosted: { workspaces: "*" } });
  for (const query of [`&cursor=${searchCursor}`, `&source=codex&cursor=${searchCursor}`])
    expect(await snapshot(await f.get(query))).toEqual({ ...json, status: 409, body: transitionBody });
  expect(f.counts().reads).toBe(0);
});

test("hosted: an object cursor on a session not served from objects is a 409 cursor_backend_transition", async () => {
  const f = fixture({ hosted: { workspaces: "*" }, created: { codex: NEW } });
  expect(await snapshot(await f.get(`&source=codex&cursor=${objectCursor()}`))).toEqual({ ...json, status: 409, body: transitionBody });
  expect(f.counts()).toEqual({ reads: 0, sourceCalls: 0, indexQueries: 0 });
});

test("hosted: a canonical generation that moved under an object cursor is a 409; without a cursor it stays a 501", async () => {
  const f = fixture({ hosted: { workspaces: "*" } });
  f.setGeneration("b".repeat(64));
  expect(await snapshot(await f.get(`&source=codex&cursor=${objectCursor()}`))).toEqual({ ...json, status: 409, body: transitionBody });
  f.setGeneration("not-a-generation");
  expect(await snapshot(await f.get("&source=codex"))).toEqual({ ...json, status: 501, body: unsupportedBody("events_canonical_source_generation_changed") });
  // A malformed object cursor stays a 400.
  expect((await f.get(`&source=codex&cursor=${encodeURIComponent(`${CANONICAL_CURSOR_PREFIX}x`)}`)).status).toBe(400);
});

test("hosted: erasure wins over canonical content", async () => {
  const after = fixture({ hosted: { workspaces: "*" } });
  after.deps.liveness = { ...after.mem.liveness, erased: async () => new Set(["codex\u0000old"]) };
  const r = await after.get("");
  expect(r.status).toBe(200);
  expect(JSON.parse(await r.text())).toEqual({ sessionId: "old", events: [], nextCursor: null, correlationId: "corr-1" });
  const tombstoned = fixture({ hosted: { workspaces: "*" } });
  tombstoned.deps.liveness = { ...tombstoned.mem.liveness, sessionStart: async () => ({ pointers: 1, live: 0, createdAtMs: null }) };
  const t = await tombstoned.get("&source=codex");
  expect(JSON.parse(await t.text())).toEqual({ sessionId: "old", events: [], nextCursor: null, correlationId: "corr-1" });
  expect(tombstoned.counts().reads).toBe(0);
  const fenced = fixture({ hosted: { workspaces: "*" } });
  fenced.deps.operatorErased = async () => true;
  const n = await fenced.get("");
  expect(n.status).toBe(404);
  expect(await n.text()).not.toContain("actual-old-message");
  expect(fenced.counts().reads).toBe(0);
});

test("hosted: a partial session refuses before and after the object read", async () => {
  const before = fixture({ hosted: { workspaces: "*" } });
  before.ch.setPartial(["old"]);
  expect(await snapshot(await before.get(""))).toEqual({ ...json, status: 501, body: unsupportedBody("session_partial") });
  expect(before.counts().reads).toBe(0);
  const during = fixture({ hosted: { workspaces: "*" } });
  during.deps.canonicalEvents!.source = async () => { during.ch.setPartial(["old"]); return { rows: [objectRow(1)], legacyCoverage: null, exists: true, generation: GENERATION }; };
  const r = await during.get("&source=codex");
  expect(r.status).toBe(501);
  expect(await r.text()).not.toContain("actual-old-message");
});

test("hosted: a tenant-mismatched row is never served", async () => {
  for (const over of [{ workspace_id: "ws-other" }, { org_id: "org-other" }, { source: "claude" }, { session_id: "other" }]) {
    const f = fixture({ hosted: { workspaces: "*" } });
    f.setRows([objectRow(1), objectRow(2, over)]);
    const r = await f.get("");
    expect([over, r.status]).toEqual([over, 501]);
    expect(await r.text()).toContain("events_canonical_tenant_mismatch");
  }
  for (const row of [sourceRow("codex", { workspace_id: "ws-other" }), sourceRow("codex", { org_id: "org-other" })]) {
    const f = fixture({ hosted: { workspaces: "*", sources: [row] } });
    const r = await f.get("");
    expect([row, r.status]).toEqual([row, 500]);
    expect(await r.text()).not.toContain("actual-old-message");
    expect(f.counts().reads).toBe(0);
  }
});

test("hosted: a source row that does not name the requested id is out of contract (503), never served", async () => {
  const rows = [
    sourceRow("codex", { session_ids: ["other"], canonical_ids: [] }),
    sourceRow("codex", { session_ids: ["other"], canonical_ids: ["other-source-id"] }),
    sourceRow("codex", { session_ids: undefined as unknown as string[] }),
    sourceRow("codex", { canonical_ids: null as unknown as string[] }),
  ];
  for (const row of rows) {
    const f = fixture({ hosted: { workspaces: "*", sources: [row] } });
    const r = await f.get("");
    // The same refusal as a malformed harness name: an unavailable backend, no content.
    expect([row, r.status, JSON.parse(await r.text())]).toEqual([row, 503, { error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId: "corr-1" }]);
    expect([row, f.counts().reads]).toEqual([row, 0]);
  }
});

test("hosted: ignored by the self-host service, whose own branch answers", async () => {
  const f = fixture({ hosted: { workspaces: "*" } });
  f.deps.selfhost = { login: async () => { throw Error("not used"); } };
  // No postgres adapter: the self-host branch does not admit, and the hosted one never runs.
  expect(await snapshot(await f.get("&source=codex"))).toEqual({ ...json, status: 501, body: unsupportedBody("history_floor") });
  expect(f.counts()).toEqual({ reads: 0, sourceCalls: 0, indexQueries: 0 });
});
