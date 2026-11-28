/**
 * The hosted Lambda's partial-session canonical fallback on GET /v1/sessions/:sessionId/events
 * (ReadApiDeps.canonicalEventsPartial, READ_API_PARTIAL_CANONICAL_EVENTS). Without the dep, or off
 * its allowlist, every answer is byte-identical to today's (status, headers, body). With it, only a
 * partial session's first page (and the canonical cursor pages that follow) changes. Every first-page
 * refusal is that same 501 session_partial, classified in the log only. Synthetic data, no network:
 * ClickHouse is a stub, the object source a fake, pg.Pool a recorder.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { createReadApi, type ReadApiDeps } from "../app.ts";
import { CanonicalReadError } from "../canonical.ts";
import { CANONICAL_CURSOR_PREFIX } from "../canonical-events.ts";
import { encodeCursor } from "../cursor.ts";
import { CONTENT_TRUNCATION_NOTE } from "../page.ts";
import { handleUrlEvent, readApiDepsFromSecret, READER_LOGIN_SQL, type FunctionUrlEvent } from "../lambda.ts";
import type { PartialCanonicalDeps } from "../partial-canonical.ts";
import type { SessionSourceRow } from "../session-sources.ts";
import { selfhostSearchComposition } from "../server.ts";
import { ObjectStoreError } from "../../../packages/object-store/src/errors.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "./reader-login-support.ts";
import { memoryLiveness } from "./support.ts";

const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const floor = Date.parse("2026-11-08T22:02:00Z");
const OLD = floor - 86_400_000;
const NEW = floor + 3_600_000;
const GENERATION = "b".repeat(64);

const objectRow = (n: number, over: Record<string, unknown> = {}) => ({
  table: "convergence_events",
  row: { session_id: "sess", source: "codex", event_id: `canon-${n}`, machine_id: "machine", kind: "history", type: n === 1 ? "user" : "assistant", ts: `2026-11-09T10:00:0${n}.000Z`, content: `synthetic-canonical-${n}-long-content`, user_id: "u", ...over },
});
const sourceRow = (harness: string, over: Partial<SessionSourceRow> = {}): SessionSourceRow =>
  ({ org_id: "org-a", workspace_id: "ws-a", harness, session_ids: ["sess"], canonical_ids: [], ...over });

function stubCh(partial: string[]) {
  const asked: string[] = [];
  const cand = { ts_ms: NEW + 1000, session_id: "sess", source: "codex", machine_id: "m-1", kind: "message", event_id: "indexed-1" };
  const newest = [{ ...cand, org_id: "org-a", workspace_id: "ws-a", type: "assistant", user_id: "u-1", actor_name: "", actor_role: "", subagent_id: "", project_id: "", task_title: "", task_ref: "", tags: [], files_touched: [], tool_name: "", tool_status: "", content: "indexed content", in_budget: 1, origin: "canonical", origin_claim_token: "" }];
  const state = { partial };
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return state.partial.filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      asked.push(query);
      return (query.includes("bytes_before") ? newest : [cand]) as T[];
    },
  };
  return { ch, asked, state };
}

interface Options {
  created?: Record<string, number>;
  partial?: string[];
  erased?: Array<{ source: string; sessionId: string }>;
  /** Off: no fallback dep (the Lambda today). */
  pc?: false | { workspaces: ReadonlySet<string> | "*"; sources?: SessionSourceRow[]; deadlineMs?: number };
}

function fixture(o: Options = {}) {
  const ch = stubCh(o.partial ?? ["sess"]);
  const created = o.created ?? { codex: NEW };
  const mem = memoryLiveness({
    createdAt: Object.fromEntries(Object.entries(created).map(([h, t]) => [`${h}\u0000sess`, t])),
    erased: (o.erased ?? []).map((k) => ({ ...tenant, ...k })),
  });
  let reads = 0, sourceCalls = 0;
  const identities: unknown[] = [];
  const logs: Array<Record<string, unknown>> = [];
  const state = {
    rows: [objectRow(2), objectRow(1)] as ReturnType<typeof objectRow>[],
    generation: GENERATION,
    fail: undefined as unknown,
    hang: false,
    afterRead: undefined as undefined | (() => void),
  };
  const deps: ReadApiDeps = {
    verify: async () => ({ ...tenant, userId: "u", scopes: ["rth:read"] }),
    ch: ch.ch,
    liveness: mem.liveness,
    completeSinceMs: floor,
    log: (fields) => { logs.push(fields as Record<string, unknown>); },
  };
  if (o.pc) {
    const pc: PartialCanonicalDeps = {
      events: {
        source: async (identity, signal) => {
          reads++;
          identities.push(identity);
          if (state.hang) await new Promise((_, reject) => signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" }))));
          if (state.fail) throw state.fail;
          state.afterRead?.();
          return { rows: state.rows, legacyCoverage: null, exists: true, generation: state.generation };
        },
      },
      workspaces: o.pc.workspaces,
      sources: async () => { sourceCalls++; return o.pc ? o.pc.sources ?? [sourceRow("codex")] : []; },
      deadlineMs: o.pc.deadlineMs ?? 3000,
    };
    deps.canonicalEventsPartial = pc;
  }
  const get = (query: string) => createReadApi(deps).request(`/v1/sessions/sess/events?workspace=ws-a${query}`, { headers: { authorization: "Bearer synthetic", "x-correlation-id": "corr-1" } });
  return {
    deps, mem, ch, get, state, logs, identities,
    counts: () => ({ reads, sourceCalls, indexQueries: ch.asked.length }),
  };
}

const snapshot = async (r: Response) => ({ status: r.status, headers: [...r.headers.entries()].sort(), body: await r.text() });
const json = { status: 0, headers: [["cache-control", "private, no-store"], ["content-type", "application/json"], ["x-correlation-id", "corr-1"], ["x-relayhistory-workspace-id", "ws-a"], ["x-sessions-read-backend", "96bulls"]] };
const unsupportedBody = (reason: string) => `{"error":{"code":"unsupported_by_backend","message":"not served by this backend: ${reason}","reason":"${reason}"},"correlationId":"corr-1"}`;
const PARTIAL_501 = { ...json, status: 501, body: unsupportedBody("session_partial") };
const transitionBody = `{"error":{"code":"cursor_backend_transition","message":"the cursor was issued by another backend for this session; restart pagination without a cursor","reason":"cursor_backend_transition"},"correlationId":"corr-1"}`;
const objectCursor = (generation = GENERATION, source = "codex") => encodeURIComponent(CANONICAL_CURSOR_PREFIX + Buffer.from(JSON.stringify([generation, encodeCursor({ tsMs: Date.parse("2026-11-09T10:00:01.000Z"), sessionId: "sess", source, machineId: "machine", kind: "history", eventId: "canon-1" })])).toString("base64url"));
const searchCursor = encodeURIComponent(encodeCursor({ tsMs: NEW, sessionId: "sess", source: "codex", machineId: "m-1", kind: "message", eventId: "e-0" }));
const refused = (f: ReturnType<typeof fixture>) => f.logs.filter((l) => l.reason === "partial_canonical_refused").map((l) => l.reason_codes);

// Golden: the fallback absent, or present for another workspace, answers byte-identically.

const QUERIES = ["&source=codex", "", `&source=codex&cursor=${objectCursor()}`, `&source=codex&cursor=${searchCursor}`, "&source=codex&q=x", "&source=codex&includeMessageId=true"];

test("golden: without the dep and off the allowlist, every answer is unchanged", async () => {
  for (const partial of [["sess"], []]) for (const created of [{ codex: NEW }, { codex: OLD }]) for (const query of QUERIES) {
    const off = fixture({ pc: false, partial, created });
    const other = fixture({ pc: { workspaces: new Set(["ws-other"]) }, partial, created });
    const empty = fixture({ pc: { workspaces: new Set() }, partial, created });
    const want = await snapshot(await off.get(query));
    expect([query, partial, await snapshot(await other.get(query))]).toEqual([query, partial, want]);
    expect([query, partial, await snapshot(await empty.get(query))]).toEqual([query, partial, want]);
    expect(other.counts().reads + empty.counts().reads + other.counts().sourceCalls).toBe(0);
  }
  // The flag-off answers this file relies on.
  expect(await snapshot(await fixture({ pc: false }).get("&source=codex"))).toEqual(PARTIAL_501);
  expect(await snapshot(await fixture({ pc: false }).get(`&source=codex&cursor=${objectCursor()}`))).toEqual({ ...json, status: 501, body: unsupportedBody("canonical_cursor_scope") });
});

describe("served", () => {
  test("a partial session's first page and its cursor pages come from the canonical objects", async () => {
    const f = fixture({ pc: { workspaces: new Set(["ws-a"]) } });
    const first = await f.get("&source=codex&limit=1");
    expect(first.status).toBe(200);
    const a = await first.json() as Record<string, any>;
    // The public shape: exactly the route's keys.
    expect(Object.keys(a)).toEqual(["sessionId", "events", "nextCursor", "correlationId"]);
    expect(a.events.map((e: any) => [e.eventId, e.source, e.content])).toEqual([["canon-1", "codex", "synthetic-canonical-1-long-content"]]);
    expect(a.nextCursor.startsWith(CANONICAL_CURSOR_PREFIX)).toBe(true);
    const second = await (await f.get(`&source=codex&limit=1&cursor=${encodeURIComponent(a.nextCursor)}`)).json() as Record<string, any>;
    expect(second.events.map((e: any) => e.eventId)).toEqual(["canon-2"]);
    expect(second.nextCursor).toBeNull();
    // No ClickHouse event query ran; the object source was asked with the caller's tenant only.
    expect(f.counts()).toEqual({ reads: 2, sourceCalls: 0, indexQueries: 0 });
    expect(f.identities).toEqual([{ ...tenant, sessionId: "sess", source: "codex" }, { ...tenant, sessionId: "sess", source: "codex" }]);
    expect(f.logs.filter((l) => l.reason === "partial_canonical_served").map((l) => [l.kind, l.count])).toEqual([["first_page", 1], ["cursor_page", 1]]);
  });

  test("filters keep the route's meaning: order, limit, maxContent", async () => {
    const f = fixture({ pc: { workspaces: "*" } });
    const desc = await (await f.get("&source=codex&order=desc")).json() as Record<string, any>;
    expect(desc.events.map((e: any) => e.eventId)).toEqual(["canon-2", "canon-1"]);
    const cut = await (await f.get("&source=codex&maxContent=9")).json() as Record<string, any>;
    // relayhistory's maxContent shape (page.ts shapeRow), as on every other events page.
    expect(cut.events.map((e: any) => [e.content, e.contentTruncated])).toEqual([[`synthetic${CONTENT_TRUNCATION_NOTE}`, true], [`synthetic${CONTENT_TRUNCATION_NOTE}`, true]]);
    const full = await (await f.get("&source=codex")).json() as Record<string, any>;
    expect(full.events.map((e: any) => e.contentTruncated)).toEqual([false, false]);
    // Rejected params stay rejected, before any read.
    expect(await snapshot(await f.get("&source=codex&q=x"))).toEqual({ ...json, status: 501, body: unsupportedBody("param_q") });
    expect(await snapshot(await f.get("&source=codex&kind=x"))).toEqual({ ...json, status: 501, body: unsupportedBody("param_kind") });
    expect(f.counts().reads).toBe(3);
  });

  test("a session that is not partial is the ClickHouse page; no object is read", async () => {
    const f = fixture({ pc: { workspaces: "*" }, partial: [] });
    const r = await f.get("&source=codex");
    expect(r.status).toBe(200);
    expect(((await r.json()) as any).events.map((e: any) => e.eventId)).toEqual(["indexed-1"]);
    expect(f.counts().reads).toBe(0);
  });

  test("a search cursor while partial continues on ClickHouse as before (no partial check on continuation)", async () => {
    const on = fixture({ pc: { workspaces: "*" } });
    const off = fixture({ pc: false });
    expect(await snapshot(await on.get(`&source=codex&cursor=${searchCursor}`))).toEqual(await snapshot(await off.get(`&source=codex&cursor=${searchCursor}`)));
    expect(on.counts().reads).toBe(0);
  });

  test("the history floor still wins: an old partial session is the 501 history_floor, no read", async () => {
    const f = fixture({ pc: { workspaces: "*" }, created: { codex: OLD } });
    expect(await snapshot(await f.get("&source=codex"))).toEqual({ ...json, status: 501, body: unsupportedBody("history_floor") });
    expect(f.counts().reads).toBe(0);
  });

  test("ignored beside the self-host and hosted Worker canonical paths, and with includeMessageId", async () => {
    const hosted = fixture({ pc: { workspaces: "*" } });
    hosted.deps.canonicalEvents = { source: async () => { throw Error("not read"); } };
    hosted.deps.canonicalEventsHosted = { workspaces: "*", sources: async () => [] };
    expect(await snapshot(await hosted.get("&source=codex"))).toEqual(PARTIAL_501);
    expect(hosted.counts().reads).toBe(0);
    const ids = fixture({ pc: { workspaces: "*" } });
    ids.deps.eventMessageIds = { source: async () => ({ rows: [], legacyCoverage: null }) };
    expect(await snapshot(await ids.get("&source=codex&includeMessageId=true"))).toEqual(PARTIAL_501);
    expect(ids.counts().reads).toBe(0);
  });
});

describe("erasure", () => {
  test("a tombstoned session is the empty page with no object read", async () => {
    const f = fixture({ pc: { workspaces: "*" }, created: {}, erased: [{ source: "codex", sessionId: "sess" }] });
    const r = await f.get("&source=codex");
    expect(await r.json()).toEqual({ sessionId: "sess", events: [], nextCursor: null, correlationId: "corr-1" });
    expect(f.counts().reads).toBe(0);
  });

  test("an erase that commits during the object read wins: empty page, no content", async () => {
    for (const query of ["&source=codex", `&source=codex&cursor=${objectCursor()}`]) {
      const f = fixture({ pc: { workspaces: "*" } });
      f.state.afterRead = () => f.mem.erase({ ...tenant, source: "codex", sessionId: "sess" });
      const r = await f.get(query);
      const body = await r.text();
      expect([query, r.status, body.includes("synthetic-canonical")]).toEqual([query, 200, false]);
      expect(JSON.parse(body).events).toEqual([]);
    }
  });

  test("an operator fence on the resolved source after the read is a 404 with no content", async () => {
    const f = fixture({ pc: { workspaces: "*", sources: [sourceRow("codex", { canonical_ids: ["sess-alias"] })] } });
    const fenced = new Set<string>();
    f.deps.operatorErased = async (key) => fenced.has(`${key.source}/${key.sessionId}`);
    f.state.afterRead = () => fenced.add("codex/sess-alias");
    const r = await f.get("");
    expect([r.status, (await r.text()).includes("synthetic-canonical")]).toEqual([404, false]);
  });
});

describe("source resolution", () => {
  test("without source: the one live source is read", async () => {
    const f = fixture({ pc: { workspaces: "*" } });
    expect((await f.get("")).status).toBe(200);
    expect(f.counts()).toMatchObject({ reads: 1, sourceCalls: 1 });
    expect(f.identities).toEqual([{ ...tenant, sessionId: "sess", source: "codex" }]);
  });

  test("a resolved source tombstoned before its own pointer check is the empty page, no read", async () => {
    // Another harness keeps the session id live (sessionGate passes); the resolved one was erased in between.
    const f = fixture({ pc: { workspaces: "*", sources: [sourceRow("codex")] }, created: { claude: NEW }, erased: [{ source: "codex", sessionId: "sess" }] });
    const r = await f.get("");
    expect(await r.json()).toEqual({ sessionId: "sess", events: [], nextCursor: null, correlationId: "corr-1" });
    expect(f.counts().reads).toBe(0);
  });

  test("ambiguous or unresolved: the exact 501 session_partial, classified in the log, no read", async () => {
    const two = fixture({ pc: { workspaces: "*", sources: [sourceRow("claude"), sourceRow("codex")] }, created: { codex: NEW, claude: NEW } });
    expect(await snapshot(await two.get(""))).toEqual(PARTIAL_501);
    expect(refused(two)).toEqual(["canonical_invalid,source_ambiguous"]);
    const none = fixture({ pc: { workspaces: "*", sources: [] } });
    expect(await snapshot(await none.get(""))).toEqual(PARTIAL_501);
    expect(refused(none)).toEqual(["canonical_missing,source_unresolved"]);
    expect(two.counts().reads + none.counts().reads).toBe(0);
  });

  test("an explicit source with no live pointer: the route's own floor answer, or (epoch floor) missing; no read", async () => {
    const floored = fixture({ pc: { workspaces: "*" }, created: { claude: NEW } });
    expect(await snapshot(await floored.get("&source=codex"))).toEqual(await snapshot(await fixture({ pc: false, created: { claude: NEW } }).get("&source=codex")));
    expect(floored.counts().reads).toBe(0);
    const f = fixture({ pc: { workspaces: "*" }, created: { claude: NEW } });
    f.deps.completeSinceMs = 0;
    expect(await snapshot(await f.get("&source=codex"))).toEqual(PARTIAL_501);
    expect(refused(f)).toEqual(["canonical_missing,pointer_missing"]);
    expect(f.counts().reads).toBe(0);
  });

  test("a cursor issued for another source is a 400, never read across sources", async () => {
    const f = fixture({ pc: { workspaces: "*" }, created: { codex: NEW, claude: NEW } });
    const r = await f.get(`&source=claude&cursor=${objectCursor(GENERATION, "codex")}`);
    expect(r.status).toBe(400);
  });
});

describe("refusals: first page is the unchanged 501; cursor pages say retry or restart", () => {
  const CASES: Array<[string, unknown, string, number, string]> = [
    ["tenant mismatch", new CanonicalReadError("tenant_mismatch"), "canonical_forbidden,tenant_mismatch", 501, PARTIAL_501.body],
    ["bucket/hash/revision pointer", new CanonicalReadError("pointer_invalid"), "canonical_forbidden,pointer_invalid", 501, PARTIAL_501.body],
    ["key prefix", new CanonicalReadError("prefix_mismatch"), "canonical_forbidden,prefix_mismatch", 501, PARTIAL_501.body],
    ["store 403", new ObjectStoreError("unauthorized", "s3", "synthetic", { status: 403 }), "canonical_forbidden,store_unauthorized", 501, PARTIAL_501.body],
    ["scope too large", new CanonicalReadError("canonical_projection_incomplete", 503, ["scope_too_large"]), "canonical_oversize,canonical_projection_incomplete,scope_too_large", 501, PARTIAL_501.body],
    ["pointer bound", new CanonicalReadError("pointer_bound"), "canonical_oversize,pointer_bound", 501, PARTIAL_501.body],
    ["object missing", new CanonicalReadError("object_missing"), "canonical_missing,object_missing", 409, transitionBody],
    ["store 503", new ObjectStoreError("transient", "s3", "synthetic", { status: 503 }), "canonical_transient,store_transient", 503, `{"error":{"code":"backend_unavailable","message":"read backend unavailable","reason":"events_canonical_transient"},"correlationId":"corr-1"}`],
    ["network", new TypeError("fetch failed"), "canonical_transient", 503, `{"error":{"code":"backend_unavailable","message":"read backend unavailable","reason":"events_canonical_transient"},"correlationId":"corr-1"}`],
    ["projection", new CanonicalReadError("canonical_projection_incomplete", 503, ["fold_failed"]), "canonical_invalid,canonical_projection_incomplete,fold_failed", 501, PARTIAL_501.body],
  ];
  test.each(CASES)("%s", async (_name, error, codes, cursorStatus, cursorBody) => {
    const first = fixture({ pc: { workspaces: "*" } });
    first.state.fail = error;
    expect(await snapshot(await first.get("&source=codex"))).toEqual(PARTIAL_501);
    expect(refused(first)).toEqual([codes]);
    const page = fixture({ pc: { workspaces: "*" } });
    page.state.fail = error;
    const r = await page.get(`&source=codex&cursor=${objectCursor()}`);
    expect([r.status, await r.text()]).toEqual([cursorStatus, cursorBody]);
  });

  test("a generation that moved under a cursor is a 409 restart; on a first page it cannot occur", async () => {
    const f = fixture({ pc: { workspaces: "*" } });
    f.state.generation = "c".repeat(64);
    expect(await snapshot(await f.get(`&source=codex&cursor=${objectCursor()}`))).toEqual({ ...json, status: 409, body: transitionBody });
    expect(refused(f)).toEqual(["canonical_missing,source_generation_changed"]);
    expect((await f.get("&source=codex")).status).toBe(200);
  });

  test("a malformed canonical cursor is the 400 it is on every canonical path", async () => {
    const f = fixture({ pc: { workspaces: "*" } });
    const r = await f.get(`&source=codex&cursor=${encodeURIComponent(CANONICAL_CURSOR_PREFIX + "not-base64-json")}`);
    expect(r.status).toBe(400);
    expect(f.counts().reads).toBe(0);
  });
});

describe("deadline", () => {
  test("a budget already spent refuses before any object read (transient)", async () => {
    const f = fixture({ pc: { workspaces: "*", deadlineMs: 100 } });
    expect(await snapshot(await f.get("&source=codex"))).toEqual(PARTIAL_501);
    expect(refused(f)).toEqual(["canonical_transient,deadline"]);
    expect(f.counts().reads).toBe(0);
  });

  test("an object read past the remaining budget is aborted and refused within the budget", async () => {
    const f = fixture({ pc: { workspaces: "*", deadlineMs: 600 } });
    f.state.hang = true;
    const started = Date.now();
    expect(await snapshot(await f.get("&source=codex"))).toEqual(PARTIAL_501);
    expect(Date.now() - started).toBeLessThan(1500);
    expect(refused(f)).toEqual(["canonical_transient,deadline"]);
    const page = fixture({ pc: { workspaces: "*", deadlineMs: 600 } });
    page.state.hang = true;
    expect((await page.get(`&source=codex&cursor=${objectCursor()}`)).status).toBe(503);
  });
});

/**
 * Through the public Function URL handler (lambda.ts handleUrlEvent: URL event -> Request -> app),
 * the Lambda's production request path past init. Pinned cases for the lander.
 */
describe("public handler (handleUrlEvent)", () => {
  const event = (query: string, auth = "Bearer synthetic"): FunctionUrlEvent => ({
    rawPath: "/v1/sessions/sess/events", rawQueryString: `workspace=ws-a${query}`,
    headers: { authorization: auth, "x-correlation-id": "corr-1" }, requestContext: { http: { method: "GET" } },
  });
  const call = async (f: ReturnType<typeof fixture>, query: string, auth?: string) => {
    const r = await handleUrlEvent(createReadApi(f.deps), event(query, auth));
    return { status: r.statusCode, body: JSON.parse(r.body) as Record<string, any> };
  };
  const noContent = (body: unknown) => !JSON.stringify(body).includes("synthetic-canonical");

  test("PH1 eligible direct session: 200 with the expected canonical events", async () => {
    const f = fixture({ pc: { workspaces: new Set(["ws-a"]) } });
    const r = await call(f, "&source=codex");
    expect(r.status).toBe(200);
    expect(Object.keys(r.body)).toEqual(["sessionId", "events", "nextCursor", "correlationId"]);
    expect(r.body.events.map((e: any) => [e.eventId, e.source, e.kind, e.content])).toEqual([
      ["canon-1", "codex", "history", "synthetic-canonical-1-long-content"],
      ["canon-2", "codex", "history", "synthetic-canonical-2-long-content"],
    ]);
    expect(r.body.nextCursor).toBeNull();
    expect(f.counts()).toEqual({ reads: 1, sourceCalls: 0, indexQueries: 0 });
  });

  test("PH2 unauthenticated: 401, no read, no events", async () => {
    const f = fixture({ pc: { workspaces: "*" } });
    f.deps.verify = async () => { throw new Error("synthetic bad token"); };
    const r = await call(f, "&source=codex", "Bearer bad");
    expect([r.status, "events" in r.body, f.counts().reads]).toEqual([401, false, 0]);
  });

  test("PH3 forbidden object (tenant, bucket, prefix, store 403): the contracted 501 session_partial, no events", async () => {
    for (const error of [new CanonicalReadError("tenant_mismatch"), new CanonicalReadError("pointer_invalid"), new CanonicalReadError("prefix_mismatch"), new ObjectStoreError("unauthorized", "s3", "synthetic", { status: 403 })]) {
      const f = fixture({ pc: { workspaces: "*" } });
      f.state.fail = error;
      const r = await call(f, "&source=codex");
      expect([r.status, r.body.error, "events" in r.body]).toEqual([501, { code: "unsupported_by_backend", message: "not served by this backend: session_partial", reason: "session_partial" }, false]);
    }
  });

  test("PH4 erased: the route's existing erased contract (200, empty events), never content", async () => {
    // Before the read (tombstone): no object IO.
    const before = fixture({ pc: { workspaces: "*" }, created: {}, erased: [{ source: "codex", sessionId: "sess" }] });
    const a = await call(before, "&source=codex");
    expect([a.status, a.body.events, a.body.nextCursor, before.counts().reads]).toEqual([200, [], null, 0]);
    // During the read: the post-IO erasure recheck drops every row.
    const during = fixture({ pc: { workspaces: "*" } });
    during.state.afterRead = () => during.mem.erase({ ...tenant, source: "codex", sessionId: "sess" });
    const b = await call(during, "&source=codex");
    expect([b.status, b.body.events, b.body.nextCursor, noContent(b.body)]).toEqual([200, [], null, true]);
    // Flag off gives the same erased answer.
    const off = fixture({ pc: false, created: {}, erased: [{ source: "codex", sessionId: "sess" }] });
    expect((await call(off, "&source=codex")).body).toEqual(a.body);
  });

  test("PH5 oversized (scope or pointer bound): the contracted 501 session_partial, no events", async () => {
    for (const error of [new CanonicalReadError("canonical_projection_incomplete", 503, ["scope_too_large"]), new CanonicalReadError("pointer_bound"), new ObjectStoreError("invalid_argument", "manifest", "manifest is larger than the reader accepts")]) {
      const f = fixture({ pc: { workspaces: "*" } });
      f.state.fail = error;
      const r = await call(f, "&source=codex");
      expect([r.status, r.body.error?.reason, "events" in r.body]).toEqual([501, "session_partial", false]);
      expect(refused(f)[0]).toMatch(/^canonical_oversize,/);
    }
  });
});

describe("startup wiring (lambda.ts)", () => {
  const RealPool = pg.Pool;
  const answer = async (text: string) => ({ rows: text === READER_LOGIN_SQL ? [{ ...READER_LOGIN_ROW }] : readerLoginAnswer(text) ?? [] });
  let pools = 0;
  class RecordingPool {
    constructor() { pools++; }
    on() { return this; }
    async query(text: string) { return answer(text); }
    async connect() { return { query: async (text: string) => answer(text), release() {} }; }
    async end() {}
  }
  beforeEach(() => { pools = 0; pg.Pool = RecordingPool as unknown as typeof pg.Pool; });
  afterEach(() => { pg.Pool = RealPool; });
  const secret = JSON.stringify({ supabase_url: "postgresql://reader:pw@db.example.internal:5432/history", clickhouse: { url: "https://ch.example.internal:8443", user: "u", password: "p", database: "relay_search" } });
  const ON = {
    READ_API_COMPLETE_SINCE: "epoch", READ_API_PARTIAL_CANONICAL_EVENTS: "true", READ_API_PARTIAL_CANONICAL_BUCKET: "synthetic-canonical",
    READ_API_PARTIAL_CANONICAL_WORKSPACES: "ws-a", AWS_REGION: "us-east-1", PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls",
  } as NodeJS.ProcessEnv;

  test("the Lambda spreads the dep only when on; off (the default) builds exactly without it", async () => {
    const on = await readApiDepsFromSecret(secret, ON);
    expect(on.deps.canonicalEventsPartial?.workspaces).toEqual(new Set(["ws-a"]));
    expect(on.deps.canonicalEventsPartial?.deadlineMs).toBe(3000);
    await on.close();
    for (const value of [undefined, "", "false"]) {
      const off = await readApiDepsFromSecret(secret, { ...ON, READ_API_PARTIAL_CANONICAL_EVENTS: value });
      expect("canonicalEventsPartial" in off.deps).toBe(false);
      await off.close();
    }
  });

  test("a bad selection refuses before any pool is built, never echoing a value", async () => {
    for (const over of [{ READ_API_PARTIAL_CANONICAL_EVENTS: "s3cret-value" }, { READ_API_PARTIAL_CANONICAL_BUCKET: "" }, { PIPELINE_ORG_FROM_PREFIX: "false" }]) {
      const error = await readApiDepsFromSecret(secret, { ...ON, ...over }).then(() => null, (e: Error) => e);
      expect(error?.message).toContain("READ_API_PARTIAL_CANONICAL_EVENTS");
      expect(error?.message).not.toContain("s3cret-value");
    }
    expect(pools).toBe(0);
  });

  test("the self-host service refuses it", async () => {
    const env = { ...ON, RELAYHISTORY_SELFHOST_READ_API: "true", SESSION_POSTGRES_TLS_PROFILE: "supabase", SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg" } as NodeJS.ProcessEnv;
    const error = await readApiDepsFromSecret(JSON.stringify({ supabase_url: "postgresql://reader:pw@db.selfhost.internal:6543/history" }), env, selfhostSearchComposition(env), { entry: "service" }).then(() => null, (e: Error) => e);
    expect(error?.message).toBe("session read api: READ_API_PARTIAL_CANONICAL_EVENTS=true is served only by the hosted Lambda");
    expect(pools).toBe(0);
  });
});
