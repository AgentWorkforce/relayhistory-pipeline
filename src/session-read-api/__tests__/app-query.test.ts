/**
 * The session list/activity `q` (READ_API_SESSION_QUERY, list-query.ts) at the routes, over a
 * recording Directory fake: flag off keeps both routes' 501 `param_q`; flag on parses q into
 * terms for directory.query / queryActivity, answers what it cannot serve with a named 501 and
 * malformed input with a 400, and pages with a `b96q1:` cursor bound to the query.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type ReadApiDeps, type VerifiedIdentity } from "../app.ts";
import {
  DIRECTORY_QUERY_ACTIVITY_SQL,
  DIRECTORY_QUERY_SQL,
  decodeQueryCursor,
  encodeListCursor,
  encodeQueryCursor,
  type Directory,
  type DirectoryListRow,
  type DirectoryQueryActivityArgs,
  type DirectoryQueryArgs,
  sqlDirectory,
} from "../directory.ts";
import type { TenantRunner } from "../liveness.ts";
import { parseSessionQuery, queryFingerprint, type SessionQueryTerms } from "../list-query.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "user-q1", orgId: "org-q", workspaceId: "ws-q", scopes: ["rth:read"] };

function row(over: Partial<DirectoryListRow> & { cursor_ts?: string } = {}): DirectoryListRow & { cursor_ts?: string } {
  return {
    org_id: "org-q", workspace_id: "ws-q", harness: "codex", session_id: "sess-q1",
    created_at: new Date("2026-10-01T00:00:00Z"), last_activity_at: new Date("2026-10-02T03:04:05.123Z"),
    cursor_ts: "2026-10-02T03:04:05.123456Z",
    title: "Deploy pipeline", task_title: null, agent_names: ["lead"], project_id: "example.test/proj",
    project_key: null, project_key_method: null, git_repo: null, git_branch: null, initial_commit: null,
    originator: null, agent_version: null, models: [], state: "active", event_count: 1,
    input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: [], user_ids: ["user-q1"],
    parent_session_ids: [], child_session_count: 0, short_summary: null, short_summary_generated_at: null,
    catalog_projected_at: null, projected_at: null,
    ...over,
  };
}

type Call =
  | { fn: "list" | "activity" }
  | { fn: "query"; args: DirectoryQueryArgs }
  | { fn: "queryActivity"; since: Date; until: Date; args: DirectoryQueryActivityArgs };

function fake(init: { rows?: Array<DirectoryListRow & { cursor_ts?: string }> | null; activity?: Array<{ day: string; harness: string; sessions: number }> | null } = {}) {
  const calls: Call[] = [];
  const directory: Directory = {
    async list() {
      calls.push({ fn: "list" });
      return [row()];
    },
    async activity() {
      calls.push({ fn: "activity" });
      return [];
    },
    async query(_o, _w, args) {
      calls.push({ fn: "query", args });
      return init.rows === undefined ? [row()] : init.rows;
    },
    async queryActivity(_o, _w, since, until, args) {
      calls.push({ fn: "queryActivity", since, until, args });
      return init.activity === undefined ? [] : init.activity;
    },
  };
  return { directory, calls };
}

function api(over: { sessionQuery?: boolean; directory?: Directory; ownerBuffered?: ReadApiDeps["ownerBuffered"] } = {}) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch: { database: "relay_search", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse here"); } } as never,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: () => {},
    directory: over.directory ?? fake().directory,
    directoryWorkspaces: new Set(["ws-q"]),
    ...(over.ownerBuffered ? { ownerBuffered: over.ownerBuffered } : {}),
    ...(over.sessionQuery !== undefined ? { sessionQuery: over.sessionQuery } : {}),
  });
  return (path: string) => app.request(path, { headers: { authorization: "Bearer good" } });
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;
const list = (q: string, extra = "") => `/v1/sessions?workspace=ws-q&q=${encodeURIComponent(q)}${extra}`;
const activity = (q: string, extra = "") =>
  `/v1/sessions/activity?workspace=ws-q&since=2026-10-01T00:00:00Z&until=2026-10-03T00:00:00Z&q=${encodeURIComponent(q)}${extra}`;
const termsOf = (q: string): SessionQueryTerms => {
  const p = parseSessionQuery(q);
  if (p.kind !== "terms") throw new Error("expected terms");
  return p.terms;
};

describe("flag off: unchanged", () => {
  for (const flag of [undefined, false]) {
    test(`sessionQuery=${flag}: q is 501 param_q on both routes and nothing is read`, async () => {
      const { directory, calls } = fake();
      const get = api({ directory, ...(flag === undefined ? {} : { sessionQuery: flag }) });
      const l = await get(list("deploy"));
      expect(l.status).toBe(501);
      expect((await json(l)).error).toMatchObject({ code: "unsupported_by_backend", reason: "param_q" });
      const a = await get(activity("deploy"));
      expect(a.status).toBe(501);
      expect((await json(a)).error.reason).toBe("param_q");
      // search stays 501 on the list; an empty activity q is still no q.
      expect((await json(await get("/v1/sessions?workspace=ws-q&search=metadata"))).error.reason).toBe("param_search");
      expect((await get(activity(""))).status).toBe(200);
      // A b96q1 cursor is simply foreign to the flag-off list.
      const foreign = await get(`/v1/sessions?workspace=ws-q&cursor=${encodeQueryCursor({ lastActivityAt: "2026-10-02T00:00:00Z", sessionId: "s", harness: "codex", fingerprint: "0".repeat(32) })}`);
      expect((await json(foreign)).error.reason).toBe("foreign_cursor");
      expect(calls.map((c) => c.fn)).toEqual(["activity"]);
    });
  }
});

describe("flag on: GET /v1/sessions?q=", () => {
  test("happy path: terms reach directory.query with the list filters; the page has a b96q1 cursor", async () => {
    const { directory, calls } = fake({ rows: [row(), row({ session_id: "sess-q2", cursor_ts: "2026-10-01T00:00:00.000001Z" })] });
    const get = api({ sessionQuery: true, directory });
    const q = 'Deploy -flaky title:"pipe line" project:proj harness:codex source:claude @lead id:sess -id:old is:active updated:>=2026-10-01';
    const r = await get(list(q, "&limit=1&user=user-q1&lastTsBefore=2026-10-20T00:00:00Z&search=metadata"));
    expect(r.status).toBe(200);
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "nextCursor", "sessions"]);
    expect(b.sessions.map((s: { sessionId: string }) => s.sessionId)).toEqual(["sess-q1"]);
    expect(calls).toHaveLength(1);
    const call = calls[0] as Extract<Call, { fn: "query" }>;
    expect(call.fn).toBe("query");
    expect(call.args.terms).toMatchObject({
      free: ["deploy"], notFree: ["flaky"], title: ["pipe line"], project: ["proj"], harness: ["codex", "claude"],
      agent: ["lead"], id: ["sess"], notId: ["old"], active: true,
    });
    expect(call.args).toMatchObject({ limit: 1, userIds: ["user-q1"] });
    expect(call.args.lastTsAfter?.toISOString()).toBe("2026-10-01T00:00:00.000Z");
    expect(call.args.lastTsBefore?.toISOString()).toBe("2026-10-20T00:00:00.000Z");
    expect(call.args.cursor).toBeUndefined();
    const cursor = decodeQueryCursor(b.nextCursor);
    expect(cursor).toMatchObject({ lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "sess-q1", harness: "codex" });
    // The next page with the same q and filters continues; another limit does not matter.
    const next = await get(list(q, `&limit=2&user=user-q1&lastTsBefore=2026-10-20T00:00:00Z&cursor=${encodeURIComponent(b.nextCursor)}`));
    expect(next.status).toBe(200);
    expect((calls[1] as Extract<Call, { fn: "query" }>).args.cursor).toEqual({ lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "sess-q1", harness: "codex" });
  });

  test("q dates intersect lastTsAfter/lastTsBefore (the tighter bound wins)", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    expect((await get(list("updated:2026-10-05..2026-10-10", "&lastTsAfter=2026-10-07T00:00:00Z&lastTsBefore=2026-10-30T00:00:00Z"))).status).toBe(200);
    const args = (calls[0] as Extract<Call, { fn: "query" }>).args;
    expect(args.lastTsAfter?.toISOString()).toBe("2026-10-07T00:00:00.000Z");
    expect(args.lastTsBefore?.toISOString()).toBe("2026-10-11T00:00:00.000Z");
    const empty = await get(list("before:2026-10-05", "&lastTsAfter=2026-10-06T00:00:00Z"));
    expect(empty.status).toBe(400);
    expect((await json(empty)).error.message).toBe("the q date terms do not overlap the requested time window");
  });

  test("every 501 reason", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    for (const [q, reason] of [
      ["path:/srv/app", "q_filter_path"],
      ["-path:/srv/app", "q_filter_path"],
      ["upload:uploaded", "q_filter_upload"],
      ["is:on-relay", "q_filter_is_on-relay"],
      ["-is:off-relay", "q_filter_is_off-relay"],
      ["-updated:2026-10-01", "q_filter_date_exclusion"],
    ] as const) {
      const r = await get(list(q));
      expect(r.status).toBe(501);
      expect((await json(r)).error).toMatchObject({ code: "unsupported_by_backend", reason });
    }
    // search other than metadata, or search without q.
    expect((await json(await get(list("deploy", "&search=semantic")))).error.reason).toBe("param_search");
    expect((await json(await get("/v1/sessions?workspace=ws-q&search=metadata"))).error.reason).toBe("param_search");
    // The other list parameters the directory never serves are still 501.
    expect((await json(await get(list("deploy", "&kind=message")))).error.reason).toBe("param_kind");
    // A cursor from another backend.
    expect((await json(await get(list("deploy", "&cursor=sessions-v2:abc")))).error.reason).toBe("foreign_cursor");
    expect(calls).toEqual([]);
  });

  test("not migrated: 501 directory_not_migrated", async () => {
    const { directory } = fake({ rows: null, activity: null });
    const get = api({ sessionQuery: true, directory });
    expect((await json(await get(list("deploy")))).error.reason).toBe("directory_not_migrated");
    expect((await json(await get(activity("deploy")))).error.reason).toBe("directory_not_migrated");
  });

  test("buffered reads on: q is 501 q_with_buffered (never a silent drop)", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory, ownerBuffered: { enabled: true, workspaces: "*" } as never });
    const r = await get(list("deploy"));
    expect(r.status).toBe(501);
    expect((await json(r)).error.reason).toBe("q_with_buffered");
    expect(calls).toEqual([]);
  });

  test("every 400", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    const cases: Array<[string, string]> = [
      ["   ", "q has no search terms"],
      ["ab", "a search needs at least one word of 3 or more characters that is not excluded"],
      ["UI", "a search needs at least one word of 3 or more characters that is not excluded"],
      ['"a b"', "a search needs at least one word of 3 or more characters that is not excluded"],
      ["-", "a search needs at least one word of 3 or more characters that is not excluded"],
      ["-ab", "a search needs at least one word of 3 or more characters that is not excluded"],
      ["ab -foo", "a search needs at least one word of 3 or more characters that is not excluded"],
      [`title:${"t".repeat(201)}`, "a q term must be 200 characters or fewer"],
      [Array.from({ length: 17 }, (_, i) => `term${i}`).join(" "), "q accepts at most 16 terms"],
      ["is:active is:inactive", "choose either is:active or is:inactive"],
      ["is:on-relay is:off-relay", "choose either is:on-relay or is:off-relay"],
      ["is:sleepy", "use is:active, is:inactive, is:on-relay or is:off-relay"],
      ["upload:sideways", "use upload:uploaded, upload:local, upload:in-progress or upload:discovering"],
      ["title:", "add a value after title:"],
      ["updated:2026-13-01", "use a date like updated:2026-10-01, updated:>=2026-10-01 or updated:2026-10-01..2026-10-11 (UTC)"],
      ["after:2026-10-05 before:2026-10-02", "the q date terms do not overlap"],
      ["harness:Bad/Name", "harness: must name a harness"],
      ["@", "add an agent name after @"],
      ["x".repeat(501), "q must be 500 characters or fewer"],
    ];
    for (const [q, message] of cases) {
      const r = await get(list(q));
      expect(r.status).toBe(400);
      expect((await json(r)).error).toEqual({ code: "bad_request", message });
    }
    // A repeated q.
    expect((await get("/v1/sessions?workspace=ws-q&q=alpha&q=beta")).status).toBe(400);
    expect(calls).toEqual([]);
  });

  test("an empty q is no q: the plain list is served", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    expect((await get("/v1/sessions?workspace=ws-q&q=")).status).toBe(200);
    expect(calls.map((c) => c.fn)).toEqual(["list"]);
  });

  test("cursors: a fingerprint mismatch, a b96s1 cursor with q, a b96q1 cursor without q are invalid", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    const position = { lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "sess-q1", harness: "codex" };
    const fp = queryFingerprint(termsOf("deploy"), {});
    const ok = encodeQueryCursor({ ...position, fingerprint: fp });
    expect((await get(list("deploy", `&cursor=${ok}`))).status).toBe(200);
    for (const [path, why] of [
      [list("rollback", `&cursor=${ok}`), "another q"],
      [list("deploy", `&cursor=${ok}&source=codex`), "another filter"],
      [list("deploy", `&cursor=${encodeListCursor(position)}`), "a b96s1 cursor"],
      [list("deploy", "&cursor=b96q1:not-json"), "a malformed b96q1 cursor"],
      [list("deploy", `&cursor=${encodeQueryCursor({ ...position, fingerprint: "X".repeat(32) })}`), "a malformed fingerprint"],
      [`/v1/sessions?workspace=ws-q&cursor=${ok}`, "a b96q1 cursor without q"],
    ] as const) {
      const r = await get(path);
      expect([why, r.status]).toEqual([why, 400]);
      expect((await json(r)).error.code).toBe("bad_request");
    }
    expect(calls).toHaveLength(1);
  });

  test("without q, the flag-on list is the plain list (b96s1 cursors still work)", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    const r = await get(`/v1/sessions?workspace=ws-q&cursor=${encodeListCursor({ lastActivityAt: "2026-10-02T03:04:05.123456Z", sessionId: "sess-q1", harness: "codex" })}`);
    expect(r.status).toBe(200);
    expect(calls.map((c) => c.fn)).toEqual(["list"]);
  });
});

describe("flag on: GET /v1/sessions/activity?q=", () => {
  test("q uses queryActivity with the parsed terms and its dates as last-activity bounds", async () => {
    const { directory, calls } = fake({ activity: [{ day: "2026-10-02", harness: "codex", sessions: 3 }] });
    const get = api({ sessionQuery: true, directory });
    const r = await get(activity("deploy -@bot after:2026-09-30", "&search=metadata"));
    expect(r.status).toBe(200);
    const b = await json(r);
    expect(b.days).toEqual([
      { date: "2026-10-01", total: 0, bySource: {} },
      { date: "2026-10-02", total: 3, bySource: { codex: 3 } },
      { date: "2026-10-03", total: 0, bySource: {} },
    ]);
    const call = calls[0] as Extract<Call, { fn: "queryActivity" }>;
    expect(call.fn).toBe("queryActivity");
    expect(call.since.toISOString()).toBe("2026-10-01T00:00:00.000Z");
    expect(call.until.toISOString()).toBe("2026-10-03T00:00:00.000Z");
    expect(call.args.terms).toMatchObject({ free: ["deploy"], notAgent: ["bot"] });
    expect(call.args.lastTsAfter?.toISOString()).toBe("2026-10-01T00:00:00.000Z");
    expect(call.args.lastTsBefore).toBeUndefined();
  });

  test("501s, 400s and search on the activity route", async () => {
    const { directory, calls } = fake();
    const get = api({ sessionQuery: true, directory });
    expect((await json(await get(activity("path:/x")))).error.reason).toBe("q_filter_path");
    expect((await json(await get(activity("deploy", "&search=semantic")))).error.reason).toBe("param_search");
    expect((await get(activity("ab"))).status).toBe(400);
    expect((await get(activity("is:active is:inactive"))).status).toBe(400);
    // Without q the activity route keeps ignoring search, as before; an empty q is no q.
    expect((await get("/v1/sessions/activity?workspace=ws-q&since=2026-10-01T00:00:00Z&until=2026-10-03T00:00:00Z&search=whatever")).status).toBe(200);
    expect((await get(activity(""))).status).toBe(200);
    expect(calls.map((c) => c.fn)).toEqual(["activity", "activity"]);
  });
});

describe("flag on: short free words beside a 3+ anchor (#475 P1)", () => {
  // The real sqlDirectory over a recording tenant runner: what the SQL functions would receive.
  function recordingSql() {
    const statements: Array<{ text: string; params: unknown[] }> = [];
    const run: TenantRunner = async (_org, _ws, work) =>
      work({
        async query(text: string, params?: unknown[]) {
          statements.push({ text, params: params ?? [] });
          if (text.includes("to_regprocedure")) return { rows: [{ installed: true }] };
          if (text === DIRECTORY_QUERY_SQL) return { rows: [row()] };
          return { rows: [] };
        },
      } as never);
    return { directory: sqlDirectory(run), statements };
  }
  const TITLE = "Kind live E2E QA A 20261109-clean-native-50172a";

  for (const [q, free, notFree] of [
    [TITLE, ["kind", "live", "e2e", "qa", "a", "20261109-clean-native-50172a"], null],
    ["Fix a bug", ["fix", "a", "bug"], null],
    ["Fix UI bug", ["fix", "ui", "bug"], null],
    ["Fix bug -a", ["fix", "bug"], ["a"]],
  ] as const) {
    test(`list and activity serve ${JSON.stringify(q)} with the short words in the SQL term arrays`, async () => {
      const { directory, statements } = recordingSql();
      const get = api({ sessionQuery: true, directory });
      const l = await get(list(q));
      expect(l.status).toBe(200);
      expect((await json(l)).sessions).toHaveLength(1);
      const a = await get(activity(q));
      expect(a.status).toBe(200);
      const listCall = statements.find((s) => s.text === DIRECTORY_QUERY_SQL)!;
      const activityCall = statements.find((s) => s.text === DIRECTORY_QUERY_ACTIVITY_SQL)!;
      for (const call of [listCall, activityCall]) {
        // $2 p_free, $3 p_not_free.
        expect(call.params[1]).toEqual([...free]);
        expect(call.params[2]).toEqual(notFree === null ? null : [...notFree]);
      }
    });
  }

  test("a short-only q never reaches SQL on either route", async () => {
    const { directory, statements } = recordingSql();
    const get = api({ sessionQuery: true, directory });
    for (const q of ["QA A", "ab -foo"]) {
      for (const path of [list(q), activity(q)]) {
        const r = await get(path);
        expect(r.status).toBe(400);
        expect((await json(r)).error).toEqual({ code: "bad_request", message: "a search needs at least one word of 3 or more characters that is not excluded" });
      }
    }
    expect(statements).toEqual([]);
  });
});
