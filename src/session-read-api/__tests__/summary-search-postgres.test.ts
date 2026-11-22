/**
 * GET /v1/sessions/summaries/search with summarySearchBackend "postgres" (postgres/summary-search.ts)
 * over a recording Directory fake: the directory query alone finds the candidates (one normalised
 * phrase term, keyset pages of DIRECTORY_LIST_MAX_LIMIT rows, at most SUMMARY_SEARCH_PG_MAX_PAGES),
 * summaryMatches re-checks every row, every row is tenant-checked, and the page keeps the
 * ClickHouse path's shape and order. With the selector unset nothing changes. No database.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type ReadApiDeps, type VerifiedIdentity } from "../app.ts";
import { DIRECTORY_LIST_MAX_LIMIT, type Directory, type DirectoryListRow, type DirectoryQueryArgs } from "../directory.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import {
  compareSummaryRows, normalizeSummarySearchQ, SUMMARY_SEARCH_PG_MAX_PAGES, type SummarySearchPgRow,
} from "../postgres/summary-search.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "user-s1", orgId: "org-s", workspaceId: "ws-s", scopes: ["rth:read"] };

/** A directory query row; `n` orders it (larger = newer). */
function row(n: number, over: Partial<SummarySearchPgRow> = {}): SummarySearchPgRow {
  const at = new Date(Date.UTC(2026, 9, 1) + n * 60_000);
  return {
    org_id: "org-s", workspace_id: "ws-s", harness: "codex", session_id: `s${n}`,
    created_at: new Date("2026-10-01T00:00:00Z"), last_activity_at: at,
    title: `Pipeline work ${n}`, task_title: null, agent_names: [], project_id: null,
    project_key: null, project_key_method: null, git_repo: null, git_branch: null, initial_commit: null,
    originator: null, agent_version: null, models: [], state: "active", event_count: 1,
    input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: [], user_ids: ["user-s1"],
    parent_session_ids: [], child_session_count: 0,
    short_summary: "Fixed the ClickHouse erasure race", short_summary_generated_at: new Date("2026-10-02T04:00:00Z"),
    catalog_projected_at: null, projected_at: null,
    cursor_ts: at.toISOString().replace(/Z$/u, "000Z"),
    ...over,
  };
}

/** Rows newest first, served as keyset pages of args.limit + 1 after args.cursor. */
function fakeDirectory(all: SummarySearchPgRow[] | null) {
  const queries: Array<{ orgId: string; workspaceId: string; args: DirectoryQueryArgs }> = [];
  const lists: unknown[] = [];
  const directory: Directory = {
    async list(_o, _w, args) { lists.push(args); return []; },
    async activity() { return []; },
    async query(orgId, workspaceId, args) {
      queries.push({ orgId, workspaceId, args });
      if (all === null) return null;
      const sorted = [...all].sort(compareSummaryRows);
      const from = args.cursor ? sorted.findIndex((r) => compareSummaryRows(r, {
        ...r, cursor_ts: args.cursor!.lastActivityAt, harness: args.cursor!.harness, session_id: args.cursor!.sessionId,
      }) > 0) : 0;
      return from < 0 ? [] : sorted.slice(from, from + args.limit + 1);
    },
    async queryActivity() { throw new Error("not used"); },
  };
  return { directory, queries, lists };
}

const noCh = (): { ch: ClickHouseClient; selects: number } => {
  const box = { selects: 0 } as { ch: ClickHouseClient; selects: number };
  box.ch = {
    database: "relay_search",
    async command() { throw new Error("no command"); },
    async insert() { throw new Error("no insert"); },
    async select() { box.selects++; throw new Error("the postgres backend never reads ClickHouse"); },
  };
  return box;
};

function api(over: {
  summarySearch?: boolean; backend?: "postgres"; ch?: ClickHouseClient; directory?: Directory;
  workspaces?: ReadonlySet<string> | "*"; ownerBuffered?: ReadApiDeps["ownerBuffered"]; logs?: Array<Record<string, unknown>>;
} = {}) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    // The self-host service binds the portable search adapter and no ClickHouse.
    ...(over.ch ? { ch: over.ch } : { adapter: {} as never }),
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: (entry) => { over.logs?.push(entry as Record<string, unknown>); },
    directory: over.directory ?? fakeDirectory([]).directory,
    directoryWorkspaces: over.workspaces ?? new Set(["ws-s"]),
    ...(over.ownerBuffered ? { ownerBuffered: over.ownerBuffered } : {}),
    ...(over.summarySearch !== undefined ? { summarySearch: over.summarySearch } : {}),
    ...(over.backend ? { summarySearchBackend: over.backend } : {}),
  } as ReadApiDeps);
  return (p: string) => app.request(p, { headers: { authorization: "Bearer good" } });
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;
const path = (q: string, extra = "") => `/v1/sessions/summaries/search?workspace=ws-s&q=${encodeURIComponent(q)}${extra}`;
const keys = (b: Record<string, any>) => b.sessions.map((s: { source: string; sessionId: string }) => `${s.source}:${s.sessionId}`);

describe("selector unset: unchanged", () => {
  test("no ClickHouse binding is still the not-configured 501, and the directory query is never used", async () => {
    const { directory, queries } = fakeDirectory([row(1)]);
    const r = await api({ summarySearch: true, directory })(path("erasure"));
    expect(r.status).toBe(501);
    expect((await json(r)).error).toMatchObject({ code: "unsupported_by_backend", reason: "summary_search_not_configured" });
    expect(queries).toHaveLength(0);
  });

  test("with ClickHouse the ClickHouse path serves, the directory query is never used", async () => {
    const box = noCh();
    const { directory, queries } = fakeDirectory([row(1)]);
    const r = await api({ summarySearch: true, ch: box.ch, directory })(path("erasure"));
    expect(r.status).toBe(503);
    expect(box.selects).toBe(1);
    expect(queries).toHaveLength(0);
  });
});

describe("postgres backend", () => {
  test("flag off is the not-configured 501 before anything else, nothing read", async () => {
    const { directory, queries } = fakeDirectory([row(1)]);
    for (const flag of [undefined, false]) {
      const get = api({ backend: "postgres", directory, ...(flag === undefined ? {} : { summarySearch: flag }) });
      for (const p of [path("erasure"), "/v1/sessions/summaries/search?workspace=ws-other&q=erasure", "/v1/sessions/summaries/search"]) {
        const r = await get(p);
        expect(r.status, p).toBe(501);
        expect((await json(r)).error, p).toMatchObject({ code: "unsupported_by_backend", reason: "summary_search_not_configured" });
      }
    }
    expect(queries).toHaveLength(0);
  });

  test("serves title and short-summary matches from the directory query alone, re-checked, in the ClickHouse order", async () => {
    const box = noCh();
    const logs: Array<Record<string, unknown>> = [];
    const { directory, queries, lists } = fakeDirectory([
      row(5, { harness: "claude", title: "Erasure follow-up", short_summary: null }),
      // search_text superset: matched on another field (or stale text) only, never served.
      row(4, { title: "Other", short_summary: "Something else", git_branch: "erasure-branch" }),
      row(3),
      row(3, { harness: "claude", session_id: "s3" }),
    ]);
    const r = await api({ summarySearch: true, backend: "postgres", ch: box.ch, directory, logs })(path("  ERASURE ", "&limit=10"));
    expect(r.status).toBe(200);
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "sessions", "truncated"]);
    expect(b.truncated).toBe(false);
    expect(keys(b)).toEqual(["claude:s5", "codex:s3", "claude:s3"]);
    expect(b.sessions[1]).toMatchObject({ shortSummary: "Fixed the ClickHouse erasure race", shortSummaryGeneratedAt: "2026-10-02T04:00:00.000Z" });
    expect(Object.keys(b.sessions[1])).not.toContain("cursor_ts");
    expect(box.selects).toBe(0);
    expect(lists).toHaveLength(0);
    // One query, the verified tenant, one normalised phrase as the only term, a full directory page.
    expect(queries).toHaveLength(1);
    expect(queries[0]!.orgId).toBe("org-s");
    expect(queries[0]!.workspaceId).toBe("ws-s");
    expect(queries[0]!.args.limit).toBe(DIRECTORY_LIST_MAX_LIMIT);
    expect(queries[0]!.args.cursor).toBeUndefined();
    const { free, ...rest } = queries[0]!.args.terms;
    // Trimmed, case kept: ILIKE and the re-check both fold case.
    expect(free).toEqual(["ERASURE"]);
    for (const list of Object.values(rest)) expect(list).toEqual([]);
    // Counts only in the log line.
    const served = logs.filter((l) => l.reason === "summary_search_served");
    expect(served).toEqual([{ route: "/v1/sessions/summaries/search", reason: "summary_search_served", kind: "postgres", listed: 4, count: 3, status: 200, correlation_id: expect.any(String) }]);
    expect(JSON.stringify(logs)).not.toContain("erasure");
  });

  test("a row of another tenant is a 500, never served", async () => {
    for (const over of [{ org_id: "org-other" }, { workspace_id: "ws-other" }]) {
      const { directory } = fakeDirectory([row(2), row(1, over)]);
      const r = await api({ summarySearch: true, backend: "postgres", directory })(path("erasure"));
      expect(r.status).toBe(500);
      expect(JSON.stringify(await json(r))).not.toContain("erasure race");
    }
  });

  test("a row without a fixed-width cursor_ts is out of contract", async () => {
    const { directory } = fakeDirectory([row(1, { cursor_ts: "2026-10-01T00:01:00Z" })]);
    const r = await api({ summarySearch: true, backend: "postgres", directory })(path("erasure"));
    expect(r.status).toBe(501);
    expect((await json(r)).error.reason).toBe("directory_row_out_of_contract");
  });

  test("more than limit matches: the first `limit`, truncated, and the scan stops", async () => {
    const all = Array.from({ length: 450 }, (_, i) => row(1000 - i));
    const { directory, queries } = fakeDirectory(all);
    const b = await json(await api({ summarySearch: true, backend: "postgres", directory })(path("erasure", "&limit=2")));
    expect(keys(b)).toEqual(["codex:s1000", "codex:s999"]);
    expect(b.truncated).toBe(true);
    expect(queries).toHaveLength(1);
  });

  test("exactly `limit` matches and the directory exhausted: not truncated", async () => {
    const { directory, queries } = fakeDirectory([row(2), row(1), row(0, { title: "x", short_summary: "y" })]);
    const b = await json(await api({ summarySearch: true, backend: "postgres", directory })(path("erasure", "&limit=2")));
    expect(keys(b)).toEqual(["codex:s2", "codex:s1"]);
    expect(b.truncated).toBe(false);
    expect(queries).toHaveLength(1);
  });

  test(`at most ${SUMMARY_SEARCH_PG_MAX_PAGES} keyset pages: past the budget the page may be empty and is truncated`, async () => {
    // Every row matches the SQL term (say a branch name) but none the title or summary.
    const all = Array.from({ length: DIRECTORY_LIST_MAX_LIMIT * 4 }, (_, i) => row(5000 - i, { title: "x", short_summary: "y", git_branch: "erasure" }));
    // One real match past the budget is never reached.
    all.push(row(1, { title: "erasure" }));
    const { directory, queries } = fakeDirectory(all);
    const logs: Array<Record<string, unknown>> = [];
    const b = await json(await api({ summarySearch: true, backend: "postgres", directory, logs })(path("erasure")));
    expect(b).toMatchObject({ sessions: [], truncated: true });
    expect(queries).toHaveLength(SUMMARY_SEARCH_PG_MAX_PAGES);
    // Keyset: each page continues after the previous page's last visible row.
    const sorted = [...all].sort(compareSummaryRows);
    expect(queries.map((q) => q.args.cursor)).toEqual([undefined, 1, 2].map((p) => p === undefined ? undefined : {
      lastActivityAt: sorted[p * DIRECTORY_LIST_MAX_LIMIT - 1]!.cursor_ts, sessionId: sorted[p * DIRECTORY_LIST_MAX_LIMIT - 1]!.session_id, harness: "codex",
    }));
    expect(logs.find((l) => l.reason === "summary_search_served")).toMatchObject({ listed: SUMMARY_SEARCH_PG_MAX_PAGES * DIRECTORY_LIST_MAX_LIMIT, count: 0 });
  });

  test("a match on the third page is still found; a full last page is truncated", async () => {
    const all = Array.from({ length: DIRECTORY_LIST_MAX_LIMIT * 3 + 1 }, (_, i) => row(5000 - i, { title: "x", short_summary: "y" }));
    all[DIRECTORY_LIST_MAX_LIMIT * 2 + 5] = row(5000 - (DIRECTORY_LIST_MAX_LIMIT * 2 + 5));
    const { directory, queries } = fakeDirectory(all);
    const b = await json(await api({ summarySearch: true, backend: "postgres", directory })(path("erasure")));
    expect(keys(b)).toEqual([`codex:s${5000 - (DIRECTORY_LIST_MAX_LIMIT * 2 + 5)}`]);
    expect(b.truncated).toBe(true);
    expect(queries).toHaveLength(3);
  });

  test("a short last page ends the scan without truncation", async () => {
    const all = Array.from({ length: DIRECTORY_LIST_MAX_LIMIT + 10 }, (_, i) => row(5000 - i, { title: "x", short_summary: "y" }));
    const { directory, queries } = fakeDirectory(all);
    const b = await json(await api({ summarySearch: true, backend: "postgres", directory })(path("erasure")));
    expect(b).toMatchObject({ sessions: [], truncated: false });
    expect(queries).toHaveLength(2);
  });

  test("an oversized directory answer is refused", async () => {
    const directory: Directory = {
      ...fakeDirectory([]).directory,
      async query() { return Array.from({ length: DIRECTORY_LIST_MAX_LIMIT + 2 }, (_, i) => row(i)); },
    };
    expect((await api({ summarySearch: true, backend: "postgres", directory })(path("erasure"))).status).toBe(501);
  });

  test("q bounds: fewer than 3 meaningful characters or more than 200 code points is 400, nothing read", async () => {
    const { directory, queries } = fakeDirectory([row(1)]);
    const get = api({ summarySearch: true, backend: "postgres", directory });
    // Combining marks, format characters and separator runs pass the trimmed 3-character check but not the meaningful count.
    const fewMeaningful = ["ab" + String.fromCodePoint(0x301, 0x301), "a" + String.fromCodePoint(0x200b) + "b", "a" + String.fromCodePoint(0xa0, 0xa0) + "b"];
    for (const q of ["ab", "   ", ...fewMeaningful, "x".repeat(201), String.fromCodePoint(0xe9).repeat(201)]) {
      const r = await get(path(q));
      expect(r.status, q).toBe(400);
    }
    expect((await json(await get(path(fewMeaningful[0]!)))).error.message).toBe("q must have at least 3 visible characters");
    expect((await json(await get(path("x".repeat(201))))).error.message).toBe("q must be 200 characters or fewer");
    expect((await get("/v1/sessions/summaries/search?workspace=ws-s")).status).toBe(400);
    expect((await get(path("abc", "&q=def"))).status).toBe(400);
    expect(queries).toHaveLength(0);
    // 200 code points is accepted.
    expect((await get(path("x".repeat(200)))).status).toBe(200);
    expect(queries).toHaveLength(1);
  });

  test("cursor and other parameters stay 501; buffered and the canary are 501 before the query", async () => {
    const { directory, queries } = fakeDirectory([row(1)]);
    const get = api({ summarySearch: true, backend: "postgres", directory });
    expect((await json(await get(path("abc", "&cursor=x")))).error.reason).toBe("param_cursor");
    expect((await json(await get(path("abc", "&source=codex")))).error.reason).toBe("param_source");
    const buffered = await api({ summarySearch: true, backend: "postgres", directory, ownerBuffered: { enabled: true, workspaces: "*" } as never })(path("erasure"));
    expect((await json(buffered)).error.reason).toBe("q_with_buffered");
    const canary = await api({ summarySearch: true, backend: "postgres", directory, workspaces: new Set(["other"]) })(path("erasure"));
    expect((await json(canary)).error.reason).toBe("directory_workspace_not_enabled");
    expect((await get("/v1/sessions/summaries/search?workspace=ws-other&q=erasure")).status).toBe(403);
    expect(queries).toHaveLength(0);
  });

  test("the directory function not installed is 501 directory_not_migrated; a failing query is 503", async () => {
    const { directory } = fakeDirectory(null);
    expect((await json(await api({ summarySearch: true, backend: "postgres", directory })(path("erasure")))).error.reason).toBe("directory_not_migrated");
    const failing: Directory = { ...fakeDirectory([]).directory, async query() { throw new Error("connection reset"); } };
    expect((await api({ summarySearch: true, backend: "postgres", directory: failing })(path("erasure"))).status).toBe(503);
  });
});

describe("normalizeSummarySearchQ", () => {
  test("normalises like the directory search and bounds meaningful characters and code points", () => {
    expect(normalizeSummarySearchQ("  Erasure   Race ")).toEqual({ ok: true, text: "Erasure Race" });
    expect(normalizeSummarySearchQ("ab").ok).toBe(false);
    expect(normalizeSummarySearchQ("x".repeat(200)).ok).toBe(true);
    expect(normalizeSummarySearchQ("x".repeat(201)).ok).toBe(false);
  });
});
