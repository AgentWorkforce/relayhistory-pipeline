/**
 * GET /v1/sessions/summaries/search (READ_API_SUMMARY_SEARCH_ENABLED, clickhouse/summary-search-query.ts)
 * over a recording ClickHouse fake and a Directory fake: flag off answers 501 and reads nothing;
 * flag on discovers candidates in ClickHouse, hydrates them from the directory (the authority) and
 * serves only rows whose CURRENT title or short summary still contains q, in ClickHouse order.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type ReadApiDeps, type VerifiedIdentity } from "../app.ts";
import type { Directory, DirectoryListArgs, DirectoryListRow } from "../directory.ts";
import { ClickHouseError, type ClickHouseClient, type QueryParams } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { buildSummarySearchQuery, foldSummaryText, SUMMARY_SEARCH_MAX_CANDIDATES, summaryMatches } from "../clickhouse/summary-search-query.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "user-s1", orgId: "org-s", workspaceId: "ws-s", scopes: ["rth:read"] };

function row(over: Partial<DirectoryListRow> = {}): DirectoryListRow {
  return {
    org_id: "org-s", workspace_id: "ws-s", harness: "codex", session_id: "s1",
    created_at: new Date("2026-10-01T00:00:00Z"), last_activity_at: new Date("2026-10-02T03:04:05.123Z"),
    title: "Pipeline work", task_title: null, agent_names: [], project_id: null,
    project_key: null, project_key_method: null, git_repo: null, git_branch: null, initial_commit: null,
    originator: null, agent_version: null, models: [], state: "active", event_count: 1,
    input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: [], user_ids: ["user-s1"],
    parent_session_ids: [], child_session_count: 0,
    short_summary: "Fixed the ClickHouse erasure race", short_summary_generated_at: new Date("2026-10-02T04:00:00Z"),
    catalog_projected_at: null, projected_at: null,
    ...over,
  };
}

function fakeCh(answer: () => Promise<Array<{ source: string; session_id: string }>>) {
  const selects: Array<{ query: string; params?: QueryParams; settings?: Record<string, string | number> }> = [];
  const ch: ClickHouseClient = {
    database: "relay_search",
    async command() { throw new Error("the read route never writes"); },
    async insert() { throw new Error("the read route never writes"); },
    async select<T>(query: string, options?: { params?: QueryParams; settings?: Record<string, string | number> }) {
      selects.push({ query, ...(options?.params ? { params: options.params } : {}), ...(options?.settings ? { settings: options.settings } : {}) });
      return (await answer()) as T[];
    },
  };
  return { ch, selects };
}

function fakeDirectory(rows: DirectoryListRow[] | null) {
  const lists: DirectoryListArgs[] = [];
  const directory: Directory = {
    async list(_o, _w, args) { lists.push(args); return rows; },
    async activity() { return []; },
    async query() { throw new Error("not used"); },
    async queryActivity() { throw new Error("not used"); },
  };
  return { directory, lists };
}

function api(over: { summarySearch?: boolean; ch?: ClickHouseClient; directory?: Directory; workspaces?: ReadonlySet<string> | "*"; ownerBuffered?: ReadApiDeps["ownerBuffered"]; operatorErased?: ReadApiDeps["operatorErased"] } = {}) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch: over.ch ?? fakeCh(async () => []).ch,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: () => {},
    directory: over.directory ?? fakeDirectory([]).directory,
    directoryWorkspaces: over.workspaces ?? new Set(["ws-s"]),
    ...(over.ownerBuffered ? { ownerBuffered: over.ownerBuffered } : {}),
    ...(over.summarySearch !== undefined ? { summarySearch: over.summarySearch } : {}),
    ...(over.operatorErased ? { operatorErased: over.operatorErased } : {}),
  });
  return (path: string) => app.request(path, { headers: { authorization: "Bearer good" } });
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;
const path = (q: string, extra = "") => `/v1/sessions/summaries/search?workspace=ws-s&q=${encodeURIComponent(q)}${extra}`;

describe("buildSummarySearchQuery", () => {
  test("tenant ids and the folded q are bound parameters; limit + 1 tells truncation", () => {
    const b = buildSummarySearchQuery("relay_search", { orgId: "org'x", workspaceId: "ws", q: "ClickHouse İ", limit: 20 });
    expect(b.params).toEqual({ org: "org'x", ws: "ws", qf: "ClickHouse İ".toLowerCase(), n: 21, cap: SUMMARY_SEARCH_MAX_CANDIDATES });
    expect(b.query).not.toContain("org'x");
    expect(b.query).not.toContain("clickhouse");
    expect(b.query).toContain("`relay_search`.`session_brief_documents`");
  });

  test("candidates on any version per column, then the NEWEST version decides (no stale or tombstoned match)", () => {
    const { query } = buildSummarySearchQuery("relay_search", { orgId: "o", workspaceId: "w", q: "abc", limit: 5 });
    // One indexed branch per column, both tenant-scoped.
    expect(query).toContain("hasAllTokens(summary_folded, {qf:String}) AND position(summary_folded, {qf:String}) > 0");
    expect(query).toContain("hasAllTokens(title_folded, {qf:String}) AND position(title_folded, {qf:String}) > 0");
    expect(query.match(/org_id = \{org:String\} AND workspace_id = \{ws:String\}/g)).toHaveLength(3);
    // Collapse to the newest epoch first, then the predicates and the tombstone filter on that row only.
    const collapse = query.indexOf("ORDER BY source, session_id, epoch DESC\nLIMIT 1 BY source, session_id");
    const outer = query.indexOf("WHERE deleted = 0 AND (position(summary_folded, {qf:String}) > 0 OR position(title_folded, {qf:String}) > 0)");
    expect(collapse).toBeGreaterThan(0);
    expect(outer).toBeGreaterThan(collapse);
    expect(query).toMatch(/ORDER BY last_activity_at DESC, source DESC, session_id DESC\nLIMIT \{n:UInt32\}$/);
  });

  test("each candidate branch is bounded (newest activity first), so the IN set never grows unbounded", () => {
    const { query } = buildSummarySearchQuery("relay_search", { orgId: "o", workspaceId: "w", q: "abc", limit: 5 });
    expect(query.match(/> 0 ORDER BY last_activity_at DESC, source DESC, session_id DESC LIMIT \{cap:UInt32\}\)/g)).toHaveLength(2);
    expect(SUMMARY_SEARCH_MAX_CANDIDATES).toBeGreaterThan(51);
  });

  test("a database name that is not an identifier is refused", () => {
    expect(() => buildSummarySearchQuery("relay_search`; DROP", { orgId: "o", workspaceId: "w", q: "abc", limit: 5 })).toThrow();
  });

  test("the re-check uses the publisher's fold on the current title and short summary", () => {
    expect(foldSummaryText("ΣΑ İ")).toBe("ΣΑ İ".toLowerCase());
    expect(summaryMatches({ title: null, short_summary: "Fixed the ERASURE race" }, "erasure")).toBe(true);
    expect(summaryMatches({ title: "Erasure work", short_summary: null }, "ERASURE")).toBe(true);
    expect(summaryMatches({ title: "x", short_summary: "[REDACTED] token" }, "sk-secret")).toBe(false);
    expect(summaryMatches({ title: null, short_summary: null }, "abc")).toBe(false);
  });
});

describe("flag off or unconfigured: 501, nothing read", () => {
  for (const flag of [undefined, false]) {
    test(`summarySearch=${flag}`, async () => {
      const { ch, selects } = fakeCh(async () => [{ source: "codex", session_id: "s1" }]);
      const { directory, lists } = fakeDirectory([row()]);
      const r = await api({ ch, directory, ...(flag === undefined ? {} : { summarySearch: flag }) })(path("erasure"));
      expect(r.status).toBe(501);
      expect((await json(r)).error).toMatchObject({ code: "unsupported_by_backend", reason: "summary_search_not_configured" });
      expect(selects).toHaveLength(0);
      expect(lists).toHaveLength(0);
    });
  }

  test("flag off: the not-configured 501 comes before workspace attestation (a foreign or missing workspace too)", async () => {
    const { ch, selects } = fakeCh(async () => []);
    for (const flag of [undefined, false] as const) {
      const get = api({ ch, ...(flag === undefined ? {} : { summarySearch: flag }) });
      for (const p of ["/v1/sessions/summaries/search?workspace=ws-other&q=erasure", "/v1/sessions/summaries/search?q=erasure", "/v1/sessions/summaries/search"]) {
        const r = await get(p);
        expect(r.status, p).toBe(501);
        expect((await json(r)).error, p).toMatchObject({ code: "unsupported_by_backend", reason: "summary_search_not_configured" });
      }
    }
    expect(selects).toHaveLength(0);
  });

  test("operator-erase fence wired (Lambda): the search path is not a session; flag-off 501 stays first, no fence lookup", async () => {
    const { ch, selects } = fakeCh(async () => []);
    let fenceCalls = 0;
    const get = api({ ch, operatorErased: async () => { fenceCalls++; return true; } });
    for (const p of ["/v1/sessions/summaries/search?workspace=ws-other&q=erasure", "/v1/sessions/summaries/search?q=erasure&source=claude"]) {
      const r = await get(p);
      expect(r.status, p).toBe(501);
      expect((await json(r)).error, p).toMatchObject({ code: "unsupported_by_backend", reason: "summary_search_not_configured" });
    }
    expect(fenceCalls).toBe(0);
    expect(selects).toHaveLength(0);
  });

  test("owner-buffered workspace: 501 q_with_buffered before ClickHouse or the directory", async () => {
    const { ch, selects } = fakeCh(async () => [{ source: "codex", session_id: "s1" }]);
    const { directory, lists } = fakeDirectory([row()]);
    const r = await api({ summarySearch: true, ch, directory, ownerBuffered: { enabled: true, workspaces: "*" } as never })(path("erasure"));
    expect(r.status).toBe(501);
    expect((await json(r)).error.reason).toBe("q_with_buffered");
    expect(selects).toHaveLength(0);
    expect(lists).toHaveLength(0);
  });

  test("a workspace off the directory canary is 501 before ClickHouse", async () => {
    const { ch, selects } = fakeCh(async () => []);
    const r = await api({ summarySearch: true, ch, workspaces: new Set(["other"]) })(path("erasure"));
    expect((await json(r)).error.reason).toBe("directory_workspace_not_enabled");
    expect(selects).toHaveLength(0);
  });

  test("another workspace than the token's is 403; no workspace is 501 org_scope", async () => {
    const get = api({ summarySearch: true });
    expect((await get("/v1/sessions/summaries/search?workspace=ws-other&q=erasure")).status).toBe(403);
    expect((await json(await get("/v1/sessions/summaries/search?q=erasure"))).error.reason).toBe("org_scope");
  });
});

describe("flag on", () => {
  test("hydrates candidates and serves only rows whose CURRENT text still matches, in ClickHouse order", async () => {
    const { ch, selects } = fakeCh(async () => [
      { source: "claude", session_id: "s2" },
      { source: "codex", session_id: "s1" },
      { source: "codex", session_id: "stale" },
      { source: "codex", session_id: "gone" },
    ]);
    const { directory, lists } = fakeDirectory([
      row(),
      row({ harness: "claude", session_id: "s2", short_summary: null, title: "erasure follow-up" }),
      // ClickHouse still has the old text, the directory's summary was cleared/replaced: never served.
      row({ session_id: "stale", short_summary: "Something else entirely", title: "Other" }),
      // "gone" is tombstoned: the directory list does not return it at all.
    ]);
    const r = await api({ summarySearch: true, ch, directory })(path("  ERASURE ", "&limit=10"));
    expect(r.status).toBe(200);
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "sessions", "truncated"]);
    expect(b.truncated).toBe(false);
    expect(b.sessions.map((s: { source: string; sessionId: string }) => `${s.source}:${s.sessionId}`)).toEqual(["claude:s2", "codex:s1"]);
    expect(b.sessions[1]).toMatchObject({ shortSummary: "Fixed the ClickHouse erasure race", shortSummaryGeneratedAt: "2026-10-02T04:00:00.000Z" });
    // One ClickHouse read with the verified tenant and the trimmed q, bounded settings.
    expect(selects).toHaveLength(1);
    expect(selects[0]!.params).toEqual({ org: "org-s", ws: "ws-s", qf: "erasure", n: 11, cap: SUMMARY_SEARCH_MAX_CANDIDATES });
    expect(selects[0]!.settings).toMatchObject({ max_execution_time: 2, result_overflow_mode: "throw" });
    // One exact-key directory read.
    expect(lists).toEqual([{ sessionKeys: [
      { harness: "claude", sessionId: "s2" }, { harness: "codex", sessionId: "s1" },
      { harness: "codex", sessionId: "stale" }, { harness: "codex", sessionId: "gone" },
    ], limit: 4 }]);
  });

  test("limit + 1 candidates: truncated, and only `limit` keys are hydrated", async () => {
    const { ch } = fakeCh(async () => [{ source: "codex", session_id: "s1" }, { source: "codex", session_id: "s9" }]);
    const { directory, lists } = fakeDirectory([row()]);
    const b = await json(await api({ summarySearch: true, ch, directory })(path("erasure", "&limit=1")));
    expect(b.truncated).toBe(true);
    expect(b.sessions).toHaveLength(1);
    expect(lists[0]!.sessionKeys).toEqual([{ harness: "codex", sessionId: "s1" }]);
  });

  test("no candidates: an empty page without a directory read", async () => {
    const { directory, lists } = fakeDirectory([row()]);
    const b = await json(await api({ summarySearch: true, directory })(path("erasure")));
    expect(b).toMatchObject({ sessions: [], truncated: false });
    expect(lists).toHaveLength(0);
  });

  test("a directory row of another tenant is a 500, never served", async () => {
    const { ch } = fakeCh(async () => [{ source: "codex", session_id: "s1" }]);
    const { directory } = fakeDirectory([row({ org_id: "org-other" })]);
    const r = await api({ summarySearch: true, ch, directory })(path("erasure"));
    expect(r.status).toBe(500);
    expect(JSON.stringify(await json(r))).not.toContain("erasure race");
  });

  test("q bounds, repeated and unknown parameters", async () => {
    const { ch, selects } = fakeCh(async () => []);
    const get = api({ summarySearch: true, ch });
    expect((await get(path("ab"))).status).toBe(400);
    expect((await get(path("   "))).status).toBe(400);
    expect((await get("/v1/sessions/summaries/search?workspace=ws-s")).status).toBe(400);
    expect((await get(path("x".repeat(501)))).status).toBe(400);
    expect((await get(path("abc", "&q=def"))).status).toBe(400);
    expect((await json(await get(path("abc", "&cursor=x")))).error.reason).toBe("param_cursor");
    expect((await json(await get(path("abc", "&source=codex")))).error.reason).toBe("param_source");
    expect(selects).toHaveLength(0);
  });

  test("the table not applied is 501 summary_search_schema_missing; other ClickHouse failures are 503", async () => {
    const missing = fakeCh(async () => { throw new ClickHouseError(404, "60", "UNKNOWN_TABLE"); });
    const r = await api({ summarySearch: true, ch: missing.ch })(path("erasure"));
    expect(r.status).toBe(501);
    expect((await json(r)).error.reason).toBe("summary_search_schema_missing");
    const denied = fakeCh(async () => { throw new ClickHouseError(403, "497", "ACCESS_DENIED"); });
    expect((await api({ summarySearch: true, ch: denied.ch })(path("erasure"))).status).toBe(503);
    const timeout = fakeCh(async () => { throw new ClickHouseError(500, "159", "TIMEOUT_EXCEEDED"); });
    const t = await api({ summarySearch: true, ch: timeout.ch })(path("erasure"));
    expect(t.status).toBe(503);
    expect((await json(t)).error.code).toBe("search_timeout");
  });

  test("malformed or oversized candidate rows are refused", async () => {
    for (const rows of [[{ source: "", session_id: "s1" }], [{ source: "codex" }], Array.from({ length: 3 }, (_, i) => ({ source: "codex", session_id: `s${i}` }))]) {
      const { ch } = fakeCh(async () => rows as never);
      const r = await api({ summarySearch: true, ch })(path("erasure", "&limit=1"));
      expect(r.status).toBe(503);
    }
  });

  test("the directory not migrated is 501", async () => {
    const { ch } = fakeCh(async () => [{ source: "codex", session_id: "s1" }]);
    const { directory } = fakeDirectory(null);
    expect((await json(await api({ summarySearch: true, ch, directory })(path("erasure")))).error.reason).toBe("directory_not_migrated");
  });
});
