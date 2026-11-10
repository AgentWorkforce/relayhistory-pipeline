import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { normalizeCoordinationFacts, type CoordinationBackend } from "../coordination-backend.ts";
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u", orgId: "org", workspaceId: "ws", scopes: ["rth:read"] };
const auth = { authorization: "Bearer good" };
const t = (minute: number) => new Date(Date.UTC(2026, 10, 5, 0, minute)).toISOString();
const lane = (sessionId: string, minute: number): LaneFacts => {
  const branch = `feat/${sessionId}`;
  return {
    sessionId, source: "codex", userId: "u", projectId: "p", repo: "acme/repo", branch, taskRef: null, title: sessionId,
    parentSessionId: null, startedAt: t(minute), lastActionAt: t(minute + 10),
    writes: [{ path: "src/shared.ts", ts: t(minute), eventId: `${sessionId}-e`, kind: "tool", branch }],
    commits: [], outcomes: [], usage: [{ ts: t(minute + 1), eventId: `${sessionId}-u`, tokens: 5, costUsdMicros: 7 }],
  };
};
const verify = async (token: string) => {
  if (token !== "good") throw new AuthError(401, "invalid_token", "invalid");
  return ID;
};
const base = "/v1/coordination/overlaps?workspace=ws&since=2026-11-05T00:00:00Z&until=2026-11-06T00:00:00Z";

function backend(readiness: "ready" | "session_partial" = "ready"): CoordinationBackend {
  return {
    kind: "postgres",
    windowReadiness: async () => readiness,
    readFacts: async () => ({ lanes: [lane("a", 0), lane("b", 5)], sessionsTruncated: 0, notes: [] }),
    isSchemaMissing: () => false,
  };
}

const injected = (enabled: boolean, b = backend()) => createReadApi({
  verify, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0,
  directoryWorkspaces: new Set([ID.workspaceId]), coordination: { enabled, backend: b },
});

describe("GET /v1/coordination/overlaps", () => {
  test("the exact flag gate is a 404; auth and workspace isolation are unchanged", async () => {
    const off = await injected(false).request(base, { headers: auth });
    expect(off.status).toBe(404);
    expect(await off.json()).toEqual({ error: "coordination_overlaps_disabled" });
    expect((await injected(false).request(base)).status).toBe(401); // auth remains identical to every /v1 read
    expect((await injected(true).request(base)).status).toBe(401);
    expect((await injected(true).request(base.replace("workspace=ws", "workspace=other"), { headers: auth })).status).toBe(403);
  });

  test("partial readiness is a 200 with honest coverage and the frozen wire contract", async () => {
    const response = await injected(true, backend("session_partial")).request(base, { headers: auth });
    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.contract).toBe("coordination-overlaps/1");
    expect(body.coverage).toEqual({ status: "partial", sessionsConsidered: 2, sessionsTruncated: 0, notes: ["facts readiness: session_partial"] });
    expect(body.detector).toMatchObject({ name: "outcome-facts", version: "1" });
    expect(body.findings).toHaveLength(1);
    expect(body.findings[0].communication).toBe("unknown");
    expect(body.summary).toMatchObject({ findingCount: 1, lanesInvolved: 2, byKind: { concurrent_edit: 1 } });
  });

  test("window and limit bounds are strict", async () => {
    const get = (path: string) => injected(true).request(path, { headers: auth });
    expect((await get(base.replace("2026-11-06", "2026-12-07"))).status).toBe(400);
    expect((await get(`${base}&limit=0`)).status).toBe(400);
    expect((await get(`${base}&limit=201`)).status).toBe(400);
    expect((await get(`${base}&limit=1&limit=2`)).status).toBe(400);
  });

  test("readiness covers the seven-day evidence lookback while the response window stays requested", async () => {
    let checked: [string, string] | undefined;
    const b = backend();
    b.windowReadiness = async (_org, _workspace, since, until) => {
      checked = [since.toISOString(), until.toISOString()];
      return "ready";
    };
    const response = await injected(true, b).request(base, { headers: auth });
    expect(response.status).toBe(200);
    expect(checked).toEqual(["2026-10-29T00:00:00.000Z", "2026-11-06T00:00:00.000Z"]);
    expect((await response.json() as any).window).toEqual({ since: "2026-11-05T00:00:00.000Z", until: "2026-11-06T00:00:00.000Z" });
  });

  test("the ClickHouse backend serves synthetic facts and never crosses the token workspace", async () => {
    const event = (session_id: string, minute: number, files_touched = ["src/shared.ts"]) => ({
      ts: t(minute).replace("T", " ").replace("Z", ""), session_id, source: "codex", event_id: `${session_id}-e`, kind: "tool",
      project_id: "p", task_ref_id: null, task_title: session_id, user_id: "u", branch: `feat/${session_id}`,
      input_tokens: 2, output_tokens: 3, reasoning_tokens: 0, cost_usd_micros: 7,
      files_touched, row_id: session_id.padEnd(64, "0"),
    });
    const output = (fact_kind: "commit" | "outcome", session_id: string, minute: number, reverted: number) => ({
      fact_kind, ts: t(minute).replace("T", " ").replace("Z", ""), scope_source: "codex", source: "codex",
      session_id, fact_key: "1234567abcdef", origin: fact_kind === "commit" ? "commit_link" : "",
      reverted, row_id: `${fact_kind}-${session_id}`.padEnd(64, "0"),
    });
    const calls: Array<{ sql: string; params: unknown }> = [];
    const ch: ClickHouseClient = {
      database: "x", command: async () => { throw new Error("write"); }, insert: async () => { throw new Error("write"); },
      async select<T>(sql, options) {
        calls.push({ sql, params: options?.params });
        if (sql.includes("output_facts_workspace_state")) return [{ complete_since: "2026-11-01 00:00:00.000000" }] as T[];
        if (sql.includes("output_facts_session_state") || sql.includes("FROM transcript_events FINAL")) return [{ n: 0 }] as T[];
        if (sql.includes("FROM transcript_event_facts FINAL") && sql.includes("arrayExists"))
          return [event("a", 0), event("b", 5), event("c", 1, ["src/commit.ts"]), event("d", 6, ["src/revert.ts"])] as T[];
        if (sql.includes("FROM transcript_event_facts FINAL"))
          return [event("a", 0), event("b", 5), event("c", 1, []), event("d", 6, [])] as T[];
        if (sql.includes("FROM session_output_facts FINAL")) return [output("commit", "c", 2, 0), output("outcome", "d", 7, 1)] as T[];
        throw new Error(`unexpected query: ${sql}`);
      },
    };
    const live = memoryLiveness();
    const app = createReadApi({
      verify, ch, liveness: live.liveness, completeSinceMs: 0,
      directoryWorkspaces: new Set([ID.workspaceId]), coordination: { enabled: true },
    });
    const response = await app.request(`${base}&project=p`, { headers: auth });
    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.coverage.status).toBe("partial"); // no directory: relation/title coverage is incomplete
    expect(body.findings.map((finding: any) => finding.kind)).toEqual(["superseded_work", "concurrent_edit"]);
    expect(live.calls.erased.flat().map((candidate) => candidate.sessionId).sort()).toEqual(["a", "b", "c", "d"]);
    expect(calls.every((call) => !JSON.stringify(call.params).includes("other"))).toBe(true);
    const evidenceCall = calls.find((call) => call.sql.includes("arrayExists"))!;
    const outputCall = calls.find((call) => call.sql.includes("fact_kind IN"))!;
    expect(evidenceCall.sql).toContain("LIMIT 10001");
    expect(outputCall.sql).toContain("LIMIT 50001");
    expect(evidenceCall.params).toMatchObject({ evidenceSince: "2026-10-29 00:00:00.000", since: "2026-11-05 00:00:00.000" });
    const before = calls.length;
    expect((await app.request(base.replace("workspace=ws", "workspace=other"), { headers: auth })).status).toBe(403);
    expect(calls).toHaveLength(before);
  });

  test("output outcomes join through their canonical scope source", async () => {
    const normalized = await normalizeCoordinationFacts(undefined, {
      orgId: "org", workspaceId: "ws", since: new Date(t(0)), until: new Date(t(60)),
    }, [{
      ts: t(0), session_id: "s", source: "codex", event_id: "e", kind: "tool", project_id: "p",
      task_ref_id: null, task_title: null, user_id: null, branch: null, input_tokens: 0,
      output_tokens: 0, reasoning_tokens: 0, cost_usd_micros: null, files_touched: [], row_id: "e-row",
    }], [{
      fact_kind: "outcome", ts: t(1), scope_source: "codex", source: "legacy-stored-source",
      session_id: "s", fact_key: "abc", origin: "", reverted: 1, row_id: "o-row",
    }, {
      fact_kind: "commit", ts: t(2), scope_source: "codex", source: "codex",
      session_id: "output-only", fact_key: "def", origin: "commit_link", reverted: 0, row_id: "c-row",
    }], [], {
      evidence: { cap: 10_000, truncated: false }, outputs: { cap: 50_000, truncated: false },
      usage: { cap: 10_000, truncated: false },
    });
    expect(normalized.lanes.find((lane) => lane.sessionId === "s")!.outcomes)
      .toEqual([{ key: "abc", ts: t(1), eventId: "o-row", reverted: true }]);
    expect(normalized.lanes.find((lane) => lane.sessionId === "output-only")!.commits)
      .toEqual([{ sha: "def", ts: t(2), eventId: "c-row", reverted: null }]);
  });

  test("a capped usage stream keeps findings but makes duplicate cost unknown", async () => {
    const events = ["a", "b"].map((session_id, i) => ({
      ts: t(i * 5), session_id, source: "codex", event_id: `${session_id}-e`, kind: "tool", project_id: "p",
      task_ref_id: null, task_title: null, user_id: "u", branch: `feat/${session_id}`, input_tokens: 1,
      output_tokens: 1, reasoning_tokens: 0, cost_usd_micros: 2, files_touched: ["src/shared.ts"], row_id: `${session_id}-row`,
    }));
    const normalized = await normalizeCoordinationFacts(undefined, {
      orgId: "org", workspaceId: "ws", since: new Date(t(0)), until: new Date(t(60)),
    }, events, [], events, {
      evidence: { cap: 10_000, truncated: false }, outputs: { cap: 50_000, truncated: false },
      usage: { cap: 1, truncated: true },
    });
    expect(normalized.sessionsTruncated).toBe(1);
    expect(normalized.notes).toContain("usage stream cap 1 reached; duplicate cost is unknown");
    expect(normalized.lanes.every((item) => item.usage.some((usage) => usage.tokens === null))).toBe(true);
  });
});
