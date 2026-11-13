import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { DirectoryError, sessionFromDirectoryRow, type Directory, type DirectoryListRow } from "../directory.ts";
import { memoryLiveness } from "./support.ts";
import { projectRelayhistorySession } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

const identity: VerifiedIdentity = { userId: "user-a", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const time = "2026-11-01T00:00:00.000Z";
const row = (over: Partial<DirectoryListRow> = {}): DirectoryListRow => ({
  org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: "session-a",
  created_at: time, last_activity_at: time, title: "Recorded title", task_title: null,
  agent_names: ["Recorded-Agent"], project_id: null, project_key: null, project_key_method: null,
  git_repo: null, git_branch: null, initial_commit: null, originator: null, agent_version: null,
  models: ["recorded-model"], state: null, event_count: 1, input_tokens: 0, output_tokens: 0,
  cost_usd_micros: null, machine_ids: [], user_ids: ["user-a"], parent_session_ids: [],
  child_session_count: 0, short_summary: null, short_summary_generated_at: null,
  catalog_projected_at: time, projected_at: time, ...over,
});
function api(options: { rows?: DirectoryListRow[] | null; selfhost?: boolean; error?: Error; listShortSummary?: boolean } = {}) {
  const calls: string[] = [];
  const list = async () => {
    calls.push("list");
    if (options.error) throw options.error;
    return options.rows === undefined ? [row()] : options.rows;
  };
  const directory: Directory = {
    list, query: async () => { calls.push("query"); return options.rows ?? [row()]; },
    activity: async () => [], queryActivity: async () => [],
  };
  const app = createReadApi({
    verify: async (token) => {
      if (token !== "good") throw new AuthError(401, "invalid_token", "Invalid token");
      return identity;
    },
    ch: { database: "unused", select: async () => { throw new Error("unexpected object/event read"); } } as never,
    liveness: memoryLiveness().liveness, completeSinceMs: 0,
    directory, directoryWorkspaces: new Set([identity.workspaceId]), sessionQuery: true,
    ...(options.selfhost === false ? {} : { selfhost: {} }),
    ...(options.listShortSummary === undefined ? {} : { listShortSummary: options.listShortSummary }),
  });
  return { calls, get: (query = "", token = "good", workspace = "ws-a") => app.request(`/v1/sessions?workspace=${workspace}${query}`, {
    headers: { authorization: `Bearer ${token}` },
  }) };
}
const body = async (response: Response) => await response.json() as { sessions?: Array<Record<string, unknown>>; correlationId?: string };
const optIn = "&includeAgentName=recorded";

describe("explicit recorded-agent list metadata", () => {
  test("default bytes/keys stay unchanged even with recorded agent metadata", async () => {
    const withName = api(), withoutName = api({ rows: [row({ agent_names: [] })] });
    const first = await body(await withName.get()), second = await body(await withoutName.get());
    expect({ ...first, correlationId: null }).toEqual({ ...second, correlationId: null });
    expect(first.sessions?.[0]).not.toHaveProperty("agentName");
    expect(first.sessions?.[0]?.catalog).not.toHaveProperty("agentName");
    expect(JSON.stringify(sessionFromDirectoryRow(row()))).toBe(JSON.stringify(sessionFromDirectoryRow(row({ agent_names: [] }))));
  });

  test("opt-in adds only the explicit top-level recorded name on one existing list read", async () => {
    const { get, calls } = api();
    const response = await get(optIn);
    expect(response.status).toBe(200);
    const item = (await body(response)).sessions![0]!;
    const { agentName, ...unchanged } = item;
    expect(agentName).toBe("Recorded-Agent");
    expect(unchanged).toEqual(sessionFromDirectoryRow(row()));
    expect(calls).toEqual(["list"]);
  });

  test("query lists use the same metadata mapping without changing query authority", async () => {
    const { get, calls } = api();
    const response = await get(`${optIn}&q=recorded`);
    expect(response.status).toBe(200);
    expect((await body(response)).sessions![0]?.agentName).toBe("Recorded-Agent");
    expect(calls).toEqual(["query"]);
  });

  test("the display opt-in does not change ordered rows or cursor bytes", async () => {
    const rows = [row(), row({ session_id: "session-b" })].map((item, i) => ({
      ...item, cursor_ts: `2026-11-01T00:00:00.00000${i}Z`,
    }));
    for (const query of ["", "&q=recorded"]) {
      const { get } = api({ rows });
      const defaults = await body(await get(`${query}&limit=1`));
      const extended = await body(await get(`${query}&limit=1${optIn}`));
      expect(extended).toHaveProperty("nextCursor", (defaults as Record<string, unknown>).nextCursor);
      expect(extended.sessions?.map((item) => item.sessionId)).toEqual(defaults.sessions?.map((item) => item.sessionId));
    }
  });

  test("historical multi-name order never creates a primary name", () => {
    for (const agent_names of [["A", "B"], ["B", "A"], ["A", "A"]])
      expect(sessionFromDirectoryRow(row({ agent_names }), { includeAgentName: "recorded" })).not.toHaveProperty("agentName");
  });

  test("unknown, unprojected or invalid recorded names are omitted without inventing a fallback", () => {
    for (const changed of [
      { agent_names: [] }, { agent_names: null }, { agent_names: [""] },
      { agent_names: [" padded "] }, { agent_names: ["bad\nname"] }, { agent_names: ["x".repeat(201)] },
      { agent_names: [42] as never }, { agent_names: "not-an-array" as never },
      { catalog_projected_at: null },
    ]) expect(sessionFromDirectoryRow(row(changed), { includeAgentName: "recorded" })).not.toHaveProperty("agentName");
    expect(sessionFromDirectoryRow(row({ agent_names: ["Agent-😀"] }), { includeAgentName: "recorded" }).agentName).toBe("Agent-😀");
    expect(sessionFromDirectoryRow(row({ agent_names: ["😀".repeat(200)] }), { includeAgentName: "recorded" }).agentName).toBe("😀".repeat(200));
  });

  test("only the exact singleton opt-in is accepted, before any directory call", async () => {
    for (const query of ["&includeAgentName=", "&includeAgentName=true", "&includeAgentName=Recorded", `${optIn}${optIn}`]) {
      const { get, calls } = api();
      expect((await get(query)).status).toBe(400);
      expect(calls).toEqual([]);
    }
    const { get, calls } = api({ selfhost: false });
    expect((await get(optIn)).status).toBe(501);
    expect(calls).toEqual([]);
  });

  test("auth and tenant binding cannot be bypassed with the opt-in", async () => {
    const denied = api();
    expect((await denied.get(optIn, "bad")).status).toBe(401);
    expect((await denied.get(optIn, "good", "ws-b")).status).toBe(403);
    expect((await denied.get(`${optIn}&workspace=ws-b`)).status).toBe(400);
    expect(denied.calls).toEqual([]);
    for (const changed of [{ org_id: "foreign-org" }, { workspace_id: "foreign-workspace" }]) {
      const foreign = api({ rows: [row({ ...changed, agent_names: ["Foreign-Agent"] })] });
      const response = await foreign.get(optIn);
      expect(response.status).toBe(500);
      expect(JSON.stringify(await body(response))).not.toContain("Foreign-Agent");
    }
  });

  test("empty/absent directory and refusal retain existing semantics", async () => {
    expect((await body(await api({ rows: [] }).get(optIn))).sessions).toEqual([]);
    expect((await api({ rows: null }).get(optIn)).status).toBe(501);
    const response = await api({ error: new DirectoryError("refused") }).get(optIn);
    expect(response.status).toBe(503);
    expect(JSON.stringify(await body(response))).not.toContain("Recorded-Agent");
  });

  test("user/history actors never enter recorded catalog agent metadata", () => {
    const records: RelayhistoryStreamRecord[] = [
      { kind: "session", origin_id: "o", record_id: "s", revision: "1", operation: "upsert", received_at: time, payload: { agent_name: "Recorded-Agent" } },
      { kind: "history", origin_id: "o", record_id: "h", revision: "1", operation: "upsert", received_at: time, payload: { role: "user", name: "Human", agent_name: "Not-Catalog", ts_ms: Date.parse(time), text: "synthetic" } },
      { kind: "session_event", origin_id: "o", record_id: "e", revision: "1", operation: "upsert", received_at: time, payload: { role: "user", name: "Human", agent_name: "Not-Catalog", ts_ms: Date.parse(time), text: "synthetic" } },
    ];
    const projected = projectRelayhistorySession(records, { workspaceId: "ws-a", source: "codex", sessionId: "s-a", machineId: "m-a", userId: "user-a" });
    expect(projected.rows.filter((item) => item.table === "session_catalog_agents").map((item) => item.row.agent_name)).toEqual(["Recorded-Agent"]);
  });
});

const summaryOptIn = "&includeShortSummary=recorded";
const stored = { short_summary: "Recorded summary", short_summary_generated_at: "2026-11-02T03:04:05.678Z" };
type ErrorBody = { error?: { code?: string; reason?: string } };

describe("opt-in list short summary (includeShortSummary=recorded, READ_API_LIST_SHORT_SUMMARY_ENABLED)", () => {
  test("flag off (absent or false): the param is 501 param_includeShortSummary before any directory call", async () => {
    for (const listShortSummary of [undefined, false]) {
      const { get, calls } = api({ rows: [row(stored)], ...(listShortSummary === undefined ? {} : { listShortSummary }) });
      const response = await get(summaryOptIn);
      expect(response.status).toBe(501);
      const error = (await response.json() as ErrorBody).error;
      expect(error).toMatchObject({ code: "unsupported_by_backend", reason: "param_includeShortSummary" });
      expect(calls).toEqual([]);
    }
  });

  test("only the exact singleton value is accepted (400 otherwise), flag on or off, before any directory call", async () => {
    for (const listShortSummary of [true, false])
      for (const query of ["&includeShortSummary=", "&includeShortSummary=true", "&includeShortSummary=Recorded", `${summaryOptIn}${summaryOptIn}`]) {
        const { get, calls } = api({ listShortSummary });
        expect([query, (await get(query)).status]).toEqual([query, 400]);
        expect(calls).toEqual([]);
      }
  });

  test("flag on + recorded: the two keys on list and query reads; nothing else changes", async () => {
    for (const query of ["", "&q=recorded"]) {
      const { get } = api({ rows: [row(stored)], listShortSummary: true });
      const response = await get(`${query}${summaryOptIn}`);
      expect(response.status).toBe(200);
      const { shortSummary, shortSummaryGeneratedAt, ...unchanged } = (await body(response)).sessions![0]!;
      expect([shortSummary, shortSummaryGeneratedAt]).toEqual(["Recorded summary", "2026-11-02T03:04:05.678Z"]);
      expect(unchanged).toEqual(sessionFromDirectoryRow(row(stored)));
    }
    const nulls = (await body(await api({ listShortSummary: true }).get(summaryOptIn))).sessions![0]!;
    expect(nulls).toMatchObject({ shortSummary: null, shortSummaryGeneratedAt: null, title: "Recorded title" });
  });

  test("combined with includeAgentName both extensions appear", async () => {
    const item = (await body(await api({ rows: [row(stored)], listShortSummary: true }).get(`${optIn}${summaryOptIn}`))).sessions![0]!;
    expect(item).toMatchObject({ agentName: "Recorded-Agent", shortSummary: "Recorded summary" });
  });

  test("no param: the keys are absent and the bytes match the flag-off response", async () => {
    const on = await body(await api({ rows: [row(stored)], listShortSummary: true }).get());
    const off = await body(await api({ rows: [row(stored)] }).get());
    expect(on.sessions?.[0]).not.toHaveProperty("shortSummary");
    expect(on.sessions?.[0]).not.toHaveProperty("shortSummaryGeneratedAt");
    expect({ ...on, correlationId: null }).toEqual({ ...off, correlationId: null });
  });

  test("an over-long stored summary is out of contract like an over-long title, never a truncated or substituted value", async () => {
    const response = await api({ rows: [row({ ...stored, short_summary: "s".repeat(1001) })], listShortSummary: true }).get(summaryOptIn);
    expect(response.status).toBe(501);
    const text = await response.text();
    expect((JSON.parse(text) as ErrorBody).error?.reason).toBe("directory_row_out_of_contract");
    expect(text).not.toContain("sss");
  });
});
