/**
 * GET /v1/sessions/output: relayhistory's 400s in its order, the canary allowlist, the
 * 501 contract (not configured, backfill incomplete, partial session), and, with a real
 * ClickHouse (READ_API_TEST_CLICKHOUSE_URL), the served body from the facts.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { memoryLiveness, CH_URL, freshClickHouse } from "./support.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { OUTPUT_FACTS_SESSION_STATE_TABLE, OUTPUT_FACTS_STATE_TABLE, outputFactsSchemaStatements } from "../../session-pipeline/output-facts-schema.ts";
import { outputFactsFromScope, reconcileOutputFacts } from "../../session-pipeline/output-facts.ts";
import { outputFixture, OUTPUT_FIXTURE_SCOPE } from "../../session-pipeline/__tests__/output-facts-fixture.ts";
import { SESSION_STATE_TABLE } from "../../session-pipeline/vendor/search-index/schema.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: OUTPUT_FIXTURE_SCOPE.orgId, workspaceId: OUTPUT_FIXTURE_SCOPE.workspaceId, scopes: ["rth:read"] };
const WS = OUTPUT_FIXTURE_SCOPE.workspaceId;
const DAY = "since=2026-11-05T00:00:00.000Z&until=2026-11-05T23:59:59.999Z";

function api(ch: ClickHouseClient, over: { output?: boolean; workspaces?: ReadonlySet<string> | "*" } = {}) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    directoryWorkspaces: over.workspaces ?? new Set([WS]),
    ...(over.output === false ? {} : { output: { enabled: true as const } }),
  });
  return (path: string, token: string | null = "good") => app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;

/** ClickHouse that must never be reached (validation and gate tests). */
const unreachable: ClickHouseClient = {
  database: "x",
  async command() { throw new Error("no command on the read path"); },
  async insert() { throw new Error("no insert on the read path"); },
  async select() { throw new Error("not reached"); },
};

describe("output route: validation and gates (no ClickHouse reached)", () => {
  test("relayhistory's 400s, in its order", async () => {
    const get = api(unreachable);
    const cases: Array<[string, number, string?]> = [
      [`/v1/sessions/output?${DAY}`, 400, "workspace is required"],
      [`/v1/sessions/output?workspace=${WS}&workspace=${WS}&${DAY}`, 400],
      [`/v1/sessions/output?workspace=other&${DAY}`, 403],
      [`/v1/sessions/output?workspace=${WS}&since=2026-11-05T00:00:00Z`, 400, "since and until are required exactly once"],
      [`/v1/sessions/output?workspace=${WS}&${DAY}&since=2026-11-04T00:00:00Z`, 400, "since and until are required exactly once"],
      [`/v1/sessions/output?workspace=${WS}&since=nope&until=2026-11-05T00:00:00Z`, 400, "Invalid output date range"],
      [`/v1/sessions/output?workspace=${WS}&since=2026-11-06T00:00:00Z&until=2026-11-05T00:00:00Z`, 400, "Invalid output date range"],
      [`/v1/sessions/output?workspace=${WS}&since=2025-01-01T00:00:00Z&until=2026-11-05T00:00:00Z`, 400, "Invalid output date range"],
    ];
    for (const [path, status, message] of cases) {
      const r = await get(path);
      expect(r.status, path).toBe(status);
      if (message) expect((await body(r)).error.message, path).toBe(message);
    }
    expect((await get(`/v1/sessions/output?workspace=${WS}&${DAY}`, null)).status).toBe(401);
  });
  test("off the allowlist, then not configured: 501 with the reason, never a ClickHouse read", async () => {
    const off = await api(unreachable, { workspaces: new Set(["other"]) })(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect(off.status).toBe(501);
    expect((await body(off)).error.reason).toBe("output_workspace_not_enabled");
    const notConfigured = await api(unreachable, { output: false })(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect((await body(notConfigured)).error.reason).toBe("output_not_configured");
  });
  test("366 UTC days is the largest accepted range", async () => {
    const get = api(unreachable, { output: false });
    expect((await get(`/v1/sessions/output?workspace=${WS}&since=2025-11-06T00:00:00Z&until=2026-11-06T23:59:59Z`)).status).toBe(501);
    expect((await get(`/v1/sessions/output?workspace=${WS}&since=2025-11-05T00:00:00Z&until=2026-11-06T23:59:59Z`)).status).toBe(400);
  });
});

const suite = CH_URL ? describe : describe.skip;
suite("output route on ClickHouse", () => {
  let ch: ClickHouseClient & { drop(): Promise<void> };
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
    await reconcileOutputFacts(ch, OUTPUT_FIXTURE_SCOPE, outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, outputFixture(), 1), { maxRows: 10_000, insertBatch: 500 });
  }, 60_000);
  afterAll(async () => {
    await ch?.drop();
  }, 60_000);

  test("501 output_backfill_incomplete until the watermark exists, then the exact body", async () => {
    const get = api(ch, { workspaces: "*" });
    const before = await get(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect(before.status).toBe(501);
    expect((await body(before)).error.reason).toBe("output_backfill_incomplete");
    await ch.insert(OUTPUT_FACTS_STATE_TABLE, [{ org_id: ID.orgId, workspace_id: WS, complete_since: "2026-11-06 00:00:00.000000", marked_at: "2026-11-06 00:00:00.000", marked_by: "test", version: 1 }]);
    const r = await get(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe(WS);
    const b = await body(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "output", "since", "until"]);
    expect(b.since).toBe("2026-11-05T00:00:00.000Z");
    expect(b.output).toEqual({ agentMessages: 3, commits: 3, linesAdded: 13, linesRemoved: 4, commitsWithoutLineCounts: 0 });
  }, 60_000);

  test("a session whose facts pass was inexact is 501 output_session_partial; complete again serves", async () => {
    const get = api(ch, { workspaces: "*" });
    const state = (complete: number, version: number) => ch.insert(OUTPUT_FACTS_SESSION_STATE_TABLE, [{ org_id: ID.orgId, workspace_id: WS, source: "codex", session_id: "s1", complete, synced_at: "2026-11-06 00:00:00.000", version }]);
    await state(0, 1);
    const r = await get(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("output_session_partial");
    await state(1, 2);
    expect((await get(`/v1/sessions/output?workspace=${WS}&${DAY}`)).status).toBe(200);
  }, 60_000);

  test("a needs_full session in the workspace is 501 output_session_partial", async () => {
    // The vendored state table: the session has rows in this workspace's transcript_events.
    await ch.insert("transcript_events", [{
      org_id: ID.orgId, workspace_id: WS, session_id: "s1", source: "codex", machine_id: "m1", kind: "session_event", event_id: "x",
      ts: "2026-11-05 10:00:00.000", fingerprint: "f", version: 1,
    }]);
    await ch.insert(SESSION_STATE_TABLE, [{ org_id: ID.orgId, session_id: "s1", revision: 1, synced_at: "2026-11-06 00:00:00.000", full_synced_at: "1970-01-01 00:00:00.000", scanned_rows: 0, needs_full: 1 }]);
    const r = await api(ch, { workspaces: "*" })(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("output_session_partial");
  }, 60_000);
});
