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
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../../session-pipeline/vendor/search-index/schema.ts";

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
  const get = () => api(ch, { workspaces: "*" });
  const reason = async (path = `/v1/sessions/output?workspace=${WS}&${DAY}`) => {
    const r = await get()(path);
    return r.status === 200 ? 200 : (await body(r)).error.reason;
  };
  const watermark = (since: string, version: number) =>
    ch.insert(OUTPUT_FACTS_STATE_TABLE, [{ org_id: ID.orgId, workspace_id: WS, complete_since: since, marked_at: "2026-11-06 00:00:00.000", marked_by: "test", version }]);
  const state = (session: string, complete: number, version: number, min: string | null, max: string | null) =>
    ch.insert(OUTPUT_FACTS_SESSION_STATE_TABLE, [{ org_id: ID.orgId, workspace_id: WS, source: "codex", session_id: session, complete, synced_at: "2026-11-06 00:00:00.000", min_ts: min, max_ts: max, version }]);
  const transcript = (session: string, ts: string) =>
    ch.insert(EVENTS_TABLE, [{ org_id: ID.orgId, workspace_id: WS, session_id: session, source: "codex", machine_id: "m1", kind: "session_event", event_id: `x-${session}-${ts}`, ts, fingerprint: "f", version: 1 }]);
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
    await reconcileOutputFacts(ch, OUTPUT_FIXTURE_SCOPE, outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, outputFixture(), 1), { maxRows: 10_000, insertBatch: 500 });
    await state("s1", 1, 1, "2026-11-05 08:00:00.000000", "2026-11-05 12:00:00.000000");
    await transcript("s1", "2026-11-05 10:00:00.000");
  }, 60_000);
  afterAll(async () => {
    await ch?.drop();
  }, 60_000);

  test("watermark: none = backfill_incomplete; later than since = before_watermark; at or before since serves", async () => {
    expect(await reason()).toBe("output_backfill_incomplete");
    await watermark("2026-11-05 00:00:00.000001", 1);
    expect(await reason()).toBe("output_before_watermark");
    await watermark("2026-11-01 00:00:00.000000", 2);
    const r = await get()(`/v1/sessions/output?workspace=${WS}&${DAY}`);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe(WS);
    const b = await body(r);
    expect(Object.keys(b).sort()).toEqual(["correlationId", "output", "since", "until"]);
    expect(b.output).toEqual({ agentMessages: 3, commits: 3, linesAdded: 13, linesRemoved: 4, commitsWithoutLineCounts: 0 });
  }, 60_000);

  test("P1-a: a session with transcript in the window, needs_full=0, and NO facts-state row is 501, never omitted", async () => {
    await transcript("s-unindexed", "2026-11-05 15:00:00.000");
    await ch.insert(SESSION_STATE_TABLE, [{ org_id: ID.orgId, session_id: "s-unindexed", revision: 1, synced_at: "2026-11-06 00:00:00.000", full_synced_at: "2026-11-06 00:00:00.000", scanned_rows: 1, needs_full: 0 }]);
    expect(await reason()).toBe("output_session_unindexed");
    // Outside that session's transcript, the window is unaffected.
    expect(await reason(`/v1/sessions/output?workspace=${WS}&since=2026-11-05T09:00:00Z&until=2026-11-05T11:00:00Z`)).toBe(200);
    await state("s-unindexed", 1, 1, "2026-11-05 15:00:00.000000", "2026-11-05 15:00:00.000000");
    expect(await reason()).toBe(200);
  }, 60_000);

  test("P1-b: an inexact session blocks only windows its facts span overlaps; an unknown span blocks all", async () => {
    await state("s-old", 0, 1, "2026-10-01 00:00:00.000000", "2026-10-02 00:00:00.000000");
    expect(await reason()).toBe(200);
    expect(await reason(`/v1/sessions/output?workspace=${WS}&since=2026-11-01T12:00:00Z&until=2026-11-05T23:59:59Z`)).toBe(200);
    await state("s-old", 1, 2, "2026-10-01 00:00:00.000000", "2026-10-02 00:00:00.000000");
    await watermark("2026-09-01 00:00:00.000000", 3);
    await state("s-old2", 0, 1, "2026-10-01 00:00:00.000000", "2026-10-02 00:00:00.000000");
    expect(await reason(`/v1/sessions/output?workspace=${WS}&since=2026-10-01T12:00:00Z&until=2026-11-05T23:59:59Z`)).toBe("output_session_partial");
    expect(await reason()).toBe(200);
    await state("s-unknown", 0, 1, null, null);
    expect(await reason()).toBe("output_session_partial");
    await state("s-unknown", 1, 2, null, null);
    await state("s-old2", 1, 2, "2026-10-01 00:00:00.000000", "2026-10-02 00:00:00.000000");
    expect(await reason()).toBe(200);
  }, 60_000);

  test("P2 (digest window): an exclusive window ignores a transcript row exactly at `until`; inclusive does not", async () => {
    const { outputWindowReadiness } = await import("../output-query.ts");
    await transcript("s-edge", "2026-11-06 00:00:00.000");
    const since = new Date("2026-11-05T00:00:00Z"), until = new Date("2026-11-06T00:00:00Z");
    expect(await outputWindowReadiness(ch, ID.orgId, WS, since, until, { untilExclusive: true })).toBe("ready");
    expect(await outputWindowReadiness(ch, ID.orgId, WS, since, until)).toBe("session_unindexed");
    await state("s-edge", 1, 1, "2026-11-06 00:00:00.000000", "2026-11-06 00:00:00.000000");
  }, 60_000);
});

(CH_URL ? describe : describe.skip)("output route without the output-facts DDL", () => {
  test("a missing table or column is 501 output_schema_missing, never a 500", async () => {
    const bare = await freshClickHouse();
    try {
      const r = await api(bare, { workspaces: "*" })(`/v1/sessions/output?workspace=${WS}&${DAY}`);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe("output_schema_missing");
    } finally {
      await bare.drop();
    }
  }, 60_000);
});
