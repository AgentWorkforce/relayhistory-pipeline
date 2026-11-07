/**
 * GET /v1/digest/daily: resolveDayWindow's 400s, the allowlist and 501 gates before any
 * ClickHouse read, and (with READ_API_TEST_CLICKHOUSE_URL) the served body: relayhistory's
 * key order, the omitted-field header, and every narrative outcome as a 200.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { CH_URL, freshClickHouse, memoryLiveness } from "./support.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { OUTPUT_FACTS_STATE_TABLE, outputFactsSchemaStatements } from "../../session-pipeline/output-facts-schema.ts";
import { outputFactsFromScope, reconcileOutputFacts } from "../../session-pipeline/output-facts.ts";
import { outputFixture, OUTPUT_FIXTURE_SCOPE } from "../../session-pipeline/__tests__/output-facts-fixture.ts";
import type { DigestNarrativeDeps, NarrativeResult } from "../digest-narrative.ts";
import type { Directory } from "../directory.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: OUTPUT_FIXTURE_SCOPE.orgId, workspaceId: OUTPUT_FIXTURE_SCOPE.workspaceId, scopes: ["rth:read"] };
const WS = ID.workspaceId;
const unreachable: ClickHouseClient = {
  database: "x",
  async command() { throw new Error("no command"); },
  async insert() { throw new Error("no insert"); },
  async select() { throw new Error("not reached"); },
};
const directory: Directory = { list: async () => [], activity: async () => [] };

function api(ch: ClickHouseClient, over: { digest?: DigestNarrativeDeps | false; workspaces?: ReadonlySet<string> | "*" } = {}) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    directory,
    directoryWorkspaces: over.workspaces ?? new Set([WS]),
    ...(over.digest === false ? {} : { digest: over.digest ?? {} }),
  });
  return (path: string, token: string | null = "good") => app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;

describe("digest route: validation and gates", () => {
  test("resolveDayWindow's 400 messages; auth and workspace attestation first", async () => {
    const get = api(unreachable);
    const bad = await get(`/v1/digest/daily?workspace=${WS}&date=2026-02-30`);
    expect(bad.status).toBe(400);
    expect((await body(bad)).error.message).toBe("date is not a real calendar date: 2026-02-30");
    expect((await body(await get(`/v1/digest/daily?workspace=${WS}&tz=Mars/Olympus_Mons`))).error.message).toBe("tz is not a recognised IANA time zone: Mars/Olympus_Mons");
    expect((await get(`/v1/digest/daily?workspace=other&date=2026-11-05`)).status).toBe(403);
    expect((await get(`/v1/digest/daily?workspace=${WS}`, null)).status).toBe(401);
  });
  test("off the allowlist / not configured: 501 with the reason, never a ClickHouse read", async () => {
    expect((await body(await api(unreachable, { workspaces: new Set(["x"]) })(`/v1/digest/daily?workspace=${WS}&date=2026-11-05`))).error.reason).toBe("digest_workspace_not_enabled");
    expect((await body(await api(unreachable, { digest: false })(`/v1/digest/daily?workspace=${WS}&date=2026-11-05`))).error.reason).toBe("digest_not_configured");
  });
});

const suite = CH_URL ? describe : describe.skip;
suite("digest route on ClickHouse", () => {
  let ch: ClickHouseClient & { drop(): Promise<void> };
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
    await reconcileOutputFacts(ch, OUTPUT_FIXTURE_SCOPE, outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, outputFixture(), 1), { maxRows: 10_000, insertBatch: 500 });
  }, 60_000);
  afterAll(async () => {
    await ch?.drop();
  }, 60_000);

  test("501 digest_backfill_incomplete until the watermark, then the body in relayhistory's key order", async () => {
    const generated: NarrativeResult = { text: "Fixed the parser.", model: "m", generatedAt: "2026-11-06T00:00:00.000Z", cached: false, unavailable: null };
    const get = api(ch, { workspaces: "*", digest: { generator: { generate: async () => generated } } });
    const before = await get(`/v1/digest/daily?workspace=${WS}&date=2026-11-05`);
    expect((await body(before)).error.reason).toBe("digest_backfill_incomplete");
    await ch.insert(OUTPUT_FACTS_STATE_TABLE, [{ org_id: ID.orgId, workspace_id: WS, complete_since: "2026-11-01 00:00:00.000000", marked_at: "2026-11-06 00:00:00.000", marked_by: "test", version: 1 }]);
    const r = await get(`/v1/digest/daily?workspace=${WS}&date=2026-11-05`);
    expect(r.status).toBe(200);
    expect(r.headers.get("x-sessions-digest-omitted")).toBe("outcomes.reverted");
    expect(r.headers.get("x-relayhistory-workspace-id")).toBe(WS);
    const b = await body(r);
    expect(Object.keys(b)).toEqual(["date", "tz", "window", "scope", "stats", "projects", "epics", "sessions", "narrative", "correlationId"]);
    expect(b.window).toEqual({ since: "2026-11-05T00:00:00.000Z", until: "2026-11-06T00:00:00.000Z" });
    expect(b.stats.outcomes.reverted).toBe(0);
    expect(b.stats.eventCount).toBe(15);
    expect(b.narrative).toEqual(generated);
    expect(b.sessions.map((s: any) => s.sessionId)).toEqual(["s1"]);
  }, 60_000);

  test("a quiet day is no_activity; a failing Worker is provider_error; both are 200s", async () => {
    const quiet = await api(ch, { workspaces: "*" })(`/v1/digest/daily?workspace=${WS}&date=2026-11-03`);
    expect(quiet.status).toBe(200);
    expect((await body(quiet)).narrative.unavailable).toBe("no_activity");
    const failing = await api(ch, { workspaces: "*", digest: { generator: { generate: async () => { throw new Error("binding"); } } } })(`/v1/digest/daily?workspace=${WS}&date=2026-11-05`);
    expect(failing.status).toBe(200);
    expect((await body(failing)).narrative.unavailable).toBe("provider_error");
  }, 60_000);
});
