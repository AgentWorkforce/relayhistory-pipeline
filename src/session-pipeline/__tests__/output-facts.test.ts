/**
 * Output facts producer (output-facts.ts) and the Output query (session-read-api/output-query.ts).
 *
 * Pure cases always run. With READ_API_TEST_CLICKHOUSE_URL (CI, or a local
 * clickhouse-server:26.8 container) the producer reconciles into a throwaway
 * database and the query answers from it.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createClickHouseClient, type ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { EVENT_FACTS_TABLE, OUTPUT_FACTS_TABLE, outputFactsSchemaStatements } from "../output-facts-schema.ts";
import {
  branchOf, chTime, commitMatches, eraseOutputFacts, normalizeStats, outputFactsFromScope, reconcileOutputFacts,
  type OutputFactsScope,
} from "../output-facts.ts";
import { OutputScopeError, readSessionOutput } from "../../session-read-api/output-query.ts";
import { LINK_SHA, OUTCOME_SHA, outputFixture, OUTPUT_FIXTURE_SCOPE } from "./output-facts-fixture.ts";

describe("normalizeStats (session-output.ts stat_entries + normalized_stats)", () => {
  test("array of numstat entries sums each side", () => {
    expect(normalizeStats([{ path: "a", additions: 3, deletions: 1 }, { path: "b", additions: "2", deletions: "0" }])).toEqual({ added: 5, removed: 1 });
  });
  test("single entry object (added/deleted keys)", () => {
    expect(normalizeStats({ added: "7", deleted: "2" })).toEqual({ added: 7, removed: 2 });
    expect(normalizeStats({ additions: 4, removed: 9 })).toEqual({ added: 4, removed: 9 });
  });
  test("object of entries", () => {
    expect(normalizeStats({ "a.ts": { additions: 1, deletions: 2 }, "b.ts": { additions: 3, deletions: 4 } })).toEqual({ added: 4, removed: 6 });
  });
  test("binary entry (path, both counts JSON null) and '-' count as zero, not unknown", () => {
    expect(normalizeStats([{ path: "img.png", additions: null, deletions: null }, { path: "x", additions: "-", deletions: "-" }, { path: "y", additions: 2, deletions: 1 }])).toEqual({ added: 2, removed: 1 });
  });
  test("a non-numeric side is NULL, independently of the other side", () => {
    expect(normalizeStats([{ path: "a", additions: "lots", deletions: 1 }])).toEqual({ added: null, removed: 1 });
  });
  test("SQL NULL, JSON null, scalars and an empty object are NULL; an empty array is 0", () => {
    for (const v of [undefined, null, 5, "x", {}]) expect(normalizeStats(v)).toEqual({ added: null, removed: null });
    expect(normalizeStats([])).toEqual({ added: 0, removed: 0 });
  });
  test("more than 12 digits is not a count", () => {
    expect(normalizeStats({ added: "1234567890123", deleted: "0" })).toEqual({ added: null, removed: 0 });
  });
});

describe("commitMatches (the commitOutput regex)", () => {
  test("needs Git's success header AND its shortstat", () => {
    expect(commitMatches("[main abc1234] fix\n 2 files changed, 5 insertions(+), 1 deletion(-)")).toEqual([{ sha: "abc1234", added: "5", deleted: "1" }]);
    expect(commitMatches("[main abc1234] fix")).toEqual([]);
    expect(commitMatches("I committed abc1234\n 2 files changed")).toEqual([]);
  });
  test("insertions or deletions may be absent; several commits in one result all match", () => {
    expect(commitMatches("[a 1111111] x\n 1 file changed, 3 deletions(-)\n[b 2222222] y\n 1 file changed, 2 insertions(+)")).toEqual([
      { sha: "1111111", added: null, deleted: "3" },
      { sha: "2222222", added: "2", deleted: null },
    ]);
  });
  test("upper-case hex is not a Git sha in this pattern", () => {
    expect(commitMatches("[main ABC1234] x\n 1 file changed")).toEqual([]);
  });
});

describe("branchOf (rollups.ts branchSql)", () => {
  test("NULL for {} and for an id with no @; text after the FIRST @ otherwise", () => {
    expect(branchOf({})).toBeNull();
    expect(branchOf({ id: "repo" })).toBeNull();
    expect(branchOf({ id: "repo@" })).toBe("");
    expect(branchOf({ id: "o/r@feat@x" })).toBe("feat@x");
  });
});

describe("chTime", () => {
  test("keeps microseconds and converts offsets to UTC", () => {
    expect(chTime("2026-11-05T23:59:59.123456Z")).toBe("2026-11-05 23:59:59.123456");
    expect(chTime("2026-11-06T01:00:00.5+02:00")).toBe("2026-11-05 23:00:00.500000");
  });
});

describe("outputFactsFromScope", () => {
  const facts = outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, outputFixture(), 1);
  const kinds = (k: string, origin?: string) => facts.facts.filter((f) => f.fact_kind === k && (origin === undefined || f.origin === origin));
  test("complete, and one event fact per projected event", () => {
    expect(facts.complete).toBe(true);
    expect(facts.events).toHaveLength(outputFixture().projectionRows.filter((r) => r.table === "convergence_events").length);
  });
  test("message qualification: assistant text with content; tool results, blank and non-text kinds excluded", () => {
    expect(kinds("message").map((f) => f.fact_key).sort()).toEqual(["msg-1", "msg-1", "msg-2", "req-9"].sort());
  });
  test("tool-result commits need one logical git-commit shell call", () => {
    expect(kinds("commit", "tool_result").map((f) => f.fact_key).sort()).toEqual(["abc1234", "def5678"]);
  });
  test("commit links, outcomes and legacy turns are emitted from their sources", () => {
    expect(kinds("commit", "commit_link").map((f) => f.fact_key)).toEqual([LINK_SHA]);
    expect(kinds("commit", "outcome").map((f) => f.fact_key)).toEqual([OUTCOME_SHA]);
    expect(kinds("outcome").map((f) => f.reverted)).toEqual([1]);
    expect(kinds("legacy_turn_message")).toHaveLength(1);
  });
  test("a legacy record naming another session makes the facts incomplete (never deletes)", () => {
    const fx = outputFixture();
    fx.records.push({ ...fx.records[0]!, record_id: "legacy:conversation_turns:t-x", payload: { ...fx.records[0]!.payload!, id: "t-x", session_id: "someone-else" } });
    const f = outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, fx, 1);
    expect(f.complete).toBe(false);
    expect(f.reasons).toContain("output_facts_legacy_invalid");
  });
  test("P1-c: a bad legacy or event timestamp marks the facts inexact and never throws", () => {
    const fx = outputFixture();
    const outcome = fx.records.find((r) => r.kind === "legacy_session_outcome")!;
    const { created_at: _c, ...noCreated } = outcome.payload!;
    fx.records = [{ ...outcome, payload: { ...noCreated, shipped_at: null } }];
    let f!: ReturnType<typeof outputFactsFromScope>;
    expect(() => (f = outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, fx, 1))).not.toThrow();
    expect(f.complete).toBe(false);
    const ev = outputFixture();
    ev.projectionRows[0] = { ...ev.projectionRows[0]!, row: { ...ev.projectionRows[0]!.row, ts: "not a time" } };
    expect(() => (f = outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, ev, 1))).not.toThrow();
    expect(f.complete).toBe(false);
    expect(f.reasons).toContain("output_facts_invalid_time");
  });
  test("the facts span covers every fact (events, links, outcomes, legacy turns)", () => {
    expect(outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, outputFixture(), 1).range).toEqual({ min: "2026-11-05 08:00:00.000000", max: "2026-11-05 12:00:00.000000" });
  });
  test("an incomplete scope read stays incomplete", () => {
    expect(outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, { ...outputFixture(), complete: false }, 1).complete).toBe(false);
  });
  test("row ids are stable across versions, so a re-pass changes nothing", () => {
    const again = outputFactsFromScope(OUTPUT_FIXTURE_SCOPE, outputFixture(), 2);
    expect(again.facts.map((f) => [f.row_id, f.fingerprint])).toEqual(facts.facts.map((f) => [f.row_id, f.fingerprint]));
    expect(again.events.map((f) => [f.row_id, f.fingerprint])).toEqual(facts.events.map((f) => [f.row_id, f.fingerprint]));
  });
});

const CH_URL = process.env.READ_API_TEST_CLICKHOUSE_URL;
const CH_USER = process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default";
const CH_PASSWORD = process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "";
const suite = CH_URL ? describe : describe.skip;

suite("output facts on ClickHouse: reconcile and query", () => {
  let admin: ClickHouseClient, ch: ClickHouseClient;
  const database = `output_facts_rt_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;
  const scope: OutputFactsScope = OUTPUT_FIXTURE_SCOPE;
  const limits = { maxRows: 10_000, insertBatch: 100 };
  const count = async (table: string) =>
    Number((await ch.select<{ n: string }>(`SELECT count() AS n FROM ${table} FINAL WHERE org_id = {o:String}`, { params: { o: scope.orgId } }))[0]!.n);
  beforeAll(async () => {
    admin = createClickHouseClient({ url: CH_URL!, user: CH_USER, password: CH_PASSWORD, database: "default" });
    await admin.command(`CREATE DATABASE ${database}`);
    ch = createClickHouseClient({ url: CH_URL!, user: CH_USER, password: CH_PASSWORD, database });
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
  }, 60_000);
  afterAll(async () => {
    await admin?.command(`DROP DATABASE IF EXISTS ${database}`);
  }, 60_000);

  test("first pass inserts every fact; a second identical pass inserts and deletes nothing", async () => {
    const f = outputFactsFromScope(scope, outputFixture(), 1);
    const first = await reconcileOutputFacts(ch, scope, f, limits);
    expect(first.inserted).toBe(f.events.length + f.facts.length);
    expect(first.deleted).toBe(0);
    const second = await reconcileOutputFacts(ch, scope, outputFactsFromScope(scope, outputFixture(), 2), limits);
    expect(second).toEqual({ inserted: 0, deleted: 0, complete: true, reasons: [] });
  }, 60_000);

  test("whole-workspace Output equals the relayhistory semantics for the fixture", async () => {
    const out = await readSessionOutput(ch, { orgId: scope.orgId, workspaceIds: [scope.workspaceId] }, {
      since: new Date("2026-11-05T00:00:00Z"), until: new Date("2026-11-05T23:59:59.999Z"),
    });
    // messages: msg-1, msg-2, req-9 (3 distinct); the legacy turn's session has messages -> not counted.
    // commits: abc1234 (tool result) resolves to the 40-char link sha -> one commit, max(5, link 10) added;
    //          def5678 (tool result, 2/0); the outcome 9999… shipped in the window (1/1).
    expect(out).toEqual({ agentMessages: 3, commits: 3, linesAdded: 10 + 2 + 1, linesRemoved: 3 + 0 + 1, commitsWithoutLineCounts: 0 });
  }, 60_000);

  test("a window excluding the day counts nothing; no window counts everything", async () => {
    const empty = await readSessionOutput(ch, { orgId: scope.orgId, workspaceIds: [scope.workspaceId] }, {
      since: new Date("2026-11-07T00:00:00Z"), until: new Date("2026-11-08T00:00:00Z"),
    });
    expect(empty).toEqual({ agentMessages: 0, commits: 0, linesAdded: 0, linesRemoved: 0, commitsWithoutLineCounts: 0 });
    const all = await readSessionOutput(ch, { orgId: scope.orgId, workspaceIds: [scope.workspaceId] }, { source: scope.harness, sessionId: scope.canonicalId });
    expect(all.commits).toBe(3);
  }, 60_000);

  test("tenant isolation: another org or workspace sees nothing; an empty workspace list throws", async () => {
    const other = await readSessionOutput(ch, { orgId: "org-other", workspaceIds: [scope.workspaceId] });
    expect(other.commits + other.agentMessages).toBe(0);
    const ws = await readSessionOutput(ch, { orgId: scope.orgId, workspaceIds: ["ws-other"] });
    expect(ws.commits + ws.agentMessages).toBe(0);
    await expect(readSessionOutput(ch, { orgId: scope.orgId, workspaceIds: [] })).rejects.toBeInstanceOf(OutputScopeError);
  }, 60_000);

  test("a retracted fact is deleted on a complete pass but kept on an incomplete one", async () => {
    const fx = outputFixture();
    fx.projectionRows = fx.projectionRows.filter((r) => r.table !== "session_commit_links");
    const partial = await reconcileOutputFacts(ch, scope, outputFactsFromScope(scope, { ...fx, complete: false }, 3), limits);
    expect(partial.deleted).toBe(0);
    const full = await reconcileOutputFacts(ch, scope, outputFactsFromScope(scope, fx, 3), limits);
    expect(full.deleted).toBe(1);
    const out = await readSessionOutput(ch, { orgId: scope.orgId, workspaceIds: [scope.workspaceId] });
    // abc1234 is now only the abbreviated tool-result sha with 5/3.
    expect(out.commits).toBe(3);
    expect(out.linesAdded).toBe(5 + 2 + 1);
  }, 60_000);

  test("erasure removes the scope from both tables", async () => {
    await eraseOutputFacts(ch, scope);
    expect(await count(EVENT_FACTS_TABLE)).toBe(0);
    expect(await count(OUTPUT_FACTS_TABLE)).toBe(0);
  }, 60_000);
});
