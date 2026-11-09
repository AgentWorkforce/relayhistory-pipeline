/**
 * Parity of the Postgres facts backends with the ClickHouse ones, for the same producer rows
 * (fixture -> outputFactsFromScope):
 *   - always (PGlite): the rows written through the consumer store and read back through the
 *     045040 readers give exactly what the pure aggregation gives over the producer's own rows
 *     (the store/reader round trip loses nothing: timestamps stay µs-exact, nulls stay null),
 *     over several windows including sub-second edges, and the same computeStatsHash;
 *     the whole-day values are the ones the ClickHouse route tests verify (output-route,
 *     digest-route);
 *   - with READ_API_TEST_CLICKHOUSE_URL: the same rows reconciled into ClickHouse, and
 *     readSessionOutput / buildDailyDigest / outputWindowReadiness there equal the Postgres
 *     backends field for field, with equal computeStatsHash.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { TENANT_SETTINGS_SQL, type SqlQuery, type TenantRunner } from "../liveness.ts";
import type { Liveness } from "../page.ts";
import type { Directory } from "../directory.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { outputFactsFromScope, reconcileOutputFacts } from "../../session-pipeline/output-facts.ts";
import { postgresOutputFactsStore } from "../../session-pipeline/output-facts-store.ts";
import { OUTPUT_FIXTURE_SCOPE, outputFixture } from "../../session-pipeline/__tests__/output-facts-fixture.ts";
import { outputFromFacts, postgresOutputBackend, type EventFact, type OutputFact, type PostgresFactsDeps } from "../postgres/output-query.ts";
import { dailyDigestFromFacts, postgresDigestBackend } from "../postgres/digest-query.ts";
import { buildDailyDigest, computeStatsHash } from "../clickhouse/digest-query.ts";
import { outputWindowReadiness, readSessionOutput } from "../clickhouse/output-query.ts";
import { CH_URL, freshClickHouse } from "./support.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { OUTPUT_FACTS_STATE_TABLE, outputFactsSchemaStatements } from "../../session-pipeline/output-facts-schema.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105045040_selfhost_output_facts.sql";
const SCOPE = OUTPUT_FIXTURE_SCOPE;
const ORG = SCOPE.orgId, WS = SCOPE.workspaceId;
const FACTS = outputFactsFromScope(SCOPE, outputFixture(), 1);
const directory: Directory = { list: async () => [], activity: async () => [] };
const live: Liveness = { erased: async () => new Set() } as unknown as Liveness;

const d = (iso: string) => new Date(iso);
/** Output windows (inclusive until): the day, open ends, and sub-second edges around the facts. */
const OUTPUT_WINDOWS: Array<{ since?: Date; until?: Date; source?: string; sessionId?: string }> = [
  {},
  { since: d("2026-11-05T00:00:00Z"), until: d("2026-11-06T00:00:00Z") },
  { since: d("2026-11-05T10:00:05Z"), until: d("2026-11-05T10:00:09Z") },
  { since: d("2026-11-05T10:00:08.001Z") },
  { until: d("2026-11-05T10:00:08Z") },
  { since: d("2026-11-05T09:00:00Z"), until: d("2026-11-05T09:00:00Z") },
  { since: d("2026-11-05T11:00:00Z"), until: d("2026-11-05T13:00:00Z") },
  { sessionId: "s1-canonical" },
  { sessionId: "s1" },
  { source: "codex", since: d("2026-11-05T10:00:03Z") },
];
/** Digest windows ([since, until)). */
const DIGEST_WINDOWS: Array<[string, string]> = [
  ["2026-11-05T00:00:00Z", "2026-11-06T00:00:00Z"],
  ["2026-11-05T10:00:00Z", "2026-11-05T10:00:08Z"],
  ["2026-11-05T10:00:05.001Z", "2026-11-05T10:00:15.001Z"],
  ["2026-11-05T10:00:15.001Z", "2026-11-05T11:00:00Z"],
  ["2026-11-04T00:00:00Z", "2026-11-05T00:00:00Z"],
];

let db: PGlite;
let deps: PostgresFactsDeps;

async function as<T>(role: string, begin: string, tenant: [string, string] | null, work: () => Promise<T>): Promise<T> {
  await db.exec(`SET ROLE ${role}`);
  try {
    await db.exec(begin);
    try {
      if (tenant) await db.query(TENANT_SETTINGS_SQL, tenant);
      const out = await work();
      await db.exec("COMMIT");
      return out;
    } catch (e) {
      await db.exec("ROLLBACK");
      throw e;
    }
  } finally {
    await db.exec("RESET ROLE");
  }
}

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    const path = `supabase/migrations/${file}`;
    await db.exec(read(path));
    if (path === MIGRATION) break;
  }
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime");
  await as("search_consumer_runtime", "BEGIN", null, async () => {
    const store = postgresOutputFactsStore(db as unknown as SqlExecutor);
    await store.reconcile(SCOPE, { eventFacts: FACTS.events, outputFacts: FACTS.facts, complete: FACTS.complete, reasons: FACTS.reasons }, 1);
    await store.recordState(SCOPE, { complete: true, at: d("2026-11-09T12:00:00Z"), range: FACTS.range ?? null, sourceVersion: 1 });
  });
  await db.query(`INSERT INTO sessions.output_facts_workspace_state (org_id, workspace_id, complete_since, marked_by) VALUES ($1, $2, '2026-11-01T00:00:00Z', 'test')`, [ORG, WS]);
  const run: TenantRunner = (orgId, workspaceId, work) => as("bulls96_read_api", "BEGIN READ ONLY", [orgId, workspaceId], () => work(db as unknown as SqlQuery));
  deps = { run, liveness: live };
}, 180_000);
afterAll(async () => {
  await db?.close();
});

/** The producer's rows, as the readers' row shape (a structural subset). */
const producerEvents = FACTS.events as unknown as EventFact[];
const producerOutputs = FACTS.facts as unknown as OutputFact[];
const inDigestWindow = (e: EventFact, since: Date, until: Date) => {
  const s = since.toISOString().replace("T", " ").replace("Z", "000"), u = until.toISOString().replace("T", " ").replace("Z", "000");
  return e.ts >= s && e.ts < u;
};

describe("postgres facts parity (store -> readers vs producer rows)", () => {
  test("the readers return every producer row, timestamps µs-exact", async () => {
    const rows = await deps.run(ORG, WS, (q) => q.query<{ ts: string; row_id: string }>(
      "SELECT to_char(f.ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS ts, f.row_id FROM sessions.output_facts_outputs_for($1, '-infinity', 'infinity', NULL, 10000) AS f ORDER BY f.row_id", [WS]));
    expect(rows.rows).toEqual(FACTS.facts.map((f) => ({ ts: f.ts, row_id: f.row_id })).sort((a, b) => (a.row_id < b.row_id ? -1 : 1)));
  });

  test("Output: the Postgres backend equals the aggregation over the producer rows, every window", async () => {
    const backend = postgresOutputBackend(deps);
    for (const w of OUTPUT_WINDOWS)
      expect([w, await backend.readOutput({ orgId: ORG, workspaceIds: [WS] }, w)]).toEqual([w, outputFromFacts(producerOutputs, w)]);
    // The ClickHouse-verified whole-day value (output-route.test.ts).
    expect(outputFromFacts(producerOutputs, OUTPUT_WINDOWS[1])).toEqual({ agentMessages: 3, commits: 3, linesAdded: 13, linesRemoved: 4, commitsWithoutLineCounts: 0 });
  });

  test("digest: the Postgres backend equals the aggregation over the producer rows, with the same stats hash", async () => {
    const backend = postgresDigestBackend(deps);
    for (const [s, u] of DIGEST_WINDOWS) {
      const p = { orgId: ORG, workspaceId: WS, since: d(s), until: d(u) };
      const got = await backend.buildDailyDigest(directory, p);
      const want = await dailyDigestFromFacts(directory, p, producerEvents.filter((e) => inDigestWindow(e, p.since, p.until)), producerOutputs);
      expect([s, got]).toEqual([s, want]);
      expect(await computeStatsHash(got, WS)).toBe(await computeStatsHash(want, WS));
    }
    // The ClickHouse-verified whole-day values (digest-route.test.ts).
    const day = await backend.buildDailyDigest(directory, { orgId: ORG, workspaceId: WS, since: d(DIGEST_WINDOWS[0]![0]), until: d(DIGEST_WINDOWS[0]![1]) });
    expect([day.stats.eventCount, day.stats.outcomes, day.sessions.map((x) => x.sessionId)]).toEqual([15, { committed: 0, reverted: 0, unknown: 0 }, ["s1-canonical"]]);
  });
});

(CH_URL ? describe : describe.skip)("postgres facts parity with ClickHouse (READ_API_TEST_CLICKHOUSE_URL)", () => {
  let ch: ClickHouseClient & { drop(): Promise<void> };
  beforeAll(async () => {
    ch = await freshClickHouse();
    for (const s of outputFactsSchemaStatements()) await ch.command(s);
    await reconcileOutputFacts(ch, SCOPE, FACTS, { maxRows: 10_000, insertBatch: 500 });
    await ch.insert(OUTPUT_FACTS_STATE_TABLE, [{ org_id: ORG, workspace_id: WS, complete_since: "2026-11-01 00:00:00.000000", marked_at: "2026-11-06 00:00:00.000", marked_by: "test", version: 1 }]);
  }, 60_000);
  afterAll(async () => {
    await ch?.drop();
  }, 60_000);

  test("Output, readiness, digest and computeStatsHash equal ClickHouse's", async () => {
    const out = postgresOutputBackend(deps);
    for (const w of OUTPUT_WINDOWS)
      expect([w, await out.readOutput({ orgId: ORG, workspaceIds: [WS] }, w)]).toEqual([w, await readSessionOutput(ch, { orgId: ORG, workspaceIds: [WS] }, w)]);
    const dig = postgresDigestBackend(deps);
    for (const [s, u] of DIGEST_WINDOWS) {
      const p = { orgId: ORG, workspaceId: WS, since: d(s), until: d(u) };
      expect([s, await out.windowReadiness(ORG, WS, p.since, p.until)]).toEqual([s, await outputWindowReadiness(ch, ORG, WS, p.since, p.until)]);
      expect([s, await dig.windowReadiness(ORG, WS, p.since, p.until, { untilExclusive: true })])
        .toEqual([s, await outputWindowReadiness(ch, ORG, WS, p.since, p.until, { untilExclusive: true })]);
      const pg = await dig.buildDailyDigest(directory, p);
      const chDigest = await buildDailyDigest(ch, directory, p);
      expect([s, pg]).toEqual([s, chDigest]);
      expect(await computeStatsHash(pg, WS)).toBe(await computeStatsHash(chDigest, WS));
    }
  }, 60_000);
});
