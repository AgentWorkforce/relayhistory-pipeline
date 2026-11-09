/**
 * The Postgres Output / digest backends (READ_API_OUTPUT_BACKEND / READ_API_DIGEST_BACKEND =
 * postgres) on PGlite with every migration through 20261105045040: facts written by the
 * consumer store from the shared fixture, read back as bulls96_read_api through the 045040
 * readers only, inside a READ ONLY tenant transaction (the read pool's poolTenantRunner shape).
 * Covers each readiness code in order, the Output and digest values (the ClickHouse-verified
 * fixture numbers), paging, liveness, the deadline, the read role's grants, and schema-missing.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { TENANT_SETTINGS_SQL, type SqlQuery, type TenantRunner } from "../liveness.ts";
import { keyOf, type Liveness } from "../page.ts";
import type { Directory } from "../directory.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { outputFactsFromScope } from "../../session-pipeline/output-facts.ts";
import { postgresOutputFactsStore } from "../../session-pipeline/output-facts-store.ts";
import { OUTPUT_FIXTURE_SCOPE, outputFixture } from "../../session-pipeline/__tests__/output-facts-fixture.ts";
import { FactsReadDeadline, isPostgresFactsSchemaMissing, postgresOutputBackend, type PostgresFactsDeps } from "../postgres/output-query.ts";
import { postgresDigestBackend } from "../postgres/digest-query.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105045040_selfhost_output_facts.sql";
const TABLES = ["session_event_facts", "session_output_facts", "output_facts_session_state", "output_facts_workspace_state"];
const SCOPE = OUTPUT_FIXTURE_SCOPE;
const ORG = SCOPE.orgId, WS = SCOPE.workspaceId;
const AT = new Date("2026-11-09T12:00:00Z");
const DAY = { since: new Date("2026-11-05T00:00:00Z"), until: new Date("2026-11-06T00:00:00Z") };
const OUTPUT = { agentMessages: 3, commits: 3, linesAdded: 13, linesRemoved: 4, commitsWithoutLineCounts: 0 };
const ZERO = { agentMessages: 0, commits: 0, linesAdded: 0, linesRemoved: 0, commitsWithoutLineCounts: 0 };

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    const path = `supabase/migrations/${file}`;
    await db.exec(read(path));
    if (path === MIGRATION) break;
  }
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime");
  return db;
}

const code = async (p: Promise<unknown>) => ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

async function as<T>(db: PGlite, role: string, begin: string, tenant: [string, string] | null, work: () => Promise<T>): Promise<T> {
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

/** poolTenantRunner's shape: bulls96_read_api, READ ONLY, the tenant GUCs, then the work. */
const runner = (db: PGlite): TenantRunner => (orgId, workspaceId, work) =>
  as(db, "bulls96_read_api", "BEGIN READ ONLY", [orgId, workspaceId], () => work(db as unknown as SqlQuery));
const noneErased: Liveness = { erased: async () => new Set() } as unknown as Liveness;
const erasing = (...keys: Array<{ source: string; sessionId: string }>): Liveness =>
  ({ erased: async () => new Set(keys.map(keyOf)) }) as unknown as Liveness;
const deps = (db: PGlite, over: Partial<PostgresFactsDeps> = {}): PostgresFactsDeps => ({ run: runner(db), liveness: noneErased, ...over });
const directory: Directory = { list: async () => [], activity: async () => [] };

const consumer = <T>(db: PGlite, work: (store: ReturnType<typeof postgresOutputFactsStore>) => Promise<T>) =>
  as(db, "search_consumer_runtime", "BEGIN", null, () => work(postgresOutputFactsStore(db as unknown as SqlExecutor)));
const facts = () => outputFactsFromScope(SCOPE, outputFixture(), 1);

async function writeFacts(db: PGlite, complete = true, version = 1): Promise<void> {
  const f = facts();
  await consumer(db, async (s) => {
    await s.reconcile(SCOPE, { eventFacts: f.events, outputFacts: f.facts, complete: f.complete, reasons: f.reasons }, version);
    expect(await s.recordState(SCOPE, { complete, at: AT, range: f.range ?? null, sourceVersion: version })).toBe(true);
  });
}
/** The workspace watermark: no store writer exists (the PR4 bootstrap writes it), so as the owner. */
const watermark = (db: PGlite, since: string) =>
  db.query(`INSERT INTO sessions.output_facts_workspace_state (org_id, workspace_id, complete_since, marked_by) VALUES ($1, $2, $3, 'test')
            ON CONFLICT (org_id, workspace_id) DO UPDATE SET complete_since = excluded.complete_since`, [ORG, WS, since]);

describe("postgres output/digest backends over 045040", () => {
  test("readiness codes in the ClickHouse order, for both backends and both window kinds", async () => {
    const db = await database();
    const out = postgresOutputBackend(deps(db));
    const dig = postgresDigestBackend(deps(db));
    const both = async () => [
      await out.windowReadiness(ORG, WS, DAY.since, DAY.until),
      await dig.windowReadiness(ORG, WS, DAY.since, DAY.until, { untilExclusive: true }),
    ];
    expect(await both()).toEqual(["backfill_incomplete", "backfill_incomplete"]);
    await writeFacts(db, false);
    expect(await both()).toEqual(["backfill_incomplete", "backfill_incomplete"]);
    // One microsecond after `since`: before_watermark (µs compare); at `since`: past it.
    await watermark(db, "2026-11-05T00:00:00.000001Z");
    expect(await both()).toEqual(["before_watermark", "before_watermark"]);
    await watermark(db, "2026-11-05T00:00:00Z");
    expect(await both()).toEqual(["session_partial", "session_partial"]);
    await writeFacts(db, true, 2);
    expect(await both()).toEqual(["ready", "ready"]);
    // A session with facts and no state row overlapping the window: session_unindexed.
    await db.query("DELETE FROM sessions.output_facts_session_state WHERE org_id = $1 AND workspace_id = $2", [ORG, WS]);
    expect(await both()).toEqual(["session_unindexed", "session_unindexed"]);
    // A quiet window (no facts, no state rows) after the watermark is ready.
    const quiet = [new Date("2026-11-07T00:00:00Z"), new Date("2026-11-08T00:00:00Z")] as const;
    expect(await out.windowReadiness(ORG, WS, ...quiet)).toBe("ready");
    // Another workspace of the same org has no watermark.
    expect(await out.windowReadiness(ORG, "ws-other", DAY.since, DAY.until)).toBe("backfill_incomplete");
  }, 180_000);

  test("Output and digest values equal the ClickHouse-verified fixture numbers; paging is transparent", async () => {
    const db = await database();
    await writeFacts(db);
    await watermark(db, "2026-11-01T00:00:00Z");
    const scope = { orgId: ORG, workspaceIds: [WS] };
    expect(await postgresOutputBackend(deps(db)).readOutput(scope, DAY)).toEqual(OUTPUT);
    expect(await postgresOutputBackend(deps(db)).readOutput(scope, {})).toEqual(OUTPUT);
    expect(await postgresOutputBackend(deps(db, { pageLimit: 2 })).readOutput(scope, DAY)).toEqual(OUTPUT);
    expect(await postgresOutputBackend(deps(db)).readOutput(scope, { since: new Date("2026-11-06T00:00:00Z") })).toEqual(ZERO);
    expect(await postgresOutputBackend(deps(db)).readOutput(scope, { source: "claude", ...DAY })).toEqual(ZERO);

    const params = { orgId: ORG, workspaceId: WS, ...DAY };
    const digest = await postgresDigestBackend(deps(db)).buildDailyDigest(directory, params);
    expect(digest.stats.eventCount).toBe(15);
    expect(digest.stats.outcomes).toEqual({ committed: 0, reverted: 0, unknown: 0 });
    expect(digest.sessions.map((s) => [s.source, s.sessionId])).toEqual([["codex", "s1-canonical"]]);
    expect(await postgresDigestBackend(deps(db, { pageLimit: 1 })).buildDailyDigest(directory, params)).toEqual(digest);
    const nextDay = await postgresDigestBackend(deps(db)).buildDailyDigest(directory, { ...params, since: DAY.until, until: new Date("2026-11-07T00:00:00Z") });
    expect([nextDay.stats.eventCount, nextDay.sessions]).toEqual([0, []]);
    // Another tenant's read sees none of these facts.
    expect(await postgresOutputBackend(deps(db)).readOutput({ orgId: ORG, workspaceIds: ["ws-other"] }, DAY)).toEqual(ZERO);
  }, 180_000);

  test("liveness: an erased session's facts are dropped before any aggregation", async () => {
    const db = await database();
    await writeFacts(db);
    await watermark(db, "2026-11-01T00:00:00Z");
    const gone = erasing({ source: SCOPE.harness, sessionId: SCOPE.canonicalId });
    expect(await postgresOutputBackend(deps(db, { liveness: gone })).readOutput({ orgId: ORG, workspaceIds: [WS] }, DAY)).toEqual(ZERO);
    const digest = await postgresDigestBackend(deps(db, { liveness: gone })).buildDailyDigest(directory, { orgId: ORG, workspaceId: WS, ...DAY });
    expect([digest.stats.eventCount, digest.stats.sessionCount, digest.sessions, digest.projects, digest.epics]).toEqual([0, 0, [], [], []]);
    // Erasing an unrelated session changes nothing.
    const other = erasing({ source: SCOPE.harness, sessionId: "unrelated" });
    expect(await postgresOutputBackend(deps(db, { liveness: other })).readOutput({ orgId: ORG, workspaceIds: [WS] }, DAY)).toEqual(OUTPUT);
  }, 180_000);

  test("the deadline is checked before each page: FactsReadDeadline, never a partial answer", async () => {
    const db = await database();
    await writeFacts(db);
    await watermark(db, "2026-11-01T00:00:00Z");
    let t = 0;
    const slow = () => (t += 1_000);
    await expect(postgresOutputBackend(deps(db, { now: slow })).readOutput({ orgId: ORG, workspaceIds: [WS] }, DAY, { timeoutMs: 500 }))
      .rejects.toBeInstanceOf(FactsReadDeadline);
    await expect(postgresDigestBackend(deps(db, { now: slow })).buildDailyDigest(directory, { orgId: ORG, workspaceId: WS, ...DAY }, { timeoutMs: 500 }))
      .rejects.toBeInstanceOf(FactsReadDeadline);
    await expect(postgresOutputBackend(deps(db, { now: slow })).windowReadiness(ORG, WS, DAY.since, DAY.until, { timeoutMs: 500 }))
      .rejects.toBeInstanceOf(FactsReadDeadline);
  }, 180_000);

  test("the read role executes the readers and cannot touch the tables; a missing reader is schema-missing", async () => {
    const db = await database();
    await writeFacts(db);
    const tenant: [string, string] = [ORG, WS];
    for (const t of TABLES) {
      expect(await code(as(db, "bulls96_read_api", "BEGIN", tenant, () => db.query(`SELECT 1 FROM sessions.${t}`)))).toBe("42501");
      expect(await code(as(db, "bulls96_read_api", "BEGIN", tenant, () => db.query(`DELETE FROM sessions.${t}`)))).toBe("42501");
      expect(await code(as(db, "bulls96_read_api", "BEGIN", tenant, () => db.query(`UPDATE sessions.${t} SET org_id = org_id`)))).toBe("42501");
    }
    expect(await code(as(db, "bulls96_read_api", "BEGIN", tenant, () =>
      db.query(`INSERT INTO sessions.output_facts_workspace_state (org_id, workspace_id, complete_since, marked_by) VALUES ($1, $2, now(), 'x')`, tenant)))).toBe("42501");
    // The writers are the consumer's alone.
    expect(await code(as(db, "bulls96_read_api", "BEGIN", tenant, () => postgresOutputFactsStore(db as unknown as SqlExecutor).erase(SCOPE)))).toBe("42501");
    expect(await postgresOutputBackend(deps(db)).readOutput({ orgId: ORG, workspaceIds: [WS] }, DAY)).toEqual(OUTPUT);

    await db.exec("DROP FUNCTION sessions.output_facts_coverage_for");
    const err = await postgresOutputBackend(deps(db)).windowReadiness(ORG, WS, DAY.since, DAY.until).then(() => null, (e: unknown) => e);
    expect((err as { code?: string }).code).toBe("42883");
    expect(postgresOutputBackend(deps(db)).isSchemaMissing(err)).toBe(true);
    expect(isPostgresFactsSchemaMissing({ code: "42501" })).toBe(false);
  }, 180_000);
});
