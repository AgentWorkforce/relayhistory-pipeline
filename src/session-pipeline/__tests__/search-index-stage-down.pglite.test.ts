/**
 * The 20261105062300 rollback's §8 rules (docs/handoffs/incremental-search-indexing.md): it refuses (55000, nothing
 * changed) while any search-index job, event or dead letter exists, and after the replay/resolve/archive path it
 * succeeds, restores the 20261031000100 stage CHECKs and keeps the audit archive with its seal, empty or not (§8).
 * Re-applying the slot afterwards (the real migrator) re-adopts the retained archive, rows intact, and refuses one
 * whose shape is not the stage's.
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { StageMessage } from "@rhc/lib/pipeline/dlq.ts";
import { archiveSearchStage, claimSearchJobs, enqueueSearchWake, searchWorkKey, settleSearchJob } from "../search-jobs.ts";
import { runMigrations } from "../../hoopdb/migrate.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

const DOWN = readFileSync(join(import.meta.dir, "../../../supabase/rollbacks/20261105062300_search_index_stage.down.sql"), "utf8");
const STEP = "upstream:20261105062300_search_index_stage.sql";
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0)) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

const scope = { orgId: "org_down", workspaceId: "ws_down", harness: "claude", sessionId: "s_down" };
const wake = (epoch: number): StageMessage => ({
  v: 1, stage: "search-index", ctx: { traceId: crypto.randomUUID(), ...scope }, workKey: searchWorkKey(scope),
  payload: { epoch, clock: { source: "pointer", at: new Date().toISOString() } },
});

async function db() {
  const { pg } = await clonedReplayDatabase();
  cleanup.push(() => pg.close());
  return pg;
}
/** The real migrator over the clone, as replay-fixture.ts runs it. */
async function migrate(pg: PGlite) {
  const executor = { async query<T>(sql: string, params?: unknown[]) {
    if (params?.length) return pg.query<T>(sql, params);
    const r = await pg.exec(sql);
    return { rows: (r.at(-1)?.rows ?? []) as T[] };
  } };
  return runMigrations(executor, { profile: { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" } });
}
const archiveObjects = async (pg: PGlite) => ({
  table: (await pg.query<{ r: string | null }>("SELECT to_regclass('sessions.search_stage_archive')::text AS r")).rows[0]!.r,
  index: (await pg.query<{ r: string | null }>("SELECT to_regclass('sessions.search_stage_archive_one_manifest')::text AS r")).rows[0]!.r,
  seal: (await pg.query<{ r: string | null }>("SELECT to_regprocedure('sessions.search_stage_archive_seal_v1()')::text AS r")).rows[0]!.r,
  triggers: (await pg.query<{ t: string }>("SELECT tgname AS t FROM pg_trigger WHERE tgrelid = to_regclass('sessions.search_stage_archive') AND NOT tgisinternal ORDER BY tgname")).rows.map((r) => r.t),
});
const KEPT = { table: "sessions.search_stage_archive", index: "sessions.search_stage_archive_one_manifest", seal: "sessions.search_stage_archive_seal_v1()", triggers: ["search_stage_archive_seal", "search_stage_archive_seal_truncate"] };
const down = async (pg: PGlite) => pg.exec(DOWN).then(() => null, async (e: { code?: string; message?: string }) => {
  await pg.exec("ROLLBACK").catch(() => {});
  return { code: e.code, message: String(e.message) };
});
const checks = async (pg: PGlite) => (await pg.query<{ def: string }>(
  `SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
    WHERE conname IN ('pipeline_jobs_stage_check','pipeline_events_stage_check','pipeline_dead_letters_stage_check') AND conrelid IN
      ('sessions.pipeline_jobs'::regclass,'sessions.pipeline_events'::regclass,'sessions.pipeline_dead_letters'::regclass) ORDER BY conname`)).rows.map((r) => r.def);
const ledgered = async (pg: PGlite) => (await pg.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name = $1", [STEP])).rows.length === 1;

test("refuses with a search-index job, then with its letter and events; changes nothing", async () => {
  const pg = await db();
  await enqueueSearchWake(pg, wake(3));
  const before = await checks(pg);
  expect(before.every((d) => d.includes("'search-index'"))).toBe(true);
  expect(await down(pg)).toMatchObject({ code: "55000", message: expect.stringContaining("search-index jobs exist") });
  expect(await checks(pg)).toEqual(before);
  expect(await ledgered(pg)).toBe(true);
  // Exhaust it: the job goes, a letter and a dead_lettered event stay.
  const [c] = await claimSearchJobs(pg, 1);
  expect(await settleSearchJob(pg, c!, "exhausted")).toMatchObject({ applied: true });
  expect(await down(pg)).toMatchObject({ code: "55000", message: expect.stringContaining("dead letters exist") });
  await pg.query("UPDATE sessions.pipeline_dead_letters SET state = 'resolved', resolved_at = now() WHERE stage = 'search-index'");
  await pg.query("DELETE FROM sessions.pipeline_dead_letters WHERE stage = 'search-index'");
  expect(await down(pg)).toMatchObject({ code: "55000", message: expect.stringContaining("events exist") });
  expect(await ledgered(pg)).toBe(true);
}, 120_000);

test("after the archive it rolls back: CHECKs restored, stage objects gone, the non-empty archive and its seal kept", async () => {
  const pg = await db();
  await enqueueSearchWake(pg, wake(5));
  const [c] = await claimSearchJobs(pg, 1);
  await settleSearchJob(pg, c!, "done");
  expect(await archiveSearchStage(pg)).toMatchObject({ letters: 0, events: 1 });
  expect(await down(pg)).toBeNull();
  expect(await ledgered(pg)).toBe(false);
  expect((await checks(pg)).some((d) => d.includes("search-index"))).toBe(false);
  const rel = async (r: string) => (await pg.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [r])).rows[0]!.r;
  expect(await rel("sessions.search_catchup_cursor")).toBeNull();
  expect(await rel("sessions.pipeline_jobs_search_index_scope")).toBeNull();
  expect(await archiveObjects(pg)).toEqual(KEPT);
  expect((await pg.query("SELECT 1 FROM sessions.search_stage_archive")).rows.length).toBe(2);
  expect((await pg.query("SELECT to_regprocedure('sessions.archive_search_stage_v1()') IS NULL AS gone")).rows).toEqual([{ gone: true }]);
}, 120_000);

test("an unused stage rolls back too: the EMPTY archive and its seal are never dropped", async () => {
  const pg = await db();
  expect((await pg.query("SELECT 1 FROM sessions.search_stage_archive")).rows).toEqual([]);
  expect(await down(pg)).toBeNull();
  expect(await ledgered(pg)).toBe(false);
  expect(await archiveObjects(pg)).toEqual(KEPT);
}, 120_000);

test("re-applying the slot after the down (the real migrator) re-adopts the retained archive: rows intact, sealed, archivable again", async () => {
  const pg = await db();
  await enqueueSearchWake(pg, wake(5));
  const [c] = await claimSearchJobs(pg, 1);
  await settleSearchJob(pg, c!, "done");
  await archiveSearchStage(pg);
  const before = (await pg.query("SELECT batch_id::text, kind, source_id, row FROM sessions.search_stage_archive ORDER BY 1, 2, 3")).rows;
  expect(before).toHaveLength(2);
  expect(await down(pg)).toBeNull();
  await migrate(pg);
  expect(await ledgered(pg)).toBe(true);
  expect(await archiveObjects(pg)).toEqual(KEPT);
  expect((await pg.query("SELECT batch_id::text, kind, source_id, row FROM sessions.search_stage_archive ORDER BY 1, 2, 3")).rows).toEqual(before);
  expect((await checks(pg)).every((d) => d.includes("'search-index'"))).toBe(true);
  // The stage works again, and its archive appends a second batch beside the retained one.
  await enqueueSearchWake(pg, wake(6));
  const [again] = await claimSearchJobs(pg, 1);
  await settleSearchJob(pg, again!, "done");
  expect(await archiveSearchStage(pg)).toMatchObject({ letters: 0, events: 1 });
  expect((await pg.query("SELECT DISTINCT batch_id FROM sessions.search_stage_archive")).rows).toHaveLength(2);
}, 180_000);

test("a retained archive whose shape is not the stage's refuses the re-apply (55000) and leaves the slot unledgered", async () => {
  const pg = await db();
  expect(await down(pg)).toBeNull();
  await pg.exec("ALTER TABLE sessions.search_stage_archive ADD COLUMN forged text");
  await expect(migrate(pg)).rejects.toThrow(/not the stage's archive/);
  await pg.exec("ROLLBACK").catch(() => {});
  expect(await ledgered(pg)).toBe(false);
  expect((await checks(pg)).some((d) => d.includes("search-index"))).toBe(false);
}, 180_000);
