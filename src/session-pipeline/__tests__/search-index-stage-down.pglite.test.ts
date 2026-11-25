/**
 * The 20261105062300 rollback's §8 rules (docs/handoffs/incremental-search-indexing.md): it refuses (55000, nothing
 * changed) while any search-index job, event or dead letter exists, and after the replay/resolve/archive path it
 * succeeds, restores the 20261031000100 stage CHECKs and keeps the non-empty audit archive with its seal.
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { StageMessage } from "@rhc/lib/pipeline/dlq.ts";
import { archiveSearchStage, claimSearchJobs, enqueueSearchWake, searchWorkKey, settleSearchJob } from "../search-jobs.ts";
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
  expect(await rel("sessions.search_stage_archive")).toBe("sessions.search_stage_archive");
  expect((await pg.query("SELECT 1 FROM sessions.search_stage_archive")).rows.length).toBe(2);
  expect((await pg.query("SELECT 1 FROM pg_trigger WHERE tgname = 'search_stage_archive_seal'")).rows.length).toBe(1);
  expect((await pg.query("SELECT to_regprocedure('sessions.archive_search_stage_v1()') IS NULL AS gone")).rows).toEqual([{ gone: true }]);
}, 120_000);
