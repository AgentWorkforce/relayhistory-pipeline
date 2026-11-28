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
// 20261105062320 (the per-org admission hold) sits above 062300 and its down refuses while it is applied: roll it back
// first (it has no hold installed here), so 062300 is newest whenever its own down is exercised.
const HOLD_DOWN = readFileSync(join(import.meta.dir, "../../../supabase/rollbacks/20261105062320_ingest_org_admission_fences.down.sql"), "utf8");
const HISTORY_DOWN = readFileSync(join(import.meta.dir, "../../../supabase/rollbacks/20261105062340_ingest_org_erasure_history.down.sql"), "utf8");
const CUSTOM_PLANS_DOWN = readFileSync(join(import.meta.dir, "../../../supabase/rollbacks/20261105062330_buffered_search_feed_custom_plans.down.sql"), "utf8");
// 20261105062500 (the brief summary v2 read) is newest: its down runs first.
const BRIEF_SUMMARY_V2_DOWN = readFileSync(join(import.meta.dir, "../../../supabase/rollbacks/20261105062500_session_brief_summary_v2_read.down.sql"), "utf8");
// 20261105062350 (the buffered feed page keyset) sits above 062330: its down runs next.
const PAGE_KEYSET_DOWN = readFileSync(join(import.meta.dir, "../../../supabase/rollbacks/20261105062350_buffered_search_feed_page_keyset.down.sql"), "utf8");
const holdOff = async (pg: PGlite) => {
  // 20261105062500 (the brief summary v2 read) is rolled back first, then 20261105062350 (the feed page keyset),
  // then 20261105062340 (the erasure history).
  const briefV2 = (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105062500_session_brief_summary_v2_read.sql'")).rows[0]!.n;
  if (briefV2) await pg.exec(BRIEF_SUMMARY_V2_DOWN);
  const keyset = (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105062350_buffered_search_feed_page_keyset.sql'")).rows[0]!.n;
  if (keyset) await pg.exec(PAGE_KEYSET_DOWN);
  const history = (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105062340_ingest_org_erasure_history.sql'")).rows[0]!.n;
  if (history) await pg.exec(HISTORY_DOWN);
  // 20261105062330 (the buffered feed custom plans) sits above 062320 and comes off next.
  const plans = (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105062330_buffered_search_feed_custom_plans.sql'")).rows[0]!.n;
  if (plans) await pg.exec(CUSTOM_PLANS_DOWN);
  const applied = (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105062320_ingest_org_admission_fences.sql'")).rows[0]!.n;
  if (applied) await pg.exec(HOLD_DOWN);
};
const down = async (pg: PGlite) => holdOff(pg).then(() => pg.exec(DOWN)).then(() => null, async (e: { code?: string; message?: string }) => {
  await pg.exec("ROLLBACK").catch(() => {});
  return { code: e.code, message: String(e.message) };
});
const checks = async (pg: PGlite) => (await pg.query<{ def: string }>(
  `SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
    WHERE conname IN ('pipeline_jobs_stage_check','pipeline_events_stage_check','pipeline_dead_letters_stage_check') AND conrelid IN
      ('sessions.pipeline_jobs'::regclass,'sessions.pipeline_events'::regclass,'sessions.pipeline_dead_letters'::regclass) ORDER BY conname`)).rows.map((r) => r.def);
/** The runtime boundary and its receipts: present after the up, gone after the down (re-created by a re-apply). */
const runtimeBoundary = async (pg: PGlite) => (await pg.query<{ claim: string | null; settle: string | null; receipts: string | null; contention: string | null; budget: string | null; resume: string | null; probe: string | null; lease: string | null; letter: string | null; attempts: string | null; trigger: boolean }>(
  `SELECT to_regprocedure('sessions.claim_search_jobs_v1(integer)')::text AS claim,
          to_regprocedure('sessions.settle_search_job_v1(bigint,uuid,text)')::text AS settle,
          to_regclass('sessions.search_job_claims')::text AS receipts,
          to_regclass('sessions.search_job_contention')::text AS contention,
          to_regclass('sessions.search_probe_budget')::text AS budget,
          to_regprocedure('sessions.resume_search_contention_v1(bigint,text)')::text AS resume,
          to_regprocedure('sessions.claim_search_probe_v1()')::text AS probe,
          to_regprocedure('sessions.search_job_lease_v1(bigint,boolean)')::text AS lease,
          to_regprocedure('sessions.search_letter_id_v1(text,text,text)')::text AS letter,
          to_regprocedure('sessions.search_job_claim_attempts_v1()')::text AS attempts,
          EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'search_job_claim_attempts' AND tgrelid = 'sessions.pipeline_jobs'::regclass) AS trigger`)).rows[0]!;
const BOUNDARY_PRESENT = { claim: "sessions.claim_search_jobs_v1(integer)", settle: "sessions.settle_search_job_v1(bigint,uuid,text)",
  receipts: "sessions.search_job_claims", contention: "sessions.search_job_contention", budget: "sessions.search_probe_budget",
  resume: "sessions.resume_search_contention_v1(bigint,text)", probe: "sessions.claim_search_probe_v1()",
  lease: "sessions.search_job_lease_v1(bigint,boolean)", letter: "sessions.search_letter_id_v1(text,text,text)", attempts: "sessions.search_job_claim_attempts_v1()", trigger: true };
const BOUNDARY_GONE = { claim: null, settle: null, receipts: null, contention: null, budget: null, resume: null, probe: null, lease: null, letter: null, attempts: null, trigger: false };
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
  // Exhaustion is the server's (settle_search_job_v1, max 12 attempts): the leased row is on its last attempt.
  await pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [c!.id, c!.lease]);
  expect(await settleSearchJob(pg, c!, "exhausted")).toMatchObject({ applied: true, action: "parked" });
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
  // The runtime RPCs and the claim-receipt table are removed with the stage.
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_GONE);
}, 120_000);

test("a refused down keeps the runtime boundary; a stale receipt or contention row of a deleted job holds the down until cleared", async () => {
  const pg = await db();
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_PRESENT);
  await enqueueSearchWake(pg, wake(4));
  const [c] = await claimSearchJobs(pg, 1);
  expect(await down(pg)).toMatchObject({ code: "55000" });
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_PRESENT);
  expect((await pg.query("SELECT 1 FROM sessions.search_job_claims WHERE job_id = $1::bigint", [c!.id])).rows).toHaveLength(1);
  // An owner deletes the job directly (no settlement): its receipt is now stale, and it still holds the down.
  await pg.query("INSERT INTO sessions.search_job_contention VALUES ($1::bigint, 4, now(), now(), 1, NULL, NULL, NULL)", [c!.id]);
  await pg.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index'");
  expect(await down(pg)).toMatchObject({ code: "55000", message: expect.stringContaining("claim receipts exist") });
  await pg.query("DELETE FROM sessions.search_job_claims");
  expect(await down(pg)).toMatchObject({ code: "55000", message: expect.stringContaining("contention rows exist") });
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_PRESENT);
  await pg.query("DELETE FROM sessions.search_job_contention");
  expect(await down(pg)).toBeNull();
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_GONE);
  expect(await archiveObjects(pg)).toEqual(KEPT);
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
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_GONE);
  await migrate(pg);
  expect(await ledgered(pg)).toBe(true);
  // The re-apply re-creates the runtime boundary (claim, settle and the empty receipt table).
  expect(await runtimeBoundary(pg)).toEqual(BOUNDARY_PRESENT);
  expect((await pg.query("SELECT 1 FROM sessions.search_job_claims")).rows).toEqual([]);
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

test("a retained archive with a foreign trigger refuses the re-apply (55000) and leaves the slot unledgered", async () => {
  const pg = await db();
  expect(await down(pg)).toBeNull();
  await pg.exec(`CREATE FUNCTION public.forged_archive_tap() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
    CREATE TRIGGER forged_tap AFTER INSERT ON sessions.search_stage_archive FOR EACH ROW EXECUTE FUNCTION public.forged_archive_tap();`);
  await expect(migrate(pg)).rejects.toThrow(/a trigger other than its seal/);
  await pg.exec("ROLLBACK").catch(() => {});
  expect(await ledgered(pg)).toBe(false);
}, 180_000);
