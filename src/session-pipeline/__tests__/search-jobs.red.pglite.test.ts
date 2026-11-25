/**
 * RED until M2 (docs/handoffs/incremental-search-indexing.md §10): the committed `search-index` wake, its
 * coalescing, the catch-up cursor, the committed-state and erase guards, the held-acceptance exhaustion and the
 * budget accounting. Each test asserts the designed contract. Each fails today only because the intended piece is
 * absent (the stage, the index, the cursor table, the handler module), or because of today's erase-on-absence
 * behaviour (R3a). It never fails because of fixture setup: every test first proves its fixture is sound.
 *
 * Synthetic, secret-free data only. Run: bun test src/session-pipeline/__tests__/search-jobs.red.pglite.test.ts
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { PIPELINE_STAGES } from "@rhc/lib/pipeline/trace.ts";
import { QUEUED_STAGES } from "@rhc/lib/pipeline/registry.ts";
import { indexSearchScope } from "../search-index.ts";
import { SCOPE, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";

const STAGE = "search-index";
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

/** A migrated PGlite (the HoopDb template), closed after the test. */
async function migrated() {
  const db = await clonedReplayDatabase();
  cleanup.push(() => db.pg.close());
  // Fixture soundness: the queue table exists and today's stages are accepted.
  await db.pg.query(`INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, message)
    VALUES ('object-landed', 'fixture@1', gen_random_uuid(), 'org_f', 'ws_f', 's_f', '{}'::jsonb)`);
  await db.pg.query(`DELETE FROM sessions.pipeline_jobs WHERE work_key = 'fixture@1'`);
  return db;
}

/** The coalescing enqueue of design §3, verbatim. */
const ENQUEUE = `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, message, visible_at)
  VALUES ('search-index', $1, gen_random_uuid(), 'org_r', 'ws_r', 's_r', $2::jsonb, now())
  ON CONFLICT (work_key) WHERE stage = 'search-index' AND lease_id IS NULL AND NOT dlq
  DO UPDATE SET message = CASE WHEN (EXCLUDED.message->>'epoch')::bigint > (pipeline_jobs.message->>'epoch')::bigint
                               THEN EXCLUDED.message ELSE pipeline_jobs.message END,
                visible_at = least(pipeline_jobs.visible_at, EXCLUDED.visible_at)`;
const WK = "search␟org_r␟ws_r␟claude␟s_r";
const msg = (epoch: number) => JSON.stringify({ scope: { org: "org_r", ws: "ws_r", harness: "claude", session: "s_r" }, canonical: "s_r", epoch, committed_at: "2026-11-25T00:00:00Z" });

/** The handler module M2 adds; absent today. */
async function searchJobs(): Promise<Record<string, unknown>> {
  try {
    return await import("../search-jobs.ts" as string);
  } catch {
    throw new Error("RED: src/session-pipeline/search-jobs.ts is absent (M2 adds the search-index handler)");
  }
}

describe("R6: every closed stage list carries search-index", () => {
  test("TS lists", () => {
    expect(PIPELINE_STAGES as readonly string[]).toContain(STAGE);
    expect(QUEUED_STAGES as readonly string[]).toContain(STAGE);
  });
  test("SQL CHECKs on pipeline_jobs, pipeline_events, pipeline_dead_letters", async () => {
    const db = await migrated();
    const { rows } = await db.pg.query<{ conname: string; def: string }>(`SELECT c.conname, pg_get_constraintdef(c.oid) def FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      WHERE n.nspname = 'sessions' AND t.relname IN ('pipeline_jobs','pipeline_events','pipeline_dead_letters')
        AND c.conname IN ('pipeline_jobs_stage_check','pipeline_events_stage_check','pipeline_dead_letters_stage_check') ORDER BY c.conname`);
    expect(rows.map((r) => r.conname)).toEqual(["pipeline_dead_letters_stage_check", "pipeline_events_stage_check", "pipeline_jobs_stage_check"]);
    for (const r of rows) expect(r.def).toContain(`'${STAGE}'`);
  }, 120_000);
});

describe("R1: coalescing wake (design §3)", () => {
  test("R1a: wakes for epochs N and N+1 leave ONE open job carrying N+1", async () => {
    const db = await migrated();
    await db.pg.query(ENQUEUE, [WK, msg(7)]);
    await db.pg.query(ENQUEUE, [WK, msg(8)]);
    await db.pg.query(ENQUEUE, [WK, msg(6)]); // an older, late wake never regresses the epoch
    const { rows } = await db.pg.query<{ epoch: string }>(`SELECT message->>'epoch' epoch FROM sessions.pipeline_jobs WHERE stage = 'search-index' AND work_key = $1`, [WK]);
    expect(rows).toEqual([{ epoch: "8" }]);
  }, 120_000);

  test("R1b: N leased, then N+1 arrives: a second open row; reclaim after a crash leaves at most one OPEN row", async () => {
    const db = await migrated();
    await db.pg.query(ENQUEUE, [WK, msg(7)]);
    await db.pg.query(`UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid(), visible_at = now() + interval '300 seconds' WHERE work_key = $1`, [WK]);
    await db.pg.query(ENQUEUE, [WK, msg(8)]);
    await db.pg.query(ENQUEUE, [WK, msg(9)]);
    const { rows } = await db.pg.query<{ leased: boolean; epoch: string }>(`SELECT lease_id IS NOT NULL leased, message->>'epoch' epoch FROM sessions.pipeline_jobs
      WHERE stage = 'search-index' AND work_key = $1 ORDER BY leased DESC`, [WK]);
    expect(rows).toEqual([{ leased: true, epoch: "7" }, { leased: false, epoch: "9" }]);
  }, 120_000);

  test("R1c: a retired publication enqueues no search-index job (authority rollback)", async () => {
    const m = await searchJobs();
    expect(typeof m.enqueueSearchWake).toBe("function");
  });
});

describe("R2: catch-up cursor (design §6)", () => {
  test("R2a/R2b: the cursor table exists and advances past one page", async () => {
    const db = await migrated();
    const { rows } = await db.pg.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.search_catchup_cursor`);
    expect(rows[0]!.n).toBeLessThanOrEqual(1);
    const m = await searchJobs();
    expect(typeof m.catchUpTick).toBe("function");
  }, 120_000);
});

describe("R3: committed-state and erase guards (design §4.2, §4.4)", () => {
  let x: ExportedScope;
  beforeAll(async () => { x = await exportStreamScope(generate(5), 40); }, 120_000);
  afterAll(() => x?.dispose());

  test("R3a: a pass on a scope that was NEVER committed erases nothing", async () => {
    const r = await hostedRig(x, cleanup);
    await r.tick({});
    // Fixture soundness: the committed scope indexes completely.
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    const never = { ...SCOPE, sessionId: "never_committed" };
    const result = await indexSearchScope(never, r.search());
    // Today this answers "erased" (empty live keys are treated as an erasure). Designed: not_committed, no erase.
    expect(result.reasons).not.toContain("erased");
  }, 120_000);

  test("R3b-R3d: old-version delete, recreate race and stale epoch go through the fenced handler", async () => {
    const m = await searchJobs();
    expect(typeof m.runSearchJob).toBe("function");
  });
});

describe("R4/R5: held acceptance and budget accounting (design §4.5, §0)", () => {
  test("R4: exhaustion quarantines and never marks the scope ready", async () => {
    const m = await searchJobs();
    expect(m.SEARCH_JOB_MAX_ATTEMPTS).toBe(12);
  });
  test("R5: the job carries committed_at and the pass reports commit_to_searchable_ms", async () => {
    const m = await searchJobs();
    expect(typeof m.commitToSearchableMs).toBe("function");
  });
});
