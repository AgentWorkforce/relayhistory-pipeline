/**
 * The search-index truncate guard must see EVERY queued search job whatever the caller's RLS policies: an
 * invoker-mode presence check let an RLS-filtered role see zero jobs and truncate them away. Regression for the
 * guard: an unconditional refusal for every non-owner (current_user), whatever rows the caller can see.
 *
 * PGlite cannot return from SET SESSION AUTHORIZATION to the superuser, so each case runs on its own clone and the
 * true row count after the switch is read through a SECURITY DEFINER counter the owner creates beforehand (a test
 * fixture, not production code). Synthetic data only.
 */
import { afterAll, beforeAll, expect, test } from "bun:test";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

async function codeOf(p: Promise<unknown>): Promise<string> {
  try { await p; return "ok"; } catch (e) { return String((e as { code?: string }).code ?? e); }
}

/** A clone with one restricted LOGIN whose RLS hides every search-index row, plus a definer-mode true counter. */
async function rig(withJob: boolean) {
  const db = await clonedReplayDatabase();
  if (withJob) {
    await db.pg.exec("SET session_replication_role = replica");
    await db.pg.query(`INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, message)
      VALUES ('search-index', 'search' || chr(31) || 'org_t' || chr(31) || 'ws_t' || chr(31) || 'claude' || chr(31) || 's_t',
              gen_random_uuid(), 'org_t', 'ws_t', 's_t', jsonb_build_object('v', 1, 'stage', 'search-index'))`);
    await db.pg.exec("SET session_replication_role = DEFAULT");
  }
  await db.pg.exec(`CREATE ROLE trunc_restricted LOGIN NOSUPERUSER NOBYPASSRLS;
    GRANT USAGE ON SCHEMA sessions TO trunc_restricted;
    GRANT SELECT, TRUNCATE ON sessions.pipeline_jobs TO trunc_restricted;
    ALTER TABLE sessions.pipeline_jobs ENABLE ROW LEVEL SECURITY;
    CREATE POLICY trunc_restricted_hides_search ON sessions.pipeline_jobs FOR SELECT TO trunc_restricted USING (stage <> 'search-index');
    CREATE FUNCTION public.test_true_search_jobs() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog
      AS $$ SELECT count(*)::int FROM sessions.pipeline_jobs WHERE stage = 'search-index' $$;
    GRANT EXECUTE ON FUNCTION public.test_true_search_jobs() TO trunc_restricted;`);
  const trueCount = async () => (await db.pg.query<{ n: number }>("SELECT public.test_true_search_jobs() AS n")).rows[0]!.n;
  expect(await trueCount()).toBe(withJob ? 1 : 0);
  await db.pg.exec("SET SESSION AUTHORIZATION trunc_restricted");
  expect((await db.pg.query<{ u: string }>("SELECT session_user AS u")).rows[0]!.u).toBe("trunc_restricted");
  return { db, trueCount };
}

test("a restricted login whose RLS hides the queued search job cannot truncate it (42501); the job survives", async () => {
  const { db, trueCount } = await rig(true);
  try {
    // Fixture soundness: the login sees zero search-index rows (the invoker-mode bug's precondition).
    expect((await db.pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows[0]!.n).toBe(0);
    expect(await codeOf(db.pg.query("TRUNCATE sessions.pipeline_jobs"))).toBe("42501");
    expect(await trueCount()).toBe(1);
  } finally {
    await db.pg.close();
  }
}, 120_000);

test("a non-owner may never truncate the queue, even with no search-index row queued (unconditional refusal)", async () => {
  const { db, trueCount } = await rig(false);
  try {
    expect(await codeOf(db.pg.query("TRUNCATE sessions.pipeline_jobs"))).toBe("42501");
    expect(await trueCount()).toBe(0);
  } finally {
    await db.pg.close();
  }
}, 120_000);
