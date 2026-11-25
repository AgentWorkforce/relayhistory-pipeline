/**
 * 20261105062300's runtime boundary on actual PostgreSQL, through a GENUINE login connection (its own credentials, no
 * SET ROLE, no membership) that holds only USAGE on sessions and EXECUTE on sessions.claim_search_jobs_v1 and
 * sessions.settle_search_job_v1; the owner seeds and observes on its own connection. The PGlite twin
 * (search-jobs-definer.pglite.test.ts) covers the full case list; this proves the same contract under the real server's
 * privilege checks. Opt in with an owned, synthetic loopback SESSION_INGEST_TEST_PG_URL (trust auth for the fixture
 * login); never a deployed database. Each run creates and drops its own database and login. Synthetic data only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { drainSearchJobs } from "../lambda.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import {
  archiveSearchStage, catchUpTick, claimSearchJobs, enqueueSearchWake, parkSearchJobs, replaySearchDeadLetter, searchLetterId,
  searchWorkKey, settleSearchJob, type SearchSql, type SearchWakeMessage,
} from "../search-jobs.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" || u.password || u.pathname !== "/postgres" || u.search || u.hash)
    throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const sql = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const cleanup: (() => Promise<void> | void)[] = [];
afterEach(async () => { for (const f of cleanup.splice(0).reverse()) await f(); }, 60_000);

const CLAIM = "sessions.claim_search_jobs_v1(integer)";
const SETTLE = "sessions.settle_search_job_v1(bigint,uuid,text)";
const PROBE = "sessions.claim_search_probe_v1()";
type Scope = { orgId: string; workspaceId: string; harness: string; sessionId: string };
const scope: Scope = { orgId: "org:pg enc", workspaceId: "ws_pg", harness: "claude", sessionId: "s_pg" };
const wake = (s: Scope, epoch: number): SearchWakeMessage => ({
  v: 1, stage: "search-index", ctx: { traceId: crypto.randomUUID(), ...s }, workKey: searchWorkKey(s), revision: 1,
  payload: { epoch, clock: { source: "pointer", at: "2026-11-01T10:00:00.000Z" } },
});
async function codeOf(p: () => Promise<unknown>): Promise<string> {
  try { await p(); return "ok"; } catch (e) { return String((e as { code?: string }).code ?? e); }
}

/**
 * A transaction-capable adapter over the login's pool (BEGIN ... ROLLBACK on one client), so the operator paths that
 * open a transaction (park's LOCK TABLE, replay, catch-up) fail on the real privilege check, not on 25P01.
 */
const transactional = (pool: pg.Pool): SearchSql => ({
  query: (text, params) => pool.query(text, params as unknown[]) as never,
  async transaction(work) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      return await work({ query: (text, params) => client.query(text, params as unknown[]) as never });
    } finally {
      await client.query("ROLLBACK").catch(() => {});
      client.release();
    }
  },
});

async function rig() {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const id = crypto.randomUUID().replaceAll("-", "");
  const name = `search_definer_${id}`, login = `search_login_${id}`;
  await admin.query(`CREATE DATABASE ${name}`);
  await admin.query(`CREATE ROLE ${login} LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS`);
  const target = new URL(url!);
  target.pathname = `/${name}`;
  const owner = new pg.Pool({ connectionString: target.href, max: 10 });
  target.username = login;
  const runtime = new pg.Pool({ connectionString: target.href, max: 1 });
  const loginUrl = target.href;
  const extra: pg.Pool[] = [];
  cleanup.push(async () => {
    for (const p of extra) await p.end();
    await runtime.end();
    await owner.end();
    await admin.query(`DROP DATABASE ${name} WITH (FORCE)`);
    await admin.query(`DROP ROLE ${login}`);
    await admin.end();
  });
  await owner.query(sql("supabase/tests/supabase_shim.sql"));
  await owner.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
  for (const f of SESSION_BOOTSTRAP_FILES) await owner.query(sql(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await owner.query(sql("supabase/migrations/" + f));
  await owner.query(`GRANT USAGE ON SCHEMA sessions TO ${login}; GRANT EXECUTE ON FUNCTION ${CLAIM}, ${PROBE}, ${SETTLE} TO ${login}`);
  /** More genuine login connections (concurrent claimers). */
  const loginPool = (max: number) => { const p = new pg.Pool({ connectionString: loginUrl, max }); extra.push(p); return p; };
  return { owner, runtime, login, loginPool };
}

describe.skipIf(!url)("062300 runtime boundary: a genuine unprivileged login (real PostgreSQL)", () => {
  test("identity and ACL; claim/settle work; the receipt decides; stale, foreign, duplicate and forged settlements and every direct path are refused", async () => {
    const { owner, runtime, login } = await rig();
    // A genuine login: session_user is the login itself, not a superuser's switch; no membership either way.
    expect((await runtime.query("SELECT session_user AS s, current_user AS c")).rows).toEqual([{ s: login, c: login }]);
    expect((await owner.query("SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid IN (m.member, m.roleid) WHERE r.rolname = $1", [login])).rows).toEqual([]);
    expect((await owner.query<{ fn: string }>(
      "SELECT p.oid::regprocedure::text AS fn FROM pg_proc p WHERE p.pronamespace = 'sessions'::regnamespace AND has_function_privilege($1, p.oid, 'EXECUTE') ORDER BY 1",
      [login])).rows.map((r) => r.fn)).toEqual([CLAIM, PROBE, SETTLE]);
    expect((await owner.query(
      `SELECT p.prosecdef AS secdef, p.proconfig AS config FROM pg_proc p WHERE p.oid IN ($1::regprocedure, $2::regprocedure, $3::regprocedure)`, [CLAIM, PROBE, SETTLE])).rows)
      .toEqual([1, 2, 3].map(() => ({ secdef: true, config: ["search_path=pg_catalog, pg_temp"] })));

    // Success: claim, then done.
    await enqueueSearchWake(owner, wake(scope, 7));
    const [c] = await claimSearchJobs(runtime, 5);
    expect({ epoch: c!.claimedEpoch, attempts: c!.attempts }).toEqual({ epoch: 7, attempts: 1 });
    // Tampering: a wake N+1 lands during the lease; the holder claims N+1 and a forged message: done reopens.
    const w8 = wake(scope, 8);
    await enqueueSearchWake(owner, w8);
    expect(await settleSearchJob(runtime, { ...c!, claimedEpoch: 8, claimedMessage: wake({ ...scope, orgId: "org_forged" }, 99) }, "done"))
      .toEqual({ applied: true, action: "reopened" });
    expect((await owner.query("SELECT message, attempts, lease_id FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows)
      .toEqual([{ message: w8, attempts: 0, lease_id: null }]);
    // Duplicate settlement.
    expect(await settleSearchJob(runtime, c!, "done")).toEqual({ applied: false });
    // Forged outcome.
    const [c8] = await claimSearchJobs(runtime, 1);
    expect(await codeOf(() => runtime.query("SELECT sessions.settle_search_job_v1($1::bigint, $2::uuid, 'exhausted')", [c8!.id, c8!.lease]))).toBe("22023");
    // An expired lease settles nothing.
    await owner.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");
    expect(await settleSearchJob(runtime, c8!, "done")).toEqual({ applied: false });
    // Another stage's job, under its own lease, is never settled.
    const lease = crypto.randomUUID();
    const other = (await owner.query<{ id: string }>(
      `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message, visible_at, lease_id, attempts)
       VALUES ('session-indexed', 'pg-other', gen_random_uuid(), 'o', 'w', '{"v":1}'::jsonb, now() + interval '5 minutes', $1::uuid, 1) RETURNING id::text`, [lease])).rows[0]!.id;
    expect(await settleSearchJob(runtime, { id: other, lease }, "done")).toEqual({ applied: false });
    expect((await owner.query("SELECT lease_id::text AS l FROM sessions.pipeline_jobs WHERE id = $1::bigint", [other])).rows).toEqual([{ l: lease }]);
    // A reclaim after expiry settles; the old receipt was replaced.
    const [again] = await claimSearchJobs(runtime, 1);
    expect(await settleSearchJob(runtime, c8!, "done")).toEqual({ applied: false });
    expect(await settleSearchJob(runtime, again!, "done")).toEqual({ applied: true, action: "deleted" });
    expect((await owner.query("SELECT count(*)::int AS n FROM sessions.search_job_claims")).rows).toEqual([{ n: 0 }]);

    // Denied: direct DML on the queue and the receipts, TRUNCATE, and the operator paths.
    await enqueueSearchWake(owner, wake(scope, 9));
    const denied = {
      updateJob: await codeOf(() => runtime.query("UPDATE sessions.pipeline_jobs SET attempts = 0")),
      deleteJob: await codeOf(() => runtime.query("DELETE FROM sessions.pipeline_jobs")),
      truncateJobs: await codeOf(() => runtime.query("TRUNCATE sessions.pipeline_jobs")),
      receipts: await codeOf(() => runtime.query("SELECT * FROM sessions.search_job_claims")),
      replay: await codeOf(() => replaySearchDeadLetter(transactional(runtime), searchLetterId(wake(scope, 9)))),
      catchUp: await codeOf(() => catchUpTick({ sql: transactional(runtime), pageSize: 10 })),
      park: await codeOf(() => parkSearchJobs(transactional(runtime), { operator: "pg-definer-test" })),
      archive: await codeOf(() => archiveSearchStage(runtime)),
      enqueue: await codeOf(() => enqueueSearchWake(runtime, wake(scope, 10))),
    };
    expect(denied).toEqual(Object.fromEntries(Object.keys(denied).map((k) => [k, "42501"])));
    // Fixture soundness for the park denial: the same adapter, as the owner, does reach and run the LOCK (no 25P01).
    expect(await codeOf(() => transactional(owner).transaction!((tx) => tx.query("LOCK TABLE sessions.pipeline_jobs IN SHARE ROW EXCLUSIVE MODE")))).toBe("ok");
  }, 300_000);

  test("two sessions: a settlement that waited on the job's row lock past the lease end applies nothing", async () => {
    const { owner, runtime } = await rig();
    await enqueueSearchWake(owner, wake(scope, 7));
    const [c] = await claimSearchJobs(runtime, 1);
    // The lease (row and receipt) ends 1.5 s from now.
    const leaseEnd = (await owner.query<{ at: Date }>(
      "UPDATE sessions.pipeline_jobs SET visible_at = clock_timestamp() + interval '1.5 seconds' WHERE id = $1::bigint RETURNING visible_at AS at", [c!.id])).rows[0]!.at;
    await owner.query("UPDATE sessions.search_job_claims SET lease_until = $2 WHERE job_id = $1::bigint", [c!.id, leaseEnd]);
    const holder = await owner.connect();
    const settler = await runtime.connect();
    try {
      // Session 1 holds the job row lock.
      await holder.query("BEGIN");
      await holder.query("SELECT 1 FROM sessions.pipeline_jobs WHERE id = $1::bigint FOR UPDATE", [c!.id]);
      // Session 2 starts its transaction while the lease is live, then settles: it blocks on the lock.
      await settler.query("BEGIN");
      // Fixture soundness: the settling transaction's start (now()) is before the lease end.
      const started = (await settler.query<{ at: Date }>("SELECT now() AS at")).rows[0]!.at;
      expect(started.getTime()).toBeLessThan(new Date(leaseEnd).getTime());
      const pending = settler.query("SELECT sessions.settle_search_job_v1($1::bigint, $2::uuid, 'done') AS r", [c!.id, c!.lease]);
      await Bun.sleep(2_500);
      await holder.query("COMMIT");
      expect((await pending).rows).toEqual([{ r: { applied: false } }]);
      await settler.query("COMMIT");
    } finally {
      await holder.query("ROLLBACK").catch(() => {});
      await settler.query("ROLLBACK").catch(() => {});
      holder.release();
      settler.release();
    }
    expect((await owner.query("SELECT lease_id::text AS l FROM sessions.pipeline_jobs WHERE id = $1::bigint", [c!.id])).rows).toEqual([{ l: c!.lease }]);
    expect((await owner.query("SELECT count(*)::int AS n FROM sessions.search_job_claims")).rows).toEqual([{ n: 1 }]);
  }, 300_000);

  test("contention, budget and resume are out of the login's reach (42501)", async () => {
    const { runtime } = await rig();
    const denied = {
      contention: await codeOf(() => runtime.query("SELECT * FROM sessions.search_job_contention")),
      budget: await codeOf(() => runtime.query("UPDATE sessions.search_probe_budget SET tokens = 6")),
      resume: await codeOf(() => runtime.query("SELECT sessions.resume_search_contention_v1(1, 'login')")),
    };
    expect(denied).toEqual({ contention: "42501", budget: "42501", resume: "42501" });
  }, 300_000);

  test("sustained load through the Lambda loop: ordinary throughput with 52 held scopes stays within 90% of the no-held control; the fleet bound holds and every held scope recovers", async () => {
    const R = 120, C = 6, K = 3, WINDOW_MS = 30_000, INVOCATION_BUDGET_MS = 200, CADENCE_MS = 20;
    /** One sustained run: 3 SQS-style claimers (ordinary first, then an extra probe if time remains) and 1
     *  scheduled-style claimer (probe first), each looping drainSearchJobs invocations for WINDOW_MS. */
    async function run(heldCount: number) {
      const { owner, loginPool } = await rig();
      // Test-only policy through the owner's config row (no code change): R = 120/min, C = 6, initial balance 6.
      await owner.query("UPDATE sessions.search_probe_budget SET rate_per_minute = $1, burst = $2, tokens = $2, refilled_at = clock_timestamp()", [R, C]);
      const held = Array.from({ length: heldCount }, (_, n) => wake({ ...scope, sessionId: `s_held_${String(n).padStart(2, "0")}` }, 1));
      const heldKeys = new Set(held.map((w) => w.workKey));
      for (const w of held) await enqueueSearchWake(owner, w);
      // Synchronized: one due time for every held scope (statement_timestamp(), one value per statement).
      const due = heldCount ? (await owner.query<{ due: Date }>(
        `INSERT INTO sessions.search_job_contention (job_id, epoch, first_contended_at, last_contended_at, contention_count, last_escalated_at, held_at, next_probe_at)
         SELECT id, 1, clock_timestamp() - interval '25 hours', clock_timestamp(), 100, clock_timestamp() - interval '10 minutes',
                clock_timestamp() - interval '1 hour', statement_timestamp()
           FROM sessions.pipeline_jobs WHERE stage = 'search-index'
         RETURNING next_probe_at AS due`)).rows.map((r) => r.due.getTime()) : [];
      expect(new Set(due).size).toBe(heldCount ? 1 : 0);
      const backlog = async () => Number((await owner.query<{ n: string }>(
        `SELECT count(*) AS n FROM sessions.pipeline_jobs x WHERE x.stage = 'search-index' AND x.lease_id IS NULL
            AND NOT EXISTS (SELECT 1 FROM sessions.search_job_contention k WHERE k.job_id = x.id AND k.held_at IS NOT NULL)`)).rows[0]!.n);
      let enqueued = 0, minBacklog = Infinity, backlogAtLastProbe = -1;
      for (let n = 0; n < 60; n++) await enqueueSearchWake(owner, wake({ ...scope, sessionId: `s_ord_${enqueued++}` }, 1));
      const started = Date.now();
      const live = () => Date.now() - started < WINDOW_MS;
      // The producer keeps ordinary work continuously ready for the whole window (no stop-and-drain).
      const producer = (async () => {
        while (live()) {
          const b = await backlog();
          minBacklog = Math.min(minBacklog, b);
          if (b < 60) for (let n = 0; n < 30; n++) await enqueueSearchWake(owner, wake({ ...scope, sessionId: `s_ord_${enqueued++}` }, 1));
          await Bun.sleep(10);
        }
      })();
      // The pass reads as the owner (the runtime's own read privileges are not this test's subject).
      const search = { transact: async (work) => {
        const client = await owner.connect();
        try { await client.query("BEGIN"); const r = await work({ query: (t: string, p?: unknown[]) => client.query(t, p as unknown[]) as never }); await client.query("COMMIT"); return r; }
        catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; } finally { client.release(); }
      } } as SearchIndexDeps;
      const tally = { sqs: { invocations: 0, ordinary: 0, probes: 0, deadlineExits: 0 }, scheduled: { invocations: 0, ordinary: 0, probes: 0, deadlineExits: 0 } };
      const probedKeys = new Set<string>();
      const claimer = async (kind: "sqs" | "scheduled") => {
        const pool = loginPool(1);
        // Observe which scopes each probe took (the summary counts them; the keys come from the probe events below).
        while (live()) {
          const t0 = Date.now();
          const s = await drainSearchJobs({ sql: pool, search, claim: K, deadline: () => Date.now() - t0 > INVOCATION_BUDGET_MS, probeFirst: kind === "scheduled" });
          const t = tally[kind];
          t.invocations += 1; t.ordinary += s.acked - s.probes; t.probes += s.probes; t.deadlineExits += s.deadlineExit ? 1 : 0;
          if (s.probes && heldCount) {
            const keys = (await owner.query<{ key: string }>(
              "SELECT DISTINCT attrs->>'work_key' AS key FROM sessions.pipeline_events WHERE error->>'code' = 'scope_contention_probe'")).rows.map((r) => r.key);
            for (const k of keys) probedKeys.add(k);
            if (probedKeys.size === heldKeys.size && backlogAtLastProbe < 0) backlogAtLastProbe = await backlog();
          }
          await Bun.sleep(CADENCE_MS);
        }
      };
      await Promise.all([claimer("sqs"), claimer("sqs"), claimer("sqs"), claimer("scheduled")]);
      await producer;
      const seconds = (Date.now() - started) / 1000;
      const ordinary = tally.sqs.ordinary + tally.scheduled.ordinary;
      const probes = (await owner.query<{ key: string; at: number; lease_key: string }>(
        `SELECT attrs->>'work_key' AS key, (extract(epoch FROM (attrs->>'admitted_at')::timestamptz) * 1000)::float8 AS at,
                idempotency_key AS lease_key FROM sessions.pipeline_events
          WHERE stage = 'search-index' AND error->>'code' = 'scope_contention_probe'`)).rows.map((r) => ({ key: r.key, at: Number(r.at), leaseKey: r.lease_key }));
      return { owner, tally, ordinary, perSecond: ordinary / seconds, seconds, probes, heldKeys, due: due[0], minBacklog, backlogAtLastProbe, enqueued };
    }

    const control = await run(0);
    expect(control.probes).toEqual([]);
    const loaded = await run(52);
    // Ordinary throughput under sustained load keeps >= 90% of the control's.
    expect(loaded.perSecond).toBeGreaterThanOrEqual(0.9 * control.perSecond);
    // Every held scope was probed once (one durable event per admitted probe) and recovered; nothing parked.
    expect(new Set(loaded.probes.map((p) => p.leaseKey)).size).toBe(loaded.probes.length);
    expect(new Set(loaded.probes.map((p) => p.key)).size).toBe(52);
    expect(loaded.probes).toHaveLength(52);
    expect((await loaded.owner.query("SELECT count(*)::int AS n FROM sessions.search_job_contention")).rows).toEqual([{ n: 0 }]);
    expect((await loaded.owner.query("SELECT count(*)::int AS n FROM sessions.pipeline_dead_letters")).rows).toEqual([{ n: 0 }]);
    // The ordinary backlog stayed positive, including when the last held scope was probed.
    expect(loaded.minBacklog).toBeGreaterThan(0);
    expect(loaded.backlogAtLastProbe).toBeGreaterThan(0);
    // The fleet bound over every interval of the observed (server-clock) admissions: count <= C + R x minutes.
    const t = loaded.probes.map((p) => p.at).sort((x, y) => x - y);
    let worst = -Infinity;
    for (let i = 0; i < t.length; i++) for (let j = i; j < t.length; j++)
      worst = Math.max(worst, (j - i + 1) - (C + R * (t[j]! - t[i]!) / 60_000));
    expect(worst).toBeLessThanOrEqual(0.01);
    // Progress deadline: all probed within ceil((N - C) / R) minutes of the synchronized due time.
    const deadlineMs = Math.ceil((52 - C) / R) * 60_000;
    const maxWaitMs = t.at(-1)! - loaded.due!;
    expect(maxWaitMs).toBeLessThanOrEqual(deadlineMs);
    const receipt = (r: typeof control) => ({ ordinaryPerSecond: Number(r.perSecond.toFixed(1)), ordinary: r.ordinary, seconds: Number(r.seconds.toFixed(1)),
      sqs: r.tally.sqs, scheduled: r.tally.scheduled, minBacklog: r.minBacklog });
    console.log(JSON.stringify({ sustainedThroughput: {
      setup: { claimers: "3 SQS-style + 1 scheduled-style, K = 3, 200 ms invocation budget, 20 ms between invocations", R, C, windowS: WINDOW_MS / 1000 },
      control: receipt(control), loaded: { ...receipt(loaded), heldScopes: 52, distinctProbed: new Set(loaded.probes.map((p) => p.key)).size,
        probes: loaded.probes.length, boundAtLast: C + R * (t.at(-1)! - t[0]!) / 60_000, backlogAtLastProbe: loaded.backlogAtLastProbe, maxProbeWaitMs: maxWaitMs, deadlineMs },
      ratio: Number((loaded.perSecond / control.perSecond).toFixed(3)) } }));
  }, 600_000);
});
