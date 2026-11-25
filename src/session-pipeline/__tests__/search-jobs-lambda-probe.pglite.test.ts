/**
 * The Lambda's two search-job entry points over the separate probe lane (migration 20261105062300; lambda.ts
 * drainSearchJobs): ordinary capacity is K passes per invocation, each claimed (claim_search_jobs_v1) just before it
 * runs; a probe (claim_search_probe_v1, one due held scope, fleet-bucket bounded) is EXTRA. The SQS follow-on probes
 * after its ordinary passes if time remains; the scheduled invocation ({searchJobs:true}) probes FIRST. The handler
 * test qualifies what an invocation does when invoked; it does not prove the hosted schedule rule exists (that is a
 * stack change with its own go). Owner-run PGlite clones; synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import type { HoopDb } from "../../shared/db.ts";
import { createPipelineLambdaHandler, drainSearchJobs, lambdaEnv, type Runtime } from "../lambda.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { enqueueSearchWake, searchWorkKey, type SearchWakeMessage } from "../search-jobs.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());
const saved = { ...process.env };
afterEach(() => { process.env = { ...saved }; });

const scope = { orgId: "org_lp", workspaceId: "ws_lp", harness: "claude", sessionId: "s_lp" };
const wake = (sessionId: string): SearchWakeMessage => {
  const s = { ...scope, sessionId };
  return { v: 1, stage: "search-index", ctx: { traceId: crypto.randomUUID(), ...s }, workKey: searchWorkKey(s), revision: 1,
    payload: { epoch: 1, clock: { source: "pointer", at: "2026-11-01T10:00:00.000Z" } } };
};

async function db() {
  const { pg } = await clonedReplayDatabase();
  cleanup.push(() => pg.close());
  // Synthetic scopes have no session_index row: each pass answers not_committed and is settled done.
  const search = { transact: (work) => pg.transaction((tx) => work(tx)) } as SearchIndexDeps;
  const counts = async () => (await pg.query<{ ordinary: number; held: number; probes: number }>(
    `SELECT (SELECT count(*)::int FROM sessions.pipeline_jobs j WHERE j.stage = 'search-index'
              AND NOT EXISTS (SELECT 1 FROM sessions.search_job_contention k WHERE k.job_id = j.id AND k.held_at IS NOT NULL)) AS ordinary,
            (SELECT count(*)::int FROM sessions.search_job_contention WHERE held_at IS NOT NULL) AS held,
            (SELECT count(*)::int FROM sessions.pipeline_events WHERE error->>'code' = 'scope_contention_probe') AS probes`)).rows[0]!;
  return { pg, search, counts };
}
/** Hold the given scopes' jobs (the 24 h cap), their probes due now. */
async function hold(pg: PGlite, keys: string[]) {
  await pg.query(
    `INSERT INTO sessions.search_job_contention (job_id, epoch, first_contended_at, last_contended_at, contention_count, last_escalated_at, held_at, next_probe_at)
     SELECT id, 1, now() - interval '25 hours', now(), 100, now(), now() - interval '1 hour', statement_timestamp() - interval '1 second'
       FROM sessions.pipeline_jobs WHERE stage = 'search-index' AND work_key = ANY($1::text[])`, [keys]);
}

describe("drainSearchJobs: ordinary capacity is K passes; a probe is extra", () => {
  test("K = 3 with ordinary work and due probes ready: exactly 3 ordinary jobs per invocation, plus at most 1 probe within the bucket", async () => {
    const { pg, search, counts } = await db();
    const held = ["h0", "h1", "h2", "h3"].map(wake);
    for (const w of held) await enqueueSearchWake(pg, w);
    await hold(pg, held.map((w) => w.workKey));
    for (let n = 0; n < 30; n++) await enqueueSearchWake(pg, wake(`o${n}`));
    // Two tokens, a negligible refill: at most two probes over the run.
    await pg.query("UPDATE sessions.search_probe_budget SET tokens = 2, rate_per_minute = 0.001, refilled_at = clock_timestamp()");
    const summaries = [];
    for (let i = 0; i < 5; i++) {
      const before = await counts();
      const s = await drainSearchJobs({ sql: pg, search, claim: 3 });
      const after = await counts();
      expect(before.ordinary - after.ordinary).toBe(3);
      expect(s.probes).toBeLessThanOrEqual(1);
      expect(s.acked).toBe(3 + s.probes);
      expect(s.deadlineExit).toBe(false);
      summaries.push(s);
    }
    expect(summaries.map((s) => s.probes)).toEqual([1, 1, 0, 0, 0]);
    expect(await counts()).toEqual({ ordinary: 15, held: 2, probes: 2 });
  }, 120_000);
});

describe("the real handler: SQS invocations that never reach the probe step, and the scheduled probe-first path", () => {
  test("continuous ordinary backlog and low remaining time: every SQS invocation completes K ordinary passes; the held scope recovers through the scheduled invocation", async () => {
    const { pg, search, counts } = await db();
    process.env.SEARCH_JOBS_ENABLED = "1";
    const BASE = {
      AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
      SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs",
    };
    const K = 3;
    const hoop = { query: (text: string, params?: unknown[]) => pg.query(text, params), end: async () => {},
      pool: { connect: async () => ({ query: (text: string, params?: unknown[]) => pg.query(text, params), release() {} }) } } as unknown as HoopDb;
    const rt = { env: lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "1", SEARCH_JOBS_CLAIM: String(K) }),
      deps: { pointer: { db: hoop, reader: { provider: "s3" } }, search } } as unknown as Runtime;
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
    /** Remaining time above the 60 s margin for exactly `checks` deadline checks, then below it. */
    const context = (checks: number) => { let calls = 0; return { getRemainingTimeInMillis: () => (++calls <= checks ? 61_000 : 59_000) }; };
    const hot = wake("hot");
    await enqueueSearchWake(pg, hot);
    await hold(pg, [hot.workKey]);
    let next = 0;
    const topUp = async () => { while ((await counts()).ordinary < 10) await enqueueSearchWake(pg, wake(`o${next++}`)); };
    const sqs = { Records: [{ messageId: "m1", body: JSON.stringify({ Event: "s3:TestEvent" }) }] };
    const ledger: Array<{ kind: "sqs" | "scheduled"; ordinary: number; probes: number; held: number }> = [];
    for (let round = 0; round < 3; round++) {
      for (let i = 0; i < 3; i++) {
        await topUp();
        const before = await counts();
        // K checks pass, the probe step's check does not: the SQS invocation never reaches its probe.
        expect(await handler(sqs as never, context(K) as never)).toEqual({ batchItemFailures: [] });
        const after = await counts();
        ledger.push({ kind: "sqs", ordinary: before.ordinary - after.ordinary, probes: after.probes - before.probes, held: after.held });
      }
      if (round === 1) {
        await topUp();
        const before = await counts();
        // The scheduled invocation: the probe first (one check), then its K ordinary passes.
        expect(await handler({ searchJobs: true } as never, context(K + 1) as never)).toEqual({ ok: true });
        const after = await counts();
        ledger.push({ kind: "scheduled", ordinary: before.ordinary - after.ordinary, probes: after.probes - before.probes, held: after.held });
      }
    }
    const sqsRuns = ledger.filter((l) => l.kind === "sqs");
    expect(sqsRuns.every((l) => l.ordinary === K && l.probes === 0)).toBe(true);
    // Before the scheduled invocation the scope stays held; through it, it is probed and recovers.
    expect(ledger.slice(0, 6).every((l) => l.held === 1)).toBe(true);
    expect(ledger[6]).toEqual({ kind: "scheduled", ordinary: K, probes: 1, held: 0 });
    expect(ledger.slice(7).every((l) => l.held === 0)).toBe(true);
    expect((await pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE work_key = $1", [hot.workKey])).rows).toEqual([]);
    expect((await pg.query("SELECT 1 FROM sessions.pipeline_dead_letters")).rows).toEqual([]);
    console.log(JSON.stringify({ handlerEntryPoints: { K, sqsInvocations: sqsRuns.length, sqsOrdinaryEach: K, sqsProbes: 0,
      scheduled: ledger[6] } }));
  }, 120_000);
});
