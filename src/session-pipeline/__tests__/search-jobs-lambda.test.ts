/**
 * The Lambda's search-job wiring (docs/handoffs/incremental-search-indexing.md §5.1, §8): with every flag off the
 * Lambda is unchanged and sends no search-index SQL; SEARCH_JOBS_ENABLED and SEARCH_WAKE_ENQUEUE parse strictly;
 * on, the batch's follow-on claim and the schedule run the stage-specific loop. Synthetic, secret-free data only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { HoopDb } from "../../shared/db.ts";
import { createPipelineLambdaHandler, drainSearchJobs, invocationDeps, lambdaEnv, runtimeFromSecret, type Runtime } from "../lambda.ts";
import type { SearchIndexDeps } from "../search-index.ts";

const BASE = {
  AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn", SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};
const SECRET = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "https://ch.invalid", user: "u", password: "p", database: "d" } });

/** Every statement any path sends, through the pool and through transactions. */
function recordingDb() {
  const sql: string[] = [];
  const tx: SqlExecutor = { async query<R>(text: string) { sql.push(text); return { rows: [] as R[] }; } };
  const db = { ...tx, pool: undefined, end: async () => {} } as unknown as HoopDb;
  const transact: SearchIndexDeps["transact"] = (work) => work(tx);
  return { sql, db, transact };
}
const searchSql = (sql: string[]) => sql.filter((s) => /search-index|pipeline_jobs|search_catchup_cursor|search_stage_archive|enqueue_managed_search_wake|claim_search_jobs_v1|settle_search_job_v1|search_job_claims/.test(s));
/** The runtime boundary's two RPCs (migration 20261105062300): the only search SQL the Lambda may send. */
const searchRpcs = (sql: string[]) => sql.filter((s) => /claim_search_jobs_v1|settle_search_job_v1/.test(s));

const saved = { ...process.env };
afterEach(() => { process.env = { ...saved }; });

describe("flags off: the Lambda is unchanged", () => {
  test("lambdaEnv carries neither search-jobs field, and the runtime's writer passes enqueueSearchWake: false", () => {
    const env = lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1" });
    expect("searchJobs" in env).toBe(false);
    expect("searchWakeEnqueue" in env).toBe(false);
    const rt = runtimeFromSecret(env, SECRET, "synthetic-generation");
    expect(rt.deps.pointer.publication?.enqueueSearchWake).toBe(false);
    expect("searchJobs" in rt.deps).toBe(false);
  });

  test("invocationDeps returns the very same deps; an SQS batch sends no search-index SQL; the schedule loads nothing", async () => {
    const { sql, db, transact } = recordingDb();
    const rt = {
      env: lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1" }),
      deps: { pointer: { db, reader: { provider: "s3" } }, search: { transact } },
    } as unknown as Runtime;
    const context = { getRemainingTimeInMillis: () => 300_000 };
    expect(invocationDeps(rt, context)).toBe(rt.deps);
    let loaded = 0;
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => { loaded++; return rt; } });
    // An S3 test event: processBatch skips it, so any SQL would come from a search-jobs follow-on.
    const body = JSON.stringify({ Event: "s3:TestEvent" });
    expect(await handler({ Records: [{ messageId: "m1", body }] } as never, context as never)).toEqual({ batchItemFailures: [] });
    expect(loaded).toBe(1);
    expect(searchSql(sql)).toEqual([]);
    expect(searchRpcs(sql)).toEqual([]);
    delete process.env.SEARCH_JOBS_ENABLED;
    expect(await handler({ searchJobs: true } as never, context as never)).toEqual({ ok: true });
    expect(loaded).toBe(1);
    // SEARCH_JOBS_ENABLED off: no search RPC at all, neither a claim nor a settlement.
    expect(searchSql(sql)).toEqual([]);
    expect(searchRpcs(sql)).toEqual([]);
    expect(sql.some((s) => /claim_search_jobs_v1|settle_search_job_v1/.test(s))).toBe(false);
  });
});

describe("flag parsing", () => {
  test("SEARCH_JOBS_ENABLED is strict, needs the search stage, and bounds SEARCH_JOBS_CLAIM", () => {
    expect(lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "1" }).searchJobs).toEqual({ claim: 5 });
    expect(lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "1", SEARCH_JOBS_CLAIM: "12" }).searchJobs).toEqual({ claim: 12 });
    expect(lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "0" }).searchJobs).toBeUndefined();
    expect(() => lambdaEnv({ ...BASE, SEARCH_JOBS_ENABLED: "1" })).toThrow("SEARCH_JOBS_ENABLED=1 needs SEARCH_INDEX_ENABLED=1");
    expect(() => lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "true" })).toThrow("SEARCH_JOBS_ENABLED must be 1 or 0");
    for (const claim of ["0", "21", "2.5", "x"])
      expect(() => lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "1", SEARCH_JOBS_CLAIM: claim })).toThrow("SEARCH_JOBS_CLAIM");
  });

  test("SEARCH_WAKE_ENQUEUE=1 turns the writer on; anything but 1/0/unset refuses init", () => {
    const env = lambdaEnv({ ...BASE, SEARCH_WAKE_ENQUEUE: "1" });
    expect(env.searchWakeEnqueue).toBe(true);
    expect(runtimeFromSecret(env, SECRET, "synthetic-generation").deps.pointer.publication?.enqueueSearchWake).toBe(true);
    expect(() => lambdaEnv({ ...BASE, SEARCH_WAKE_ENQUEUE: "yes" })).toThrow("SEARCH_WAKE_ENQUEUE must be 1 or 0");
  });
});

describe("flags on: the stage-specific loop", () => {
  test("the batch's follow-on claims search-index jobs once after the batch; the schedule does the same", async () => {
    process.env.SEARCH_JOBS_ENABLED = "1";
    const { sql, db, transact } = recordingDb();
    const rt = {
      env: lambdaEnv({ ...BASE, SEARCH_INDEX_ENABLED: "1", SEARCH_JOBS_ENABLED: "1", SEARCH_JOBS_CLAIM: "3" }),
      deps: { pointer: { db, reader: { provider: "s3" } }, search: { transact } },
    } as unknown as Runtime;
    const context = { getRemainingTimeInMillis: () => 300_000 };
    const deps = invocationDeps(rt, context);
    expect(typeof deps.searchJobs?.drain).toBe("function");
    const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
    const body = JSON.stringify({ Event: "s3:TestEvent" });
    expect(await handler({ Records: [{ messageId: "m1", body }] } as never, context as never)).toEqual({ batchItemFailures: [] });
    const claims = () => searchSql(sql).filter((s) => s.includes("sessions.claim_search_jobs_v1("));
    // The loop's only search SQL is the runtime boundary: no direct pipeline_jobs statement.
    const direct = () => searchSql(sql).filter((s) => !/claim_search_jobs_v1|settle_search_job_v1/.test(s));
    expect(claims()).toHaveLength(1);
    expect(await handler({ searchJobs: true } as never, context as never)).toEqual({ ok: true });
    expect(claims()).toHaveLength(2);
    // Past the deadline margin nothing is claimed.
    expect(await drainSearchJobs({ sql: db, search: rt.deps.search!, claim: 3, deadline: () => true })).toMatchObject({ claimed: 0 });
    expect(claims()).toHaveLength(2);
    expect(direct()).toEqual([]);
  });
});
