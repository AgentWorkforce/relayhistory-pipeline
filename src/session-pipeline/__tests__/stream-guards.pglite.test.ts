/**
 * SEARCH_INDEX_STREAMING_CH=1 guards ported from the deployed backport lineage (59b74b18, 6264d119, 51f876b6):
 *  - the guarded search transaction: a pg client 'error' (a dropped connection) mid-stream rejects the pass instead
 *    of crashing the invocation; the client is destroyed, the listener removed, the messages fail for redelivery,
 *    and the redelivered batch completes;
 *  - STREAM_MIN_START_MS: no stream starts with less than 120 s before the invocation's stream deadline;
 *  - one streamed scope per invocation also without a Lambda context;
 *  - the same guards on the scheduled {searchJobs:true} path, with M2's settlement mapping unchanged.
 * Through the real exported handler (createPipelineLambdaHandler) over a migrated PGlite (search-index-hosted-harness.ts)
 * and the legacy ClickHouse fake. Synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import type { PGlite } from "@electric-sql/pglite";
import { STREAM_MIN_START_MS, type SearchIndexDeps } from "../search-index.ts";
import { processBatch } from "../consumer.ts";
import { enqueueSearchWake, SEARCH_JOB_BUSY_RETRY_SECONDS } from "../search-jobs.ts";
import { createPipelineLambdaHandler, invocationDeps, invocationSearchDeps, searchTransaction, transaction, type Runtime } from "../lambda.ts";
import type { HoopDb } from "../../shared/db.ts";
import { ID, T0, USER, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { MARGIN, STREAM_ENV, THRESHOLD, TINY, UNLIMITED, ctx, deferred, frozen, reconciled, streamRig } from "./stream-rig.ts";
import { captureLogs } from "./search-index-skip-flags-harness.ts";


const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

const rig = (opts: Parameters<typeof streamRig>[1] = {}) => streamRig(cleanup, opts);

describe("STREAM_MIN_START_MS through the real handler", () => {
  test("remaining just below the threshold: no stream starts; both scopes deferred and requeued, nothing failed", async () => {
    const { handler, messages, r } = await rig();
    const { result, lines } = await captureLogs(() => frozen(1_000_000, () => handler({ Records: messages } as never, ctx(THRESHOLD - 1) as never) as Promise<unknown>));
    expect(result).toEqual({ batchItemFailures: [] });
    expect(deferred(lines)).toHaveLength(2);
    expect(reconciled(lines)).toEqual([]);
    expect(r.requeued).toHaveLength(2);
  }, 120_000);

  test("remaining exactly at the threshold: the first scope streams (complete), the second is deferred (one slot)", async () => {
    const { handler, messages, r } = await rig();
    const { result, lines } = await captureLogs(() => frozen(1_000_000, () => handler({ Records: messages } as never, ctx(THRESHOLD) as never) as Promise<unknown>));
    expect(result).toEqual({ batchItemFailures: [] });
    expect(reconciled(lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(deferred(lines)).toHaveLength(1);
    expect(r.requeued).toHaveLength(1);
  }, 120_000);
});

describe("no Lambda context", () => {
  test("one streamed scope per invocation: the second is deferred and requeued (9d1adf84 streamed both, unbounded)", async () => {
    const { handler, messages, r } = await rig();
    const { result, lines } = await captureLogs(() => handler({ Records: messages } as never) as Promise<unknown>);
    expect(result).toEqual({ batchItemFailures: [] });
    expect(reconciled(lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(deferred(lines)).toHaveLength(1);
    expect(r.requeued).toHaveLength(1);
  }, 120_000);

  test("invocationSearchDeps: slot without a deadline; unarmed, the deps themselves", () => {
    const search = { stream: UNLIMITED } as unknown as SearchIndexDeps;
    const armed = { env: STREAM_ENV, deps: { search } } as unknown as Runtime;
    for (const c of [undefined, {} as never, ctx(Number.NaN)]) {
      const s = invocationSearchDeps(armed, c)!;
      expect(s.streamSlot).toEqual({ used: false });
      expect("streamDeadline" in s).toBe(false);
    }
    expect(invocationSearchDeps(armed, ctx(300_000), () => 5)!.streamDeadline).toEqual({ at: 5 + 300_000 - MARGIN });
    const plain = { env: {}, deps: { search: {} } } as unknown as Runtime;
    expect(invocationSearchDeps(plain, ctx(300_000))).toBe(plain.deps.search);
    expect(invocationDeps(plain, ctx(300_000))).toBe(plain.deps);
  });
});

describe("a dropped connection mid-stream (guarded search transaction)", () => {
  test("rejects the pass, destroys the client, fails the batch's messages; the redelivered batch completes", async () => {
    const first = await rig();
    const { r, rt, pool, messages } = first;
    rt.deps.search = { ...rt.deps.search!, transact: searchTransaction(pool.db, STREAM_ENV) };
    // Drop the connection when the streamed pass reads its first keyset page, with no statement in flight.
    const select = r.fake.ch.select.bind(r.fake.ch);
    let dropped = false;
    r.fake.ch.select = (async (q: string, o?: never) => {
      if (!dropped && q.includes("transcript_events") && q.includes("ORDER BY")) {
        dropped = true;
        const client = pool.clients.at(-1)!;
        setImmediate(() => client.emit("error", Object.assign(new Error("Connection terminated unexpectedly"), { code: "57P01" })));
        await new Promise((res) => setImmediate(res));
      }
      return select(q, o);
    }) as never;
    let uncaught = 0;
    const onUncaught = () => { uncaught += 1; };
    process.on("uncaughtException", onUncaught);
    let out: Awaited<ReturnType<typeof captureLogs>>;
    try {
      out = await captureLogs(() => first.handler({ Records: messages.slice(0, 1) } as never) as Promise<unknown>);
    } finally {
      process.off("uncaughtException", onUncaught);
      // The broken "connection" left its PGlite transaction open (a real one is destroyed): close it.
      await r.db.pg.query("ROLLBACK").catch(() => {});
    }
    expect(dropped).toBe(true);
    expect(uncaught).toBe(0);
    expect(out.result).toEqual({ batchItemFailures: [{ itemIdentifier: "m-0" }] });
    expect(out.lines.filter((l) => l.message === "search index failed")).toEqual([expect.objectContaining({ error_code: "57P01" })]);
    const broken = pool.clients.find((c) => c.released[0] instanceof Error)!;
    expect(broken.released).toHaveLength(1);
    expect(broken.listenerCount("error")).toBe(0);
    // SQS redelivers the failed message: the next batch streams it to completion.
    const again = await captureLogs(() => first.handler({ Records: messages.slice(0, 1) } as never) as Promise<unknown>);
    expect(again.result).toEqual({ batchItemFailures: [] });
    expect(reconciled(again.lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(pool.clients.at(-1)!.released).toEqual([undefined]);
  }, 120_000);

  test("transaction(): guarded and unguarded run the same statements; unguarded, an emitted error has no listener (it would crash)", async () => {
    const statements: Record<string, string[]> = {};
    for (const guarded of [false, true]) {
      const seen: string[] = [];
      const client = Object.assign(new EventEmitter(), { query: async (t: string) => { seen.push(t); return { rows: [] }; }, release: () => {} });
      const db = { pool: { connect: async () => client } } as unknown as HoopDb;
      await transaction(db, async (tx) => tx.query("SELECT 1"), guarded);
      statements[String(guarded)] = seen;
      expect(client.listenerCount("error")).toBe(0);
      if (!guarded) expect(() => client.emit("error", new Error("dropped"))).toThrow("dropped");
    }
    expect(statements.true).toEqual(statements.false!);
    expect(statements.false).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SELECT 1", "COMMIT"]);
  });

  test("searchTransaction is guarded exactly when streaming is armed", async () => {
    for (const [env, guarded] of [[{}, false], [STREAM_ENV, true]] as const) {
      let listening = -1;
      const client = Object.assign(new EventEmitter(), { query: async () => ({ rows: [] }), release: () => {} });
      const db = { pool: { connect: async () => client } } as unknown as HoopDb;
      await searchTransaction(db, env as never)(async () => { listening = client.listenerCount("error"); });
      expect(listening).toBe(guarded ? 1 : 0);
    }
  });
});

describe("the scheduled {searchJobs:true} path carries the same guards; M2 settlement mapping unchanged", () => {
  /** Both scopes' wakes enqueued at their directory epochs. */
  async function wakes(pg: PGlite) {
    const { rows } = await pg.query<{ org_id: string; workspace_id: string; harness: string; session_id: string; epoch: string }>(
      "SELECT org_id, workspace_id, harness, session_id, event_projection_epoch::text AS epoch FROM sessions.session_directory ORDER BY session_id");
    expect(rows).toHaveLength(2);
    for (const [n, d] of rows.entries())
      await enqueueSearchWake(pg, {
        v: 1, stage: "search-index", ctx: { traceId: `00000000-0000-4000-8000-00000000001${n}`, orgId: d.org_id, workspaceId: d.workspace_id, harness: d.harness, sessionId: d.session_id },
        workKey: ["search", d.org_id, d.workspace_id, d.harness, d.session_id].join("\u001f"), revision: 1,
        payload: { epoch: Number(d.epoch), clock: { source: "publish", at: "2026-11-01T12:00:00.000Z" } },
      } as never);
  }
  const settled = (lines: Array<Record<string, unknown>>) => lines.filter((l) => l.message === "search job settled").map((l) => ({ outcome: l.outcome, state: l.state, reason: l.reason }));

  /** A rig whose two scopes are committed (pointer stage only) and whose two wakes are claimable now. */
  async function jobsRig(prepare?: (s: Awaited<ReturnType<typeof rig>>) => void) {
    const s = await rig({ env: { searchJobs: { claim: 5 } } });
    prepare?.(s);
    // The pointer stage for both scopes (search unarmed for this batch: whole read refused for size, nothing streamed).
    await processBatch(s.messages, s.r.consumer({ limits: TINY }));
    await wakes(s.r.db.pg);
    const visible = () => s.r.db.pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");
    await visible();
    /** One scheduled invocation (not frozen: PGlite's now() follows the JS clock, and the claim compares visible_at with it). */
    const run = async (remaining: number | undefined) => {
      const saved = process.env.SEARCH_JOBS_ENABLED;
      process.env.SEARCH_JOBS_ENABLED = "1";
      try {
        return await captureLogs(() => s.handler({ searchJobs: true } as never, (remaining === undefined ? undefined : ctx(remaining)) as never) as Promise<unknown>);
      } finally {
        if (saved === undefined) delete process.env.SEARCH_JOBS_ENABLED; else process.env.SEARCH_JOBS_ENABLED = saved;
      }
    };
    const jobs = async () => (await s.r.db.pg.query<{ id: string; attempts: number; leased: boolean; dlq: boolean }>(
      "SELECT id::text, attempts, lease_id IS NOT NULL AS leased, dlq FROM sessions.pipeline_jobs WHERE stage='search-index' ORDER BY id")).rows;
    const letters = async () => (await s.r.db.pg.query("SELECT id FROM sessions.pipeline_dead_letters WHERE stage='search-index'")).rows.length;
    return { ...s, run, visible, jobs, letters };
  }
  async function scheduled(remaining: number | undefined, prepare?: (s: Awaited<ReturnType<typeof rig>>) => void) {
    const j = await jobsRig(prepare);
    return { ...j, out: await j.run(remaining) };
  }
  const runState = (lines: Array<Record<string, unknown>>) => lines.find((l) => l.message === "search jobs run")?.state;

  test("ample time: one job streams (indexed, done); the slot is then spent, so the other is not claimed (no attempt)", async () => {
    const { out, jobs, letters, run } = await scheduled(300_000);
    expect(out.result).toEqual({ ok: true });
    expect(settled(out.lines)).toEqual([expect.objectContaining({ outcome: "indexed" })]);
    expect(runState(out.lines)).toBe("stream_exhausted");
    expect((await jobs()).map((j) => [j.attempts, j.leased])).toEqual([[0, false]]);
    // The next invocation streams it.
    const next = await run(300_000);
    expect(settled(next.lines)).toEqual([expect.objectContaining({ outcome: "indexed" })]);
    expect(await jobs()).toEqual([]);
    expect(await letters()).toBe(0);
  }, 120_000);

  test("just below the threshold, twelve invocations: nothing is claimed, no attempt is spent, no letter; then both complete", async () => {
    const j = await jobsRig();
    for (let n = 0; n < 12; n++) {
      await j.visible();
      const out = await j.run(THRESHOLD - 1);
      expect(out.result).toEqual({ ok: true });
      expect(settled(out.lines)).toEqual([]);
      expect(runState(out.lines)).toBe("stream_exhausted");
    }
    expect((await j.jobs()).map((x) => [x.attempts, x.leased, x.dlq])).toEqual([[0, false, false], [0, false, false]]);
    expect(await j.letters()).toBe(0);
    for (let n = 0; n < 2; n++) {
      await j.visible();
      expect(settled((await j.run(300_000)).lines)).toEqual([expect.objectContaining({ outcome: "indexed" })]);
    }
    expect(await j.jobs()).toEqual([]);
    expect(await j.letters()).toBe(0);
  }, 300_000);

  test("mixed SQS, scheduled and probe load: a held job recovers through the probe lane, no attempt is lost to a deferral", async () => {
    const j = await jobsRig();
    const [first, held] = await j.jobs();
    // The second job is contention-held with its hourly probe due (the probe lane admits it, the ordinary lane skips it).
    const epoch = (await j.r.db.pg.query<{ epoch: string }>("SELECT message->'payload'->>'epoch' AS epoch FROM sessions.pipeline_jobs WHERE id=$1::bigint", [held!.id])).rows[0]!.epoch;
    await j.r.db.pg.query(
      `INSERT INTO sessions.search_job_contention (job_id, epoch, first_contended_at, last_contended_at, contention_count, held_at, next_probe_at)
       VALUES ($1::bigint, $2::bigint, now() - interval '25 hours', now() - interval '1 hour', 99, now() - interval '1 hour', now() - interval '1 minute')`,
      [held!.id, epoch]);
    // An SQS batch for the first scope only, ample time: its pass streams (the slot); the follow-on claims nothing.
    const sqs = await captureLogs(() => j.handler({ Records: j.messages.slice(0, 1) } as never, ctx(300_000) as never) as Promise<unknown>);
    expect(sqs.result).toEqual({ batchItemFailures: [] });
    expect(reconciled(sqs.lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(settled(sqs.lines)).toEqual([]);
    // Scheduled, too little time: neither lane claims (the probe keeps its due time, the job its attempts).
    await j.visible();
    const low = await j.run(THRESHOLD - 1);
    expect(settled(low.lines)).toEqual([]);
    expect((await j.jobs()).map((x) => x.attempts)).toEqual([0, 0]);
    // Scheduled, ample time: the probe lane takes the held job first and streams it; then the slot is spent.
    await j.visible();
    const ample = await j.run(300_000);
    expect(ample.lines.filter((l) => l.message === "search jobs run")).toEqual([expect.objectContaining({ count: 1, state: "stream_exhausted" })]);
    expect(settled(ample.lines)).toEqual([expect.objectContaining({ outcome: "indexed" })]);
    expect((await j.jobs()).map((x) => x.id)).toEqual([first!.id]);
    expect((await j.r.db.pg.query("SELECT 1 FROM sessions.search_job_contention")).rows).toEqual([]);
    // The first scope is already current from the SQS pass: its job settles unchanged.
    await j.visible();
    expect(settled((await j.run(300_000)).lines)).toEqual([expect.objectContaining({ outcome: "unchanged" })]);
    expect(await j.jobs()).toEqual([]);
    expect(await j.letters()).toBe(0);
  }, 300_000);

  test("at the deadline margin (the M2 analogue of stream_batch_deadline): nothing is claimed, attempts unchanged", async () => {
    const { out, jobs, letters } = await scheduled(MARGIN);
    expect(settled(out.lines)).toEqual([]);
    expect(runState(out.lines)).toBe("deadline");
    expect((await jobs()).map((j) => j.attempts)).toEqual([0, 0]);
    expect(await letters()).toBe(0);
  }, 120_000);

  test("real handler on migrated PGlite: the start minimum crossed after the claim settles busy in 062300 (attempt given back, receipt gone)", async () => {
    const j = await jobsRig();
    // The decision transaction of the first pass is slowed by 300 ms: the claim (checked with ~120.1 s left) happens,
    // then the pass finds less than STREAM_MIN_START_MS before its deadline and defers.
    const transact = j.rt.deps.search!.transact;
    let slowed = false;
    j.rt.deps.search = { ...j.rt.deps.search!, transact: async (work) => {
      if (!slowed) { slowed = true; await new Promise((res) => setTimeout(res, 300)); }
      return transact(work);
    } };
    const out = await j.run(THRESHOLD + 100);
    expect(slowed).toBe(true);
    expect(out.lines.filter((l) => l.message === "search job settled")).toEqual([expect.objectContaining({ outcome: "busy", reason: "stream_deferred", state: "busy", attempt: 1 })]);
    const rows = (await j.r.db.pg.query<{ id: string; attempts: number; leased: boolean; dlq: boolean; delay: number }>(
      "SELECT id::text, attempts, lease_id IS NOT NULL AS leased, dlq, extract(epoch FROM visible_at - now())::int AS delay FROM sessions.pipeline_jobs WHERE stage='search-index' ORDER BY id")).rows;
    // The deferred job: attempts back to 0, no lease, no DLQ, held 30 s; the other was never claimed (the slot check).
    expect(rows.map((r) => [r.attempts, r.leased, r.dlq])).toEqual([[0, false, false], [0, false, false]]);
    expect(rows.some((r) => Math.abs(r.delay - 30) <= 5)).toBe(true);
    expect((await j.r.db.pg.query("SELECT 1 FROM sessions.search_job_claims")).rows).toEqual([]);
    expect((await j.r.db.pg.query<{ n: number }>("SELECT contention_count AS n FROM sessions.search_job_contention")).rows).toEqual([{ n: 1 }]);
    expect(await j.letters()).toBe(0);
  }, 120_000);

  test("a fence failure after COMMIT is observable to M2: the job fails as an error and is retried; the scope stays owed", async () => {
    const j = await jobsRig();
    const select = j.r.fake.ch.select.bind(j.r.fake.ch);
    let reads = 0;
    j.r.fake.ch.select = (async (q: string, o?: never) => {
      if (q.includes("FROM search_ingest_session_state") && ++reads === 3) throw new Error("synthetic fence timeout");
      return select(q, o);
    }) as never;
    const out = await j.run(300_000);
    expect(out.lines.filter((l) => l.message === "search job failed")).toEqual([expect.objectContaining({ error_code: "search_completion_unrecorded", state: "retried" })]);
    expect(out.lines.filter((l) => l.message === "search index completion not recorded")).toEqual([expect.objectContaining({ reason: "completion_fence_unreadable" })]);
    expect((await j.jobs()).some((x) => x.attempts === 1 && !x.leased)).toBe(true);
  }, 120_000);

  test("a job claimed and then deferred (the start minimum crossed after the claim) settles busy: the attempt is given back", async () => {
    const { searchSettlementFor } = await import("../search-jobs.ts");
    expect(searchSettlementFor({ outcome: "busy", reasons: ["stream_deferred"] }, 3)).toBe("busy");
    expect(searchSettlementFor({ outcome: "busy", reasons: ["epoch_behind_wake"] }, 3)).toEqual({ retry: SEARCH_JOB_BUSY_RETRY_SECONDS });
  });

  test("a dropped connection mid-stream: the job fails as an error result, is retried on M2's backoff, nothing crashes", async () => {
    let uncaught = 0;
    const onUncaught = () => { uncaught += 1; };
    process.on("uncaughtException", onUncaught);
    let dropped = false;
    try {
      const { out, r } = await scheduled(300_000, ({ r, rt, pool }) => {
        rt.deps.search = { ...rt.deps.search!, transact: searchTransaction(pool.db, STREAM_ENV) };
        const select = r.fake.ch.select.bind(r.fake.ch);
        r.fake.ch.select = (async (q: string, o?: never) => {
          if (!dropped && q.includes("transcript_events") && q.includes("ORDER BY")) {
            dropped = true;
            const client = pool.clients.at(-1)!;
            setImmediate(() => client.emit("error", Object.assign(new Error("Connection terminated unexpectedly"), { code: "57P01" })));
            await new Promise((res) => setImmediate(res));
            // The broken "connection" leaves its PGlite transaction open (a real one is destroyed): close it after.
            setImmediate(() => { void r.db.pg.query("ROLLBACK").catch(() => {}); });
          }
          return select(q, o);
        }) as never;
      });
      expect(dropped).toBe(true);
      expect(uncaught).toBe(0);
      expect(out.result).toEqual({ ok: true });
      const failed = out.lines.filter((l) => l.message === "search job failed");
      expect(failed).toEqual([expect.objectContaining({ error_code: "57P01", state: "retried" })]);
      const { rows } = await r.db.pg.query<{ leased: boolean; attempts: number }>("SELECT lease_id IS NOT NULL AS leased, attempts FROM sessions.pipeline_jobs WHERE stage='search-index' ORDER BY id");
      expect(rows.some((row) => row.attempts === 1 && !row.leased)).toBe(true);
    } finally {
      process.off("uncaughtException", onUncaught);
    }
  }, 120_000);

  test("no context: still one streamed scope per run (slot), no deadline; the other is not claimed", async () => {
    const { out, jobs } = await scheduled(undefined);
    expect(settled(out.lines)).toEqual([expect.objectContaining({ outcome: "indexed" })]);
    expect((await jobs()).map((j) => [j.attempts, j.leased])).toEqual([[0, false]]);
  }, 120_000);
});
