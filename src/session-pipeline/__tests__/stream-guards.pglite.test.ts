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
import { hostedRig } from "./search-index-hosted-harness.ts";
import { captureLogs } from "./search-index-skip-flags-harness.ts";

const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const MARGIN = 60_000;
const STREAM_ENV = { searchStreamCh: { ...UNLIMITED, deadlineMarginMs: MARGIN } };
/** Remaining time at which the stream deadline is exactly STREAM_MIN_START_MS away. */
const THRESHOLD = STREAM_MIN_START_MS + MARGIN;
const SECOND = { ...ID, sessionId: "stream:second" };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

/** Two sessions in one store, each too large for the whole read (TINY) and small enough to stream. */
async function twoScopes(): Promise<ExportedScope & { allKeys: string[] }> {
  const x = await exportStreamScope(generate(33), 1200);
  cleanup.push(x.dispose);
  const encoder = new TextEncoder();
  const second = await exportUnit({ store: x.store, keyRoot: "96bulls", grantId: "stream_parity", maxChunkBytes: 1200 },
    { orgId: SECOND.orgId, workspaceId: SECOND.workspaceId, source: SECOND.source, sessionId: SECOND.sessionId, firstMs: T0, lastMs: T0 + 60_000, machineIds: [], totals: {} },
    { fenced: async () => false, async *lines() { for (const { machine, record } of generate(35)) yield { machine, userId: USER, bytes: encoder.encode(JSON.stringify(record) + "\n") }; } });
  return { ...x, allKeys: [...x.keys, ...second.manifests.map((m) => m.manifestKey)] };
}

/** A pg client over the PGlite session that can drop its "connection" (emit 'error'), as pg's client does. */
class PgliteClient extends EventEmitter {
  released: Array<Error | undefined> = [];
  constructor(private pg: PGlite) { super(); }
  query(text: string, params?: unknown[]) { return this.pg.query(text, params as unknown[]); }
  release(error?: Error) { this.released.push(error); }
}
/** A HoopDb whose pool hands out PgliteClients (the last one is kept for the test). */
function pgliteHoopDb(pg: PGlite) {
  const clients: PgliteClient[] = [];
  const db = {
    pool: { connect: async () => { const c = new PgliteClient(pg); clients.push(c); return c; } },
    query: async (text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as unknown[])).rows }),
    end: async () => {},
  } as unknown as HoopDb;
  return { db, clients };
}

async function rig(opts: { env?: Partial<Runtime["env"]>; search?: Partial<SearchIndexDeps> } = {}) {
  const x = await twoScopes();
  const r = await hostedRig(x, cleanup);
  const messages = x.allKeys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
  const pool = pgliteHoopDb(r.db.pg);
  const base = r.consumer({ limits: TINY, stream: UNLIMITED, ...opts.search });
  const deps = { ...base, pointer: { ...base.pointer, db: { ...(base.pointer.db as object), pool: pool.db.pool } as never } };
  const rt = { env: { ...STREAM_ENV, ...opts.env }, deps } as unknown as Runtime;
  const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
  return { x, r, rt, handler, messages, pool };
}

/** Runs `work` with Date.now frozen at `at` (so the threshold arithmetic is exact). */
async function frozen<T>(at: number, work: () => Promise<T>): Promise<T> {
  const real = Date.now;
  Date.now = () => at;
  try { return await work(); } finally { Date.now = real; }
}
const ctx = (remaining: number) => ({ getRemainingTimeInMillis: () => remaining });
const reconciled = (lines: Array<Record<string, unknown>>) => lines.filter((l) => l.message === "search index reconciled");
const deferred = (lines: Array<Record<string, unknown>>) => lines.filter((l) => l.message === "search index stream deferred, requeued");

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

  async function scheduled(remaining: number | undefined, prepare?: (s: Awaited<ReturnType<typeof rig>>) => void) {
    const s = await rig({ env: { searchJobs: { claim: 5 } } });
    prepare?.(s);
    // The pointer stage for both scopes (search unarmed for this batch: whole read refused for size, nothing streamed).
    await processBatch(s.messages, s.r.consumer({ limits: TINY }));
    await wakes(s.r.db.pg);
    // Past the wakes' coalescing delay: both jobs are claimable now.
    await s.r.db.pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");
    const saved = process.env.SEARCH_JOBS_ENABLED;
    process.env.SEARCH_JOBS_ENABLED = "1";
    try {
      const run = () => s.handler({ searchJobs: true } as never, (remaining === undefined ? undefined : ctx(remaining)) as never) as Promise<unknown>;
      // Not frozen: PGlite's now() follows the JS clock, and the claim compares visible_at with it. Below the
      // threshold, elapsed time only moves the deadline closer.
      return { ...s, out: await captureLogs(run) };
    } finally {
      if (saved === undefined) delete process.env.SEARCH_JOBS_ENABLED; else process.env.SEARCH_JOBS_ENABLED = saved;
    }
  }

  test("ample time: one job streams (indexed, done), the other is deferred (busy, a short retry as on 9d1adf84)", async () => {
    const { out } = await scheduled(300_000);
    expect(out.result).toEqual({ ok: true });
    const s = settled(out.lines);
    expect(s.filter((x) => x.outcome === "indexed")).toHaveLength(1);
    expect(s.filter((x) => x.outcome === "busy" && x.reason === "stream_deferred")).toHaveLength(1);
  }, 120_000);

  test("just below the threshold: no job streams; both are busy (stream_deferred) and retried after the short busy delay", async () => {
    const { out, r } = await scheduled(THRESHOLD - 1);
    expect(out.result).toEqual({ ok: true });
    expect(settled(out.lines)).toEqual([
      expect.objectContaining({ outcome: "busy", reason: "stream_deferred" }),
      expect.objectContaining({ outcome: "busy", reason: "stream_deferred" }),
    ]);
    const { rows } = await r.db.pg.query<{ secs: number }>("SELECT extract(epoch FROM visible_at - now())::int AS secs FROM sessions.pipeline_jobs WHERE stage='search-index'");
    expect(rows).toHaveLength(2);
    for (const row of rows) expect(Math.abs(row.secs - SEARCH_JOB_BUSY_RETRY_SECONDS)).toBeLessThanOrEqual(5);
  }, 120_000);

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

  test("no context: still one streamed scope per run (slot), no deadline", async () => {
    const { out } = await scheduled(undefined);
    const s = settled(out.lines);
    expect(s.filter((x) => x.outcome === "indexed")).toHaveLength(1);
    expect(s.filter((x) => x.outcome === "busy" && x.reason === "stream_deferred")).toHaveLength(1);
  }, 120_000);
});
