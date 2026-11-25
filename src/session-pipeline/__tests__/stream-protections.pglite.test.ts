/**
 * The deployed backport's remaining streamed-reconcile protections on main (SEARCH_INDEX_STREAMING_CH=1; e79df11b,
 * 6264d119, 51f876b6, c9a0c68e; ecb5d981 and 9279264f for the telemetry they carry), through the real exported
 * Lambda handler over a migrated PGlite and the ClickHouse fake (stream-rig.ts):
 *  - stream_batch_deadline: past the stream deadline the rest of the batch is requeued, with the original upload
 *    time carried (s3_event_age_ms end to end); a failing requeue fails exactly that scope's messages;
 *  - the 30 s tail reserve before the first and before every actual delete statement;
 *  - the lock-checking keepalive: a lost scope lock stops the pass with no further write and no finalization;
 *  - needs_full = 1 recorded before the streamed pass reads, kept through an interruption, so the next pass is full;
 *  - strict keyset pages (length, order, cursor progress) before any projection write;
 *  - the init check that the margin leaves the start minimum; contender_seats on the search pass lines.
 * Synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { STREAM_DELETE_KEY_TOO_LARGE, STREAM_LOCK_HELD_SQL, STREAM_TAIL_RESERVE_MS, SCAN_PAGE_INVALID, deleteStatementChunks, type SearchIndexDeps } from "../search-index.ts";
import { scopeLockKey } from "../pointer.ts";
import { requeueBody } from "../s3-events.ts";
import { lambdaEnv } from "../lambda.ts";
import { sanitizeLogField } from "../../shared/log.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { SCOPE } from "../../session-read-api/__tests__/stream-fixture.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { captureLogs, realHandler } from "./search-index-skip-flags-harness.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";
import { exportStreamScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { crossManifestAcceptanceLines } from "./acceptance-fixture.ts";
import { MARGIN, ctx, reconciled, streamRig } from "./stream-rig.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

const NOW = Date.parse("2026-11-01T12:00:00.000Z");
const UPLOADED = "2026-11-01T11:00:00.000Z";
const REMAINING = 300_000;
const DEADLINE = NOW + REMAINING - MARGIN;

/** An S3 ObjectCreated notification body for `key`, uploaded at UPLOADED. */
const s3Body = (key: string) => JSON.stringify({
  Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: UPLOADED, s3: { bucket: { name: "stream-parity" }, object: { key, size: 10, eTag: "etag-synthetic", sequencer: "0A" } } }],
});

/** A rig with a controllable frozen clock (Date.now and the consumer's own clock) and a recording requeue. */
async function clocked(opts: Parameters<typeof streamRig>[1] = {}) {
  const s = await streamRig(cleanup, opts);
  const clock = { ms: NOW };
  const sent: Array<{ key: string; carried?: string }> = [];
  s.rt.deps.requeue = async (key, carried) => { sent.push({ key, ...(carried ? { carried } : {}) }); };
  s.rt.deps.now = () => clock.ms;
  // Each pass's own time follows the clock (the harness pins one instant, under which a later pass could never
  // finalize over an earlier pass's rows).
  s.rt.deps.search = { ...s.rt.deps.search!, now: () => new Date(clock.ms) };
  const run = async <T,>(work: () => Promise<T>): Promise<T> => {
    const real = Date.now;
    Date.now = () => clock.ms;
    try { return await work(); } finally { Date.now = real; }
  };
  const send = (messages: Array<{ messageId: string; body: string }>, remaining: number | undefined = REMAINING) =>
    run(() => captureLogs(() => s.handler({ Records: messages } as never, (remaining === undefined ? undefined : ctx(remaining)) as never) as Promise<unknown>));
  const s3Messages = s.x.allKeys.map((key, n) => ({ messageId: `s3-${n}`, body: s3Body(key) }));
  /** The latest state row of the first scope. */
  const latestState = () => [...s.r.fake.state].filter((r) => r.session_id === SCOPE.sessionId).sort((a, b) => String(a.synced_at) < String(b.synced_at) ? -1 : 1).at(-1);
  const deletes = () => s.r.fake.calls.filter((c) => c.ch === "command" && String(c.query).startsWith("DELETE FROM"));
  return { ...s, clock, sent, send, s3Messages, latestState, deletes };
}

/** The first scope's directory readiness (true only after a complete pass's directory write committed). */
const readyOf = async (c: Awaited<ReturnType<typeof clocked>>) => (await c.r.db.pg.query<{ ready: boolean }>(
  "SELECT event_projection_ready AS ready FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, c.x.canonicalId])).rows[0]?.ready;

/** Index the first scope completely once (ample time), then push `n` stale rows the next pass must delete. */
async function indexedWithStale(c: Awaited<ReturnType<typeof clocked>>, n: number) {
  const first = await c.send(c.s3Messages.slice(0, 1));
  expect(reconciled(first.lines)).toEqual([expect.objectContaining({ ok: true })]);
  const template = c.r.fake.rows.find((r) => r.session_id === SCOPE.sessionId || r.session_id.startsWith("stream"))!;
  for (let i = 0; i < n; i++) c.r.fake.rows.push({ ...template, kind: "history", event_id: `stale-${String(i).padStart(4, "0")}`, fingerprint: "s3stale", version: template.version + 1 });
  c.r.fake.calls.length = 0;
  c.clock.ms += 1_000;
}

describe("stream_batch_deadline (SQS path)", () => {
  test("past the deadline mid-batch the rest is requeued with the original upload time; the redelivery logs s3_event_age_ms from it", async () => {
    const c = await clocked();
    // The first scope's final state row: the batch's stream deadline passes right after it.
    const insert = c.r.fake.ch.insert.bind(c.r.fake.ch);
    c.r.fake.ch.insert = (async (table: string, rows: Array<Record<string, unknown>>, o?: never) => {
      await insert(table, rows, o);
      if (table === SESSION_STATE_TABLE && rows.some((r) => r.needs_full === 0)) c.clock.ms = DEADLINE + 1;
    }) as never;
    const out = await c.send(c.s3Messages);
    expect(out.result).toEqual({ batchItemFailures: [] });
    // Logged after the pass, at the moved clock: still measured from the upload.
    expect(reconciled(out.lines)).toEqual([expect.objectContaining({ ok: true, queue_origin: "s3", s3_event_age_ms: DEADLINE + 1 - Date.parse(UPLOADED) })]);
    expect(out.lines.filter((l) => l.message === "search index past the batch deadline, requeued")).toEqual([expect.objectContaining({ reason: "stream_batch_deadline" })]);
    expect(c.sent).toEqual([{ key: c.x.allKeys.at(-1)!, carried: UPLOADED }]);
    // The redelivered requeue body, in a fresh invocation an hour later: the age is still from the upload.
    c.r.fake.ch.insert = insert as never;
    c.clock.ms = NOW + 3_600_000;
    const again = await c.send([{ messageId: "rq-0", body: requeueBody(c.sent[0]!.key, c.sent[0]!.carried) }]);
    expect(reconciled(again.lines)).toEqual([expect.objectContaining({ ok: true, queue_origin: "requeue", s3_event_age_ms: NOW + 3_600_000 - Date.parse(UPLOADED) })]);
  }, 120_000);

  test("a requeue that fails fails exactly that scope's messages (redelivered); the finished scope is acknowledged", async () => {
    const c = await clocked();
    c.rt.deps.requeue = async () => { throw Object.assign(new Error("synthetic SendMessage failure"), { code: "ServiceUnavailable" }); };
    const insert = c.r.fake.ch.insert.bind(c.r.fake.ch);
    c.r.fake.ch.insert = (async (table: string, rows: Array<Record<string, unknown>>, o?: never) => {
      await insert(table, rows, o);
      if (table === SESSION_STATE_TABLE && rows.some((r) => r.needs_full === 0)) c.clock.ms = DEADLINE + 1;
    }) as never;
    const out = await c.send(c.s3Messages);
    // Only the second scope's message fails (s3-1, the last key); s3-0 (the streamed scope) is acknowledged.
    expect(out.result).toEqual({ batchItemFailures: c.s3Messages.slice(c.x.keys.length).map((m) => ({ itemIdentifier: m.messageId })) });
    expect(out.lines.filter((l) => l.message === "search index failed")).toEqual([expect.objectContaining({ error_code: "ServiceUnavailable" })]);
  }, 120_000);
});

describe("the 30 s tail reserve", () => {
  test("a pass entering the reserve before its tail defers safely: no delete, no directory write, needs_full=1; the next pass completes", async () => {
    const c = await clocked();
    await indexedWithStale(c, 3);
    const deadline = c.clock.ms + REMAINING - MARGIN;
    // Into the reserve (20 s before the deadline) while the keyset scan runs: the scan and pass 4 still run.
    const select = c.r.fake.ch.select.bind(c.r.fake.ch);
    c.r.fake.ch.select = (async (q: string, o?: never) => {
      if (q.includes("ORDER BY ts, session_id")) c.clock.ms = deadline - (STREAM_TAIL_RESERVE_MS - 10_000);
      return select(q, o);
    }) as never;
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [] });
    expect(out.lines.filter((l) => l.message === "search index partial")).toEqual([expect.objectContaining({ reason: "scope_too_large,stream_deadline" })]);
    expect(c.deletes()).toEqual([]);
    expect(c.r.fake.rows.filter((r) => r.event_id.startsWith("stale-"))).toHaveLength(3);
    expect(c.latestState()!.needs_full).toBe(1);
    c.r.fake.ch.select = select as never;
    // Later than the stopped pass's own state rows (a pass never finalizes over a newer marker).
    c.clock.ms += 60_000;
    const next = await c.send(c.s3Messages.slice(0, 1));
    expect(reconciled(next.lines)).toEqual([expect.objectContaining({ ok: true, deleted: 3 })]);
    expect(c.latestState()!.needs_full).toBe(0);
  }, 120_000);

  test("every actual delete statement is guarded: entering the reserve after the first stops before the second", async () => {
    const c = await clocked();
    await indexedWithStale(c, 700);
    // This invocation's own deadline (the clock moved since NOW).
    const deadline = c.clock.ms + REMAINING - MARGIN;
    const command = c.r.fake.ch.command.bind(c.r.fake.ch);
    c.r.fake.ch.command = (async (q: string, o?: never) => {
      await command(q, o);
      if (q.startsWith("DELETE FROM")) c.clock.ms = deadline - (STREAM_TAIL_RESERVE_MS - 1_000);
    }) as never;
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [] });
    expect(c.deletes()).toHaveLength(1);
    expect(c.r.fake.rows.filter((r) => r.event_id.startsWith("stale-"))).toHaveLength(200);
    expect(out.lines.filter((l) => l.message === "search index reconciled")).toEqual([expect.objectContaining({ ok: false, deleted: 500 })]);
    expect(c.latestState()!.needs_full).toBe(1);
  }, 120_000);

  test("the gap after the final delete: past the deadline inside the last DELETE, the pass is not finalized", async () => {
    const c = await clocked();
    await indexedWithStale(c, 1);
    const deadline = c.clock.ms + REMAINING - MARGIN;
    const command = c.r.fake.ch.command.bind(c.r.fake.ch);
    c.r.fake.ch.command = (async (q: string, o?: never) => {
      await command(q, o);
      if (q.startsWith("DELETE FROM")) c.clock.ms = deadline + 1;
    }) as never;
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [] });
    expect(c.deletes()).toHaveLength(1);
    expect(reconciled(out.lines)).toEqual([expect.objectContaining({ ok: false, deleted: 1, reason: "scope_too_large,stream_deadline" })]);
    expect(c.latestState()!.needs_full).toBe(1);
    expect(await readyOf(c)).not.toBe(true);
  }, 120_000);

  test("one delete key whose literal could exceed the statement budget: nothing deleted, the pass is owed", async () => {
    const c = await clocked();
    await indexedWithStale(c, 0);
    const template = c.r.fake.rows[0]!;
    c.r.fake.rows.push({ ...template, kind: "history", machine_id: "m".repeat(300_000), event_id: "stale-huge", fingerprint: "s3stale", version: template.version + 1 });
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [] });
    expect(reconciled(out.lines)).toEqual([expect.objectContaining({ ok: false, reason: STREAM_DELETE_KEY_TOO_LARGE })]);
    expect(c.deletes()).toEqual([]);
    expect(c.latestState()!.needs_full).toBe(1);
    const huge = { machineId: "m".repeat(300_000), sessionId: "s", workspaceId: "w", source: "claude", kind: "history", eventId: "e", tsMs: 1, fingerprint: "f" };
    expect(deleteStatementChunks(SCOPE, [huge])).toBeNull();
  }, 120_000);

  test("deleteStatementChunks: at most 500 keys and within the byte budget per statement", () => {
    const id = (n: number, len = 10) => ({ machineId: "m".repeat(len), sessionId: "s", workspaceId: "w", source: "claude", kind: "history", eventId: `e${n}`, tsMs: 1, fingerprint: "f" });
    expect(deleteStatementChunks(SCOPE, Array.from({ length: 1_201 }, (_, n) => id(n))).map((c) => c.length)).toEqual([500, 500, 201]);
    const wide = deleteStatementChunks(SCOPE, Array.from({ length: 600 }, (_, n) => id(n, 2_000)))!;
    expect(wide.length).toBeGreaterThan(2);
    expect(wide.every((c) => c.length < 500)).toBe(true);
  });
});

describe("the lock-checking keepalive", () => {
  /** Search transactions whose `n`th keepalive lock-continuity check (STREAM_LOCK_HELD_SQL) answers ok = false. */
  function losingLock(c: Awaited<ReturnType<typeof clocked>>, n: number): SearchIndexDeps["transact"] {
    const key = scopeLockKey(SCOPE, "search");
    let seen = 0;
    return (work) => c.r.transact((tx) => work({
      query: (text: string, params?: unknown[]) => {
        if (text === STREAM_LOCK_HELD_SQL && params?.[0] === key && ++seen === n) return Promise.resolve({ rows: [{ ok: false }] }) as never;
        return tx.query(text, params);
      },
    }));
  }
  const ready = async (c: Awaited<ReturnType<typeof clocked>>) => (await c.r.db.pg.query<{ ready: boolean }>(
    "SELECT event_projection_ready AS ready FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, c.x.canonicalId])).rows[0]?.ready;

  test("lost after the scan (2nd continuity check): no row inserted, nothing deleted, no directory write, no final state; the message fails", async () => {
    const c = await clocked();
    c.rt.deps.search = { ...c.rt.deps.search!, transact: losingLock(c, 2) };
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [{ itemIdentifier: "s3-0" }] });
    expect(out.lines.filter((l) => l.message === "search index failed")).toEqual([expect.objectContaining({ error_code: "stream_keepalive_failed" })]);
    expect(c.r.fake.calls.filter((x) => x.ch === "insert" && x.table === EVENTS_TABLE)).toEqual([]);
    expect(c.deletes()).toEqual([]);
    expect(await ready(c)).not.toBe(true);
    expect(c.r.fake.state.filter((r) => r.session_id === SCOPE.sessionId).every((r) => r.needs_full === 1)).toBe(true);
  }, 120_000);

  test("lost before a delete statement: that statement never runs and the pass is not finalized; the redelivery completes", async () => {
    const c = await clocked();
    await indexedWithStale(c, 3);
    // Continuity checks: 1 and 2 around the scan, 3 before the first delete statement.
    const transact = c.rt.deps.search!.transact;
    c.rt.deps.search = { ...c.rt.deps.search!, transact: losingLock(c, 3) };
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [{ itemIdentifier: "s3-0" }] });
    expect(c.deletes()).toEqual([]);
    expect(c.latestState()!.needs_full).toBe(1);
    c.rt.deps.search = { ...c.rt.deps.search!, transact };
    c.clock.ms += 60_000;
    expect(reconciled((await c.send(c.s3Messages.slice(0, 1))).lines)).toEqual([expect.objectContaining({ ok: true, deleted: 3 })]);
  }, 120_000);
});

describe("needs_full recorded before the streamed pass reads", () => {
  test("the pre-mark precedes every read; a crash that loses even the best-effort state write leaves needs_full=1; the next pass reads in full", async () => {
    const c = await clocked();
    // A partial state first (the whole read refused, no stream): the scope owes a full pass.
    const order: string[] = [];
    const reader = c.r.store.getObject.bind(c.r.store);
    (c.r.store as { getObject: unknown }).getObject = ((...a: unknown[]) => { order.push("read"); return (reader as (...x: unknown[]) => unknown)(...a); }) as never;
    // The search stage's manifest reads (admission, before any chunk): the marker must precede them too.
    const manifests = c.rt.deps.search!.reader;
    c.rt.deps.search = { ...c.rt.deps.search!, reader: { provider: manifests.provider, bucket: manifests.bucket, read: async (key: string) => { order.push("manifest"); return manifests.read(key); } } };
    const insert = c.r.fake.ch.insert.bind(c.r.fake.ch);
    let dead = false;
    c.r.fake.ch.insert = (async (table: string, rows: Array<Record<string, unknown>>, o?: never) => {
      if (dead) throw new Error("synthetic crash");
      if (table === SESSION_STATE_TABLE) order.push(`state:${rows[0]!.needs_full}`);
      await insert(table, rows, o);
      // The invocation "dies" at its first index write: nothing else reaches ClickHouse.
      if (table === EVENTS_TABLE) dead = true;
    }) as never;
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(out.result).toEqual({ batchItemFailures: [{ itemIdentifier: "s3-0" }] });
    expect(order[0]).toBe("state:1");
    expect(order.indexOf("manifest")).toBeGreaterThan(0);
    expect(order.indexOf("state:1")).toBeLessThan(order.indexOf("manifest"));
    expect(order.indexOf("state:1")).toBeLessThan(order.indexOf("read"));
    expect(c.latestState()!.needs_full).toBe(1);
    // Recovery: the next pass reads the source in full and completes.
    c.r.fake.ch.insert = insert as never;
    dead = false;
    order.length = 0;
    c.clock.ms += 60_000;
    expect(reconciled((await c.send(c.s3Messages.slice(0, 1))).lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(order.filter((o) => o === "read").length).toBeGreaterThan(0);
    expect(c.latestState()!.needs_full).toBe(0);
  }, 120_000);
});

describe("strict keyset pages before any projection write", () => {
  for (const [name, mangle] of [
    ["three identical rows for LIMIT 2", (rows: unknown[]) => [rows[0], rows[0], rows[0]]],
    ["out of order", (rows: unknown[]) => [...rows].reverse()],
    ["a page that does not advance past the previous one", (rows: unknown[], n: number) => (n > 1 ? rows.slice(0, 2) : rows)],
  ] as const) {
    test(`${name}: the pass stops with ${SCAN_PAGE_INVALID}, no insert, no delete, needs_full=1`, async () => {
      const c = await clocked({ search: { limits: { maxScopeBytes: 1, maxScopeEvents: 1, insertBatch: 2 } } });
      await indexedWithStale(c, 0);
      const select = c.r.fake.ch.select.bind(c.r.fake.ch);
      let pages = 0;
      let first: unknown[] = [];
      c.r.fake.ch.select = (async (q: string, o?: { params?: Record<string, unknown> }) => {
        const rows = await select(q, o as never) as unknown[];
        if (!q.includes("ORDER BY ts, session_id")) return rows;
        pages += 1;
        if (pages === 1) first = rows;
        return name.startsWith("a page") ? (pages > 1 ? first : rows) : (mangle as (r: unknown[], n: number) => unknown[])(rows.length >= 2 ? rows : [...rows, ...rows], pages);
      }) as never;
      c.r.fake.calls.length = 0;
      const out = await c.send(c.s3Messages.slice(0, 1));
      expect(out.result).toEqual({ batchItemFailures: [] });
      expect(reconciled(out.lines)).toEqual([expect.objectContaining({ ok: false, reason: SCAN_PAGE_INVALID })]);
      expect(c.r.fake.calls.filter((x) => x.ch === "insert" && x.table === EVENTS_TABLE)).toEqual([]);
      expect(c.deletes()).toEqual([]);
      // Intended: an untrusted scan writes the owed marker and invalidates the directory, so the next pass is full.
      expect(c.latestState()!.needs_full).toBe(1);
      expect(await readyOf(c)).toBe(false);
    }, 120_000);
  }
});

describe("the owed marker until the Postgres outcome is durable", () => {
  /** Search transactions that run the pass and then fail: `mode` before COMMIT (rollback) or after it (ambiguous). */
  function failing(c: Awaited<ReturnType<typeof clocked>>, mode: "rollback" | "ambiguous"): SearchIndexDeps["transact"] {
    return async (work) => {
      if (mode === "rollback") return c.r.transact(async (tx) => { await work(tx); throw Object.assign(new Error("synthetic failure before COMMIT"), { code: "40001" }); });
      await c.r.transact(work);
      throw Object.assign(new Error("synthetic COMMIT outcome unknown"), { code: "08006" });
    };
  }
  for (const mode of ["rollback", "ambiguous"] as const) {
    test(`${mode}: the pass's work ran, the transaction failed; the message fails and needs_full stays 1; the next pass completes`, async () => {
      const c = await clocked();
      const transact = c.rt.deps.search!.transact;
      c.rt.deps.search = { ...c.rt.deps.search!, transact: failing(c, mode) };
      const out = await c.send(c.s3Messages.slice(0, 1));
      expect(out.result).toEqual({ batchItemFailures: [{ itemIdentifier: "s3-0" }] });
      expect(c.r.fake.state.filter((r) => r.session_id === SCOPE.sessionId).every((r) => r.needs_full === 1)).toBe(true);
      c.rt.deps.search = { ...c.rt.deps.search!, transact };
      c.clock.ms += 60_000;
      expect(reconciled((await c.send(c.s3Messages.slice(0, 1))).lines)).toEqual([expect.objectContaining({ ok: true })]);
      expect(c.latestState()!.needs_full).toBe(0);
    }, 120_000);
  }

  test("overlapping redelivery: a newer pass marked the scope after this one committed; this one does not clear the newer marker", async () => {
    const c = await clocked();
    const select = c.r.fake.ch.select.bind(c.r.fake.ch);
    let raced = false;
    c.r.fake.ch.select = (async (q: string, o?: { params?: Record<string, unknown> }) => {
      // The completion fence's read: a redelivered pass of the same scope started (and marked it) just now.
      if (!raced && q.includes(`FROM ${SESSION_STATE_TABLE}`) && q.includes("synced_ms") && !q.includes("needs_full")) {
        raced = true;
        await c.r.fake.ch.insert(SESSION_STATE_TABLE, [{ org_id: SCOPE.orgId, session_id: SCOPE.sessionId, revision: 2, synced_at: new Date(c.clock.ms + 5_000).toISOString().replace("T", " ").replace("Z", ""), full_synced_at: "1970-01-01 00:00:00.000", scanned_rows: 0, needs_full: 1 }]);
      }
      return select(q, o as never);
    }) as never;
    const out = await c.send(c.s3Messages.slice(0, 1));
    expect(raced).toBe(true);
    expect(reconciled(out.lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(out.lines.filter((l) => l.message === "search index completion not recorded")).toEqual([expect.objectContaining({ reason: "completion_superseded" })]);
    expect(c.latestState()!.needs_full).toBe(1);
    // The newer pass itself then completes.
    c.r.fake.ch.select = select as never;
    c.clock.ms += 60_000;
    expect(reconciled((await c.send(c.s3Messages.slice(0, 1))).lines)).toEqual([expect.objectContaining({ ok: true })]);
    expect(c.latestState()!.needs_full).toBe(0);
  }, 120_000);
});

describe("init and telemetry", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q", SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1",
  };
  test("a deadline margin that leaves no 120 s start minimum in the 300 s function timeout is refused at init, naming the variable", () => {
    expect(lambdaEnv({ ...base, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: "180000" }).searchStreamCh?.deadlineMarginMs).toBe(180_000);
    for (const v of ["180001", "240000"])
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS: v })).toThrow("SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS must be at most 180000");
    expect(lambdaEnv(base).searchStreamCh?.deadlineMarginMs).toBe(60_000);
  });

  test("the function timeout constant equals the stack's", async () => {
    const { LAMBDA_FUNCTION_TIMEOUT_MS } = await import("../lambda.ts");
    const { FUNCTION_TIMEOUT_SECONDS } = await import("../../../stacks/aws/session-pipeline.ts");
    expect(LAMBDA_FUNCTION_TIMEOUT_MS).toBe(FUNCTION_TIMEOUT_SECONDS * 1000);
  });

  test("contender_seats on the reconciled line of an acceptance refusal (all_unseated), through the real handler", async () => {
    const x = await exportStreamScope(crossManifestAcceptanceLines(), 1200);
    cleanup.push(x.dispose);
    const r = await hostedRig(x, cleanup);
    const out = await captureLogs(() => realHandler(r.consumer({}))(r.messages));
    expect(reconciled(out.lines)).toEqual([expect.objectContaining({ ok: false, reason: "relayhistory_acceptance_missing", contender_seats: "all_unseated" })]);
  }, 120_000);

  test("contender_seats on a streamed acceptance refusal: explicitly unknown (main's streamed reader does not classify)", async () => {
    const x = await exportStreamScope(crossManifestAcceptanceLines(), 1200);
    cleanup.push(x.dispose);
    const r = await hostedRig(x, cleanup);
    const out = await captureLogs(() => realHandler(r.consumer({ limits: { maxScopeBytes: 1, maxScopeEvents: 1 }, stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 } }), { searchStreamCh: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24, deadlineMarginMs: MARGIN } })(r.messages, ctx(REMAINING)));
    const line = reconciled(out.lines)[0]!;
    expect(String(line.reason)).toContain("relayhistory_acceptance_missing");
    expect(line.contender_seats).toBe("unknown");
  }, 120_000);

  test("the log value rules: closed words and non-negative integers only", () => {
    expect(sanitizeLogField("contender_seats", "all_unseated")).toBe("all_unseated");
    expect(sanitizeLogField("contender_seats", "org_123")).toBeUndefined();
    expect(sanitizeLogField("contender_seats", "unknown")).toBe("unknown");
    expect(sanitizeLogField("queue_origin", "requeue")).toBe("requeue");
    expect(sanitizeLogField("queue_origin", "s3://x")).toBeUndefined();
    expect(sanitizeLogField("s3_event_age_ms", 42)).toBe(42);
    expect(sanitizeLogField("s3_event_age_ms", -1)).toBeUndefined();
    expect(sanitizeLogField("event_age_ms", "42")).toBeUndefined();
  });
});
