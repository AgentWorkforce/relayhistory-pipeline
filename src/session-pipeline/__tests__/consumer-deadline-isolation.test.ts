// SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED (search-deadline-isolation.ts) in the event consumer: real pointer stage
// and real sessions.enqueue_session_large_read (PGlite), an in-memory search_ingest_state, and the search pass
// replaced by one that always ends like the prod loop (partial, scope_too_large + stream_deadline). Synthetic data only.
import { afterAll, afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { objectStoreManifestReader, prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { STREAM_DEADLINE } from "../../session-read-api/canonical-stream-projection.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { INGEST_STATE_TABLE } from "../vendor/search-index/schema.ts";
import type { SearchIndexResult } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout } from "../pointer.ts";
import { runLargeReadOnce } from "../large-read-worker.ts";
import {
  DEADLINE_MARK_PREFIX, DEADLINE_OVER_BOUND, LARGE_READ_HOLD_MAX_MS, LARGE_READ_UNCLAIMED_MS, decideHold, deadlineMarkName, enqueueLargeRead, largeReadProgress,
} from "../search-deadline-isolation.ts";
import { clonedReplayDatabase, closeReplayTemplate, createFsStore, exportReplayFixture } from "./replay-fixture.ts";
import { recordingReplayClickHouse } from "./replay-clickhouse-fixture.ts";

// Mocked boundary: the consumer's S3-event search pass (ConsumerDeps.searchPass, tests only) stands in for the prod
// loop's ~220 s stream that ends partial for the deadline. Counted, never sleeping. Everything else is real: the pointer
// stage, the enqueue and job SQL (PGlite), and, where named, the large-read worker with the real search pass.
let passes = 0;
let nextResult: () => SearchIndexResult = () => ({
  manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["scope_too_large", STREAM_DEADLINE],
});
const searchPass: NonNullable<ConsumerDeps["searchPass"]> = async () => { passes++; return nextResult(); };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
  passes = 0;
  nextResult = () => ({ manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["scope_too_large", STREAM_DEADLINE] });
});
afterAll(async () => { await closeReplayTemplate(); });
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };

/** search_ingest_state in memory (ReplacingMergeTree by updated_at: the newest row per name wins). */
function stateTable(opts: { failSelect?: boolean; failInsert?: boolean } = {}) {
  const rows: { name: string; value: string; updated_at: string }[] = [];
  const reads: string[] = [];
  const ch = {
    database: "test",
    async command() { throw new Error("unexpected command"); },
    async select<T>(query: string, o?: { params?: Record<string, unknown> }): Promise<T[]> {
      if (!query.includes(`FROM ${INGEST_STATE_TABLE}`)) throw new Error("unexpected select");
      if (opts.failSelect) throw Object.assign(new Error("ch down"), { code: "ECONNRESET" });
      const name = String(o!.params!.name);
      reads.push(name);
      const hit = rows.filter((r) => r.name === name).sort((a, b) => (a.updated_at < b.updated_at ? 1 : -1))[0];
      return hit ? ([{ value: hit.value, at_ms: Date.parse(`${hit.updated_at.replace(" ", "T")}Z`) }] as T[]) : [];
    },
    async insert(table: string, inserted: readonly object[]) {
      if (table !== INGEST_STATE_TABLE) throw new Error("unexpected insert");
      if (opts.failInsert) throw new Error("ch down");
      rows.push(...(inserted as typeof rows));
    },
  } as unknown as ClickHouseClient;
  return { ch, rows, reads };
}

async function fixture() {
  const x = await clonedReplayDatabase();
  cleanup.push(() => x.pg.close());
  const root = await mkdtemp(join(tmpdir(), "consumer-deadline-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const store = createFsStore({ root });
  const source = await exportReplayFixture(store, x.db, 3);
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "replay-fixture" });
  const requeued: string[] = [];
  const observed = recordingReplayClickHouse();
  const deps: ConsumerDeps = {
    pointer: { db: { ...x.db, pool: undefined, end: async () => {} } as any, lock: async (_s: any, work: any) => work(), reader,
      exists: async (k: any) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver,
      allowPrefix: (p: any, o: any) => prefixAllowed(bindings, p, o), fanout: droppedFanout },
    search: { transact: <T>(work: any) => x.pg.transaction((tx: any) => work({ query: (sql: string, params?: unknown[]) => tx.query(sql, params) })) as Promise<T>,
      reader, store, ch: observed.ch, prefixFor: (org: string) => `96bulls/${org}`, stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 } },
    requeue: async (key) => { requeued.push(key); },
    searchPass,
  };
  const key = source.manifests[0]!.manifestKey;
  let n = 0;
  /** One delivery of the scope's event (an S3 event, a busy or batch-deadline requeue, or the next upload). */
  const deliver = async () => {
    const lines: any[] = [], out = process.stdout.write, err = process.stderr.write;
    const tap = (w: any, stream: NodeJS.WriteStream) => (chunk: any, ...rest: any[]) => {
      for (const l of String(chunk).split("\n")) if (l.startsWith("{")) try { const j = JSON.parse(l); if (j.stage === "search-index") lines.push(j); } catch {}
      return w.call(stream, chunk, ...rest);
    };
    process.stdout.write = tap(out, process.stdout) as any; process.stderr.write = tap(err, process.stderr) as any;
    try {
      const r = await processBatch([{ messageId: `m${++n}`, body: JSON.stringify({ reconcile: { key } }) }], deps);
      return { failures: r.batchItemFailures.map((f) => f.itemIdentifier), lines };
    } finally { process.stdout.write = out; process.stderr.write = err; }
  };
  const jobs = async () => (await x.db.query<{ state: string; session_id: string }>("SELECT state,session_id FROM sessions.session_large_read_jobs")).rows;
  // The real large-read worker (large-read-worker.ts runLargeReadOnce): the real search pass (not searchPass) and replay.
  const worker = () => runLargeReadOnce({ db: x.db, search: { ...deps.search!, stream: undefined }, replay: { db: x.db, store, provider: "s3", bucket: "replay-fixture", prefix: bindings } as any });
  return { x, deps, deliver, jobs, requeued, observed, worker };
}
const routed = (lines: any[]) => lines.filter((l) => l.message === "search index over-bound, routed to large-read");

test("flag off (today): every delivery re-runs the full deadline pass, nothing reaches the large-read job", async () => {
  const f = await fixture();
  for (let i = 0; i < 19; i++) expect((await f.deliver()).failures).toEqual([]);
  // The prod loop: 19 re-reads of one scope, each the whole ~220 s stream to the deadline.
  expect(passes).toBe(19);
  expect(await f.jobs()).toEqual([]);
}, 60_000);

test("flag on: the first deadline hit marks and enqueues; the next 18 deliveries never re-run the pass", async () => {
  const f = await fixture();
  const s = stateTable();
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope) };
  const first = await f.deliver();
  expect(first.failures).toEqual([]);
  expect(passes).toBe(1);
  expect(s.rows).toHaveLength(1);
  expect(s.rows[0]!.name.startsWith(DEADLINE_MARK_PREFIX)).toBe(true);
  expect(JSON.parse(s.rows[0]!.value)).toEqual({ v: 1, reason: STREAM_DEADLINE });
  expect(routed(first.lines)).toEqual([expect.objectContaining({ level: "info", reason: DEADLINE_OVER_BOUND, outcome: "first_hit", route: "large-read" })]);
  // The pass's own partial line still logs, so prod counts of stream_deadline passes stay comparable.
  expect(first.lines.some((l) => l.message === "search index partial" && l.reason === `scope_too_large,${STREAM_DEADLINE}`)).toBe(true);
  expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
  for (let i = 0; i < 18; i++) {
    const r = await f.deliver();
    expect(r.failures).toEqual([]);
    expect(routed(r.lines)).toEqual([expect.objectContaining({ outcome: "marked", route: "large-read" })]);
  }
  expect(passes).toBe(1);
  expect(f.requeued).toEqual([]);
  // Enqueue is idempotent on an unchanged source: still one queued job.
  expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
  // Metadata only: no content-like fields on the route lines.
  for (const l of routed(first.lines)) expect(Object.keys(l).every((k) => !/text|content|payload|body/.test(k))).toBe(true);
}, 60_000);

test("flag on: a marked scope is routed even past the batch deadline, instead of a stream_batch_deadline requeue", async () => {
  const f = await fixture();
  const s = stateTable();
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope) };
  await f.deliver();
  f.deps.search!.streamDeadline = { at: 0 };
  const r = await f.deliver();
  expect(r.failures).toEqual([]);
  expect(f.requeued).toEqual([]);
  expect(routed(r.lines)).toHaveLength(1);
  // An unmarked scope past the batch deadline is still requeued exactly as before.
  s.rows.length = 0;
  const unmarked = await f.deliver();
  expect(f.requeued).toHaveLength(1);
  expect(unmarked.lines.some((l) => l.reason === "stream_batch_deadline")).toBe(true);
}, 60_000);

test("flag on: a source the enqueue reports unavailable (erased, blocked) runs the normal pass every time", async () => {
  const f = await fixture();
  const s = stateTable();
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async () => ({ enqueued: false }) };
  for (let i = 0; i < 3; i++) expect((await f.deliver()).failures).toEqual([]);
  expect(passes).toBe(3);
  expect(await f.jobs()).toEqual([]);
}, 60_000);

test("flag on: a failed mark read, a malformed mark or an expired one runs the normal pass", async () => {
  const f = await fixture();
  const down = stateTable({ failSelect: true });
  f.deps.deadlineIsolation = { ch: down.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope) };
  await f.deliver(); await f.deliver();
  expect(passes).toBe(2);
  const s = stateTable();
  const scope = { orgId: "org_replay", workspaceId: "ws_replay", harness: "claude", sessionId: "large_replay" };
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async () => ({ enqueued: true }), now: () => Date.parse("2026-11-28T12:00:00Z") };
  nextResult = () => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
  s.rows.push({ name: deadlineMarkName(scope), value: "not json", updated_at: "2026-11-28 11:59:00.000" });
  await f.deliver();
  s.rows.length = 0;
  s.rows.push({ name: deadlineMarkName(scope), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: "2026-11-28 05:59:59.000" });
  await f.deliver();
  expect(passes).toBe(4);
  // Within the age bound it routes.
  s.rows.length = 0;
  s.rows.push({ name: deadlineMarkName(scope), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: "2026-11-28 06:00:01.000" });
  const r = await f.deliver();
  expect(passes).toBe(4);
  expect(routed(r.lines)).toHaveLength(1);
}, 60_000);

test("flag on: a complete pass or another partial reason writes no mark", async () => {
  const f = await fixture();
  const s = stateTable();
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async () => { throw new Error("must not enqueue"); } };
  nextResult = () => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
  expect((await f.deliver()).failures).toEqual([]);
  nextResult = () => ({ manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["scope_too_large", "stream_read_bytes_budget"] });
  expect((await f.deliver()).failures).toEqual([]);
  expect(s.rows).toEqual([]);
}, 60_000);

test("flag on: a mark write failure still enqueues; an enqueue error on a marked scope fails its message for retry", async () => {
  const f = await fixture();
  const broken = stateTable({ failInsert: true });
  f.deps.deadlineIsolation = { ch: broken.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope) };
  const first = await f.deliver();
  expect(first.failures).toEqual([]);
  expect(first.lines.some((l) => l.reason === "deadline_mark_write_failed")).toBe(true);
  expect(await f.jobs()).toHaveLength(1);
  const s = stateTable();
  s.rows.push({ name: deadlineMarkName({ orgId: "org_replay", workspaceId: "ws_replay", harness: "claude", sessionId: "large_replay" }), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: new Date().toISOString().replace("T", " ").replace("Z", "") });
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async () => { throw Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }); } };
  const r = await f.deliver();
  expect(r.failures).toEqual(["m2"]);
  expect(passes).toBe(1);
}, 60_000);

test("enqueueLargeRead (real SQL): queues the canonical job with no size threshold; an erased scope is not enqueued", async () => {
  const f = await fixture();
  const scope = { orgId: "org_replay", workspaceId: "ws_replay", harness: "claude", sessionId: "large_replay" };
  // The route refuses this small scope (under its size threshold); the forced enqueue takes it.
  expect((await f.x.db.query<{ r: { delegated: boolean } }>("SELECT sessions.route_session_large_read($1,$2,$3,$4) r", [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId])).rows[0]!.r.delegated).toBe(false);
  await f.deliver();
  expect(await f.jobs()).toEqual([]);
  expect(await enqueueLargeRead(f.x.db, scope)).toEqual({ enqueued: true, identity: { ...scope, sessionId: "large:replay" }, source: { epoch: expect.any(String), fingerprint: expect.stringMatching(/^[0-9a-f]{64}$/) } });
  expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
  await f.x.pg.exec("UPDATE sessions.session_index SET deleted_at=now()");
  expect(await enqueueLargeRead(f.x.db, scope)).toEqual({ enqueued: false });
}, 60_000);

test("tenant-scoped: a mark names one (org, workspace, harness, session); the same session id elsewhere is not routed", async () => {
  const f = await fixture();
  const s = stateTable();
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope) };
  const own = { orgId: "org_replay", workspaceId: "ws_replay", harness: "claude", sessionId: "large_replay" };
  const names = new Set([
    deadlineMarkName(own),
    deadlineMarkName({ ...own, orgId: "org_other" }),
    deadlineMarkName({ ...own, workspaceId: "ws_other" }),
    deadlineMarkName({ ...own, harness: "codex" }),
  ]);
  expect(names.size).toBe(4);
  // Marks for the same session id in another org, workspace and harness: this scope still runs inline.
  const at = new Date().toISOString().replace("T", " ").replace("Z", "");
  for (const other of [{ ...own, orgId: "org_other" }, { ...own, workspaceId: "ws_other" }, { ...own, harness: "codex" }])
    s.rows.push({ name: deadlineMarkName(other), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: at });
  nextResult = () => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
  const r = await f.deliver();
  expect(passes).toBe(1);
  expect(routed(r.lines)).toEqual([]);
  expect(s.reads).toEqual([deadlineMarkName(own)]);
  expect(await f.jobs()).toEqual([]);
}, 60_000);

test("crash after the mark, before the enqueue: the redelivered message routes and enqueues without another pass", async () => {
  const f = await fixture();
  const s = stateTable();
  let crash = true;
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async (scope) => { if (crash) throw new Error("invocation lost"); return enqueueLargeRead(f.x.db, scope); } };
  const first = await f.deliver();
  // The first hit wrote its mark, then the enqueue died: the message fails and SQS redelivers it.
  expect(first.failures).toEqual(["m1"]);
  expect(s.rows).toHaveLength(1);
  expect(await f.jobs()).toEqual([]);
  crash = false;
  const retry = await f.deliver();
  expect(retry.failures).toEqual([]);
  expect(passes).toBe(1);
  expect(routed(retry.lines)).toEqual([expect.objectContaining({ outcome: "marked" })]);
  expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
}, 60_000);

test("a routed scope whose large-read job is failed or missing is logged as a warn (it will not be processed)", async () => {
  const f = await fixture();
  const s = stateTable();
  const lines: any[] = [];
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async () => ({ enqueued: true }), jobState: async () => ({ state: "failed", attempts: 5 }) };
  const out = process.stdout.write, err = process.stderr.write;
  const tap = (w: any, st: NodeJS.WriteStream) => (c: any, ...r: any[]) => { for (const l of String(c).split("\n")) if (l.startsWith("{")) try { const j = JSON.parse(l); if (j.stage === "large-read") lines.push(j); } catch {} return w.call(st, c, ...r); };
  process.stdout.write = tap(out, process.stdout) as any; process.stderr.write = tap(err, process.stderr) as any;
  try { expect((await f.deliver()).failures).toEqual([]); } finally { process.stdout.write = out; process.stderr.write = err; }
  expect(lines).toEqual([expect.objectContaining({ level: "warn", message: "large scope job will not be processed", state: "failed", attempt: 5, reason: "large_job_failed" })]);
}, 60_000);

test("the job state is read under the canonical identity the enqueue returned (real SQL)", async () => {
  const f = await fixture();
  const s = stateTable();
  const asked: string[] = [];
  const { largeReadJobState } = await import("../large-read-worker.ts");
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope), jobState: async (scope) => { asked.push(scope.sessionId); return largeReadJobState(f.x.db, scope); } };
  await f.deliver();
  const job = await f.jobs();
  expect(job).toHaveLength(1);
  expect(asked).toEqual([job[0]!.session_id]);
}, 60_000);

describe("hold until this scope's own job completes (progress armed)", () => {
  const own = { orgId: "org_replay", workspaceId: "ws_replay", harness: "claude", sessionId: "large_replay" };
  // Clock assumptions: the test clock `t` drives the mark age and the hold age; messages carry no upload time, so the
  // anchor is the mark's write time (first hit: the pass start). Queue assumption: each requeue is one new SQS
  // message (recorded in f.requeued); a redelivery is modelled by calling deliver() again.
  const capture = async (f: Awaited<ReturnType<typeof fixture>>) => {
    const lines: any[] = [], out = process.stdout.write, err = process.stderr.write;
    const tap = (w: any, st: NodeJS.WriteStream) => (c: any, ...r: any[]) => { for (const l of String(c).split("\n")) if (l.startsWith("{")) try { const j = JSON.parse(l); if (j.stage === "large-read") lines.push(j); } catch {} return w.call(st, c, ...r); };
    process.stdout.write = tap(out, process.stdout) as any; process.stderr.write = tap(err, process.stderr) as any;
    try { const r = await f.deliver(); return { ...r, large: lines }; } finally { process.stdout.write = out; process.stderr.write = err; }
  };
  const armed = (f: Awaited<ReturnType<typeof fixture>>, s: ReturnType<typeof stateTable>, clock: { t: number }) => {
    f.deps.deadlineIsolation = { ch: s.ch, enqueue: (scope) => enqueueLargeRead(f.x.db, scope), progress: (scope) => largeReadProgress(f.x.db, scope), now: () => clock.t };
  };
  const MIN = 60_000;

  test("single-shot upload, missing worker: first hit holds (new message), redeliveries hold until 5 min, then one inline pass", async () => {
    const f = await fixture();
    const s = stateTable();
    const clock = { t: Date.now() };
    armed(f, s, clock);
    const first = await capture(f);
    expect(first.failures).toEqual([]);
    expect(passes).toBe(1);
    expect(f.requeued).toHaveLength(1); // the durable successor exists before this delivery is acked
    expect(routed(first.lines)).toEqual([expect.objectContaining({ outcome: "first_hit", state: "held_queued" })]);
    for (let m = 1; m <= 4; m++) {
      clock.t += MIN;
      const r = await capture(f);
      expect(r.failures).toEqual([]);
      expect(routed(r.lines)).toEqual([expect.objectContaining({ outcome: "marked", state: "held_queued" })]);
    }
    expect(passes).toBe(1);
    expect(f.requeued).toHaveLength(5);
    // Past 5 min from the mark with the job never claimed: eligible, and this delivery runs the inline pass itself.
    clock.t += 2 * MIN;
    nextResult = () => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
    const fallback = await capture(f);
    expect(fallback.failures).toEqual([]);
    expect(fallback.large).toEqual([expect.objectContaining({ level: "warn", message: "large-read job unclaimed", reason: "large_job_unclaimed", route: "inline" })]);
    expect(passes).toBe(2);
    expect(fallback.lines.some((l) => l.message === "search index reconciled" && l.ok === true)).toBe(true);
    expect(f.requeued).toHaveLength(5); // the fallback pass completed: acked, no further hold
  }, 60_000);

  test("busy worker starving this job: other jobs' progress does not suppress the fallback", async () => {
    const f = await fixture();
    const s = stateTable();
    const clock = { t: Date.now() };
    armed(f, s, clock);
    await f.deliver();
    await f.x.db.query(
      `INSERT INTO sessions.session_large_read_jobs(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint,state,lease_nonce,lease_expires_at,updated_at)
       VALUES('org_other','ws_other','claude','other',1,$1,'running',gen_random_uuid(),now()+interval '2 minutes',now())`, ["a".repeat(64)]);
    clock.t += 6 * MIN;
    const before = f.requeued.length;
    const r = await capture(f);
    // Eligible (the marked check), then the inline pass, which hits the deadline again (the fixture's default):
    // its first-hit route finds the job still unclaimed and acks without another hold. Today's loop, no worse.
    expect(r.large).toEqual([
      expect.objectContaining({ message: "large-read job unclaimed", reason: "large_job_unclaimed" }),
      expect.objectContaining({ message: "large-read job unclaimed", reason: "large_job_unclaimed" }),
    ]);
    expect(passes).toBe(2);
    expect(r.failures).toEqual([]);
    expect(f.requeued).toHaveLength(before);
  }, 60_000);

  test("job running with a live lease: held past 8 cycles, each a new message, never failed; released at 30 min", async () => {
    const f = await fixture();
    const s = stateTable();
    const clock = { t: Date.now() };
    armed(f, s, clock);
    await f.deliver();
    await f.x.db.query("UPDATE sessions.session_large_read_jobs SET state='running',lease_nonce=gen_random_uuid(),lease_expires_at=now()+interval '1 hour',updated_at=now()");
    for (let m = 1; m <= 12; m++) {
      clock.t += 2 * MIN;
      const r = await capture(f);
      expect(r.failures).toEqual([]);
      expect(routed(r.lines)).toEqual([expect.objectContaining({ state: "held_running" })]);
    }
    expect(passes).toBe(1);
    expect(f.requeued).toHaveLength(13);
    clock.t += 7 * MIN;
    const released = await capture(f);
    expect(released.failures).toEqual([]);
    expect(released.large).toEqual([expect.objectContaining({ level: "warn", message: "large-read hold released", reason: "large_hold_max" })]);
    expect(f.requeued).toHaveLength(13);
    expect(passes).toBe(1);
  }, 60_000);

  test("complete for the captured incarnation: acked, no hold; complete for another incarnation keeps holding", async () => {
    const f = await fixture();
    const s = stateTable();
    const clock = { t: Date.now() };
    armed(f, s, clock);
    await f.deliver();
    await f.x.db.query("UPDATE sessions.session_large_read_jobs SET state='complete',source_fingerprint=$1,updated_at=now()", ["b".repeat(64)]);
    // The enqueue re-queues a job whose fingerprint differs from the live source; force the stale-complete row back.
    const stale = { ...f.deps.deadlineIsolation!, enqueue: async (scope: any) => { const r = await enqueueLargeRead(f.x.db, scope); await f.x.db.query("UPDATE sessions.session_large_read_jobs SET state='complete',source_fingerprint=$1", ["b".repeat(64)]); return r; } };
    f.deps.deadlineIsolation = stale;
    clock.t += MIN;
    const held = await capture(f);
    expect(routed(held.lines)).toEqual([expect.objectContaining({ state: "held_queued" })]);
    armed(f, s, clock);
    await enqueueLargeRead(f.x.db, own);
    await f.x.db.query("UPDATE sessions.session_large_read_jobs SET state='complete',updated_at=now()");
    const before = f.requeued.length;
    const done = await capture(f);
    expect(done.failures).toEqual([]);
    expect(routed(done.lines)).toEqual([expect.objectContaining({ state: "complete" })]);
    expect(f.requeued).toHaveLength(before);
    expect(passes).toBe(1);
  }, 60_000);

  test("a failed requeue fails the delivery (no ack without a successor); a failed job goes inline at once", async () => {
    const f = await fixture();
    const s = stateTable();
    const clock = { t: Date.now() };
    armed(f, s, clock);
    f.deps.requeue = async () => { throw new Error("sqs down"); };
    const r = await capture(f);
    expect(r.failures).toEqual(["m1"]);
    f.deps.requeue = async (k) => { f.requeued.push(k); };
    await f.x.db.query("UPDATE sessions.session_large_read_jobs SET state='failed',updated_at=now()");
    nextResult = () => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
    clock.t += MIN;
    const failed = await capture(f);
    expect(failed.large).toEqual([expect.objectContaining({ message: "large-read job unclaimed", reason: "large_job_failed" })]);
    expect(passes).toBe(2);
  }, 60_000);

  test("job completion beyond enqueue: held, the real worker claims and publishes, the next delivery acks without a pass", async () => {
    const f = await fixture();
    const s = stateTable();
    const clock = { t: Date.now() };
    armed(f, s, clock);
    await f.deliver();
    expect(f.requeued).toHaveLength(1);
    expect(f.observed.stats().uniqueRows).toBe(0);
    expect(await f.worker()).toBe("published");
    expect((await f.jobs())[0]!.state).toBe("complete");
    expect(f.observed.stats().uniqueRows).toBeGreaterThan(0); // indexed by the large read, not the inline pass
    clock.t += MIN;
    const r = await capture(f);
    expect(r.failures).toEqual([]);
    expect(routed(r.lines)).toEqual([expect.objectContaining({ outcome: "marked", state: "complete" })]);
    expect(f.requeued).toHaveLength(1);
    expect(passes).toBe(1);
  }, 60_000);

  test("decideHold: only this job's row and the captured incarnation decide", () => {
    const src = { epoch: "7", fingerprint: "c".repeat(64) };
    const row = (state: string, leaseLive = false, source = src) => ({ state, leaseLive, source });
    expect(decideHold(null, src, 0)).toEqual({ kind: "inline", why: "missing" });
    expect(decideHold(row("complete"), src, 0)).toEqual({ kind: "done" });
    expect(decideHold(row("complete", false, { ...src, epoch: "6" }), src, 0)).toEqual({ kind: "hold", why: "queued" });
    expect(decideHold(row("complete"), undefined, 0)).toEqual({ kind: "hold", why: "queued" });
    expect(decideHold(row("failed"), src, 0)).toEqual({ kind: "inline", why: "failed" });
    expect(decideHold(row("running", true), src, LARGE_READ_HOLD_MAX_MS)).toEqual({ kind: "hold", why: "running" });
    expect(decideHold(row("running", true), src, LARGE_READ_HOLD_MAX_MS + 1)).toEqual({ kind: "release" });
    expect(decideHold(row("running", false), src, LARGE_READ_UNCLAIMED_MS + 1)).toEqual({ kind: "inline", why: "unclaimed" });
    expect(decideHold(row("queued"), src, LARGE_READ_UNCLAIMED_MS)).toEqual({ kind: "hold", why: "queued" });
    expect(decideHold(row("superseded"), src, LARGE_READ_UNCLAIMED_MS + 1)).toEqual({ kind: "inline", why: "unclaimed" });
  });
});

test("healthy scope, flag on: every delivery runs its pass exactly as flag off; one mark read each, no mark, no enqueue, no route", async () => {
  const f = await fixture();
  nextResult = () => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
  const off: any[] = [];
  for (let i = 0; i < 3; i++) off.push(await f.deliver());
  const s = stateTable();
  let enqueues = 0;
  f.deps.deadlineIsolation = { ch: s.ch, enqueue: async () => { enqueues++; return { enqueued: true }; } };
  const on: any[] = [];
  for (let i = 0; i < 3; i++) on.push(await f.deliver());
  expect(passes).toBe(6);
  const shape = (r: any) => ({ failures: r.failures, messages: r.lines.map((l: any) => [l.message, l.ok, l.reason]) });
  expect(on.map(shape)).toEqual(off.map(shape));
  expect({ enqueues, marks: s.rows.length, reads: s.reads.length }).toEqual({ enqueues: 0, marks: 0, reads: 3 });
  expect(await f.jobs()).toEqual([]);
}, 60_000);
