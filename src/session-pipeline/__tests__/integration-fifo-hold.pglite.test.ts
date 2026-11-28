/**
 * T124 + T126 integration (offline): SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED holds on BOTH queue paths.
 *
 * Rule under test (option 1(b), Skip and Pipeline): a FIFO head is acked only after durable recovery outside its group
 * is committed for the exact job incarnation. Here that recovery is T124's hold requeue onto the STANDARD queue (a new
 * message carrying the expected incarnation): it is outside the FIFO group and it is itself the wakeup. A failed or
 * ambiguous handoff (the requeue throws) keeps the head, which blocks the group's later messages behind it.
 *
 * Real: the pointer stage, sessions.enqueue_session_large_read and the job rows (PGlite), the consumer, the FIFO
 * dispatch. Mocked: the search pass (searchPass: it ends partial for stream_deadline like the prod loop),
 * search_ingest_state (in memory), SQS (records built as the ESM delivers them; visibility and requeues recorded),
 * the clock. Queue assumptions: one FIFO group per session; a redelivery is modelled by delivering the same record
 * again with ApproximateReceiveCount + 1. Synthetic data only.
 */
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
import { dispatchSqsBatch, type RevisionWiring, type SqsRecord } from "../revision-dispatch.ts";
import { revisionMessage, type RevisionEvent } from "../revision-events.ts";
import { DEADLINE_MARK_PREFIX, captureLargeReadSource, deadlineMarkName, enqueueLargeRead, holdId, largeReadProgress } from "../search-deadline-isolation.ts";
import { parseQueueBody, requeueBody, type LargeReadHoldBody } from "../s3-events.ts";
import { clonedReplayDatabase, closeReplayTemplate, createFsStore, exportReplayFixture } from "./replay-fixture.ts";
import { recordingReplayClickHouse } from "./replay-clickhouse-fixture.ts";

let passes = 0;
let nextResult: () => SearchIndexResult = () => partial();
const partial = (): SearchIndexResult => ({ manifests: 1, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, reasons: ["scope_too_large", STREAM_DEADLINE] });
const complete = (): SearchIndexResult => ({ manifests: 1, rows: 3, inserted: 3, deleted: 0, complete: true, reasons: [] });
const searchPass: NonNullable<ConsumerDeps["searchPass"]> = async () => { passes++; return nextResult(); };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
  passes = 0;
  nextResult = () => partial();
});
afterAll(async () => { await closeReplayTemplate(); });
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const ARN = "arn:aws:sqs:us-east-1:000000000000:synthetic-revisions.fifo";
const MIN = 60_000;

function stateTable() {
  const rows: { name: string; value: string; updated_at: string }[] = [];
  const ch = {
    database: "test",
    async command() { throw new Error("unexpected command"); },
    async select<T>(query: string, o?: { params?: Record<string, unknown> }): Promise<T[]> {
      if (!query.includes(`FROM ${INGEST_STATE_TABLE}`)) throw new Error("unexpected select");
      const name = String(o!.params!.name);
      const hit = rows.filter((r) => r.name === name).sort((a, b) => (a.updated_at < b.updated_at ? 1 : -1))[0];
      return hit ? ([{ value: hit.value, at_ms: Date.parse(`${hit.updated_at.replace(" ", "T")}Z`) }] as T[]) : [];
    },
    async insert(table: string, inserted: readonly object[]) {
      if (table !== INGEST_STATE_TABLE) throw new Error("unexpected insert");
      rows.push(...(inserted as typeof rows));
    },
  } as unknown as ClickHouseClient;
  return { ch, rows };
}

async function fixture() {
  const x = await clonedReplayDatabase();
  cleanup.push(() => x.pg.close());
  const root = await mkdtemp(join(tmpdir(), "integration-fifo-hold-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const store = createFsStore({ root });
  const source = await exportReplayFixture(store, x.db, 3);
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "replay-fixture" });
  /** Standard-queue messages this consumer sent (requeues and holds): each is a NEW message outside the FIFO group. */
  const standard: { key: string; oldest?: string; hold?: LargeReadHoldBody }[] = [];
  const handoff = { fail: false };
  const clock = { t: Date.now() };
  const s = stateTable();
  const deps: ConsumerDeps = {
    pointer: { db: { ...x.db, pool: undefined, end: async () => {} } as any, lock: async (_s: any, work: any) => work(), reader,
      exists: async (k: any) => (await store.head(objectKey(k))) !== undefined, orgs: prefixOrgResolver,
      allowPrefix: (p: any, o: any) => prefixAllowed(bindings, p, o), fanout: droppedFanout },
    search: { transact: <T>(work: any) => x.pg.transaction((tx: any) => work({ query: (sql: string, params?: unknown[]) => tx.query(sql, params) })) as Promise<T>,
      reader, store, ch: recordingReplayClickHouse().ch, prefixFor: (org: string) => `96bulls/${org}`, stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 } },
    requeue: async (key, oldest, hold) => {
      if (handoff.fail) throw Object.assign(new Error("synthetic sqs send failure"), { code: "ECONNRESET" });
      standard.push({ key, ...(oldest ? { oldest } : {}), ...(hold ? { hold } : {}) });
    },
    searchPass,
    deadlineIsolation: { ch: s.ch, enqueue: (scope) => enqueueLargeRead(x.db, scope), progress: (scope) => largeReadProgress(x.db, scope), now: () => clock.t },
  };
  const key = source.manifests[0]!.manifestKey;
  const etag = async () => (await store.head(objectKey(key as never)))!.version.eTag!.replace(/^"|"$/g, "");
  const visibility: { handles: string[]; seconds: number }[] = [];
  const wiring: RevisionWiring = {
    config: { queueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/synthetic-revisions.fifo", mode: "direct", busyRetrySeconds: 5 },
    sender: async () => ({ failed: [] }),
    changeVisibility: async (handles, seconds) => { visibility.push({ handles: [...handles], seconds }); },
    now: () => clock.t,
  };
  let n = 0;
  /** One FIFO record for a revision event of the scope's manifest, as the ESM delivers it. */
  const fifoRecord = async (revision: number, receives = 1, id?: string): Promise<SqsRecord> => {
    const event: RevisionEvent = { key, etag: await etag(), revision, committedAt: new Date(clock.t).toISOString(), source: "writer" };
    const m = revisionMessage(event)!;
    const messageId = id ?? `f${++n}`;
    return { messageId, receiptHandle: `rh-${messageId}-${receives}`, body: m.MessageBody, eventSourceARN: ARN,
      attributes: { MessageGroupId: m.MessageGroupId, MessageDeduplicationId: m.MessageDeduplicationId, SentTimestamp: String(clock.t), ApproximateReceiveCount: String(receives) } };
  };
  const failures = (r: { batchItemFailures: { itemIdentifier: string }[] }) => r.batchItemFailures.map((f) => f.itemIdentifier);
  const fifo = async (records: SqsRecord[]) => failures(await dispatchSqsBatch(records, deps, wiring));
  /** A standard-queue delivery: an S3-style reconcile body, or a requeued body this consumer sent. */
  const plain = async (body: string = JSON.stringify({ reconcile: { key } })) => failures(await processBatch([{ messageId: `s${++n}`, body }], deps));
  const jobs = async () => (await x.db.query<{ state: string }>("SELECT state FROM sessions.session_large_read_jobs")).rows;
  return { x, deps, key, fifo, fifoRecord, plain, standard, handoff, clock, visibility, jobs, s };
}
type F = Awaited<ReturnType<typeof fixture>>;
/** Re-deliver a standard-queue message this consumer sent (the hold wakeup), as SQS would. */
const wake = (f: F, m: F["standard"][number]) =>
  f.plain(JSON.stringify({ reconcile: { key: m.key, requeued: true, ...(m.oldest ? { oldest_event_time: m.oldest } : {}),
    ...(m.hold ? { large_read_hold: m.hold } : {}) } }));

describe("(a) failed or ambiguous handoff: the FIFO head is retained", () => {
  test("FIFO: the requeue throws, so the head and the later same-group message fail (normal visibility), stay failed on each redelivery as the receive count grows, and are released only once the successor exists", async () => {
    const f = await fixture();
    f.handoff.fail = true;
    const head = await f.fifoRecord(1, 1, "head");
    const next = await f.fifoRecord(2, 1, "next");
    expect(await f.fifo([head, next])).toEqual(["head", "next"]);
    expect(passes).toBe(1);
    // The job row exists (first hit enqueued it), but no successor message: no ACK.
    expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
    expect(f.standard).toEqual([]);
    // Both messages are the same key, so they coalesce into ONE scope and fail together through the search stage's
    // catch: neither is a busy retry, so both keep the queue's normal visibility and DLQ path (no short visibility).
    // OPEN for review: a failed hold requeue is transient; it could use the ordered short backoff instead.
    expect(f.visibility).toEqual([]);
    // Redelivered in order (receive 2 and 3): still retained while the handoff keeps failing, never acked without a successor.
    for (const receives of [2, 3]) {
      f.clock.t += MIN;
      expect(await f.fifo([await f.fifoRecord(1, receives, "head"), await f.fifoRecord(2, receives, "next")])).toEqual(["head", "next"]);
    }
    expect(f.standard).toEqual([]);
    // The handoff recovers: the head is released only now, after its successor exists.
    f.handoff.fail = false;
    f.clock.t += MIN;
    expect(await f.fifo([await f.fifoRecord(1, 4, "head"), await f.fifoRecord(2, 4, "next")])).toEqual([]);
    expect(f.standard).toEqual([expect.objectContaining({ key: f.key, hold: expect.objectContaining({ seq: expect.any(Number) }) })]);
  }, 60_000);

  test("standard: the requeue throws, so the delivery fails (no ack without a successor)", async () => {
    const f = await fixture();
    f.handoff.fail = true;
    expect(await f.plain()).toHaveLength(1);
    expect(f.standard).toEqual([]);
    f.handoff.fail = false;
    expect(await f.plain()).toEqual([]);
    expect(f.standard).toHaveLength(1);
  }, 60_000);
});

describe("(b) confirmed handoff: the head is released and a later append is picked up without waiting for the old job", () => {
  test("FIFO: the head acks once its hold is on the standard queue; the next append's batch runs at once (the old job is still queued)", async () => {
    const f = await fixture();
    expect(await f.fifo([await f.fifoRecord(1)])).toEqual([]);
    expect(f.standard).toHaveLength(1);
    expect(parseQueueBody(JSON.stringify({ reconcile: { key: f.key, requeued: true, large_read_hold: f.standard[0]!.hold } }))).toMatchObject({ kind: "events" });
    expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
    // A later append of the same session: its own FIFO batch is not blocked (the head was deleted), and it is
    // handled at once: routed to the same job (marked), held by its own successor, acked. No second pass.
    f.clock.t += 1_000;
    expect(await f.fifo([await f.fifoRecord(2)])).toEqual([]);
    expect(passes).toBe(1);
    expect(f.standard).toHaveLength(2);
    expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
  }, 60_000);

  test("FIFO, stalled worker and NO later append: the hold message is the wakeup; it keeps holding until 5 min, then one inline pass", async () => {
    const f = await fixture();
    expect(await f.fifo([await f.fifoRecord(1)])).toEqual([]);
    // No further FIFO traffic. Only the standard-queue hold comes back.
    for (let m = 1; m <= 4; m++) {
      f.clock.t += MIN;
      const held = f.standard.at(-1)!;
      expect(await wake(f, held)).toEqual([]);
    }
    expect(passes).toBe(1);
    expect(f.standard).toHaveLength(5);
    f.clock.t += 2 * MIN;
    nextResult = () => complete();
    expect(await wake(f, f.standard.at(-1)!)).toEqual([]);
    expect(passes).toBe(2);
    // The inline fallback completed: acked with no further hold.
    expect(f.standard).toHaveLength(5);
  }, 60_000);

  test("standard: the same sequence (first hit holds, a later delivery is handled at once)", async () => {
    const f = await fixture();
    expect(await f.plain()).toEqual([]);
    expect(f.standard).toHaveLength(1);
    f.clock.t += 1_000;
    expect(await f.plain()).toEqual([]);
    expect(passes).toBe(1);
    expect(f.standard).toHaveLength(2);
  }, 60_000);
});

describe("(c) delayed and duplicate A against B: B's queue work stays intact", () => {
  const own = { orgId: "org_replay", workspaceId: "ws_replay", harness: "claude", sessionId: "large_replay" };
  const jobRow = async (f: F) => (await f.x.db.query<{ j: unknown }>("SELECT row_to_json(j)::text j FROM sessions.session_large_read_jobs j")).rows.map((r) => r.j);
  const markNow = (f: F) =>
    f.s.rows.push({ name: deadlineMarkName(own), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: new Date(f.clock.t).toISOString().replace("T", " ").replace("Z", "") });

  test("FIFO: a delayed A event, then its duplicate, after B's own routing: each acks, routes to B's job (row byte-identical), and holds for B's incarnation, never A's; no pass", async () => {
    const f = await fixture();
    f.deps.deadlineIsolation = { ...f.deps.deadlineIsolation!, current: (scope) => captureLargeReadSource(f.x.db, scope) };
    // B is the live manifest; its own routing already enqueued its job and marked the scope.
    await enqueueLargeRead(f.x.db, own);
    markNow(f);
    const b = (await captureLargeReadSource(f.x.db, own))!.source;
    const before = await jobRow(f);
    expect(before).toHaveLength(1);
    // A: an earlier write of the same key (a stale ETag and a lower revision), delivered late, then again.
    const a = await f.fifoRecord(1, 1, "a1");
    const stale = { ...a, body: a.body.replace(/"etag":"[^"]+"/, '"etag":"stale-a-etag"') };
    for (const [i, rec] of [stale, { ...stale, messageId: "a1-dup", receiptHandle: "rh-a1-dup-1" }].entries()) {
      f.clock.t += 1_000;
      expect(await f.fifo([rec])).toEqual([]);
      expect(await jobRow(f)).toEqual(before);
      expect(f.standard).toHaveLength(i + 1);
      const hold = f.standard.at(-1)!.hold!;
      expect({ epoch: hold.epoch, fingerprint: hold.fingerprint }).toEqual({ epoch: b.epoch, fingerprint: b.fingerprint });
    }
    expect(passes).toBe(0);
    expect(f.s.rows.filter((r) => r.name.startsWith(DEADLINE_MARK_PREFIX))).toHaveLength(1);
    // After A's deliveries are done, B's own wakeup still holds for B (its job is still queued) and changes no row.
    f.clock.t += MIN;
    expect(await wake(f, f.standard.at(-1)!)).toEqual([]);
    expect(await jobRow(f)).toEqual(before);
    expect(passes).toBe(0);
  }, 60_000);

  test("standard: the same delayed A and duplicate (S3-style bodies) leave B's job row byte-identical and hold for B", async () => {
    const f = await fixture();
    f.deps.deadlineIsolation = { ...f.deps.deadlineIsolation!, current: (scope) => captureLargeReadSource(f.x.db, scope) };
    await enqueueLargeRead(f.x.db, own);
    markNow(f);
    const b = (await captureLargeReadSource(f.x.db, own))!.source;
    const before = await jobRow(f);
    const body = JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: new Date(f.clock.t).toISOString(),
      s3: { bucket: { name: "replay-fixture" }, object: { key: f.key, eTag: "stale-a-etag", sequencer: "0A" } } }] });
    for (let i = 0; i < 2; i++) {
      f.clock.t += 1_000;
      expect(await f.plain(body)).toEqual([]);
      expect(await jobRow(f)).toEqual(before);
      const hold = f.standard.at(-1)!.hold!;
      expect({ epoch: hold.epoch, fingerprint: hold.fingerprint }).toEqual({ epoch: b.epoch, fingerprint: b.fingerprint });
    }
    expect(passes).toBe(0);
  }, 60_000);

  // A hold-only delivery bound to A (a standard-queue message carrying A's expected incarnation), as opposed to the
  // ordinary manifest hints above. On the FIFO deployment holds always travel the standard queue (the out-of-group
  // handoff), so this is the path a delayed or duplicate A hold takes on both deployments.
  const A = { epoch: "1", fingerprint: "f".repeat(64) };
  const holdA = (seq: number) => ({ ...A, seq, id: holdId({ ...own, sessionId: "large:replay" }, A, seq) });
  const byIncarnation = (f: F) => f.standard.map((m) => (m.hold ? `${m.hold.epoch}:${m.hold.fingerprint.slice(0, 4)}#${m.hold.seq}` : "plain"));

  test("hold-only A, B already enqueued: delayed then duplicate A hold acks as covered; NO send at all (B's hold neither created nor extended)", async () => {
    const f = await fixture();
    f.deps.deadlineIsolation = { ...f.deps.deadlineIsolation!, current: (scope) => captureLargeReadSource(f.x.db, scope) };
    await enqueueLargeRead(f.x.db, own);
    markNow(f);
    const before = await jobRow(f);
    for (const body of [requeueBody(f.key, undefined, holdA(3)), requeueBody(f.key, undefined, holdA(3))]) {
      f.clock.t += 1_000;
      expect(await f.plain(body)).toEqual([]);
    }
    expect(f.standard).toEqual([]);
    expect(await jobRow(f)).toEqual(before);
    expect(passes).toBe(0);
  }, 60_000);

  test("hold-only A, B not yet enqueued: each A hold re-holds for A only (A's incarnation, next seq); no send for B's incarnation; then B's own hint creates B's single hold", async () => {
    const f = await fixture();
    f.deps.deadlineIsolation = { ...f.deps.deadlineIsolation!, current: (scope) => captureLargeReadSource(f.x.db, scope) };
    markNow(f);
    const b = (await captureLargeReadSource(f.x.db, own))!.source;
    f.clock.t += 1_000;
    expect(await f.plain(requeueBody(f.key, new Date(f.clock.t).toISOString(), holdA(3)))).toEqual([]);
    // The duplicate of the SAME A hold (seq 3) mints the same successor id, and still only for A.
    expect(await f.plain(requeueBody(f.key, new Date(f.clock.t).toISOString(), holdA(3)))).toEqual([]);
    expect(byIncarnation(f)).toEqual(["1:ffff#4", "1:ffff#4"]);
    expect(f.standard.every((m) => m.hold!.id === holdA(4).id)).toBe(true);
    expect(f.standard.some((m) => m.hold!.fingerprint === b.fingerprint)).toBe(false);
    expect(await f.jobs()).toEqual([]);
    // B's own ordinary hint (FIFO) routes and creates exactly one hold, for B's incarnation.
    f.clock.t += 1_000;
    expect(await f.fifo([await f.fifoRecord(1)])).toEqual([]);
    expect(f.standard.slice(2).map((m) => m.hold && { epoch: m.hold.epoch, fingerprint: m.hold.fingerprint })).toEqual([{ epoch: b.epoch, fingerprint: b.fingerprint }]);
    expect(await f.jobs()).toEqual([expect.objectContaining({ state: "queued" })]);
  }, 60_000);

  test("saved B wakeup FIRST, then delayed and duplicate hold-only A: zero B holds created or extended before B's wakeup runs; then the saved B wakeup runs and B's work survives", async () => {
    const f = await fixture();
    f.deps.deadlineIsolation = { ...f.deps.deadlineIsolation!, current: (scope) => captureLargeReadSource(f.x.db, scope) };
    const b = (await captureLargeReadSource(f.x.db, own))!.source;
    const isB = (m: F["standard"][number]) => m.hold?.epoch === b.epoch && m.hold?.fingerprint === b.fingerprint;
    // B's own routing on the FIFO path: first hit, job enqueued, one hold for B. That hold is B's scheduled wakeup: saved.
    expect(await f.fifo([await f.fifoRecord(1)])).toEqual([]);
    expect(f.standard.filter(isB)).toHaveLength(1);
    const savedB = f.standard[0]!;
    const job = await jobRow(f);
    const sentBefore = f.standard.length;
    // Delayed, then duplicate, hold-only deliveries carrying expected A.
    for (let i = 0; i < 2; i++) {
      f.clock.t += 1_000;
      expect(await f.plain(requeueBody(f.key, new Date(f.clock.t).toISOString(), holdA(3)))).toEqual([]);
    }
    const sentByA = f.standard.slice(sentBefore);
    // Counted BEFORE B's wakeup runs: the A holds created or extended ZERO B holds. B's job row exists, so the
    // obsolete A hold is covered by it: no send at all.
    expect(sentByA.filter(isB)).toHaveLength(0);
    expect(sentByA).toEqual([]);
    expect(await jobRow(f)).toEqual(job);
    expect(passes).toBe(1);
    // Now the saved B wakeup runs: it holds again for B (its job is still queued), with the next sequence.
    f.clock.t += MIN;
    expect(await wake(f, savedB)).toEqual([]);
    const after = f.standard.slice(sentBefore);
    expect(after).toHaveLength(1);
    expect(isB(after[0]!)).toBe(true);
    expect(after[0]!.hold!.seq).toBe(savedB.hold!.seq + 1);
    expect(await jobRow(f)).toEqual(job);
    expect(passes).toBe(1);
  }, 60_000);
});
