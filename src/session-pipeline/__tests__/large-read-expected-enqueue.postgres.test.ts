/**
 * OFFLINE PROTOTYPE evidence for T124 (layered-sessions-pipeline d99235d6fca0): the capture-bound large-read enqueue
 * (fixtures/large-read-expected-enqueue.prototype.sql, not a migration) on real PostgreSQL
 * (SESSION_INGEST_TEST_PG_URL; hold-harness run.ts gives a disposable 15/16/17). Two independent backends (distinct
 * pg_backend_pid, asserted by openPair) plus an observer; every interleaving is a pg_locks-observed block on the
 * fixture's test gate (an advisory key the RPC waits on right after its capture and validation), never timing.
 *
 * Recorded separately in each case: the job row's mutation (row_to_json before/after), its lease state, and the
 * committed effect (what claim_session_large_read then does).
 *
 *   1  A captured, then B commits (epoch moves), slot empty: A inserts a STALE A job; claim refuses it (superseded);
 *      B's ordinary enqueue then sets B. (Stale A work is possible: the contract decision is Pipeline's.)
 *   2  A captured, then B commits AND B's job is claimed (leased): A writes nothing; B's row and lease byte-identical.
 *   3  same epoch, different fingerprint (a pending-row move): A captured, then A' commits and A' is enqueued: A
 *      writes nothing (newer_job); the A' row byte-identical.
 *   4  B committed before A's capture (the other order): A returns obsolete at once, no mutation.
 *   5  a matching failed row: enqueued:true, changed:false, state 'failed' (not durable recovery; the caller must
 *      not treat it as such); row unchanged.
 *   6  the ordinary enqueue (gated copy of 20261105021000's) captured B, then C commits and C's job is enqueued:
 *      B's late upsert OVERWRITES the newer C job (pre-existing behaviour of enqueue_session_large_read, recorded
 *      here as a finding, not changed by this prototype).
 *   7  B's durable wakeup with its original delivery lost, for a measured-large scope: discover_session_large_reads
 *      enqueues B after A's stale job was superseded. (Below the measured thresholds there is no SQL-side wakeup;
 *      the consumer's sweep / S3 redelivery is the only one, not provable here.)
 */
import { afterAll, afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "../../session-ingest/__tests__/hold-harness/pg.ts";
import { expectBlocks, openPair, type Conn, type Pair } from "../../session-ingest/__tests__/hold-harness/two-conn.ts";
import { createFsStore, exportReplayFixture } from "./replay-fixture.ts";
import { enqueueLargeReadExpected } from "../search-deadline-isolation.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const PROTOTYPE = readFileSync(join(import.meta.dir, "fixtures/large-read-expected-enqueue.prototype.sql"), "utf8");
const GATE = 424242;
const S = { org: "org_replay", ws: "ws_replay", harness: "claude", session: "large_replay" };
const ARGS = [S.org, S.ws, S.harness, S.session];
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);
afterAll(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); });

interface Rig { db: ScratchDatabase; o: pg.Client; pair: Pair }
async function rig(): Promise<Rig> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "lrexp" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  const o = new pg.Client({ connectionString: db.ownerUrl }); await o.connect();
  cleanups.push(() => o.end());
  await o.query(PROTOTYPE);
  const root = await mkdtemp(join(tmpdir(), "lrexp-"));
  cleanups.push(() => rm(root, { recursive: true, force: true }));
  await exportReplayFixture(createFsStore({ root }), o as never, 3);
  const pair = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => pair.close());
  expect(pair.a.pid).not.toBe(pair.b.pid);
  return { db, o, pair };
}
type Src = { epoch: string; fingerprint: string };
const capture = async (c: pg.Client | Conn): Promise<Src> =>
  (await c.query("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4)->'source' s", ARGS)).rows[0].s;
const job = async (c: pg.Client) => (await c.query("SELECT row_to_json(j)::text r FROM sessions.session_large_read_jobs j")).rows.map((r) => r.r as string);
const jobRow = async (c: pg.Client) => (await c.query("SELECT state,source_epoch::text epoch,source_fingerprint fp,lease_nonce IS NOT NULL leased FROM sessions.session_large_read_jobs")).rows[0];
const expected = (c: Conn | pg.Client, src: Src) =>
  c.query("SELECT sessions.enqueue_session_large_read_expected($1,$2,$3,$4,$5::bigint,$6) r", [...ARGS, src.epoch, src.fingerprint]).then((r) => r.rows[0].r);
const ordinary = (c: Conn | pg.Client, gated = false) =>
  c.query(`SELECT sessions.enqueue_session_large_read${gated ? "_gated" : ""}($1,$2,$3,$4) r`, ARGS).then((r) => r.rows[0].r);
const claim = (c: pg.Client) => c.query("SELECT sessions.claim_session_large_read(gen_random_uuid(),120) j").then((r) => r.rows[0].j);
const moveEpoch = (c: pg.Client) => c.query("UPDATE sessions.session_index SET revision=revision+1 WHERE org_id=$1 AND workspace_id=$2 AND harness=$3", [S.org, S.ws, S.harness]);
const gateOn = (c: Conn) => c.query("SELECT set_config('rh.large_read_test_gate',$1,false)", [String(GATE)]);
const holdGate = (c: Conn) => c.query("SELECT pg_advisory_xact_lock($1)", [GATE]);

describe.skipIf(!url)("capture-bound large-read enqueue prototype (real PostgreSQL, two backends)", () => {
  test("1. A captured, then B commits, slot empty: stale A job inserted; claim refuses it; B's ordinary enqueue sets B", async () => {
    const { o, pair } = await rig();
    const A = await capture(o);
    await gateOn(pair.b);
    let B: Src | undefined;
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A),
      whileBlocked: async () => { await moveEpoch(o); B = await capture(o); },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted)).toBe(true);
    expect(r.receipt.holderPid).toBe(pair.a.pid);
    expect(B!.epoch).not.toBe(A.epoch);
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: true, changed: true, state: "queued" } });
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: A.epoch, fp: A.fingerprint, leased: false }); // the stale A job
    expect(await claim(o)).toBeNull(); // committed effect: claim never runs stale A
    expect(await jobRow(o)).toMatchObject({ state: "superseded", epoch: A.epoch });
    expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: true });
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: B!.epoch, fp: B!.fingerprint, leased: false });
  });

  test("2. A captured, then B commits and B's job is claimed (leased): A writes nothing; B's row and lease byte-identical", async () => {
    const { o, pair } = await rig();
    const A = await capture(o);
    await gateOn(pair.b);
    let before: string[] = [];
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A),
      whileBlocked: async () => {
        await moveEpoch(o);
        await ordinary(o);
        expect(await claim(o)).not.toBeNull();
        before = await job(o);
      },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: false, obsolete: true, reason: "newer_job" } });
    expect(await job(o)).toEqual(before);
    expect(await jobRow(o)).toMatchObject({ state: "running", leased: true });
  });

  test("3. same epoch, different fingerprint: A captured, A' commits and is enqueued: A writes nothing; A' row byte-identical", async () => {
    const { o, pair } = await rig();
    const A = await capture(o);
    const pending = (await o.query("SELECT count(*)::int n FROM sessions.session_directory_event_pending WHERE org_id=$1", [S.org])).rows[0].n;
    if (pending === 0) await o.query("INSERT INTO sessions.session_directory_event_pending(org_id,workspace_id,harness,source_session_id) VALUES($1,$2,$3,$4)", ARGS);
    const A0 = await capture(o); // the fixture's capture after any pending row exists
    await gateOn(pair.b);
    let A1: Src | undefined;
    let before: string[] = [];
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A0),
      whileBlocked: async () => {
        await o.query("UPDATE sessions.session_directory_event_pending SET epoch=epoch+1 WHERE org_id=$1", [S.org]);
        A1 = await capture(o);
        await ordinary(o);
        before = await job(o);
      },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
    expect(A1!.epoch).toBe(A0.epoch);
    expect(A1!.fingerprint).not.toBe(A0.fingerprint);
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: false, obsolete: true, reason: "newer_job" } });
    expect(await job(o)).toEqual(before);
    expect(await jobRow(o)).toMatchObject({ fp: A1!.fingerprint });
    void A;
  });

  test("4. B committed before A's capture: obsolete at once (incarnation_moved), no mutation", async () => {
    const { o } = await rig();
    const A = await capture(o);
    await moveEpoch(o);
    expect(await expected(o, A)).toMatchObject({ enqueued: false, obsolete: true, reason: "incarnation_moved" });
    expect(await job(o)).toEqual([]);
  });

  test("5. a matching failed row: enqueued true, changed false, state 'failed' (not recovery); row unchanged", async () => {
    const { o } = await rig();
    const A = await capture(o);
    await ordinary(o);
    await o.query("UPDATE sessions.session_large_read_jobs SET state='failed',attempts=5,last_error='attempts_exhausted'");
    const before = await job(o);
    expect(await expected(o, A)).toMatchObject({ enqueued: true, changed: false, state: "failed" });
    expect(await job(o)).toEqual(before);
  });

  test("6. FINDING: the ordinary enqueue, paused after its capture of B, overwrites a newer C job", async () => {
    const { o, pair } = await rig();
    await moveEpoch(o);
    const B = await capture(o);
    await gateOn(pair.b);
    let C: Src | undefined;
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => ordinary(c, true),
      whileBlocked: async () => { await moveEpoch(o); C = await capture(o); await ordinary(o); },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: true } });
    // Pre-existing 20261105021000 behaviour: the late upsert of B replaces the newer C job.
    expect(await jobRow(o)).toMatchObject({ state: "queued", epoch: B.epoch });
    expect(Number(C!.epoch)).toBeGreaterThan(Number(B.epoch));
  });

  test("7. B's durable wakeup without its delivery (measured-large scope): discovery enqueues B after stale A is superseded", async () => {
    const { o } = await rig();
    await o.query("UPDATE sessions.session_index SET raw_bytes=67108865 WHERE org_id=$1", [S.org]);
    const A = await capture(o);
    expect(await expected(o, A)).toMatchObject({ enqueued: true, changed: true });
    await moveEpoch(o);
    const B = await capture(o);
    expect(await claim(o)).toBeNull();
    expect(await jobRow(o)).toMatchObject({ state: "superseded", epoch: A.epoch });
    await o.query("SELECT sessions.discover_session_large_reads()");
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: B.epoch, fp: B.fingerprint, leased: false });
  });

  test("8. the TS caller keeps source_unavailable (retry) apart from obsolete, and reports a failed row's state", async () => {
    const { o } = await rig();
    const db = { query: (q: string, p?: unknown[]) => o.query(q, p as unknown[]) } as never;
    const scope = { orgId: S.org, workspaceId: S.ws, harness: S.harness, sessionId: S.session };
    const A = await capture(o);
    expect(await enqueueLargeReadExpected(db, scope, A)).toMatchObject({ kind: "enqueued", changed: true, state: "queued", identity: { sessionId: "large:replay" }, source: A });
    await o.query("UPDATE sessions.session_large_read_jobs SET state='failed'");
    expect(await enqueueLargeReadExpected(db, scope, A)).toMatchObject({ kind: "enqueued", changed: false, state: "failed" });
    await moveEpoch(o);
    expect(await enqueueLargeReadExpected(db, scope, A)).toEqual({ kind: "obsolete", reason: "incarnation_moved" });
    await o.query("UPDATE sessions.session_index SET deleted_at=now() WHERE org_id=$1", [S.org]);
    expect(await enqueueLargeReadExpected(db, scope, A)).toEqual({ kind: "unavailable" });
  });

  test("9. previously leased A row, B now current (authoritative mismatch): obsolete, zero mutation; A's row and lease byte-identical", async () => {
    const { o } = await rig();
    const A = await capture(o);
    await ordinary(o);
    expect(await claim(o)).not.toBeNull(); // A's job is running with a live lease
    await moveEpoch(o); // B commits; nothing re-routes it in this case
    const before = await job(o);
    expect(await expected(o, A)).toMatchObject({ enqueued: false, obsolete: true, reason: "incarnation_moved" });
    expect(await job(o)).toEqual(before);
    expect(await jobRow(o)).toMatchObject({ state: "running", epoch: A.epoch, leased: true });
  });

  test("10. previously leased A row, A still current: enqueued, changed false, state running; row and lease byte-identical", async () => {
    const { o } = await rig();
    const A = await capture(o);
    await ordinary(o);
    expect(await claim(o)).not.toBeNull();
    const before = await job(o);
    expect(await expected(o, A)).toMatchObject({ enqueued: true, changed: false, state: "running" });
    expect(await job(o)).toEqual(before);
  });
});
