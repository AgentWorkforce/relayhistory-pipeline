/**
 * Migration 20261105062410 (slot assigned by layered-sessions-pipeline 220fe2274d20; code only) on real PostgreSQL
 * (SESSION_INGEST_TEST_PG_URL; hold-harness run.ts gives a disposable 15/16/17). Two independent backends (distinct
 * pg_backend_pid, asserted by openPair) plus an observer; every interleaving is a pg_locks-observed block on the
 * fixture's test gate (an advisory key the RPC waits on right after its capture and validation), never timing.
 *
 * Recorded separately in each case: the job row's mutation (row_to_json before/after), its lease state, and the
 * committed effect (what claim_session_large_read then does).
 *
 *   1  parked A, empty slot, B commits: A creates nothing (no_job, Pipeline ae37e77b3809: only an existing exact
 *      job may be acted on); B's originating route creates B. 1b: A's own superseded job is only re-queued (claim
 *      supersedes it again); 1c: an older incarnation's job is never advanced (no_exact_job).
 *   2  A captured, then B commits AND B's job is claimed (leased): A writes nothing; B's row and lease byte-identical.
 *   3  same epoch, different fingerprint (a pending-row move): A captured, then A' commits and A' is enqueued: A
 *      writes nothing (newer_job); the A' row byte-identical.
 *   4  B committed before A's capture (the other order): A returns obsolete at once, no mutation.
 *   5  a matching failed row: enqueued:true, changed:false, state 'failed' (not durable recovery; the caller must
 *      not treat it as such); row unchanged.
 *   6  the ordinary enqueue (062410's newer-epoch guard) captured B, then C commits and C's job is enqueued and
 *      claimed: B's late upsert changes nothing; C's row and lease byte-identical (021000 overwrote it).
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
// The migration under test is applied by migrateAsOwner. Test-only gated copies (*_gated) are derived from ITS text:
// the same bodies, renamed, with one wait on an advisory key at the point under test (after the expected enqueue's
// capture and validation; after the ordinary enqueue's capture), so another backend can commit in exactly that window.
const MIGRATION = readFileSync(join(import.meta.dir, "../../../supabase/migrations/20261105062410_session_large_read_expected_enqueue.sql"), "utf8");
const GATE_SQL = `
 IF coalesce(current_setting('rh.large_read_test_gate',true),'')<>'' THEN
  PERFORM pg_advisory_xact_lock_shared(current_setting('rh.large_read_test_gate')::bigint);
 END IF;`;
function gated(name: string, tag: string, after: string): string {
  const start = MIGRATION.indexOf(`FUNCTION sessions.${name}(`);
  const create = MIGRATION.lastIndexOf("CREATE", start);
  const end = MIGRATION.indexOf(`END ${tag};`, start) + `END ${tag};`.length;
  const body = MIGRATION.slice(create, end);
  if (start < 0 || !body.includes(after)) throw Error(`gate anchor missing in ${name}`);
  return body.replace(/^CREATE (OR REPLACE )?FUNCTION sessions\.(\w+)\(/, "CREATE FUNCTION sessions.$2_gated(").replace(after, after + GATE_SQL);
}
const PROTOTYPE = [
  gated("enqueue_session_large_read_expected", "$expected$", "RETURN jsonb_build_object('enqueued',false,'obsolete',true,'reason','incarnation_moved');\n END IF;"),
  gated("enqueue_session_large_read", "$enqueue$", "RETURN jsonb_build_object('enqueued',false,'reason','source_unavailable'); END IF;"),
].join("\n");
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
const expected = (c: Conn | pg.Client, src: Src, gate = false) =>
  c.query(`SELECT sessions.enqueue_session_large_read_expected${gate ? "_gated" : ""}($1,$2,$3,$4,$5::bigint,$6) r`, [...ARGS, src.epoch, src.fingerprint]).then((r) => r.rows[0].r);
const ordinary = (c: Conn | pg.Client, gated = false) =>
  c.query(`SELECT sessions.enqueue_session_large_read${gated ? "_gated" : ""}($1,$2,$3,$4) r`, ARGS).then((r) => r.rows[0].r);
const claim = (c: pg.Client) => c.query("SELECT sessions.claim_session_large_read(gen_random_uuid(),120) j").then((r) => r.rows[0].j);
const moveEpoch = (c: pg.Client) => c.query("UPDATE sessions.session_index SET revision=revision+1 WHERE org_id=$1 AND workspace_id=$2 AND harness=$3", [S.org, S.ws, S.harness]);
const gateOn = (c: Conn) => c.query("SELECT set_config('rh.large_read_test_gate',$1,false)", [String(GATE)]);
const holdGate = (c: Conn) => c.query("SELECT pg_advisory_xact_lock($1)", [GATE]);

describe.skipIf(!url)("capture-bound large-read enqueue prototype (real PostgreSQL, two backends)", () => {
  test("1. parked A, empty slot, B commits: A creates NOTHING (no_job); zero lease change; B's originating route then creates B", async () => {
    const { o, pair } = await rig();
    const A = await capture(o);
    await gateOn(pair.b);
    let B: Src | undefined;
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A, true),
      whileBlocked: async () => { await moveEpoch(o); B = await capture(o); },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
    expect(B!.epoch).not.toBe(A.epoch);
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: false, reason: "no_job" } });
    expect(await job(o)).toEqual([]); // zero insertion into the empty slot, so no lease either
    expect(await claim(o)).toBeNull();
    expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: true }); // B's own originating route
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: B!.epoch, fp: B!.fingerprint, leased: false });
  });

  test("1b. parked A, its own exact job superseded, B commits: A only re-queues its own superseded job (which claim supersedes again); never writes B", async () => {
    const { o, pair } = await rig();
    const A = await capture(o);
    await ordinary(o);
    await o.query("UPDATE sessions.session_large_read_jobs SET state='superseded'");
    await gateOn(pair.b);
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A, true),
      whileBlocked: async () => { await moveEpoch(o); },
    });
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: true, changed: true, state: "queued" } });
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: A.epoch, fp: A.fingerprint, leased: false });
    expect(await claim(o)).toBeNull();
    expect(await jobRow(o)).toMatchObject({ state: "superseded", epoch: A.epoch });
  });

  test("1c. an older incarnation's job, A current: no_exact_job, row byte-identical (no authority to advance it)", async () => {
    const { o } = await rig();
    await ordinary(o); // Z's job
    await moveEpoch(o);
    const A = await capture(o);
    const before = await job(o);
    expect(await expected(o, A)).toMatchObject({ enqueued: false, reason: "no_exact_job" });
    expect(await job(o)).toEqual(before);
  });

  test("2. A captured, then B commits and B's job is claimed (leased): A writes nothing; B's row and lease byte-identical", async () => {
    const { o, pair } = await rig();
    const A = await capture(o);
    await gateOn(pair.b);
    let before: string[] = [];
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A, true),
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
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => expected(c, A0, true),
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

  test("6. the ordinary enqueue (062410), paused after its capture of B, never overwrites C's newer leased job", async () => {
    const { o, pair } = await rig();
    await moveEpoch(o);
    const B = await capture(o);
    await gateOn(pair.b);
    let C: Src | undefined;
    let before: string[] = [];
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => ordinary(c, true),
      whileBlocked: async () => {
        await moveEpoch(o); C = await capture(o); await ordinary(o);
        expect(await claim(o)).not.toBeNull(); // C's job running with a live lease
        before = await job(o);
      },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
    expect(Number(C!.epoch)).toBeGreaterThan(Number(B.epoch));
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: true, changed: false } });
    expect(await job(o)).toEqual(before); // C's row and lease byte-identical
    expect(await jobRow(o)).toMatchObject({ state: "running", epoch: C!.epoch, leased: true });
    // C's own enqueue still succeeds (no change: its job is current and leased).
    expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: false, source: C });
    expect(await jobRow(o)).toMatchObject({ state: "running", epoch: C!.epoch, leased: true });
  });

  test("6b. the ordinary enqueue, paused after its capture of B, never overwrites C's newer QUEUED job either", async () => {
    const { o, pair } = await rig();
    await moveEpoch(o);
    const B = await capture(o);
    await gateOn(pair.b);
    let C: Src | undefined;
    let before: string[] = [];
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => ordinary(c, true),
      whileBlocked: async () => { await moveEpoch(o); C = await capture(o); await ordinary(o); before = await job(o); },
    });
    expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
    expect(Number(C!.epoch)).toBeGreaterThan(Number(B.epoch));
    expect(r.waited).toMatchObject({ ok: true, value: { enqueued: true, changed: false } });
    expect(await job(o)).toEqual(before);
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: C!.epoch, fp: C!.fingerprint, leased: false });
    // C's own enqueue still succeeds and leaves C queued.
    expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: false, source: C });
    expect(await jobRow(o)).toEqual({ state: "queued", epoch: C!.epoch, fp: C!.fingerprint, leased: false });
  });

  for (const leased of [false, true]) {
    test(`6${leased ? "d" : "c"}. FINDING: the ordinary enqueue, paused after capturing A0, REPLACES a same-epoch newer A1 ${leased ? "leased" : "queued"} job (equal-epoch ordering unresolved)`, async () => {
      const { o, pair } = await rig();
      const pending = (await o.query("SELECT count(*)::int n FROM sessions.session_directory_event_pending WHERE org_id=$1", [S.org])).rows[0].n;
      if (pending === 0) await o.query("INSERT INTO sessions.session_directory_event_pending(org_id,workspace_id,harness,source_session_id) VALUES($1,$2,$3,$4)", ARGS);
      const A0 = await capture(o);
      await gateOn(pair.b);
      let A1: Src | undefined;
      let before: string[] = [];
      const r = await expectBlocks(pair, {
        holder: pair.a, waiter: pair.b, hold: holdGate, wait: (c) => ordinary(c, true),
        whileBlocked: async () => {
          // A same-epoch writer (the directory event projection's pending rows) moves the fingerprint; A1 is routed.
          await o.query("UPDATE sessions.session_directory_event_pending SET epoch=epoch+1 WHERE org_id=$1", [S.org]);
          A1 = await capture(o);
          await ordinary(o);
          if (leased) expect(await claim(o)).not.toBeNull();
          before = await job(o);
        },
      });
      expect(r.receipt.waiting.some((l) => l.locktype === "advisory" && !l.granted) && r.receipt.holderPid === pair.a.pid).toBe(true);
      expect(A1!.epoch).toBe(A0.epoch);
      expect(A1!.fingerprint).not.toBe(A0.fingerprint);
      expect(before.length).toBe(1);
      expect(r.waited).toMatchObject({ ok: true, value: { enqueued: true, changed: true } });
      // Recorded behaviour (021000 rule kept within one epoch): the stale A0 upsert replaces A1's row${leased ? " and clears its lease" : ""}.
      expect(await jobRow(o)).toEqual({ state: "queued", epoch: A0.epoch, fp: A0.fingerprint, leased: false });
      // Claim then refuses the stale A0 (superseded), and A1's own enqueue restores A1.
      expect(await claim(o)).toBeNull();
      expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: true, source: A1 });
      expect(await jobRow(o)).toEqual({ state: "queued", epoch: A1!.epoch, fp: A1!.fingerprint, leased: false });
    });
  }

  test("7. B's durable wakeup without its delivery (measured-large scope): discovery enqueues B after stale A is superseded", async () => {
    const { o } = await rig();
    await o.query("UPDATE sessions.session_index SET raw_bytes=67108865 WHERE org_id=$1", [S.org]);
    const A = await capture(o);
    expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: true }); // A's originating route
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
    expect(await enqueueLargeReadExpected(db, scope, A)).toEqual({ kind: "no_job", reason: "no_job" });
    await ordinary(o);
    expect(await enqueueLargeReadExpected(db, scope, A)).toMatchObject({ kind: "enqueued", changed: false, state: "queued", identity: { sessionId: "large:replay" }, source: A });
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

  test("11. source change after claim: the worker's heartbeat refuses (lease lost, no publication path); the row is not rewritten", async () => {
    const { o } = await rig();
    const A = await capture(o);
    expect(await ordinary(o)).toMatchObject({ enqueued: true, changed: true });
    const claimed = await claim(o);
    expect(claimed).toMatchObject({ source: A });
    const nonce = claimed.leaseNonce as string;
    const beat = () => o.query("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok", [S.org, S.ws, S.harness, "large:replay", nonce]).then((r) => r.rows[0].ok);
    expect(await beat()).toBe(true);
    await moveEpoch(o);
    const before = await job(o);
    expect(await beat()).toBe(false); // runLargeReadOnce stops on a lost lease; publish_session_large_read also checks the source
    expect(await job(o)).toEqual(before);
    expect(await jobRow(o)).toMatchObject({ state: "running", epoch: A.epoch, leased: true });
  });
});
