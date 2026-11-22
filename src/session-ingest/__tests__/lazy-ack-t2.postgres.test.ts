/**
 * Opt-in loopback PostgreSQL (SESSION_INGEST_TEST_PG_URL, the fixture superuser on a disposable cluster; skipped with no
 * connection attempt when the variable is unset): multi-connection schedules for 20261105062210_lazy_ack_t2 (gate row
 * sessions.lazy_ack_t2_mode(gate, mode), try-lock-only setters). Everything under test runs as a NON-superuser database
 * owner (owner-role-harness.ts: LOGIN CREATEROLE NOINHERIT NOSUPERUSER NOBYPASSRLS, no added privilege): the migration,
 * the setters, the policy and seat-clock setup. Commits run as the runtime role (fixture SET LOCAL ROLE
 * bulls96_session_ingest) through the production entry point ingest_origin_commit_buffered_batch (062010 origin-claim
 * fence on), at READ COMMITTED unless stated; current_user and role flags are recorded at each. Every setter attempt is
 * ONE autocommit statement (lazy-ack-t2-flip.ts). Which body ran is read in the committing transaction: with the history
 * switch off, main fires the slot statement trigger once per accept (2 here), T2 once per round (1).
 *   S1   a fenced main ACK paused at the clock (holding 50000 shared): a flip attempt is refused at once, the mode is
 *        unchanged and NO exclusive request is queued, so another user's ACK on the same origin still passes 50000; after
 *        both commit the flip succeeds and the next ACK reads 't2' and runs T2.
 *   PRE  an invocation open with the gate off makes the gate change (set_gate('fenced')) busy; after it ends, it passes.
 *   S2   one transaction with two ACKs: a flip attempt between them is busy; the second ACK keeps 'main'.
 *   RACE REPEATABLE READ and SERIALIZABLE, snapshot forced before a committed off/main -> fenced/t2 change (and the
 *        reverse), then the ACK takes 50000 shared and reads the row FOR SHARE: 40001, nothing durable; a control
 *        transaction with the same snapshot shows a plain and a FOR KEY SHARE read returning the stale row without error.
 *        The retry restarts the whole transaction. Stable off/main: commits on main. A busy owner row lock: READ
 *        COMMITTED does not wait; REPEATABLE READ waits (55P03 under lock_timeout) or proceeds once it is released.
 *   WIN  a successful flip holds 50000 exclusive until its COMMIT: a writer arriving in that window waits for exactly
 *        that commit and then runs the new body; acquisition through COMMIT is measured. WIN-GATE: the same with the
 *        gate and the mode set in one owner transaction (READ COMMITTED: the queued ACK sees both new values).
 *   MATRIX REPEATABLE READ / SERIALIZABLE x off/main, fenced/main, fenced/t2 through the runtime entry points: staged
 *        commit, and a suppressed receipt's claim (043500 refuses it, 40001, in every state; at READ COMMITTED it commits).
 *   ROW  the row busy: the attempt takes 50000, fails NOWAIT on the row, and its transaction ends before the reply;
 *        a writer then takes 50000 shared at once; the operator loop returns no_switch.
 *   ERA  an erase-shaped exclusive 50000 holder: attempts are refused at once; the budget ends in no_switch.
 *   BUD  a held ingest transaction longer than the budget: no_switch, mode unchanged; then switched after it ends.
 *   CYC  set_live(true)'s lock shape (clock FOR UPDATE, then a session_directory write that needs 50000 shared) while
 *        flips are attempted and an ACK holding 50000 shared waits on the clock: with no queued exclusive the write is
 *        granted at once, even with lock_timeout below deadlock_timeout.
 *   GATE fenced->off (lazy_ack_t2_set_gate) and main->t2 (set_mode) overlapping, in both orders: the second is busy
 *        (fail-fast, nothing changed); after the first commits, the second is refused; no sequence ends off/t2.
 *   UP   T2 ACK A plans under KEY SHARE, then its upgrade waits on T2 ACK B (another origin, NO KEY UPDATE to commit);
 *        B commits; A's upgrade reads B's last_seat (fresh); seats contiguous and unique; no 40P01.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { migrateAsOwner, ownedLoopbackUrl, ownerDatabase, roleReceipt, OWNER_ROLE, type OwnerDatabase } from "./owner-role-harness.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { FLIP_STATEMENT, lazyAckT2FlipOutcome as flipLazyAckT2Mode, type FlipMode, type FlipOptions } from "../lazy-ack-t2-flip.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// pg-proof sets SESSION_INGEST_TEST_PG_REQUIRED=1: there an unset URL is a failure, never a skip.
if (process.env.SESSION_INGEST_TEST_PG_REQUIRED === "1" && !url) throw Error("SESSION_INGEST_TEST_PG_URL required (SESSION_INGEST_TEST_PG_REQUIRED=1)");
ownedLoopbackUrl(url);
const epoch = "3".repeat(64), identity = "c".repeat(64);
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const CALLS = "SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_history_native_changed'),0)::int n";

describe.skipIf(!url)("062210 T2 staged ACK: fence and upgrade schedules (owned PostgreSQL)", () => {
  // pool: the owner login (setters, setup, owner reads). fx: the fixture superuser (observation, and the only login that
  // can SET ROLE to the runtime role). target: the fixture URL of the database under test.
  let db: OwnerDatabase, pool: pg.Pool, fx: pg.Pool, generation: string, target: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const ledgerFor = (intercept?: (sql: string, p: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await fx.connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
  async function custody(t: any, raw: any) {
    const p = await prepareDelivery(t, raw);
    const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    return { witness, sourceDigest: (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
  }
  const warmed = new Set<string>();
  /** Claim and stage committed for a two-record batch of (user, origin); the commit payload is captured, not run. */
  async function prepared(user: string, origin: string, n = 2) {
    const t = { ...TENANT, userId: user };
    const mk = async (id: string, ids: string[]) => batch(ids.map((rid) => record({ record_id: rid, revision: 2, session_id: "s-" + rid, origin_id: origin })),
      { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
    if (!warmed.has(user + "/" + origin)) {
      const warm = await mk(`t2-warm-${++serial}`, [`t2-warm-r${serial}`]);
      expect((await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, warm) } as never, t, warm)).outcome).toBe("accepted_buffered");
      warmed.add(user + "/" + origin);
    }
    const s = ++serial;
    const raw = await mk(`t2-b-${s}`, Array.from({ length: n }, (_, i) => `t2-r${s}-${i}`));
    let payload: string | undefined;
    const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
    if (typeof payload !== "string") throw Error("payload_not_captured");
    return payload;
  }
  /** A suppressed receipt's claim (its session deleted after a warm delivery); the claim payload is captured, not run. */
  async function suppressedClaim(user: string, origin: string) {
    await prepared(user, origin, 1); // warm (first-seen adoption) for this user/origin
    const t = { ...TENANT, userId: user }, s = ++serial, sid = `s-supp-${s}`;
    await pool.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5)", [t.orgId, t.workspaceId, user, origin, sid]);
    const raw = await batch([record({ record_id: `supp-r${s}`, revision: 2, session_id: sid, origin_id: origin })], { batch_id: `supp-b${s}`, account_id: await deliveryAccount(t), origin_id: origin });
    let payload: string | undefined, sql: string | undefined;
    const ledger = ledgerFor((q, params) => { if (q.includes("claim_buffered_batch")) { payload = params[0] as string; sql = q; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
    if (typeof payload !== "string" || !sql) throw Error("claim_not_captured");
    return { payload, sql, batchId: `supp-b${s}` };
  }
  /** The supported gate change (owner, one autocommit statement; mode must be 'main'). */
  const setGate = async (v: "off" | "fenced") => { await pool.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [v]); };
  const mode = async () => (await pool.query("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows[0].mode as string;
  const gateMode = async () => (await pool.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode")).rows[0] as { gate: string; mode: string };
  const setMode = async (m: FlipMode) => { expect((await flipLazyAckT2Mode((sql, p) => pool.query(sql, p as never[]) as never, m, { budgetMs: 5000 })).result).toBe("switched"); };
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  /** Wait until backend pid is blocked on a heavyweight lock. */
  async function lockWaiting(pid: number, ms = 10000) {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if ((await fx.query("SELECT wait_event_type='Lock' w FROM pg_stat_activity WHERE pid=$1", [pid])).rows[0]?.w) return;
      await sleep(15);
    }
    throw Error(`pid ${pid} never waited on a lock`);
  }
  /** A dedicated connection with an open transaction. */
  async function conn(app: string, iso = "READ COMMITTED") {
    const c = new pg.Client({ connectionString: target, application_name: app }); await c.connect();
    const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
    // track_functions per transaction (this is the fixture superuser's connection): no dependence on the server's setting.
    await c.query(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='30s';SET LOCAL track_functions='pl'`);
    return { c, pid, end: async () => { await c.query("ROLLBACK").catch(() => {}); await c.end(); } };
  }
  /** One commit as the runtime role in c's open transaction; returns its result kind and the trigger calls it made. */
  const runtimeReceipts: string[] = [];
  async function commitIn(c: pg.Client, payload: string) {
    const before = (await c.query(CALLS)).rows[0].n as number;
    await c.query("SET LOCAL ROLE bulls96_session_ingest");
    const who = await roleReceipt(c);
    expect({ user: who.current_user, rolsuper: who.rolsuper, rolbypassrls: who.rolbypassrls }).toEqual({ user: "bulls96_session_ingest", rolsuper: false, rolbypassrls: false });
    runtimeReceipts.push(who.current_user);
    try {
      const r = (await c.query(COMMIT, [payload])).rows[0].result;
      await c.query("RESET ROLE");
      return { ok: true as const, kind: r.kind as string, calls: ((await c.query(CALLS)).rows[0].n as number) - before };
    } catch (e: any) { return { ok: false as const, code: String(e.code), message: String(e.message) }; }
  }
  /** One operator attempt: ONE autocommit statement (its transaction ends before the reply), timed. */
  async function tryFlip(to: FlipMode) {
    const t0 = performance.now();
    try { const old = (await pool.query(FLIP_STATEMENT, [to])).rows[0].old as string; return { ok: true as const, old, ms: performance.now() - t0 }; }
    catch (e: any) { return { ok: false as const, code: String(e.code), message: String(e.message), ms: performance.now() - t0 }; }
  }
  const operatorFlip = (to: FlipMode, opts: FlipOptions = {}) => flipLazyAckT2Mode((sql, p) => pool.query(sql, p as never[]) as never, to, opts);
  const queuedAdvisory = async () => Number((await fx.query("SELECT count(*) n FROM pg_locks WHERE locktype='advisory' AND NOT granted")).rows[0].n);
  const busyFlip = async (to: FlipMode) => {
    const r = await tryFlip(to);
    expect(r).toMatchObject({ ok: false, code: "55P03", message: expect.stringContaining("flip barrier busy") });
    expect(r.ms).toBeLessThan(500); // refused at once, never waits
    expect(await queuedAdvisory()).toBe(0); // and no exclusive request is left queued
    return r;
  };
  /** Owner holds the clock FOR UPDATE: any ACK pauses at its clock lock, AFTER its fence call and mode read. */
  async function clockBlocker() {
    const b = await conn("t2-clock-blocker");
    await b.c.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
    return { release: async () => { await b.c.query("COMMIT"); await b.c.end(); } };
  }

  /** An owner-login connection with an open READ COMMITTED transaction (setter windows, owner row locks). */
  async function ownerConn(app: string) {
    const c = new pg.Client({ connectionString: db.ownerUrl, application_name: app }); await c.connect();
    const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
    await c.query("BEGIN;SET LOCAL lock_timeout='15s'");
    return { c, pid, end: async () => { await c.query("ROLLBACK").catch(() => {}); await c.end(); } };
  }

  beforeAll(async () => {
    db = await ownerDatabase(url!, "t2pg");
    target = db.fixtureUrl;
    pool = new pg.Pool({ connectionString: db.ownerUrl, max: 12 });
    fx = new pg.Pool({ connectionString: db.fixtureUrl, max: 12 });
    scratch = scratchStore();
    const who = await roleReceipt(pool);
    expect(who).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false, rolbypassrls: false, rolcreaterole: true, rolinherit: false });
    console.log(JSON.stringify({ case: "owner", ...who }));
    await migrateAsOwner(db.ownerUrl);
    generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await pool.query("UPDATE sessions.ingest_history_switch SET live=false WHERE singleton");
  }, 300000);
  afterAll(async () => {
    console.log(JSON.stringify({ case: "runtime", commits: runtimeReceipts.length, users: [...new Set(runtimeReceipts)] }));
    await pool?.end(); await fx?.end(); scratch?.done();
    await db?.drop();
  }, 60000);
  // The supported order back: mode main first, then the gate off through lazy_ack_t2_set_gate.
  // Bounded retries on the try-lock's 55P03 only, so a busy barrier at cleanup does not replace the test's own failure.
  const reset = async () => {
    await setMode("main");
    for (let i = 0; ; i++) {
      try { await pool.query("SELECT sessions.lazy_ack_t2_set_gate('off')"); return; }
      catch (e: any) { if (e?.code !== "55P03" || i >= 100) throw e; await sleep(50); }
    }
  };

  test("S1: a busy flip queues nothing: a same-origin ACK of another user still passes 50000 while a fenced main ACK is paused", async () => {
    const pa = await prepared("s1-u1", "s1-o"), pb = await prepared("s1-u2", "s1-o"), pc = await prepared("s1-u3", "s1-o");
    await setGate("fenced");
    try {
      const blocker = await clockBlocker();
      const A = await conn("s1-A"), B = await conn("s1-B");
      try {
        const ra = commitIn(A.c, pa);
        await lockWaiting(A.pid); // at the clock: mode already read ('main'), 50000 shared held
        await busyFlip("t2");
        const rb = commitIn(B.c, pb);
        await lockWaiting(B.pid);
        // B is past the barrier: it waits on the clock (a tuple/transaction lock), not on any advisory lock.
        expect((await fx.query("SELECT locktype FROM pg_locks WHERE pid=$1 AND NOT granted", [B.pid])).rows.map((x) => x.locktype)).not.toContain("advisory");
        await busyFlip("t2");
        expect(await mode()).toBe("main");
        await blocker.release();
        expect(await ra).toEqual({ ok: true, kind: "committed", calls: 2 });
        await A.c.query("COMMIT");
        expect(await rb).toEqual({ ok: true, kind: "committed", calls: 2 });
        await busyFlip("t2"); // B still open
        await B.c.query("COMMIT");
      } finally { await A.end(); await B.end(); }
      expect(await operatorFlip("t2")).toMatchObject({ result: "switched", from: "main", to: "t2" });
      const C = await conn("s1-C");
      try { expect(await commitIn(C.c, pc)).toEqual({ ok: true, kind: "committed", calls: 1 }); await C.c.query("COMMIT"); } finally { await C.end(); }
    } finally { await reset(); }
  }, 60000);

  test("PRE: an invocation open with the gate off makes the gate change busy (fail-fast, nothing queued); after it ends, the change passes", async () => {
    const pa = await prepared("pre-u1", "pre-o1");
    const blocker = await clockBlocker();
    const A = await conn("pre-A");
    try {
      const ra = commitIn(A.c, pa);
      await lockWaiting(A.pid); // gate off: the row read done, 50000 shared held, waiting at the clock
      const t0 = performance.now();
      await expect(pool.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')")).rejects.toMatchObject({ code: "55P03", message: expect.stringContaining("gate unchanged") });
      expect(performance.now() - t0).toBeLessThan(500);
      expect(await queuedAdvisory()).toBe(0);
      expect(await gateMode()).toEqual({ gate: "off", mode: "main" });
      await blocker.release();
      expect(await ra).toEqual({ ok: true, kind: "committed", calls: 2 });
      await expect(pool.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')")).rejects.toMatchObject({ code: "55P03" }); // still open
      await A.c.query("COMMIT");
      expect((await pool.query("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("off");
      expect(await tryFlip("t2")).toMatchObject({ ok: true, old: "main" });
    } finally { await A.end(); await reset(); }
  }, 60000);

  test("S2: two ACKs in one transaction: a flip attempt between them is busy; the second ACK keeps 'main'", async () => {
    const p1 = await prepared("s2-u1", "s2-o1"), p2 = await prepared("s2-u1", "s2-o2");
    await setGate("fenced");
    try {
      const A = await conn("s2-A");
      try {
        expect(await commitIn(A.c, p1)).toEqual({ ok: true, kind: "committed", calls: 2 });
        await busyFlip("t2");
        expect(await commitIn(A.c, p2)).toEqual({ ok: true, kind: "committed", calls: 2 });
        await A.c.query("COMMIT");
      } finally { await A.end(); }
      expect(await tryFlip("t2")).toMatchObject({ ok: true, old: "main" });
    } finally { await reset(); }
  }, 60000);

  test("WIN: a successful flip holds 50000 exclusive until its COMMIT; a writer arriving in that window waits for that commit, then runs T2", async () => {
    const pb = await prepared("win-u1", "win-o1");
    await setGate("fenced");
    try {
      // The operator path: one autocommit statement, acquisition through COMMIT.
      const auto = await tryFlip("t2"); expect(auto).toMatchObject({ ok: true, old: "main" });
      const back = await tryFlip("main"); expect(back).toMatchObject({ ok: true, old: "t2" });
      console.log(JSON.stringify({ case: "WIN", autocommitFlipMs: [auto.ms, back.ms].map((x) => Math.round(x * 10) / 10) }));
      // The same window held open on purpose, so a writer can arrive inside it.
      const F = await ownerConn("win-F"), B = await conn("win-B");
      try {
        expect((await F.c.query("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main");
        const rb = commitIn(B.c, pb);
        await lockWaiting(B.pid);
        expect((await fx.query("SELECT locktype FROM pg_locks WHERE pid=$1 AND NOT granted", [B.pid])).rows.map((x) => x.locktype)).toEqual(["advisory"]);
        await sleep(100);
        const t0 = performance.now(); await F.c.query("COMMIT");
        expect(await rb).toEqual({ ok: true, kind: "committed", calls: 1 }); // reads the committed 't2'
        console.log(JSON.stringify({ case: "WIN", writerReleasedAfterCommitMs: Math.round((performance.now() - t0) * 10) / 10 }));
        await B.c.query("COMMIT");
      } finally { await F.end(); await B.end(); }
    } finally { await reset(); }
  }, 60000);

  test("WIN-GATE (READ COMMITTED): a commit queued on 50000 while ONE owner transaction sets the gate fenced and the mode t2 sees both committed values: the ACK's own gate-read statement takes its snapshot after the barrier is granted", async () => {
    const pb = await prepared("wing-u1", "wing-o1");
    try {
      const F = await ownerConn("wing-F"), B = await conn("wing-B");
      try {
        expect((await F.c.query("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("off");
        expect((await F.c.query("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main"); // re-entrant try-lock
        const rb = commitIn(B.c, pb);
        await lockWaiting(B.pid);
        const w = (await fx.query("SELECT locktype,classid,objid,mode FROM pg_locks WHERE pid=$1 AND NOT granted", [B.pid])).rows;
        expect(w.map((x) => [x.locktype, Number(x.objid), x.mode])).toEqual([["advisory", 50000, "ShareLock"]]); // the transaction's first fence call
        await F.c.query("COMMIT");
        expect(await rb).toEqual({ ok: true, kind: "committed", calls: 1 }); // fenced/t2: the T2 body ran
        await B.c.query("COMMIT");
      } finally { await F.end(); await B.end(); }
    } finally { await reset(); }
  }, 60000);

  test("ROW: mode row busy: the attempt takes 50000, fails NOWAIT on the row, ends its transaction; a writer then takes 50000 at once", async () => {
    const pb = await prepared("row-u1", "row-o1");
    await setGate("fenced");
    try {
      const H = await ownerConn("row-holder"); // owner only: the runtime role cannot lock this row (PGlite gate test)
      const B = await conn("row-B");
      try {
        await H.c.query("SELECT 1 FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE");
        const r = await tryFlip("t2");
        expect(r).toMatchObject({ ok: false, code: "55P03", message: expect.stringContaining('could not obtain lock on row in relation "lazy_ack_t2_mode"') });
        expect(r.ms).toBeLessThan(500);
        expect((await fx.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND objid=50000 AND classid=961996")).rows[0].n).toBe(0);
        const t0 = performance.now();
        expect(await commitIn(B.c, pb)).toEqual({ ok: true, kind: "committed", calls: 2 });
        const wMs = performance.now() - t0;
        console.log(JSON.stringify({ case: "ROW", failedAttemptMs: Math.round(r.ms * 10) / 10, writerAfterMs: Math.round(wMs * 10) / 10 }));
        expect(wMs).toBeLessThan(2000);
        await B.c.query("COMMIT");
        const nr = await operatorFlip("t2", { budgetMs: 300 });
        expect(nr).toMatchObject({ result: "no_switch", reason: "barrier_busy_budget_exhausted" });
        expect(nr.attempts).toBeGreaterThan(1);
        expect(await queuedAdvisory()).toBe(0);
      } finally { await H.end(); await B.end(); }
      expect(await mode()).toBe("main");
    } finally { await reset(); }
  }, 60000);

  test("ERA: an erase-shaped exclusive 50000 holder: every attempt is refused at once; the budget ends in no_switch", async () => {
    const E = await conn("era-E");
    try {
      await E.c.query("SELECT pg_advisory_xact_lock(961996,50000)");
      await busyFlip("t2");
      const nr = await operatorFlip("t2", { budgetMs: 300 });
      expect(nr).toMatchObject({ result: "no_switch", to: "t2" });
      expect(await queuedAdvisory()).toBe(0);
    } finally { await E.end(); }
    expect(await mode()).toBe("main");
  }, 60000);

  test("BUD: an ingest transaction held past the budget gives no_switch with the mode unchanged; switched once it ends", async () => {
    const pa = await prepared("bud-u1", "bud-o1");
    await setGate("fenced");
    try {
      const A = await conn("bud-A");
      try {
        expect(await commitIn(A.c, pa)).toEqual({ ok: true, kind: "committed", calls: 2 });
        const nr = await operatorFlip("t2", { budgetMs: 500 });
        expect(nr).toMatchObject({ result: "no_switch", to: "t2", reason: "barrier_busy_budget_exhausted" });
        expect(nr.elapsedMs).toBeGreaterThanOrEqual(500);
        expect(nr.elapsedMs).toBeLessThan(1500);
        expect(await mode()).toBe("main");
        const later = operatorFlip("t2", { budgetMs: 5000 });
        await sleep(300); await A.c.query("COMMIT");
        expect(await later).toMatchObject({ result: "switched", from: "main", to: "t2" });
      } finally { await A.end(); }
    } finally { await reset(); }
  }, 60000);

  test("CYC: set_live(true)'s lock shape vs flip attempts: no queued exclusive, so the directory write is granted at once (lock_timeout below deadlock_timeout)", async () => {
    const pa = await prepared("cyc-u1", "cyc-o1");
    await setGate("fenced");
    try {
      const X = await conn("cyc-X"), A = await conn("cyc-A");
      try {
        await X.c.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
        const ra = commitIn(A.c, pa);
        await lockWaiting(A.pid); // A holds 50000 shared and waits on X's clock lock
        expect((await operatorFlip("t2", { budgetMs: 300 })).result).toBe("no_switch");
        await busyFlip("t2");
        const dt = Number((await pool.query("SELECT setting FROM pg_settings WHERE name='deadlock_timeout'")).rows[0].setting); // ms, whatever SHOW's unit
        const lt = Math.max(1, Math.floor(dt / 5));
        await X.c.query(`SET LOCAL lock_timeout='${lt}ms'`);
        const t0 = performance.now();
        await X.c.query("UPDATE sessions.session_directory SET state=state WHERE false"); // 050010 trigger: 50000 shared
        const xMs = performance.now() - t0;
        console.log(JSON.stringify({ case: "CYC", lockTimeoutMs: lt, deadlockTimeoutMs: dt, directoryWriteMs: Math.round(xMs * 10) / 10 }));
        expect(xMs).toBeLessThan(lt);
        await X.c.query("COMMIT");
        expect(await ra).toEqual({ ok: true, kind: "committed", calls: 2 });
        await A.c.query("COMMIT");
      } finally { await X.end(); await A.end(); }
      expect(await mode()).toBe("main");
    } finally { await reset(); }
  }, 60000);

  /** Drive the row to a state through the supported setters only (owner, one autocommit statement each). */
  const toState = async (gate: "off" | "fenced", m: FlipMode) => { await setMode("main"); await setGate(gate); if (m === "t2") await setMode("t2"); };
  /** The staged-commit rows of one payload, durable or not (read by the fixture, after the attempt ended). */
  const durable = async (payload: string) => {
    const b = JSON.parse(payload);
    const batchId = String(b.batchId ?? b.batch_id ?? b.batch?.batchId ?? "");
    return Number((await fx.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id=$1 AND state='committed'", [batchId])).rows[0].n);
  };
  /** A transaction at iso whose snapshot is forced now, before any 50000 lock (the first statement reads an unrelated catalog row). */
  async function staleTxn(app: string, iso: string) {
    const R = await conn(app, iso);
    await R.c.query("SELECT 1 FROM pg_catalog.pg_class LIMIT 1");
    expect((await fx.query("SELECT count(*)::int n FROM pg_locks WHERE pid=$1 AND locktype='advisory'", [R.pid])).rows[0].n).toBe(0);
    return R;
  }
  const raceLog: unknown[] = [];
  for (const iso of ["REPEATABLE READ", "SERIALIZABLE"] as const) {
    // how: "setters" = the supported path (each setter locks the row FOR UPDATE NOWAIT, then UPDATEs it); "bare" = one
    // UPDATE by the owner holding 50000 EXCLUSIVE but no row lock first (passes the keep trigger; out of protocol). A FOR
    // KEY SHARE comparator on the same snapshot is recorded for both (observed on 15/16/17: 40001 after the setters,
    // whose prior FOR UPDATE makes the update key-level; the stale row without error after "bare"). The ACK's FOR SHARE
    // must give 40001 in both: the design does not depend on how the row was updated.
    for (const how of ["setters", "bare"] as const)
    for (const [from, to] of [[["off", "main"], ["fenced", "t2"]], [["fenced", "t2"], ["off", "main"]]] as const)
      test(`RACE ${iso} (${how}): snapshot forced before a committed ${from.join("/")} -> ${to.join("/")} change; the ACK then takes 50000 and reads the row FOR SHARE: 40001, nothing durable; the whole-transaction retry sees the new row`, async () => {
        const p = await prepared(`race-${iso[0]}-${how[0]}-${from[0]}`, `race-o-${iso[0]}-${how[0]}-${from[0]}`);
        await toState(from[0], from[1]);
        try {
          const R = await staleTxn("race-R", iso), K1 = await staleTxn("race-K1", iso), K2 = await staleTxn("race-K2", iso);
          try {
            const t0 = performance.now();
            if (how === "setters") await toState(to[0], to[1]); // committed after the snapshots, while none holds 50000
            else {
              const O = await ownerConn("race-bare");
              try { await O.c.query("SELECT pg_advisory_xact_lock(961996,50000)"); await O.c.query("UPDATE sessions.lazy_ack_t2_mode SET gate=$1,mode=$2 WHERE singleton", [to[0], to[1]]); await O.c.query("COMMIT"); }
              finally { await O.end(); }
            }
            expect(await gateMode()).toEqual({ gate: to[0], mode: to[1] });
            // Controls on the same snapshot (fixture; the table is owner-only).
            expect((await K1.c.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode WHERE singleton")).rows[0]).toEqual({ gate: from[0], mode: from[1] });
            // Recorded, not asserted: what a FOR KEY SHARE read does here (the design does not rely on it).
            const keyShare = await K1.c.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode WHERE singleton FOR KEY SHARE").then((r) => r.rows[0].gate + "/" + r.rows[0].mode, (e: any) => String(e.code));
            await expect(K2.c.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode WHERE singleton FOR SHARE")).rejects.toMatchObject({ code: "40001" });
            const r = await commitIn(R.c, p);
            expect(r).toMatchObject({ ok: false, code: "40001", message: expect.stringContaining("could not serialize access due to concurrent update") });
            raceLog.push({ iso, how, from: from.join("/"), to: to.join("/"), ack: (r as any).code, control: { plain: from.join("/"), keyShare, share: "40001" }, ms: Math.round(performance.now() - t0) });
          } finally { await R.end(); await K1.end(); await K2.end(); }
          expect(await durable(p)).toBe(0);
          // The retry restarts the WHOLE transaction (new snapshot): fenced at this level is refused; off/main commits on main.
          const R2 = await conn("race-R2", iso);
          try {
            const r2 = await commitIn(R2.c, p);
            if (to[0] === "fenced") expect(r2).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("the fenced staged ACK requires read committed") });
            else { expect(r2).toEqual({ ok: true, kind: "committed", calls: 2 }); await R2.c.query("COMMIT"); }
          } finally { await R2.end(); }
          if (to[0] === "fenced") {
            const C = await conn("race-C"); // and at READ COMMITTED the same batch runs the new body
            try { expect(await commitIn(C.c, p)).toEqual({ ok: true, kind: "committed", calls: to[1] === "t2" ? 1 : 2 }); await C.c.query("COMMIT"); } finally { await C.end(); }
          }
          expect(await durable(p)).toBe(1);
        } finally { await reset(); }
      }, 60000);
    test(`RACE ${iso}: stable off/main with an early snapshot commits on main (FOR SHARE on a current row)`, async () => {
      const p = await prepared(`stable-${iso[0]}`, `stable-o-${iso[0]}`);
      const R = await staleTxn("stable-R", iso);
      try {
        expect(await commitIn(R.c, p)).toEqual({ ok: true, kind: "committed", calls: 2 });
        // The ACK's FOR SHARE is held to the end of its transaction (and blocks no other ACK's plain or FOR SHARE read).
        expect((await fx.query("SELECT count(*)::int n FROM pg_locks WHERE pid=$1 AND locktype='relation' AND relation='sessions.lazy_ack_t2_mode'::regclass AND mode='RowShareLock'", [R.pid])).rows[0].n).toBe(1);
        await R.c.query("COMMIT");
      } finally { await R.end(); }
      expect(await durable(p)).toBe(1);
    }, 60000);
    for (const st of [["fenced", "main"], ["fenced", "t2"]] as const)
      test(`RACE ${iso}: stable ${st.join("/")} with an early snapshot: FOR SHARE reads the current row, then the staged ACK is refused (55000), nothing durable`, async () => {
        const p = await prepared(`stable-${iso[0]}-${st[1]}`, `stable-o-${iso[0]}-${st[1]}`);
        await toState(st[0], st[1]);
        try {
          const R = await staleTxn("stable-f-R", iso);
          try {
            expect(await commitIn(R.c, p)).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("the fenced staged ACK requires read committed") });
          } finally { await R.end(); }
          expect(await durable(p)).toBe(0);
        } finally { await reset(); }
      }, 60000);
    test(`MATRIX ${iso} through the runtime entry points: staged commit (off/main commits on main; fenced refused 55000) and a suppressed receipt's claim (refused by 043500 itself, 40001, in every state, as on main)`, async () => {
      const rows: unknown[] = [];
      for (const st of [["off", "main"], ["fenced", "main"], ["fenced", "t2"]] as const) {
        const p = await prepared(`mx-${iso[0]}-${st.join("")}`, `mx-o-${iso[0]}-${st.join("")}`);
        const sp = await suppressedClaim(`mxs-${iso[0]}-${st.join("")}`, `mxs-o-${iso[0]}-${st.join("")}`);
        await toState(st[0], st[1]);
        try {
          const A = await conn("mx-A", iso);
          let staged: any;
          try { staged = await commitIn(A.c, p); if (staged.ok) await A.c.query("COMMIT"); } finally { await A.end(); }
          if (st[0] === "off") expect(staged).toEqual({ ok: true, kind: "committed", calls: 2 });
          else expect(staged).toMatchObject({ ok: false, code: "55000" });
          const S = await conn("mx-S", iso);
          let supp: any;
          try { await S.c.query("SET LOCAL ROLE bulls96_session_ingest"); supp = await S.c.query(sp.sql, [sp.payload]).then(() => ({ ok: true }), (e: any) => ({ ok: false, code: String(e.code), message: String(e.message) })); }
          finally { await S.end(); }
          expect(supp).toMatchObject({ ok: false, code: "40001", message: expect.stringContaining("lazy records require read committed") });
          // The same suppressed claim at READ COMMITTED: commits a suppressed receipt in this state (no gate read).
          const C = await conn("mx-C");
          try {
            await C.c.query("SET LOCAL ROLE bulls96_session_ingest");
            await C.c.query(sp.sql, [sp.payload]);
            await C.c.query("RESET ROLE");
            const gl = Number((await fx.query("SELECT count(*)::int n FROM pg_locks WHERE pid=$1 AND relation='sessions.lazy_ack_t2_mode'::regclass", [C.pid])).rows[0].n);
            await C.c.query("COMMIT");
            expect(gl).toBe(0);
          } finally { await C.end(); }
          expect(Number((await fx.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id=$1 AND buffer_receipt_kind='suppressed' AND state='committed'", [sp.batchId])).rows[0].n)).toBe(1);
          rows.push({ iso, state: st.join("/"), staged: staged.ok ? "committed" : staged.code, suppressedClaim: supp.code, suppressedAtRc: "committed" });
        } finally { await reset(); }
      }
      console.log(JSON.stringify({ case: "MATRIX", rows }));
    }, 120000);
    test(`RACE ${iso}: a busy owner row lock (FOR UPDATE, no change): READ COMMITTED does not wait; ${iso} waits (55P03 under lock_timeout), then commits once it is released`, async () => {
      const pc = await prepared(`busy-rc-${iso[0]}`, `busy-o-${iso[0]}`), pr = await prepared(`busy-rr-${iso[0]}`, `busy-o2-${iso[0]}`);
      const H = await ownerConn("busy-H");
      try {
        await H.c.query("SELECT 1 FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE");
        const C = await conn("busy-C");
        try { const t0 = performance.now(); expect(await commitIn(C.c, pc)).toEqual({ ok: true, kind: "committed", calls: 2 }); expect(performance.now() - t0).toBeLessThan(2000); await C.c.query("COMMIT"); }
        finally { await C.end(); }
        const R = await conn("busy-R", iso);
        try {
          await R.c.query("SET LOCAL lock_timeout='500ms'");
          const r = await commitIn(R.c, pr);
          expect(r).toMatchObject({ ok: false, code: "55P03" });
          raceLog.push({ iso, busyOwnerRowLock: { rc: "committed", other: (r as any).code } });
        } finally { await R.end(); }
        const R2 = await conn("busy-R2", iso);
        try {
          const rr = commitIn(R2.c, pr);
          await lockWaiting(R2.pid);
          expect((await fx.query("SELECT pg_blocking_pids($1) b", [R2.pid])).rows[0].b).toEqual([H.pid]);
          await H.c.query("ROLLBACK");
          expect(await rr).toEqual({ ok: true, kind: "committed", calls: 2 });
          await R2.c.query("COMMIT");
        } finally { await R2.end(); }
      } finally { await H.end(); }
      // An owner UPDATE without the barrier is refused outright (keep trigger), whatever else is open.
      await expect(pool.query("UPDATE sessions.lazy_ack_t2_mode SET gate='fenced'")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("changed only by") });
      expect(await gateMode()).toEqual({ gate: "off", mode: "main" });
    }, 60000);
  }
  test("MISSING: the row removed (keep trigger disabled, row deleted, in the same transaction): a staged ACK fails closed 55000 at READ COMMITTED, REPEATABLE READ and SERIALIZABLE; nothing durable; never main", async () => {
    const out: Record<string, string> = {};
    for (const iso of ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]) {
      const p = await prepared(`miss-${iso[0]}${iso[1]}`, `miss-o-${iso[0]}${iso[1]}`);
      const M = await conn("missing", iso);
      try {
        await M.c.query("ALTER TABLE sessions.lazy_ack_t2_mode DISABLE TRIGGER lazy_ack_t2_mode_keep_row");
        expect((await M.c.query("DELETE FROM sessions.lazy_ack_t2_mode")).rowCount).toBe(1);
        const r = await commitIn(M.c, p);
        expect(r).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("the T2 gate row is missing") });
        out[iso] = (r as any).code;
      } finally { await M.end(); }
      expect(await durable(p)).toBe(0);
    }
    expect(await gateMode()).toEqual({ gate: "off", mode: "main" });
    console.log(JSON.stringify({ case: "MISSING", server: (await fx.query("SHOW server_version")).rows[0].server_version, out }));
  }, 60000);
  test("RACE READ COMMITTED: an earlier statement in the transaction does not pin the gate: the ACK reads the row after 50000 and runs the new body", async () => {
    const p = await prepared("race-rc", "race-o-rc");
    const R = await conn("race-rc-R");
    try {
      await R.c.query("SELECT gate FROM sessions.lazy_ack_t2_mode");
      await toState("fenced", "t2");
      expect(await commitIn(R.c, p)).toEqual({ ok: true, kind: "committed", calls: 1 });
      await R.c.query("COMMIT");
    } finally { await R.end(); await reset(); }
    console.log(JSON.stringify({ case: "RACE", server: (await fx.query("SHOW server_version")).rows[0].server_version, results: raceLog }));
  }, 60000);
  /** The supported gate change and the flip overlap: one is open (holding 50000 exclusive), the other is tried. */
  const call = async (sql: string) => { try { return { ok: true as const, v: (await pool.query(sql)).rows[0].o as string }; } catch (e: any) { return { ok: false as const, code: String(e.code), message: String(e.message) }; } };
  test("GATE-A: fenced->off tried while main->t2 is open: busy (fail-fast); after t2 commits, off is refused; never off/t2", async () => {
    expect((await call("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).ok).toBe(true);
    try {
      const F = await ownerConn("gate-a-flip");
      try {
        expect((await F.c.query("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main"); // open, holding 50000 X
        const t0 = performance.now();
        expect(await call("SELECT sessions.lazy_ack_t2_set_gate('off') o")).toMatchObject({ ok: false, code: "55P03", message: expect.stringContaining("gate unchanged") });
        expect(performance.now() - t0).toBeLessThan(500);
        await F.c.query("COMMIT");
      } finally { await F.end(); }
      expect(await gateMode()).toEqual({ gate: "fenced", mode: "t2" });
      expect(await call("SELECT sessions.lazy_ack_t2_set_gate('off') o")).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("only while the mode is main") });
      expect(await gateMode()).toEqual({ gate: "fenced", mode: "t2" });
    } finally { await setMode("main"); expect((await call("SELECT sessions.lazy_ack_t2_set_gate('off') o")).ok).toBe(true); }
  }, 60000);

  test("GATE-B: main->t2 tried while fenced->off is open: busy (fail-fast); after off commits, t2 is refused; never off/t2", async () => {
    expect((await call("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).ok).toBe(true);
    try {
      const G = await ownerConn("gate-b-gate");
      try {
        expect((await G.c.query("SELECT sessions.lazy_ack_t2_set_gate('off') o")).rows[0].o).toBe("fenced"); // open, holding 50000 X
        const t0 = performance.now();
        expect(await call("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).toMatchObject({ ok: false, code: "55P03", message: expect.stringContaining("mode unchanged") });
        expect(performance.now() - t0).toBeLessThan(500);
        await G.c.query("COMMIT");
      } finally { await G.end(); }
      expect(await gateMode()).toEqual({ gate: "off", mode: "main" });
      expect(await call("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("requires the gate fenced first") });
      expect(await gateMode()).toEqual({ gate: "off", mode: "main" });
    } finally { await setMode("main"); }
  }, 60000);

  test("UP: a T2 ACK whose upgrade waits on another origin's T2 ACK reads that ACK's last_seat after it commits; seats contiguous", async () => {
    const pa = await prepared("up-u1", "up-o1"), pb = await prepared("up-u2", "up-o2");
    await setGate("fenced"); await setMode("t2");
    try {
      const last0 = BigInt((await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock")).rows[0].s);
      const A = await conn("up-A"), B = await conn("up-B");
      try {
        expect(await commitIn(B.c, pb)).toEqual({ ok: true, kind: "committed", calls: 1 }); // B holds the clock NO KEY UPDATE to COMMIT
        const ra = commitIn(A.c, pa);
        await lockWaiting(A.pid);
        // A waits at its upgrade (KEY SHARE is compatible with B's NO KEY UPDATE): its plan already ran.
        const w = (await fx.query(`SELECT l.locktype,l.mode FROM pg_locks l WHERE l.pid=$1 AND NOT l.granted`, [A.pid])).rows;
        expect(w.map((x) => x.locktype)).toEqual(["transactionid"]);
        await B.c.query("COMMIT");
        expect(await ra).toEqual({ ok: true, kind: "committed", calls: 1 });
        await A.c.query("COMMIT");
        const last = BigInt((await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock")).rows[0].s);
        expect(last).toBe(last0 + 4n);
        const seats = (await pool.query("SELECT seat::text s FROM sessions.ingest_receipt_seats WHERE seat>$1::bigint ORDER BY seat", [String(last0)])).rows.map((r) => BigInt(r.s));
        expect(seats).toEqual([last0 + 1n, last0 + 2n, last0 + 3n, last0 + 4n]);
      } finally { await A.end(); await B.end(); }
    } finally { await reset(); }
  }, 60000);
});
