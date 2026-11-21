/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, a loopback fixture; skipped with no connection attempt when the
 * variable is unset): multi-connection schedules for 20261105062210_lazy_ack_t2 (proconfig bulls96.lazy_ack_t2, mode row
 * sessions.lazy_ack_t2_mode, try-lock-only flip sessions.lazy_ack_t2_set_mode). Serial parity is the PGlite suite's
 * claim. Every commit goes through the production entry point ingest_origin_commit_buffered_batch (062010 origin-claim
 * fence on), as the runtime role, at READ COMMITTED unless stated. Every flip attempt is ONE autocommit statement (the
 * operator path, lazy-ack-t2-flip.ts). Which body ran is read in the committing transaction: with the history switch off,
 * main fires the slot statement trigger once per accept (2 here), T2 once per round (1).
 *   S1   a fenced main ACK paused at the clock (holding 50000 shared): a flip attempt is refused at once, the mode is
 *        unchanged and NO exclusive request is queued, so another user's ACK on the same origin still passes 50000; after
 *        both commit the flip succeeds and the next ACK reads 't2' and runs T2.
 *   PRE  a genuinely pre-ALTER invocation (gate off when it started) makes the first flip after the ALTER busy.
 *   S2   one transaction with two ACKs: a flip attempt between them is busy; the second ACK keeps 'main'.
 *   RR   a REPEATABLE READ snapshot pinned before a flip is refused after it (55000), both directions.
 *   WIN  a successful flip holds 50000 exclusive until its COMMIT: a writer arriving in that window waits for exactly
 *        that commit and then runs the new body; acquisition through COMMIT is measured.
 *   ROW  the mode row busy: the attempt takes 50000, fails NOWAIT on the row, and its transaction ends before the reply;
 *        a writer then takes 50000 shared at once; the operator loop returns no_switch.
 *   ERA  an erase-shaped exclusive 50000 holder: attempts are refused at once; the budget ends in no_switch.
 *   BUD  a held ingest transaction longer than the budget: no_switch, mode unchanged; then switched after it ends.
 *   CYC  set_live(true)'s lock shape (clock FOR UPDATE, then a session_directory write that needs 50000 shared) while
 *        flips are attempted and an ACK holding 50000 shared waits on the clock: with no queued exclusive the write is
 *        granted at once, even with lock_timeout below deadlock_timeout.
 *   UP   T2 ACK A plans under KEY SHARE, then its upgrade waits on T2 ACK B (another origin, NO KEY UPDATE to commit);
 *        B commits; A's upgrade reads B's last_seat (fresh); seats contiguous and unique; no 40P01.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { FLIP_STATEMENT, lazyAckT2FlipOutcome as flipLazyAckT2Mode, type FlipMode, type FlipOptions } from "../lazy-ack-t2-flip.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const CALLS = "SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_history_native_changed'),0)::int n";

describe.skipIf(!url)("062210 T2 staged ACK: fence and upgrade schedules (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string, generation: string, target: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const ledgerFor = (intercept?: (sql: string, p: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await pool.connect();
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
  const setGate = (v: "off" | "fenced") => pool.query(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_t2='${v}'`);
  const mode = async () => (await pool.query("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows[0].mode as string;
  const setMode = async (m: FlipMode) => { expect((await flipLazyAckT2Mode((sql, p) => pool.query(sql, p as never[]) as never, m, { budgetMs: 5000 })).result).toBe("switched"); };
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  /** Wait until backend pid is blocked on a heavyweight lock. */
  async function lockWaiting(pid: number, ms = 10000) {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if ((await pool.query("SELECT wait_event_type='Lock' w FROM pg_stat_activity WHERE pid=$1", [pid])).rows[0]?.w) return;
      await sleep(15);
    }
    throw Error(`pid ${pid} never waited on a lock`);
  }
  /** A dedicated connection with an open transaction. */
  async function conn(app: string, iso = "READ COMMITTED") {
    const c = new pg.Client({ connectionString: target, application_name: app }); await c.connect();
    const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
    // track_functions per transaction (the owner connection is a superuser): no dependence on the server's setting.
    await c.query(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='30s';SET LOCAL track_functions='pl'`);
    return { c, pid, end: async () => { await c.query("ROLLBACK").catch(() => {}); await c.end(); } };
  }
  /** One commit as the runtime role in c's open transaction; returns its result kind and the trigger calls it made. */
  async function commitIn(c: pg.Client, payload: string) {
    const before = (await c.query(CALLS)).rows[0].n as number;
    await c.query("SET LOCAL ROLE bulls96_session_ingest");
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
  const queuedAdvisory = async () => Number((await pool.query("SELECT count(*) n FROM pg_locks WHERE locktype='advisory' AND NOT granted")).rows[0].n);
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

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    name = "t2_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const u = new URL(url!); u.pathname = "/" + name; target = u.href;
    pool = new pg.Pool({ connectionString: target, max: 12 });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); }
    generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await pool.query("UPDATE sessions.ingest_history_switch SET live=false WHERE singleton");
  }, 300000);
  afterAll(async () => {
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 60000);
  const reset = async () => { await setGate("off"); await setMode("main"); };

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
        expect((await pool.query("SELECT locktype FROM pg_locks WHERE pid=$1 AND NOT granted", [B.pid])).rows.map((x) => x.locktype)).not.toContain("advisory");
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

  test("PRE: an invocation that started with the gate off makes the first flip after the ALTER busy", async () => {
    const pa = await prepared("pre-u1", "pre-o1");
    const blocker = await clockBlocker();
    const A = await conn("pre-A");
    try {
      const ra = commitIn(A.c, pa);
      await lockWaiting(A.pid); // gate off: no mode read; main's ACK already took 50000 shared in ingest_lock_fences
      await setGate("fenced");
      await busyFlip("t2");
      await blocker.release();
      expect(await ra).toEqual({ ok: true, kind: "committed", calls: 2 });
      await busyFlip("t2");
      await A.c.query("COMMIT");
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
      const F = await conn("win-F"), B = await conn("win-B");
      try {
        expect((await F.c.query("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main");
        const rb = commitIn(B.c, pb);
        await lockWaiting(B.pid);
        expect((await pool.query("SELECT locktype FROM pg_locks WHERE pid=$1 AND NOT granted", [B.pid])).rows.map((x) => x.locktype)).toEqual(["advisory"]);
        await sleep(100);
        const t0 = performance.now(); await F.c.query("COMMIT");
        expect(await rb).toEqual({ ok: true, kind: "committed", calls: 1 }); // reads the committed 't2'
        console.log(JSON.stringify({ case: "WIN", writerReleasedAfterCommitMs: Math.round((performance.now() - t0) * 10) / 10 }));
        await B.c.query("COMMIT");
      } finally { await F.end(); await B.end(); }
    } finally { await reset(); }
  }, 60000);

  test("ROW: mode row busy: the attempt takes 50000, fails NOWAIT on the row, ends its transaction; a writer then takes 50000 at once", async () => {
    const pb = await prepared("row-u1", "row-o1");
    await setGate("fenced");
    try {
      const H = await conn("row-holder"); // owner only: the runtime role cannot lock this row (PGlite gate test)
      const B = await conn("row-B");
      try {
        await H.c.query("SELECT 1 FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE");
        const r = await tryFlip("t2");
        expect(r).toMatchObject({ ok: false, code: "55P03", message: expect.stringContaining('could not obtain lock on row in relation "lazy_ack_t2_mode"') });
        expect(r.ms).toBeLessThan(500);
        expect((await pool.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND objid=50000 AND classid=961996")).rows[0].n).toBe(0);
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
        const dt = Number.parseInt((await pool.query("SHOW deadlock_timeout")).rows[0].deadlock_timeout);
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

  for (const [from, to] of [["main", "t2"], ["t2", "main"]] as const)
    test(`RR: a REPEATABLE READ snapshot pinned before a ${from}->${to} flip is refused after it, before any mode read`, async () => {
      const p = await prepared(`rr-${from}`, `rr-o-${from}`);
      await setGate("fenced"); await setMode(from);
      try {
        const R = await conn("rr-R", "REPEATABLE READ");
        try {
          expect((await R.c.query("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows[0].mode).toBe(from); // snapshot pinned
          expect(await tryFlip(to)).toMatchObject({ ok: true, old: from });
          const r = await commitIn(R.c, p);
          expect(r).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("the fenced staged ACK requires read committed") });
        } finally { await R.end(); }
        // The same batch then commits at READ COMMITTED with the new mode.
        const C = await conn("rr-C");
        try { expect(await commitIn(C.c, p)).toEqual({ ok: true, kind: "committed", calls: to === "t2" ? 1 : 2 }); await C.c.query("COMMIT"); } finally { await C.end(); }
      } finally { await reset(); }
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
        const w = (await pool.query(`SELECT l.locktype,l.mode FROM pg_locks l WHERE l.pid=$1 AND NOT l.granted`, [A.pid])).rows;
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
