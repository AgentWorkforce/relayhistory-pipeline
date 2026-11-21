/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): multi-connection schedules for
 * 20261105062210_lazy_ack_t2 (proconfig bulls96.lazy_ack_t2, mode row sessions.lazy_ack_t2_mode). Serial parity is the
 * PGlite suite's claim. Every commit goes through the production entry point ingest_origin_commit_buffered_batch
 * (062010 origin-claim fence on), as the runtime role, at READ COMMITTED unless stated. Which body ran is read in the
 * committing transaction itself: with the history switch off, main fires the slot statement trigger once per accept
 * (2 here), T2 once per round (1).
 *   S1   fenced, mode main: ACK A (user 1) paused at the clock after its mode read; a flip to t2 queues behind A's
 *        (961996,50000) shared hold; ACK B (user 2, SAME origin) queues behind the flip. A runs main and commits; the flip
 *        commits only after A; B then reads the committed mode ('t2') in its post-lock statement and runs T2.
 *   PRE  a genuinely pre-ALTER invocation (gate off when it started, paused at the clock); the owner ALTERs the gate to
 *        fenced and queues a flip: the flip waits for the pre-ALTER transaction (main's ACK also calls
 *        ingest_lock_fences), which runs main.
 *   S2   one outer transaction with two ACKs: the first holds 50000 shared, a flip queues, the second ACK in the same
 *        transaction re-enters 50000 (already held, granted), reads the unchanged mode and commits; then the flip.
 *   RR   a REPEATABLE READ snapshot pinned before a flip, used after it: refused 55000 in both directions (main->t2 and
 *        t2->main) before any mode read; also when the row says main.
 *   TO   a flip that times out (lock_timeout) behind a held ACK leaves the mode unchanged, and a fenced ACK queued
 *        behind the flip proceeds with the OLD mode.
 *   UP   T2 ACK A (origin 1) plans under KEY SHARE, then its upgrade waits on T2 ACK B (origin 2, NO KEY UPDATE held to
 *        commit); B commits; A's upgrade reads B's last_seat (fresh), seats are contiguous and unique, no 40P01.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

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
  const setMode = async (m: "main" | "t2") => { await pool.query("SELECT sessions.lazy_ack_t2_set_mode($1)", [m]); };
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
    await c.query(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='30s'`);
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
  /** The owner's flip in its own transaction; resolves with the old mode or the error. */
  async function flip(to: "main" | "t2", lockTimeout = "15s") {
    const f = await conn("t2-flip");
    const done = (async () => {
      try {
        await f.c.query(`SET LOCAL lock_timeout='${lockTimeout}'`);
        const old = (await f.c.query("SELECT sessions.lazy_ack_t2_set_mode($1) o", [to])).rows[0].o as string;
        await f.c.query("COMMIT");
        return { ok: true as const, old, at: Date.now() };
      } catch (e: any) { await f.c.query("ROLLBACK").catch(() => {}); return { ok: false as const, code: String(e.code), at: Date.now() }; }
      finally { await f.c.end(); }
    })();
    return { pid: f.pid, done };
  }
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
    expect((await pool.query("SHOW track_functions")).rows[0].track_functions).not.toBe("none");
  }, 300000);
  afterAll(async () => {
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 60000);
  const reset = async () => { await setGate("off"); await setMode("main"); };

  test("S1: a paused fenced main ACK holds the flip; a same-origin ACK of another user queued behind the flip runs T2", async () => {
    const pa = await prepared("s1-u1", "s1-o"), pb = await prepared("s1-u2", "s1-o");
    await setGate("fenced");
    try {
      const blocker = await clockBlocker();
      const A = await conn("s1-A"), B = await conn("s1-B");
      try {
        const ra = commitIn(A.c, pa);
        await lockWaiting(A.pid); // at the clock: mode already read ('main'), 50000 shared held
        const F = await flip("t2");
        await lockWaiting(F.pid); // queued exclusive behind A
        const rb = commitIn(B.c, pb);
        await lockWaiting(B.pid); // queued for 50000 shared behind the flip (or its origin key behind A)
        expect(await mode()).toBe("main");
        await blocker.release();
        expect(await ra).toEqual({ ok: true, kind: "committed", calls: 2 }); // main's loop
        await sleep(200);
        expect(await mode()).toBe("main"); // A has not committed: the flip still waits
        const aCommitted = Date.now(); await A.c.query("COMMIT");
        const f = await F.done;
        expect(f).toMatchObject({ ok: true, old: "main" });
        expect(f.at).toBeGreaterThanOrEqual(aCommitted);
        expect(await rb).toEqual({ ok: true, kind: "committed", calls: 1 }); // T2: one round, fresh post-lock mode read
        await B.c.query("COMMIT");
      } finally { await A.end(); await B.end(); }
    } finally { await reset(); }
  }, 60000);

  test("PRE: an invocation that started with the gate off is drained by the first flip after the ALTER", async () => {
    const pa = await prepared("pre-u1", "pre-o1");
    const blocker = await clockBlocker();
    const A = await conn("pre-A");
    try {
      const ra = commitIn(A.c, pa);
      await lockWaiting(A.pid); // gate off: no mode read; main's ACK already took 50000 shared in ingest_lock_fences
      await setGate("fenced");
      const F = await flip("t2");
      await lockWaiting(F.pid);
      await blocker.release();
      expect(await ra).toEqual({ ok: true, kind: "committed", calls: 2 });
      await sleep(200);
      expect(await mode()).toBe("main");
      await A.c.query("COMMIT");
      expect(await F.done).toMatchObject({ ok: true, old: "main" });
    } finally { await A.end(); await reset(); }
  }, 60000);

  test("S2: two ACKs in one outer transaction straddling a queued flip: the second re-enters 50000 and keeps the old mode", async () => {
    const p1 = await prepared("s2-u1", "s2-o1"), p2 = await prepared("s2-u1", "s2-o2");
    await setGate("fenced");
    try {
      const A = await conn("s2-A");
      try {
        expect(await commitIn(A.c, p1)).toEqual({ ok: true, kind: "committed", calls: 2 });
        const F = await flip("t2");
        await lockWaiting(F.pid);
        expect(await commitIn(A.c, p2)).toEqual({ ok: true, kind: "committed", calls: 2 }); // still main, no wait, no 40P01
        await A.c.query("COMMIT");
        expect(await F.done).toMatchObject({ ok: true, old: "main" });
      } finally { await A.end(); }
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
          expect(await (await flip(to)).done).toMatchObject({ ok: true, old: from });
          const r = await commitIn(R.c, p);
          expect(r).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("the fenced staged ACK requires read committed") });
        } finally { await R.end(); }
        // The same batch then commits at READ COMMITTED with the new mode.
        const C = await conn("rr-C");
        try { expect(await commitIn(C.c, p)).toEqual({ ok: true, kind: "committed", calls: to === "t2" ? 1 : 2 }); await C.c.query("COMMIT"); } finally { await C.end(); }
      } finally { await reset(); }
    }, 60000);

  test("TO: a flip timing out behind a held ACK leaves the mode unchanged; an ACK queued behind it proceeds with the old mode", async () => {
    const pa = await prepared("to-u1", "to-o1"), pb = await prepared("to-u2", "to-o2");
    await setGate("fenced");
    try {
      const A = await conn("to-A"), B = await conn("to-B");
      try {
        expect(await commitIn(A.c, pa)).toEqual({ ok: true, kind: "committed", calls: 2 }); // holds 50000 shared to COMMIT
        const F = await flip("t2", "1s");
        await lockWaiting(F.pid);
        const rb = commitIn(B.c, pb);
        await lockWaiting(B.pid); // behind the queued exclusive
        expect(await F.done).toMatchObject({ ok: false, code: "55P03" });
        expect(await mode()).toBe("main");
        // B is past the barrier now; in main mode it then waits on A's clock NO KEY UPDATE (main serializes ACKs).
        const waitsOn = async () => (await pool.query("SELECT locktype FROM pg_locks WHERE pid=$1 AND NOT granted", [B.pid])).rows.map((x) => x.locktype as string);
        for (const end = Date.now() + 10000; Date.now() < end && !(await waitsOn()).includes("transactionid");) await sleep(15);
        expect(await waitsOn()).toEqual(["transactionid"]);
        await A.c.query("COMMIT");
        expect(await rb).toEqual({ ok: true, kind: "committed", calls: 2 });
        await B.c.query("COMMIT");
      } finally { await A.end(); await B.end(); }
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
  /**
   * CYC: the shape of ingest_history_set_live(true) (026000: clock FOR UPDATE, later a session_directory UPDATE whose
   * 050010 statement trigger takes 50000 shared). X holds the clock FOR UPDATE; ACK A takes 50000 shared in
   * ingest_lock_fences and waits on the clock (X); an exclusive 50000 request E queues behind A; X's directory write
   * then requests 50000 shared, which conflicts with the QUEUED E (a soft edge X->E). Cycle X->E->A->X. Runs with E =
   * the T2 flip and E = a plain exclusive advisory holder shaped like the operator erase (main today, gate off).
   */
  for (const kind of ["flip", "erase-shaped (gate off: main today)"] as const)
    test(`CYC ${kind}: the queue is rearranged (soft deadlock), no 40P01; everything completes`, async () => {
      const pa = await prepared(`cyc-${kind.length}`, `cyc-o-${kind.length}`);
      if (kind === "flip") await setGate("fenced");
      try {
        const X = await conn("cyc-X"), A = await conn("cyc-A");
        try {
          await X.c.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
          const ra = commitIn(A.c, pa);
          await lockWaiting(A.pid);
          let E: { pid: number; done: Promise<{ ok: boolean; code?: string; at: number }> };
          if (kind === "flip") E = await flip("t2");
          else {
            const e = await conn("cyc-erase");
            E = { pid: e.pid, done: e.c.query("SELECT pg_advisory_xact_lock(961996,50000)").then(async () => { await e.c.query("COMMIT"); await e.c.end(); return { ok: true, at: Date.now() }; },
              async (err: any) => { await e.end(); return { ok: false, code: String(err.code), at: Date.now() }; }) };
          }
          await lockWaiting(E.pid);
          const t0 = Date.now();
          let xr: string;
          try { await X.c.query("UPDATE sessions.session_directory SET state=state WHERE false"); xr = "ok"; } catch (err: any) { xr = String(err.code); }
          const xWait = Date.now() - t0;
          const dt = Number.parseInt((await pool.query("SHOW deadlock_timeout")).rows[0].deadlock_timeout);
          console.log(JSON.stringify({ kind, x: xr, xWaitMs: xWait, deadlockTimeoutMs: dt }));
          expect(xr).toBe("ok");
          expect(xWait).toBeGreaterThanOrEqual(dt * 0.8); // granted by the detector's queue rearrangement, after deadlock_timeout
          await X.c.query("COMMIT");
          expect(await ra).toMatchObject({ ok: true, kind: "committed", calls: 2 });
          await A.c.query("COMMIT");
          expect(await E.done).toMatchObject({ ok: true });
        } finally { await X.end(); await A.end(); }
      } finally { await reset(); }
    }, 60000);
});
