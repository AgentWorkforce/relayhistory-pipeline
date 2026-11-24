/**
 * Opt-in loopback PostgreSQL (SESSION_INGEST_TEST_PG_URL, the fixture superuser on a disposable cluster; skipped with no
 * connection attempt when the variable is unset): 20261105062250 mode 'p' (the pre-planned staged ACK) under a
 * NON-superuser database owner (owner-role-harness.ts), with no privilege, role, grant or parameter ACL added for it.
 * Each case gets its own database.
 *
 * The pause is the 062210 suite's, test-only, in that database: an AFTER STATEMENT trigger on sessions.ingest_fence_owners
 * that blocks on a test advisory key for the test's application_name at trigger depth >= 2, i.e. inside the ACK's own
 * ingest_lock_fences: the ACK holds (961996,50000) shared and nothing on the gate table, and has not run the pre-plan.
 *   LIFE     fresh apply by the owner; off/main installed; a delivery commits off; fenced/p by the owner's setters; a
 *            delivery commits on p using its pre-plan (sessions.ingest_receipt_seat_records not called in the commit);
 *            the setter's refusals; the down's refusals (p, t2, fenced/main) and success (062210's bodies and CHECK
 *            restored exactly, ledger row gone); a reapply restores 062250 exactly. Receipts: role flags, version.
 *   APPLY    with 062250 rolled back: (a) an ACK paused holding 50000 shared -> the owner's migrate fails fast (55P03)
 *            with nothing applied; the released ACK commits; (b) a direct reader holding the gate table AccessShareLock
 *            (no 50000) -> the migrate passes the barrier and fails fast on LOCK TABLE NOWAIT (55P03), nothing applied,
 *            while another delivery commits beside the reader; (c) quiet -> the migrate applies 062250.
 *   DOWN     an ACK paused holding 50000 shared -> the down fails fast (55P03), nothing changed; a gate-table reader
 *            -> 55P03 at the table step, nothing changed; the released ACK commits; the down then succeeds.
 *   SETTER   an ACK paused holding 50000 shared -> set_mode('p') and set_gate fail fast (55P03), row unchanged.
 *   BLOCK    a p ACK waiting for the clock (held NO KEY UPDATE by another transaction) has run its pre-plan and holds
 *            the serialization point of every captured input's writer: the origin-claim key (same-user same-origin
 *            claims), the fence row FOR SHARE (retirement takes it FOR UPDATE: NOWAIT -> 55P03), (961996,50000) shared
 *            (erase: exclusive try-lock false); the clock-identity writer (FOR UPDATE NOWAIT on the clock -> 55P03) waits
 *            for the holder and then for the ACK. The waiting ACK holds exactly one transactionid lock (its own: the
 *            pre-plan's exception block assigned no subtransaction id). Released, it commits on its pre-plan.
 *   RACE     two users delivering concurrently into ONE origin with overlapping, reversed record ids, mode p: no 40P01,
 *            every delivery committed or refused with P0W01 (another user's native key), never a lock-order error.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { laterSlotDowns, migrateAsOwner, ownedLoopbackUrl, ownerDatabase, rollBackLaterSlots, roleReceipt, OWNER_ROLE } from "./owner-role-harness.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { PREPLAN_DOWN, PREPLAN_MIG, PREPLAN_STEP, T2_MIG, ackBodyOf, setModeBodyOf } from "./lazy-ack-preplan-support.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (process.env.SESSION_INGEST_TEST_PG_REQUIRED === "1" && !url) throw Error("SESSION_INGEST_TEST_PG_URL required (SESSION_INGEST_TEST_PG_REQUIRED=1)");
ownedLoopbackUrl(url);
const epoch = "3".repeat(64), identity = "c".repeat(64);
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const DOWN = readFileSync(PREPLAN_DOWN, "utf8");
const SLOT = "20261105062250_lazy_ack_preplan";
const P_BODY = ackBodyOf(readFileSync(PREPLAN_MIG, "utf8")), T2_BODY = ackBodyOf(readFileSync(T2_MIG, "utf8"));
const P_SET = setModeBodyOf(readFileSync(PREPLAN_MIG, "utf8")), T2_SET = setModeBodyOf(readFileSync(T2_MIG, "utf8"));
const CHECK_P = "CHECK ((mode = ANY (ARRAY['main'::text, 't2'::text, 'p'::text])))", CHECK_T2 = "CHECK ((mode = ANY (ARRAY['main'::text, 't2'::text])))";
const PAUSE_APP = "p-paused-ack", PAUSE_KEY = 424243;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const receipts: unknown[] = [];

describe.skipIf(!url)("062250 mode p lifecycle, apply/down/setter under live ACKs, writer blocking, races (non-superuser owner, loopback PostgreSQL)", () => {
  const cleanups: Array<() => Promise<void>> = [];
  afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c(); }, 60000);

  async function scratchDb() {
    const db = await ownerDatabase(url!, "p62250");
    const pool = new pg.Pool({ connectionString: db.ownerUrl, max: 10 });
    const fx = new pg.Pool({ connectionString: db.fixtureUrl, max: 20 });
    const scratch = scratchStore();
    pool.on("error", () => {}); fx.on("error", () => {});
    cleanups.push(async () => { await pool.end(); await fx.end(); scratch.done(); await db.drop(); });
    const migrate = async () => migrateAsOwner(db.ownerUrl);
    await migrate();
    // Slots above 062250 (none today) come off first, newest first, as the owner, so 062250 is newest.
    const later = laterSlotDowns(SLOT).map((s) => s.name);
    if (later.length) expect(await rollBackLaterSlots(db.ownerUrl, SLOT)).toEqual(later);
    const generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await fx.query(`CREATE FUNCTION public.p_test_pause() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $p$
      BEGIN IF current_setting('application_name')='${PAUSE_APP}' AND pg_trigger_depth()>=2 THEN
        PERFORM pg_advisory_xact_lock(${PAUSE_KEY}); END IF; RETURN NULL; END $p$;
      CREATE TRIGGER p_test_pause AFTER INSERT ON sessions.ingest_fence_owners FOR EACH STATEMENT EXECUTE FUNCTION public.p_test_pause();`);
    const ledgerFor = (intercept?: (sql: string, p: unknown[]) => void) => sqlLedger(async (sql, params) => {
      intercept?.(sql, params as unknown[]);
      const c = await fx.connect();
      try { await c.query("BEGIN;SET LOCAL lock_timeout='10s';SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
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
    let serial = 0;
    const mk = async (t: any, origin: string, id: string, ids: string[], revision = 2) => batch(ids.map((rid) => record({ record_id: rid, revision, session_id: "s-" + rid, origin_id: origin })),
      { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
    const deliver = async (user: string, origin: string, ids?: string[], revision = 2) => {
      const t = { ...TENANT, userId: user };
      const raw = await mk(t, origin, `p-b-${++serial}`, ids ?? [`p-r${serial}-0`, `p-r${serial}-1`], revision);
      return (await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw)).outcome;
    };
    /** Claim and stage committed (after one warm delivery of the user); the commit payload is captured, not run. */
    async function prepared(user: string, origin: string) {
      const t = { ...TENANT, userId: user };
      expect(await deliver(user, origin)).toBe("accepted_buffered");
      const raw = await mk(t, origin, `p-c-${++serial}`, [`p-c${serial}-0`, `p-c${serial}-1`]);
      let payload: string | undefined;
      const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
      await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
      if (typeof payload !== "string") throw Error("payload_not_captured");
      return payload;
    }
    const conn = async (app: string, as: "fixture" | "owner" = "fixture") => {
      const c = new pg.Client({ connectionString: as === "owner" ? db.ownerUrl : db.fixtureUrl, application_name: app }); c.on("error", () => {}); await c.connect();
      const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
      return { c, pid, end: async () => { await c.query("ROLLBACK").catch(() => {}); await c.end(); } };
    };
    const lockWaiting = async (pid: number) => {
      for (const end = Date.now() + 10000; Date.now() < end; await sleep(15))
        if ((await fx.query("SELECT wait_event_type='Lock' w FROM pg_stat_activity WHERE pid=$1", [pid])).rows[0]?.w) return;
      throw Error(`pid ${pid} never waited on a lock`);
    };
    /** One statement on its own OWNER login; returns null or the error (rolled back) and the elapsed ms. */
    const asOwner = async (sql: string) => {
      const d = await conn("p-owner", "owner");
      const who = await roleReceipt(d.c);
      expect(who).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false, rolbypassrls: false, rolinherit: false, rolcreaterole: true });
      receipts.push(who);
      const t0 = Date.now();
      try { await d.c.query(sql); return { err: null, ms: Date.now() - t0 }; }
      catch (e: any) { await d.c.query("ROLLBACK").catch(() => {}); return { err: { code: String(e.code), message: String(e.message) }, ms: Date.now() - t0 }; }
      finally { await d.c.end(); }
    };
    const migrateResult = async (): Promise<{ err: { code: string; message: string } | null; ms: number }> => { const t0 = Date.now(); try { await migrate(); return { err: null, ms: Date.now() - t0 }; } catch (e: any) { return { err: { code: String(e.code ?? e.cause?.code), message: String(e.message) }, ms: Date.now() - t0 }; } };
    const state = async () => ({
      ledger: (await fx.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [PREPLAN_STEP])).rows[0].n as number,
      check: (await fx.query("SELECT pg_get_constraintdef(oid) d FROM pg_constraint WHERE conname='lazy_ack_t2_mode_mode_check'")).rows[0].d as string,
      ack: (await fx.query("SELECT prosrc s FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].s === P_BODY ? "p" :
        (await fx.query("SELECT prosrc s FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].s === T2_BODY ? "062210" : "other",
      setter: (await fx.query("SELECT prosrc s FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_mode(text)'::regprocedure")).rows[0].s === P_SET ? "p" :
        (await fx.query("SELECT prosrc s FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_mode(text)'::regprocedure")).rows[0].s === T2_SET ? "062210" : "other",
      row: (await fx.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode")).rows[0] as { gate: string; mode: string },
    });
    /** An ACK paused after its own ingest_lock_fences (50000 shared held), before its gate read and pre-plan. */
    async function pausedAck(user = "p-u1", origin = "p-o1") {
      const payload = await prepared(user, origin);
      const T = await conn("p-pause-holder"); await T.c.query(`BEGIN;SELECT pg_advisory_xact_lock(${PAUSE_KEY})`);
      const A = await conn(PAUSE_APP);
      await A.c.query("BEGIN;SET LOCAL lock_timeout='30s';SET LOCAL statement_timeout='60s';SET LOCAL ROLE bulls96_session_ingest");
      const ack = A.c.query(COMMIT, [payload]).then((r) => ({ ok: true as const, kind: r.rows[0].result.kind as string }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
      await lockWaiting(A.pid);
      const locks = (await fx.query("SELECT locktype,classid,objid,mode,granted FROM pg_locks WHERE pid=$1", [A.pid])).rows;
      expect(locks.some((l) => l.locktype === "advisory" && Number(l.classid) === 961996 && Number(l.objid) === 50000 && l.mode === "ShareLock" && l.granted)).toBe(true);
      return { ack, release: async () => { await T.c.query("COMMIT"); await T.end(); }, finish: async () => { await A.c.query("COMMIT").catch(() => {}); await A.end(); } };
    }
    const setP = async () => { expect((await asOwner("SELECT sessions.lazy_ack_t2_set_gate('fenced')")).err).toBeNull(); expect((await asOwner("SELECT sessions.lazy_ack_t2_set_mode('p')")).err).toBeNull(); };
    const setMain = async () => { expect((await asOwner("SELECT sessions.lazy_ack_t2_set_mode('main')")).err).toBeNull(); expect((await asOwner("SELECT sessions.lazy_ack_t2_set_gate('off')")).err).toBeNull(); };
    /** The commit of a prepared payload in one fixture transaction as the runtime role, with seat-records call counts. */
    const commitCounted = async (payload: string) => {
      const c = await fx.connect();
      try {
        // A delta inside the transaction: on PostgreSQL 15+ the xact view also carries this backend's not yet flushed
        // counts of earlier transactions (pooled connection, server-wide track_functions).
        await c.query("BEGIN;SET LOCAL track_functions='pl'");
        const n0 = Number((await c.query("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_receipt_seat_records'),0) n")).rows[0].n);
        await c.query("SET LOCAL ROLE bulls96_session_ingest");
        const r = (await c.query(COMMIT, [payload])).rows[0].result;
        await c.query("RESET ROLE");
        const n = Number((await c.query("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_receipt_seat_records'),0) n")).rows[0].n) - n0;
        await c.query("COMMIT");
        return { kind: r.kind as string, seatRecordCalls: n };
      } catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
    };
    return { pool, fx, migrate, migrateResult, asOwner, state, pausedAck, setP, setMain, deliver, prepared, conn, lockWaiting, commitCounted, ownerUrl: db.ownerUrl };
  }

  test("LIFE: apply, off/main, commits off and on p (pre-plan used), setter refusals, down refusals and success, reapply", async () => {
    const d = await scratchDb();
    const o = await d.conn("p-who", "owner"); receipts.push({ owner: await roleReceipt(o.c) }); await o.end();
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "off", mode: "main" } });
    expect(await d.deliver("p-u1", "p-o1")).toBe("accepted_buffered");
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_mode('p')")).err).toMatchObject({ code: "55000" }); // gate not fenced
    await d.setP();
    expect(await d.state()).toMatchObject({ row: { gate: "fenced", mode: "p" } });
    expect(await d.deliver("p-u1", "p-o1")).toBe("accepted_buffered");
    const pl = await d.prepared("p-u2", "p-o2");
    expect(await d.commitCounted(pl)).toEqual({ kind: "committed", seatRecordCalls: 0 }); // pre-plan used
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_mode('t2')")).err).toMatchObject({ code: "55000", message: expect.stringContaining("only through main") });
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_gate('off')")).err).toMatchObject({ code: "55000", message: expect.stringContaining("only while the mode is main") });
    // Runtime role: no setter, no table.
    const rt = await d.fx.connect();
    try { await rt.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); await expect(rt.query("SELECT sessions.lazy_ack_t2_set_mode('main')")).rejects.toMatchObject({ code: "42501" }); }
    finally { await rt.query("ROLLBACK"); rt.release(); }
    // Down: refused under p, under t2 (via main), under fenced/main; nothing changed each time.
    expect((await d.asOwner(DOWN)).err).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_mode('main')")).err).toBeNull();
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_mode('t2')")).err).toBeNull();
    expect((await d.asOwner(DOWN)).err).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_mode('main')")).err).toBeNull();
    expect((await d.asOwner(DOWN)).err).toMatchObject({ code: "55000", message: expect.stringContaining("the gate is not off") });
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "fenced", mode: "main" } });
    expect((await d.asOwner("SELECT sessions.lazy_ack_t2_set_gate('off')")).err).toBeNull();
    expect((await d.asOwner(DOWN)).err).toBeNull();
    expect(await d.state()).toEqual({ ledger: 0, check: CHECK_T2, ack: "062210", setter: "062210", row: { gate: "off", mode: "main" } });
    expect((await d.asOwner(DOWN)).err).toMatchObject({ code: "55000", message: expect.stringContaining("own applied checksum required") });
    // The marker is pending after the down (as 062210's): the migrator reapplies 062250 and validates.
    await d.migrate();
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "off", mode: "main" } });
    expect(await d.deliver("p-u1", "p-o1")).toBe("accepted_buffered");
  }, 300000);

  test("APPLY: the 062250 file fails fast (55P03, nothing applied) under a paused ACK and a gate-table reader; the migrator waits at its marker step first, then applies", async () => {
    const d = await scratchDb();
    expect((await d.asOwner(DOWN)).err).toBeNull();
    // Test-only shortcut: after the down the readiness marker is pending (every staged commit refused) until the operator
    // restores it at the target tree's generation (runbook; 062210's rollback-target suite exercises that procedure).
    // Here the fixture marks it pass again so live ACKs exist on the 062210 body while the apply is attempted.
    const pass = async () => d.fx.query("UPDATE sessions.__96bulls_role_validation SET status='pass' WHERE name='session_directory_search_worker'");
    await pass();
    const marker = async () => (await d.fx.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0].status as string;
    const before = { ledger: 0, check: CHECK_T2, ack: "062210", setter: "062210" };
    expect(await d.state()).toMatchObject(before);
    // The file alone, as the owner, in one transaction (what the migrator runs for this step, without its ledger row).
    const file = "BEGIN;" + readFileSync(PREPLAN_MIG, "utf8") + "\nCOMMIT;";
    // (a) an ACK paused holding (961996,50000) shared.
    const a = await d.pausedAck();
    const fa = await d.asOwner(file);
    const faR = JSON.stringify(fa);
    expect(fa.err).toMatchObject({ code: "55P03", message: expect.stringContaining("barrier busy") });
    expect(fa.ms).toBeLessThan(5000);
    expect(await d.state()).toMatchObject(before);
    // The real migrator under the same paused ACK: its first step marks the readiness row pending, which waits on the
    // row the live ACK holds FOR SHARE (ingest_lazy_guard); it never reaches 062250 while the ACK is open.
    const mig = d.migrateResult();
    let waitQuery = "";
    for (const end = Date.now() + 10000; Date.now() < end && !waitQuery; await sleep(25))
      waitQuery = (await d.fx.query("SELECT left(query,120) q FROM pg_stat_activity WHERE usename=$1 AND datname=current_database() AND wait_event_type='Lock'", [OWNER_ROLE])).rows[0]?.q ?? "";
    expect(waitQuery).toContain("INSERT INTO sessions.__96bulls_role_validation");
    expect(await d.state()).toMatchObject(before);
    await a.release();
    expect((await a.ack).ok).toBe(true);
    await a.finish();
    const ma = await mig;
    expect(ma.err).toBeNull();
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "off", mode: "main" } });
    expect(await marker()).toBe("pass");
    // Back to the pre-062250 state for (b).
    expect((await d.asOwner(DOWN)).err).toBeNull(); await pass();
    // (b) a direct gate-table reader (AccessShareLock, no 50000, no marker lock).
    const R = await d.conn("p-table-reader", "owner");
    await R.c.query("BEGIN;SELECT gate FROM sessions.lazy_ack_t2_mode");
    const fb = await d.asOwner(file);
    const fbR = JSON.stringify(fb);
    expect(fb.err).toMatchObject({ code: "55P03", message: expect.stringContaining("the gate table is in use") });
    expect(fb.ms).toBeLessThan(5000);
    expect(await d.state()).toMatchObject(before);
    // Nothing queued behind the refused file: a delivery (its ACK reads the gate table) commits beside the reader.
    expect(await d.deliver("p-u3", "p-o3")).toBe("accepted_buffered");
    // The migrator under the reader: it passes its marker step, then 062250 refuses (55P03); nothing of 062250 is
    // applied and the marker stays pending (its pending write is its own earlier step) until a rerun succeeds.
    const mb = await d.migrateResult();
    const mbR = JSON.stringify(mb);
    expect(mb.err).toMatchObject({ message: expect.stringContaining("the gate table is in use") });
    expect(await d.state()).toMatchObject(before);
    expect(await marker()).toBe("pending");
    await R.end();
    // (c) quiet: the rerun applies 062250 and marks the generation pass.
    const mc = await d.migrateResult();
    receipts.push({ apply: { fileUnderPausedAck: JSON.parse(faR), migratorUnderPausedAck: { waitedAt: waitQuery.slice(0, 60), ms: ma.ms }, fileUnderReader: JSON.parse(fbR), migratorUnderReader: JSON.parse(mbR), quietMs: mc.ms } });
    expect(mc.err).toBeNull();
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "off", mode: "main" } });
    expect(await marker()).toBe("pass");
  }, 300000);

  test("DOWN and SETTER: fail fast (55P03) under a paused ACK and a gate-table reader, nothing changed; succeed when quiet", async () => {
    const d = await scratchDb();
    const a = await d.pausedAck();
    const r1 = await d.asOwner(DOWN), s1 = await d.asOwner("SELECT sessions.lazy_ack_t2_set_gate('fenced')"), s2 = await d.asOwner("SELECT sessions.lazy_ack_t2_set_mode('main')");
    expect(r1.err).toMatchObject({ code: "55P03", message: expect.stringContaining("barrier busy") });
    expect(s1.err).toMatchObject({ code: "55P03" }); expect(s2.err).toMatchObject({ code: "55P03" });
    for (const r of [r1, s1, s2]) expect(r.ms).toBeLessThan(5000);
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "off", mode: "main" } });
    await a.release();
    expect((await a.ack).ok).toBe(true);
    await a.finish();
    const R = await d.conn("p-table-reader", "owner");
    await R.c.query("BEGIN;SELECT gate FROM sessions.lazy_ack_t2_mode");
    const r2 = await d.asOwner(DOWN);
    expect(r2.err).toMatchObject({ code: "55P03", message: expect.stringContaining("the gate table is in use") });
    expect(await d.state()).toEqual({ ledger: 1, check: CHECK_P, ack: "p", setter: "p", row: { gate: "off", mode: "main" } });
    await R.end();
    expect((await d.asOwner(DOWN)).err).toBeNull();
    expect(await d.state()).toMatchObject({ ledger: 0, check: CHECK_T2, ack: "062210", setter: "062210" });
    receipts.push({ down: { pausedAck: r1.ms, setters: [s1.ms, s2.ms], tableReader: r2.ms } });
  }, 300000);

  test("BLOCK: a p ACK waiting for the clock after its pre-plan holds each captured input's writer serialization point; no subtransaction xid; commits on its pre-plan", async () => {
    const d = await scratchDb();
    await d.setP();
    const payload = await d.prepared("p-ub", "p-ob");
    const H = await d.conn("p-clock-holder", "owner");
    await H.c.query("BEGIN;SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE");
    const A = await d.conn("p-blocked-ack");
    await A.c.query("BEGIN;SET LOCAL track_functions='pl';SET LOCAL lock_timeout='30s'");
    const seat0 = Number((await A.c.query("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_receipt_seat_records'),0) n")).rows[0].n);
    await A.c.query("SET LOCAL ROLE bulls96_session_ingest");
    const ack = A.c.query(COMMIT, [payload]).then((r) => ({ ok: true as const, kind: r.rows[0].result.kind as string }), (e: any) => ({ ok: false as const, code: String(e.code) }));
    await d.lockWaiting(A.pid);
    const locks = (await d.fx.query("SELECT locktype,relation::regclass::text rel,classid,objid,objsubid,mode,granted FROM pg_locks WHERE pid=$1", [A.pid])).rows;
    // Waiting on the clock row (tuple or the holder's xid), nothing else.
    const waiting = locks.filter((l) => !l.granted);
    expect(waiting.length).toBe(1);
    expect(["tuple", "transactionid"]).toContain(waiting[0].locktype);
    // Exactly one transactionid lock held: the top-level transaction's (the pre-plan's exception block has no xid).
    expect(locks.filter((l) => l.locktype === "transactionid" && l.granted).length).toBe(1);
    const probe = async (sql: string) => { const c = await d.fx.connect(); try { await c.query("BEGIN;SET LOCAL lock_timeout='300ms'"); const r = await c.query(sql); return { ok: true, v: r.rows[0] }; } catch (e: any) { return { ok: false, code: String(e.code) }; } finally { await c.query("ROLLBACK").catch(() => {}); c.release(); } };
    const originKey = "hashtextextended(jsonb_build_array('96bulls:ingest-origin-claim-v1','org-a','ws-a','p-ub','p-ob')::text,0)";
    const blocks = {
      originClaimKey: await probe(`SELECT pg_try_advisory_xact_lock(${originKey}) got`),
      fenceRowForUpdate: await probe("SELECT 1 FROM sessions.ingest_fence_owners WHERE org_id='org-a' AND workspace_id='ws-a' AND user_id='p-ub' FOR UPDATE NOWAIT"),
      barrierExclusive: await probe("SELECT pg_try_advisory_xact_lock(961996,50000) got"),
      clockForUpdate: await probe("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE NOWAIT"),
    };
    expect(blocks).toEqual({ originClaimKey: { ok: true, v: { got: false } }, fenceRowForUpdate: { ok: false, code: "55P03" },
      barrierExclusive: { ok: true, v: { got: false } }, clockForUpdate: { ok: false, code: "55P03" } });
    receipts.push({ block: { locksHeld: locks.filter((l) => l.granted).length, waiting: waiting[0].locktype, blocks } });
    await H.c.query("COMMIT"); await H.end();
    expect(await ack).toEqual({ ok: true, kind: "committed" });
    await A.c.query("RESET ROLE");
    expect(Number((await A.c.query("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_receipt_seat_records'),0) n")).rows[0].n) - seat0).toBe(0);
    await A.c.query("COMMIT"); await A.end();
  }, 300000);

  test("RACE: two users into one origin, overlapping reversed record ids, mode p: no 40P01; committed or P0W01 only", async () => {
    const d = await scratchDb();
    await d.setP();
    const outcomes: Record<string, number> = {};
    for (let round = 0; round < 6; round++) {
      const ids = Array.from({ length: 20 }, (_, i) => `race-${round}-${i}`);
      const r = await Promise.all([d.deliver("p-ra", "p-shared", ids, 2 + round).then((x) => x, (e: any) => "error:" + String(e?.code ?? e?.cause_code ?? e?.name)),
        d.deliver("p-rb", "p-shared", [...ids].reverse(), 2 + round).then((x) => x, (e: any) => "error:" + String(e?.code ?? e?.cause_code ?? e?.name))]);
      for (const x of r) outcomes[x] = (outcomes[x] ?? 0) + 1;
    }
    receipts.push({ race: outcomes });
    for (const k of Object.keys(outcomes)) expect(k).not.toMatch(/40P01|deadlock/i);
    expect(outcomes["accepted_buffered"] ?? 0).toBeGreaterThan(0);
  }, 300000);

  test("receipts", () => { console.log("062250_RECEIPTS " + JSON.stringify(receipts)); });
});
