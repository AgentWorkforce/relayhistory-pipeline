/**
 * Opt-in loopback PostgreSQL (SESSION_INGEST_TEST_PG_URL, the fixture superuser on a disposable cluster; skipped with no
 * connection attempt when the variable is unset): the 062210 lifecycle and rollback
 * (supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql) under a NON-superuser database owner (owner-role-harness.ts),
 * with no privilege, role, grant or parameter ACL added for it. Each case gets its own database (the down removes 062210).
 *
 * The pause is test-only, in that database: an AFTER STATEMENT trigger on sessions.ingest_fence_owners that blocks on a
 * test advisory key, only for the test's application_name and only at trigger depth >= 2, i.e. inside the ACK's own
 * ingest_lock_fences (the ACK runs in the seat-ACK trigger on ingest_batches), never in the outer commit's. So the ACK is
 * paused after its own fence call (holding (961996,50000) shared, as the outer commit already does) and BEFORE its own
 * gate-row read: it holds no lock on sessions.lazy_ack_t2_mode.
 *   LIFE    fresh apply by the owner, default off/main, both setters' allowed and refused cases, the runtime role's
 *           normal ACK path and its denials, the down's refusals and success, and a reapply; receipts per step.
 *   DEP     the down WITHOUT its quiescence step still cannot drop the table under that ACK: its marker UPDATE waits on
 *           the marker row the ACK's ingest_lazy_guard holds FOR SHARE (status 'pass', the normal state), until the ACK
 *           ends. Later share readers are granted beside it (no queued ingest pause); the down just waits, and can
 *           starve. The shipped down fails fast instead and does not rely on the marker being 'pass'.
 *   FIX     the shipped down fails fast (55P03, never queued) while the ACK is open: schema, row, ledger and marker
 *           intact; the released ACK commits on main's body; the down then succeeds and restores 043800 exactly.
 *   TABLE   a holder of the gate table's AccessShareLock that holds no 50000 (a direct reader): the down passes its
 *           barrier, then fails fast on LOCK TABLE ... NOWAIT (55P03) instead of queueing an ACCESS EXCLUSIVE request
 *           while holding 50000 exclusive; nothing changed.
 *   MARKER  a reader holding the validation marker FOR SHARE (before its own 50000, as the publisher does): the down
 *           fails fast instead of holding 50000 exclusive while it waits on that reader.
 *   Not exercised: an ACK admitted while the marker is not 'pass' (044030 apply-window admission, off by default). Its
 *   guard takes no marker lock, so DEP's dependency does not hold for it; the shipped down's protection there is the
 *   source argument that it too holds 50000 S from its ingest_lock_fences call.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { migrateAsOwner, ownedLoopbackUrl, ownerDatabase, roleReceipt, OWNER_ROLE } from "./owner-role-harness.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// pg-proof sets SESSION_INGEST_TEST_PG_REQUIRED=1: there an unset URL is a failure, never a skip.
if (process.env.SESSION_INGEST_TEST_PG_REQUIRED === "1" && !url) throw Error("SESSION_INGEST_TEST_PG_URL required (SESSION_INGEST_TEST_PG_REQUIRED=1)");
ownedLoopbackUrl(url);
const epoch = "3".repeat(64), identity = "c".repeat(64);
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const DOWN = readFileSync("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql", "utf8");
const MAIN_BODY = (() => {
  const m = readFileSync("supabase/migrations/20261105043800_lazy_ack_clock_no_key_update.sql", "utf8")
    .match(/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_ack_batch\(p jsonb\)[\s\S]*?AS \$lazy_ack_batch\$([\s\S]*?)\$lazy_ack_batch\$;/);
  return m![1]!;
})();
const PAUSE_APP = "t2-paused-ack", PAUSE_KEY = 424242;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe.skipIf(!url)("062210 lifecycle and rollback under a non-superuser owner (loopback PostgreSQL)", () => {
  const cleanups: Array<() => Promise<void>> = [];
  // One scratch database at a time: the migrator's role check refuses while another database grants to the runtime roles.
  afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c(); }, 60000);

  /** A fresh scratch database with every migration, the policy, the seat clock and the test-only pause trigger. */
  async function scratchDb() {
    const db = await ownerDatabase(url!, "t2rb");
    const target = db.fixtureUrl;
    const pool = new pg.Pool({ connectionString: db.ownerUrl, max: 10 }); // the owner login
    const fx = new pg.Pool({ connectionString: db.fixtureUrl, max: 10 }); // fixture superuser: observation, SET ROLE
    const scratch = scratchStore();
    pool.on("error", () => {}); fx.on("error", () => {}); // idle clients are terminated by the forced drop below
    cleanups.push(async () => {
      const left = (await fx.query("SELECT application_name a,state FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND application_name<>''")).rows;
      if (left.length) console.log("LINGERING", JSON.stringify(left));
      await pool.end(); await fx.end(); scratch.done(); await db.drop();
    });
    const migrate = async (o: { target?: string } = {}) => migrateAsOwner(db.ownerUrl, o);
    const log: unknown[] = [];
    await migrate();
    const generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await fx.query(`CREATE FUNCTION public.t2_test_pause() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $p$
      BEGIN IF current_setting('application_name')='${PAUSE_APP}' AND pg_trigger_depth()>=2 THEN
        PERFORM pg_advisory_xact_lock(${PAUSE_KEY}); END IF; RETURN NULL; END $p$;
      CREATE TRIGGER t2_test_pause AFTER INSERT ON sessions.ingest_fence_owners FOR EACH STATEMENT EXECUTE FUNCTION public.t2_test_pause();`);
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
    let serial = 0;
    /** Claim and stage committed (after one warm delivery); the commit payload is captured, not run. */
    async function prepared(user: string, origin: string) {
      const t = { ...TENANT, userId: user };
      const mk = async (id: string, ids: string[]) => batch(ids.map((rid) => record({ record_id: rid, revision: 2, session_id: "s-" + rid, origin_id: origin })),
        { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
      const warm = await mk(`rb-warm-${++serial}`, [`rb-warm-r${serial}`]);
      expect((await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, warm) } as never, t, warm)).outcome).toBe("accepted_buffered");
      const s = ++serial;
      const raw = await mk(`rb-b-${s}`, [`rb-r${s}-0`, `rb-r${s}-1`]);
      let payload: string | undefined;
      const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
      await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
      if (typeof payload !== "string") throw Error("payload_not_captured");
      return payload;
    }
    const conn = async (app: string, as: "fixture" | "owner" = "fixture") => {
      const c = new pg.Client({ connectionString: as === "owner" ? db.ownerUrl : target, application_name: app }); c.on("error", () => {}); await c.connect();
      const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
      return { c, pid, end: async () => { await c.query("ROLLBACK").catch(() => {}); await c.end(); } };
    };
    const lockWaiting = async (pid: number) => {
      for (const end = Date.now() + 10000; Date.now() < end; await sleep(15))
        if ((await fx.query("SELECT wait_event_type='Lock' w FROM pg_stat_activity WHERE pid=$1", [pid])).rows[0]?.w) return;
      throw Error(`pid ${pid} never waited on a lock`);
    };
    /** Run the down (or a variant) on its own OWNER connection; returns null or the error, rolled back. */
    const downReceipts: unknown[] = [];
    const down = async (sql: string) => {
      const d = await conn("t2-down", "owner");
      const who = await roleReceipt(d.c);
      expect(who).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false, rolbypassrls: false });
      downReceipts.push(who.current_user);
      try { await d.c.query(sql); return null; }
      catch (e: any) { await d.c.query("ROLLBACK").catch(() => {}); return { code: String(e.code), message: String(e.message) }; }
      finally { await d.c.end(); }
    };
    const state = async () => ({
      table: (await fx.query("SELECT to_regclass('sessions.lazy_ack_t2_mode')::text r")).rows[0].r as string | null,
      row: (await fx.query("SELECT to_regclass('sessions.lazy_ack_t2_mode') IS NOT NULL e")).rows[0].e
        ? (await fx.query("SELECT jsonb_agg(to_jsonb(m)) r FROM sessions.lazy_ack_t2_mode m")).rows[0].r as unknown : null,
      functions: (await fx.query("SELECT array_agg(proname::text ORDER BY proname) f FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname LIKE 'lazy\\_ack\\_t2\\_%'")).rows[0].f as string[] | null,
      ledger: (await fx.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062210_lazy_ack_t2.sql'")).rows[0].n as number,
      marker: (await fx.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0].status as string,
      t2Body: (await fx.query("SELECT prosrc LIKE '%lazy_ack_t2_mode%' b FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].b as boolean,
    });
    /** An ACK paused after its own ingest_lock_fences, before its own gate-row read (row off/main). */
    async function pausedAck() {
      const payload = await prepared("rb-u1", "rb-o1");
      const T = await conn("t2-pause-holder"); await T.c.query(`BEGIN;SELECT pg_advisory_xact_lock(${PAUSE_KEY})`);
      const A = await conn(PAUSE_APP);
      await A.c.query("BEGIN;SET LOCAL lock_timeout='30s';SET LOCAL statement_timeout='60s';SET LOCAL ROLE bulls96_session_ingest");
      expect((await roleReceipt(A.c))).toMatchObject({ current_user: "bulls96_session_ingest", rolsuper: false, rolbypassrls: false });
      const ack = A.c.query(COMMIT, [payload]).then((r) => ({ ok: true as const, kind: r.rows[0].result.kind as string }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
      await lockWaiting(A.pid);
      const locks = (await fx.query(`SELECT locktype,classid,objid,relation::regclass::text rel,mode,granted FROM pg_locks WHERE pid=$1`, [A.pid])).rows;
      // Paused where intended: 50000 shared held, the test key awaited, and no lock of any kind on the gate table yet.
      expect(locks.some((l) => l.locktype === "advisory" && Number(l.classid) === 961996 && Number(l.objid) === 50000 && l.mode === "ShareLock" && l.granted)).toBe(true);
      expect(locks.filter((l) => !l.granted).map((l) => [l.locktype, Number(l.objid)])).toEqual([["advisory", PAUSE_KEY]]);
      expect(locks.some((l) => l.rel === "sessions.lazy_ack_t2_mode")).toBe(false);
      log.push({ pause: "a", granted: locks.filter((l) => l.granted).map((l) => `${l.locktype}:${l.rel ?? l.objid}:${l.mode}`), waiting: locks.filter((l) => !l.granted).map((l) => `${l.locktype}:${l.objid}`) });
      expect((await fx.query("SELECT gate,mode FROM sessions.lazy_ack_t2_mode")).rows[0]).toEqual({ gate: "off", mode: "main" });
      let finished = false;
      return { ack, pid: A.pid, release: async () => { await T.c.query("COMMIT"); await T.end(); },
        finish: async () => { if (finished) return; finished = true; await A.c.query("COMMIT").catch(() => {}); await A.end(); } };
    }
    const asRuntime = async (sql: string, params: unknown[] = []) => {
      const c = await fx.connect();
      try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); await c.query(sql, params as never[]); return null; }
      catch (e: any) { return String(e.code); } finally { await c.query("ROLLBACK").catch(() => {}); c.release(); }
    };
    const deliverOne = async (user: string, origin: string) => {
      const t = { ...TENANT, userId: user };
      const raw = await batch([record({ record_id: `rb-one-${++serial}`, revision: 2, session_id: "s-one", origin_id: origin })], { batch_id: `rb-one-b${serial}`, account_id: await deliveryAccount(t), origin_id: origin });
      return (await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw)).outcome;
    };
    return { pool, fx, down, downReceipts, state, pausedAck, conn, lockWaiting, migrate, asRuntime, deliverOne, preparedPayload: prepared, log };
  }

  test("DEP: without its quiescence step the down still cannot drop the table under the ACK: it waits on the marker row the ACK's guard holds FOR SHARE (no queued ingest pause)", async () => {
    const d = await scratchDb();
    const unguarded = DOWN.replace(/-- Quiescence, fail fast:[\s\S]*?END \$t2_quiesce\$;\n/, "");
    expect(unguarded).not.toContain("t2_quiesce");
    const p = await d.pausedAck();
    const D = await d.conn("t2-down-unguarded", "owner"), R = await d.conn("t2-later-reader");
    try {
      const downDone = D.c.query(unguarded).then(() => null, async (e: any) => { await D.c.query("ROLLBACK").catch(() => {}); return String(e.code); });
      await d.lockWaiting(D.pid);
      // The concrete dependency: the down's marker UPDATE waits on the paused ACK (ingest_lazy_guard: marker FOR SHARE).
      expect((await d.fx.query("SELECT pg_blocking_pids($1) b", [D.pid])).rows[0].b).toEqual([p.pid]);
      expect((await d.state()).table).toBe("sessions.lazy_ack_t2_mode");
      // No queued ingest pause from that wait: a later marker reader (every ingest guard takes it FOR SHARE) is granted at
      // once, beside the ACK's share lock, while the down keeps waiting. The old down simply waits (and can starve).
      await R.c.query("BEGIN");
      const t0 = performance.now();
      expect((await R.c.query("SELECT 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' FOR SHARE")).rowCount).toBe(1);
      expect(performance.now() - t0).toBeLessThan(1000);
      await R.c.query("COMMIT");
      await p.release();
      expect(await p.ack).toEqual({ ok: true, kind: "committed" }); // read 'main' from the still-present table
      await p.finish();
      expect(await downDone).toBeNull(); // only now, after the ACK ended
      expect((await d.state()).table).toBeNull();
    } finally { await p.finish(); await D.end(); await R.end(); }
  }, 180000);

  test("FIX: the shipped down fails fast while that ACK is open (schema intact), the ACK commits on main, then the down succeeds", async () => {
    const d = await scratchDb();
    const before = await d.state();
    expect(before).toEqual({ table: "sessions.lazy_ack_t2_mode", row: [{ singleton: true, gate: "off", mode: "main" }],
      functions: ["lazy_ack_t2_mode_keep", "lazy_ack_t2_set_gate", "lazy_ack_t2_set_mode"], ledger: 1, marker: "pass", t2Body: true });
    const p = await d.pausedAck();
    try {
      const t0 = performance.now();
      expect(await d.down(DOWN)).toMatchObject({ code: "55P03", message: expect.stringContaining("ingest or erase transactions are open") });
      expect(performance.now() - t0).toBeLessThan(2000); // refused at once, nothing queued
      expect((await d.fx.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND objid=50000 AND NOT granted")).rows[0].n).toBe(0);
      expect(await d.state()).toEqual(before);
      await p.release();
      expect(await p.ack).toEqual({ ok: true, kind: "committed" });
    } finally { await p.finish(); }
    expect(await d.down(DOWN)).toBeNull();
    expect(await d.state()).toMatchObject({ table: null, row: null, functions: null, ledger: 0, t2Body: false });
    const proc = (await d.fx.query("SELECT proconfig,prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
    expect(proc.proconfig).toEqual(["search_path=pg_catalog, pg_temp"]);
    expect(proc.prosrc).toBe(MAIN_BODY);
  }, 180000);

  test("MARKER: a reader holding the validation marker FOR SHARE makes the down fail fast; nothing changed", async () => {
    const d = await scratchDb();
    const before = await d.state();
    const R = await d.conn("t2-marker-reader");
    try {
      await R.c.query("BEGIN;SELECT 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' FOR SHARE");
      const t0 = performance.now();
      expect(await d.down(DOWN)).toMatchObject({ code: "55P03", message: expect.stringContaining("validation marker is in use") });
      expect(performance.now() - t0).toBeLessThan(2000);
      expect(await d.state()).toEqual(before);
    } finally { await R.end(); }
    expect(await d.down(DOWN)).toBeNull();
  }, 180000);

  test("LIFE: owner apply, default off/main, setters allowed and refused, the runtime role's ACK path and denials, down refusals and success, reapply", async () => {
    const d = await scratchDb();
    const log: Record<string, unknown> = {};
    const who = await roleReceipt(d.pool);
    expect(who).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false, rolbypassrls: false, rolcreaterole: true, rolinherit: false });
    log.owner = who;
    // Effective privilege on the old parameter: none (and none needed).
    log.paramSet = (await d.pool.query("SELECT has_parameter_privilege(current_user,'bulls96.lazy_ack_t2','SET') s")).rows[0].s;
    expect(log.paramSet).toBe(false);
    expect((await d.fx.query("SELECT count(*)::int n FROM pg_proc p,unnest(p.proconfig) c WHERE c LIKE 'bulls96.%'")).rows[0].n).toBe(0);
    expect((await d.state()).row).toEqual([{ singleton: true, gate: "off", mode: "main" }]);
    const q = async (sql: string) => d.pool.query(sql).then((r) => ({ ok: true as const, o: r.rows[0]?.o as string }), (e: any) => ({ ok: false as const, code: String(e.code) }));
    const steps: Array<[string, string, unknown]> = [
      ["set_mode('t2') with the gate off", "SELECT sessions.lazy_ack_t2_set_mode('t2') o", { ok: false, code: "55000" }],
      ["set_gate('on')", "SELECT sessions.lazy_ack_t2_set_gate('on') o", { ok: false, code: "22023" }],
      ["set_mode('x')", "SELECT sessions.lazy_ack_t2_set_mode('x') o", { ok: false, code: "22023" }],
      ["set_gate('fenced')", "SELECT sessions.lazy_ack_t2_set_gate('fenced') o", { ok: true, o: "off" }],
      ["set_mode('t2')", "SELECT sessions.lazy_ack_t2_set_mode('t2') o", { ok: true, o: "main" }],
      ["set_gate('off') while t2", "SELECT sessions.lazy_ack_t2_set_gate('off') o", { ok: false, code: "55000" }],
      ["direct UPDATE without the barrier", "UPDATE sessions.lazy_ack_t2_mode SET mode='main' RETURNING 'x' o", { ok: false, code: "55000" }],
      ["DELETE", "DELETE FROM sessions.lazy_ack_t2_mode RETURNING 'x' o", { ok: false, code: "55000" }],
      ["set_mode('main')", "SELECT sessions.lazy_ack_t2_set_mode('main') o", { ok: true, o: "t2" }],
      ["set_gate('off')", "SELECT sessions.lazy_ack_t2_set_gate('off') o", { ok: true, o: "fenced" }],
    ];
    const results: unknown[] = [];
    for (const [name, sql, want] of steps) { const r = await q(sql); results.push({ name, ...r }); expect({ name, r }).toEqual({ name, r: want }); }
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      const c = await d.conn("life-iso", "owner");
      try { await c.c.query(`BEGIN ISOLATION LEVEL ${iso}`); await expect(c.c.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')")).rejects.toMatchObject({ code: "55000" }); results.push({ name: `set_gate at ${iso}`, code: "55000" }); }
      finally { await c.end(); }
    }
    { // busy barrier (an open ingest-shaped holder): 55P03, nothing changed
      const E = await d.conn("life-busy");
      try { await E.c.query("BEGIN;SELECT pg_advisory_xact_lock_shared(961996,50000)"); expect(await q("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).toEqual({ ok: false, code: "55P03" }); results.push({ name: "set_gate busy", code: "55P03" }); }
      finally { await E.end(); }
    }
    log.setters = results;
    // The runtime role: the normal ACK path (main, then T2) commits; direct table access and the setters stay denied.
    expect(await d.deliverOne("life-u1", "life-o1")).toBe("accepted_buffered");
    expect(await d.deliverOne("life-u1", "life-o1")).toBe("accepted_buffered");
    await d.pool.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await d.pool.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
    expect(await d.deliverOne("life-u1", "life-o1")).toBe("accepted_buffered");
    await d.pool.query("SELECT sessions.lazy_ack_t2_set_mode('main')"); await d.pool.query("SELECT sessions.lazy_ack_t2_set_gate('off')");
    const denied: Record<string, string | null> = {};
    for (const sql of ["SELECT sessions.lazy_ack_t2_set_gate('fenced')", "SELECT sessions.lazy_ack_t2_set_mode('main')", "SELECT gate FROM sessions.lazy_ack_t2_mode",
      "UPDATE sessions.lazy_ack_t2_mode SET gate='fenced'", "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR SHARE"]) denied[sql] = await d.asRuntime(sql);
    for (const v of Object.values(denied)) expect(v).toBe("42501");
    log.runtimeDenied = denied;
    // Reachability: the runtime role cannot invoke the ACK or its trigger function directly; it reaches the ACK only
    // through the SECURITY DEFINER commit/claim entry points, which take 50000 shared before the batch row is written.
    const exec = (await d.fx.query(`SELECT has_function_privilege('bulls96_session_ingest','sessions.ingest_lazy_ack_batch(jsonb)','EXECUTE') ack,
      has_function_privilege('bulls96_session_ingest','sessions.ingest_receipt_seat_ack()','EXECUTE') trg,
      has_function_privilege('bulls96_session_ingest','sessions.ingest_origin_commit_buffered_batch(jsonb)','EXECUTE') commit_entry,
      (SELECT prosecdef FROM pg_proc WHERE oid='sessions.ingest_origin_commit_buffered_batch(jsonb)'::regprocedure) commit_secdef`)).rows[0];
    expect(exec).toEqual({ ack: false, trg: false, commit_entry: true, commit_secdef: true });
    expect(await d.asRuntime("SELECT sessions.ingest_lazy_ack_batch('{}'::jsonb)")).toBe("42501");
    log.runtimeReach = exec;
    // The keep trigger and DISABLE TRIGGER as the owner login (not a superuser): a missing row fails the setters closed.
    const O = await d.conn("life-owner-disable", "owner");
    try {
      await O.c.query("BEGIN");
      expect(await roleReceipt(O.c)).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false });
      await O.c.query("ALTER TABLE sessions.lazy_ack_t2_mode DISABLE TRIGGER lazy_ack_t2_mode_keep_row");
      expect((await O.c.query("DELETE FROM sessions.lazy_ack_t2_mode")).rowCount).toBe(1);
      await expect(O.c.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("the gate row is missing") });
      log.ownerDisableTrigger = "ok";
    } finally { await O.end(); }
    expect((await d.state()).row).toEqual([{ singleton: true, gate: "off", mode: "main" }]);
    expect((await d.fx.query("SELECT tgname,tgenabled FROM pg_trigger WHERE tgrelid='sessions.lazy_ack_t2_mode'::regclass AND NOT tgisinternal ORDER BY 1")).rows)
      .toEqual([{ tgname: "lazy_ack_t2_mode_keep_row", tgenabled: "O" }, { tgname: "lazy_ack_t2_mode_no_truncate", tgenabled: "O" }]);
    // Down: refusals leave everything intact; then success; then a reapply by the same owner.
    const intact = await d.state();
    for (const [gate, m, msg] of [["fenced", "t2", "the mode is not main"], ["fenced", "main", "the gate is not off"]] as const) {
      await d.pool.query("SELECT sessions.lazy_ack_t2_set_mode('main')"); await d.pool.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [gate]);
      if (m === "t2") await d.pool.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
      expect(await d.down(DOWN)).toMatchObject({ code: "55000", message: expect.stringContaining(msg) });
      expect(await d.state()).toEqual({ ...intact, row: [{ singleton: true, gate, mode: m }] });
    }
    await d.pool.query("SELECT sessions.lazy_ack_t2_set_mode('main')"); await d.pool.query("SELECT sessions.lazy_ack_t2_set_gate('off')");
    expect(await d.down(DOWN)).toBeNull();
    expect(await d.state()).toMatchObject({ table: null, row: null, functions: null, ledger: 0, t2Body: false });
    expect((await d.fx.query("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].prosrc).toBe(MAIN_BODY);
    expect(await d.down(DOWN)).toMatchObject({ code: "55000" }); // rerun refused
    expect((await d.state()).marker).toBe("pending"); // the down leaves the marker pending until the next migrate verifies
    const re = await d.migrate();
    log.reapply = re;
    expect(await d.state()).toEqual(intact);
    expect(await d.deliverOne("life-u1", "life-o1")).toBe("accepted_buffered");
    log.downUsers = [...new Set(d.downReceipts)];
    log.server = (await d.fx.query("SHOW server_version")).rows[0].server_version;
    console.log(JSON.stringify({ case: "LIFE", ...log }));
  }, 300000);

  test("TABLE: a direct reader holding the gate table (no 50000) makes the down fail fast on LOCK TABLE NOWAIT; nothing changed", async () => {
    const d = await scratchDb();
    const before = await d.state();
    const R = await d.conn("t2-table-reader");
    try {
      await R.c.query("BEGIN;SELECT gate FROM sessions.lazy_ack_t2_mode");
      const t0 = performance.now();
      expect(await d.down(DOWN)).toMatchObject({ code: "55P03", message: expect.stringContaining("the gate table is in use") });
      expect(performance.now() - t0).toBeLessThan(2000);
      expect((await d.fx.query("SELECT count(*)::int n FROM pg_locks WHERE NOT granted")).rows[0].n).toBe(0);
      expect(await d.state()).toEqual(before);
    } finally { await R.end(); }
    expect(await d.down(DOWN)).toBeNull();
  }, 180000);

  test("(b) AFTER-READ: an ACK that has read the gate row and waits at the clock: the down fails fast on the barrier (55P03); with the barrier try-lock removed, on the marker NOWAIT (the ACK's guard holds it FOR SHARE); unchanged; then succeeds", async () => {
    const d = await scratchDb();
    const before = await d.state();
    const payload = await d.preparedPayload("rb-b-u1", "rb-b-o1");
    const X = await d.conn("t2-clock-holder");
    await X.c.query("BEGIN;SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
    const A = await d.conn("t2-after-read");
    try {
      await A.c.query("BEGIN;SET LOCAL lock_timeout='30s';SET LOCAL ROLE bulls96_session_ingest");
      const ack = A.c.query(COMMIT, [payload]).then((r) => ({ ok: true as const, kind: r.rows[0].result.kind as string }), (e: any) => ({ ok: false as const, code: String(e.code) }));
      await d.lockWaiting(A.pid);
      const locks = (await d.fx.query("SELECT locktype,classid,objid,relation::regclass::text rel,mode,granted FROM pg_locks WHERE pid=$1", [A.pid])).rows;
      expect(locks.some((l) => l.locktype === "advisory" && Number(l.objid) === 50000 && l.mode === "ShareLock" && l.granted)).toBe(true);
      expect(locks.some((l) => l.rel === "sessions.lazy_ack_t2_mode" && l.mode === "AccessShareLock" && l.granted)).toBe(true);
      d.log.push({ pause: "b", granted: locks.filter((l) => l.granted).map((l) => `${l.locktype}:${l.rel ?? l.objid}:${l.mode}`), waiting: locks.filter((l) => !l.granted).map((l) => `${l.locktype}:${l.mode}`) });
      expect(await d.down(DOWN)).toMatchObject({ code: "55P03", message: expect.stringContaining("ingest or erase transactions are open") });
      const noBarrier = DOWN.replace(/DO \$t2_quiesce\$ BEGIN IF NOT pg_try_advisory_xact_lock\(961996,50000\) THEN[^\n]*\n[^\n]*END IF;\n/, "DO $t2_quiesce$ BEGIN\n");
      expect(noBarrier).not.toBe(DOWN);
      // Next layer: the marker row the ACK's ingest_lazy_guard holds FOR SHARE (the table NOWAIT is the layer after; see TABLE).
      expect(await d.down(noBarrier)).toMatchObject({ code: "55P03", message: expect.stringContaining("the validation marker is in use") });
      expect(await d.state()).toEqual(before);
      await X.c.query("COMMIT");
      expect(await ack).toEqual({ ok: true, kind: "committed" });
      await A.c.query("COMMIT");
    } finally { await A.end(); await X.end(); }
    expect(await d.down(DOWN)).toBeNull();
    console.log(JSON.stringify({ case: "ROLLBACK-b", log: d.log }));
  }, 180000);

  test("(d) FIRST-ENTRY: a commit whose first fence call queues behind a successful down: no 42P01; a documented retryable refusal, no partial write; after the next migrate the same delivery completes once on the restored main body", async () => {
    const d = await scratchDb();
    const payload = await d.preparedPayload("rb-d-u1", "rb-d-o1");
    const batchId = JSON.parse(payload).batchId as string;
    const D = await d.conn("t2-down-open", "owner");
    let out: any;
    try {
      // The shipped down, held open just before its COMMIT (50000 exclusive, the table dropped, the body restored, uncommitted).
      expect(DOWN.trimEnd().endsWith("COMMIT;")).toBe(true);
      await D.c.query(DOWN.trimEnd().replace(/COMMIT;$/, ""));
      const A = await d.conn("t2-first-entry");
      try {
        await A.c.query("BEGIN;SET LOCAL lock_timeout='30s';SET LOCAL ROLE bulls96_session_ingest");
        const r = A.c.query(COMMIT, [payload]).then((x) => ({ ok: true as const, kind: x.rows[0].result.kind as string }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
        await d.lockWaiting(A.pid);
        const locks = (await d.fx.query("SELECT locktype,classid,objid,relation::regclass::text rel,mode,granted FROM pg_locks WHERE pid=$1", [A.pid])).rows;
        // Before the transaction's first fence call: no 50000 held, no lock on anything the down dropped.
        expect(locks.some((l) => l.locktype === "advisory" && Number(l.objid) === 50000 && l.granted)).toBe(false);
        expect(locks.some((l) => l.rel === "sessions.lazy_ack_t2_mode")).toBe(false);
        const waiting = locks.filter((l) => !l.granted).map((l) => `${l.locktype}:${l.locktype === "advisory" ? l.objid : l.rel ?? ""}:${l.mode}`);
        d.log.push({ pause: "d", granted: locks.filter((l) => l.granted).map((l) => `${l.locktype}:${l.rel ?? l.objid}:${l.mode}`), waiting,
          blockers: (await d.fx.query("SELECT pg_blocking_pids($1) b", [A.pid])).rows[0].b });
        expect((await d.fx.query("SELECT pg_blocking_pids($1) b", [A.pid])).rows[0].b).toEqual([D.pid]);
        await D.c.query("COMMIT");
        out = await r;
        await A.c.query("ROLLBACK").catch(() => {});
      } finally { await A.end(); }
    } finally { await D.end(); }
    d.log.push({ pause: "d", outcome: out });
    // Never an undefined-table error. The wrapper queued on the marker row the down UPDATEd (before its first fence
    // call), and after the down's COMMIT it is refused by the existing marker-pending check (55000 'generation not
    // ready'), which the Worker answers 503 (IngestUnavailable): the client retries the whole delivery.
    expect(out).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("generation not ready") });
    expect(await d.state()).toMatchObject({ table: null, ledger: 0, marker: "pending", t2Body: false });
    expect(Number((await d.fx.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id=$1 AND state='committed'", [batchId])).rows[0].n)).toBe(0);
    // Recovery: the next migrate (it re-verifies the marker; 062210 is pending again, so hold it: target 062200).
    await d.migrate(); // the real recovery path: a migrate re-verifies the marker (and reapplies 062210, gate off/main)
    expect((await d.state()).marker).toBe("pass");
    const R = await d.conn("t2-first-entry-retry");
    try {
      await R.c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
      expect((await R.c.query(COMMIT, [payload])).rows[0].result.kind).toBe("committed");
      await R.c.query("COMMIT");
    } finally { await R.end(); }
    expect(Number((await d.fx.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id=$1 AND state='committed'", [batchId])).rows[0].n)).toBe(1);
    console.log(JSON.stringify({ case: "ROLLBACK-d", log: d.log }));
  }, 180000);
});
