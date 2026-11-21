/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, a loopback fixture; skipped with no connection attempt when the
 * variable is unset): the 062210 rollback (supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql) against an ACK that
 * entered with the gate 'fenced' and is paused AFTER its own ingest_lock_fences (so it holds (961996,50000) shared) and
 * BEFORE its unlocked mode-table read (so it holds no lock on sessions.lazy_ack_t2_mode). Mode 'main' and gate 'off'
 * cannot see such an ACK. Each case gets its own scratch database (the down removes 062210).
 *
 * The pause is test-only, in the scratch database: an AFTER STATEMENT trigger on sessions.ingest_fence_owners that
 * blocks on a test advisory key, only for the test's application_name and only while the ACK's function-local gate
 * setting is 'fenced', i.e. inside that ACK's first ingest_lock_fences call and never in the outer commit's.
 *   DEP     the down WITHOUT its quiescence step still cannot drop the table under that ACK: its marker UPDATE waits on
 *           the marker row the ACK's ingest_lazy_guard holds FOR SHARE (status 'pass', the normal state), until the ACK
 *           ends. Later share readers are granted beside it (no queued ingest pause); the down just waits, and can
 *           starve. The shipped down fails fast instead and does not rely on the marker being 'pass'.
 *   FIX     the shipped down fails fast (55P03, never queued) while the ACK is open: schema, ledger and marker intact;
 *           the released ACK commits on main's body; the down then succeeds and restores 043800 exactly.
 *   Not exercised: an ACK admitted while the marker is not 'pass' (044030 apply-window admission, off by default). Its
 *   guard takes no marker lock, so DEP's dependency does not hold for it; the shipped down's protection there is the
 *   source argument that it too holds 50000 S from its ingest_lock_fences call.
 *   MARKER  a reader holding the validation marker FOR SHARE (before its own 50000, as the publisher does): the down
 *           fails fast instead of holding 50000 exclusive while it waits on that reader.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// pg-proof sets SESSION_INGEST_TEST_PG_REQUIRED=1: there an unset URL is a failure, never a skip.
if (process.env.SESSION_INGEST_TEST_PG_REQUIRED === "1" && !url) throw Error("SESSION_INGEST_TEST_PG_URL required (SESSION_INGEST_TEST_PG_REQUIRED=1)");
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
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

describe.skipIf(!url)("062210 rollback vs a paused fenced ACK (owned PostgreSQL)", () => {
  const cleanups: Array<() => Promise<void>> = [];
  // One scratch database at a time: the migrator's role check refuses while another database grants to the runtime roles.
  afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c(); }, 60000);

  /** A fresh scratch database with every migration, the policy, the seat clock and the test-only pause trigger. */
  async function scratchDb() {
    const admin = new pg.Client({ connectionString: url }); await admin.connect();
    const name = "t2rb_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const u = new URL(url!); u.pathname = "/" + name; const target = u.href;
    const pool = new pg.Pool({ connectionString: target, max: 10 });
    const scratch = scratchStore();
    pool.on("error", () => {}); // idle pool clients are terminated by the forced drop below
    cleanups.push(async () => {
      const left = (await pool.query("SELECT application_name a,state FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")).rows;
      if (left.length) console.log("LINGERING", JSON.stringify(left));
      await pool.end(); scratch.done(); await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end();
    });
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    { const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); } }
    const generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await pool.query(`CREATE FUNCTION public.t2_test_pause() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $p$
      BEGIN IF current_setting('application_name')='${PAUSE_APP}' AND current_setting('bulls96.lazy_ack_t2',true)='fenced' THEN
        PERFORM pg_advisory_xact_lock(${PAUSE_KEY}); END IF; RETURN NULL; END $p$;
      CREATE TRIGGER t2_test_pause AFTER INSERT ON sessions.ingest_fence_owners FOR EACH STATEMENT EXECUTE FUNCTION public.t2_test_pause();`);
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
    const conn = async (app: string) => {
      const c = new pg.Client({ connectionString: target, application_name: app }); c.on("error", () => {}); await c.connect();
      const pid = (await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
      return { c, pid, end: async () => { await c.query("ROLLBACK").catch(() => {}); await c.end(); } };
    };
    const lockWaiting = async (pid: number) => {
      for (const end = Date.now() + 10000; Date.now() < end; await sleep(15))
        if ((await pool.query("SELECT wait_event_type='Lock' w FROM pg_stat_activity WHERE pid=$1", [pid])).rows[0]?.w) return;
      throw Error(`pid ${pid} never waited on a lock`);
    };
    /** Run the down (or a variant) on its own owner connection; returns null or the error, rolled back. */
    const down = async (sql: string) => {
      const d = await conn("t2-down");
      try { await d.c.query(sql); return null; }
      catch (e: any) { await d.c.query("ROLLBACK").catch(() => {}); return { code: String(e.code), message: String(e.message) }; }
      finally { await d.c.end(); }
    };
    const state = async () => ({
      table: (await pool.query("SELECT to_regclass('sessions.lazy_ack_t2_mode')::text r")).rows[0].r as string | null,
      ledger: (await pool.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062210_lazy_ack_t2.sql'")).rows[0].n as number,
      marker: (await pool.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0].status as string,
      t2Body: (await pool.query("SELECT prosrc LIKE '%lazy_ack_t2_mode%' b FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].b as boolean,
    });
    /** An ACK entering with the gate 'fenced' (mode 'main'), paused after its own ingest_lock_fences; then the gate set off. */
    async function pausedFencedAck() {
      const payload = await prepared("rb-u1", "rb-o1");
      await pool.query(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_t2='fenced'`);
      const T = await conn("t2-pause-holder"); await T.c.query(`BEGIN;SELECT pg_advisory_xact_lock(${PAUSE_KEY})`);
      const A = await conn(PAUSE_APP);
      await A.c.query("BEGIN;SET LOCAL lock_timeout='30s';SET LOCAL statement_timeout='60s';SET LOCAL ROLE bulls96_session_ingest");
      const ack = A.c.query(COMMIT, [payload]).then((r) => ({ ok: true as const, kind: r.rows[0].result.kind as string }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
      await lockWaiting(A.pid);
      const locks = (await pool.query(`SELECT locktype,classid,objid,relation::regclass::text rel,mode,granted FROM pg_locks WHERE pid=$1`, [A.pid])).rows;
      // Paused where intended: 50000 shared held, the test key awaited, and no lock of any kind on the mode table yet.
      expect(locks.some((l) => l.locktype === "advisory" && Number(l.classid) === 961996 && Number(l.objid) === 50000 && l.mode === "ShareLock" && l.granted)).toBe(true);
      expect(locks.filter((l) => !l.granted).map((l) => [l.locktype, Number(l.objid)])).toEqual([["advisory", PAUSE_KEY]]);
      expect(locks.some((l) => l.rel === "sessions.lazy_ack_t2_mode")).toBe(false);
      // Bare ALTER on purpose (out of protocol): the premise is a gate set off while this ACK is open; the supported
      // lazy_ack_t2_set_gate would be busy here, which is itself the protection.
      await pool.query(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_t2='off'`);
      expect((await pool.query("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows[0].mode).toBe("main");
      let finished = false;
      return { ack, pid: A.pid, release: async () => { await T.c.query("COMMIT"); await T.end(); },
        finish: async () => { if (finished) return; finished = true; await A.c.query("COMMIT").catch(() => {}); await A.end(); } };
    }
    return { pool, down, state, pausedFencedAck, conn, lockWaiting };
  }

  test("DEP: without its quiescence step the down still cannot drop the table under the ACK: it waits on the marker row the ACK's guard holds FOR SHARE (no queued ingest pause)", async () => {
    const d = await scratchDb();
    const unguarded = DOWN.replace(/-- Quiescence, fail fast:[\s\S]*?END \$t2_quiesce\$;\n/, "");
    expect(unguarded).not.toContain("t2_quiesce");
    const p = await d.pausedFencedAck();
    const D = await d.conn("t2-down-unguarded"), R = await d.conn("t2-later-reader");
    try {
      const downDone = D.c.query(unguarded).then(() => null, async (e: any) => { await D.c.query("ROLLBACK").catch(() => {}); return String(e.code); });
      await d.lockWaiting(D.pid);
      // The concrete dependency: the down's marker UPDATE waits on the paused ACK (ingest_lazy_guard: marker FOR SHARE).
      expect((await d.pool.query("SELECT pg_blocking_pids($1) b", [D.pid])).rows[0].b).toEqual([p.pid]);
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
    expect(before).toEqual({ table: "sessions.lazy_ack_t2_mode", ledger: 1, marker: "pass", t2Body: true });
    const p = await d.pausedFencedAck();
    try {
      const t0 = performance.now();
      expect(await d.down(DOWN)).toMatchObject({ code: "55P03", message: expect.stringContaining("ingest or erase transactions are open") });
      expect(performance.now() - t0).toBeLessThan(2000); // refused at once, nothing queued
      expect((await d.pool.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND objid=50000 AND NOT granted")).rows[0].n).toBe(0);
      expect(await d.state()).toEqual(before);
      await p.release();
      expect(await p.ack).toEqual({ ok: true, kind: "committed" });
    } finally { await p.finish(); }
    expect(await d.down(DOWN)).toBeNull();
    expect(await d.state()).toMatchObject({ table: null, ledger: 0, t2Body: false });
    const proc = (await d.pool.query("SELECT proconfig,prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
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
});
