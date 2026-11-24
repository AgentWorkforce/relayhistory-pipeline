/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): two-connection proof of the buffered
 * commit lock wait (ledger-sql.ts bufferCommitLockWaitSql, INGEST_BUFFER_COMMIT_LOCK_WAIT_MS;
 * docs/design/buffer-commit-lock-wait.md). The waiter is a LOGIN role carrying the role settings that
 * supabase/bootstrap/session_ingest_role.sql gives the ingest login (read from that file: statement_timeout 5s,
 * lock_timeout 2s, idle_in_transaction_session_timeout 10s), acting as bulls96_session_ingest, and sends the exact
 * statement the Worker sends: the real sqlLedger(...).commitBuffered over the input the real deliverBatch passes it,
 * as ONE autocommit statement. The holder keeps the receipt seat clock row FOR UPDATE (which the staged ACK's FOR NO
 * KEY UPDATE waits on) and releases it on a schedule.
 *   a  option absent, holder ~3 s: 55P03 at ~2 s (the role's lock_timeout).
 *   b  option 4000, holder ~3 s: the commit succeeds after ~3 s.
 *   c  option 3000, holder ~4.5 s: 55P03 (not 57014) at ~3 s.
 *   d  holder ~7 s: option 4000 gives 55P03 at ~4 s; option absent 55P03 at ~2 s; the same statement with an
 *      out-of-range 6000 ms (bypassing validation) ends 57014 at ~5 s, which is why the range stops at 4000.
 *   e  after every outcome the next statement on the same connection shows lock_timeout 2s again.
 * Order: a probe function with the ingest functions' shape reports, from inside, the lock_timeout it runs under,
 * through unnamed, named (prepared) and forced-generic-plan executions.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { deliverBatch } from "../deliver.ts";
import { BUFFER_COMMIT_LOCK_WAIT_MS_MAX, bufferCommitLockWaitSql, sqlLedger } from "../ledger-sql.ts";
import { LedgerUnavailableError, type Ledger } from "../ledger.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// Refuse credentials, remote endpoints and nonfixture database identities before connecting.
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
/** The ingest login's role settings, exactly as the bootstrap file sets them. */
const ROLE_SETTINGS = [...readFileSync(join(root, "supabase/bootstrap/session_ingest_role.sql"), "utf8")
  .matchAll(/^ALTER ROLE bulls96_session_ingest SET (\w+) = '([^']+)';$/gmu)].map((m) => [m[1]!, m[2]!] as const);
type Tenant = typeof TENANT;
type Sent = { text: string; params: unknown[] };
type Outcome = { ok: true; ms: number } | { ok: false; ms: number; code: string; sqlstate: string | undefined };

describe.skipIf(!url)("buffered commit lock wait (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, waiter: pg.Client, name: string, login: string, generation: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0, waiterPid = 0;
  const receipts: Record<string, unknown>[] = [];
  const tenant = (label: string): Tenant => ({ ...TENANT, userId: `commit-wait-${label}-${++serial}` });
  /** Setup ledger (claim and stage): a pooled connection per call, as the runtime role. Not the path under proof. */
  const setupLedger = () => sqlLedger(async (sql, params) => {
    const c = await pool.connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    finally { await c.query("ROLLBACK").catch(() => {}); c.release(); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  /** The waiter's run: the Worker's shape (one pg Client, autocommit, one query per call), recording what is sent. */
  const waiterRun = (sent: Sent[]) => async (text: string, params: unknown[]) => {
    sent.push({ text, params: [...params] });
    return (await waiter.query(text, params as never[])) as unknown as { rows: Record<string, unknown>[] };
  };
  const waiterLedger = (sent: Sent[], bufferCommitLockWaitMs?: number, originClaimFence = false): Ledger => sqlLedger(waiterRun(sent), {
    acceptanceMode: "buffered", expectedGeneration: generation, ...(bufferCommitLockWaitMs === undefined ? {} : { bufferCommitLockWaitMs }), ...(originClaimFence ? { originClaimFence } : {}) });

  async function custody(t: Tenant, raw: any) {
    const p = await prepareDelivery(t, raw);
    const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string;
    return { witness, sourceDigest };
  }
  async function raw(t: Tenant, id: string, records = 1) {
    return batch(Array.from({ length: records }, (_, i) => record({ record_id: records === 1 ? id : `${id}-${i}`, revision: 2, session_id: `${id}-s` })),
      { batch_id: id, account_id: await deliveryAccount(t) });
  }
  /** The real deliverBatch up to the buffered commit: claim and stage commit, and the commit's exact input is returned. */
  async function stagedCommit(label: string, records = 1): Promise<Parameters<NonNullable<Ledger["commitBuffered"]>>[0]> {
    const t = tenant(label), id = `${label}-${serial}`;
    const warm = await raw(t, `warm-${id}`);
    expect((await deliverBatch({ ledger: setupLedger(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, warm) } as never, t, warm)).outcome).toBe("accepted_buffered");
    const r = await raw(t, id, records);
    let input: Parameters<NonNullable<Ledger["commitBuffered"]>>[0] | undefined;
    const base = setupLedger();
    const ledger: Ledger = { ...base, commitBuffered: async (i) => { input = i; throw Object.assign(Error("captured"), { code: "XXCAP" }); } };
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r).catch(() => {});
    if (!input) throw Error("commit_input_not_captured");
    return input;
  }
  /** Holder: the seat clock row FOR UPDATE on its own connection, released after `holdMs` by ROLLBACK (it wrote nothing
   *  to keep; a ROLLBACK does not wait for a WAL flush, so the release time does not depend on the disk). */
  async function holdClock(holdMs: number): Promise<{ pid: number; released: Promise<number> }> {
    const c: PoolClient = await pool.connect();
    const pid = (await c.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
    await c.query("BEGIN");
    await c.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
    const t0 = performance.now();
    // Resolves with the ms from lock to the holder's COMMIT completing (the real hold, scheduler delay included).
    const released = Bun.sleep(holdMs).then(async () => { try { await c.query("ROLLBACK"); return Math.round(performance.now() - t0); } finally { c.release(); } });
    return { pid, released };
  }
  /** Proves the waiter is queued behind the holder (not something else) while it waits. */
  async function expectBlockedBy(holder: number) {
    const end = Date.now() + 1500;
    while (Date.now() < end) {
      const r = await pool.query("SELECT $2::int = ANY(pg_blocking_pids($1::int)) AS ok", [waiterPid, holder]);
      if (r.rows[0].ok) return;
      await Bun.sleep(10);
    }
    throw Error("waiter_not_blocked_by_holder");
  }
  /** One schedule: holder keeps the clock `holdMs`; the waiter sends the Worker's commit with the option (or none). */
  async function schedule(label: string, holdMs: number, option?: number, origin = false): Promise<{ outcome: Outcome; sent: Sent[]; after: string }> {
    const input = await stagedCommit(label);
    const sent: Sent[] = [];
    const holder = await holdClock(holdMs);
    const t0 = performance.now();
    const pending = waiterLedger(sent, option, origin).commitBuffered!(input).then(
      () => ({ ok: true as const, ms: Math.round(performance.now() - t0) }),
      (e: unknown) => ({ ok: false as const, ms: Math.round(performance.now() - t0), code: e instanceof LedgerUnavailableError ? e.code : String(e),
        sqlstate: e instanceof LedgerUnavailableError ? e.db?.db_error_code : undefined }));
    await expectBlockedBy(holder.pid);
    const outcome: Outcome = await pending;
    const heldMs = await holder.released;
    // e: the next statement on the same connection is back on the role's setting.
    const after = (await waiter.query("SHOW lock_timeout")).rows[0].lock_timeout as string;
    receipts.push({ case: label, entry: sent[0]?.text.match(/sessions\.(\w+)\(/)?.[1], holdMs, heldMs, option: option ?? null, ...outcome, nextStatementLockTimeout: after });
    return { outcome, sent, after };
  }

  const heldOf = (label: string) => (receipts.find((r) => r.case === label)?.heldMs as number | undefined) ?? Number.NaN;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(150000);
    name = "commit_wait_" + crypto.randomUUID().replaceAll("-", "");
    login = "commit_wait_login_" + crypto.randomUUID().replaceAll("-", "").slice(0, 16);
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 6 });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); }
    generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    // The waiter's login: the bootstrap file's role settings on a disposable LOGIN role that may act as the ingest role.
    expect(Object.fromEntries(ROLE_SETTINGS)).toMatchObject({ statement_timeout: "5s", lock_timeout: "2s" });
    await admin.query(`CREATE ROLE ${login} LOGIN NOINHERIT`);
    for (const [k, v] of ROLE_SETTINGS) await admin.query(`ALTER ROLE ${login} SET ${k} = '${v}'`);
    await admin.query(`GRANT bulls96_session_ingest TO ${login}`);
    const as = new URL(target.href); as.username = login;
    waiter = new pg.Client({ connectionString: as.href }); await waiter.connect();
    await waiter.query("SET ROLE bulls96_session_ingest"); // session level, as ingest_require_role reads it
    waiterPid = (await waiter.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
    const s = (await waiter.query("SELECT current_setting('statement_timeout') st, current_setting('lock_timeout') lt")).rows[0];
    expect([s.st, s.lt]).toEqual(["5s", "2s"]);
    // The commit chain sets no function-level lock_timeout (one would override the statement's local value).
    const cfg = await pool.query("SELECT p.proname, p.proconfig FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname = ANY($1)",
      [["ingest_commit_buffered_batch", "ingest_origin_commit_buffered_batch", "ingest_lazy_ack_batch"]]);
    expect(cfg.rows).toHaveLength(3);
    for (const r of cfg.rows) expect(String(r.proconfig ?? "")).not.toContain("lock_timeout");
    console.log(JSON.stringify({ proof: "buffer-commit-lock-wait-fixture", serverVersion: (await admin.query("SELECT version() v")).rows[0].v, serverVersionNum: version, generation }));
  }, 180000);
  afterAll(async () => {
    console.log(JSON.stringify({ proof: "buffer-commit-lock-wait-receipts", receipts }));
    await waiter?.end().catch(() => {});
    await pool?.end(); scratch?.done();
    if (admin) {
      if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)");
      if (login) await admin.query(`DROP ROLE IF EXISTS ${login}`);
      await admin.end();
    }
  }, 30000);

  test("order: the statement's set_config is in effect inside the function on every execution and plan shape, and never after", async () => {
    // Same shape as the ingest functions: plpgsql, SECURITY DEFINER, its own search_path, no function-level lock_timeout.
    await pool.query(`CREATE FUNCTION sessions.commit_wait_order_probe(p jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER
      SET search_path = pg_catalog, pg_temp AS $f$ BEGIN RETURN jsonb_build_object('lock_timeout', current_setting('lock_timeout'), 'p', p); END $f$`);
    await pool.query("GRANT EXECUTE ON FUNCTION sessions.commit_wait_order_probe(jsonb) TO bulls96_session_ingest");
    const text = bufferCommitLockWaitSql("commit_wait_order_probe");
    const seen: string[] = [];
    for (const [i, ms] of [3000, 4000, 2001, 3500].entries()) {
      const r = await waiter.query(text, [JSON.stringify({ i }), `${ms}ms`]);
      expect(r.rows[0].result.p).toEqual({ i });
      seen.push(r.rows[0].result.lock_timeout);
      expect((await waiter.query("SHOW lock_timeout")).rows[0].lock_timeout).toBe("2s");
    }
    expect(seen).toEqual(["3s", "4s", "2001ms", "3500ms"]);
    // A named prepared statement executed past the custom-plan threshold, then with a forced generic plan.
    for (const mode of ["auto", "force_generic_plan"]) {
      await waiter.query(`SET plan_cache_mode = ${mode}`);
      for (let i = 0; i < 8; i++) {
        const r = await waiter.query({ name: `commit_wait_probe_${mode}`, text, values: [JSON.stringify({ i }), "3500ms"] });
        expect(r.rows[0].result).toEqual({ lock_timeout: "3500ms", p: { i } });
        expect((await waiter.query("SHOW lock_timeout")).rows[0].lock_timeout).toBe("2s");
      }
    }
    await waiter.query("RESET plan_cache_mode");
    // A missing entry point fails at parse time with the message call() matches for the origin-fence fallback.
    const missing = await waiter.query(bufferCommitLockWaitSql("commit_wait_absent_fn"), ["{}", "3000ms"]).catch((e: any) => e);
    expect(missing.code).toBe("42883");
    expect(String(missing.message)).toContain("function sessions.commit_wait_absent_fn(jsonb)");
    expect((await waiter.query("SHOW lock_timeout")).rows[0].lock_timeout).toBe("2s");
  }, 60000);

  test("a: option absent, holder ~3 s: 55P03 at ~2 s; the statement is the plain one-parameter call", async () => {
    const { outcome, sent, after } = await schedule("a", 3000);
    expect(outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(outcome.ms).toBeGreaterThanOrEqual(1900); expect(outcome.ms).toBeLessThan(2900);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.text).toBe("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result");
    expect(sent[0]!.params).toHaveLength(1);
    expect(after).toBe("2s");
  }, 120000);

  test("b: option 4000, holder ~3 s: the same commit succeeds after the holder releases; one statement", async () => {
    const { outcome, sent, after } = await schedule("b", 3000, 4000);
    expect(heldOf("b")).toBeLessThan(3900); // fixture precondition: the holder really released before the wait
    expect(outcome.ok).toBe(true);
    // Waited past the role's 2 s; the upper bound is the hold plus the commit's own work after the wait (not the option).
    expect(outcome.ms).toBeGreaterThanOrEqual(2900); expect(outcome.ms).toBeLessThan(4900);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.text).toBe(bufferCommitLockWaitSql("ingest_commit_buffered_batch"));
    expect(sent[0]!.params[1]).toBe("4000ms");
    expect(after).toBe("2s");
  }, 120000);

  test("c: option 3000, holder ~4.5 s: 55P03 (not 57014) at ~3 s", async () => {
    const { outcome, after } = await schedule("c", 4500, 3000);
    expect(outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(outcome.ms).toBeGreaterThanOrEqual(2900); expect(outcome.ms).toBeLessThan(3900);
    expect(after).toBe("2s");
  }, 120000);

  test("d: holder ~7 s: option at the maximum ends 55P03 at ~4 s; absent 55P03 at ~2 s; past the range 57014 at ~5 s", async () => {
    const max = await schedule("d-max", 7000, BUFFER_COMMIT_LOCK_WAIT_MS_MAX);
    expect(max.outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(max.outcome.ms).toBeGreaterThanOrEqual(3900); expect(max.outcome.ms).toBeLessThan(4900);
    expect(max.after).toBe("2s");
    const absent = await schedule("d-absent", 7000);
    expect(absent.outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(absent.outcome.ms).toBeLessThan(2900);
    // The validation refuses 6000; the same statement sent directly shows what a value past the range would do.
    expect(() => waiterLedger([], 6000)).toThrow(LedgerUnavailableError);
    const input = await stagedCommit("d-over");
    const holder = await holdClock(7000);
    const t0 = performance.now();
    const over = await waiter.query(bufferCommitLockWaitSql("ingest_commit_buffered_batch"), [JSON.stringify({ ...input, generation }), "6000ms"]).catch((e: any) => e);
    const ms = Math.round(performance.now() - t0);
    await holder.released;
    const after = (await waiter.query("SHOW lock_timeout")).rows[0].lock_timeout as string;
    receipts.push({ case: "d-over-range-direct", holdMs: 7000, option: 6000, ok: false, ms, sqlstate: over.code, nextStatementLockTimeout: after });
    expect(over.code).toBe("57014");
    expect(ms).toBeGreaterThanOrEqual(4900); expect(ms).toBeLessThan(5900);
    expect(after).toBe("2s");
  }, 180000);

  test("budget: the statement timer is armed before the statement runs; lock waits just under, at and over 5 s", async () => {
    // A transaction-local statement_timeout set inside the same statement does not move that statement's timer.
    await pool.query(`CREATE FUNCTION sessions.commit_wait_sleep_probe(p jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER
      SET search_path = pg_catalog, pg_temp AS $f$ BEGIN PERFORM pg_sleep((p->>'s')::float8); RETURN jsonb_build_object('statement_timeout', current_setting('statement_timeout')); END $f$`);
    await pool.query("GRANT EXECUTE ON FUNCTION sessions.commit_wait_sleep_probe(jsonb) TO bulls96_session_ingest");
    const t0 = performance.now();
    const timer = await waiter.query("SELECT sessions.commit_wait_sleep_probe(CASE WHEN set_config('statement_timeout', $2, true) IS NOT NULL THEN $1::jsonb END) AS result",
      [JSON.stringify({ s: 6 }), "9000ms"]).catch((e: any) => e);
    const timerMs = Math.round(performance.now() - t0);
    const timerAfter = (await waiter.query("SHOW statement_timeout")).rows[0].statement_timeout as string;
    receipts.push({ case: "statement-timer-in-statement", set: "9000ms", sleepS: 6, sqlstate: timer.code ?? null, ms: timerMs, nextStatementStatementTimeout: timerAfter });
    expect(timer.code).toBe("57014");
    expect(timerMs).toBeGreaterThanOrEqual(4900); expect(timerMs).toBeLessThan(5900);
    expect(timerAfter).toBe("5s");
    // Lock waits around the 5 s budget on the real commit statement (values sent directly; validation refuses them).
    const seen: Record<number, string> = {};
    for (const ms of [4500, 4900, 5000, 5100]) {
      const input = await stagedCommit(`budget-${ms}`);
      const holder = await holdClock(7000);
      const s0 = performance.now();
      const r = await waiter.query(bufferCommitLockWaitSql("ingest_commit_buffered_batch"), [JSON.stringify({ ...input, generation }), `${ms}ms`]).catch((e: any) => e);
      const took = Math.round(performance.now() - s0);
      await holder.released;
      const after = (await waiter.query("SHOW lock_timeout")).rows[0].lock_timeout as string;
      receipts.push({ case: `budget-lock-wait-${ms}`, holdMs: 7000, option: ms, ok: false, ms: took, sqlstate: r.code ?? null, nextStatementLockTimeout: after });
      seen[ms] = r.code;
      expect(["55P03", "57014"]).toContain(r.code);
      expect(after).toBe("2s");
    }
    expect([seen[4500], seen[4900], seen[5100]]).toEqual(["55P03", "55P03", "57014"]);
  }, 240000);

  test("origin entry point (ingest_origin_commit_buffered_batch): the same effective waits a-d", async () => {
    const entry = bufferCommitLockWaitSql("ingest_origin_commit_buffered_batch");
    const a = await schedule("origin-a", 3000, undefined, true);
    expect(a.sent.map((x) => x.text)).toEqual(["SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result"]);
    expect(a.outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(a.outcome.ms).toBeGreaterThanOrEqual(1900); expect(a.outcome.ms).toBeLessThan(2900);
    const b = await schedule("origin-b", 3000, 4000, true);
    expect(heldOf("origin-b")).toBeLessThan(3900);
    expect(b.sent.map((x) => x.text)).toEqual([entry]);
    expect(b.outcome.ok).toBe(true);
    expect(b.outcome.ms).toBeGreaterThanOrEqual(2900); expect(b.outcome.ms).toBeLessThan(4900);
    const c = await schedule("origin-c", 4500, 3000, true);
    expect(c.outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(c.outcome.ms).toBeGreaterThanOrEqual(2900); expect(c.outcome.ms).toBeLessThan(3900);
    const d = await schedule("origin-d-max", 7000, BUFFER_COMMIT_LOCK_WAIT_MS_MAX, true);
    expect(d.outcome).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" });
    expect(d.outcome.ms).toBeGreaterThanOrEqual(3900); expect(d.outcome.ms).toBeLessThan(4900);
    for (const r of [a, b, c, d]) expect(r.after).toBe("2s");
  }, 240000);

  // Opt-in (BUFFER_COMMIT_WAIT_CELLS=1): synthetic queue cells. N commits of B records each queue on the seat clock
  // behind one holder (H ms) and then behind each other; each cell runs with the option absent and at 4000, and
  // counts outcomes. Diagnostic: prints the table, asserts only that every outcome is one of the three classes.
  test.skipIf(!process.env.BUFFER_COMMIT_WAIT_CELLS)("cells: queue depth x hold, option absent vs 4000", async () => {
    const as = new URL(url!); as.pathname = "/" + name; as.username = login;
    const cells: Record<string, unknown>[] = [];
    for (const [records, n, holdMs] of [[1, 8, 1500], [1, 8, 3000], [250, 4, 1500], [250, 8, 1500], [500, 4, 1500], [500, 8, 1500]] as const) {
      for (const option of [undefined, 4000]) {
        const inputs = [];
        for (let i = 0; i < n; i++) inputs.push(await stagedCommit(`cell-${records}-${n}-${holdMs}-${option ?? 0}-${i}`, records));
        const clients = await Promise.all(inputs.map(async () => { const c = new pg.Client({ connectionString: as.href }); await c.connect(); await c.query("SET ROLE bulls96_session_ingest"); return c; }));
        const holder = await holdClock(holdMs);
        const t0 = performance.now();
        const results = await Promise.all(inputs.map((input, i) => sqlLedger(async (text, params) => (await clients[i]!.query(text, params as never[])) as never,
          { acceptanceMode: "buffered", expectedGeneration: generation, ...(option === undefined ? {} : { bufferCommitLockWaitMs: option }) }).commitBuffered!(input).then(
          () => ({ cls: "ok", ms: Math.round(performance.now() - t0) }),
          (e: unknown) => ({ cls: e instanceof LedgerUnavailableError ? String(e.db?.db_error_code ?? e.code) : "other", ms: Math.round(performance.now() - t0) }))));
        await holder.released;
        const leaks = await Promise.all(clients.map(async (c) => (await c.query("SHOW lock_timeout")).rows[0].lock_timeout));
        await Promise.all(clients.map((c) => c.end()));
        const count = (k: string) => results.filter((r) => r.cls === k).length;
        const cell = { records, n, holdMs, option: option ?? null, ok: count("ok"), "55P03": count("55P03"), "57014": count("57014"),
          other: results.filter((r) => !["ok", "55P03", "57014"].includes(r.cls)).map((r) => r.cls), maxMs: Math.max(...results.map((r) => r.ms)), each: results.map((r) => `${r.cls}@${r.ms}`).sort(),
          leaked: leaks.filter((l) => l !== "2s").length };
        cells.push(cell);
        console.log(JSON.stringify({ proof: "buffer-commit-lock-wait-cell", ...cell }));
        expect(cell.other).toEqual([]);
        expect(cell.leaked).toBe(0);
      }
    }
    receipts.push({ case: "cells", cells });
  }, 1800000);
});
