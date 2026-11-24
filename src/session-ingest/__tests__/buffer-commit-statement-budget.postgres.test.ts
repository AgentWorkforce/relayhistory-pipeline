/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): the buffered commit's own statement
 * budget (ledger-sql.ts inBufferCommitBudget, INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS; docs/design/buffer-commit-statement-budget.md).
 * Same fixture as buffer-commit-lock-wait.postgres.test.ts: a LOGIN role carrying the bootstrap file's role settings,
 * acting as bulls96_session_ingest, sending the real sqlLedger(...).commitBuffered over the real deliverBatch input.
 *   budget     two sequential waits (seat clock, then the origin advisory lock) that pass 5 s: 57014 at 5 s with the
 *              single statement, committed with a 7000 ms budget; past the budget, 57014 at ~7 s.
 *   hygiene    after success, 55P03, 57014, a failed opening flight, a failed COMMIT and a client that dies mid
 *              transaction: the next statement sees the role's 2s / 5s and the backend is not in a transaction.
 *   certainty  a COMMIT whose answer is lost committed; the repeat of the same commit answers the same result, also
 *              when it arrives while the first transaction is still open.
 *   cost       round trips (1 vs 3) and how much longer the seat clock is held past the call's answer, at 250 and 500.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { deliverBatch } from "../deliver.ts";
import { bufferCommitBudgetOpen, inBufferCommitBudget, sqlLedger } from "../ledger-sql.ts";
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

describe.skipIf(!url)("buffered commit statement budget (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, waiter: pg.Client, name: string, login: string, generation: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0, waiterPid = 0;
  const receipts: Record<string, unknown>[] = [];
  const tenant = (label: string): Tenant => ({ ...TENANT, userId: `commit-budget-${label}-${++serial}` });
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
    name = "commit_budget_" + crypto.randomUUID().replaceAll("-", "");
    login = "commit_budget_login_" + crypto.randomUUID().replaceAll("-", "").slice(0, 16);
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
    console.log(JSON.stringify({ proof: "buffer-commit-budget-fixture", serverVersion: (await admin.query("SELECT version() v")).rows[0].v, serverVersionNum: version, generation }));
  }, 180000);
  afterAll(async () => {
    console.log(JSON.stringify({ proof: "buffer-commit-budget-receipts", receipts }));
    await waiter?.end().catch(() => {});
    await pool?.end(); scratch?.done();
    if (admin) {
      if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)");
      if (login) await admin.query(`DROP ROLE IF EXISTS ${login}`);
      await admin.end();
    }
  }, 30000);

  const budgetLedger = (sent: Sent[], opts: { budget?: number; lockWait?: number } = {}, run = waiterRun(sent)): Ledger => sqlLedger(run, {
    acceptanceMode: "buffered", expectedGeneration: generation,
    ...(opts.budget === undefined ? {} : { bufferCommitBudgetMs: opts.budget }), ...(opts.lockWait === undefined ? {} : { bufferCommitLockWaitMs: opts.lockWait }) });
  /** The origin advisory lock the staged ACK takes after the seat clock (043800), held on its own connection. */
  async function holdOrigin(holdMs: number, origin = "origin-1"): Promise<{ released: Promise<number> }> {
    const c = await pool.connect(); const t0 = performance.now();
    await c.query("BEGIN");
    await c.query("SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',$1::text,$2::text,$3::text)::text,0))", [TENANT.orgId, TENANT.workspaceId, origin]);
    // Wrapped: an async function would otherwise adopt the returned promise and wait out the whole hold.
    return { released: Bun.sleep(holdMs).then(async () => { try { await c.query("ROLLBACK"); return Math.round(performance.now() - t0); } finally { c.release(); } }) };
  }
  const outcome = (p: Promise<unknown>, t0: number) => p.then(() => ({ ok: true as const, ms: Math.round(performance.now() - t0) }),
    (e: unknown) => ({ ok: false as const, ms: Math.round(performance.now() - t0), code: e instanceof LedgerUnavailableError ? e.code : String(e), sqlstate: e instanceof LedgerUnavailableError ? e.db?.db_error_code : (e as any)?.code }));
  /** The waiter connection's state after an outcome: role settings back, and not inside a transaction. */
  async function hygiene() {
    const s = (await waiter.query("SELECT current_setting('lock_timeout') lt, current_setting('statement_timeout') st, current_setting('idle_in_transaction_session_timeout') it")).rows[0];
    const a = (await pool.query("SELECT state, backend_xid::text xid FROM pg_stat_activity WHERE pid=$1", [waiterPid])).rows[0];
    return { lockTimeout: s.lt, statementTimeout: s.st, idleInTx: s.it, state: a.state, xid: a.xid };
  }
  const CLEAN = { lockTimeout: "2s", statementTimeout: "5s", idleInTx: "10s", state: "idle", xid: null };
  const batchState = async (input: { batchId: string; originId: string }) =>
    (await pool.query("SELECT state FROM sessions.ingest_batches WHERE batch_id=$1 AND origin_id=$2", [input.batchId, input.originId])).rows[0]?.state as string | undefined;

  test("budget: two sequential waits past 5 s end 57014 as one statement and commit with a 7000 ms budget; past the budget 57014 at the budget", async () => {
    for (const [label, budget, originHold, expect_] of [["single-statement", undefined, 6000, "57014"], ["budget-7000", 7000, 6000, "ok"], ["budget-6000-past", 6000, 9000, "57014"]] as const) {
      const input = await stagedCommit(`budget-${label}`);
      const sent: Sent[] = [];
      const clock = await holdClock(3000), origin = await holdOrigin(originHold);
      const t0 = performance.now();
      const pending = outcome(budgetLedger(sent, { budget, lockWait: 4000 }).commitBuffered!(input), t0);
      const r = await pending;
      const held = [await clock.released, await origin.released];
      const h = await hygiene();
      receipts.push({ case: `budget:${label}`, budget: budget ?? null, lockWait: 4000, clockHeld: held[0], originHeld: held[1], ...r, flights: sent.length, hygiene: h });
      if (expect_ === "ok") { expect(r.ok).toBe(true); expect(r.ms).toBeGreaterThanOrEqual(5500); expect(r.ms).toBeLessThan(6900); }
      else { expect(r).toMatchObject({ ok: false, code: "ledger_timeout", sqlstate: "57014" }); expect(r.ms).toBeGreaterThanOrEqual((budget ?? 5000) - 100); expect(r.ms).toBeLessThan((budget ?? 5000) + 900); }
      expect(sent.length).toBe(budget === undefined ? 1 : expect_ === "ok" ? 3 : 3); // open, call, COMMIT or ROLLBACK
      if (budget !== undefined) { expect(sent[0]!.text).toBe(bufferCommitBudgetOpen(budget, 4000)); expect(sent[1]!.text).toBe("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result"); expect(sent[2]!.text).toBe(expect_ === "ok" ? "COMMIT" : "ROLLBACK"); }
      expect(h).toEqual(CLEAN);
    }
  }, 240000);

  test("hygiene: success, 55P03, 57014, failed opening flight, failed COMMIT send: role settings back, no open transaction, nothing half-written", async () => {
    // success
    let input = await stagedCommit("h-ok"); let sent: Sent[] = [];
    expect((await outcome(budgetLedger(sent, { budget: 6000 }).commitBuffered!(input), performance.now())).ok).toBe(true);
    expect(await batchState(input)).toBe("committed"); expect(await hygiene()).toEqual(CLEAN);
    receipts.push({ case: "hygiene:success", flights: sent.map((s) => s.text.split(" ")[0]), hygiene: await hygiene() });
    // 55P03
    input = await stagedCommit("h-55p03"); sent = [];
    let clock = await holdClock(4000);
    let r = await outcome(budgetLedger(sent, { budget: 6000, lockWait: 3000 }).commitBuffered!(input), performance.now());
    await clock.released;
    expect(r).toMatchObject({ ok: false, code: "ledger_lock_timeout", sqlstate: "55P03" }); expect(sent.at(-1)!.text).toBe("ROLLBACK");
    expect(await batchState(input)).not.toBe("committed"); expect(await hygiene()).toEqual(CLEAN);
    receipts.push({ case: "hygiene:55P03", ...r, hygiene: await hygiene() });
    // 57014
    input = await stagedCommit("h-57014"); sent = [];
    clock = await holdClock(3000); const origin = await holdOrigin(7000);
    r = await outcome(budgetLedger(sent, { budget: 5001, lockWait: 4000 }).commitBuffered!(input), performance.now());
    await clock.released; await origin.released;
    expect(r).toMatchObject({ ok: false, code: "ledger_timeout", sqlstate: "57014" }); expect(sent.at(-1)!.text).toBe("ROLLBACK");
    expect(await batchState(input)).not.toBe("committed"); expect(await hygiene()).toEqual(CLEAN);
    receipts.push({ case: "hygiene:57014", ...r, hygiene: await hygiene() });
    // a failed opening flight (the server refuses the SET): ROLLBACK, nothing open
    const failedOpen = await inBufferCommitBudget(waiterRun([]), -5, undefined, async () => "never").catch((e: unknown) => e);
    expect(failedOpen).toBeInstanceOf(LedgerUnavailableError);
    expect(await hygiene()).toEqual(CLEAN);
    receipts.push({ case: "hygiene:failed-open", code: (failedOpen as LedgerUnavailableError).code, sqlstate: (failedOpen as LedgerUnavailableError).db?.db_error_code, hygiene: await hygiene() });
    // COMMIT never sent (the client fails before sending it): ROLLBACK, not committed, the repeat commits
    input = await stagedCommit("h-commit-unsent"); sent = [];
    const unsent = waiterRun(sent);
    const noCommit = async (text: string, params: unknown[]) => { if (text === "COMMIT") throw Object.assign(new Error("socket"), { code: "ECONNRESET" }); return unsent(text, params); };
    r = await outcome(budgetLedger(sent, { budget: 6000 }, noCommit).commitBuffered!(input), performance.now());
    expect(r).toMatchObject({ ok: false, code: "ledger_connection" });
    expect(await batchState(input)).not.toBe("committed"); expect(await hygiene()).toEqual(CLEAN);
    expect((await outcome(budgetLedger([], { budget: 6000 }).commitBuffered!(input), performance.now())).ok).toBe(true);
    receipts.push({ case: "hygiene:commit-unsent", ...r, afterRepeat: await batchState(input), hygiene: await hygiene() });
  }, 240000);

  test("certainty: a COMMIT whose answer is lost did commit; the repeat answers the same result, also while the first transaction is open", async () => {
    // Answer lost after COMMIT ran on the server.
    let input = await stagedCommit("c-lost");
    const send = waiterRun([]);
    const lost = async (text: string, params: unknown[]) => { const r = await send(text, params); if (text === "COMMIT") throw Object.assign(new Error("socket"), { code: "ECONNRESET" }); return r; };
    const r = await outcome(budgetLedger([], { budget: 6000 }, lost).commitBuffered!(input), performance.now());
    expect(r).toMatchObject({ ok: false, code: "ledger_connection" }); // the Worker answers 503: the outcome is unknown to it
    expect(await batchState(input)).toBe("committed");
    const body = JSON.stringify({ ...input, generation });
    const repeat1 = (await waiter.query("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result", [body])).rows[0].result;
    const seat1 = (await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock WHERE singleton")).rows[0].s;
    const repeat2 = (await waiter.query("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result", [body])).rows[0].result;
    const seat2 = (await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock WHERE singleton")).rows[0].s;
    expect(repeat1.kind).toBe("committed"); expect(repeat2).toEqual(repeat1); expect(seat2).toBe(seat1);
    receipts.push({ case: "certainty:lost-commit-answer", ...r, stateAfter: "committed", repeatEqual: true, seatUnchanged: true });
    // Repeat while the first transaction is still open (after its call, before COMMIT).
    input = await stagedCommit("c-open");
    const as = new URL(url!); as.pathname = "/" + name; as.username = login;
    const first = new pg.Client({ connectionString: as.href }); await first.connect(); await first.query("SET ROLE bulls96_session_ingest");
    const b2 = JSON.stringify({ ...input, generation });
    await first.query(bufferCommitBudgetOpen(6000), []);
    const firstResult = (await first.query("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result", [b2])).rows[0].result;
    const t0 = performance.now();
    const second = waiter.query("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result", [b2]).then((x) => ({ result: x.rows[0].result, ms: Math.round(performance.now() - t0) }), (e: any) => ({ error: e.code, ms: Math.round(performance.now() - t0) }));
    await Bun.sleep(800);
    await first.query("COMMIT"); await first.end();
    const got = await second as any;
    receipts.push({ case: "certainty:repeat-while-open", firstKind: firstResult.kind, secondWaitedMs: got.ms, secondKind: got.result?.kind, secondError: got.error ?? null, equal: JSON.stringify(got.result) === JSON.stringify(firstResult) });
    expect(got.error).toBeUndefined();
    expect(got.ms).toBeGreaterThanOrEqual(700); // it waited for the first transaction
    expect(got.result).toEqual(firstResult);
    expect(await hygiene()).toEqual(CLEAN);
  }, 240000);

  test("a client that dies mid transaction: its locks end with the connection; a stranded live connection ends by the 1 s idle bound", async () => {
    const as = new URL(url!); as.pathname = "/" + name; as.username = login;
    for (const mode of ["socket-closed", "stranded-alive"] as const) {
      const input = await stagedCommit(`die-${mode}`);
      const d = new pg.Client({ connectionString: as.href }); d.on("error", () => {}); await d.connect(); await d.query("SET ROLE bulls96_session_ingest");
      const dpid = (await d.query("SELECT pg_backend_pid() p")).rows[0].p as number;
      await d.query(bufferCommitBudgetOpen(6000), []);
      await d.query("SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result", [JSON.stringify({ ...input, generation })]); // holds clock, fence, origin
      if (mode === "socket-closed") (d as any).connection.stream.destroy();
      const t0 = performance.now();
      const c = await pool.connect();
      try { await c.query("BEGIN; SET LOCAL lock_timeout = '5s'"); await c.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE"); await c.query("ROLLBACK"); }
      finally { c.release(); }
      const freedMs = Math.round(performance.now() - t0);
      const alive = (await pool.query("SELECT count(*)::int n FROM pg_stat_activity WHERE pid=$1", [dpid])).rows[0].n;
      receipts.push({ case: `die:${mode}`, clockFreedAfterMs: freedMs, backendAlive: alive, batchState: await batchState(input) });
      expect(await batchState(input)).not.toBe("committed");
      expect(freedMs).toBeLessThan(mode === "socket-closed" ? 1000 : 2500);
      if (mode === "stranded-alive") expect(freedMs).toBeGreaterThanOrEqual(800);
      await d.end().catch(() => {});
    }
  }, 120000);

  test("cost: round trips and the extra seat-clock hold after the call's answer, at 250 and 500 records", async () => {
    const rows: Record<string, unknown>[] = [];
    for (const records of [250, 500]) for (const budget of [undefined, 6000]) {
      const stage: number[] = [], extraHold: number[] = []; let flights = 0;
      for (let i = 0; i < 5; i++) {
        const input = await stagedCommit(`cost-${records}-${budget ?? 0}-${i}`, records);
        let answeredAt = 0; const s: Sent[] = []; const base = waiterRun(s);
        const timedRun = async (text: string, params: unknown[]) => { const r = await base(text, params); if (text.includes("ingest_commit_buffered_batch")) answeredAt = performance.now(); return r; };
        const t0 = performance.now();
        await budgetLedger(s, { budget }, timedRun).commitBuffered!(input);
        const done = performance.now();
        stage.push(done - t0); extraHold.push(done - answeredAt); flights = s.length;
      }
      const med = (a: number[]) => Math.round([...a].sort((x, y) => x - y)[2]! * 10) / 10;
      rows.push({ records, budget: budget ?? null, flights, stageMsMedian: med(stage), holdAfterAnswerMsMedian: med(extraHold) });
    }
    receipts.push({ case: "cost", rows });
    console.log(JSON.stringify({ proof: "buffer-commit-budget-cost", rows }));
    for (const r of rows) expect(r.flights).toBe(r.budget === null ? 1 : 3);
  }, 600000);
});
