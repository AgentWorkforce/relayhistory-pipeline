/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): two/three-connection proof of
 * 20261105043500_lazy_claim_clock_narrowing. Claims read the singleton seat clock under KEY SHARE and serialize
 * per (org, workspace, origin) on ONE transaction advisory lock, so a claim of another origin no longer queues
 * behind it; same-origin claims serialize and a same-key loser takes the P0W01 ownership path, never 23505/40001.
 * F1/F2 bound the advisory footprint (one lock per claim, never one per record: 53200 safety).
 * C1/C2 (20261105043800_lazy_ack_clock_no_key_update): a staged ACK holds the clock FOR NO KEY UPDATE, so another
 * user's claim (KEY SHARE) no longer queues behind it, while FOR SHARE / FOR UPDATE holders still exclude the ACK.
 * D1 (043800): the staged ACK takes the same origin advisory lock as the claim before any slot (fence -> clock ->
 * origin -> slot), so an ACK and a same-origin claim of another user over reversed slot order never deadlock (40P01).
 * D1n: the causal negative -- the intermediate e79ee9f4 ACK (clock NO KEY UPDATE, no origin lock) deadlocks (40P01) there.
 * Every test that reinstalls older bodies restores 043500 AND THEN 043800 and asserts the 043800 ACK at the restore.
 * Claim/commit payloads are captured from the real deliverBatch -> sqlLedger path and replayed on held connections.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import type { SourceCurrentRecord } from "../custody-protocol.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { originalBodies } from "../../../scripts/sessions-function-boundary/lanes/lazy-claim-clock-narrowing.ts";
import { originalBodies as ackOriginalBodies } from "../../../scripts/sessions-function-boundary/lanes/lazy-ack-clock-no-key-update.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// Refuse credentials, remote endpoints and nonfixture database identities before connecting.
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const epoch = "3".repeat(64), identity = "c".repeat(64);
const migrationPath = "supabase/migrations/20261105043500_lazy_claim_clock_narrowing.sql";
const migrationName = "upstream:20261105043500_lazy_claim_clock_narrowing.sql";
const ackMigrationPath = "supabase/migrations/20261105043800_lazy_ack_clock_no_key_update.sql";
const ACK_CLOCK_LINE = "WHERE singleton FOR NO KEY UPDATE;";
const ACK_ORIGIN_LINE = "PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',p->>'org_id',p->>'workspace_id',p->>'origin_id')::text,0));";
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const text = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const CLAIM = "SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS result";
const COMMIT = "SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result";
type Tenant = typeof TENANT;
type Opts = { current?: SourceCurrentRecord | null; fenced?: boolean; revision?: number; origin?: string };
type Held = { client: PoolClient; pid: number; done: Promise<{ ok: true; result: any } | { ok: false; code: string; message: string }> };

describe.skipIf(!url)("043500 lazy claim seat-clock narrowing (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string, generation: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const sessions = new Set<PoolClient>();
  /** Held transactions not yet finished; afterEach cancels and rolls them back so a failed assertion never leaks an open
   *  transaction (and its locks) into the next test. */
  const open = new Set<Held>();
  const connect = async () => { const c = await pool.connect(); sessions.add(c); return c; };
  const release = async (c: PoolClient) => { await c.query("ROLLBACK").catch(() => {}); sessions.delete(c); c.release(); };
  const tenant = (label: string): Tenant => ({ ...TENANT, userId: `clock-${label}-${++serial}` });
  const ledgerFor = (intercept?: (sql: string, params: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    finally { await release(c); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation });

  async function custody(t: Tenant, raw: any, opts: Opts) {
    const p = await prepareDelivery(t, raw);
    const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: opts.fenced ?? false, currentFenced: false, current: opts.current ?? null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string;
    return { witness, sourceDigest };
  }
  /** Source current custody owned by `owner`, identical for every claimant (never contradictory). */
  async function current(owner: Tenant, id: string): Promise<SourceCurrentRecord> {
    const r = (await prepareDelivery(owner, await batch([record({ record_id: id, revision: 1, session_id: id + "-s" })], { account_id: await deliveryAccount(owner) }))).records[0]!;
    return { userId: owner.userId, revision: String(r.revision), digest: r.digest, revisionId: r.revision_id, kind: r.kind, source: r.source, sessionId: r.session_id,
      operation: r.operation, catalogAcceptedSeq: null, projectionAcceptedSeq: "17" };
  }
  async function raw(t: Tenant, id: string, ids: string[], opts: Opts) {
    const origin = opts.origin ? { origin_id: opts.origin } : {};
    return batch(ids.map((rid) => record({ record_id: rid, revision: opts.revision ?? 2, session_id: rid + "-s", ...origin })), { batch_id: id, account_id: await deliveryAccount(t), ...origin });
  }
  /** Full real delivery (claim, stage, staged ACK). */
  async function deliver(t: Tenant, id: string, ids: string[], opts: Opts = {}) {
    const r = await raw(t, id, ids, opts);
    return deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r, opts) } as never, t, r);
  }
  /** Real completed deliveries that COMMIT the first-claim bookkeeping before any held-claim experiment: the org's
   *  ingest_buffer_admission row (ingest_lazy_begin INSERT ... ON CONFLICT DO NOTHING keyed by org_id -- an
   *  uncommitted first insert makes every other claimant of the org wait on that xid, which is NOT the seat clock) and
   *  each user's fence owner / seed baseline / privacy seal. Held claims then contend only on what 043500 changes. */
  async function warm(...ts: Tenant[]) {
    for (const t of ts) expect((await deliver(t, `warm-${t.userId}`, [`warm-${t.userId}`])).outcome).toBe("accepted_buffered");
  }
  /** The exact jsonb the real path sends to `fn`; the delivery stops there (nothing of that call executes). */
  async function capture(fn: "ingest_claim_buffered_batch" | "ingest_commit_buffered_batch", t: Tenant, id: string, ids: string[], opts: Opts = {}) {
    let payload: string | undefined;
    const r = await raw(t, id, ids, opts);
    const ledger = ledgerFor((sql, params) => { if (sql.includes(fn)) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r, opts) } as never, t, r).catch(() => {});
    if (typeof payload !== "string") throw Error("payload_not_captured:" + fn);
    return payload;
  }
  /** Runs `sql` in an open runtime-role transaction on its own connection; COMMIT/ROLLBACK is the caller's. */
  async function hold(sql: string, payload: string, settings = "SET LOCAL statement_timeout=20000"): Promise<Held> {
    const client = await connect();
    const pid = (await client.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
    await client.query(`BEGIN;${settings};SET LOCAL ROLE bulls96_session_ingest`);
    const held = { client, pid } as Held; open.add(held);
    const done = client.query(sql, [payload]).then((r) => ({ ok: true as const, result: r.rows[0].result }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
    held.done = done; return held;
  }
  const finish = async (h: Held, verb: "COMMIT" | "ROLLBACK") => { open.delete(h); try { await h.client.query(verb); } finally { await release(h.client); } };
  afterEach(async () => {
    const left = [...open]; open.clear();
    for (const h of left) await pool.query("SELECT pg_cancel_backend($1)", [h.pid]).catch(() => {});
    for (const h of left) { await h.done; await release(h.client); }
  });
  const settled = async (p: Promise<unknown>, ms: number) => Promise.race([p.then(() => true), Bun.sleep(ms).then(() => false)]);
  const waitFor = async (what: string, sql: string, params: unknown[]) => {
    const end = Date.now() + 5000;
    while (Date.now() < end) { if ((await pool.query(sql, params)).rows[0].ok) return; await Bun.sleep(10); }
    throw Error("expected_fixture_wait_missing:" + what);
  };
  // Recursive blocker ancestry: a waiter may queue behind another waiter (e.g. claim Z behind the queued allocator Y)
  // rather than directly behind the holder.
  const waitBlockedBy = (waiter: number, blocker: number) => waitFor("blocker",
    "WITH RECURSIVE a(pid) AS(SELECT unnest(pg_blocking_pids($1::int)) UNION SELECT unnest(pg_blocking_pids(a.pid)) FROM a) SELECT $2::int IN(SELECT pid FROM a) AS ok",
    [waiter, blocker]);
  const waitAdvisory = (pid: number) => waitFor("advisory", "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted) AS ok", [pid]);
  const slots = async (rid: string) => (await pool.query("SELECT current_owner FROM sessions.ingest_lazy_native_slots WHERE record_id=$1", [rid])).rows;
  const custodyRows = async (rid: string) => Number((await pool.query("SELECT count(*) n FROM sessions.ingest_custody_records WHERE record_id=$1", [rid])).rows[0].n);
  const lastSeat = async () => (await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock WHERE singleton")).rows[0].s as string;
  /** pg_advisory_xact_lock(bigint) as pg_locks shows it: classid = high 32 bits, objid = low 32 bits, objsubid = 1. */
  const originLock = async (origin: string) => (await pool.query(
    "SELECT ((k>>32)&4294967295)::text classid,(k&4294967295)::text objid FROM(SELECT hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',$1::text,$2::text,$3::text)::text,0) k)s",
    [TENANT.orgId, TENANT.workspaceId, origin])).rows[0] as { classid: string; objid: string };
  const advisory = async (pids: number[]) => (await pool.query(
    "SELECT pid,classid::text::bigint::text classid,objid::text::bigint::text objid,objsubid,mode,granted FROM pg_locks WHERE locktype='advisory' AND pid=ANY($1::int[]) ORDER BY pid,classid,objid",
    [pids])).rows as { pid: number; classid: string; objid: string; objsubid: number; mode: string; granted: boolean }[];
  const ids500 = (label: string) => Array.from({ length: 500 }, (_, i) => `${label}-${serial}-${i}`);
  const prosrc = async (sig: string) => (await pool.query("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [sig])).rows[0].prosrc as string;
  /** Fails AT the restore (not in a later test) if a reinstall left an older staged-ACK body in place. */
  const expectAck043800 = async () => {
    const ack = await prosrc("sessions.ingest_lazy_ack_batch(jsonb)");
    expect(ack).toContain(ACK_CLOCK_LINE);
    expect(ack).toContain(ACK_ORIGIN_LINE);
  };
  /** Restores the shipped bodies: 043500 (claim path and its ACK), then 043800 (the ACK) on top, as the migrator does. */
  const restoreShipped = async () => {
    await pool.query(text(migrationPath));
    await pool.query(text(ackMigrationPath));
    expect(await prosrc("sessions.ingest_lazy_records(jsonb)")).toContain("lazy-native-origin-v1");
    await expectAck043800();
  };

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(150000); // the pg-proof brief-publication matrix: 15, 16, 17
    name = "claim_clock_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    // 16 + admin stays within the constrained pg-proof server (max_connections=20, superuser connections).
    pool = new pg.Pool({ connectionString: target.href, max: 16 });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await connect(); try { await runMigrations(c, { profile }); } finally { await release(c); }
    const assets = await derivedSchemaAssets(); generation = assets.expected.generation;
    expect(assets.expected.names).toContain(migrationName);
    console.log(JSON.stringify({ proof: "claim-clock-lock-fixture", serverVersionNum: version, generation, migrationCount: assets.expected.names.length }));
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    // Commit the org's ingest_buffer_admission row once with a real completed delivery: a first claimant's uncommitted
    // admission insert would otherwise make every other claimant of the org wait on its xid (not the seat clock).
    await warm(tenant("org"));
  }, 180000);
  afterAll(async () => {
    for (const c of sessions) await release(c);
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  test("A1: while X holds an open claim, Y's claim from a DIFFERENT origin proceeds; with the 055000 bodies it queues on the clock (55P03 under lock_timeout)", async () => {
    const run = async () => {
      const x = tenant("a1x"), y = tenant("a1y");
      await warm(x, y);
      const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `a1x-${serial}`, [`a1-x-${serial}`], { origin: `a1-ox-${serial}` }));
      expect((await hx.done).ok).toBe(true);
      const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `a1y-${serial}`, [`a1-y-${serial}`], { origin: `a1-oy-${serial}` }), "SET LOCAL lock_timeout=750");
      const y1 = await hy.done;
      await finish(hy, "COMMIT"); await finish(hx, "COMMIT");
      return y1;
    };
    expect(await run()).toMatchObject({ ok: true });
    // Before the fix: the exact 055000 bodies take the singleton clock FOR UPDATE, so Y waits for X.
    // The restore must re-apply 043800 after 043500: the 043500 file also contains ingest_lazy_ack_batch (FOR UPDATE on
    // the clock, no origin lock), so re-applying it alone silently reverted the ACK for every later test (B1, C1, D1).
    for (const body of originalBodies(text("supabase/migrations/20261104055000_lazy_current_custody.sql"))) await pool.query(body);
    try { expect(await run()).toMatchObject({ ok: false, code: "55P03" }); }
    finally { await restoreShipped(); }
  }, 60000);

  test("A2: same owned key -- Y waits on the origin advisory lock, then gets P0W01 (never 23505/40001); one slot, one custody row", async () => {
    const x = tenant("a2x"), y = tenant("a2y"), rid = `a2-${serial}`, cur = await current(x, rid);
    await warm(x, y);
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `a2x-${serial}`, [rid], { current: cur }));
    expect((await hx.done).ok).toBe(true);
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `a2y-${serial}`, [rid], { current: cur }));
    await waitAdvisory(hy.pid); await waitBlockedBy(hy.pid, hx.pid);
    expect(await settled(hy.done, 200)).toBe(false);
    await finish(hx, "COMMIT");
    expect(await hy.done).toMatchObject({ ok: false, code: "P0W01" });
    await finish(hy, "ROLLBACK");
    expect(await slots(rid)).toEqual([{ current_owner: x.userId }]);
    expect(await custodyRows(rid)).toBe(1);
  }, 60000);

  // Pre-fix (055000) Y queued on the clock FOR UPDATE instead (55P03 under the 2s lock_timeout when loaded).
  test("A2b: same source-absent key -- Y waits on the origin advisory lock, then finds X's NULL-owned slot and CONTINUEs", async () => {
    const x = tenant("a2bx"), y = tenant("a2by"), rid = `a2b-${serial}`;
    await warm(x, y);
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `a2bx-${serial}`, [rid]));
    expect((await hx.done).ok).toBe(true);
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `a2by-${serial}`, [rid]));
    await waitAdvisory(hy.pid);
    await finish(hx, "COMMIT");
    expect(await hy.done).toMatchObject({ ok: true });
    await finish(hy, "COMMIT");
    expect(await slots(rid)).toEqual([{ current_owner: null }]);
  }, 60000);

  test("A3: X rolls back -- Y initializes the slot (owner NULL until a non-suppressed ACK); Y's staged ACK then owns it", async () => {
    const x = tenant("a3x"), y = tenant("a3y"), rid = `a3-${serial}`;
    await warm(x, y);
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `a3x-${serial}`, [rid]));
    expect((await hx.done).ok).toBe(true);
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `a3y-${serial}`, [rid]));
    await waitAdvisory(hy.pid);
    await finish(hx, "ROLLBACK");
    expect(await hy.done).toMatchObject({ ok: true });
    await finish(hy, "COMMIT");
    expect(await slots(rid)).toEqual([{ current_owner: null }]);
    expect((await deliver(y, `a3y2-${serial}`, [rid], { revision: 3 })).outcome).toBe("accepted_buffered");
    expect(await slots(rid)).toEqual([{ current_owner: y.userId }]);
  }, 60000);

  test("A3b: same origin, overlapping keys -- Y serializes behind X on the one origin lock; one slot per key, no double owner", async () => {
    const x = tenant("a3bx"), y = tenant("a3by"), origin = `a3b-o-${serial}`, k = [0, 1, 2].map((i) => `a3b-${serial}-${i}`);
    await warm(x, y);
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `a3bx-${serial}`, [k[0]!, k[1]!], { origin }));
    expect((await hx.done).ok).toBe(true);
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `a3by-${serial}`, [k[1]!, k[2]!], { origin }));
    await waitAdvisory(hy.pid); await waitBlockedBy(hy.pid, hx.pid);
    expect(await settled(hy.done, 200)).toBe(false);
    await finish(hx, "COMMIT");
    expect(await hy.done).toMatchObject({ ok: true });
    await finish(hy, "COMMIT");
    for (const rid of k) expect(await slots(rid)).toEqual([{ current_owner: null }]);
    // Staged ACKs then own: X its keys, Y only the key nobody owns (the shared key is X's: Y gets P0W01, 403).
    expect((await deliver(x, `a3bx2-${serial}`, [k[0]!, k[1]!], { origin, revision: 3 })).outcome).toBe("accepted_buffered");
    const y2 = await deliver(y, `a3by2-${serial}`, [k[1]!, k[2]!], { origin, revision: 3 }).then(() => null, (e: any) => e);
    expect(y2?.status).toBe(403);
    expect((await deliver(y, `a3by3-${serial}`, [k[2]!], { origin, revision: 3 })).outcome).toBe("accepted_buffered");
    expect([await slots(k[0]!), await slots(k[1]!), await slots(k[2]!)]).toEqual([[{ current_owner: x.userId }], [{ current_owner: x.userId }], [{ current_owner: y.userId }]]);
  }, 60000);

  test("F1: a 500-record claim holds exactly ONE advisory lock (the origin key); another origin never waits on it, the same origin does", async () => {
    const x = tenant("f1x"), y = tenant("f1y"), z = tenant("f1z"), origin = `f1-o-${serial}`;
    await warm(x, y, z);
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `f1x-${serial}`, ids500("f1x"), { origin }));
    expect(await hx.done).toMatchObject({ ok: true });
    const want = await originLock(origin);
    const locks = await advisory([hx.pid]);
    expect(locks.length).toBeLessThanOrEqual(1); // upper bound: the origin lock only (the fence is a row lock)
    expect(locks).toEqual([{ pid: hx.pid, classid: want.classid, objid: want.objid, objsubid: 1, mode: "ExclusiveLock", granted: true }]);
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `f1y-${serial}`, [`f1-y-${serial}`], { origin: `f1-other-${serial}` }));
    expect(await settled(hy.done, 2000)).toBe(true);
    expect(await hy.done).toMatchObject({ ok: true });
    const hz = await hold(CLAIM, await capture("ingest_claim_buffered_batch", z, `f1z-${serial}`, [`f1-z-${serial}`], { origin }));
    await waitAdvisory(hz.pid); await waitBlockedBy(hz.pid, hx.pid);
    expect(await settled(hz.done, 200)).toBe(false);
    await finish(hx, "COMMIT");
    expect(await hz.done).toMatchObject({ ok: true });
    await finish(hz, "COMMIT"); await finish(hy, "COMMIT");
  }, 120000);

  test("F2: K simultaneously held 500-record claims never raise 53200; total advisory locks <= K (causal when K*500 exceeds the measured lock capacity)", async () => {
    const show = async (n: string) => Number((await pool.query("SHOW " + n)).rows[0][n]);
    const names = ["max_locks_per_transaction", "max_connections", "max_prepared_transactions", "autovacuum_max_workers", "max_worker_processes", "max_wal_senders", "superuser_reserved_connections"];
    const cfg: Record<string, number> = {};
    for (const n of names) cfg[n] = await show(n);
    // Lock table sizing as PostgreSQL computes it (MaxBackends includes the reserves); recorded only -- the shared
    // hash has slack beyond it, so capacity is MEASURED below, never inferred from this number.
    const maxBackends = cfg.max_connections! + cfg.autovacuum_max_workers! + 1 + cfg.max_worker_processes! + cfg.max_wal_senders!;
    const nominalLockTable = cfg.max_locks_per_transaction! * (maxBackends + cfg.max_prepared_transactions!);
    // Measured capacity: advisory locks one transaction can take before 53200 (rolled back at once; nothing else
    // runs on this disposable fixture server meanwhile). Pre-fix, K claims of 500 keys took K*500 such entries.
    const maxHeld = 10, cap = (maxHeld + 1) * 500, chunk = 100;
    const probe = await connect(); let capacity = 0, probeCode = "none";
    try {
      await probe.query("BEGIN");
      for (; capacity < cap; capacity += chunk) {
        const e = await probe.query("SELECT count(pg_advisory_xact_lock(k)) FROM generate_series($1::bigint,$2::bigint) k", [-9e15 + capacity, -9e15 + capacity + chunk - 1]).then(() => null, (x: any) => x);
        if (e) { probeCode = String(e.code); break; }
      }
    } finally { await release(probe); }
    const constrained = probeCode === "53200";
    if (!constrained) expect(probeCode).toBe("none");
    const k = constrained ? Math.floor(capacity / 500) + 1 : 3;
    console.log(JSON.stringify({ proof: "claim-clock-lock-53200", ...cfg, maxBackends, nominalLockTable, measuredAdvisoryCapacity: constrained ? `<${capacity + chunk}` : `>=${cap}`,
      constrained, heldClaims: k, preFixAdvisoryFootprint: k * 500 }));
    if (process.env.CLAIM_CLOCK_LOCK_EXPECT_CONSTRAINED === "1") {
      // The constrained CI fixture must actually be constrained: fail instead of degrading to the unconstrained case.
      expect(constrained).toBe(true);
      if (k > maxHeld) throw new Error(`constrained server too large for the proof: measured advisory capacity <${capacity + chunk} needs ${k} held claims > ${maxHeld}`);
    }
    if (constrained) expect(k * 500).toBeGreaterThan(capacity + chunk - 1);
    expect(k).toBeLessThanOrEqual(maxHeld);
    const ts = Array.from({ length: k }, (_, i) => tenant(`f2-${i}`));
    await warm(...ts);
    const payloads: string[] = [];
    for (const [i, t] of ts.entries()) payloads.push(await capture("ingest_claim_buffered_batch", t, `f2-${i}-${serial}`, ids500(`f2-${i}`), { origin: `f2-o${i}-${serial}` }));
    const hs = await Promise.all(payloads.map((p) => hold(CLAIM, p)));
    const results = await Promise.all(hs.map((h) => h.done));
    // All K claims hold their transactions (and locks) open here, simultaneously.
    const locks = await advisory(hs.map((h) => h.pid));
    expect(results.map((r) => (r.ok ? "ok" : r.code))).toEqual(Array(k).fill("ok"));
    expect(locks.length).toBeLessThanOrEqual(k);
    expect(new Set(locks.map((l) => l.pid)).size).toBe(locks.length);
    for (const h of hs) await finish(h, "ROLLBACK");
  }, 300000);

  test("A4: 50 rounds of concurrent claims over the same two keys in reversed order never deadlock (no 40P01)", async () => {
    const codes: string[] = [];
    for (let i = 0; i < 50; i++) {
      const x = tenant("a4x"), y = tenant("a4y"), a = `a4-${serial}-a`, b = `a4-${serial}-b`;
      await warm(x, y);
      const [px, py] = await Promise.all([capture("ingest_claim_buffered_batch", x, `a4x-${serial}`, [a, b]), capture("ingest_claim_buffered_batch", y, `a4y-${serial}`, [b, a])]);
      const [hx, hy] = await Promise.all([hold(CLAIM, px), hold(CLAIM, py)]);
      const [rx, ry] = await Promise.all([hx.done.then(async (r) => { await finish(hx, "COMMIT"); return r; }), hy.done.then(async (r) => { await finish(hy, "COMMIT"); return r; })]);
      for (const r of [rx, ry]) codes.push(r.ok ? "ok" : r.code);
    }
    expect(codes.filter((c) => c !== "ok")).toEqual([]);
  }, 300000);

  test("B1: staged commits serialize on the clock with contiguous seats; a claim's KEY SHARE vs an allocator per the final lock mode", async () => {
    const x = tenant("b1x"), y = tenant("b1y"), z = tenant("b1z");
    await warm(x, y, z);
    const before = BigInt(await lastSeat());
    const xs = [`b1-x-${serial}-0`, `b1-x-${serial}-1`], ys = [`b1-y-${serial}-0`, `b1-y-${serial}-1`];
    const px = await capture("ingest_commit_buffered_batch", x, `b1x-${serial}`, xs, { origin: `b1-ox-${serial}` }), py = await capture("ingest_commit_buffered_batch", y, `b1y-${serial}`, ys, { origin: `b1-oy-${serial}` });
    const hx = await hold(COMMIT, px);
    expect((await hx.done).ok).toBe(true);
    const hy = await hold(COMMIT, py);
    await waitBlockedBy(hy.pid, hx.pid);
    // Claim Z: since 043800 the staged ACK holds the clock FOR NO KEY UPDATE, which does not conflict with Z's KEY
    // SHARE, so Z completes while X still holds the clock and Y is queued behind X (lock_timeout bounds a regression).
    // X, Y and Z use different origins because the 043800 ACK also holds the origin advisory lock, so a same-origin
    // claim would queue on it (that case is D1).
    await expectAck043800();
    const hz = await hold(CLAIM, await capture("ingest_claim_buffered_batch", z, `b1z-${serial}`, [`b1-z-${serial}`], { origin: `b1-oz-${serial}` }), "SET LOCAL lock_timeout=750");
    expect(await hz.done).toMatchObject({ ok: true }); await finish(hz, "COMMIT");
    expect(await settled(hy.done, 200)).toBe(false);
    await finish(hx, "COMMIT");
    expect(await hy.done).toMatchObject({ ok: true }); await finish(hy, "COMMIT");
    const rows = (await pool.query("SELECT seat::text,key_material->>7 rid FROM sessions.ingest_receipt_seats WHERE seat>$1 ORDER BY seat", [before.toString()])).rows as { seat: string; rid: string }[];
    expect(rows.map((r) => r.rid)).toEqual([...xs, ...ys]);
    rows.forEach((r, i) => expect(BigInt(r.seat)).toBe(before + BigInt(i + 1)));
    expect(await lastSeat()).toBe(rows.at(-1)!.seat);
  }, 60000);

  test("C1 (043800): a claim of another user and origin proceeds while a staged ACK holds the clock; with the 043500 ACK body restored it queues (55P03 under lock_timeout)", async () => {
    const run = async () => {
      const x = tenant("c1x"), y = tenant("c1y");
      await warm(x, y);
      const hx = await hold(COMMIT, await capture("ingest_commit_buffered_batch", x, `c1x-${serial}`, [`c1-x-${serial}`], { origin: `c1-ox-${serial}` }));
      expect((await hx.done).ok).toBe(true); // X's staged ACK ran inside this still-open transaction: it holds the clock
      const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `c1y-${serial}`, [`c1-y-${serial}`], { origin: `c1-oy-${serial}` }), "SET LOCAL lock_timeout=750");
      const y1 = await hy.done;
      await finish(hy, y1.ok ? "COMMIT" : "ROLLBACK"); await finish(hx, "COMMIT");
      return y1;
    };
    expect(await run()).toMatchObject({ ok: true });
    // Before the fix: the exact 043500 ACK body takes the clock FOR UPDATE, which conflicts with the claim's KEY SHARE.
    const fixed = text(ackMigrationPath);
    for (const body of ackOriginalBodies(text(migrationPath))) await pool.query(body);
    try { expect(await run()).toMatchObject({ ok: false, code: "55P03" }); }
    finally { await pool.query(fixed); }
    await expectAck043800();
  }, 60000);

  // D1 (043800): reviewer P1. Same org/workspace/origin, two users (the per-user fence does not order them), two
  // initialized NULL-owned native slots A and B. Without the ACK's origin lock: the ACK (U1, [A,B]) queues on A, the
  // claim (U2, [B,A]) passes the clock (KEY SHARE vs NO KEY UPDATE) and the free origin lock and queues on B; on release
  // the claim gets B, the ACK gets A, each then waits for the other -> 40P01. With it: the claim queues on the ORIGIN
  // advisory lock the ACK holds, never reaches B, and runs after the ACK's transaction ends.
  // Negative shape (043500 ACK body: clock FOR UPDATE, no origin lock): the schedule cannot arise there because the
  // claim's KEY SHARE stops at the clock the ACK holds FOR UPDATE (C1 proves that 55P03); pinned below on the bodies.
  test("D1 (043800): an ACK [A,B] and a same-origin claim [B,A] of another user never deadlock; the claim queues on the ACK's origin lock", async () => {
    const shipped = await prosrc("sessions.ingest_lazy_ack_batch(jsonb)");
    await expectAck043800();
    const at = (needle: string) => { const i = shipped.indexOf(needle); expect(i).toBeGreaterThan(-1); return i; };
    // fence -> clock -> origin -> stage row -> slot, in the installed body
    expect(at("ingest_lock_fences(")).toBeLessThan(at(ACK_CLOCK_LINE));
    expect(at(ACK_CLOCK_LINE)).toBeLessThan(at(ACK_ORIGIN_LINE));
    expect(at(ACK_ORIGIN_LINE)).toBeLessThan(at("FOR UPDATE OF i;"));
    expect(at(ACK_ORIGIN_LINE)).toBeLessThan(at("FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;"));
    const base = ackOriginalBodies(text(migrationPath))[0]!;
    expect(base).toContain("WHERE singleton FOR UPDATE;v_last:=c.last_seat;");
    expect(base).not.toContain("lazy-native-origin-v1");
    const slotLock = "SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=$1 AND workspace_id=$2 AND origin_id=$3 AND record_id=$4 FOR UPDATE";
    for (const ackEnd of ["COMMIT", "ROLLBACK"] as const) {
      const u1 = tenant("d1u1"), u2 = tenant("d1u2"), origin = `d1-o-${serial}`, a = `d1-${serial}-a`, b = `d1-${serial}-b`;
      await warm(u1, u2);
      // The real path commits U1's claim (initializing A and B, owner NULL) and stage, and stops at its staged commit.
      const pAck = await capture("ingest_commit_buffered_batch", u1, `d1u1-${serial}`, [a, b], { origin });
      const pClaim = await capture("ingest_claim_buffered_batch", u2, `d1u2-${serial}`, [b, a], { origin });
      for (const rid of [a, b]) expect(await slots(rid)).toEqual([{ current_owner: null }]);
      const blockers: PoolClient[] = [];
      try {
        for (const rid of [a, b]) {
          const c = await connect(); blockers.push(c);
          await c.query("BEGIN");
          expect((await c.query(slotLock, [u1.orgId, u1.workspaceId, origin, rid])).rowCount).toBe(1);
        }
        const [bA, bB] = blockers as [PoolClient, PoolClient];
        const pidOf = async (c: PoolClient) => (await c.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
        const [pidA, pidB] = [await pidOf(bA), await pidOf(bB)];
        const hAck = await hold(COMMIT, pAck);
        await waitBlockedBy(hAck.pid, pidA); // the ACK is at slot A (its first rank) and already holds the origin lock
        const want = await originLock(origin);
        expect(await advisory([hAck.pid])).toEqual([{ pid: hAck.pid, classid: want.classid, objid: want.objid, objsubid: 1, mode: "ExclusiveLock", granted: true }]);
        const hClaim = await hold(CLAIM, pClaim);
        await waitAdvisory(hClaim.pid);
        // Directly blocked by the ACK on the origin lock only; never by B's holder.
        expect((await pool.query("SELECT pg_blocking_pids($1::int) AS p", [hClaim.pid])).rows[0].p).toEqual([hAck.pid]);
        blockers.shift(); await release(bA); // release A
        await waitBlockedBy(hAck.pid, pidB); // the ACK moved on to B
        expect(await settled(hClaim.done, 200)).toBe(false);
        blockers.shift(); await release(bB); // release B
        const ack = await hAck.done;
        expect(ack).toMatchObject({ ok: true });
        // The ACK statement is done but its transaction (and origin lock) is still open: the claim still waits.
        expect(await settled(hClaim.done, 200)).toBe(false);
        await finish(hAck, ackEnd);
        const claim = await hClaim.done;
        // COMMIT: U1 now owns A and B, so U2's claim takes the ownership path (P0W01, 403), never 40P01.
        // ROLLBACK: A and B stay NULL-owned, so U2's claim completes normally.
        expect(claim).toMatchObject(ackEnd === "COMMIT" ? { ok: false, code: "P0W01" } : { ok: true });
        await finish(hClaim, claim.ok ? "COMMIT" : "ROLLBACK");
        for (const r of [ack, claim]) if (!r.ok) expect(r.code).not.toBe("40P01");
        const owner = ackEnd === "COMMIT" ? u1.userId : null;
        for (const rid of [a, b]) expect(await slots(rid)).toEqual([{ current_owner: owner }]);
      } finally { for (const c of blockers) await release(c); }
    }
  }, 120000);

  // D1n (e79 negative): the causal counterpart of D1. The intermediate e79ee9f4 ACK (043800 text minus the origin lock:
  // clock FOR NO KEY UPDATE, then stage row and slots) under D1's schedule. The claim passes the clock (KEY SHARE vs NO
  // KEY UPDATE) and the free origin lock and queues on B (holding B's tuple lock) BEFORE A is released; the ACK then
  // takes A and queues on B behind the claim. Releasing B grants B to the claim, which then requests A (held by the
  // ACK) while the ACK waits on the claim's xid for B: a cycle, and the detector aborts exactly one with 40P01.
  test("D1n (e79 negative): with the clock at NO KEY UPDATE but no origin lock in the ACK, the reversed-two-slot schedule deadlocks (40P01)", async () => {
    const shipped = text(ackMigrationPath);
    expect(shipped.split(ACK_ORIGIN_LINE).length - 1).toBe(1);
    const e79 = shipped.replace(ACK_ORIGIN_LINE, "");
    expect(e79).toContain(ACK_CLOCK_LINE);
    expect(e79).not.toContain(ACK_ORIGIN_LINE);
    const slotLock = "SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=$1 AND workspace_id=$2 AND origin_id=$3 AND record_id=$4 FOR UPDATE";
    const fast = "SET LOCAL statement_timeout=20000;SET LOCAL deadlock_timeout='200ms'"; // superuser GUC: set before SET ROLE
    const u1 = tenant("d1nu1"), u2 = tenant("d1nu2"), origin = `d1n-o-${serial}`, a = `d1n-${serial}-a`, b = `d1n-${serial}-b`;
    await warm(u1, u2);
    // Captured under the shipped bodies: U1's claim (initializing A and B, owner NULL) and stage are committed.
    const pAck = await capture("ingest_commit_buffered_batch", u1, `d1nu1-${serial}`, [a, b], { origin });
    const pClaim = await capture("ingest_claim_buffered_batch", u2, `d1nu2-${serial}`, [b, a], { origin });
    for (const rid of [a, b]) expect(await slots(rid)).toEqual([{ current_owner: null }]);
    const blockers: PoolClient[] = [];
    try {
      await pool.query(e79);
      const ackSrc = await prosrc("sessions.ingest_lazy_ack_batch(jsonb)");
      expect(ackSrc).toContain(ACK_CLOCK_LINE);
      expect(ackSrc).not.toContain("lazy-native-origin-v1");
      expect(await prosrc("sessions.ingest_lazy_records(jsonb)")).toContain("lazy-native-origin-v1"); // the claim still takes it
      for (const rid of [a, b]) {
        const c = await connect(); blockers.push(c);
        await c.query("BEGIN");
        expect((await c.query(slotLock, [u1.orgId, u1.workspaceId, origin, rid])).rowCount).toBe(1);
      }
      const [bA, bB] = blockers as [PoolClient, PoolClient];
      const pidOf = async (c: PoolClient) => (await c.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
      const [pidA, pidB] = [await pidOf(bA), await pidOf(bB)];
      const hAck = await hold(COMMIT, pAck, fast);
      await waitBlockedBy(hAck.pid, pidA); // the ACK is at slot A (its first rank)
      expect(await advisory([hAck.pid])).toEqual([]); // e79: no origin lock
      const hClaim = await hold(CLAIM, pClaim, fast);
      await waitBlockedBy(hClaim.pid, pidB); // passed the clock and the free origin lock; queued on B first
      expect((await pool.query("SELECT pg_blocking_pids($1::int) AS p", [hClaim.pid])).rows[0].p).toEqual([pidB]);
      const want = await originLock(origin);
      expect(await advisory([hClaim.pid])).toEqual([{ pid: hClaim.pid, classid: want.classid, objid: want.objid, objsubid: 1, mode: "ExclusiveLock", granted: true }]);
      blockers.shift(); await release(bA); // release A: the ACK takes A, then queues on B behind the claim
      await waitBlockedBy(hAck.pid, hClaim.pid);
      await waitBlockedBy(hAck.pid, pidB);
      expect(await settled(hClaim.done, 200)).toBe(false);
      blockers.shift(); await release(bB); // release B: granted to the claim, which then waits on A held by the ACK
      const [ack, claim] = await Promise.all([hAck.done, hClaim.done]);
      // Exactly one victim; the cycle is then broken and the survivor's statement completes.
      expect([ack, claim].filter((r) => !r.ok).map((r) => (r.ok ? "ok" : r.code))).toEqual(["40P01"]);
      await finish(hAck, ack.ok ? "COMMIT" : "ROLLBACK");
      await finish(hClaim, claim.ok ? "COMMIT" : "ROLLBACK");
      const owner = ack.ok ? u1.userId : null;
      for (const rid of [a, b]) expect(await slots(rid)).toEqual([{ current_owner: owner }]);
      for (const rid of [a, b]) for (const s of await slots(rid)) expect(s.current_owner).not.toBe(u2.userId);
    } finally {
      for (const c of blockers) await release(c);
      await restoreShipped(); // 043500 then 043800: no later test sees the e79 body
    }
    await expectAck043800();
  }, 60000);

  test("C2 (043800): a FOR SHARE or FOR UPDATE holder of the clock (custody/GC/policy, quiesce) still excludes a staged ACK (55P03 under lock_timeout)", async () => {
    for (const mode of ["FOR SHARE", "FOR UPDATE"]) {
      const x = tenant("c2x");
      await warm(x);
      // Capture first: the capture's own claim reads the clock under KEY SHARE, which a FOR UPDATE holder would block.
      const px = await capture("ingest_commit_buffered_batch", x, `c2x-${serial}`, [`c2-x-${serial}`]);
      const before = await lastSeat();
      const holder = await connect();
      try {
        await holder.query("BEGIN");
        await holder.query(`SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton ${mode}`);
        const hx = await hold(COMMIT, px, "SET LOCAL lock_timeout=750");
        expect(await hx.done).toMatchObject({ ok: false, code: "55P03" });
        await finish(hx, "ROLLBACK");
      } finally { await release(holder); }
      expect(await lastSeat()).toBe(before);
    }
  }, 60000);

  test("B2: two suppressed claims from different origins run concurrently (the second never waits for the first), no 40P01, last_seat unchanged", async () => {
    const x = tenant("b2x"), y = tenant("b2y");
    await warm(x, y);
    const before = await lastSeat();
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `b2x-${serial}`, [`b2-x-${serial}`], { fenced: true, origin: `b2-ox-${serial}` }));
    expect(await hx.done).toMatchObject({ ok: true });
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `b2y-${serial}`, [`b2-y-${serial}`], { fenced: true, origin: `b2-oy-${serial}` }));
    expect(await settled(hy.done, 2000)).toBe(true);
    expect(await hy.done).toMatchObject({ ok: true });
    await finish(hy, "COMMIT"); await finish(hx, "COMMIT");
    expect((await pool.query("SELECT buffer_receipt_kind k,state FROM sessions.ingest_batches WHERE batch_id IN($1,$2) ORDER BY batch_id", [`b2x-${serial}`, `b2y-${serial}`])).rows)
      .toEqual([{ k: "suppressed", state: "committed" }, { k: "suppressed", state: "committed" }]);
    // Concurrent fire, both directions, repeated.
    expect(await lastSeat()).toBe(before);
    const codes: string[] = [], seats: string[] = [];
    for (let i = 0; i < 20; i++) {
      const a = tenant("b2a"), b = tenant("b2b");
      await warm(a, b);
      const round = await lastSeat();
      const [pa, pb] = await Promise.all([capture("ingest_claim_buffered_batch", a, `b2a-${serial}`, [`b2-a-${serial}`], { fenced: true, origin: `b2-oa-${serial}` }), capture("ingest_claim_buffered_batch", b, `b2b-${serial}`, [`b2-b-${serial}`], { fenced: true, origin: `b2-ob-${serial}` })]);
      const hs = await Promise.all([hold(CLAIM, pa), hold(CLAIM, pb)]);
      for (const r of await Promise.all(hs.map((h) => h.done.then(async (r) => { await finish(h, "COMMIT"); return r; })))) codes.push(r.ok ? "ok" : r.code);
      if (await lastSeat() !== round) seats.push(round);
    }
    expect(codes.filter((c) => c !== "ok")).toEqual([]);
    expect(seats).toEqual([]);
  }, 120000);

  test("U4: a buffered claim never commits a non-suppressed receipt inside the claim transaction", async () => {
    const x = tenant("u4");
    await warm(x);
    const staged = `u4s-${serial}`, suppressed = `u4p-${serial}`;
    const row = async (id: string) => (await pool.query("SELECT state,buffer_receipt_kind k FROM sessions.ingest_batches WHERE batch_id=$1", [id])).rows;
    const h = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, staged, [`u4-s-${serial}`]));
    expect((await h.done).ok).toBe(true); await finish(h, "COMMIT");
    expect(await row(staged)).toEqual([{ state: "pending", k: null }]);
    const hp = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, suppressed, [`u4-p-${serial}`], { fenced: true }));
    expect((await hp.done).ok).toBe(true); await finish(hp, "COMMIT");
    expect(await row(suppressed)).toEqual([{ state: "committed", k: "suppressed" }]);
  }, 60000);

  test("2c: the suppressed ACK path writes no tuple (nothing for history tracking / retire-fence triggers) and allocates no seat", async () => {
    const x = tenant("c2"), id = `c2-${serial}`;
    await warm(x);
    const h = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, id, [`c2-${serial}`], { fenced: true }));
    expect((await h.done).ok).toBe(true); await finish(h, "COMMIT");
    const before = await lastSeat();
    const c = await connect();
    try {
      await c.query("BEGIN");
      const writes = "SELECT coalesce(sum(n_tup_ins+n_tup_upd+n_tup_del),0)::bigint::text n FROM pg_stat_xact_user_tables";
      const w0 = (await c.query(writes)).rows[0].n;
      await c.query("SELECT sessions.ingest_lazy_ack_batch(to_jsonb(b)) FROM sessions.ingest_batches b WHERE batch_id=$1", [id]);
      expect((await c.query(writes)).rows[0].n).toBe(w0);
      expect((await c.query("SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' AND relation='sessions.ingest_receipt_seat_clock'::regclass ORDER BY mode")).rows.map((r) => r.mode))
        .toEqual(["AccessShareLock", "RowShareLock"]); // plain read (lazy_guard) + the KEY SHARE row lock; nothing stronger
      await c.query("CREATE TEMP TABLE __probe(x int);INSERT INTO __probe VALUES(1)");
      expect(BigInt((await c.query(writes)).rows[0].n)).toBeGreaterThan(BigInt(w0));
    } finally { await release(c); }
    expect(await lastSeat()).toBe(before);
  }, 60000);
});
