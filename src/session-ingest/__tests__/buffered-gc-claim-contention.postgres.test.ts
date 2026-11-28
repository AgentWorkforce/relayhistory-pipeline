/**
 * Opt-in OWNED PostgreSQL 15-17 (SESSION_INGEST_TEST_PG_URL, loopback fixture only): the buffered GC claim under a live
 * staged ACK (20261105062370_buffered_gc_claim_clock_nowait).
 *
 * The production shape: a staged ACK (the Worker's buffered commit) holds the singleton seat clock FOR NO KEY UPDATE
 * until its transaction ends. The 058010 claim locks the candidate user's fence, batch, intent and outbox NOWAIT, then
 * its retention decision (ingest_buffer_gc_ordinals) waits for the clock FOR SHARE with those locks held. So the claim
 * hits the GC job's 1800 ms per-operation bound (sql_57014), and while it waits a commit of the candidate user waits on
 * the claim, and a later ACK of a third user queues on the clock behind it.
 * With 062370 the claim takes the clock FOR SHARE NOWAIT inside its acquisition subtransaction: it skips, returns at
 * once without a rotation, and blocks nobody; once the ACK ends the next claim rotates the candidate.
 * Claim and commit payloads come from the real deliverBatch -> sqlLedger path and are replayed on held connections;
 * the GC claim runs through sqlBufferedGcLedger on the job-shaped bounded executor and runtime-role pool.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { sqlBufferedGcLedger } from "../buffered-gc.ts";
import { SemanticPostgresExecutor } from "../../search-features/semantic-copy-postgres.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const epoch = "3".repeat(64), identity = "c".repeat(64);
const OPERATION_MS = 1800;
const MIGRATION = "supabase/migrations/20261105062370_buffered_gc_claim_clock_nowait.sql";
const MIGRATION_NAME = "upstream:20261105062370_buffered_gc_claim_clock_nowait.sql";
const ROLLBACK = "supabase/rollbacks/20261105062370_buffered_gc_claim_clock_nowait.down.sql";
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const text = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const claimBody = (sql: string) => {
  const m = sql.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_buffer_gc_claim\(p jsonb\)[\s\S]*?END \$claim\$;/);
  if (!m) throw Error("fixture_claim_definition_missing"); return m[0];
};
const BEFORE = claimBody(text("supabase/migrations/20261105058010_operator_user_erase.sql"));
const AFTER = claimBody(text(MIGRATION));
const CLOCK_STEP = "PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE NOWAIT;";
const COMMIT = "SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result";
type Tenant = typeof TENANT;
type Outcome = { ok: true; result: any } | { ok: false; code: string; message: string };
type Held = { client: PoolClient; pid: number; done: Promise<Outcome> };

describe.skipIf(!url)("062370 buffered GC claim vs a live staged ACK (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, gcPool: pg.Pool, name: string, generation: string, GC_PID: number;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const sessions = new Set<PoolClient>();
  const open = new Set<Held>();
  const connect = async () => { const c = await pool.connect(); sessions.add(c); return c; };
  const release = async (c: PoolClient) => { await c.query("ROLLBACK").catch(() => {}); sessions.delete(c); c.release(); };
  const tenant = (label: string): Tenant => ({ ...TENANT, userId: `gcc-${label}-${++serial}` });
  const ledgerFor = (intercept?: (sql: string, params: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    finally { await release(c); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
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
  const raw = async (t: Tenant, id: string, n: number) =>
    batch(Array.from({ length: n }, (_, i) => record({ record_id: `${id}-${i}`, revision: 2, session_id: `${id}-${i}-s` })), { batch_id: id, account_id: await deliveryAccount(t) });
  async function deliver(t: Tenant, id: string, n = 1) {
    const r = await raw(t, id, n);
    const ack = await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r);
    expect(ack.outcome).toBe("accepted_buffered");
  }
  /** The exact commit payload the real path sends; claim and stage ran and committed, the commit did not run. */
  async function captureCommit(t: Tenant, id: string) {
    let payload: string | undefined;
    const r = await raw(t, id, 1);
    const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r).catch(() => {});
    if (typeof payload !== "string") throw Error("payload_not_captured");
    return payload;
  }
  async function hold(sql: string, payload: string): Promise<Held> {
    const client = await connect();
    const pid = (await client.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
    await client.query("BEGIN;SET LOCAL statement_timeout=20000;SET LOCAL ROLE bulls96_session_ingest");
    const held = { client, pid } as Held; open.add(held);
    held.done = client.query(sql, [payload]).then((r) => ({ ok: true as const, result: r.rows[0].result }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
    return held;
  }
  const finish = async (h: Held, verb: "COMMIT" | "ROLLBACK") => { open.delete(h); try { await h.client.query(verb); } finally { await release(h.client); } };
  afterEach(async () => {
    const left = [...open]; open.clear();
    for (const h of left) await pool.query("SELECT pg_cancel_backend($1)", [h.pid]).catch(() => {});
    for (const h of left) { await h.done; await release(h.client); }
    await pool.query(AFTER);
  });
  /** The job's exact bound: one operation, 1800 ms, runtime-role pool. */
  const gcClaim = async (limit = 1) => {
    const sql = new SemanticPostgresExecutor(gcPool), signal = new AbortController().signal;
    const ledger = sqlBufferedGcLedger({ query: (s, p) => sql.queryBounded(s, p, { signal, deadline: Date.now() + OPERATION_MS }) }, generation);
    const t0 = performance.now();
    try {
      const tickets = await ledger.claim(limit), ms = performance.now() - t0;
      expect(await gcBackend()).toBe(GC_PID); // the claim ran on the pinned backend (the pool's only connection)
      return { ok: true as const, tickets, ms };
    } catch (e: any) { return { ok: false as const, code: String(e.code), ms: performance.now() - t0 }; }
  };
  /** The GC pool has ONE connection that never idles out. The bounded executor discards it after a cancel (57014), so
   *  each round re-pins GC_PID right before its claim, and a successful claim checks it ran there. */
  const gcBackend = async () => (await gcPool.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
  const pinGc = async () => { GC_PID = await gcBackend(); };
  /** The claim's backend is waiting on a heavyweight lock (a row/tuple lock, an advisory lock or an xid). */
  const gcLockWait = async () => (await pool.query("SELECT wait_event_type='Lock' AS w FROM pg_stat_activity WHERE pid=$1", [GC_PID])).rows[0]?.w === true;
  /** Recursive blocker ancestry of `pid` (a waiter may queue behind another waiter, not the holder). */
  const blockedTransitivelyBy = async (pid: number, blocker: number) => (await pool.query(
    "WITH RECURSIVE a(pid) AS(SELECT unnest(pg_blocking_pids($1::int)) UNION SELECT unnest(pg_blocking_pids(a.pid)) FROM a) SELECT $2::int IN(SELECT pid FROM a) AS ok",
    [pid, blocker])).rows[0].ok as boolean;
  const waitFor = async (what: string, check: () => Promise<boolean>, ms = 1500) => {
    const end = Date.now() + ms;
    while (Date.now() < end) { if (await check()) return true; await Bun.sleep(5); }
    throw Error("expected_wait_missing:" + what);
  };
  /** Makes `t`'s candidate stage the ONLY row the claim's candidate query can select: every other stage is under a
   *  complete far-future fixture GC lease (the claim filters gc_lease_until<=now; the lease shape CHECK needs all three
   *  columns), so no other user's rows are ever locked. Only the publication/history claims read leases; none runs here. */
  const isolate = async (t: Tenant) => {
    await pool.query(`UPDATE sessions.ingest_stage_intents SET
      gc_lease_token=CASE WHEN user_id=$1 AND batch_id=$2 THEN NULL ELSE gen_random_uuid() END,
      gc_lease_until=CASE WHEN user_id=$1 AND batch_id=$2 THEN NULL ELSE '2999-01-01'::timestamptz END,
      gc_lease_version=CASE WHEN user_id=$1 AND batch_id=$2 THEN NULL ELSE greatest(row_version,1) END,
      gc_checked_at=CASE WHEN user_id=$1 AND batch_id=$2 THEN NULL ELSE gc_checked_at END,
      created_at=CASE WHEN user_id=$1 AND batch_id=$2 THEN '2000-01-01'::timestamptz ELSE created_at END`, [t.userId, `cand-${t.userId}`]);
    expect((await pool.query(`SELECT user_id,batch_id FROM sessions.ingest_stage_intents
      WHERE cleanup_state<>'redacted' AND (gc_lease_until IS NULL OR gc_lease_until<=clock_timestamp())`)).rows).toEqual([{ user_id: t.userId, batch_id: `cand-${t.userId}` }]);
  };
  const blockers = async (pid: number) => (await pool.query("SELECT pg_blocking_pids($1::int) AS b", [pid])).rows[0].b as number[];
  /** Samples `pid`'s blockers every ~10 ms for `ms`: per blocker pid, the milliseconds it was seen blocking. */
  const blockedBy = async (pid: number, ms: number) => {
    const seen = new Map<number, number>(), end = Date.now() + ms;
    let last = Date.now();
    while (Date.now() < end) {
      const b = await blockers(pid), now = Date.now();
      for (const x of b) seen.set(x, (seen.get(x) ?? 0) + (now - last));
      last = now; await Bun.sleep(10);
    }
    return seen;
  };
  const intent = async (t: Tenant) => (await pool.query(
    "SELECT stage_token,gc_checked_at,gc_lease_token FROM sessions.ingest_stage_intents WHERE user_id=$1 AND batch_id=$2", [t.userId, `cand-${t.userId}`])).rows[0];

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(150000); // the pg-proof matrix: 15, 16, 17
    name = "gc_claim_contention_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 12 });
    gcPool = new pg.Pool({ connectionString: target.href, max: 1, idleTimeoutMillis: 0, connectionTimeoutMillis: 1000, options: "-c role=bulls96_session_ingest" });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await connect(); try { await runMigrations(c, { profile }); } finally { await release(c); }
    const assets = await derivedSchemaAssets(); generation = assets.expected.generation;
    expect(assets.expected.names).toContain(MIGRATION_NAME);
    console.log(JSON.stringify({ proof: "gc-claim-contention-fixture", serverVersionNum: version, generation, migrationCount: assets.expected.names.length }));
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await deliver(tenant("org"), "warm-org");
  }, 180000);
  afterAll(async () => {
    for (const c of sessions) await release(c);
    await gcPool?.end(); await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  /** One round: candidate user A (a healthy acknowledged 50-record stage at the head of the GC queue), a held staged
   *  ACK of user B (holds the clock), then the GC claim, a commit of A and an ACK of user C while the claim runs. */
  async function round(body: string) {
    await pool.query(body);
    const a = tenant("a"), b = tenant("b"), c = tenant("c");
    await deliver(a, `cand-${a.userId}`, 50); await deliver(b, `warm-${b.userId}`); await deliver(c, `warm-${c.userId}`);
    const pa = await captureCommit(a, `live-${a.userId}`), pb = await captureCommit(b, `ack-${b.userId}`), pc = await captureCommit(c, `ack-${c.userId}`);
    await isolate(a); // after the captures: their new stages are not candidates either
    const hb = await hold(COMMIT, pb);
    expect(await hb.done).toMatchObject({ ok: true, result: { kind: "committed" } }); // B's staged ACK ran; its transaction holds the clock
    // The claim's own lock waits, sampled on its pinned backend for the whole call.
    await pinGc();
    let gcWaitSamples = 0, sampling = true;
    const sampler = (async () => { while (sampling) { if (await gcLockWait()) gcWaitSamples++; await Bun.sleep(5); } })();
    const claim = gcClaim(1).finally(() => { sampling = false; });
    // Live traffic while the claim runs: a commit of the candidate user and a staged ACK of a third user.
    const ha = await hold(COMMIT, pa), hc = await hold(COMMIT, pc);
    const settledFirst = await Promise.race([claim.then(() => true), Bun.sleep(0).then(() => false)]);
    // Direct instrumentation while the claim is still running (only the 058010 body is): it waits behind the ACK's
    // clock, the candidate user's commit waits on the claim, and the third user's ACK queues behind the claim.
    let stall: { gcWaitsBehindAck: boolean; candidateCommitWaitsOnGc: boolean; thirdUserAckQueuedBehindGc: boolean } | null = null;
    if (!settledFirst && body === BEFORE) stall = {
      gcWaitsBehindAck: await waitFor("gc behind ack", async () => (await gcLockWait()) && await blockedTransitivelyBy(GC_PID, hb.pid)),
      candidateCommitWaitsOnGc: await waitFor("candidate commit on gc", async () => (await blockers(ha.pid)).includes(GC_PID)),
      thirdUserAckQueuedBehindGc: await waitFor("third ack behind gc", async () => (await blockers(hc.pid)).includes(GC_PID)),
    };
    const [seenA, seenC] = await Promise.all([blockedBy(ha.pid, 600), blockedBy(hc.pid, 600)]);
    const gcResult = await claim; await sampler;
    // Milliseconds (sampled over 600 ms) that the claim's pinned backend blocked each live writer.
    const duringClaim = { gcLockWaitSamples: gcWaitSamples, candidateCommitBlockedByGcMs: seenA.get(GC_PID) ?? 0, thirdUserAckBlockedByGcMs: seenC.get(GC_PID) ?? 0, ...(stall ?? {}) };
    const candidate = await intent(a);
    await finish(hb, "COMMIT");
    // A and C then queue on the clock in either order: each commits as soon as its statement returns.
    const settle = async (h: Held) => { const r = await h.done; await finish(h, r.ok ? "COMMIT" : "ROLLBACK"); return r; };
    const [ra, rc] = await Promise.all([settle(ha), settle(hc)]);
    return { gcResult, duringClaim, candidate, a, ra, rc };
  }

  test("before (058010 body): the claim waits on the held clock with the candidate's locks held, stalls a commit of that user and queues a later ACK, and fails 57014 at the 1800 ms bound", async () => {
    const r = await round(BEFORE);
    console.log(JSON.stringify({ proof: "gc-claim-contention-before", ms: Math.round(r.gcResult.ms), result: r.gcResult.ok ? "ok" : r.gcResult.code, ...r.duringClaim }));
    expect(r.gcResult).toMatchObject({ ok: false, code: "sql_57014" });
    expect(r.gcResult.ms).toBeGreaterThanOrEqual(OPERATION_MS - 50);
    expect(r.duringClaim).toMatchObject({ gcWaitsBehindAck: true, candidateCommitWaitsOnGc: true, thirdUserAckQueuedBehindGc: true });
    expect(r.duringClaim.gcLockWaitSamples).toBeGreaterThan(0);
    expect(r.duringClaim.candidateCommitBlockedByGcMs).toBeGreaterThanOrEqual(400);
    expect(r.duringClaim.thirdUserAckBlockedByGcMs).toBeGreaterThanOrEqual(400);
    expect(r.candidate.gc_checked_at).toBeNull(); // the rotation rolled back: the next run starts at the same candidate
    expect(r.ra).toMatchObject({ ok: true, result: { kind: "committed" } });
    expect(r.rc).toMatchObject({ ok: true, result: { kind: "committed" } });
  }, 60000);

  test("after (062370): the claim skips at once without a rotation and blocks nobody; once the ACK ends the next claim rotates the candidate", async () => {
    const r = await round(AFTER);
    console.log(JSON.stringify({ proof: "gc-claim-contention-after", ms: Math.round(r.gcResult.ms), result: r.gcResult.ok ? "ok" : r.gcResult.code, ...r.duringClaim }));
    expect(r.gcResult).toMatchObject({ ok: true, tickets: [] });
    expect(r.gcResult.ms).toBeLessThan(OPERATION_MS / 2);
    // The claim never waits on a lock. It may hold the candidate user's own locks for its few milliseconds (NOWAIT,
    // released on the skip); it never touches the third user's rows (isolated) and never queues on the clock.
    expect(r.duringClaim.gcLockWaitSamples).toBe(0);
    expect(r.duringClaim.candidateCommitBlockedByGcMs).toBeLessThan(100);
    expect(r.duringClaim.thirdUserAckBlockedByGcMs).toBe(0);
    expect(r.candidate.gc_checked_at).toBeNull(); // skipped, not rotated
    expect(r.candidate.gc_lease_token).toBeNull();
    expect(r.ra).toMatchObject({ ok: true, result: { kind: "committed" } });
    expect(r.rc).toMatchObject({ ok: true, result: { kind: "committed" } });
    // Clock free: the same candidate is evaluated (healthy: rotated, no ticket).
    await pinGc(); const next = await gcClaim(1);
    expect(next).toMatchObject({ ok: true, tickets: [] });
    expect((await intent(r.a)).gc_checked_at).not.toBeNull();
  }, 60000);

  /** Latency round: as `round`, but the staged ACK commits RELEASE_MS after the claim starts. Reports the claim's
   *  duration and its seat-clock window (after the ACK released the clock, until the claim returned: the clock is held
   *  FOR SHARE from acquisition to the claim's commit), and the end-to-end latency of the two concurrent commits. */
  const RELEASE_MS = 300;
  async function latencyRound(body: string) {
    await pool.query(body);
    const a = tenant("la"), b = tenant("lb"), c = tenant("lc");
    await deliver(a, `cand-${a.userId}`, 50); await deliver(b, `warm-${b.userId}`); await deliver(c, `warm-${c.userId}`);
    const pa = await captureCommit(a, `live-${a.userId}`), pb = await captureCommit(b, `ack-${b.userId}`), pc = await captureCommit(c, `ack-${c.userId}`);
    await isolate(a);
    const hb = await hold(COMMIT, pb);
    expect(await hb.done).toMatchObject({ ok: true, result: { kind: "committed" } });
    await pinGc();
    const t0 = performance.now();
    const claim = gcClaim(1).then((r) => ({ ...r, end: performance.now() - t0 }));
    await Bun.sleep(20);
    const timed = async (h: Held) => { const r = await h.done; const ms = performance.now() - t0; await finish(h, r.ok ? "COMMIT" : "ROLLBACK"); return { r, ms }; };
    const ha = await hold(COMMIT, pa), hc = await hold(COMMIT, pc), issued = performance.now() - t0;
    const pending = [timed(ha), timed(hc)];
    await Bun.sleep(Math.max(0, RELEASE_MS - (performance.now() - t0)));
    await finish(hb, "COMMIT"); const released = performance.now() - t0;
    const [gc, ra, rc] = [await claim, ...(await Promise.all(pending))];
    expect(ra.r).toMatchObject({ ok: true, result: { kind: "committed" } });
    expect(rc.r).toMatchObject({ ok: true, result: { kind: "committed" } });
    return { claimMs: Math.round(gc.ms), claimResult: gc.ok ? "ok" : gc.code, claimClockWindowMs: Math.max(0, Math.round(gc.end - released)),
      ackReleasedAtMs: Math.round(released), candidateCommitLatencyMs: Math.round(ra.ms - issued), thirdUserAckLatencyMs: Math.round(rc.ms - issued),
      candidateRotated: (await intent(a)).gc_checked_at !== null };
  }

  test("latency: with the ACK released at 300 ms, 058010's claim waits for the clock and then holds it; 062370's never waits and the commits wait only for the ACK", async () => {
    const before = await latencyRound(BEFORE), after = await latencyRound(AFTER);
    console.log(JSON.stringify({ proof: "gc-claim-contention-latency", releaseMs: RELEASE_MS, before, after }));
    expect(before.claimResult).toBe("ok"); expect(before.claimMs).toBeGreaterThanOrEqual(RELEASE_MS - 30); expect(before.candidateRotated).toBe(true);
    expect(after.claimResult).toBe("ok"); expect(after.claimMs).toBeLessThan(RELEASE_MS / 2); expect(after.claimClockWindowMs).toBe(0);
    expect(after.candidateRotated).toBe(false); // skipped while the clock was held; a later claim rotates it (see above)
  }, 60000);

  test("after (062370): a clock held FOR SHARE by another reader does not make the claim skip", async () => {
    const a = tenant("s");
    await deliver(a, `cand-${a.userId}`, 5);
    await isolate(a);
    const reader = await connect();
    try {
      await reader.query("BEGIN"); await reader.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE");
      await pinGc(); expect(await gcClaim(1)).toMatchObject({ ok: true, tickets: [] });
      expect((await intent(a)).gc_checked_at).not.toBeNull();
    } finally { await release(reader); }
  }, 60000);

  test("062370 body is 058010's plus exactly the clock step; the rollback restores 058010's exact claim and the migrator re-applies 062370", async () => {
    expect(AFTER.split(CLOCK_STEP)).toHaveLength(2);
    const stripped = AFTER.replace(/\n {3}-- 062370:[^\n]*(\n {3}--[^\n]*)*\n {3}IF s\.cleanup_state<>'retired'[^\n]*\n {4}PERFORM 1 FROM sessions\.ingest_receipt_seat_clock WHERE singleton FOR SHARE NOWAIT;\n {3}END IF;/, "");
    expect(stripped).toBe(BEFORE);
    const prosrc = async () => (await pool.query("SELECT prosrc,prosecdef,proacl::text acl FROM pg_proc WHERE oid='sessions.ingest_buffer_gc_claim(jsonb)'::regprocedure")).rows[0];
    const applied = await prosrc();
    expect(applied.prosrc).toContain(CLOCK_STEP); expect(applied.prosecdef).toBe(true);
    await pool.query(text(ROLLBACK));
    const restored = await prosrc();
    expect(restored.prosrc).not.toContain(CLOCK_STEP); expect(restored.prosrc).toContain("operator_user_erasures");
    expect(restored.acl).toBe(applied.acl);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [MIGRATION_NAME])).rows[0].n).toBe(0);
    await expect(pool.query(text(ROLLBACK))).rejects.toMatchObject({ code: "55000" });
    const c = await connect(); try { await runMigrations(c, { profile }); } finally { await release(c); }
    expect((await prosrc()).prosrc).toContain(CLOCK_STEP);
  }, 120000);
});
