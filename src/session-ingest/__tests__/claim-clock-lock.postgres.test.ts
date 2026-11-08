/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): two/three-connection proof of
 * 20261105043500_lazy_claim_clock_narrowing. Claims read the singleton seat clock under KEY SHARE and serialize
 * per native slot key on a sorted transaction advisory lock, so a claim no longer queues behind another user's
 * claim; same-key racers serialize and the loser takes the P0W01 ownership path, never 23505/40001.
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
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const text = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const CLAIM = "SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS result";
const COMMIT = "SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS result";
type Tenant = typeof TENANT;
type Opts = { current?: SourceCurrentRecord | null; fenced?: boolean; revision?: number };
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
    return batch(ids.map((rid) => record({ record_id: rid, revision: opts.revision ?? 2, session_id: rid + "-s" })), { batch_id: id, account_id: await deliveryAccount(t) });
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
  const waitBlockedBy = (waiter: number, blocker: number) => waitFor("blocker", "SELECT $2::int=ANY(pg_blocking_pids($1)) AS ok", [waiter, blocker]);
  const waitAdvisory = (pid: number) => waitFor("advisory", "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted) AS ok", [pid]);
  const slots = async (rid: string) => (await pool.query("SELECT current_owner FROM sessions.ingest_lazy_native_slots WHERE record_id=$1", [rid])).rows;
  const custodyRows = async (rid: string) => Number((await pool.query("SELECT count(*) n FROM sessions.ingest_custody_records WHERE record_id=$1", [rid])).rows[0].n);
  const lastSeat = async () => (await pool.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock WHERE singleton")).rows[0].s as string;
  const ackClockMode = async () => /WHERE singleton FOR NO KEY UPDATE;/.test((await pool.query("SELECT prosrc FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure")).rows[0].prosrc) ? "no key update" : "update";

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(150000); // the pg-proof brief-publication matrix: 15, 16, 17
    name = "claim_clock_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 12 });
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
  }, 180000);
  afterAll(async () => {
    for (const c of sessions) await release(c);
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  test("A1: while X holds an open claim, Y's claim of a different key proceeds; with the 055000 bodies it queues on the clock (55P03 under lock_timeout)", async () => {
    const run = async () => {
      const x = tenant("a1x"), y = tenant("a1y");
      await warm(x, y);
      const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `a1x-${serial}`, [`a1-x-${serial}`]));
      expect((await hx.done).ok).toBe(true);
      const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `a1y-${serial}`, [`a1-y-${serial}`]), "SET LOCAL lock_timeout=750");
      const y1 = await hy.done;
      await finish(hy, "COMMIT"); await finish(hx, "COMMIT");
      return y1;
    };
    expect(await run()).toMatchObject({ ok: true });
    // Before the fix: the exact 055000 bodies take the singleton clock FOR UPDATE, so Y waits for X.
    const fixed = text(migrationPath);
    for (const body of originalBodies(text("supabase/migrations/20261104055000_lazy_current_custody.sql"))) await pool.query(body);
    try { expect(await run()).toMatchObject({ ok: false, code: "55P03" }); }
    finally { await pool.query(fixed); }
    expect((await pool.query("SELECT prosrc FROM pg_proc WHERE oid='sessions.ingest_lazy_records(jsonb)'::regprocedure")).rows[0].prosrc).toContain("lazy-native-slot-v1");
  }, 60000);

  test("A2: same owned key -- Y waits on the per-key advisory lock, then gets P0W01 (never 23505/40001); one slot, one custody row", async () => {
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

  test("A2b: same source-absent key -- Y waits, then finds X's NULL-owned slot and CONTINUEs (pre-fix race: 23505)", async () => {
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
    const px = await capture("ingest_commit_buffered_batch", x, `b1x-${serial}`, xs), py = await capture("ingest_commit_buffered_batch", y, `b1y-${serial}`, ys);
    const hx = await hold(COMMIT, px);
    expect((await hx.done).ok).toBe(true);
    const hy = await hold(COMMIT, py);
    await waitBlockedBy(hy.pid, hx.pid);
    // Claim Z: KEY SHARE does not conflict with NO KEY UPDATE (2d) but does with FOR UPDATE (e1 only).
    const hz = await hold(CLAIM, await capture("ingest_claim_buffered_batch", z, `b1z-${serial}`, [`b1-z-${serial}`]));
    if ((await ackClockMode()) === "no key update") expect(await settled(hz.done, 2000)).toBe(true);
    else { await waitBlockedBy(hz.pid, hx.pid); expect(await settled(hz.done, 200)).toBe(false); }
    await finish(hx, "COMMIT");
    expect(await hy.done).toMatchObject({ ok: true }); await finish(hy, "COMMIT");
    expect(await hz.done).toMatchObject({ ok: true }); await finish(hz, "COMMIT");
    const rows = (await pool.query("SELECT seat::text,key_material->>7 rid FROM sessions.ingest_receipt_seats WHERE seat>$1 ORDER BY seat", [before.toString()])).rows as { seat: string; rid: string }[];
    expect(rows.map((r) => r.rid)).toEqual([...xs, ...ys]);
    rows.forEach((r, i) => expect(BigInt(r.seat)).toBe(before + BigInt(i + 1)));
    expect(await lastSeat()).toBe(rows.at(-1)!.seat);
  }, 60000);

  test("B2: two suppressed claims run concurrently (the second never waits for the first), no 40P01, last_seat unchanged", async () => {
    const x = tenant("b2x"), y = tenant("b2y");
    await warm(x, y);
    const before = await lastSeat();
    const hx = await hold(CLAIM, await capture("ingest_claim_buffered_batch", x, `b2x-${serial}`, [`b2-x-${serial}`], { fenced: true }));
    expect(await hx.done).toMatchObject({ ok: true });
    const hy = await hold(CLAIM, await capture("ingest_claim_buffered_batch", y, `b2y-${serial}`, [`b2-y-${serial}`], { fenced: true }));
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
      const [pa, pb] = await Promise.all([capture("ingest_claim_buffered_batch", a, `b2a-${serial}`, [`b2-a-${serial}`], { fenced: true }), capture("ingest_claim_buffered_batch", b, `b2b-${serial}`, [`b2-b-${serial}`], { fenced: true })]);
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
      expect((await c.query("SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' AND relation='sessions.ingest_receipt_seat_clock'::regclass")).rows.map((r) => r.mode)).toEqual(["RowShareLock"]);
      await c.query("CREATE TEMP TABLE __probe(x int);INSERT INTO __probe VALUES(1)");
      expect(BigInt((await c.query(writes)).rows[0].n)).toBeGreaterThan(BigInt(w0));
    } finally { await release(c); }
    expect(await lastSeat()).toBe(before);
  }, 60000);
});
