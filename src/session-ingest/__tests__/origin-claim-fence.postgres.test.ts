/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): multi-connection proof of
 * 20261105062010_ingest_origin_claim_fence (docs/design/per-origin-claim-fence.md).
 *   O1  two origins of ONE user claim concurrently through the origin-scoped entry point; the per-user entry point
 *       (flag off, unchanged) still queues the second machine (55P03): the production failure.
 *   O2  the same origin still serializes (origin advisory lock), then proceeds when the first claim commits.
 *   O3  a held origin-scoped COMMIT (staged ACK) does not block another origin's claim of the same user.
 *   X1  every per-user control path (ingest_lock_fences without a mark: FOR UPDATE) waits for an in-flight
 *       origin-scoped claim, and an in-flight control blocks the claim.
 *   X2  the operator user erase begin waits for an in-flight origin-scoped claim, an open erase blocks the claim, and
 *       after begin commits every origin of the user is refused RH041.
 *   D1  flag-on deliveries end to end (claim, stage, commit) for two origins, and no mark row is left behind.
 * Claim/commit payloads are captured from the real deliverBatch -> sqlLedger path and replayed on held connections.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
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
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const LEGACY_CLAIM = "SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS result";
const CLAIM = "SELECT sessions.ingest_origin_claim_buffered_batch($1::jsonb) AS result";
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
type Tenant = typeof TENANT;
type Held = { client: PoolClient; pid: number; done: Promise<{ ok: true; result: any } | { ok: false; code: string; message: string }> };

describe.skipIf(!url)("062010 per-origin claim fence (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string, generation: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const sessions = new Set<PoolClient>();
  const open = new Set<Held>();
  const connect = async () => { const c = await pool.connect(); sessions.add(c); return c; };
  const release = async (c: PoolClient) => { await c.query("ROLLBACK").catch(() => {}); sessions.delete(c); c.release(); };
  const tenant = (label: string): Tenant => ({ ...TENANT, userId: `origin-${label}-${++serial}` });
  const ledgerFor = (originClaimFence: boolean, intercept?: (sql: string, params: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    finally { await release(c); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence });

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
  async function raw(t: Tenant, id: string, ids: string[], origin: string) {
    return batch(ids.map((rid) => record({ record_id: rid, revision: 2, session_id: rid + "-s", origin_id: origin })), { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
  }
  async function deliver(t: Tenant, id: string, ids: string[], origin: string, originClaimFence = true) {
    const r = await raw(t, id, ids, origin);
    return deliverBatch({ ledger: ledgerFor(originClaimFence), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r);
  }
  /** Commits the user's fence owner / seed baseline / privacy seal (a first claim's uncommitted inserts make every other claimant wait on its xid). */
  async function warm(t: Tenant) {
    expect((await deliver(t, `warm-${t.userId}`, [`warm-${t.userId}`], `warm-o-${t.userId}`)).outcome).toBe("accepted_buffered");
  }
  /** The exact jsonb the real path sends to `fn` (per-user name: the flag-off ledger); the delivery stops there. */
  async function capture(fn: "ingest_claim_buffered_batch" | "ingest_commit_buffered_batch", t: Tenant, id: string, ids: string[], origin: string) {
    let payload: string | undefined;
    const r = await raw(t, id, ids, origin);
    const ledger = ledgerFor(false, (sql, params) => { if (sql.includes(fn)) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, r) } as never, t, r).catch(() => {});
    if (typeof payload !== "string") throw Error("payload_not_captured:" + fn);
    return payload;
  }
  /** Runs `sql` in an open transaction (runtime role unless `owner`) on its own connection; COMMIT/ROLLBACK is the caller's. */
  async function hold(sql: string, params: unknown[], settings = "SET LOCAL statement_timeout=20000", owner = false): Promise<Held> {
    const client = await connect();
    const pid = (await client.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
    await client.query(`BEGIN;${settings}${owner ? "" : ";SET LOCAL ROLE bulls96_session_ingest"}`);
    const held = { client, pid } as Held; open.add(held);
    held.done = client.query(sql, params as never).then((r) => ({ ok: true as const, result: r.rows[0]?.result }), (e: any) => ({ ok: false as const, code: String(e.code), message: String(e.message) }));
    return held;
  }
  const finish = async (h: Held, verb: "COMMIT" | "ROLLBACK") => { open.delete(h); try { await h.client.query(verb); } finally { await release(h.client); } };
  afterEach(async () => {
    const left = [...open]; open.clear();
    for (const h of left) await pool.query("SELECT pg_cancel_backend($1)", [h.pid]).catch(() => {});
    for (const h of left) { await h.done; await release(h.client); }
  });
  const settled = async (p: Promise<unknown>, ms: number) => Promise.race([p.then(() => true), Bun.sleep(ms).then(() => false)]);
  const waitBlockedBy = async (waiter: number, blocker: number) => {
    const end = Date.now() + 5000;
    while (Date.now() < end) {
      if ((await pool.query("WITH RECURSIVE a(pid) AS(SELECT unnest(pg_blocking_pids($1::int)) UNION SELECT unnest(pg_blocking_pids(a.pid)) FROM a) SELECT $2::int IN(SELECT pid FROM a) AS ok", [waiter, blocker])).rows[0].ok) return;
      await Bun.sleep(10);
    }
    throw Error("expected_fixture_wait_missing");
  };
  const marks = async () => Number((await pool.query("SELECT count(*) n FROM sessions.ingest_origin_claim_marks")).rows[0].n);
  const fenceRowLocks = async (pid: number) => (await pool.query(
    "SELECT l.mode FROM pg_locks l WHERE l.pid=$1 AND l.locktype='advisory' ORDER BY l.mode", [pid])).rows.map((r) => r.mode as string);

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(150000);
    name = "origin_fence_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 16 });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await connect(); try { await runMigrations(c, { profile }); } finally { await release(c); }
    const assets = await derivedSchemaAssets(); generation = assets.expected.generation;
    expect(assets.expected.names).toContain("upstream:20261105062010_ingest_origin_claim_fence.sql");
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await warm(tenant("org"));
  }, 180000);
  afterAll(async () => {
    for (const c of sessions) await release(c);
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  test("O1: two origins of one user claim concurrently through the origin-scoped entry point; the per-user one queues them (55P03)", async () => {
    const u = tenant("o1"); await warm(u);
    const run = async (sql: string) => {
      const a = await hold(sql, [await capture("ingest_claim_buffered_batch", u, `o1a-${serial}-${sql.length}`, [`o1-a-${serial}-${sql.length}`], `o1-machine-a`)]);
      expect((await a.done).ok).toBe(true);
      const b = await hold(sql, [await capture("ingest_claim_buffered_batch", u, `o1b-${serial}-${sql.length}`, [`o1-b-${serial}-${sql.length}`], `o1-machine-b`)], "SET LOCAL lock_timeout=750");
      const second = await b.done;
      await finish(b, second.ok ? "COMMIT" : "ROLLBACK"); await finish(a, "COMMIT");
      return second;
    };
    const fixed = await run(CLAIM);
    expect(fixed).toMatchObject({ ok: true });
    expect((fixed as any).result.kind).toBe("claimed");
    expect(await run(LEGACY_CLAIM)).toMatchObject({ ok: false, code: "55P03" });
    expect(await marks()).toBe(0);
  }, 60000);

  test("O2: the same origin of one user still serializes on the origin lock, then proceeds after the first commits", async () => {
    const u = tenant("o2"); await warm(u);
    const a = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o2a-${serial}`, [`o2-a-${serial}`], "o2-machine")]);
    expect((await a.done).ok).toBe(true);
    expect(await fenceRowLocks(a.pid)).toContain("ExclusiveLock"); // the origin advisory lock (and the shared erase gate)
    const b = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o2b-${serial}`, [`o2-b-${serial}`], "o2-machine")]);
    await waitBlockedBy(b.pid, a.pid);
    expect(await settled(b.done, 300)).toBe(false);
    await finish(a, "COMMIT");
    expect(await b.done).toMatchObject({ ok: true });
    await finish(b, "COMMIT");
    // And under a short lock wait the queued same-origin claim gives up instead (the 429 path), writing nothing.
    const c = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o2c-${serial}`, [`o2-c-${serial}`], "o2-machine")]);
    expect((await c.done).ok).toBe(true);
    const d = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o2d-${serial}`, [`o2-d-${serial}`], "o2-machine")], "SET LOCAL lock_timeout=300");
    expect(await d.done).toMatchObject({ ok: false, code: "55P03" });
    await finish(d, "ROLLBACK"); await finish(c, "COMMIT");
  }, 60000);

  test("O4: a brand-new user's first claims from two machines: the second waits for the first's seal adoption, then is admitted (never 'unsealed user')", async () => {
    const u = tenant("o4"); // not warmed: no fence row, seal or baseline yet
    const a = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o4a-${serial}`, [`o4-a-${serial}`], "o4-machine-a")]);
    expect((await a.done).ok).toBe(true);
    const b = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o4b-${serial}`, [`o4-b-${serial}`], "o4-machine-b")]);
    await waitBlockedBy(b.pid, a.pid);
    expect(await settled(b.done, 300)).toBe(false);
    await finish(a, "COMMIT");
    expect(await b.done).toMatchObject({ ok: true });
    await finish(b, "COMMIT");
    // Rolled-back first adoption: the waiter becomes the first and adopts the seal itself.
    const w = tenant("o4w");
    const c = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", w, `o4c-${serial}`, [`o4-c-${serial}`], "o4-machine-c")]);
    expect((await c.done).ok).toBe(true);
    const d = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", w, `o4d-${serial}`, [`o4-d-${serial}`], "o4-machine-d")]);
    await waitBlockedBy(d.pid, c.pid);
    await finish(c, "ROLLBACK");
    expect(await d.done).toMatchObject({ ok: true });
    await finish(d, "COMMIT");
    expect(Number((await pool.query("SELECT count(*) n FROM sessions.ingest_lazy_user_seals WHERE user_id=ANY($1)", [[u.userId, w.userId]])).rows[0].n)).toBe(2);
  }, 60000);

  test("O3: a held origin-scoped commit (staged ACK) of machine A does not block machine B's claim of the same user", async () => {
    const u = tenant("o3"); await warm(u);
    const commit = await capture("ingest_commit_buffered_batch", u, `o3a-${serial}`, [`o3-a-${serial}`], "o3-machine-a");
    const a = await hold(COMMIT, [commit]);
    expect(await a.done).toMatchObject({ ok: true, result: { kind: "committed" } });
    const b = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `o3b-${serial}`, [`o3-b-${serial}`], "o3-machine-b")], "SET LOCAL lock_timeout=750");
    expect(await b.done).toMatchObject({ ok: true });
    await finish(b, "COMMIT"); await finish(a, "COMMIT");
  }, 60000);

  test("X1: per-user controls (ingest_lock_fences without a mark, FOR UPDATE) and origin-scoped claims exclude each other", async () => {
    const u = tenant("x1"); await warm(u);
    const lockFences = "SELECT sessions.ingest_lock_fences($1,$2,$3) AS result";
    // In-flight claim -> the control waits (55P03 under a short wait).
    const a = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `x1a-${serial}`, [`x1-a-${serial}`], "x1-machine-a")]);
    expect((await a.done).ok).toBe(true);
    const ctl = await hold(lockFences, [u.orgId, u.workspaceId, u.userId], "SET LOCAL lock_timeout=500", true);
    expect(await ctl.done).toMatchObject({ ok: false, code: "55P03" });
    await finish(ctl, "ROLLBACK");
    // ... and proceeds once the claim commits.
    const ctl2 = await hold(lockFences, [u.orgId, u.workspaceId, u.userId], "SET LOCAL statement_timeout=20000", true);
    await waitBlockedBy(ctl2.pid, a.pid);
    await finish(a, "COMMIT");
    expect(await ctl2.done).toMatchObject({ ok: true });
    // In-flight control -> a claim of ANY origin of the user waits.
    const b = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `x1b-${serial}`, [`x1-b-${serial}`], "x1-machine-b")]);
    await waitBlockedBy(b.pid, ctl2.pid);
    expect(await settled(b.done, 300)).toBe(false);
    await finish(ctl2, "COMMIT");
    expect(await b.done).toMatchObject({ ok: true });
    await finish(b, "COMMIT");
    // Another user is never affected by this user's control.
    const v = tenant("x1v"); await warm(v);
    const ctl3 = await hold(lockFences, [u.orgId, u.workspaceId, u.userId], "SET LOCAL statement_timeout=20000", true);
    expect((await ctl3.done).ok).toBe(true);
    const c = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", v, `x1c-${serial}`, [`x1-c-${serial}`], "x1-machine-c")], "SET LOCAL lock_timeout=750");
    expect(await c.done).toMatchObject({ ok: true });
    await finish(c, "COMMIT"); await finish(ctl3, "COMMIT");
  }, 60000);

  test("X2: the operator user erase excludes origin-scoped claims in both directions; afterwards every origin is refused RH041", async () => {
    const u = tenant("x2"); await warm(u);
    const begin = "SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) AS result";
    const a = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `x2a-${serial}`, [`x2-a-${serial}`], "x2-machine-a")]);
    expect((await a.done).ok).toBe(true);
    const e1 = await hold(begin, [`x2-req-${serial}`, u.orgId, u.workspaceId, u.userId], "SET LOCAL lock_timeout=500", true);
    expect(await e1.done).toMatchObject({ ok: false, code: "55P03" });
    await finish(e1, "ROLLBACK"); await finish(a, "COMMIT");
    // An open erase begin blocks a claim from another machine of the user.
    const e2 = await hold(begin, [`x2-req-${serial}`, u.orgId, u.workspaceId, u.userId], "SET LOCAL statement_timeout=20000", true);
    expect(await e2.done).toMatchObject({ ok: true });
    const b = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `x2b-${serial}`, [`x2-b-${serial}`], "x2-machine-b")]);
    await waitBlockedBy(b.pid, e2.pid);
    expect(await settled(b.done, 300)).toBe(false);
    await finish(e2, "COMMIT");
    expect(await b.done).toMatchObject({ ok: false, code: "RH041" });
    await finish(b, "ROLLBACK");
    const c = await hold(CLAIM, [await capture("ingest_claim_buffered_batch", u, `x2c-${serial}`, [`x2-c-${serial}`], "x2-machine-c")]);
    expect(await c.done).toMatchObject({ ok: false, code: "RH041" });
    await finish(c, "ROLLBACK");
    expect(await marks()).toBe(0);
  }, 60000);

  test("X3: a mark downgrades only its own tenant: under a mark for user U, ingest_lock_fences(V) still takes V's row FOR UPDATE", async () => {
    const u = tenant("x3u"), v = tenant("x3v"); await warm(u); await warm(v);
    const lockTx = async (markFor: Tenant, lockFor: Tenant) => {
      const c = await connect();
      await c.query("BEGIN");
      // The exact form enter() writes (jsonb text, as ingest_lock_fences compares it).
      await c.query("INSERT INTO sessions.ingest_origin_claim_marks(xact,tenant) VALUES(pg_current_xact_id(),jsonb_build_array($1::text,$2::text,$3::text)::text)",
        [markFor.orgId, markFor.workspaceId, markFor.userId]);
      await c.query("SELECT sessions.ingest_lock_fences($1,$2,$3)", [lockFor.orgId, lockFor.workspaceId, lockFor.userId]);
      return c;
    };
    const probe = async (t: Tenant, mode: "SHARE" | "UPDATE") => {
      const c = await connect();
      try {
        await c.query("BEGIN");
        return await c.query(`SELECT 1 FROM sessions.ingest_fence_owners WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 FOR ${mode} NOWAIT`, [t.orgId, t.workspaceId, t.userId])
          .then(() => "ok", (e: any) => String(e.code));
      } finally { await release(c); }
    };
    // Mark for U, lock V: exclusive on V (another FOR SHARE cannot coexist).
    const other = await lockTx(u, v);
    expect(await probe(v, "SHARE")).toBe("55P03");
    await release(other);
    // Mark for V, lock V: shared on V (FOR SHARE coexists, FOR UPDATE does not).
    const own = await lockTx(v, v);
    expect(await probe(v, "SHARE")).toBe("ok");
    expect(await probe(v, "UPDATE")).toBe("55P03");
    await release(own);
    // A mark of ANOTHER transaction never counts: same tenant, but this transaction has none -> exclusive.
    const marked = await lockTx(v, u); // holds a mark for v in its own xid
    const c = await connect();
    await c.query("BEGIN");
    await c.query("SET LOCAL lock_timeout=500");
    expect(await c.query("SELECT sessions.ingest_lock_fences($1,$2,$3)", [v.orgId, v.workspaceId, v.userId]).then(() => "ok", (e: any) => String(e.code))).toBe("ok");
    expect(await probe(v, "SHARE")).toBe("55P03");
    await release(c); await release(marked);
  }, 60000);

  test("D1: flag-on deliveries (claim, stage, commit) of two machines of one user are accepted; no mark is left behind", async () => {
    const u = tenant("d1");
    expect((await deliver(u, `d1a-${serial}`, [`d1-a-${serial}`], "d1-machine-a")).outcome).toBe("accepted_buffered");
    const [x, y] = await Promise.all([
      deliver(u, `d1b-${serial}`, [`d1-b-${serial}`], "d1-machine-b"),
      deliver(u, `d1c-${serial}`, [`d1-c-${serial}`], "d1-machine-c"),
    ]);
    expect([x.outcome, y.outcome]).toEqual(["accepted_buffered", "accepted_buffered"]);
    // A replay through the per-user entry points answers the same receipt (the two paths share every row).
    expect((await deliver(u, `d1b-${serial}`, [`d1-b-${serial}`], "d1-machine-b", false)).outcome).toBe("replay");
    expect(await marks()).toBe(0);
  }, 60000);
});
