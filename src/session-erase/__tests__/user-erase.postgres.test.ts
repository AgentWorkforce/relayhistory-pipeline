/**
 * 20261105058010's source-control gate against a concurrent operator user erase, on actual PostgreSQL (real advisory
 * locks, real READ COMMITTED snapshots, two connections). PGlite is single-connection, so the pglite suite proves the
 * order statically (pg_locks after the fact, prosrc); this file forces both interleavings. Opt in with an owned,
 * synthetic loopback SESSION_INGEST_TEST_PG_URL; never a deployed database. Each test creates and drops its own database.
 * Every test connection carries lock_timeout and statement_timeout, so a lock cycle fails fast (PostgreSQL reports 40P01
 * after deadlock_timeout; anything else stuck hits lock_timeout) instead of hanging CI.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { TENANT } from "../../session-ingest/__tests__/support.ts";
import { epoch, generation, identity, baseline, INDEX_SQL } from "../../session-ingest/__tests__/privacy-authority-support.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" || u.password || u.pathname !== "/postgres" || u.search || u.hash)
    throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
/** psql meta-commands (backslash lines) in the bootstrap files are dropped, as the other pg-proof fixtures do. */
const sql = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const cleanup: (() => Promise<void> | void)[] = [];
afterEach(async () => {
  for (const f of cleanup.splice(0).reverse()) await f();
}, 60_000);

const ALICE = TENANT;
const BOB = { ...TENANT, userId: "user-2" };
type Tenant = typeof TENANT;
/** Per-connection bounds: a blocked call is released by the test within a few seconds; nothing may wait longer. */
const LOCK_TIMEOUT = "20s", STATEMENT_TIMEOUT = "60s";
const GATED = "operator user erase: source controls for this user are refused until the erase is verified";

async function database() {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "user_erase_race_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url!);
  target.pathname = "/" + name;
  const pool = new pg.Pool({ connectionString: target.href, max: 6 });
  const held: pg.PoolClient[] = [];
  cleanup.push(async () => {
    for (const c of held) c.release(true);
    await pool.end();
    await admin.query("DROP DATABASE " + name + " WITH (FORCE)");
    await admin.end();
  });
  await pool.query(sql("supabase/tests/supabase_shim.sql"));
  await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
  for (const f of SESSION_BOOTSTRAP_FILES) await pool.query(sql(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pool.query(sql("supabase/migrations/" + f));
  // The C3 post-apply index (one CREATE INDEX CONCURRENTLY statement; its own simple query, outside any transaction).
  await pool.query(sql(INDEX_SQL));
  await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
  await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker',$1,'pass')", [generation]);
  const owner = async (fn: string, p: unknown, c: pg.ClientBase | pg.Pool = pool) => (await c.query("SELECT sessions." + fn + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0].r;
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })), allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
  await pool.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baseline, "e".repeat(64)]);
  /** A dedicated, bounded connection: A (the owner, as the erase job runs) or B (the ingest role, as the control route runs). */
  const conn = async (role?: "ingest") => {
    const c = await pool.connect();
    held.push(c);
    await c.query(`SET lock_timeout='${LOCK_TIMEOUT}'; SET statement_timeout='${STATEMENT_TIMEOUT}'`);
    if (role) await c.query("SET ROLE bulls96_session_ingest");
    return c;
  };
  /** The privacy authority active with an empty Neon (fence, then activate), as the C3 handoff leaves it. */
  const activate = async () => {
    await owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    const neonFingerprintSha256 = (await pool.query("SELECT sessions.ingest_privacy_authority_packet('[]','[]','[]') AS f")).rows[0].f;
    const neonOwned = (await pool.query("SELECT sessions.ingest_privacy_authority_owned_digest() AS d")).rows[0].d;
    await owner("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256, heads: [], deletions: [], resets: [], neonOwned });
  };
  return { pool, conn, activate };
}

const pid = (c: pg.ClientBase) => (c as unknown as { processID: number }).processID;
/** Resolves once `waiter` is queued, ungranted, on the erase key (961996,50000) behind `blocker`; bounded (~6 s). */
async function blockedOnEraseLock(pool: pg.Pool, waiter: number, blocker: number) {
  for (let i = 0; i < 240; i++) {
    const r = await pool.query(
      `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND classid=961996 AND objid=50000 AND objsubid=2 AND NOT granted) AS w,
              $2=ANY(pg_blocking_pids($1)) AS b,
              (SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1) AS t`, [waiter, blocker]);
    if (r.rows[0].w && r.rows[0].b && r.rows[0].t === "Lock") return;
    await Bun.sleep(25);
  }
  throw new Error("expected_erase_lock_wait_not_observed");
}
/** A call's outcome without throwing: the value, or SQLSTATE and message. */
const settle = (p: Promise<unknown>) => p.then((v) => ({ ok: true as const, v }), (e) => ({ ok: false as const, code: (e as { code?: string }).code, e: String((e as Error).message) }));
const rpc = (c: pg.ClientBase, fn: string, p: unknown) => c.query("SELECT sessions." + fn + "($1::jsonb) AS r", [JSON.stringify(p)]).then((r) => r.rows[0].r);
const control = (c: pg.ClientBase, fn: "ingest_source_control_begin" | "ingest_privacy_authority_begin", t: Tenant, id: string) =>
  rpc(c, fn, { epoch, tenant: t, kind: "reset", payload: { request_id: id } });
const erase = {
  begin: (c: pg.ClientBase, req: string, t: Tenant) => c.query("SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) AS r", [req, t.orgId, t.workspaceId, t.userId]).then((r) => r.rows[0].r),
  step: (c: pg.ClientBase, req: string) => c.query("SELECT sessions.operator_user_erase_step($1,100) AS r", [req]).then((r) => r.rows[0].r),
  complete: (c: pg.ClientBase, req: string) => c.query("SELECT sessions.operator_user_erase_complete($1) AS r", [req]).then((r) => r.rows[0].r),
};
const heads = async (pool: pg.Pool, t: Tenant) => (await pool.query(
  "SELECT count(*)::int AS n FROM sessions.ingest_source_control_heads WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [t.orgId, t.workspaceId, t.userId])).rows[0].n;
const state = async (pool: pg.Pool, req: string) => (await pool.query("SELECT state FROM sessions.operator_user_erasures WHERE request_id=$1", [req])).rows[0]?.state;

describe.skipIf(!url)("operator user erase vs source-control begins on actual PostgreSQL", () => {
  for (const authority of [false, true]) {
    const fn = authority ? "ingest_privacy_authority_begin" : "ingest_source_control_begin";
    const label = authority ? "authority active, ingest_privacy_authority_begin" : "legacy, ingest_source_control_begin";

    test(`erase first (${label}): a control begin queues on the erase lock and, once the erase commits, is refused with no head`, async () => {
      const d = await database();
      if (authority) await d.activate();
      const a = await d.conn(), b = await d.conn("ingest");
      // (a0) The erase begin itself, uncommitted: the control waits, then its fresh snapshot sees the committed ledger row.
      await a.query("BEGIN");
      expect(await erase.begin(a, "req-1", ALICE)).toMatchObject({ state: "purging" });
      const first = settle(control(b, fn, ALICE, "during-begin"));
      await blockedOnEraseLock(d.pool, pid(b), pid(a));
      await a.query("COMMIT");
      expect(await first).toMatchObject({ ok: false, code: "55000", e: GATED });
      expect(await heads(d.pool, ALICE)).toBe(0);
      // (a) An open step (50000 X, then 96 S) after the committed begin: the control waits, then is refused.
      await a.query("BEGIN");
      expect(await erase.step(a, "req-1")).toMatchObject({ state: "metadata_purged" });
      const second = settle(control(b, fn, ALICE, "during-step"));
      await blockedOnEraseLock(d.pool, pid(b), pid(a));
      await a.query("COMMIT");
      expect(await second).toMatchObject({ ok: false, code: "55000", e: GATED });
      expect(await heads(d.pool, ALICE)).toBe(0);
      // The same for an open complete; then the gate lifts for Alice, and Bob was never gated.
      await a.query("BEGIN");
      expect(await erase.complete(a, "req-1")).toMatchObject({ state: "bytes_verified" });
      const third = settle(control(b, fn, BOB, "bob-during-complete"));
      await blockedOnEraseLock(d.pool, pid(b), pid(a));
      await a.query("COMMIT");
      expect(await third).toMatchObject({ ok: true, v: { apply: true } });
      expect(await control(b, fn, ALICE, "after-verified")).toMatchObject({ apply: true });
      expect(await heads(d.pool, ALICE)).toBe(1);
    }, 120_000);

    test(`control first (${label}): the erase queues behind an in-flight control, then purges and counts its head and completes`, async () => {
      const d = await database();
      if (authority) await d.activate();
      const a = await d.conn(), b = await d.conn("ingest");
      // (b0) Alice's control in flight (head written, 50000 S and 96 S held): the erase begin waits for it.
      await b.query("BEGIN");
      expect(await control(b, fn, ALICE, "in-flight")).toMatchObject({ apply: true });
      const begun = settle(erase.begin(a, "req-1", ALICE));
      await blockedOnEraseLock(d.pool, pid(a), pid(b));
      await b.query("COMMIT");
      expect(await begun).toMatchObject({ ok: true, v: { state: "purging" } });
      expect(await heads(d.pool, ALICE)).toBe(1);
      // (b) Another user's control in flight: the step (50000 X) waits for it too, then purges Alice's committed head.
      await b.query("BEGIN");
      expect(await control(b, fn, BOB, "bob-in-flight")).toMatchObject({ apply: true });
      const stepped = settle(erase.step(a, "req-1"));
      await blockedOnEraseLock(d.pool, pid(a), pid(b));
      await b.query("COMMIT");
      const s = await stepped;
      expect(s).toMatchObject({ ok: true, v: { state: "metadata_purged", deleted: { ingest_source_control_heads: 1 } } });
      expect(await heads(d.pool, ALICE)).toBe(0);
      expect(await heads(d.pool, BOB)).toBe(1);
      // Nothing of Alice's came back: complete certifies (and the head stays gone).
      expect(await erase.complete(a, "req-1")).toMatchObject({ state: "bytes_verified" });
      expect(await state(d.pool, "req-1")).toBe("bytes_verified");
      expect(await heads(d.pool, ALICE)).toBe(0);
    }, 120_000);
  }
});
