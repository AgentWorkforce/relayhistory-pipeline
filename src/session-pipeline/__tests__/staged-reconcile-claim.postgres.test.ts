/**
 * 20261105062110 search_reconcile_claim_v1 never waits on a run row another transaction holds, on actual
 * PostgreSQL with two connections (PGlite is single-connection, so staged-reconcile.pglite.test.ts cannot force
 * the interleavings). A claim answers busy (no row) at once while the holder sits inside a long transaction
 * (a pass 4 write or the final step) or a finish/erase of the run is in flight; against another claim's
 * uncommitted first insert of the same scope it waits at most its own lock_timeout (2 s) and then answers busy.
 * Opt in with an owned, synthetic loopback SESSION_INGEST_TEST_PG_URL; never a deployed database. Each test creates
 * and drops its own database. Every connection carries lock_timeout and statement_timeout, so a regression to a
 * blocking claim fails on the elapsed-time bound instead of hanging CI.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
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

const RUNTIME = "search_consumer_runtime";
const SCOPE = ["org-a", "ws-a", "claude", "sess-1"] as const;
/** A blocked claim would wait this long before failing; the busy answers must come far sooner. */
const LOCK_TIMEOUT = "20s", STATEMENT_TIMEOUT = "60s";

async function database() {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "staged_claim_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url!);
  target.pathname = "/" + name;
  const pool = new pg.Pool({ connectionString: target.href, max: 4 });
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
  await pool.query(`GRANT USAGE ON SCHEMA sessions TO ${RUNTIME}`); // as session_consumer_role.sql does
  /** A dedicated, bounded connection acting as the consumer login. */
  const conn = async () => {
    const c = await pool.connect();
    held.push(c);
    await c.query(`SET lock_timeout='${LOCK_TIMEOUT}'; SET statement_timeout='${STATEMENT_TIMEOUT}'; SET ROLE ${RUNTIME}`);
    return c;
  };
  return { pool, conn };
}

type Claim = { run_id: string; lease_nonce: string; attempts: number };
const claim = async (c: pg.ClientBase) =>
  (await c.query<Claim>(
    "SELECT run_id::text, lease_nonce::text, attempts FROM sessions.search_reconcile_claim_v1($1,$2,$3,$4,$4,7,'fp',60000)", [...SCOPE])).rows;
/** The claim's rows and how long it took. */
const timed = async (c: pg.ClientBase) => {
  const t0 = performance.now();
  const rows = await claim(c);
  return { rows, ms: performance.now() - t0 };
};
const expire = (pool: pg.Pool) => pool.query("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");

describe.skipIf(!url)("062110 claim never waits on a held run row (real PostgreSQL, two connections)", () => {
  test("holder inside a long transaction with an expired lease: the second claim is busy at once, then takes over after the holder ends", async () => {
    const { pool, conn } = await database();
    const a = await conn();
    const b = await conn();
    const first = (await claim(a))[0]!;
    expect(first.attempts).toBe(1);
    await expire(pool);
    // A stays inside one transaction holding the run row (as a pass 4 write or the final step does).
    await a.query("BEGIN");
    await a.query("SELECT sessions.search_reconcile_stage_batch_v1($1::bigint,$2::uuid,'want',3::smallint,'[]'::jsonb,'k')", [first.run_id, first.lease_nonce]);
    const busy = await timed(b);
    expect(busy.rows).toEqual([]);
    expect(busy.ms).toBeLessThan(1500);
    await a.query("COMMIT");
    // A renewed its lease in that transaction: still busy, but now on the lease, not the lock.
    expect(await claim(b)).toEqual([]);
    await expire(pool);
    const taken = (await claim(b))[0]!;
    expect([taken.run_id, taken.attempts]).toEqual([first.run_id, 2]);
  }, 120_000);

  test("a finish or erase of the run in flight: the claim is busy at once; after it commits the claim starts a new run", async () => {
    const { pool, conn } = await database();
    const a = await conn();
    const b = await conn();
    const first = (await claim(a))[0]!;
    await expire(pool);
    await a.query("BEGIN");
    expect((await a.query<{ n: string }>("SELECT sessions.search_reconcile_erase_scope_v1($1,$2,$3,ARRAY[$4])::text n", [...SCOPE])).rows[0]!.n).toBe("1");
    const busy = await timed(b);
    expect(busy.rows).toEqual([]);
    expect(busy.ms).toBeLessThan(1500);
    await a.query("COMMIT");
    const fresh = (await claim(b))[0]!;
    expect(fresh.run_id).not.toBe(first.run_id);
    expect(fresh.attempts).toBe(1);
  }, 120_000);

  test("another claim's uncommitted first insert of the scope: the claim waits at most its own 2 s lock_timeout, then is busy", async () => {
    const { conn } = await database();
    const a = await conn();
    const b = await conn();
    await a.query("BEGIN");
    expect((await claim(a)).length).toBe(1); // inserted, not committed
    const busy = await timed(b);
    expect(busy.rows).toEqual([]);
    expect(busy.ms).toBeGreaterThan(1500);
    expect(busy.ms).toBeLessThan(10_000); // the connection's own lock_timeout is 20 s: the function's 2 s applied
    await a.query("COMMIT");
    expect(await claim(b)).toEqual([]); // A's lease is live
  }, 120_000);
});
