/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): two-connection proof of the
 * claim lock wait (ledger-sql.ts inClaimLockWait, INGEST_CLAIM_LOCK_WAIT_MS). With a row lock held by another
 * transaction, the wrapped statement gives up after the configured wait with 55P03 instead of the session's
 * longer lock_timeout (the role's 2 s); the transaction is rolled back and nothing it wrote survives; the
 * SET LOCAL never leaks into the connection afterwards; and with the lock free the same statement commits.
 * The claim function itself is unchanged (same SQL, same lock order): this proves only the wait bound.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { inClaimLockWait } from "../ledger-sql.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}

describe.skipIf(!url)("claim lock wait (owned PostgreSQL)", () => {
  let holder: pg.Client, claimant: pg.Client;
  const table = `claim_lock_wait_${process.pid}`;
  const run = (c: pg.Client) => async (text: string, params: unknown[]) => (await c.query(text, params as never[])) as unknown as { rows: Record<string, unknown>[] };

  beforeAll(async () => {
    holder = new pg.Client({ connectionString: url }); claimant = new pg.Client({ connectionString: url });
    await holder.connect(); await claimant.connect();
    await holder.query(`CREATE TABLE ${table}(id int PRIMARY KEY, n int NOT NULL DEFAULT 0)`);
    await holder.query(`INSERT INTO ${table}(id) VALUES (1)`);
    // The role's production setting, so the bound is visibly shorter than what an unwrapped claim would wait.
    await claimant.query("SET lock_timeout = '2s'");
    await claimant.query("SET idle_in_transaction_session_timeout = '10s'");
  });
  afterAll(async () => {
    await holder?.query("ROLLBACK").catch(() => {});
    await holder?.query(`DROP TABLE IF EXISTS ${table}`).catch(() => {});
    await holder?.end(); await claimant?.end();
  });

  test("lock held: 55P03 after ~the configured wait (not 2 s), rolled back, nothing written, no SET LOCAL leak", async () => {
    await holder.query("BEGIN");
    await holder.query(`SELECT 1 FROM ${table} WHERE id=1 FOR UPDATE`);
    const t0 = Date.now();
    const got = await inClaimLockWait(run(claimant), 100, async () => {
      await claimant.query(`INSERT INTO ${table}(id) VALUES (2)`); // a write before the wait, as a claim makes
      return claimant.query(`UPDATE ${table} SET n=n+1 WHERE id=1`);
    }).catch((e: unknown) => e);
    const waited = Date.now() - t0;
    expect((got as { code?: string }).code).toBe("55P03");
    expect(waited).toBeLessThan(1500);
    await holder.query("ROLLBACK");
    // Rolled back: the earlier write in the same transaction is gone, the row is untouched.
    expect((await claimant.query(`SELECT count(*)::int AS c FROM ${table} WHERE id=2`)).rows[0].c).toBe(0);
    expect((await claimant.query(`SELECT n FROM ${table} WHERE id=1`)).rows[0].n).toBe(0);
    // SET LOCAL ended with the transaction: the connection is back on its own setting.
    expect((await claimant.query("SHOW lock_timeout")).rows[0].lock_timeout).toBe("2s");
  });

  test("lock free: the same statement commits and the connection keeps its own lock_timeout", async () => {
    const r = await inClaimLockWait(run(claimant), 100, () => claimant.query(`UPDATE ${table} SET n=n+1 WHERE id=1 RETURNING n`));
    expect(r.rows[0].n).toBe(1);
    expect((await holder.query(`SELECT n FROM ${table} WHERE id=1`)).rows[0].n).toBe(1);
    expect((await claimant.query("SHOW lock_timeout")).rows[0].lock_timeout).toBe("2s");
    expect((await claimant.query("SHOW idle_in_transaction_session_timeout")).rows[0].idle_in_transaction_session_timeout).toBe("10s");
  });

  test("a transaction stranded inside the wrapper is ended by the local idle-in-transaction bound (~1 s), releasing the row", async () => {
    const stranded = new pg.Client({ connectionString: url });
    await stranded.connect();
    stranded.on("error", () => {}); // the server terminating the idle session is the expected outcome
    await stranded.query("SET idle_in_transaction_session_timeout = '10s'");
    // A Worker that dies after the claim statement: BEGIN + the wrapper's SET + a row lock, never COMMIT/ROLLBACK.
    const strandedRun = run(stranded);
    const never = new Promise<never>(() => {});
    void inClaimLockWait(strandedRun, 100, async () => { await stranded.query(`SELECT 1 FROM ${table} WHERE id=1 FOR UPDATE`); return never; });
    await new Promise((r) => setTimeout(r, 200));
    const t0 = Date.now();
    await claimant.query("SET lock_timeout = '5s'");
    const r = await claimant.query(`UPDATE ${table} SET n=n+0 WHERE id=1 RETURNING n`);
    expect(r.rows.length).toBe(1);
    expect(Date.now() - t0).toBeLessThan(3000); // released by the 1 s bound, far below the role's 10 s
    await claimant.query("SET lock_timeout = '2s'");
    await stranded.end().catch(() => {});
  });

  test("a held lock released within the wait: the statement proceeds and commits (only a real contention gives up)", async () => {
    await holder.query("BEGIN");
    await holder.query(`SELECT 1 FROM ${table} WHERE id=1 FOR UPDATE`);
    setTimeout(() => { void holder.query("COMMIT"); }, 50);
    const r = await inClaimLockWait(run(claimant), 1000, () => claimant.query(`UPDATE ${table} SET n=n+1 WHERE id=1 RETURNING n`));
    expect(r.rows[0].n).toBe(2);
  });
});
