/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only; run.ts runs it on 15, 16 and 17):
 * self-test of the hold harness's two-connection helper. Two independent OWNER_ROLE (non-superuser) connections on a
 * scratch database, observed from the fixture superuser: an advisory lock blocks across them in both orderings, with
 * the wait seen in pg_locks and pg_stat_activity, not inferred from timing.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { fixturePgUrl, OWNER_ROLE, roleReceipt, scratchDatabase, serverMajor, type ScratchDatabase } from "./pg.ts";
import { expectBlocks, expectBlocksBothWays, expectNotBlocked, openPair, type Conn, type Pair } from "./two-conn.ts";

const url = fixturePgUrl();

const KEY = "96bulls:hold-harness-selftest:org-a";
const xLock = (key = KEY) => async (c: Conn) => (await c.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [key])).rowCount;
const sLock = (key = KEY) => async (c: Conn) => (await c.query("SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))", [key])).rowCount;

describe.skipIf(!url)("hold harness: two connections (owned PostgreSQL)", () => {
  let db: ScratchDatabase, pair: Pair, major: number;

  beforeAll(async () => {
    major = await serverMajor(url!);
    db = await scratchDatabase(url!, { prefix: "holdh" });
    pair = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
    // Its own schema: on PostgreSQL 15 a NOINHERIT owner gets no CREATE on public through pg_database_owner (16+ does).
    await pair.a.query("CREATE SCHEMA holdh; CREATE TABLE holdh.probe(id int PRIMARY KEY)");
  });
  afterAll(async () => { await pair?.close(); await db?.drop(); });

  test("fixture: PostgreSQL 15+, two distinct non-superuser owner backends on a scratch database", async () => {
    expect(major).toBeGreaterThanOrEqual(15);
    expect(pair.a.pid).not.toBe(pair.b.pid);
    for (const c of [pair.a, pair.b]) {
      const who = await roleReceipt(c.client);
      expect(who).toMatchObject({ current_user: OWNER_ROLE, rolsuper: false, rolbypassrls: false });
      expect((await c.query("SELECT current_database() AS d")).rows[0].d).toBe(db.name);
    }
  });

  test(`exclusive advisory lock blocks across connections, both orderings, seen in pg_locks/pg_stat_activity`, async () => {
    const { aHolds, bHolds } = await expectBlocksBothWays(pair, xLock(), xLock());
    for (const [r, waiter, holder] of [[aHolds, pair.b, pair.a], [bHolds, pair.a, pair.b]] as const) {
      expect(r.receipt).toMatchObject({ pid: waiter.pid, holderPid: holder.pid, waitEventType: "Lock", waitEvent: "advisory", state: "active" });
      expect(r.receipt.blockingPids).toContain(holder.pid);
      expect(r.receipt.waiting).toHaveLength(1);
      // A bigint key: classid/objid are its halves, objsubid 1 (two-int keys are objsubid 2).
      expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory", mode: "ExclusiveLock", granted: false, objsubid: 1 });
      expect(r.receipt.holderLocks).toEqual([{ ...r.receipt.waiting[0]!, granted: true }]);
      expect(r.receipt.query).toContain("pg_advisory_xact_lock");
      expect(r.waited).toMatchObject({ ok: true, value: 1 });
      expect(r.holderEnded.ok).toBe(true);
    }
    // Nothing is left held or waiting.
    const left = (await pair.observer.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND pid = ANY($1)", [[pair.a.pid, pair.b.pid]])).rows[0].n;
    expect(left).toBe(0);
  });

  test("shared holder blocks an exclusive waiter and an exclusive holder blocks a shared waiter (the hold's key S/X)", async () => {
    const { aHolds, bHolds } = await expectBlocksBothWays(pair, sLock(), xLock());
    expect(aHolds.receipt.waiting[0]).toMatchObject({ locktype: "advisory", mode: "ExclusiveLock" });
    expect(aHolds.receipt.holderLocks[0]).toMatchObject({ mode: "ShareLock", granted: true });
    expect(bHolds.receipt.waiting[0]).toMatchObject({ locktype: "advisory", mode: "ShareLock" });
    expect(bHolds.receipt.holderLocks[0]).toMatchObject({ mode: "ExclusiveLock", granted: true });
  });

  test("shared/shared and different keys never block (and expectBlocks refuses a case that does not block)", async () => {
    const shared = await expectNotBlocked(pair, { holder: pair.a, waiter: pair.b, hold: sLock(), wait: sLock() });
    expect(shared.waited.ok).toBe(true);
    expect(shared.observed.every((w) => !w.blockingPids.includes(pair.a.pid))).toBe(true);
    const other = await expectNotBlocked(pair, { holder: pair.b, waiter: pair.a, hold: xLock(), wait: xLock(KEY + "-other") });
    expect(other.waited.ok).toBe(true);
    await expect(expectBlocks(pair, { holder: pair.a, waiter: pair.b, hold: xLock(), wait: xLock(KEY + "-other"), timeoutMs: 1000 }))
      .rejects.toThrow(/waiter_settled_unblocked/);
    // The failed case cleaned up: both connections are idle and usable.
    expect((await pair.a.query("SELECT 1 AS one")).rows[0].one).toBe(1);
    expect((await pair.b.query("SELECT 1 AS one")).rows[0].one).toBe(1);
  });

  test("a waiter's PostgreSQL error is an outcome (row wait on the holder's xid, then 23505), and isolation is the caller's", async () => {
    const r = await expectBlocks(pair, {
      holder: pair.a, waiter: pair.b,
      hold: async (c) => (await c.query("INSERT INTO holdh.probe VALUES (1)")).rowCount,
      wait: { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ", op: async (c) => {
        const iso = (await c.query("SELECT current_setting('transaction_isolation') AS i")).rows[0].i;
        await c.query("INSERT INTO holdh.probe VALUES (1)");
        return iso;
      } },
    });
    expect(r.receipt).toMatchObject({ waitEventType: "Lock", waitEvent: "transactionid" });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "transactionid", mode: "ShareLock" });
    expect(r.receipt.holderLocks[0]).toMatchObject({ locktype: "transactionid", mode: "ExclusiveLock", granted: true });
    expect(r.waited).toMatchObject({ ok: false, error: { code: "23505" } });
    // The holder committed, the waiter rolled back.
    expect((await pair.a.query("SELECT count(*)::int n FROM holdh.probe")).rows[0].n).toBe(1);
    await pair.a.query("DELETE FROM holdh.probe");
  });

  test("a rollback-ending holder releases the waiter onto unchanged state", async () => {
    const r = await expectBlocks(pair, {
      holder: pair.b, waiter: pair.a, holderEnd: "rollback",
      hold: async (c) => (await c.query("INSERT INTO holdh.probe VALUES (2)")).rowCount,
      wait: async (c) => (await c.query("INSERT INTO holdh.probe VALUES (2) RETURNING id")).rows[0].id,
    });
    expect(r.waited).toMatchObject({ ok: true, value: 2 });
    expect((await pair.b.query("SELECT count(*)::int n FROM holdh.probe WHERE id=2")).rows[0].n).toBe(1);
  });
});
