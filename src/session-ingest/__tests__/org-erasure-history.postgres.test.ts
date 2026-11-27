/**
 * 20261105062340 per-org erasure history on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable
 * 15/16/17), two independent connections, every block proven from pg_locks (hold-harness), never from timing.
 *   1 writer first: a release waits for an in-flight erasure write (any source, checked entry or not), then refuses:
 *     the write is in the history it counts
 *   2 release first: a write waits for an in-flight release, then proceeds after it (a writer that resumes after the
 *     release), and is recorded after the release
 *   3 a writer holding a row lock (an UPDATE of an existing row) blocks the release, never deadlocks; a second writer
 *     queued on that row lock resolves too
 *   4 the down, then a reapply: a new coverage period; an epoch installed before it cannot bind
 *   5 the cutoff is a fence: a write still uncommitted when the install starts blocks the install, so it commits (and
 *     is visible to a FULL started after the install) before the cutoff; a write after the install is in the history
 *   6 a maintenance TRUNCATE in a REPEATABLE READ or SERIALIZABLE transaction whose snapshot predates a committed
 *     install is refused 40001
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { fixturePgUrl, migrateAsOwner, rollBackLaterSlots, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, openPair, type Pair } from "./hold-harness/two-conn.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "org-hold";
const EVIDENCE = "e".repeat(64);
const DOWN = readFileSync("supabase/rollbacks/20261105062340_ingest_org_erasure_history.down.sql", "utf8");
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<ScratchDatabase> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "ehist" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  return db;
}
async function pair(db: ScratchDatabase): Promise<Pair> {
  const p = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => p.close());
  return p;
}
async function owner(db: ScratchDatabase) {
  const c = new pg.Client({ connectionString: db.ownerUrl }); await c.connect();
  cleanups.push(() => c.end());
  return c;
}
const deletion = (session: string) =>
  `INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES('${ORG}','ws','u','o','claude','${session}')`;
const SEAL = "b".repeat(64);
/** The bridge's selected FULL: started (integer ms) 1 ms after the install (the install-seal gap is >= 2 ms). */
const MID = "(SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 FROM sessions.ingest_org_admission_holds WHERE epoch=$2::uuid)";
const FULL_ARGS = `'full-run-1',${MID}`;
/** install, seal (the FULL receipt), bind that FULL, attempt: ready for bridge_complete; returns the epoch. */
async function toAttempt(o: pg.Client) {
  const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).rows[0].e as string;
  await o.query("SELECT pg_sleep(0.002)"); // a FULL start strictly between install and seal
  await o.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [ORG, e]);
  await o.query(`SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,${FULL_ARGS},$3)`, [ORG, e, SEAL]);
  await o.query(`SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,'${"c".repeat(64)}')`, [ORG, e]);
  return e;
}
const watermark = async (o: pg.Client) => Number((await o.query("SELECT (sessions.ingest_org_admission_hold_status($1)->>'erasures')::bigint n", [ORG])).rows[0].n);
const releaseSql = "SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)";

describe.skipIf(!url)("062340 two-connection orderings (real PostgreSQL)", () => {
  test("1. writer first: the release waits for an in-flight erasure write, then refuses (the write is counted)", async () => {
    const db = await database();
    const o = await owner(db);
    const e = await toAttempt(o);
    const seen = await watermark(o);
    const p = await pair(db);
    // A direct owner write: no entry check, only the recorder trigger takes the org key SHARED.
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(deletion("s-w1")),
      wait: (c) => c.query(releaseSql, [ORG, e, EVIDENCE, seen]) });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited).toMatchObject({ ok: false, error: { code: "55000" } });
    expect(await watermark(o)).toBe(seen + 1);
    await o.query(releaseSql, [ORG, e, EVIDENCE, seen + 1]);
  });

  test("2. release first: a write waits for the release, resumes after it, and is recorded after the release", async () => {
    const db = await database();
    const o = await owner(db);
    const e = await toAttempt(o);
    const seen = await watermark(o);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(releaseSql, [ORG, e, EVIDENCE, seen]),
      wait: (c) => c.query(deletion("s-after")) });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.holderEnded.ok).toBe(true);
    expect(r.waited.ok).toBe(true);
    const at = (await o.query(`SELECT (SELECT released_at FROM sessions.ingest_org_admission_holds WHERE epoch=$1::uuid) < max(h.at) AS after
                                FROM sessions.ingest_org_erasure_history h WHERE h.erased_org=$2`, [e, ORG])).rows[0].after;
    expect(at).toBe(true);
  });

  test("3. a writer holding a row lock blocks the release without a deadlock; a writer queued on the row resolves", async () => {
    const db = await database();
    const o = await owner(db);
    await o.query(deletion("s-row"));
    const e = await toAttempt(o);
    const seen = await watermark(o);
    const p = await pair(db);
    const third = new pg.Client({ connectionString: db.ownerUrl }); await third.connect(); cleanups.push(() => third.end());
    let queued: Promise<unknown> | undefined;
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b,
      hold: (c) => c.query("UPDATE sessions.upload_deletions SET deleted_at=clock_timestamp() WHERE org_id=$1 AND session_id='s-row'", [ORG]),
      wait: (c) => c.query(releaseSql, [ORG, e, EVIDENCE, seen]),
      whileBlocked: async () => {
        queued = third.query("UPDATE sessions.upload_deletions SET deleted_at=clock_timestamp() WHERE org_id=$1 AND session_id='s-row'", [ORG]).then(() => "ok", (x) => String(x.code));
      } });
    expect(r.waited).toMatchObject({ ok: false, error: { code: "55000" } }); // the UPDATE is counted
    expect(await queued).toBe("ok"); // no 40P01
    expect(await watermark(o)).toBe(seen + 2);
  });

  test("4. down then reapply: a new coverage period; an epoch installed before it cannot bind", async () => {
    const db = await database();
    const o = await owner(db);
    const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).rows[0].e as string;
    await o.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'verified_rollback',$3,NULL,NULL)", [ORG, e, EVIDENCE]);
    // Slots above 062340 (062350 on the combined head) come off first: 062340's down refuses while one is applied.
    await rollBackLaterSlots(db.ownerUrl, "20261105062340_ingest_org_erasure_history");
    await o.query(DOWN);
    await o.query(deletion("s-gap")); // written while no recorder exists: never recorded
    // A hold installed in the gap, then the reapply.
    const e2 = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).rows[0].e as string;
    await migrateAsOwner(db.ownerUrl);
    const cov = (await o.query("SELECT kind FROM sessions.ingest_org_erasure_history_coverage ORDER BY at")).rows.map((x) => x.kind);
    expect(cov).toEqual(["started", "ended", "started"]);
    expect((await o.query("SELECT count(*)::int n FROM sessions.ingest_org_erasure_history WHERE erased_org=$1", [ORG])).rows[0].n).toBe(0);
    await o.query("SELECT pg_sleep(0.002)");
    await o.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [ORG, e2]);
    await expect(o.query(`SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,${FULL_ARGS},$3)`, [ORG, e2, SEAL])).rejects.toMatchObject({ code: "55000" });
    expect((await o.query("SELECT sessions.ingest_org_erasure_history_recording() r")).rows[0].r).toBe(true); // the reapply re-armed
  });

  test("5. the cutoff is a fence: an uncommitted write blocks the install and commits before it; a later write is counted", async () => {
    const db = await database();
    const o = await owner(db);
    const p = await pair(db);
    // The reproduced race: a write recorded, still uncommitted, when the hold is installed.
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(deletion("s-open")),
      wait: (c) => c.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG]) });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    const e = (r.waited as { value: pg.QueryResult }).value.rows[0].e as string;
    // It committed before the install, so its row predates the cutoff and a FULL begun after the install sees it.
    const t = (await o.query(`SELECT (SELECT max(at) FROM sessions.ingest_org_erasure_history WHERE erased_org=$1) < h.installed_at AS before,
                                     EXISTS(SELECT 1 FROM sessions.upload_deletions WHERE org_id=$1 AND session_id='s-open') AS visible
                                FROM sessions.ingest_org_admission_holds h WHERE h.epoch=$2::uuid`, [ORG, e])).rows[0];
    expect(t).toEqual({ before: true, visible: true });
    await o.query("SELECT pg_sleep(0.002)"); // a FULL start strictly between install and seal
  await o.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [ORG, e]);
    await o.query(`SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,${FULL_ARGS},$3)`, [ORG, e, SEAL]);
    expect(await watermark(o)).toBe(0); // the pre-install write is FULL's, not the window's
    await o.query(deletion("s-after"));
    expect(await watermark(o)).toBe(1);
  });
  test("6. a REPEATABLE READ or SERIALIZABLE maintenance transaction older than a committed install cannot TRUNCATE a source", async () => {
    const db = await database();
    const o = await owner(db);
    for (const [i, iso] of (["REPEATABLE READ", "SERIALIZABLE"] as const).entries()) {
      const m = new pg.Client({ connectionString: db.ownerUrl }); await m.connect(); cleanups.push(() => m.end());
      await m.query(`BEGIN ISOLATION LEVEL ${iso}`);
      await m.query("SELECT count(*) FROM sessions.ingest_org_admission_holds"); // the snapshot is taken here
      if (i === 0) await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())", [ORG]); // committed after it
      await expect(m.query("TRUNCATE sessions.upload_resets")).rejects.toMatchObject({ code: "40001" });
      await m.query("ROLLBACK");
    }
    await expect(o.query("TRUNCATE sessions.upload_resets")).rejects.toMatchObject({ code: "55000" }); // READ COMMITTED sees the hold
  });
});
