/**
 * 20261105062340 per-org erasure history on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable
 * 15/16/17), two independent connections, every block proven from pg_locks (hold-harness), never from timing.
 *   1 writer first: a release waits for an in-flight erasure write (any source, checked entry or not), then refuses:
 *     the write is in the history it counts
 *   2 release first: a write waits for an in-flight release, then proceeds after it (a writer that resumes after the
 *     release), and is recorded after the release
 *   3 a writer holding a row lock (an UPDATE of an existing row) blocks the release, never deadlocks; a second writer
 *     queued on that row lock resolves too
 *   4 the down, then a reapply: a new coverage period; a cutoff bound before it is not covered
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
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
/** install, seal, bind (cutoff = install time), attempt: ready for bridge_complete; returns the epoch. */
async function toAttempt(o: pg.Client) {
  const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).rows[0].e as string;
  await o.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${"b".repeat(64)}')`, [ORG, e]);
  await o.query("SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,(SELECT installed_at FROM sessions.ingest_org_admission_holds WHERE epoch=$2::uuid))", [ORG, e]);
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

  test("4. down then reapply: a new coverage period, and a cutoff before it is not covered", async () => {
    const db = await database();
    const o = await owner(db);
    const before = (await o.query("SELECT clock_timestamp() t")).rows[0].t;
    await o.query(DOWN);
    await o.query(deletion("s-gap")); // written while no recorder exists: never recorded
    await migrateAsOwner(db.ownerUrl);
    const cov = (await o.query("SELECT kind FROM sessions.ingest_org_erasure_history_coverage ORDER BY at")).rows.map((x) => x.kind);
    expect(cov).toEqual(["started", "ended", "started"]);
    expect((await o.query("SELECT sessions.ingest_org_erasure_history_covers($1) c", [before])).rows[0].c).toBe(false);
    expect((await o.query("SELECT count(*)::int n FROM sessions.ingest_org_erasure_history WHERE erased_org=$1", [ORG])).rows[0].n).toBe(0);
    const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).rows[0].e as string;
    await o.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${"b".repeat(64)}')`, [ORG, e]);
    await expect(o.query("SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,$3)", [ORG, e, before])).rejects.toMatchObject({ code: "55000" });
  });
});
