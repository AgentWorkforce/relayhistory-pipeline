/**
 * W2 visibility stand-in on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; hold-harness run.ts gives disposable
 * 15/16/17). ISOLATED SYNTHETIC PROOF: the stand-in schema (cutover-visibility-standin.ts) over every real migration;
 * no product code. Every block is proven from pg_locks / pg_stat_activity (hold-harness), never from timing.
 *   1 erase first: a publish waits for an in-flight erasure write (its 062340 trigger holds the org key SHARED), then
 *     refuses: the erase committed before the publish's snapshot
 *   2 publish first: an erasure write waits for an in-flight publish (EXCLUSIVE), resumes after it, and revokes
 *   3 an erase transaction that takes its xid BEFORE the publish but commits AFTER it revokes: numeric xid order would
 *     have misclassified it (its xid is below the publish snapshot's xmax), snapshot visibility does not
 *   4 an unrelated org's erase never blocks the publish
 * Not run by its author: the reserved PG17 lane (claim-ci) runs it.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { STAND_IN } from "./cutover-visibility-standin.ts";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, expectNotBlocked, openPair, type Pair } from "./hold-harness/two-conn.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "org-vis", OTHER = "org-vis-other";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<{ db: ScratchDatabase; o: pg.Client }> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "cvis" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  const o = new pg.Client({ connectionString: db.ownerUrl }); await o.connect();
  cleanups.push(() => o.end());
  await o.query(STAND_IN);
  for (const org of [ORG, OTHER]) await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())", [org]);
  return { db, o };
}
async function pair(db: ScratchDatabase): Promise<Pair> {
  const p = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => p.close());
  return p;
}
const deletion = (org: string, session: string) =>
  `INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES('${org}','ws','u','o','claude','${session}')`;
const snapshot = async (o: pg.Client) => (await o.query("SELECT pg_current_snapshot()::text s")).rows[0].s as string;
const publishSql = "SELECT cutover_proof.publish($1, 'drain', $2, $3, $4::pg_snapshot) AS r";
const verdict = async (o: pg.Client, digest: string, session: string) =>
  (await o.query("SELECT cutover_proof.verdict($1, 'drain', $2, $3) AS r", [ORG, digest, session])).rows[0].r as string;

describe.skipIf(!url)("W2 visibility stand-in: two-connection orderings (real PostgreSQL)", () => {
  test("1. erase first: the publish waits for the in-flight erasure write, then refuses", async () => {
    const { db, o } = await database();
    const bound = await snapshot(o);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(deletion(ORG, "s-1")),
      wait: (c) => c.query(publishSql, [ORG, "stage-1", "s-1", bound]).then((x) => x.rows[0].r) });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited).toMatchObject({ ok: true, value: "refused_erased" });
    expect(await verdict(o, "stage-1", "s-1")).toBe("absent");
  });

  test("2. publish first: the erasure write waits for the in-flight publish, resumes, and revokes", async () => {
    const { db, o } = await database();
    const bound = await snapshot(o);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b,
      hold: (c) => c.query(publishSql, [ORG, "stage-2", "s-2", bound]).then((x) => x.rows[0].r),
      wait: (c) => c.query(deletion(ORG, "s-2")) });
    expect(r.held).toBe("visible");
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    expect(await verdict(o, "stage-2", "s-2")).toBe("revoked");
  });

  test("3. an erase whose xid predates the publish but which commits after it revokes (snapshot order, not xid order)", async () => {
    const { db, o } = await database();
    const bound = await snapshot(o);
    const p = await pair(db);
    // b takes its xid first (no lock yet: its trigger takes the key only at the source write).
    await p.b.query("BEGIN");
    const early = (await p.b.query("SELECT pg_current_xact_id()::text x")).rows[0].x as string;
    // a publishes and commits in between (no conflict: b holds no key). Its own xid is taken AFTER b's.
    await p.a.query("BEGIN");
    const late = (await p.a.query("SELECT pg_current_xact_id()::text x")).rows[0].x as string;
    expect((await p.a.query(publishSql, [ORG, "stage-3", "s-3", bound])).rows[0].r).toBe("visible");
    await p.a.query("COMMIT");
    // b now writes an erasure source and commits after the publish.
    await p.b.query(deletion(ORG, "s-3"));
    await p.b.query("COMMIT");
    // Numeric xid order says b came first (its xid is below the publisher's), yet b committed after the publish.
    // (Not pg_snapshot_xmax: PostgreSQL sets it to latestCompletedXid + 1, so an in-progress xid may exceed it.
    // The 8b7fc40e run failed on exactly that auxiliary claim, window linuyc-cvis-8b7fc40e-20261128T123823Z.)
    const probe = (await o.query(`SELECT $1::xid8 < $2::xid8 AS numerically_before,
      pg_visible_in_snapshot($1::xid8, published_snapshot) AS visible FROM cutover_proof.effects WHERE digest = 'stage-3'`, [early, late])).rows[0];
    expect(probe).toEqual({ numerically_before: true, visible: false });
    expect(await verdict(o, "stage-3", "s-3")).toBe("revoked");
  });

  test("4. an unrelated org's in-flight erase never blocks the publish", async () => {
    const { db, o } = await database();
    const bound = await snapshot(o);
    const p = await pair(db);
    const r = await expectNotBlocked(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(deletion(OTHER, "s-x")),
      wait: (c) => c.query(publishSql, [ORG, "stage-4", "s-4", bound]).then((x) => x.rows[0].r) });
    expect(r.waited).toMatchObject({ ok: true, value: "visible" });
  });
});
