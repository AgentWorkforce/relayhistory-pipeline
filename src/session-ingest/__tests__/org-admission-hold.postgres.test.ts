/**
 * 20261105062320 per-org admission hold on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable
 * 15/16/17), two independent connections, every block proven from pg_stat_activity/pg_locks (hold-harness), never from
 * timing. The single-connection semantics and the TS round trip are in org-admission-hold.pglite.test.ts.
 *   1 install waits for an in-flight admission, then refuses on pending / proceeds on committed
 *   2 an admission waits for an install, then gets RH042
 *   3 seal waits for an in-flight publication; publication refused after
 *   4 release waits for an in-flight erase; the erasure watermark refuses a release read before the erase committed
 *   5 the mirror gate at the privilege boundary: a non-mirror login refused while sealed (with or without the GUC),
 *     a member of bulls96_session_ingest_mirror passes
 *   6 REPEATABLE READ / SERIALIZABLE admission refused 40001
 *   7 the down vs lifecycle: a down waits for an in-flight install; a lifecycle call blocked behind a down refuses
 *   8 an unrelated org is never blocked by an install
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, expectNotBlocked, openPair, type Pair } from "./hold-harness/two-conn.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "org-hold", OTHER = "org-free";
const EVIDENCE = "e".repeat(64);
const DOWN = readFileSync("supabase/rollbacks/20261105062320_ingest_org_admission_fences.down.sql", "utf8");
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<ScratchDatabase> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "hold" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  return db;
}
async function pair(db: ScratchDatabase, ownerSide = true): Promise<Pair> {
  const p = await openPair({ url: ownerSide ? db.ownerUrl : db.fixtureUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => p.close());
  return p;
}
async function owner(db: ScratchDatabase) {
  const c = new pg.Client({ connectionString: db.ownerUrl }); await c.connect();
  cleanups.push(() => c.end());
  return c;
}
const loginUrl = (db: ScratchDatabase, user: string) => { const u = new URL(db.fixtureUrl); u.username = user; return u.href; };
const install = (c: { query: pg.Client["query"] }, org = ORG) =>
  c.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [org]).then((r) => r.rows[0].e as string);
const insertBatch = (state: "pending" | "committed", id: string, org = ORG) =>
  `INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state${state === "committed" ? ",committed_at" : ""})
   VALUES('${org}','ws','o','${id}','u','${"d".repeat(64)}','rhr_${"f".repeat(64)}','${state}'${state === "committed" ? ",clock_timestamp()" : ""})`;

describe.skipIf(!url)("062320 two-connection orderings (real PostgreSQL)", () => {
  test("1. install waits for an in-flight admission; then refuses on its pending batch, or proceeds once it committed", async () => {
    const db = await database();
    const p = await pair(db);
    // The admission: the entry check (shared key) and the batch INSERT (backstop trigger, same key) in one transaction.
    const admitPending = async (c: any) => { await c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG]); await c.query(insertBatch("pending", "b-pending")); };
    const r1 = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: admitPending, wait: (c) => install(c) });
    expect(r1.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r1.waited).toMatchObject({ ok: false, error: { code: "55000" } }); // the committed pending batch refuses install
    const o = await owner(db);
    await o.query("UPDATE sessions.ingest_batches SET state='committed', committed_at=clock_timestamp() WHERE batch_id='b-pending'");
    const admitCommitted = async (c: any) => { await c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG]); await c.query(insertBatch("committed", "b-done")); };
    const r2 = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: admitCommitted, wait: (c) => install(c) });
    expect(r2.waited.ok).toBe(true); // nothing pending: the hold installs after the admission ended
  });

  test("2. an admission waits for an install, then gets RH042 (fresh snapshot after the grant)", async () => {
    const db = await database();
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => install(c),
      wait: (c) => c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG]) });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited).toMatchObject({ ok: false, error: { code: "RH042" } });
    // The backstop on a direct batch INSERT, too.
    const o = await owner(db);
    await expect(o.query(insertBatch("pending", "late"))).rejects.toMatchObject({ code: "RH042" });
  });

  test("3. seal waits for an in-flight publication; publication is refused after the seal", async () => {
    const db = await database();
    const o = await owner(db);
    const e = await install(o);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b,
      hold: (c) => c.query("SELECT sessions.ingest_org_admission_hold_check($1,'publication')", [ORG]),
      wait: (c) => c.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [ORG, e]) });
    expect(r.waited.ok).toBe(true);
    await expect(o.query("SELECT sessions.ingest_org_admission_hold_check($1,'publication')", [ORG])).rejects.toMatchObject({ code: "RH042" });
  });

  test("4. release waits for an in-flight erase; an erasure committed after the bridge's count refuses bridge_complete", async () => {
    const db = await database();
    const o = await owner(db);
    const e = await install(o);
    await o.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [ORG, e]);
    await o.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,NULL)", [ORG, e]);
    const seen = Number((await o.query("SELECT (sessions.ingest_org_admission_hold_status($1)->>'erasures')::bigint n", [ORG])).rows[0].n);
    const p = await pair(db);
    // The erase: erasure-class check (shared key) and its durable record, in one transaction.
    const erase = async (c: any) => {
      await c.query("SELECT sessions.ingest_org_admission_hold_check($1,'erasure')", [ORG]);
      await c.query("INSERT INTO sessions.operator_user_erasures(request_id,org_id,workspace_id,user_id,state,origins) VALUES('erase-1',$1,'ws','u','purging','{}')", [ORG]);
    };
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: erase,
      wait: (c) => c.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)", [ORG, e, EVIDENCE, seen]) });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited).toMatchObject({ ok: false, error: { code: "55000" } }); // erasures not replayed: the count moved
    await o.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)", [ORG, e, EVIDENCE, seen + 1]);
  });

  test("5. the mirror gate is the role: a non-mirror login is refused while sealed (GUC or not), a mirror member passes", async () => {
    const db = await database();
    const o = await owner(db);
    const e = await install(o);
    await o.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,NULL)", [ORG, e]);
    const su = new pg.Client({ connectionString: db.fixtureUrl }); await su.connect(); cleanups.push(() => su.end());
    const tag = db.name.slice(-8);
    const plain = `worker_plain_${tag}`, mirror = `worker_mirror_${tag}`;
    await su.query(`CREATE ROLE ${plain} LOGIN IN ROLE bulls96_session_ingest`);
    await su.query(`DO $m$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_session_ingest_mirror') THEN CREATE ROLE bulls96_session_ingest_mirror NOLOGIN; END IF; END $m$`);
    await su.query(`CREATE ROLE ${mirror} LOGIN IN ROLE bulls96_session_ingest, bulls96_session_ingest_mirror`);
    cleanups.push(async () => { await su.query(`DROP ROLE IF EXISTS ${plain}`); await su.query(`DROP ROLE IF EXISTS ${mirror}`); });
    const control = async (user: string, guc: boolean) => {
      const c = new pg.Client({ connectionString: loginUrl(db, user) }); await c.connect();
      try {
        await c.query("BEGIN");
        if (guc) await c.query("SELECT set_config('bulls96.s3_fence_mirror','on',true)");
        await c.query("SET LOCAL ROLE bulls96_session_ingest");
        await c.query("SELECT sessions.ingest_set_session_uploads($1::jsonb)", [JSON.stringify({ tenant: { orgId: ORG, workspaceId: "ws", userId: "u" } })]);
        return "passed";
      } catch (x) { return String((x as { code?: unknown }).code); } finally { await c.query("ROLLBACK").catch(() => {}); await c.end(); }
    };
    expect(await control(plain, false)).toBe("RH042");
    expect(await control(plain, true)).toBe("RH042"); // an authorized direct SQL caller setting the GUC is still refused
    expect(await control(mirror, false)).not.toBe("RH042"); // past the gate (the empty selection then refuses 22023)
  });

  test("6. REPEATABLE READ and SERIALIZABLE admissions are refused 40001 before reading the hold", async () => {
    const db = await database();
    const o = await owner(db);
    await install(o);
    for (const level of ["REPEATABLE READ", "SERIALIZABLE"]) {
      await o.query(`BEGIN ISOLATION LEVEL ${level}`);
      await expect(o.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG])).rejects.toMatchObject({ code: "40001" });
      await o.query("ROLLBACK");
    }
  });

  test("7a. the down waits for an in-flight install and then refuses: a hold is live", async () => {
    const db = await database();
    const p = await pair(db);
    const body = DOWN.replace(/^BEGIN;$/mu, "").replace(/^COMMIT;$/mu, "");
    // The install holds (961996,1) SHARED and the org key; the down waits on (961996,1), then refuses on the live hold.
    const a = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => install(c), wait: (c) => c.query(body) });
    expect(a.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(a.waited).toMatchObject({ ok: false, error: { code: "55000" } });
  });

  test("7b. a lifecycle call behind a down in progress refuses once the down commits (fresh slot check)", async () => {
    // One scratch database at a time: the migrator's role check refuses while another database grants to the runtime roles.
    const db = await database();
    const p = await pair(db);
    const body = DOWN.replace(/^BEGIN;$/mu, "").replace(/^COMMIT;$/mu, "");
    // No live hold: the down holds (961996,1) EXCLUSIVE through its DDL; an install waits on it; after the down commits,
    // the install refuses (its fresh slot check finds no ledger row, or its function is gone) and writes nothing.
    const b = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(body), wait: (c) => install(c, "org-after-down") });
    expect(b.holderEnded.ok).toBe(true);
    expect(b.waited.ok).toBe(false);
    expect(["55000", "42883"]).toContain((b.waited as { error: { code: string } }).error.code);
    const o = await owner(db);
    expect((await o.query("SELECT count(*)::int n FROM sessions.ingest_org_admission_holds WHERE held_org='org-after-down'")).rows[0].n).toBe(0);
    // The history survived the down (retained, inert).
    expect((await o.query("SELECT to_regclass('sessions.ingest_org_admission_hold_events') IS NOT NULL ok")).rows[0].ok).toBe(true);
  });

  test("8. an install never blocks another org's admission", async () => {
    const db = await database();
    const p = await pair(db);
    const r = await expectNotBlocked(p, { holder: p.a, waiter: p.b, hold: (c) => install(c),
      wait: (c) => c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [OTHER]) });
    expect(r.waited.ok).toBe(true);
  });
});
