/**
 * hold-lifecycle.ts on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable 15/16/17), two
 * independent connections, every block proven from pg_stat_activity/pg_locks (hold-harness), never from timing:
 *   1 an install and an open canonical claim serialize on the org key: the install waits for the claim, then sees it
 *     and rolls back whole (hold_canonical_open, no hold row); the other way round, the claim's entry check waits for
 *     the install and gets RH042.
 *   2 a rollback racing a marker attempt: the rollback waits for the attempt and then refuses (no rollback after an
 *     attempt); the other way round, the attempt waits for the rollback and then refuses (the epoch is released).
 * The tool's own logic (holdRun) runs as the waiter; its per-org transaction is a SAVEPOINT inside the harness's
 * transaction, so a refusal rolls its statements back exactly as the tool's own transaction would.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, openPair, type Conn, type Pair } from "./hold-harness/two-conn.ts";
import { holdRun, type HoldSqlSession } from "../hold-lifecycle.ts";
import { epochHash } from "../erasure-window.ts";
import { evidenceHash } from "../authority-evidence.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "5e5e5e5e-0000-4000-8000-0000000000c1";
const SHA = "1".repeat(40), DRY = "901";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<ScratchDatabase> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "holdlife" });
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
/** holdRun's session on a harness connection: its transaction is a savepoint inside the harness's transaction. */
const harnessSession = (c: Conn): HoldSqlSession => {
  const q = async (t: string, p: readonly unknown[]) => (await c.query(t, [...p])).rows;
  return { read: q, async tx(fn) {
    await c.query("SAVEPOINT hold_step");
    try { const v = await fn(q); await c.query("RELEASE SAVEPOINT hold_step"); return v; }
    catch (e) { await c.query("ROLLBACK TO SAVEPOINT hold_step"); throw e; }
  } };
};
const RC = "BEGIN ISOLATION LEVEL READ COMMITTED";
const status = async (o: pg.Client) => (await o.query("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).rows[0].s;
const installVia = (c: Conn) => holdRun({ step: "install", orgs: [ORG], sha: SHA, dryRun: DRY, approved: [{ orgHash: evidenceHash("org", ORG), epochHash: null }],
  withSession: (fn) => fn(harnessSession(c)), now: () => Date.now(), concurrency: 1 });
/** An open canonical claim: the admission entry check (shared key), then a committed non-buffered batch and its claim attempt. */
const canonicalClaim = async (c: Conn) => {
  await c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG]);
  await c.query(`INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at)
    VALUES($1,'ws','o','b-canon','u',$2,$3,'committed',clock_timestamp())`, [ORG, "d".repeat(64), "rhr_" + "f".repeat(64)]);
  await c.query(`INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs)
    VALUES(gen_random_uuid(),$1,'ws','u','o','b-canon',$2,'[]'::jsonb)`, [ORG, "d".repeat(64)]);
};

describe.skipIf(!url)("hold lifecycle two-connection orderings (real PostgreSQL)", () => {
  test("1. an install and an open canonical claim serialize on the org key, both ways", async () => {
    const db = await database();
    const p = await pair(db);
    const o = await owner(db);
    // claim holds the key SHARED; the install (EXCLUSIVE) waits, then sees the claim and rolls back whole.
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: canonicalClaim, begin: RC }, wait: { op: installVia, begin: RC } });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    expect((r.waited as any).value.per_org[0]).toMatchObject({ verdict: "refused", refused: "hold_canonical_open" });
    expect((await status(o)).hold).toBeNull(); // no hold row survived
    // The other way: an install holds the key; the claim's entry check waits, then gets RH042.
    await o.query("DELETE FROM sessions.ingest_claim_attempts WHERE org_id=$1", [ORG]);
    const r2 = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: installVia, begin: RC },
      wait: { op: (c) => c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG]), begin: RC } });
    expect((r2.held as any).per_org[0].verdict).toBe("installed");
    expect(r2.waited).toMatchObject({ ok: false, error: { code: "RH042" } });
    expect((await status(o)).hold.state).toBe("held");
  });

  test("2. a rollback racing a marker attempt: whichever commits first wins; the other refuses", async () => {
    const db = await database();
    const o = await owner(db);
    const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [ORG])).rows[0].e as string;
    await o.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,$3)", [ORG, e, "b".repeat(64)]);
    const p = await pair(db);
    const rollbackVia = (c: Conn) => holdRun({ step: "rollback", orgs: [ORG], sha: SHA, dryRun: DRY, evidence: "d".repeat(64),
      approved: [{ orgHash: evidenceHash("org", ORG), epochHash: epochHash(e) }], withSession: (fn) => fn(harnessSession(c)), now: () => Date.now(), concurrency: 1 });
    const attempt = (c: Conn) => c.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,$3)", [ORG, e, "c".repeat(64)]);
    // The attempt holds the key; the rollback waits and then refuses once the attempt committed.
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: attempt, begin: RC }, wait: { op: rollbackVia, begin: RC } });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    expect(["hold_marker_attempted", "hold_transition_refused"]).toContain((r.waited as any).value.per_org[0].refused);
    expect((await status(o)).hold.state).toBe("marker_attempted");
    // A fresh epoch for the other order: the rollback holds the key; the attempt waits and then refuses (released).
    const db2 = await database();
    const o2 = await owner(db2);
    const e2 = (await o2.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [ORG])).rows[0].e as string;
    await o2.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,$3)", [ORG, e2, "b".repeat(64)]);
    const p2 = await pair(db2);
    const rollback2 = (c: Conn) => holdRun({ step: "rollback", orgs: [ORG], sha: SHA, dryRun: DRY, evidence: "d".repeat(64),
      approved: [{ orgHash: evidenceHash("org", ORG), epochHash: epochHash(e2) }], withSession: (fn) => fn(harnessSession(c)), now: () => Date.now(), concurrency: 1 });
    const r2 = await expectBlocks(p2, { holder: p2.a, waiter: p2.b, hold: { op: rollback2, begin: RC },
      wait: { op: (c) => c.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,$3)", [ORG, e2, "c".repeat(64)]), begin: RC } });
    expect((r2.held as any).per_org[0].verdict).toBe("rolled_back");
    expect(r2.waited).toMatchObject({ ok: false, error: { code: "55000" } });
    expect((await status(o2)).hold.state).toBe("released"); // the latest (released) hold
  });
});
