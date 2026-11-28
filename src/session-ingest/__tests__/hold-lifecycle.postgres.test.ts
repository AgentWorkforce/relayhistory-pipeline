/**
 * hold-lifecycle.ts on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable 15/16/17), two
 * independent connections, every block proven from pg_stat_activity/pg_locks (hold-harness), never from timing:
 *   1a/1b an install and a canonical commit serialize on the org key, both orders (canonical history rolls the install
 *         back; an install in flight makes the commit RH042).
 *   2a/2b a rollback and a marker attempt, both orders: whichever commits first wins, the other refuses.
 *   3     CHARACTERIZATION of gap C: a seed capture is not fenced by the install's key (expected until the interlock).
 * The tool's own logic (holdRun) runs as the waiter; its per-org transaction is a SAVEPOINT inside the harness's
 * transaction, so a refusal rolls its statements back exactly as the tool's own transaction would.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, expectNotBlocked, openPair, type Conn, type Pair } from "./hold-harness/two-conn.ts";
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
// SYNTHETIC fan-out proof (production has none, and every install refuses before _install without it).
const installVia = (c: Conn) => holdRun({ step: "install", orgs: [ORG], sha: SHA, dryRun: DRY, approved: [{ orgHash: evidenceHash("org", ORG), epochHash: null }],
  withSession: (fn) => fn(harnessSession(c)), now: () => Date.now(), concurrency: 1, fanoutProof: async () => true });
/** A canonical commit in flight: the admission entry check (shared key), then a committed canonical batch row. */
const canonicalCommit = async (c: Conn) => {
  await c.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG]);
  await c.query(`INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state)
    VALUES($1,'ws','o','b-canon','u',$2,$3,'committed',clock_timestamp(),'canonical')`, [ORG, "d".repeat(64), "rhr_" + "f".repeat(64)]);
};
/** A seed capture issued the way 20261104040000's lease mode stores it (its function takes no org key: gap C). */
const seedCapture = async (c: Conn) => {
  const gen = (await c.query("SELECT generation FROM sessions.__96bulls_role_validation WHERE generation IS NOT NULL LIMIT 1")).rows[0]?.generation ?? "a".repeat(64);
  await c.query(`INSERT INTO sessions.ingest_seed_captures(org_id,workspace_id,source,session_id,generation,source_epoch,fingerprint,state)
    VALUES($1,'ws','claude','s1',$2,0,$3,'{}'::jsonb)`, [ORG, gen, "a".repeat(64)]);
};
const sealed = async (db: ScratchDatabase) => {
  const o = await owner(db);
  const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [ORG])).rows[0].e as string;
  await o.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,$3)", [ORG, e, "b".repeat(64)]);
  return { o, e };
};
const rollbackVia = (e: string) => (c: Conn) => holdRun({ step: "rollback", orgs: [ORG], sha: SHA, dryRun: DRY, evidence: "d".repeat(64),
  approved: [{ orgHash: evidenceHash("org", ORG), epochHash: epochHash(e) }], withSession: (fn) => fn(harnessSession(c)), now: () => Date.now(), concurrency: 1 });
const attemptVia = (e: string) => (c: Conn) => c.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,$3)", [ORG, e, "c".repeat(64)]);

// Each ordering builds and drops its OWN migrated database (two concurrent migrated databases in one test trip the
// role verification guard, which stays intact).
describe.skipIf(!url)("hold lifecycle two-connection orderings (real PostgreSQL)", () => {
  test("1a. a canonical commit in flight: the install waits for it, then sees the canonical history and rolls back whole", async () => {
    const db = await database();
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: canonicalCommit, begin: RC }, wait: { op: installVia, begin: RC } });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    expect((r.waited as any).value.per_org[0]).toMatchObject({ verdict: "refused", refused: "hold_canonical_history" });
    expect((await status(await owner(db))).hold).toBeNull();
  });

  test("1b. an install in flight: a canonical commit's entry check waits for it, then gets RH042", async () => {
    const db = await database();
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: installVia, begin: RC }, wait: { op: canonicalCommit, begin: RC } });
    expect((r.held as any).per_org[0].verdict).toBe("installed");
    expect(r.waited).toMatchObject({ ok: false, error: { code: "RH042" } });
    expect((await status(await owner(db))).hold.state).toBe("held");
  });

  test("2a. a marker attempt in flight: a rollback waits for it, then refuses (no rollback after an attempt)", async () => {
    const db = await database();
    const { o, e } = await sealed(db);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: attemptVia(e), begin: RC }, wait: { op: rollbackVia(e), begin: RC } });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    expect(["hold_marker_attempted", "hold_transition_refused"]).toContain((r.waited as any).value.per_org[0].refused);
    expect((await status(o)).hold.state).toBe("marker_attempted");
  });

  test("2b. a rollback in flight: a marker attempt waits for it, then refuses (the epoch is released)", async () => {
    const db = await database();
    const { o, e } = await sealed(db);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: { op: rollbackVia(e), begin: RC }, wait: { op: attemptVia(e), begin: RC } });
    expect((r.held as any).per_org[0].verdict).toBe("rolled_back");
    expect(r.waited).toMatchObject({ ok: false, error: { code: "55000" } });
    expect((await status(o)).hold.state).toBe("released");
  });

  test("3. CHARACTERIZATION of gap C (expected until the issuance interlock slot exists): a seed capture is NOT fenced by the install's key", async () => {
    const db = await database();
    const p = await pair(db);
    // An install holding the key EXCLUSIVE does not block a capture insert (no key taken): the capture commits.
    const r = await expectNotBlocked(p, { holder: p.a, waiter: p.b, hold: { op: installVia, begin: RC }, wait: { op: seedCapture, begin: RC } });
    expect((r.held as any).per_org[0].verdict).toBe("installed");
    expect(r.waited.ok).toBe(true);
    const o = await owner(db);
    expect((await o.query("SELECT count(*)::int n FROM sessions.ingest_seed_captures WHERE org_id=$1", [ORG])).rows[0].n).toBe(1);
    expect((await status(o)).hold.state).toBe("held");
  });
});
