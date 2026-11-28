/**
 * hold-lifecycle.ts on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable 15/16/17), two
 * independent connections, every block proven from pg_stat_activity/pg_locks (hold-harness), never from timing:
 *   1a/1b an install and a canonical commit serialize on the org key, both orders (canonical history rolls the install
 *         back; an install in flight makes the commit RH042).
 *   2a/2b a rollback and a marker attempt, both orders: whichever commits first wins, the other refuses.
 *   3a-3d gap C against the REAL sessions.ingest_seed_scope_capture, called as a login in bulls96_session_ingest_seeder:
 *         post-062390, capture first makes the install wait and refuse the committed lease (no hold row, no installed
 *         event, the lease row unchanged); install first makes the lease capture wait and get RH042 with zero capture
 *         rows; a preview capture is never fenced and writes nothing. Pre-062390 (the slot rolled back by its real down,
 *         which refuses captures until readiness is restored) the lease capture is NOT fenced (characterization).
 * The tool's own logic (holdRun) runs as the waiter; its per-org transaction is a SAVEPOINT inside the harness's
 * transaction, so a refusal rolls its statements back exactly as the tool's own transaction would.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { fixturePgUrl, migrateAsOwner, rollBackLaterSlots, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
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
/** The real seeder path (as 062390's own test sets it up): a source epoch row, the passed generation, a seeder login. */
const SEEDER_TX = "BEGIN; SET LOCAL ROLE bulls96_session_ingest_seeder";
const SLOT_062390 = "20261105062390_ingest_seed_capture_admission";
async function seeding(db: ScratchDatabase) {
  const o = await owner(db);
  const generation = (await o.query("SELECT generation FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass'")).rows[0].generation as string;
  await o.query("INSERT INTO sessions.ingest_seed_source_epochs VALUES($1,'ws','claude','sess-1',1) ON CONFLICT DO NOTHING", [ORG]);
  const su = new pg.Client({ connectionString: db.fixtureUrl }); await su.connect(); cleanups.push(() => su.end());
  const seeder = `seeder_${db.name.slice(-8)}`;
  await su.query(`CREATE ROLE ${seeder} LOGIN IN ROLE bulls96_session_ingest_seeder`);
  cleanups.push(async () => { await su.query(`DROP ROLE IF EXISTS ${seeder}`); });
  const u = new URL(db.fixtureUrl); u.username = seeder;
  const seeders = await openPair({ url: u.href, observerUrl: db.fixtureUrl }); cleanups.push(() => seeders.close());
  return { o, generation, seeders, owners: await pair(db) };
}
const capture = (generation: string, mode: "lease" | "preview") => (c: Conn) => c.query("SELECT sessions.ingest_seed_scope_capture($1::jsonb) AS r",
  [JSON.stringify({ org_id: ORG, workspace_id: "ws", source: "claude", session_id: "sess-1", generation, mode })]);
const captures = async (o: pg.Client) => (await o.query("SELECT count(*)::int n FROM sessions.ingest_seed_captures WHERE org_id=$1", [ORG])).rows[0].n as number;
/** Seeder and owner pairs share one cluster: either observer sees both connections' locks. */
const both = (s: Pair, w: Pair): Pair => ({ a: s.a, b: w.a, observer: s.observer, close: async () => {} });
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

  test("3a. post-062390, capture first: the install waits on the in-flight lease capture, then refuses the committed lease whole", async () => {
    const db = await database();
    const { o, generation, seeders, owners } = await seeding(db);
    const r = await expectBlocks(both(seeders, owners), { holder: seeders.a, waiter: owners.a,
      hold: { op: capture(generation, "lease"), begin: SEEDER_TX }, wait: { op: installVia, begin: RC } });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.holderEnded.ok).toBe(true);
    expect(r.waited.ok).toBe(true);
    expect((r.waited as any).value.per_org[0]).toMatchObject({ verdict: "refused", refused: "hold_seed_capture_active" });
    expect((r.waited as any).value.per_org[0].seed_captures).toBe(1);
    // The refusal is the tool's own, inside its transaction before the harness's COMMIT: no hold row, no installed
    // event, and the original lease row intact (same id and expiry as the capture returned).
    const lease = (r.held as any).rows[0].r;
    expect((await o.query("SELECT count(*)::int n FROM sessions.ingest_org_admission_holds")).rows[0].n).toBe(0);
    expect((await o.query("SELECT count(*)::int n FROM sessions.ingest_org_admission_hold_events")).rows[0].n).toBe(0);
    const rows = (await o.query("SELECT id::text id, to_jsonb(expires_at) #>> '{}' x FROM sessions.ingest_seed_captures WHERE org_id=$1", [ORG])).rows;
    expect(rows).toEqual([{ id: String(lease.capture_id), x: lease.expires_at }]);
    expect((await status(o)).hold).toBeNull();
  });

  test("3b. post-062390, install first: the lease capture waits on the in-flight install, then gets RH042 with zero lease writes", async () => {
    const db = await database();
    const { o, generation, seeders, owners } = await seeding(db);
    const r = await expectBlocks(both(owners, seeders), { holder: owners.a, waiter: seeders.a,
      hold: { op: installVia, begin: RC }, wait: { op: capture(generation, "lease"), begin: SEEDER_TX } });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect((r.held as any).per_org[0].verdict).toBe("installed");
    expect(r.waited).toMatchObject({ ok: false, error: { code: "RH042" } });
    expect(await captures(o)).toBe(0);
    expect((await status(o)).hold.state).toBe("held");
  });

  test("3c. post-062390, preview stays metadata-only: never fenced by an install in flight, and writes no capture", async () => {
    const db = await database();
    const { o, generation, seeders, owners } = await seeding(db);
    const r = await expectNotBlocked(both(owners, seeders), { holder: owners.a, waiter: seeders.a,
      hold: { op: installVia, begin: RC }, wait: { op: capture(generation, "preview"), begin: SEEDER_TX } });
    expect((r.held as any).per_org[0].verdict).toBe("installed");
    expect(r.waited.ok).toBe(true);
    expect(await captures(o)).toBe(0);
    expect((await status(o)).hold.state).toBe("held");
  });

  test("3d. pre-062390 CHARACTERIZATION (the slot rolled back by its real down): a lease capture is NOT fenced by the install", async () => {
    const db = await database();
    const { o, generation, seeders, owners } = await seeding(db);
    expect(await rollBackLaterSlots(db.ownerUrl, "20261105062389")).toEqual(["upstream:" + SLOT_062390 + ".sql"]);
    // The down resets readiness to pending, so a lease capture is refused outright until revalidation...
    const s = seeders.a; await s.query(SEEDER_TX);
    expect(await capture(generation, "lease")(s).then(() => "ok", (e: any) => e.code)).toBe("55000");
    await s.query("ROLLBACK");
    // ...so, test-only and in this disposable database, the fixture superuser restores the same passed generation to
    // exercise the pre-062390 body itself (no other row or function is touched).
    const su = new pg.Client({ connectionString: db.fixtureUrl }); await su.connect(); cleanups.push(() => su.end());
    expect((await su.query("UPDATE sessions.__96bulls_role_validation SET status='pass' WHERE name='session_directory_search_worker' AND generation=$1", [generation])).rowCount).toBe(1);
    const r = await expectNotBlocked(both(owners, seeders), { holder: owners.a, waiter: seeders.a,
      hold: { op: installVia, begin: RC }, wait: { op: capture(generation, "lease"), begin: SEEDER_TX } });
    expect((r.held as any).per_org[0].verdict).toBe("installed");
    expect(r.waited.ok).toBe(true);
    expect(await captures(o)).toBe(1);
    expect((await status(o)).hold.state).toBe("held");
  });
});
