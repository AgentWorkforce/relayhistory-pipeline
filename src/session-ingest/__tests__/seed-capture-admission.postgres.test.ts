/**
 * 20261105062390 seed capture lease issuance vs the org admission hold install, on real PostgreSQL
 * (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable 15/16/17), two independent connections, every block proven from
 * pg_locks (hold-harness), never from timing. The seeder calls run as a login in bulls96_session_ingest_seeder.
 *   1 capture first: an install waits for an in-flight lease capture (it holds the org key SHARED), then installs; the
 *     installer's post-key active-lease check then sees the committed lease (the wave tooling rolls the install back)
 *   2 install first: a lease capture waits for the in-flight install, then is refused RH042 with zero captures
 *   3 an unrelated org's capture never waits on an install
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, expectNotBlocked, openPair, type Pair } from "./hold-harness/two-conn.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "org-hold", OTHER = "org-free", WS = "ws";
const SEEDER_TX = "BEGIN; SET LOCAL ROLE bulls96_session_ingest_seeder";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database() {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "seedcap" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  const o = new pg.Client({ connectionString: db.ownerUrl }); await o.connect(); cleanups.push(() => o.end());
  const generation = (await o.query("SELECT generation FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass'")).rows[0].generation as string;
  for (const org of [ORG, OTHER]) await o.query("INSERT INTO sessions.ingest_seed_source_epochs VALUES($1,$2,'claude','sess-1',1) ON CONFLICT DO NOTHING", [org, WS]);
  const su = new pg.Client({ connectionString: db.fixtureUrl }); await su.connect(); cleanups.push(() => su.end());
  const seeder = `seeder_${db.name.slice(-8)}`;
  await su.query(`CREATE ROLE ${seeder} LOGIN IN ROLE bulls96_session_ingest_seeder`);
  cleanups.push(async () => { await su.query(`DROP ROLE IF EXISTS ${seeder}`); });
  const u = new URL(db.fixtureUrl); u.username = seeder;
  const seeders = await openPair({ url: u.href, observerUrl: db.fixtureUrl }); cleanups.push(() => seeders.close());
  const owners = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl }); cleanups.push(() => owners.close());
  return { db, o, generation, seeders, owners };
}
const captureSql = (generation: string, org = ORG) => ({
  text: "SELECT sessions.ingest_seed_scope_capture($1::jsonb) AS r",
  params: [JSON.stringify({ org_id: org, workspace_id: WS, source: "claude", session_id: "sess-1", generation, mode: "lease" })],
});
const installSql = (org = ORG) => ({ text: "SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", params: [org] });
const activeLeases = async (o: pg.Client, org = ORG) =>
  Number((await o.query("SELECT count(*)::int n FROM sessions.ingest_seed_captures WHERE org_id=$1 AND expires_at>clock_timestamp()", [org])).rows[0].n);
/** Two pairs share one cluster: the observer of either sees both connections' locks. */
const both = (s: Pair, w: Pair): Pair => ({ a: s.a, b: w.a, observer: s.observer, close: async () => {} });

describe.skipIf(!url)("062390 seed capture vs install (real PostgreSQL)", () => {
  test("1. capture first: the install waits for the in-flight lease capture, then the post-key check sees the lease", async () => {
    const { o, generation, seeders, owners } = await database();
    const c = captureSql(generation), i = installSql();
    const r = await expectBlocks(both(seeders, owners), {
      holder: seeders.a, waiter: owners.a,
      hold: { begin: SEEDER_TX, op: (x) => x.query(c.text, c.params) },
      wait: (x) => x.query(i.text, i.params),
    });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.holderEnded.ok).toBe(true);
    expect(r.waited.ok).toBe(true); // the hold installs once the capture has committed
    // The installer's post-key active-lease check (sessions-wave-tooling) refuses here: a committed lease exists.
    expect(await activeLeases(o)).toBe(1);
  });

  test("2. install first: the lease capture waits for the in-flight install, then is refused RH042 with zero captures", async () => {
    const { o, generation, seeders, owners } = await database();
    const c = captureSql(generation), i = installSql();
    const r = await expectBlocks(both(owners, seeders), {
      holder: owners.a, waiter: seeders.a,
      hold: (x) => x.query(i.text, i.params),
      wait: { begin: SEEDER_TX, op: (x) => x.query(c.text, c.params) },
    });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited).toMatchObject({ ok: false, error: { code: "RH042" } });
    expect(Number((await o.query("SELECT count(*)::int n FROM sessions.ingest_seed_captures WHERE org_id=$1", [ORG])).rows[0].n)).toBe(0);
  });

  test("3. an unrelated org's lease capture never waits on an install", async () => {
    const { o, generation, seeders, owners } = await database();
    const c = captureSql(generation, OTHER), i = installSql(ORG);
    const r = await expectNotBlocked(both(owners, seeders), {
      holder: owners.a, waiter: seeders.a,
      hold: (x) => x.query(i.text, i.params),
      wait: { begin: SEEDER_TX, op: (x) => x.query(c.text, c.params) },
    });
    expect(r.waited.ok).toBe(true);
    expect(await activeLeases(o, OTHER)).toBe(1);
  });
});
