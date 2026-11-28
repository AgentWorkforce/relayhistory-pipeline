/**
 * The empty-window bridge release (erasure-window.ts releaseEmptyWindowIn) on real PostgreSQL (SESSION_INGEST_TEST_PG_URL;
 * hold-harness/run.ts gives disposable 15/16/17), two independent connections, every block proven from pg_locks
 * (hold-harness), never from timing. Synthetic, disposable scratch databases only.
 *   1 erase first: the release waits at its lock statement (EMPTY_RELEASE_LOCK_SQL, the org key EXCLUSIVE) for an
 *     in-flight erasure write, then its final proof read (ERASURE_WINDOW_SQL, after the key) sees the write: refused
 *     release_window_nonempty, nothing written, the org stays held
 *   2 release first: an erasure write waits for the in-flight release (the recorder's SHARED org key), resumes after it
 *     commits, and takes effect under post-release authority (recorded after released_at)
 *   3 an erasure through the checked entry point ('erasure' class, never refused) is serialized the same way
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, openPair, type Pair } from "./hold-harness/two-conn.ts";
import { EMPTY_RELEASE_LOCK_SQL, orgHash, releaseEmptyWindowIn, type EmptyReleaseInput, type EmptyReleaseResult } from "../erasure-window.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "77777777-2222-4333-8444-555555555555";
const SEAL = "b".repeat(64), FULL = "444", RC = "BEGIN ISOLATION LEVEL READ COMMITTED";
/** The synthetic org is the test's wave (the head's own wave list is empty). */
const WAVE = [orgHash(ORG)];
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<ScratchDatabase> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "erel" });
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
/** install, seal (the FULL receipt), bind the FULL (1 ms after the install), attempt the marker: the release input. */
async function toAttempt(o: pg.Client): Promise<EmptyReleaseInput> {
  const e = (await o.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).rows[0].e as string;
  await o.query("SELECT pg_sleep(0.003)");
  await o.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [ORG, e]);
  const started = Number((await o.query("SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 AS ms FROM sessions.ingest_org_admission_holds WHERE epoch=$1::uuid", [e])).rows[0].ms);
  await o.query("SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,$3,$4::bigint,$5)", [ORG, e, FULL, String(started), SEAL]);
  await o.query(`SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,'${"c".repeat(64)}')`, [ORG, e]);
  await o.query("SELECT pg_sleep(0.003)"); // the final read is after the seal
  return { org: ORG, epoch: e, cutoffMs: started - 15 * 60_000, fullRun: FULL, fullEvidence: SEAL, fullStartedMs: started };
}
const hold = async (o: pg.Client, e: string) =>
  (await o.query("SELECT state, release_reason, released_at FROM sessions.ingest_org_admission_holds WHERE epoch=$1::uuid", [e])).rows[0];
const watermark = async (o: pg.Client) => Number((await o.query("SELECT (sessions.ingest_org_admission_hold_status($1)->>'erasures')::bigint n", [ORG])).rows[0].n);

describe.skipIf(!url)("empty-window release, two-connection orderings (real PostgreSQL)", () => {
  test("1. erase first: the release waits at the org-key lock, then its final proof read sees the erase: refused, the org stays held", async () => {
    const db = await database();
    const o = await owner(db);
    const input = await toAttempt(o);
    expect(await watermark(o)).toBe(0);
    const p = await pair(db);
    const r = await expectBlocks<unknown, EmptyReleaseResult>(p, { holder: p.a, waiter: p.b, hold: (c) => c.query(deletion("s-erase-first")),
      wait: { begin: RC, op: (c) => releaseEmptyWindowIn(c, input, Date.now, WAVE) } });
    // Blocked on the advisory org key, at the lock statement: before any proof read.
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.receipt.query).toBe(EMPTY_RELEASE_LOCK_SQL);
    expect(r.holderEnded.ok).toBe(true);
    expect(r.waited.ok).toBe(true);
    if (!r.waited.ok) return;
    expect(r.waited.value).toMatchObject({ status: "refused", code: "release_window_nonempty" });
    expect(r.waited.value.proof?.watermark_erasures).toBe(1);
    expect((await hold(o, input.epoch)).state).toBe("marker_attempted");
    expect(await watermark(o)).toBe(1);
  });

  test("2. release first: the erase waits for the in-flight release, then takes effect under post-release authority", async () => {
    const db = await database();
    const o = await owner(db);
    const input = await toAttempt(o);
    const p = await pair(db);
    const r = await expectBlocks<EmptyReleaseResult, unknown>(p, { holder: p.a, waiter: p.b, hold: { begin: RC, op: (c) => releaseEmptyWindowIn(c, input, Date.now, WAVE) },
      wait: (c) => c.query(deletion("s-release-first")) });
    expect(r.held).toMatchObject({ status: "released" });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.holderEnded.ok).toBe(true);
    expect(r.waited.ok).toBe(true);
    const h = await hold(o, input.epoch);
    expect([h.state, h.release_reason]).toEqual(["released", "bridge_complete"]);
    // The erase is in effect (its row is there) and was recorded after the release committed.
    expect(Number((await o.query("SELECT count(*) n FROM sessions.upload_deletions WHERE org_id=$1 AND session_id='s-release-first'", [ORG])).rows[0].n)).toBe(1);
    const after = (await o.query(`SELECT (SELECT released_at FROM sessions.ingest_org_admission_holds WHERE epoch=$1::uuid) < max(h.at) AS after
                                   FROM sessions.ingest_org_erasure_history h WHERE h.erased_org=$2`, [input.epoch, ORG])).rows[0].after;
    expect(after).toBe(true);
  });

  test("3. the checked erasure entry ('erasure' class) is serialized the same way: erase first refuses the release", async () => {
    const db = await database();
    const o = await owner(db);
    const input = await toAttempt(o);
    const p = await pair(db);
    const r = await expectBlocks<unknown, EmptyReleaseResult>(p, { holder: p.a, waiter: p.b,
      hold: async (c) => { await c.query("SELECT sessions.ingest_org_admission_hold_check($1,'erasure')", [ORG]); return c.query(deletion("s-checked")); },
      wait: { begin: RC, op: (c) => releaseEmptyWindowIn(c, input, Date.now, WAVE) } });
    expect(r.receipt.query).toBe(EMPTY_RELEASE_LOCK_SQL);
    expect(r.waited.ok && r.waited.value).toMatchObject({ status: "refused", code: "release_window_nonempty" });
    expect((await hold(o, input.epoch)).state).toBe("marker_attempted");
  });
});
