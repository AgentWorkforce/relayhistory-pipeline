/**
 * The enrollment boundary for in-flight external-send jobs, on real PostgreSQL (SESSION_INGEST_TEST_PG_URL;
 * hold-harness run.ts). ISOLATED SYNTHETIC PROOF of the design in docs/design/org-bridge-gate.md ("The enrollment
 * boundary for in-flight external-send jobs"); the sender is a test stand-in, no product code. Blocks are proven
 * from pg_locks / pg_stat_activity (hold-harness), never from timing.
 *   1 a sender past its check (holding the org hold key SHARED until its send returns) blocks _install; the install
 *     commits only after the send, so "enroll, erase, resume and send" cannot interleave
 *   2 a sender that starts after the install refuses before any read (the live hold, read under the key)
 *   3 an erase (its 062340 trigger takes the key SHARED) is never blocked by an in-flight sender
 *   4 COUNTEREXAMPLE (lander 6b1d1deebc42): the sender passes its check, its DB session is terminated (the SHARED key
 *     goes with it), the install commits, then the sender's process resumes and sends to a controlled receiver. The
 *     receiver observes a request initiated AFTER the install: the DB lock cannot bound the external boundary.
 * Not run by its author: needs its own reserved PG17 slot (claim-ci).
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, expectNotBlocked, openPair, type Conn, type Pair } from "./hold-harness/two-conn.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "org-send";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<{ db: ScratchDatabase; o: pg.Client }> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "csend" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  const o = new pg.Client({ connectionString: db.ownerUrl }); await o.connect();
  cleanups.push(() => o.end());
  return { db, o };
}
async function pair(db: ScratchDatabase): Promise<Pair> {
  const p = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => p.close());
  return p;
}
/** The sender stand-in's check, inside the caller's transaction: the key SHARED, then the hold read in a later statement. */
async function senderCheck(c: Conn): Promise<"may_send" | "refused_enrolled"> {
  await c.query("SELECT pg_advisory_xact_lock_shared(sessions.ingest_org_admission_hold_key($1))", [ORG]);
  const held = (await c.query("SELECT EXISTS (SELECT 1 FROM sessions.ingest_org_admission_holds WHERE held_org = $1 AND (state <> 'released' OR release_reason = 'bridge_complete')) AS h", [ORG])).rows[0].h;
  return held ? "refused_enrolled" : "may_send";
}
const install = (c: Conn | pg.Client) => c.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [ORG]);
const deletion = `INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES('${ORG}','ws','u','o','claude','s-1')`;

describe.skipIf(!url)("external-send fence at the enrollment boundary (real PostgreSQL)", () => {
  test("1. a sender past its check blocks the install until its send returns", async () => {
    const { db } = await database();
    const p = await pair(db);
    const sends: string[] = [];
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b,
      // The holder passed its check and is "sending" (its transaction, and the SHARED key, stay open).
      hold: async (c) => { const v = await senderCheck(c); if (v === "may_send") sends.push("pre-enrollment"); return v; },
      wait: (c) => install(c) });
    expect(r.held).toBe("may_send");
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true); // the install committed only after the send's transaction ended
    expect(sends).toEqual(["pre-enrollment"]);
  });

  test("2. a sender starting after the install refuses before any read", async () => {
    const { db, o } = await database();
    await install(o);
    const p = await pair(db);
    await p.a.query("BEGIN");
    expect(await senderCheck(p.a)).toBe("refused_enrolled");
    await p.a.query("ROLLBACK");
  });

  test("3. an erase is never blocked by an in-flight sender (both take the key SHARED)", async () => {
    const { db } = await database();
    const p = await pair(db);
    const r = await expectNotBlocked(p, { holder: p.a, waiter: p.b, hold: (c) => senderCheck(c), wait: (c) => c.query(deletion) });
    expect(r.waited.ok).toBe(true);
  });

  test("4. counterexample: a sender whose DB session is terminated after its check still sends after the install commits", async () => {
    const { db, o } = await database();
    const p = await pair(db);
    // A controlled receiver: what it observes, in order, against the install's commit.
    const received: { at: "before_install" | "after_install" }[] = [];
    let installed = false;
    const receiver = (input: string) => { void input; received.push({ at: installed ? "after_install" : "before_install" }); };
    await p.a.query("BEGIN");
    expect(await senderCheck(p.a)).toBe("may_send");
    // The sender's DB session dies (a crash, a network partition, an operator kill): its transaction and key go.
    await p.observer.query("SELECT pg_terminate_backend($1)", [p.a.pid]);
    // The install no longer waits: it commits.
    await install(o);
    installed = true;
    // The sender's process resumes and sends: nothing in the DB can stop it.
    receiver("pre-enrollment content");
    expect(received).toEqual([{ at: "after_install" }]);
  });
});
