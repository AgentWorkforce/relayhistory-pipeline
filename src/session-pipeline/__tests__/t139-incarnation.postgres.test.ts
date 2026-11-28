/**
 * T139 OFFLINE PROTOTYPE evidence (design f959bd92, revision 3 fb6c7fd4; Pipeline acceptance d1cea47d043d) on REAL
 * PostgreSQL: fixtures/t139-incarnation.prototype.sql (not a migration) over a scratch database migrated with every
 * migration (SESSION_INGEST_TEST_PG_URL; hold-harness). Two independent backends (distinct pg_backend_pid, asserted by
 * openPair) plus an observer. Every interleaving is a pg_locks-observed advisory block (expectBlocks: pg_stat_activity
 * wait_event_type Lock, an ungranted advisory pg_locks row, pg_blocking_pids), never timing.
 *
 * "Publication" here is t139_publish_check inside the publication transaction, plus a marker row written in that same
 * transaction (t139_published) standing in for the pointer upsert. The pointer-stage HEAD and the parsed authorized
 * reads are the PGlite end-to-end fixture's job (a separate checkpoint).
 *
 * Interleavings EXERCISED (each named in its test):
 *   D1 delete-first: deletion holds k EXCLUSIVE; publication is paused BEFORE its shared k (blocked), then refuses.
 *   D2 publish-first: publication holds k SHARED (paused AFTER k, check passed); deletion is blocked; publication commits
 *      first, then deletion commits; a later publication refuses.
 *   D3 no-row race: D1 and D2 with no t139_session_incarnations row for the owner (incarnation 0).
 *   R1 concurrent reissue for one receipt: the second reissue is blocked on k, then issues nothing; exactly one N+1.
 *   E1 erase vs publication, both orders; reissue refused under the fence.
 *   O1 two owners under one logical session id: X's deletion never revokes Y; Y cannot consume X's receipt; the scope is
 *      ambiguous, so reissue is refused without mutation; k still serializes them (an observed block across owners).
 *   O2 single-owner control: only X ever seen, so the reissue succeeds.
 *   S1 stale publication of incarnation N after the reissue to N+1: refused (incarnation_moved).
 */
import { afterAll, afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, type ScratchDatabase } from "../../session-ingest/__tests__/hold-harness/pg.ts";
import { expectBlocks, openPair, type Conn, type Pair } from "../../session-ingest/__tests__/hold-harness/two-conn.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const PROTOTYPE = readFileSync(join(import.meta.dir, "fixtures/t139-incarnation.prototype.sql"), "utf8");
const L = { org: "org_t139", ws: "ws_t139", source: "claude", session: "sess_t139" };
const X = { user: "user_x", origin: "origin_x" };
const Y = { user: "user_y", origin: "origin_y" };
type Owner = typeof X;

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);
afterAll(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); });

interface Rig { db: ScratchDatabase; o: pg.Client; pair: Pair }
async function rig(): Promise<Rig> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "t139" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  const o = new pg.Client({ connectionString: db.ownerUrl }); await o.connect();
  cleanups.push(() => o.end());
  await o.query(PROTOTYPE);
  await o.query("CREATE TABLE sessions.t139_published (owner text NOT NULL, incarnation bigint NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())");
  const pair = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => pair.close());
  expect(pair.a.pid).not.toBe(pair.b.pid);
  return { db, o, pair };
}

const del = (c: Conn | pg.Client, w: Owner) =>
  c.query("SELECT sessions.t139_delete($1,$2,$3,$4,$5,$6) n", [L.org, L.ws, w.user, w.origin, L.source, L.session]).then((r) => Number(r.rows[0].n));
const reissue = (c: Conn | pg.Client, w: Owner) =>
  c.query("SELECT sessions.t139_reissue($1,$2,$3,$4,$5,$6) n", [L.org, L.ws, w.user, w.origin, L.source, L.session]).then((r) => (r.rows[0].n === null ? null : Number(r.rows[0].n)));
const erase = (c: Conn | pg.Client) => c.query("SELECT sessions.t139_erase($1,$2,$3,$4)", [L.org, L.ws, L.source, L.session]).then(() => "erased");
const check = (c: Conn | pg.Client, w: Owner, inc: number) =>
  c.query("SELECT sessions.t139_publish_check($1,$2,$3,$4,$5,$6,$7) v", [L.org, L.ws, L.source, L.session, w.user, w.origin, inc]).then((r) => r.rows[0].v as string);
/** The publication transaction's body: the check, and on 'ok' the marker row (the pointer upsert's stand-in). */
const publish = async (c: Conn | pg.Client, w: Owner, inc: number) => {
  const v = await check(c, w, inc);
  if (v === "ok") await c.query("INSERT INTO sessions.t139_published (owner, incarnation) VALUES ($1, $2)", [w.user, inc]);
  return v;
};
const published = async (o: pg.Client) => (await o.query("SELECT owner, incarnation::int FROM sessions.t139_published ORDER BY at")).rows;
const incarnationRows = async (o: pg.Client) => (await o.query("SELECT user_id, incarnation::int FROM sessions.t139_session_incarnations ORDER BY user_id")).rows;
const receipts = async (o: pg.Client) => (await o.query("SELECT user_id, incarnation::int, consumed_by::int FROM sessions.t139_deletion_receipts ORDER BY user_id, incarnation")).rows;
const advisory = (receipt: { waiting: { locktype?: string }[] }) => expect(receipt.waiting[0]).toMatchObject({ locktype: "advisory" });

describe.skipIf(!url)("T139 incarnation prototype (real PostgreSQL, two backends)", () => {
  test("D1+D3 delete-first, no incarnation row: deletion holds k; publication paused BEFORE shared k is blocked, then refuses (incarnation_deleted); nothing published", async () => {
    const { o, pair } = await rig();
    expect(await incarnationRows(o)).toEqual([]);
    const r = await expectBlocks(pair, { holder: pair.b, waiter: pair.a, hold: (c) => del(c, X), wait: (c) => publish(c, X, 0) });
    advisory(r.receipt);
    expect(r.held).toBe(0);
    expect(r.waited).toMatchObject({ ok: true, value: "incarnation_deleted" });
    expect(await published(o)).toEqual([]);
    expect(await receipts(o)).toEqual([{ user_id: "user_x", incarnation: 0, consumed_by: null }]);
  });

  test("D2+D3 publish-first, no incarnation row: publication holds k SHARED (paused AFTER k, check ok); deletion is blocked; publication commits first; a later publication refuses", async () => {
    const { o, pair } = await rig();
    const r = await expectBlocks(pair, { holder: pair.a, waiter: pair.b, hold: (c) => publish(c, X, 0), wait: (c) => del(c, X) });
    advisory(r.receipt);
    expect(r.held).toBe("ok");
    expect(r.holderEnded.ok).toBe(true);
    expect(r.waited).toMatchObject({ ok: true, value: 0 });
    // Committed order: the publication, then the receipt; never a publication after the delete.
    const pub = (await o.query("SELECT max(at) t FROM sessions.t139_published")).rows[0].t as Date;
    const rec = (await o.query("SELECT deleted_at t FROM sessions.t139_deletion_receipts")).rows[0].t as Date;
    expect(pub.getTime()).toBeLessThan(rec.getTime());
    expect(await publish(o, X, 0)).toBe("incarnation_deleted");
    expect(await published(o)).toEqual([{ owner: "user_x", incarnation: 0 }]);
  });

  test("R1 concurrent reissue for one receipt: the second reissue blocks on k, then issues nothing; exactly one incarnation 1, receipt consumed once", async () => {
    const { o, pair } = await rig();
    await del(o, X);
    const r = await expectBlocks(pair, { holder: pair.a, waiter: pair.b, hold: (c) => reissue(c, X), wait: (c) => reissue(c, X) });
    advisory(r.receipt);
    expect(r.held).toBe(1);
    expect(r.waited).toMatchObject({ ok: true, value: null });
    expect(await incarnationRows(o)).toEqual([{ user_id: "user_x", incarnation: 1 }]);
    expect(await receipts(o)).toEqual([{ user_id: "user_x", incarnation: 0, consumed_by: 1 }]);
    // The new incarnation is active; replaying the reissue still issues nothing.
    expect(await publish(o, X, 1)).toBe("ok");
    expect(await reissue(o, X)).toBeNull();
  });

  test("S1 stale publication of incarnation 0 after the reissue to 1: refused (incarnation_moved); incarnation 1 publishes", async () => {
    const { o } = await rig();
    await del(o, X);
    expect(await reissue(o, X)).toBe(1);
    expect(await publish(o, X, 0)).toBe("incarnation_moved");
    expect(await publish(o, X, 1)).toBe("ok");
    expect(await published(o)).toEqual([{ owner: "user_x", incarnation: 1 }]);
  });

  test("E1 erase vs publication, publication first: erase blocks on k; then every publication refuses (erased) and reissue issues nothing", async () => {
    const { o, pair } = await rig();
    const r = await expectBlocks(pair, { holder: pair.a, waiter: pair.b, hold: (c) => publish(c, X, 0), wait: (c) => erase(c) });
    advisory(r.receipt);
    expect(r.held).toBe("ok");
    expect(r.waited).toMatchObject({ ok: true, value: "erased" });
    expect(await publish(o, X, 0)).toBe("erased");
    await del(o, X);
    expect(await reissue(o, X)).toBeNull();
    expect(await publish(o, X, 1)).toBe("erased");
    expect(await incarnationRows(o)).toEqual([]);
  });

  test("E1 erase vs publication, erase first: the publication paused before shared k is blocked, then refuses (erased)", async () => {
    const { o, pair } = await rig();
    const r = await expectBlocks(pair, { holder: pair.b, waiter: pair.a, hold: (c) => erase(c), wait: (c) => publish(c, X, 0) });
    advisory(r.receipt);
    expect(r.waited).toMatchObject({ ok: true, value: "erased" });
    expect(await published(o)).toEqual([]);
  });

  test("O1 two owners, one logical session id: X's deletion blocks Y's publication on k (serialization) but never revokes Y; Y cannot consume X's receipt; ownership is AMBIGUOUS, so X's reissue is refused WITHOUT mutation", async () => {
    const { o, pair } = await rig();
    // X publishes first (X is an owner), then X deletes while Y's publication waits on k.
    expect(await publish(o, X, 0)).toBe("ok");
    const r = await expectBlocks(pair, { holder: pair.b, waiter: pair.a, hold: (c) => del(c, X), wait: (c) => publish(c, Y, 0) });
    advisory(r.receipt);
    expect(r.waited).toMatchObject({ ok: true, value: "ok" });
    // Y is not revoked by X's deletion; X is.
    expect(await publish(o, Y, 0)).toBe("ok");
    expect(await publish(o, X, 0)).toBe("incarnation_deleted");
    const before = { inc: await incarnationRows(o), rec: await receipts(o) };
    // Y cannot consume X's receipt; X cannot reissue either: two owners make the scope ambiguous.
    expect(await reissue(o, Y)).toBeNull();
    expect(await reissue(o, X)).toBeNull();
    // Refused WITHOUT mutation: no incarnation row, X's receipt still unconsumed.
    expect({ inc: await incarnationRows(o), rec: await receipts(o) }).toEqual(before);
    expect(before).toEqual({ inc: [], rec: [{ user_id: "user_x", incarnation: 0, consumed_by: null }] });
    // Y keeps publishing at incarnation 0 after all of it.
    expect(await publish(o, Y, 0)).toBe("ok");
  });

  test("O2 single-owner control: with only X ever seen, the same deletion lets X reissue (positive reissue only for a demonstrably single-owner session)", async () => {
    const { o } = await rig();
    expect(await publish(o, X, 0)).toBe("ok");
    await del(o, X);
    expect(await reissue(o, X)).toBe(1);
    expect(await publish(o, X, 1)).toBe("ok");
  });
});
