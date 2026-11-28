/**
 * T144 (slot 20261105062520): the trailing state on sessions.session_brief_wakeups, on PGlite with every migration.
 * The SQL in store.ts (armWakeup, claimWakeups, finishWakeup, rescheduleWakeup) must match trigger.ts's pure rule:
 * evidence armed while a lease is outstanding keeps its FIRST commit time (trailing_since), a claim clears it, a
 * failure keeps it, a finish on a generation mismatch starts the trailing window there, a stale finish is fenced by
 * lease_id, and a legacy NULL keeps the conservative pending_since.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { armWakeup, claimWakeups, finishWakeup, rescheduleWakeup, type ClaimedWakeup } from "../store.ts";
import { database, hoopDb, KEY } from "./harness.ts";

setDefaultTimeout(60_000);
const open: PGlite[] = [];
afterEach(async () => { for (const pg of open.splice(0)) await pg.close(); });

const WINDOW = { idleSeconds: 600, maxWaitSeconds: 3_600, trailing: true };
interface Row { pending_since: string; evidence_at: string; due_at: string; generation: string; lease_id: string | null; trailing_since: string | null; attempts: number }
const row = async (pg: PGlite) =>
  (await pg.query<Row>(`SELECT pending_since::text, evidence_at::text, due_at::text, generation::text, lease_id::text,
     trailing_since::text, attempts FROM sessions.session_brief_wakeups`)).rows[0];
/** Shift every timestamp of the row into the past, so later arms are visibly later (PGlite's now() is wall time). */
const age = (pg: PGlite, seconds: number) => pg.exec(`UPDATE sessions.session_brief_wakeups SET
  pending_since = pending_since - interval '${seconds} seconds', evidence_at = evidence_at - interval '${seconds} seconds',
  due_at = now() - interval '1 second',
  trailing_since = trailing_since - interval '${seconds} seconds'`);
async function setup() {
  const pg = await database();
  open.push(pg);
  return { pg, db: hoopDb(pg) };
}
const arm = (db: ReturnType<typeof hoopDb>) => armWakeup(db, KEY, WINDOW);
const claim = async (db: ReturnType<typeof hoopDb>): Promise<ClaimedWakeup> => {
  const [c] = await claimWakeups(db, 5, 300, undefined, { trailing: true });
  if (!c) throw new Error("nothing claimable");
  return c;
};

describe("T144 trailing state (062520) on PGlite", () => {
  test("the migration adds exactly the nullable column", async () => {
    const { pg } = await setup();
    const cols = await pg.query<{ is_nullable: string; data_type: string }>(
      "SELECT is_nullable, data_type FROM information_schema.columns WHERE table_schema='sessions' AND table_name='session_brief_wakeups' AND column_name='trailing_since'");
    expect(cols.rows).toEqual([{ is_nullable: "YES", data_type: "timestamp with time zone" }]);
  });

  test("B then C during A's lease: the first trailing commit is kept; A's finish starts the window there", async () => {
    const { pg, db } = await setup();
    await arm(db); // A
    await age(pg, 600);
    const a = await claim(db);
    expect((await row(pg))!.trailing_since).toBeNull();
    await arm(db); // B, during the lease
    const tB = (await row(pg))!.trailing_since;
    expect(tB).not.toBeNull();
    await pg.exec("UPDATE sessions.session_brief_wakeups SET trailing_since = trailing_since - interval '30 seconds'");
    const tBShifted = (await row(pg))!.trailing_since;
    await arm(db); // C, during the lease: trailing_since stays B's
    expect((await row(pg))!.trailing_since).toBe(tBShifted);
    await finishWakeup(db, a, WINDOW); // A covered A only (generation mismatch)
    const after = (await row(pg))!;
    expect(after.pending_since).toBe(tBShifted);
    expect([after.trailing_since, after.lease_id]).toEqual([null, null]);
    const dueOk = await pg.query<{ ok: boolean }>(`SELECT due_at = LEAST(evidence_at + interval '600 seconds', pending_since + interval '3600 seconds') AS ok
      FROM sessions.session_brief_wakeups`);
    expect(dueOk.rows[0]!.ok).toBe(true);
  });

  test("a failed A retires nothing; the retry's claim clears trailing; a new D; the stale first claim is fenced", async () => {
    const { pg, db } = await setup();
    await arm(db); // A
    await age(pg, 600);
    const tA = (await row(pg))!.pending_since;
    const a1 = await claim(db);
    await arm(db); // B during A
    const tB = (await row(pg))!.trailing_since;
    await rescheduleWakeup(db, a1, { seconds: 60 }, { error: "brief_processing_failed" }); // A fails
    const failed = (await row(pg))!;
    expect([failed.pending_since, failed.trailing_since, failed.lease_id]).toEqual([tA, tB, null]);
    await pg.exec("UPDATE sessions.session_brief_wakeups SET due_at = now() - interval '1 second'");
    const a2 = await claim(db); // the retry: a new generation boundary
    const retried = (await row(pg))!;
    expect([retried.pending_since, retried.trailing_since]).toEqual([tA, null]); // tA kept: oldest uncovered
    await arm(db); // D during the retry
    const tD = (await row(pg))!.trailing_since;
    expect(tD).not.toBeNull();
    const before = await row(pg);
    await finishWakeup(db, a1, WINDOW); // the FIRST claim's late finish: stale, fenced by lease_id
    expect(await row(pg)).toEqual(before);
    await finishWakeup(db, a2, WINDOW); // the retry finishes, D trailing
    expect((await row(pg))!.pending_since).toBe(tD);
  });

  test("B arms after the lease expired but before reclaim; reclaim clears it; the stale finish changes nothing", async () => {
    const { pg, db } = await setup();
    await arm(db);
    await age(pg, 600);
    const a = await claim(db);
    await pg.exec("UPDATE sessions.session_brief_wakeups SET lease_expires_at = now() - interval '1 second'"); // expired
    await arm(db); // B: lease_id still set, so it is trailing work
    expect((await row(pg))!.trailing_since).not.toBeNull();
    await pg.exec("UPDATE sessions.session_brief_wakeups SET due_at = now() - interval '1 second'");
    const r = await claim(db); // reclaim under a new lease
    expect(r.leaseId).not.toBe(a.leaseId);
    expect((await row(pg))!.trailing_since).toBeNull();
    const before = await row(pg);
    await finishWakeup(db, a, WINDOW); // stale
    expect(await row(pg)).toEqual(before);
    await finishWakeup(db, r, WINDOW); // the reclaim's run covers everything: the wakeup goes
    expect(await row(pg)).toBeUndefined();
  });

  test("a legacy row (generation mismatch, trailing_since NULL) keeps the conservative pending_since, not now()", async () => {
    const { pg, db } = await setup();
    await arm(db);
    await age(pg, 900);
    const tA = (await row(pg))!.pending_since;
    const a = await claim(db);
    // Armed during the lease before the column existed: the generation moved, trailing_since stayed NULL.
    await pg.exec("UPDATE sessions.session_brief_wakeups SET generation = generation + 1, evidence_at = now(), trailing_since = NULL");
    await finishWakeup(db, a, WINDOW);
    expect((await row(pg))!.pending_since).toBe(tA);
  });
});
