import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { database, hoopDb, pointer, KEY, FakeModel, testConfig } from "./harness.ts";
import { briefTick } from "../worker.ts";
import { briefInputFromCanonical } from "../canonical-input.ts";
import type { BriefConfig } from "../config.ts";

// Issue #494: in canonical mode a wakeup parked until the next UTC midnight by a
// rate_limited refusal is recovered by new pointer revisions (F1), by admission opening
// again mid-day (F3), and by the sweep after the stale-wakeup prune (F4). Every test
// drives the real briefTick -> claim -> admit -> reschedule path; due_at is never edited.
setDefaultTimeout(120_000);
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((d) => d.close())); });

type Key = typeof KEY;
const K2: Key = { ...KEY, sessionId: "s-2" };
const MINUTE = 61_000;

const canonicalInput = async (_db: unknown, key: Key, pointers: Parameters<typeof briefInputFromCanonical>[1]) => ({
  ...briefInputFromCanonical(key, pointers, { exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: [
    { table: "convergence_events", row: { source: key.harness, session_id: key.sessionId, event_id: "u", kind: "session_event", type: "user", content: "Synthetic greeting", ts: "2026-11-09T01:00:00Z" } },
    { table: "convergence_events", row: { source: key.harness, session_id: key.sessionId, event_id: "a", kind: "session_event", type: "assistant", content: "Synthetic response", ts: "2026-11-09T01:00:01Z" } },
  ] }),
  canonicalSessionId: key.sessionId,
});

/** Seconds until the wakeup is due. */
const dueIn = async (pg: PGlite, key: Key) => (await pg.query<{ left: number }>(
  "SELECT extract(epoch FROM due_at - now())::float AS left FROM sessions.session_brief_wakeups WHERE workspace_id = $1 AND session_id = $2",
  [key.workspaceId, key.sessionId])).rows[0]!.left;

async function setup(keys: Key[] = [KEY]) {
  const pg = await database(); open.push(pg);
  const db = hoopDb(pg), model = new FakeModel();
  for (const key of keys) await pointer(pg, { key });
  let clock = Date.now();
  /** Runs once inside the next claim's input build (after livePointers, before the refusal is saved). */
  let duringInput: (() => Promise<unknown>) | undefined;
  const input: typeof canonicalInput = async (db, key, pointers) => {
    const hook = duringInput;duringInput = undefined;
    if (hook) await hook();
    return canonicalInput(db, key, pointers);
  };
  /** One worker (one briefTick instance per config); each call is a minute later, so housekeeping runs every call. */
  const worker = (o: Partial<BriefConfig> = {}, canonical = true) => {
    const tick = briefTick({ reader: {} as never, store: {} as never, transport: model,
      ...(canonical ? { canonicalInput: input } : {}),
      config: testConfig({ sweepSeconds: 1, throttleSeconds: 0, idleSeconds: 0, sessionDailyCap: 3,
        workspaceIds: [...new Set(keys.map((k) => k.workspaceId))], ...o }) });
    return () => tick(db, new Date((clock += MINUTE)));
  };
  const spent = (key: Key, n: number) => pg.query(
    `INSERT INTO sessions.session_brief_attempts VALUES ($1,$2,$3,$4,(now() AT TIME ZONE 'UTC')::date,$5)
     ON CONFLICT (org_id, day, workspace_id, harness, session_id) DO UPDATE SET attempts=excluded.attempts`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId, n]);
  const wakeup = async (key: Key = KEY) => (await pg.query<Record<string, unknown>>(
    `SELECT generation::text AS generation, attempts, last_error, pending_since, evidence_at, due_at,
            lease_id, lease_expires_at,
            due_at = (date_trunc('day', now() AT TIME ZONE 'UTC') + interval '1 day') AT TIME ZONE 'UTC' AS midnight
       FROM sessions.session_brief_wakeups WHERE workspace_id = $1 AND session_id = $2`, [key.workspaceId, key.sessionId])).rows[0];
  const brief = async (key: Key = KEY) => (await pg.query<{ status: string; error: string | null; source_revisions: unknown }>(
    "SELECT status, error, source_revisions FROM sessions.session_briefs WHERE workspace_id = $1 AND session_id = $2", [key.workspaceId, key.sessionId])).rows[0];
  const attempts = async (key: Key = KEY) => (await pg.query<{ attempts: number }>(
    "SELECT attempts FROM sessions.session_brief_attempts WHERE workspace_id = $1 AND session_id = $2", [key.workspaceId, key.sessionId])).rows[0]?.attempts ?? 0;
  /** Park every key: the per-session cap (3) is already spent, so the sweep's wakeup is refused and parked to midnight. */
  const park = async () => {
    for (const key of keys) await spent(key, 3);
    expect(await worker()()).toEqual({ rate_limited: keys.length });
    for (const key of keys) {
      expect((await wakeup(key))!.midnight).toBe(true);
      expect(await brief(key)).toMatchObject({ status: "rate_limited", error: "session_brief_capped" });
    }
  };
  const left = (key: Key = KEY) => dueIn(pg, key);
  return { pg, db, model, worker, spent, wakeup, brief, attempts, park, left,
    during: (hook: () => Promise<unknown>) => { duringInput = hook; } };
}

test("baseline: a capped canonical session is parked until the next UTC midnight and stays parked", async () => {
  const s = await setup();
  await s.park();
  const parked = await s.wakeup();
  expect(parked).toMatchObject({ attempts: 0, last_error: null, lease_id: null });
  const tick = s.worker();
  expect(await tick()).toEqual({});expect(await tick()).toEqual({});
  expect(await s.wakeup()).toEqual(parked);expect(s.model.requests).toHaveLength(0);expect(await s.attempts()).toBe(3);
});

test("F1: a newer pointer revision re-arms a parked wakeup like armWakeup; the refusal records it, then no repeat", async () => {
  const s = await setup();
  await s.park();
  const parked = await s.wakeup();
  await pointer(s.pg, { revision: 2 });
  const tick = s.worker();
  // Cap still spent: the re-armed wakeup is claimed and refused again with the new revisions.
  expect(await tick()).toEqual({ rate_limited: 1 });
  const rearmed = await s.wakeup();
  expect(rearmed!.generation).toBe(String(Number(parked!.generation) + 1));
  expect(+(rearmed!.evidence_at as Date)).toBeGreaterThan(+(parked!.evidence_at as Date));
  expect(+(rearmed!.pending_since as Date)).toBe(+(parked!.pending_since as Date));
  expect(rearmed).toMatchObject({ midnight: true, attempts: 0, last_error: null, lease_id: null });
  expect((await s.brief())!.source_revisions).toEqual({ "m-1": 2 });
  expect(await tick()).toEqual({});expect(await s.wakeup()).toEqual(rearmed);
  expect(s.model.requests).toHaveLength(0);expect(await s.attempts()).toBe(3);
  // With admission open as well, the new revision is generated.
  await pointer(s.pg, { revision: 3 });
  expect(await s.worker({ sessionDailyCap: 5 })()).toEqual({ generated: 1 });
  expect(await s.brief()).toMatchObject({ status: "ready", source_revisions: { "m-1": 3 } });
});

test("F1 keeps armWakeup's debounce: with an idle window the re-armed wakeup waits, it is not due at once", async () => {
  const s = await setup();
  await s.park();
  await pointer(s.pg, { revision: 2 });
  expect(await s.worker({ idleSeconds: 600 })()).toEqual({});
  const w = await s.pg.query<{ left: number }>("SELECT extract(epoch FROM due_at - now())::int AS left FROM sessions.session_brief_wakeups");
  expect(w.rows[0]!.left).toBeGreaterThan(500);expect(w.rows[0]!.left).toBeLessThanOrEqual(600);
});

test("F3: raising the per-session cap mid-day (3 -> 5) makes the parked wakeup due and it is generated once", async () => {
  const s = await setup();
  await s.park();
  const tick = s.worker({ sessionDailyCap: 5 });
  expect(await tick()).toEqual({ generated: 1 });
  expect(s.model.requests).toHaveLength(1);expect(await s.attempts()).toBe(4);
  expect((await s.brief())!.status).toBe("ready");
  const after = await s.wakeup();
  expect(await tick()).toEqual({});expect(await tick()).toEqual({});
  expect(await s.wakeup()).toEqual(after);expect(s.model.requests).toHaveLength(1);expect(await s.attempts()).toBe(4);
});

test("F3: the global daily cap is checked first and bounds how many parked wakeups are made due", async () => {
  const s = await setup([KEY, K2]);
  // Other orgs spent 14 today: with 3 + 3 here the global total is 20 = the global cap.
  await s.pg.query(`INSERT INTO sessions.session_brief_attempts VALUES ('org-z','ws-z','codex','z',(now() AT TIME ZONE 'UTC')::date,14)`);
  await s.park();
  const parked = [await s.wakeup(KEY), await s.wakeup(K2)];
  // The per-session cap is raised, but the global total is at the cap: nothing moves.
  const raised = s.worker({ sessionDailyCap: 5, globalDailyCap: 20 });
  expect(await raised()).toEqual({});expect(await raised()).toEqual({});
  expect([await s.wakeup(KEY), await s.wakeup(K2)]).toEqual(parked);expect(s.model.requests).toHaveLength(0);
  // One unit of global headroom: exactly one parked wakeup is made due and generated.
  const headroom = s.worker({ sessionDailyCap: 5, globalDailyCap: 21 });
  expect(await headroom()).toEqual({ generated: 1 });
  expect(await headroom()).toEqual({});
  expect(s.model.requests).toHaveLength(1);
  const statuses = [(await s.brief(KEY))!.status, (await s.brief(K2))!.status].sort();
  expect(statuses).toEqual(["rate_limited", "ready"]);
  const still = (await s.brief(KEY))!.status === "rate_limited" ? KEY : K2;
  expect((await s.wakeup(still))!.midnight).toBe(true);
});

test("a live lease held by another worker is never touched; an expired lease counts as free", async () => {
  const s = await setup();
  await s.park();
  await s.pg.exec("UPDATE sessions.session_brief_wakeups SET lease_id = gen_random_uuid(), lease_expires_at = now() + interval '5 minutes'");
  await pointer(s.pg, { revision: 2 });
  const leased = await s.wakeup();
  const tick = s.worker({ sessionDailyCap: 5 });
  expect(await tick()).toEqual({});expect(await tick()).toEqual({});
  expect(await s.wakeup()).toEqual(leased);expect(s.model.requests).toHaveLength(0);
  await s.pg.exec("UPDATE sessions.session_brief_wakeups SET lease_expires_at = now() - interval '1 second'");
  expect(await tick()).toEqual({ generated: 1 });
  expect(await s.brief()).toMatchObject({ status: "ready", source_revisions: { "m-1": 2 } });
});

test("no churn: repeated ticks on parked wakeups make no claims, writes or model calls", async () => {
  const s = await setup([KEY, K2]);
  await s.park();
  await pointer(s.pg, { key: K2, revision: 2 });
  const tick = s.worker();
  expect(await tick()).toEqual({ rate_limited: 1 });
  const snapshot = async () => (await s.pg.query("SELECT * FROM sessions.session_brief_wakeups ORDER BY session_id")).rows;
  const briefs = async () => (await s.pg.query("SELECT session_id, status, error, source_revisions, attempted_at FROM sessions.session_briefs ORDER BY session_id")).rows;
  const before = [await snapshot(), await briefs()];
  const results: unknown[] = [];
  for (let i = 0; i < 6; i++) results.push(await tick());
  expect(results).toEqual([{}, {}, {}, {}, {}, {}]);
  expect([await snapshot(), await briefs()]).toEqual(before);
  expect(s.model.requests).toHaveLength(0);
  expect([await s.attempts(KEY), await s.attempts(K2)]).toEqual([3, 3]);
});

test("workspace scope and tombstones: only configured live sessions are recovered", async () => {
  const W2: Key = { ...KEY, workspaceId: "ws-b" }, T: Key = { ...KEY, sessionId: "s-gone" };
  const s = await setup([KEY, W2, T]);
  await s.park();
  const foreign = await s.wakeup(W2);
  await s.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = $1", [T.sessionId]);
  const scoped = s.worker({ sessionDailyCap: 5, workspaceIds: [KEY.workspaceId] });
  expect(await scoped()).toEqual({ generated: 1 });expect(await scoped()).toEqual({});
  expect(s.model.requests).toHaveLength(1);
  expect((await s.brief(KEY))!.status).toBe("ready");
  expect(await s.wakeup(W2)).toEqual(foreign);expect((await s.brief(W2))!.status).toBe("rate_limited");
  const gone = await s.wakeup(T);
  if (gone) expect(gone.midnight).toBe(true);
  expect((await s.brief(T))?.status).not.toBe("ready");
});

test("F4: a parked wakeup pruned after SESSION_WAKE_MAX_AGE_HOURS is found again by the sweep", async () => {
  const s = await setup();
  await s.park();
  // Seed age only (evidence older than the 72 h prune); due_at is untouched.
  await s.pg.exec("UPDATE sessions.session_brief_wakeups SET evidence_at = now() - interval '73 hours'");
  const tick = s.worker();
  // Prune drops it, the sweep re-arms the rate_limited session, the claim is refused and parks it again.
  expect(await tick()).toEqual({ rate_limited: 1 });
  const again = await s.wakeup();
  expect(again).toMatchObject({ midnight: true, attempts: 0 });
  expect(Date.now() - +(again!.evidence_at as Date)).toBeLessThan(3_600_000);
  expect(await tick()).toEqual({});expect(await s.wakeup()).toEqual(again);
  expect(s.model.requests).toHaveLength(0);expect(await s.attempts()).toBe(3);
  expect(await s.worker({ sessionDailyCap: 5 })()).toEqual({ generated: 1 });
});

test("canonical off: none of the recoveries runs (legacy behaviour unchanged)", async () => {
  const s = await setup();
  await s.park();
  const parked = await s.wakeup();
  await pointer(s.pg, { revision: 2 });
  const legacy = s.worker({ sessionDailyCap: 5 }, false);
  expect(await legacy()).toEqual({});expect(await legacy()).toEqual({});
  expect(await s.wakeup()).toEqual(parked);expect(s.model.requests).toHaveLength(0);
  expect((await s.brief())!.source_revisions).toEqual({ "m-1": 1 });
  await s.pg.exec("UPDATE sessions.session_brief_wakeups SET evidence_at = now() - interval '73 hours'");
  expect(await legacy()).toEqual({});
  expect(await s.wakeup()).toBeUndefined();expect(await legacy()).toEqual({});
  expect(await s.wakeup()).toBeUndefined();expect((await s.brief())!.status).toBe("rate_limited");
});

test("M1: a refusal dropped as stale after an F1 re-arm retries after the idle window, not at midnight", async () => {
  const s = await setup();
  await s.park();
  await pointer(s.pg, { revision: 2 });
  const tick = s.worker({ idleSeconds: 1 });
  // F1 re-arms with the 1 s debounce; nothing is due yet.
  expect(await tick()).toEqual({});expect(await s.left()).toBeLessThanOrEqual(1);
  await Bun.sleep(1100);
  // The claim reads revision 2; revision 3 lands while the input is built, so the refusal is stale.
  s.during(() => pointer(s.pg, { revision: 3 }));
  expect(await tick()).toEqual({ rate_limited: 1 });
  expect((await s.brief())!.source_revisions).toEqual({ "m-1": 1 });
  const retry = await s.wakeup();
  expect(retry).toMatchObject({ midnight: false, attempts: 0, lease_id: null });
  expect(await s.left()).toBeLessThanOrEqual(1);
  await Bun.sleep(1100);
  // Claimed again with revision 3: the refusal is recorded and parks to midnight; no repeat.
  expect(await tick()).toEqual({ rate_limited: 1 });
  expect((await s.brief())!.source_revisions).toEqual({ "m-1": 3 });
  expect((await s.wakeup())!.midnight).toBe(true);
  expect(await tick()).toEqual({});expect(s.model.requests).toHaveLength(0);expect(await s.attempts()).toBe(3);
});

test("L1: F3 never overrides the retry backoff of an admitted call that failed", async () => {
  const s = await setup();
  await s.park();
  s.model.next.push(new Error("synthetic transient model failure"));
  const tick = s.worker({ sessionDailyCap: 10 });
  expect(await tick()).toEqual({ retrying: 1 });
  expect(s.model.requests).toHaveLength(1);expect(await s.attempts()).toBe(4);
  const backoff = await s.wakeup();
  expect(backoff).toMatchObject({ midnight: false, last_error: "brief_processing_failed" });
  expect(await s.left()).toBeGreaterThan(30);
  for (let i = 0; i < 4; i++) expect(await tick()).toEqual({});
  expect(await s.wakeup()).toEqual(backoff);expect(s.model.requests).toHaveLength(1);expect(await s.attempts()).toBe(4);
});

test("F3 waits out the throttle before reconsidering a refusal", async () => {
  const s = await setup();
  await s.park();
  const parked = await s.wakeup();
  const tick = s.worker({ sessionDailyCap: 5, throttleSeconds: 3 });
  expect(await tick()).toEqual({});expect(await s.wakeup()).toEqual(parked);expect(s.model.requests).toHaveLength(0);
  await Bun.sleep(3100);
  expect(await tick()).toEqual({ generated: 1 });expect(s.model.requests).toHaveLength(1);
});

test("F3 follows the org budget: no session is made due while the org's distinct-session pool is full", async () => {
  const s = await setup([KEY, K2]);
  await s.park();
  const parked = [await s.wakeup(KEY), await s.wakeup(K2)];
  // Each session's "others" is 1 (the other one): a budget of 1 still refuses both.
  const full = s.worker({ sessionDailyCap: 5, backgroundDailyBudget: 1 });
  expect(await full()).toEqual({});expect(await full()).toEqual({});
  expect([await s.wakeup(KEY), await s.wakeup(K2)]).toEqual(parked);expect(s.model.requests).toHaveLength(0);
  expect(await s.worker({ sessionDailyCap: 5, backgroundDailyBudget: 2 })()).toEqual({ generated: 2 });
});
