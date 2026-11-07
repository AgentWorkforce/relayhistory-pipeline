/**
 * C3 purge of Neon-era analysis (046000 purge_session_analysis_for_owned): exported/backfilled
 * sessions have no SP publication head, so a reset/disable never tombstones a pointer for them.
 * Lane A's authority (041000) calls these, guarded, before deleting the owned rows.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { database, read } from "../../session-brief/__tests__/harness.ts";

setDefaultTimeout(60_000);
let pg: PGlite | undefined;
afterEach(async () => {
  await pg?.close();
  pg = undefined;
});

/** 041000's owned-set table (lane A, #289); the delivery records are 010000's. */
const LANE_A = `
CREATE TABLE sessions.ingest_frozen_source_owned_sessions(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
 source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));`;

let n = 0;
async function backfilled(db: PGlite, org: string, ws: string, session: string) {
  n += 1;
  const key = `${org}/${ws}/m/claude/2026-01-01/${session}/normalized/session-analysis-v2/${n}.json`;
  const job = (await db.query<{ id: string }>(
    `INSERT INTO sessions.session_analysis_jobs_v2 (org_id, workspace_id, session_id, status, analysis_version, schema_version,
       model_version, result_s3_key, result_sha256, result_bytes, origin, completed_at)
     VALUES ($1, $2, $3, 'completed', 'session-analysis-v2.4', 2, 'gpt-5.4', $4, repeat('a', 64), 10, 'neon_backfill', now()) RETURNING id`,
    [org, ws, session, key],
  )).rows[0]!.id;
  await db.query(
    `INSERT INTO sessions.session_analysis_requests_v2 (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id, origin)
     VALUES ($1, $2, $3, 'someone', 'completed', $4, 'neon_backfill')`,
    [org, ws, session, job],
  );
  return key;
}
const jobs = async (db: PGlite) =>
  (await db.query<{ s: string }>("SELECT org_id || '/' || workspace_id || '/' || session_id AS s FROM sessions.session_analysis_jobs_v2 ORDER BY 1")).rows.map((r) => r.s);
const requests = async (db: PGlite) =>
  (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_analysis_requests_v2")).rows[0]!.n;
const tickets = async (db: PGlite) =>
  (await db.query<{ k: string; reason: string }>("SELECT result_key k, reason FROM sessions.session_analysis_result_tickets ORDER BY 1")).rows;

async function world() {
  const db = (pg = await database());
  await db.exec(LANE_A);
  await db.exec(`INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES
    ('org-a','ws-1','user-1','origin-1','claude','S1'),
    ('org-a','ws-1','user-1','origin-2','claude','S1'),
    ('org-a','ws-2','user-1','origin-1','codex','S2'),
    ('org-a','ws-9','user-2','origin-9','claude','S1')`);
  // A delivery-record-only session (the reset's owned superset).
  await db.exec(`INSERT INTO sessions.ingest_frozen_source_records (org_id, workspace_id, origin_id, record_id, source, session_id, user_id)
    VALUES ('org-a','ws-1','origin-1','r1','claude','S3','user-1'), ('org-a','ws-1','origin-1','r2','claude',NULL,'user-1'),
           ('org-a','ws-1','origin-1','r3','claude','S4','user-2')`);
  const k = {
    s1: await backfilled(db, "org-a", "ws-1", "S1"),
    s2: await backfilled(db, "org-a", "ws-2", "S2"),
    s3: await backfilled(db, "org-a", "ws-1", "S3"),
    otherUser: await backfilled(db, "org-a", "ws-9", "S1"), // user-2's same-id session
    otherOrg: await backfilled(db, "org-b", "ws-1", "S1"), // same workspace and id, other org
    notOwned: await backfilled(db, "org-a", "ws-1", "S4"),
  };
  return { db, k };
}

test("reset: the workspace's owned set (owned sessions + delivery records) is purged and its result keys ticketed; another workspace and others untouched", async () => {
  const { db, k } = await world();
  expect((await db.query<{ n: number }>("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1') n")).rows[0]!.n).toBe(2);
  // A reset in ws-1 leaves the same user's ws-2 analysis intact (C3 resets are per workspace).
  expect(await jobs(db)).toEqual(["org-a/ws-1/S4", "org-a/ws-2/S2", "org-a/ws-9/S1", "org-b/ws-1/S1"]);
  expect(await requests(db)).toBe(4);
  expect(await tickets(db)).toEqual([k.s1, k.s3].sort().map((key) => ({ k: key, reason: "forgotten" })));
  expect(k.otherUser && k.otherOrg && k.notOwned).toBeTruthy();
  // Idempotent.
  expect((await db.query<{ n: number }>("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1') n")).rows[0]!.n).toBe(0);
  // Then the ws-2 reset purges only ws-2.
  expect((await db.query<{ n: number }>("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-2','user-1') n")).rows[0]!.n).toBe(1);
  expect(await jobs(db)).toEqual(["org-a/ws-1/S4", "org-a/ws-9/S1", "org-b/ws-1/S1"]);
});

test("disable: only the listed (source, session) in the user's owned set; a listed session another user owns is untouched", async () => {
  const { db, k } = await world();
  const listed = JSON.stringify([{ source: "claude", session_id: "S1" }, { source: "codex", session_id: "NOT-OWNED" }, { source: "codex", session_id: "S1" }]);
  expect((await db.query<{ n: number }>("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1',$1::jsonb) n", [listed])).rows[0]!.n).toBe(1);
  expect(await jobs(db)).toEqual(["org-a/ws-1/S3", "org-a/ws-1/S4", "org-a/ws-2/S2", "org-a/ws-9/S1", "org-b/ws-1/S1"]);
  expect(await tickets(db)).toEqual([{ k: k.s1, reason: "forgotten" }]);
  // An empty list purges nothing.
  expect((await db.query<{ n: number }>("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1','[]'::jsonb) n")).rows[0]!.n).toBe(0);
});

test("a publishing ticket for a purged key becomes a lease-free 'unknown' ticket (its PUT outcome is not known)", async () => {
  const { db, k } = await world();
  await db.query("INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, lease_expires_at) SELECT result_s3_key, id, org_id, workspace_id, session_id, 'publishing', now() + interval '10 minutes' FROM sessions.session_analysis_jobs_v2 WHERE result_s3_key = $1", [k.s1]);
  await db.query("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1')");
  const t = (await db.query<{ reason: string; lease: unknown }>("SELECT reason, lease_expires_at lease FROM sessions.session_analysis_result_tickets WHERE result_key = $1", [k.s1])).rows;
  expect(t).toEqual([{ reason: "unknown", lease: null }]);
});

test("bad input is refused; without lane A's tables it is a no-op; EXECUTE is owner-only", async () => {
  const db = (pg = await database());
  await backfilled(db, "org-a", "ws-1", "S1");
  expect((await db.query<{ n: number }>("SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1') n")).rows[0]!.n).toBe(0);
  expect(await jobs(db)).toEqual(["org-a/ws-1/S1"]);
  for (const bad of ["SELECT sessions.purge_session_analysis_for_owned('', 'w', 'u')", "SELECT sessions.purge_session_analysis_for_owned('o', 'w', NULL)",
    "SELECT sessions.purge_session_analysis_for_owned('o', '', 'u')",
    `SELECT sessions.purge_session_analysis_for_owned('o','w','u','{"source":"claude"}'::jsonb)`,
    `SELECT sessions.purge_session_analysis_for_owned('o','w','u','[{"source":"claude"}]'::jsonb)`]) {
    await expect(db.query(bad)).rejects.toThrow("purge_session_analysis_for_owned");
  }
  const grants = (await db.query<{ g: string }>(`
    SELECT DISTINCT r.rolname g FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
      JOIN pg_roles r ON r.oid = a.grantee
     WHERE p.proname = 'purge_session_analysis_for_owned' AND a.grantee <> p.proowner`)).rows;
  expect(grants).toEqual([]);
  const cfg = (await db.query<{ secdef: boolean; cfg: string[] }>("SELECT prosecdef secdef, proconfig cfg FROM pg_proc WHERE proname = 'purge_session_analysis_for_owned'")).rows;
  expect(cfg).toHaveLength(2);
  for (const c of cfg) expect(c).toEqual({ secdef: true, cfg: ["search_path=pg_catalog, sessions, pg_temp"] });
});

/** Run `sql` as `role` in a rolled-back transaction; returns the error message or "ok". */
async function as(db: PGlite, role: string, sql: string, setup = "") {
  await db.exec("BEGIN");
  try {
    if (setup) await db.exec(setup);
    await db.exec(`SET LOCAL ROLE ${role}`);
    await db.query(sql);
    return "ok";
  } catch (error) {
    return (error as Error).message;
  } finally {
    await db.exec("ROLLBACK");
  }
}

test("caller pin: every SECURITY DEFINER in 046000 refuses a role it does not serve, even one granted EXECUTE", async () => {
  const { db } = await world();
  for (const role of ["intruder", "service_role", "bulls96_read_api", "bulls96_analysis_admit", "bulls96_session_ingest"])
    await db.exec(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN CREATE ROLE ${role}; END IF; END $$`);
  await db.exec(`GRANT USAGE ON SCHEMA sessions TO intruder, bulls96_session_ingest, service_role, bulls96_read_api, bulls96_analysis_admit;
    GRANT EXECUTE ON FUNCTION sessions.session_analysis_neon_free(text,text,text) TO intruder, bulls96_read_api, bulls96_analysis_admit;
    GRANT EXECUTE ON FUNCTION sessions.purge_session_analysis_for_owned(text,text,text) TO intruder, bulls96_session_ingest, service_role;
    GRANT EXECUTE ON FUNCTION sessions.purge_session_analysis_for_owned(text,text,text,jsonb) TO intruder, bulls96_session_ingest`);
  const free = "SELECT sessions.session_analysis_neon_free('org-a','ws-1','S1')";
  expect(await as(db, "intruder", free)).toContain("caller is not a role this function serves");
  for (const ok of ["service_role", "bulls96_read_api", "bulls96_analysis_admit"]) expect(await as(db, ok, free)).toBe("ok");
  const purge = "SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1')";
  expect(await as(db, "intruder", purge)).toContain("caller is not a role this function serves");
  expect(await as(db, "service_role", purge)).toContain("caller is not a role this function serves");
  expect(await as(db, "intruder", "SELECT sessions.purge_session_analysis_for_owned('org-a','ws-1','user-1','[]'::jsonb)")).toContain("caller is not");
  // The ingest role only inside the authority begin (its writer GUC).
  expect(await as(db, "bulls96_session_ingest", purge)).toContain("outside the privacy authority");
  expect(await as(db, "bulls96_session_ingest", purge, "SELECT set_config('bulls96.privacy_authority_writer','authority',true)")).toBe("ok");
  // The search_path is pinned on all of them.
  const unpinned = (await db.query<{ f: string }>(`
    SELECT p.oid::regprocedure::text f FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef
       AND p.proname IN ('session_analysis_neon_free', 'session_neon_free_for', 'purge_session_analysis_for_owned',
         'analysis_read_v1', 'analysis_request_v1', 'session_analysis_v2_forget')
       AND NOT coalesce(p.proconfig, '{}') @> ARRAY['search_path=pg_catalog, sessions, pg_temp']`)).rows;
  expect(unpinned).toEqual([]);
  expect((await db.query<{ n: number }>(`SELECT count(*)::int n FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'sessions' AND p.prosecdef AND p.proname IN ('session_analysis_neon_free', 'purge_session_analysis_for_owned')`)).rows[0]!.n).toBe(3);
});

test("no runtime or read role holds any privilege on the frozen custody, census or purge-fence tables (only the definers read them)", async () => {
  const db = (pg = await database());
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  const grants = (await db.query<{ t: string; g: string; p: string }>(`
    SELECT c.relname t, r.rolname g, a.privilege_type p
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
      JOIN pg_roles r ON r.oid = a.grantee
     WHERE n.nspname = 'sessions' AND c.relkind = 'r' AND a.grantee <> c.relowner
       AND (c.relname LIKE 'ingest_frozen_source%' OR c.relname IN ('session_neon_census', 'session_neon_census_seal', 'session_analysis_purge_fences'))
       AND r.rolname IN ('service_role', 'bulls96_read_api', 'bulls96_analysis_admit', 'anon', 'authenticated', 'bulls96_session_ingest')`)).rows;
  expect(grants).toEqual([]);
});
