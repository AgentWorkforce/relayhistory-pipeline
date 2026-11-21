/**
 * 20261105062180 claims scheduler, on PGlite with the full migration plan: analysis_request_v1 keeps 046000's
 * behaviour (parity against the restored 046000 body), the scheduler admission's checks and every budget, the
 * claims control and the READ COMMITTED guard, retention, the ledger across erases (spend is never refunded), the
 * live-session candidates (round-robin across workspaces, failed sessions wait) and the grants; the worker's
 * scheduled-job gate; the rollback's refusal and override, executed; plus the TS tick against the real functions.
 */
import { afterEach, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { database, hoopDb, pointer, KEY, read } from "../../session-brief/__tests__/harness.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { postgresAnalysisJobStore } from "../store.ts";
import { encodeAnalysisResult } from "../results.ts";
import type { AnalysisJob, AnalysisJobOutput } from "../processor.ts";
import {
  SESSION_ANALYSIS_MODEL_VERSION, SESSION_ANALYSIS_RESULT_SCHEMA_VERSION, SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
  SESSION_ANALYSIS_SUMMARY_VERSION, SESSION_ANALYSIS_VERSION,
} from "../versions.ts";
import { claimsSchedulerSettings, claimsSchedulerTick, CLAIMS_SCHEDULER_RESERVE_TOKENS, scheduledJobGate, USER_REQUESTED_JOB_SQL } from "../claims-scheduler.ts";
import { runAnalysisJob } from "../processor.ts";
import { analysisTickDeps } from "../runtime-deps.ts";
import { analysisTick } from "../worker.ts";
import { PGlite as MigrationPGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { sessionIntelControls } from "../../shared/session-intel-controls.ts";
import { MIGRATION_SENTINELS } from "../../hoopdb/migration-sentinels.ts";
import { RUNTIME_APPLICATION_POLICY, runtimeApplicationAclCatalog } from "../../hoopdb/migrate-guards.ts";

setDefaultTimeout(120_000);
type Json = Record<string, any>;
let pg: PGlite;
const UP = read("supabase/migrations/20261105062180_claims_scheduler.sql");
const DOWN = read("supabase/rollbacks/20261105062180_claims_scheduler.down.sql");
const ORIGINAL = read("supabase/migrations/20261105046000_analysis_jobs.sql");
const SCHEDULE = "sessions.session_analysis_schedule_v1(text,text,text,integer,integer,integer,bigint,bigint)";
const CANDIDATES = "sessions.session_analysis_schedule_candidates_v1(integer,integer,integer)";
const JOB = "sessions.session_analysis_schedule_job_v1(uuid)";

const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;
const bodyOf = (src: string, name: string, tag: string) => {
  const start = src.search(new RegExp(`CREATE(?: OR REPLACE)? FUNCTION sessions\\.${name}\\(`));
  const open = src.indexOf(tag, start) + tag.length;
  return src.slice(open, src.indexOf(tag, open));
};

async function asRole<T>(role: string, work: () => Promise<T>, bound?: { org: string; ws: string }): Promise<T> {
  await pg.exec("BEGIN");
  try {
    if (bound) await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [bound.org, bound.ws]);
    await pg.exec(`SET LOCAL ROLE ${role}`);
    const r = await work();
    await pg.exec("COMMIT");
    return r;
  } catch (error) {
    await pg.exec("ROLLBACK");
    throw error;
  }
}
const request = (session = KEY.sessionId, workspace = KEY.workspaceId, org = KEY.orgId) =>
  asRole("bulls96_analysis_admit", async () =>
    (await pg.query<{ r: Json }>("SELECT sessions.analysis_request_v1($1, $2, 'u-1', $3) AS r", [org, workspace, session])).rows[0]!.r,
  { org, ws: workspace });
type Budget = { hourly?: number; daily?: number; interval?: number; ceiling?: number; reserve?: number };
const schedule = (key: { org?: string; ws?: string; session?: string } = {}, b: Budget = {}) =>
  asRole("service_role", async () => (await pg.query<{ r: Json }>(`SELECT ${SCHEDULE.replace(/\(.*$/, "")}($1,$2,$3,$4,$5,$6,$7,$8) AS r`, [
    key.org ?? KEY.orgId, key.ws ?? KEY.workspaceId, key.session ?? KEY.sessionId,
    b.hourly ?? 5, b.daily ?? 40, b.interval ?? 600, b.ceiling ?? 5_600_000, b.reserve ?? CLAIMS_SCHEDULER_RESERVE_TOKENS,
  ])).rows[0]!.r);
const candidates = (active = 7200, interval = 600, limit = 50) =>
  asRole("service_role", async () => (await pg.query<{ org_id: string; workspace_id: string; session_id: string }>(
    `SELECT org_id, workspace_id, session_id FROM ${CANDIDATES.replace(/\(.*$/, "")}($1,$2,$3)`, [active, interval, limit])).rows
    .map((r) => `${r.org_id}/${r.workspace_id}/${r.session_id}`));
/** Ledger rows, keyed by their job's session; a row whose job was erased has no session ("-"). */
const ledger = async () => (await pg.query<{ k: string; reserved: number }>(
  `SELECT coalesce(j.org_id || '/' || j.workspace_id || '/' || j.session_id, l.org_id || '/' || l.workspace_id || '/-') k,
          l.reserved_tokens::int reserved
     FROM sessions.session_analysis_schedule_ledger_v1 AS l LEFT JOIN sessions.session_analysis_jobs_v2 AS j ON j.id = l.analysis_job_id
    ORDER BY l.id`)).rows;
const jobs = async () => (await pg.query<{ k: string; status: string }>(
  "SELECT org_id || '/' || workspace_id || '/' || session_id k, status FROM sessions.session_analysis_jobs_v2 ORDER BY created_at, id")).rows;
const key = (session: string, ws = KEY.workspaceId, org = KEY.orgId) => ({ ...KEY, orgId: org, workspaceId: ws, sessionId: session });
const controls = async (rows: [string, string | null, string | null, boolean][]) => {
  for (const [scope, org, ws, enabled] of rows)
    await pg.query("INSERT INTO sessions.session_intel_controls (scope, org_id, workspace_id, feature, enabled, updated_by) VALUES ($1,$2,$3,'claims',$4,'test') ON CONFLICT (scope, org_id, workspace_id, feature) DO UPDATE SET enabled = excluded.enabled", [scope, org, ws, enabled]);
};
/** Completes the oldest queued job as the worker does (claim, reserve, put, complete). */
async function complete() {
  const store = postgresAnalysisJobStore({ db: hoopDb(pg), store: createFsStore({ root: mkdtempSync(join(tmpdir(), "claims-scheduler-")) }), anchorManifestKey: async () => "org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json" });
  const job = (await store.claim({ leaseSeconds: 600 })) as AnalysisJob;
  const out = { harness: "claude", sourceFingerprint: "f".repeat(64), sourceGeneration: "e".repeat(64), inputTokens: 5, outputTokens: 2,
    result: { schemaVersion: 1, analysisVersion: "v", sourceFingerprint: "f", analysis: {} as never, durationMs: 1, brief: {} as never } } as AnalysisJobOutput;
  const { bytes, sha256 } = encodeAnalysisResult(out.result);
  const k = await store.reserve(job, out, { sha256, bytes: bytes.length });
  await store.put(k!, bytes, sha256, job);
  expect(await store.complete({ job, output: out, stored: { key: k!, sha256, bytes: bytes.length } })).toBe(true);
}

beforeEach(async () => {
  pg = await database();
  await pg.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pg.exec(read("supabase/bootstrap/session_analysis_admit_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(pg);
});
afterEach(async () => { await pg.close(); });

describe("062180: analysis_request_v1 keeps 046000's behaviour", () => {
  test("its body is 046000's with the two blocks moved, verbatim, into the helpers; the engine identity is versions.ts", async () => {
    const body = (await pg.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.analysis_request_v1(text,text,text,text)'::regprocedure")).rows[0]!.s;
    expect(body).toContain("sessions.session_analysis_admit_live_v1(p_org_id, p_workspace_id, p_session_id)");
    expect(body).toContain("sessions.session_analysis_admit_job_v1(p_org_id, p_workspace_id, p_session_id)");
    const helpers = bodyOf(UP, "session_analysis_admit_live_v1", "$admit_live$") + bodyOf(UP, "session_analysis_admit_job_v1", "$admit_job$");
    const original = bodyOf(ORIGINAL, "analysis_request_v1", "$request$");
    for (const line of original.split("\n").filter((l) => /pg_advisory_xact_lock|purge_fences|session_analysis_source_revision|INSERT INTO sessions.session_analysis_jobs_v2|completed_at > now\(\) - interval '30 minutes'/.test(l)))
      expect(helpers).toContain(line);
    expect(helpers).toContain(`c_analysis_version constant text := '${SESSION_ANALYSIS_VERSION}'`);
    expect(helpers).toContain(`c_schema_version constant integer := ${SESSION_ANALYSIS_RESULT_SCHEMA_VERSION}`);
    expect(helpers).toContain(`c_model_version constant text := '${SESSION_ANALYSIS_MODEL_VERSION}'`);
    expect(helpers).toContain(`c_summary_version constant text := '${SESSION_ANALYSIS_SUMMARY_VERSION}'`);
    expect(helpers).toContain(`c_summary_model_version constant text := '${SESSION_ANALYSIS_SUMMARY_MODEL_VERSION}'`);
    // The ACL is unchanged: only the admission principal.
    const grantees = (await pg.query<{ g: string }>("SELECT DISTINCT pg_get_userbyid(a.grantee) g FROM pg_proc p, aclexplode(p.proacl) a WHERE p.oid='sessions.analysis_request_v1(text,text,text,text)'::regprocedure AND a.grantee <> p.proowner")).rows.map((r) => r.g);
    expect(grantees).toEqual(["bulls96_analysis_admit"]);
  });

  test("parity: the same scenario gives the same outcomes and rows under the replaced body and under 046000's", async () => {
    const scenario = async () => {
      const out: unknown[] = [];
      const shape = (r: Json) => ({ outcome: r.outcome, status: r.request?.status ?? null, error: r.request?.error ?? null });
      out.push(shape(await request("")));
      out.push(shape(await request("missing")));
      const first = await request();
      out.push(shape(first));
      out.push(shape(await request()));                          // in flight answers
      await complete();
      await pg.query("UPDATE sessions.session_analysis_requests_v2 SET created_at = created_at - interval '3 minutes'");
      const reused = await request();                           // reused within 30 minutes
      out.push(shape(reused), reused.request.analysisId === first.request.analysisId);
      out.push(shape(await request()));                          // throttled
      await pg.query("INSERT INTO sessions.session_analysis_purge_fences (org_id, workspace_id, session_id, purged_at) VALUES ($1,$2,'fenced',now())", [KEY.orgId, KEY.workspaceId]);
      await pointer(pg, { key: key("fenced") });
      await pg.query("UPDATE sessions.session_index SET created_at = now() - interval '1 hour' WHERE session_id='fenced'");
      out.push(shape(await request("fenced")));                  // purged: not found
      out.push((await jobs()).length, (await pg.query("SELECT 1 FROM sessions.session_analysis_requests_v2")).rows.length);
      return out;
    };
    const replaced = await scenario();
    await pg.close();
    pg = await database();
    await pg.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
    await pg.exec(read("supabase/bootstrap/session_analysis_admit_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
    await pointer(pg);
    await pg.exec(`CREATE OR REPLACE${ORIGINAL.slice(ORIGINAL.indexOf("CREATE FUNCTION sessions.analysis_request_v1(") + "CREATE".length, ORIGINAL.indexOf("$request$", ORIGINAL.indexOf("$request$", ORIGINAL.indexOf("CREATE FUNCTION sessions.analysis_request_v1(")) + 9) + 9)};`);
    expect((await pg.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.analysis_request_v1(text,text,text,text)'::regprocedure")).rows[0]!.s)
      .toBe(bodyOf(ORIGINAL, "analysis_request_v1", "$request$"));
    expect(await scenario()).toEqual(replaced);
    expect(replaced).toEqual([
      { outcome: "invalid_request", status: null, error: null }, { outcome: "not_found", status: null, error: null },
      { outcome: "accepted", status: "queued", error: null }, { outcome: "accepted", status: "queued", error: null },
      { outcome: "accepted", status: "completed", error: null }, true, { outcome: "accepted", status: "completed", error: null },
      { outcome: "not_found", status: null, error: null }, 1, 2,
    ]);
  });
});

describe("062180: the scheduler admission", () => {
  beforeEach(async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true], ["workspace", "org-z", "ws-z", true]]);
  });

  test("grants: service_role (and the runtime role) only; helpers owner-only; the ledger has RLS and no table privilege", async () => {
    const can = async (role: string, fn: string) => (await pg.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of [SCHEDULE, CANDIDATES, JOB]) {
      expect(await can("service_role", fn)).toBe(true);
      for (const role of ["anon", "authenticated", "bulls96_read_api", "bulls96_analysis_admit", "search_consumer_runtime"]) expect([fn, role, await can(role, fn)]).toEqual([fn, role, false]);
      const r = (await pg.query<{ secdef: boolean; cfg: string[]; pub: boolean }>("SELECT prosecdef secdef, proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(proacl) a WHERE a.grantee=0) pub FROM pg_proc WHERE oid=$1::regprocedure", [fn])).rows[0]!;
      expect(r).toEqual({ secdef: true, cfg: ["search_path=pg_catalog, sessions, pg_temp"], pub: false });
    }
    for (const fn of ["sessions.session_analysis_admit_live_v1(text,text,text)", "sessions.session_analysis_admit_job_v1(text,text,text)"])
      for (const role of ["service_role", "bulls96_read_api", "bulls96_analysis_admit"]) expect(await can(role, fn)).toBe(false);
    for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
      for (const role of ["service_role", "bulls96_read_api", "bulls96_analysis_admit"])
        expect((await pg.query<{ ok: boolean }>("SELECT has_table_privilege($1, 'sessions.session_analysis_schedule_ledger_v1', $2) ok", [role, priv])).rows[0]!.ok).toBe(false);
    expect((await pg.query<{ rls: boolean }>("SELECT relrowsecurity rls FROM pg_class WHERE oid='sessions.session_analysis_schedule_ledger_v1'::regclass")).rows[0]!.rls).toBe(true);
    // The caller pin: a non-worker role is refused even with a grant.
    await pg.exec(`GRANT EXECUTE ON FUNCTION ${SCHEDULE} TO bulls96_read_api`);
    expect(await code(asRole("bulls96_read_api", () => pg.query(`SELECT ${SCHEDULE.replace(/\(.*$/, "")}('org-a','ws-a','s-1',5,40,600,5000000,1000)`)))).toBe("42501");
    expect((await pg.query<{ applied: boolean }>(MIGRATION_SENTINELS["upstream:20261105062180_claims_scheduler.sql"]!.sql)).rows[0]!.applied).toBe(true);
  });

  test("admission checks: invalid, not found, purged; enqueued writes a job and a ledger row; in flight; reused writes nothing; never a request row", async () => {
    expect(await schedule({ session: "" })).toEqual({ outcome: "invalid_request" });
    expect((await asRole("service_role", () => pg.query<{ r: Json }>(`SELECT ${SCHEDULE.replace(/\(.*$/, "")}('org-a','ws-a','s-1',NULL,40,600,5000000,1000) AS r`))).rows[0]!.r).toEqual({ outcome: "invalid_request" });
    expect(await schedule({ session: "missing" })).toEqual({ outcome: "not_found" });
    expect(await schedule()).toEqual({ outcome: "enqueued", reservedTokens: CLAIMS_SCHEDULER_RESERVE_TOKENS });
    expect(await ledger()).toEqual([{ k: "org-a/ws-a/s-1", reserved: CLAIMS_SCHEDULER_RESERVE_TOKENS }]);
    expect(await jobs()).toEqual([{ k: "org-a/ws-a/s-1", status: "queued" }]);
    expect(await schedule()).toEqual({ outcome: "in_flight" });
    // A user request sees the scheduler's job only through reuse once complete (it has no request row).
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_requests_v2")).rows).toHaveLength(0);
    await complete();
    await pg.query("UPDATE sessions.session_analysis_schedule_ledger_v1 SET created_at = created_at - interval '20 minutes'");
    expect(await schedule()).toEqual({ outcome: "reused" });
    expect((await ledger()).length).toBe(1);
    expect((await jobs()).length).toBe(1);
    await pg.query("INSERT INTO sessions.session_analysis_purge_fences (org_id, workspace_id, session_id, purged_at) VALUES ('org-a','ws-a','fenced',now())");
    await pointer(pg, { key: key("fenced") });
    await pg.query("UPDATE sessions.session_index SET created_at = now() - interval '1 hour' WHERE session_id='fenced'");
    expect(await schedule({ session: "fenced" })).toEqual({ outcome: "not_found" });
  });

  test("budgets: session interval, workspace hourly, global daily across orgs, daily tokens; over budget writes nothing; counts survive a restart", async () => {
    for (const s of ["s-2", "s-3", "s-4"]) await pointer(pg, { key: key(s) });
    await pointer(pg, { key: key("o-1", "ws-z", "org-z") });
    expect(await schedule({}, { interval: 600 })).toMatchObject({ outcome: "enqueued" });
    // Per session: the job completes; within the interval it is over budget, after it not.
    await complete();
    await pointer(pg);
    expect(await schedule({}, { interval: 600 })).toEqual({ outcome: "over_budget", reason: "session_interval" });
    // Workspace hourly cap 2: s-2 fits, s-3 does not; another org's workspace is not counted.
    expect(await schedule({ session: "s-2" }, { hourly: 2 })).toMatchObject({ outcome: "enqueued" });
    expect(await schedule({ session: "s-3" }, { hourly: 2 })).toEqual({ outcome: "over_budget", reason: "workspace_hourly" });
    // Global daily cap 2 (two rows today): another org is refused.
    expect(await schedule({ org: "org-z", ws: "ws-z", session: "o-1" }, { daily: 2 })).toEqual({ outcome: "over_budget", reason: "global_daily" });
    // Tokens: two reservations of 1000 today... here the two rows reserve the default; a ceiling just above them refuses a third.
    const spent = (await ledger()).reduce((n, r) => n + r.reserved, 0);
    expect(await schedule({ org: "org-z", ws: "ws-z", session: "o-1" }, { ceiling: spent + CLAIMS_SCHEDULER_RESERVE_TOKENS - 1 })).toEqual({ outcome: "over_budget", reason: "daily_tokens" });
    expect(await schedule({ org: "org-z", ws: "ws-z", session: "o-1" }, { ceiling: spent + CLAIMS_SCHEDULER_RESERVE_TOKENS })).toMatchObject({ outcome: "enqueued" });
    expect((await ledger()).length).toBe(3);
    // A "restart" (a new connection or worker) sees the same counts: they live in the ledger.
    await pg.query("SELECT 1");
    expect(await schedule({ session: "s-3" }, { hourly: 2 })).toEqual({ outcome: "over_budget", reason: "workspace_hourly" });
    // Yesterday's rows do not count against today's caps; an hour-old row frees the hourly cap.
    await pg.query("UPDATE sessions.session_analysis_schedule_ledger_v1 SET created_at = created_at - interval '2 days'");
    expect(await schedule({ session: "s-3" }, { hourly: 2, daily: 1 })).toMatchObject({ outcome: "enqueued" });
  });

  test("clamping and retention: out-of-range budgets are clamped; rows older than 8 days are deleted", async () => {
    await pointer(pg, { key: key("s-2") });
    // hourly 0 -> 1, reserve 10^9 -> 10^6.
    expect(await schedule({}, { hourly: 0, reserve: 1_000_000_000 })).toEqual({ outcome: "enqueued", reservedTokens: 1_000_000 });
    expect(await schedule({ session: "s-2" }, { hourly: 0 })).toEqual({ outcome: "over_budget", reason: "workspace_hourly" });
    await pg.query("UPDATE sessions.session_analysis_schedule_ledger_v1 SET created_at = now() - interval '9 days'");
    expect(await schedule({ session: "s-2" })).toMatchObject({ outcome: "enqueued" });
    expect((await ledger()).map((r) => r.k)).toEqual(["org-a/ws-a/s-2"]);
  });

  test("erase never refunds spend: cap 1, s-1 enqueued, s-2 over budget; after s-1's jobs are erased s-2 stays over budget", async () => {
    await pointer(pg, { key: key("s-2") });
    expect(await schedule({}, { hourly: 1 })).toMatchObject({ outcome: "enqueued" });
    expect(await schedule({ session: "s-2" }, { hourly: 1 })).toEqual({ outcome: "over_budget", reason: "workspace_hourly" });
    // The operator erases (per session and per user) tombstone or delete the pointers; the forget trigger removes
    // the session's jobs. The ledger row stays, without its job, and still counts.
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 's-1'");
    expect(await jobs()).toEqual([]);
    expect(await ledger()).toEqual([{ k: "org-a/ws-a/-", reserved: CLAIMS_SCHEDULER_RESERVE_TOKENS }]);
    expect(await schedule({ session: "s-2" }, { hourly: 1 })).toEqual({ outcome: "over_budget", reason: "workspace_hourly" });
    expect(await schedule({ session: "s-2" }, { daily: 1 })).toEqual({ outcome: "over_budget", reason: "global_daily" });
    expect(await schedule({ session: "s-2" }, { ceiling: CLAIMS_SCHEDULER_RESERVE_TOKENS })).toEqual({ outcome: "over_budget", reason: "daily_tokens" });
    expect((await ledger()).length).toBe(1);
    // The kept row holds no session identity at all: there is no session column, and its job id is gone.
    const cols = (await pg.query<{ c: string }>("SELECT column_name c FROM information_schema.columns WHERE table_schema='sessions' AND table_name='session_analysis_schedule_ledger_v1' ORDER BY ordinal_position")).rows.map((r) => r.c);
    expect(cols).toEqual(["id", "org_id", "workspace_id", "analysis_job_id", "reserved_tokens", "created_at"]);
    expect((await pg.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_analysis_schedule_ledger_v1 WHERE analysis_job_id IS NULL")).rows[0]!.n).toBe(1);
    // Any other job delete (the C3 purge, a direct delete) keeps the spend the same way.
    expect(await schedule({ session: "s-2" }, { hourly: 2 })).toMatchObject({ outcome: "enqueued" });
    await pg.query("DELETE FROM sessions.session_analysis_jobs_v2");
    expect(await ledger()).toEqual([{ k: "org-a/ws-a/-", reserved: CLAIMS_SCHEDULER_RESERVE_TOKENS }, { k: "org-a/ws-a/-", reserved: CLAIMS_SCHEDULER_RESERVE_TOKENS }]);
    expect(await schedule({ session: "s-2" }, { hourly: 2 })).toEqual({ outcome: "over_budget", reason: "workspace_hourly" });
  });

  test("the claims control: off globally or for the workspace is control_off, before any lock, and writes nothing", async () => {
    await controls([["workspace", "org-a", "ws-a", false]]);
    expect(await schedule()).toEqual({ outcome: "control_off" });
    await controls([["workspace", "org-a", "ws-a", true], ["global", null, null, false]]);
    expect(await schedule()).toEqual({ outcome: "control_off" });
    // No workspace row at all (another workspace with a pointer): off.
    await controls([["global", null, null, true]]);
    await pointer(pg, { key: key("q-1", "ws-q") });
    expect(await schedule({ ws: "ws-q", session: "q-1" })).toEqual({ outcome: "control_off" });
    expect(await ledger()).toEqual([]);
    expect(await jobs()).toEqual([]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
  });

  test("READ COMMITTED only: under REPEATABLE READ or SERIALIZABLE the admission refuses (25000) and writes nothing", async () => {
    for (const level of ["REPEATABLE READ", "SERIALIZABLE"]) {
      await pg.exec(`BEGIN ISOLATION LEVEL ${level}`);
      try {
        await pg.exec("SET LOCAL ROLE service_role");
        expect(await code(pg.query(`SELECT ${SCHEDULE.replace(/\(.*$/, "")}('org-a','ws-a','s-1',5,40,600,5600000,1000) AS r`))).toBe("25000");
      } finally { await pg.exec("ROLLBACK"); }
    }
    expect(await ledger()).toEqual([]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
  });
});

describe("062180: live-session candidates", () => {
  test("only workspaces whose claims control is on globally and for the workspace", async () => {
    expect(await candidates()).toEqual([]);
    await controls([["workspace", "org-a", "ws-a", true]]);
    expect(await candidates()).toEqual([]);
    await controls([["global", null, null, true]]);
    expect(await candidates()).toEqual(["org-a/ws-a/s-1"]);
    await controls([["workspace", "org-a", "ws-a", false]]);
    expect(await candidates()).toEqual([]);
  });

  test("live window, in flight, activity newer than the last analysis, and the interval", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    await pointer(pg, { key: key("old") });
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() - interval '3 hours' WHERE session_id = 'old'");
    expect(await candidates()).toEqual(["org-a/ws-a/s-1"]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    expect(await candidates()).toEqual([]);                     // in flight
    await complete();
    expect(await candidates()).toEqual([]);                     // completed after the last activity
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET completed_at = now() - interval '5 minutes'");
    await pg.query("UPDATE sessions.session_analysis_schedule_ledger_v1 SET created_at = now() - interval '20 minutes'");
    expect(await candidates()).toEqual([]);                     // completed within the interval
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET completed_at = now() - interval '20 minutes'");
    expect(await candidates()).toEqual(["org-a/ws-a/s-1"]);     // newer activity, interval passed
    expect(await candidates(7200, 1800)).toEqual([]);           // a longer interval: the ledger row is too recent
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() - interval '30 minutes' WHERE session_id = 's-1'");
    expect(await candidates()).toEqual([]);                     // no activity since the analysis
  });

  test("a session whose last analysis failed waits for new activity (it is not re-enqueued every interval)", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET status = 'failed', updated_at = now() - interval '15 minutes'");
    await pg.query("UPDATE sessions.session_analysis_schedule_ledger_v1 SET created_at = now() - interval '20 minutes'");
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() - interval '30 minutes' WHERE session_id = 's-1'");
    expect(await candidates()).toEqual([]);                     // failed after the last activity
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() - interval '1 minute' WHERE session_id = 's-1'");
    expect(await candidates()).toEqual(["org-a/ws-a/s-1"]);     // new activity since the failure
  });

  test("round-robin across workspaces: 55 live sessions in ws-a and one older in ws-b; ws-b is read and enqueued", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true], ["workspace", "org-a", "ws-b", true]]);
    for (let i = 2; i <= 55; i += 1) await pointer(pg, { key: key(`s-${i}`) });
    await pointer(pg, { key: key("b-1", "ws-b") });
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() - interval '90 minutes' WHERE workspace_id = 'ws-b'");
    const list = await candidates(7200, 600, 50);
    expect(list.length).toBe(50);
    // Round 1 is each workspace's newest (newest first), so ws-b's only session is second, not starved.
    expect(list[1]).toBe("org-a/ws-b/b-1");
    expect(list.filter((k) => k.startsWith("org-a/ws-a/")).length).toBe(49);
    const db = { query: (t: string, p?: unknown[]) => asRole("service_role", () => pg.query(t, p as never)) } as never;
    const counts = await claimsSchedulerTick({ db, controls: sessionIntelControls(db), settings: claimsSchedulerSettings({}) });
    expect(counts).toEqual({ selected: 50, enqueued: 6, skipped_budget: 44, skipped_control: 0, skipped: 0, failed: 0 });
    expect((await ledger()).map((r) => r.k)).toContain("org-a/ws-b/b-1");
  });
});

describe("062180: the worker's scheduled-job gate", () => {
  test("a queued scheduled job whose control is turned off is cancelled before any evidence read or provider call; other jobs run", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    const db = { query: (t: string, p?: unknown[]) => asRole("service_role", () => pg.query(t, p as never)) } as never;
    const counts: Record<string, number> = {};
    const gate = scheduledJobGate(db, counts);
    const id = (await pg.query<{ id: string }>("SELECT id::text id FROM sessions.session_analysis_jobs_v2")).rows[0]!.id;
    expect(await gate({ id })).toEqual({ run: true });
    await controls([["workspace", "org-a", "ws-a", false]]);
    expect(await gate({ id })).toEqual({ run: false, code: "claims_control_off", retryable: false });
    const store = postgresAnalysisJobStore({ db: hoopDb(pg), store: createFsStore({ root: mkdtempSync(join(tmpdir(), "claims-gate-")) }), anchorManifestKey: async () => "unused" });
    let touched = 0;
    const untouchable = new Proxy({}, { get: () => { touched += 1; throw new Error("must not be read"); } }) as never;
    expect(await runAnalysisJob(store, { evidence: untouchable, transport: untouchable, admit: gate })).toBe("cancelled");
    expect(touched).toBe(0);
    expect((await pg.query("SELECT status, last_error, attempts FROM sessions.session_analysis_jobs_v2")).rows).toEqual([{ status: "failed", last_error: "claims_control_off", attempts: 1 }]);
    expect(counts).toEqual({ scheduled_control_off: 2 }); // the direct gate call above, and the worker's
    // The spend stays (the job was enqueued); a user-requested job, with no ledger row, is unscheduled and runs.
    expect((await ledger()).length).toBe(1);
    await pointer(pg, { key: key("s-2") });
    expect((await request("s-2")).outcome).toBe("accepted");
    const userJob = (await pg.query<{ id: string }>("SELECT id::text id FROM sessions.session_analysis_jobs_v2 WHERE session_id = 's-2'")).rows[0]!.id;
    expect(await gate({ id: userJob })).toEqual({ run: true });
    expect(await gate({ id: "00000000-0000-4000-8000-000000000000" })).toEqual({ run: true });
    // The gate reads as service_role only.
    expect(await code(asRole("bulls96_read_api", () => pg.query(`SELECT ${JOB.replace(/\(.*$/, "")}($1::uuid)`, [id])))).toBe("42501");
  });

  test("wired whenever analysis runs: with every claims flag off, the tick fails a queued scheduled job as claims_control_off, no provider call", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    await controls([["workspace", "org-a", "ws-a", false]]);
    const db = { query: (t: string, p?: unknown[]) => asRole("service_role", () => pg.query(t, p as never)) } as never;
    const deps = analysisTickDeps({
      db, transact: async () => { throw new Error("no transaction"); }, store: createFsStore({ root: mkdtempSync(join(tmpdir(), "claims-off-")) }),
      bucket: "bucket", region: "us-east-1", keyPrefix: "", modelKey: "k", credentials: () => ({ accessKeyId: "a", secretAccessKey: "s" }),
      env: { SESSION_ANALYSIS_ENABLED: "true" },
    });
    expect(deps.claims).toBeUndefined();
    expect(deps.decisionRecords).toBeUndefined();
    expect(deps.scheduledJobGate).toBe(scheduledJobGate);
    let touched = 0;
    const untouchable = new Proxy({}, { get: () => { touched += 1; throw new Error("must not be called"); } }) as never;
    const counts = await analysisTick({ config: { ...deps.config, batch: 1 }, db, store: deps.store, canonical: deps.canonical, transport: untouchable, scheduledJobGate: deps.scheduledJobGate })();
    expect(counts).toMatchObject({ cancelled: 1, scheduled_control_off: 1 });
    expect(touched).toBe(0);
    expect((await pg.query("SELECT status, last_error FROM sessions.session_analysis_jobs_v2")).rows).toEqual([{ status: "failed", last_error: "claims_control_off" }]);
  });

  test("a failing gate read never stops a user-requested job; a scheduled job is held back (retryable)", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    await pointer(pg, { key: key("s-2") });
    expect((await request("s-2")).outcome).toBe("accepted");
    const idOf = async (s: string) => (await pg.query<{ id: string }>("SELECT id::text id FROM sessions.session_analysis_jobs_v2 WHERE session_id = $1", [s])).rows[0]!.id;
    // The gate function fails (here: a statement timeout); the request-row read is real.
    const db = { query: (t: string, p?: unknown[]) => t.includes("session_analysis_schedule_job_v1")
      ? Promise.reject(Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }))
      : asRole("service_role", () => pg.query(t, p as never)) } as never;
    const counts: Record<string, number> = {};
    const gate = scheduledJobGate(db, counts);
    expect(await gate({ id: await idOf("s-2") })).toEqual({ run: true });
    expect(await gate({ id: await idOf("s-1") })).toEqual({ run: false, code: "claims_scheduler_gate_failed", retryable: true });
    expect(counts).toEqual({ scheduled_gate_failed: 2 });
  });
});

describe("062180: gate holds and the runtime role", () => {
  test("tick level: a scheduled job held by gate errors is requeued with backoff twice, failed for good on its third hold, then its session waits for new activity", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    const db = { query: (t: string, p?: unknown[]) => t.includes("session_analysis_schedule_job_v1")
      ? Promise.reject(Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }))
      : asRole("service_role", () => pg.query(t, p as never)) } as never;
    const deps = analysisTickDeps({
      db, transact: async () => { throw new Error("no transaction"); }, store: createFsStore({ root: mkdtempSync(join(tmpdir(), "claims-hold-")) }),
      bucket: "bucket", region: "us-east-1", keyPrefix: "", modelKey: "k", credentials: () => ({ accessKeyId: "a", secretAccessKey: "s" }),
      env: { SESSION_ANALYSIS_ENABLED: "true" },
    });
    let touched = 0;
    const untouchable = new Proxy({}, { get: () => { touched += 1; throw new Error("must not be called"); } }) as never;
    const tick = () => analysisTick({ config: { ...deps.config, batch: 1 }, db, store: deps.store, canonical: deps.canonical, transport: untouchable, scheduledJobGate: deps.scheduledJobGate })();
    const row = async () => (await pg.query<{ status: string; attempts: number; last_error: string; due: boolean }>(
      "SELECT status, attempts, last_error, available_at > now() AS due FROM sessions.session_analysis_jobs_v2")).rows;
    for (const attempt of [1, 2]) {
      expect(await tick()).toMatchObject({ cancelled: 1, scheduled_gate_failed: 1 });
      // Requeued for its backoff (15 s, then 30 s): not claimable until then.
      expect(await row()).toEqual([{ status: "queued", attempts: attempt, last_error: "claims_scheduler_gate_failed", due: true }]);
      expect(await tick()).toMatchObject({ idle: 1 });
      await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET available_at = now() - interval '1 second'");
    }
    expect(await tick()).toMatchObject({ cancelled: 1, scheduled_gate_failed: 1 });
    expect((await row()).map(({ due: _, ...r }) => r)).toEqual([{ status: "failed", attempts: 3, last_error: "claims_scheduler_gate_failed" }]);
    expect(touched).toBe(0);
    // Failed after the last activity: the candidates skip the session until it is active again.
    await pg.query("UPDATE sessions.session_analysis_schedule_ledger_v1 SET created_at = now() - interval '20 minutes'");
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() - interval '30 minutes'");
    expect(await candidates()).toEqual([]);
    await pg.query("UPDATE sessions.session_index SET last_activity_at = now() + interval '1 second'");
    expect(await candidates()).toEqual(["org-a/ws-a/s-1"]);
  });

  test("the request-row read and the gate work under RLS as a NOBYPASSRLS runtime role with the migrator's runtime ACL", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    await pointer(pg, { key: key("s-2") });
    expect((await request("s-2")).outcome).toBe("accepted");
    // The runtime role exactly as the migrator installs it (installRuntimeApplicationAcl): table grants, the
    // trusted-backend all-row policy on every RLS table, function EXECUTE. No BYPASSRLS, no ownership.
    await pg.exec("CREATE ROLE history_rt NOLOGIN NOINHERIT NOBYPASSRLS; GRANT USAGE ON SCHEMA sessions TO history_rt");
    const catalog = runtimeApplicationAclCatalog();
    for (const [rel, privs] of Object.entries(catalog.tables))
      if ((await pg.query<{ r: string | null }>("SELECT to_regclass($1)::text r", [rel])).rows[0]!.r) await pg.exec(`GRANT ${privs.join(", ")} ON TABLE ${rel} TO history_rt`);
    for (const rel of catalog.rlsTables)
      if ((await pg.query<{ r: string | null }>("SELECT to_regclass($1)::text r", [rel])).rows[0]!.r)
        await pg.exec(`CREATE POLICY ${RUNTIME_APPLICATION_POLICY} ON ${rel} AS PERMISSIVE FOR ALL TO history_rt USING (true) WITH CHECK (true)`);
    for (const fn of catalog.functions)
      if ((await pg.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [fn])).rows[0]!.r) await pg.exec(`GRANT EXECUTE ON FUNCTION ${fn} TO history_rt`);
    expect((await pg.query<{ b: boolean }>("SELECT rolbypassrls b FROM pg_roles WHERE rolname = 'history_rt'")).rows[0]!.b).toBe(false);
    expect((await pg.query<{ rls: boolean }>("SELECT relrowsecurity rls FROM pg_class WHERE oid = 'sessions.session_analysis_requests_v2'::regclass")).rows[0]!.rls).toBe(true);
    const asRt = { query: (t: string, p?: unknown[]) => asRole("history_rt", () => pg.query(t, p as never)) } as never;
    const idOf = async (sid: string) => (await pg.query<{ id: string }>("SELECT id::text id FROM sessions.session_analysis_jobs_v2 WHERE session_id = $1", [sid])).rows[0]!.id;
    const requested = async (id: string) => ((await (asRt as { query: (t: string, p: unknown[]) => Promise<{ rows: { requested: boolean }[] }> }).query(USER_REQUESTED_JOB_SQL, [id])).rows[0]!.requested);
    expect(await requested(await idOf("s-2"))).toBe(true);
    expect(await requested(await idOf("s-1"))).toBe(false);
    // The real gate as the runtime role: the user job is unscheduled, the scheduled one is on, then off.
    expect(await scheduledJobGate(asRt)({ id: await idOf("s-2") })).toEqual({ run: true });
    expect(await scheduledJobGate(asRt)({ id: await idOf("s-1") })).toEqual({ run: true });
    await controls([["workspace", "org-a", "ws-a", false]]);
    expect(await scheduledJobGate(asRt)({ id: await idOf("s-1") })).toEqual({ run: false, code: "claims_control_off", retryable: false });
    // A gate failure as the runtime role still lets the user job through (the request row is visible under RLS).
    const failing = { query: (t: string, p?: unknown[]) => t.includes("session_analysis_schedule_job_v1")
      ? Promise.reject(Object.assign(new Error("down"), { code: "57014" })) : asRole("history_rt", () => pg.query(t, p as never)) } as never;
    expect(await scheduledJobGate(failing)({ id: await idOf("s-2") })).toEqual({ run: true });
    expect(await scheduledJobGate(failing)({ id: await idOf("s-1") })).toEqual({ run: false, code: "claims_scheduler_gate_failed", retryable: true });
  });
});

describe("the scheduler tick against the real functions", () => {
  test("enqueues through the admission, skips a workspace whose control is off, and counts budgets", async () => {
    await controls([["global", null, null, true], ["workspace", "org-a", "ws-a", true]]);
    for (const s of ["s-2", "s-3"]) await pointer(pg, { key: key(s) });
    await pointer(pg, { key: key("b-1", "ws-b") });
    await controls([["workspace", "org-a", "ws-b", true]]);
    const db = { query: (t: string, p?: unknown[]) => asRole("service_role", () => pg.query(t, p as never)) } as never;
    // The shared reader decides: ws-b reads as off (its read fails), even though the candidates query lists it.
    const real = sessionIntelControls(db);
    const reader = { read: async (o: string, w: string) => { if (w === "ws-b") throw new Error("down"); return real.read(o, w); } };
    const settings = claimsSchedulerSettings({ SESSION_INTEL_CLAIMS_WORKSPACE_HOURLY_CAP: "2" });
    const counts = await claimsSchedulerTick({ db, controls: reader, settings });
    expect(counts).toEqual({ selected: 4, enqueued: 2, skipped_budget: 1, skipped_control: 1, skipped: 0, failed: 0 });
    expect((await ledger()).length).toBe(2);
    // The next tick: nothing new is due.
    expect(await claimsSchedulerTick({ db, controls: reader, settings })).toEqual({ selected: 2, enqueued: 0, skipped_budget: 1, skipped_control: 1, skipped: 0, failed: 0 });
  });
});

describe("062180 rollback", () => {
  test("the down restores 046000's analysis_request_v1 exactly and refuses while ledger rows exist unless overridden", () => {
    expect(DOWN).toContain(new Bun.CryptoHasher("sha256").update(UP).digest("hex"));
    expect(DOWN).toContain(`CREATE OR REPLACE FUNCTION sessions.analysis_request_v1(`);
    expect(DOWN).toContain(bodyOf(ORIGINAL, "analysis_request_v1", "$request$"));
    expect(DOWN).toContain("claims_scheduler.rollback_drop_ledger");
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
  });

  test("executed: refuses (55000) while a ledger row exists unless the override is exactly 'true'; then drops everything and restores 046000", async () => {
    const STEP = "upstream:20261105062180_claims_scheduler.sql";
    const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
    const db = new MigrationPGlite({ extensions: { btree_gin, pg_trgm } });
    const client: MigrationClient = { async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      return { rows: ((await db.exec(text)).at(-1)?.rows ?? []) as R[] };
    } };
    const refusal = async (sql: string) => {
      const e = await db.exec(sql).then(() => null, (err: { code?: string; message?: string }) => err);
      await db.exec("ROLLBACK");
      return e === null ? null : { code: e.code, message: e.message };
    };
    try {
      await runMigrations(client, { profile: LOCAL });
      const later = (await plan(LOCAL)).filter((x) => x.name.startsWith("upstream:") && x.name > STEP).map((x) => x.name);
      expect(later).toEqual(["upstream:20261105062190_coordination_advisories_pending_priority.sql", "upstream:20261105062200_session_directory_list_readiness.sql", "upstream:20261105062210_lazy_ack_t2.sql"]);
      expect((await refusal(DOWN))?.message).toContain("a later migration is applied");
      await db.exec(read("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql")); // 062210 (T2 staged ACK) sits above 062200: first.
      await db.exec(read("supabase/rollbacks/20261105062200_session_directory_list_readiness.down.sql"));
      expect((await refusal(DOWN))?.message).toContain("a later migration is applied");
      await db.exec(read("supabase/rollbacks/20261105062190_coordination_advisories_pending_priority.down.sql"));
      // An empty ledger needs no override; one row of spend (here an erased job's) refuses.
      await db.query("INSERT INTO sessions.session_analysis_schedule_ledger_v1 (org_id, workspace_id, reserved_tokens) VALUES ('org-a','ws-a',1)");
      const refused = await refusal(DOWN);
      expect(refused?.code).toBe("55000");
      expect(refused?.message).toContain("ledger rows exist");
      for (const v of ["on", "TRUE", "1", "yes", ""]) {
        await db.exec(`SET claims_scheduler.rollback_drop_ledger = '${v}'`);
        expect([v, (await refusal(DOWN))?.code]).toEqual([v, "55000"]);
      }
      await db.exec("RESET claims_scheduler.rollback_drop_ledger");
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_analysis_schedule_ledger_v1")).rows[0]!.n).toBe(1);
      await db.exec("SET claims_scheduler.rollback_drop_ledger = 'true'");
      await db.exec(DOWN);
      await db.exec("RESET claims_scheduler.rollback_drop_ledger");
      expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.analysis_request_v1(text,text,text,text)'::regprocedure")).rows[0]!.s)
        .toBe(bodyOf(ORIGINAL, "analysis_request_v1", "$request$"));
      for (const fn of [SCHEDULE, CANDIDATES, JOB, "sessions.session_analysis_admit_live_v1(text,text,text)", "sessions.session_analysis_admit_job_v1(text,text,text)"])
        expect([fn, (await db.query<{ r: string | null }>("SELECT to_regprocedure($1)::text r", [fn])).rows[0]!.r]).toEqual([fn, null]);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.session_analysis_schedule_ledger_v1')::text r")).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect((await refusal(DOWN))?.message).toContain("own applied checksum required");
    } finally { await db.close(); }
  });
});
