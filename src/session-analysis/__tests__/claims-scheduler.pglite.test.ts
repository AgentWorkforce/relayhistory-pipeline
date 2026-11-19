/**
 * 20261105062180 (provisional) claims scheduler, on PGlite with the full migration plan: analysis_request_v1 keeps
 * 046000's behaviour (parity against the restored 046000 body), the scheduler admission's checks and every
 * budget, retention, the ledger's erase paths, the live-session candidates and the grants; plus the TS tick
 * against the real functions.
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
import { claimsSchedulerSettings, claimsSchedulerTick, CLAIMS_SCHEDULER_RESERVE_TOKENS } from "../claims-scheduler.ts";
import { sessionIntelControls } from "../../shared/session-intel-controls.ts";
import { MIGRATION_SENTINELS } from "../../hoopdb/migration-sentinels.ts";

setDefaultTimeout(120_000);
type Json = Record<string, any>;
let pg: PGlite;
const UP = read("supabase/migrations/20261105062180_claims_scheduler.sql");
const DOWN = read("supabase/rollbacks/20261105062180_claims_scheduler.down.sql");
const ORIGINAL = read("supabase/migrations/20261105046000_analysis_jobs.sql");
const SCHEDULE = "sessions.session_analysis_schedule_v1(text,text,text,integer,integer,integer,bigint,bigint)";
const CANDIDATES = "sessions.session_analysis_schedule_candidates_v1(integer,integer,integer)";

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
const ledger = async () => (await pg.query<{ k: string; reserved: number }>(
  "SELECT org_id || '/' || workspace_id || '/' || session_id k, reserved_tokens::int reserved FROM sessions.session_analysis_schedule_ledger_v1 ORDER BY id")).rows;
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
  test("grants: service_role (and the runtime role) only; helpers owner-only; the ledger has RLS and no table privilege", async () => {
    const can = async (role: string, fn: string) => (await pg.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of [SCHEDULE, CANDIDATES]) {
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

  test("erase: the ledger never outlives its job (tombstone forget, C3 purge, any job delete)", async () => {
    await pointer(pg, { key: key("s-2") });
    expect(await schedule()).toMatchObject({ outcome: "enqueued" });
    expect(await schedule({ session: "s-2" })).toMatchObject({ outcome: "enqueued" });
    // The operator erases (per session and per user) tombstone or delete the pointers; the forget trigger removes
    // the session's jobs, and the ledger rows go with them.
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 's-1'");
    expect((await ledger()).map((r) => r.k)).toEqual(["org-a/ws-a/s-2"]);
    await pg.query("DELETE FROM sessions.session_analysis_jobs_v2");
    expect(await ledger()).toEqual([]);
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
});
