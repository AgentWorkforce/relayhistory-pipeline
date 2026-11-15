/**
 * Operator repair of directory sessions whose event times hold 1970-01-01 (event_ts = 'epoch').
 *
 * The relayhistory Claude parser writes ts_ms 0 for a record that carries no time. Before the fix in
 * packages/pipeline/src/lib/pipeline/relayhistory-projection.ts (activityMs), the projection took 0 as
 * 1970-01-01 and settled every timeless tool call under that event to the same instant, so the index
 * pass wrote an event_ts = 'epoch' row to sessions.session_directory_event_times (and event_first_ts)
 * and the read API showed events in 1970. The fixed projection gives such an event its delivery time,
 * but a session whose manifests do not change is never re-projected, so its epoch rows stay until
 * this tool marks it.
 *
 * Why an operator tool and not a migration: the fix is in the TypeScript projection, so only a pass of
 * the fixed consumer can write the right times. A migration that deleted the rows would leave the
 * directory claiming readiness over a wrong event set, and one that marked them would run whatever
 * consumer version happens to be deployed when the migrator runs. Run this after the fixed consumer
 * is deployed; a scope an old consumer re-projects meanwhile gets its epoch row back and a rerun
 * marks it again.
 *
 * Selection: per workspace (the DISTINCT org_id, workspace_id of sessions.session_directory, so each
 * per-workspace read uses session_directory_event_times_window_idx), the DISTINCT
 * (org_id, workspace_id, harness, session_id) of session_directory_event_times WHERE
 * event_ts = 'epoch'::timestamptz, keyset-paged. session_id there is the directory (canonical) id.
 * Only that exact instant is the bug's signature; a 1 ms or later 1970 time is a time the client
 * reported and is left alone.
 *
 * Mark (--apply only): the pipeline's own "owes a pass" state for a directory session, as the pointer
 * trigger sessions.session_directory_events_changed sets it (20261104021000):
 * event_projection_ready = false and event_projection_epoch + 1. Not ready is what
 * invalidateDirectoryEvents (search-index.ts) sets after an incomplete pass; the epoch bump also
 * fences a pass that read the old epoch (writeDirectoryEvents compares it), so a pass still running
 * old code cannot mark the session ready over the epoch rows after this tool ran. One autocommit
 * UPDATE per scope, re-checking in SQL that the row is live, ready, and still has an epoch row; a
 * session already not ready is counted `already_pending` and left alone (it owes a pass already, and
 * bumping its epoch again would only fence the pass that is about to fix it), so a rerun is a no-op
 * until the consumer has re-projected. Event rows are not deleted here: the consumer's next complete
 * pass replaces the scope's event times (writeDirectoryEvents drop_times/put_times).
 *
 * Re-projection: the self-host consumer (SERVICE=session-pipeline-consumer) picks marked sessions up
 * by itself: its freshness check (postgres-search-state.ts partials) treats a directory row that is
 * not ready, or whose epoch differs from its projection_state, as stale and re-sends a reconcile body
 * for every live manifest. The AWS Lambda consumer has no such sweep: there a marked session is
 * re-projected on its next S3 event or reconcile body for one of its manifests. Until a scope is
 * re-projected, the directory event readers treat its workspace as not ready
 * (sessions.session_directory_events_require_ready), as for any other pass the session owes.
 *
 * statement_timeout 30s, lock_timeout 1s (session-wide, reads included). A refused UPDATE is counted
 * by SQLSTATE in `refused` and the run goes on.
 *
 * Usage (a one-shot Job or exec in the services image):
 *   bun run /app/src/session-pipeline/epoch-event-time-repair.ts            plan (default): writes nothing
 *   bun run /app/src/session-pipeline/epoch-event-time-repair.ts --apply [--page 200] [--max-pages N]
 * Env: EPOCH_EVENT_TIME_REPAIR_DATABASE_URL or EPOCH_EVENT_TIME_REPAIR_DATABASE_URL_FILE (exactly one):
 *   a login that can SELECT sessions.session_directory_event_times and UPDATE sessions.session_directory
 *   (the schema owner); SESSION_POSTGRES_TLS_PROFILE as for the other self-host workloads.
 * Exit: 0 done; 3 incomplete (--max-pages reached, or a refused scope: rerun); 2 refused or
 *   misconfigured; 4 any other error (rerun). Output: one JSON line of counts, no ids.
 * Runbook: runbooks/epoch-event-time-repair.md.
 */
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import { postgresTlsOptions } from "../shared/postgres-tls.ts";
import { supabasePool } from "./lambda.ts";

export class EpochRepairRefused extends Error {
  override name = "EpochRepairRefused";
  constructor(readonly code: string) { super(`epoch event time repair refused: ${code}`); }
}
const refuse = (code: string): never => { throw new EpochRepairRefused(code); };

export interface EpochRepairDb {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

export interface EpochRepairOptions {
  apply: boolean;
  /** Scopes per page, within one workspace. */
  pageSize: number;
  /** Pages over all workspaces; unset is no limit. */
  maxPages?: number;
  log?: (event: Record<string, unknown>) => void;
}

export interface EpochRepairResult {
  mode: "plan" | "apply";
  workspaces: number;
  /** Directory sessions with an event_ts = 'epoch' row. */
  affected: number;
  /** Of those, live and ready: what apply marks (plan) or marked (apply). */
  markable: number;
  marked: number;
  /** Not ready already: owes a pass, left alone. */
  already_pending: number;
  /** Tombstoned, or no directory row: nothing to re-project. */
  not_live: number;
  /** Changed between selection and UPDATE (re-projected, tombstoned or marked by the pipeline). */
  raced: number;
  refused: number;
  refused_codes: Record<string, number>;
  complete: boolean;
}

interface Scope { org_id: string; workspace_id: string; harness: string; session_id: string; live: boolean; ready: boolean }

export const WORKSPACES_SQL = `SELECT DISTINCT org_id, workspace_id FROM sessions.session_directory ORDER BY org_id, workspace_id`;

export const PAGE_SQL = `SELECT t.org_id, t.workspace_id, t.harness, t.session_id,
       coalesce(d.deleted_at IS NULL, false) AS live, coalesce(d.event_projection_ready, false) AS ready
  FROM (SELECT DISTINCT org_id, workspace_id, harness, session_id FROM sessions.session_directory_event_times
         WHERE org_id = $1 AND workspace_id = $2 AND event_ts = 'epoch'::timestamptz
           AND ($3::text IS NULL OR (harness, session_id) > ($3, $4))
         ORDER BY harness, session_id LIMIT $5) t
  LEFT JOIN sessions.session_directory d
    ON d.org_id = t.org_id AND d.workspace_id = t.workspace_id AND d.harness = t.harness AND d.session_id = t.session_id
 ORDER BY t.harness, t.session_id`;

/** The pointer trigger's directory invalidation (20261104021000), re-checking every selection predicate. */
export const MARK_SQL = `UPDATE sessions.session_directory d
   SET event_projection_ready = false, event_projection_epoch = d.event_projection_epoch + 1
 WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = $4
   AND d.deleted_at IS NULL AND d.event_projection_ready
   AND EXISTS (SELECT 1 FROM sessions.session_directory_event_times t
                WHERE t.org_id = d.org_id AND t.workspace_id = d.workspace_id AND t.harness = d.harness
                  AND t.session_id = d.session_id AND t.event_ts = 'epoch'::timestamptz)
 RETURNING 1 AS marked`;

const SQLSTATE = /^[0-9A-Z]{5}$/;

export async function repairEpochEventTimes(db: EpochRepairDb, o: EpochRepairOptions): Promise<EpochRepairResult> {
  if (!Number.isInteger(o.pageSize) || o.pageSize < 1 || o.pageSize > 1000) refuse("page_invalid");
  if (o.maxPages !== undefined && (!Number.isInteger(o.maxPages) || o.maxPages < 1)) refuse("max_pages_invalid");
  const r: EpochRepairResult = { mode: o.apply ? "apply" : "plan", workspaces: 0, affected: 0, markable: 0, marked: 0, already_pending: 0, not_live: 0, raced: 0, refused: 0, refused_codes: {}, complete: false };
  await db.query("SET statement_timeout = '30s'");
  await db.query("SET lock_timeout = '1s'");
  const workspaces = (await db.query<{ org_id: string; workspace_id: string }>(WORKSPACES_SQL)).rows;
  let pages = 0;
  let stopped = false;
  for (const ws of workspaces) {
    r.workspaces++;
    let after: Scope | undefined;
    for (;;) {
      if (o.maxPages !== undefined && pages >= o.maxPages) { stopped = true; break; }
      pages++;
      const rows = (await db.query<Scope>(PAGE_SQL, [ws.org_id, ws.workspace_id, after?.harness ?? null, after?.session_id ?? null, o.pageSize])).rows;
      if (rows.length) after = rows[rows.length - 1];
      for (const row of rows) {
        r.affected++;
        if (!row.live) { r.not_live++; continue; }
        if (!row.ready) { r.already_pending++; continue; }
        r.markable++;
        if (!o.apply) continue;
        try {
          const done = await db.query(MARK_SQL, [row.org_id, row.workspace_id, row.harness, row.session_id]);
          if (done.rows.length === 1) r.marked++;
          else r.raced++;
        } catch (e) {
          const code = (e as { code?: unknown })?.code;
          if (typeof code !== "string" || !SQLSTATE.test(code)) throw e;
          r.refused++;
          r.refused_codes[code] = (r.refused_codes[code] ?? 0) + 1;
        }
      }
      o.log?.({ event: "epoch_event_time_repair_page", page: pages, affected: r.affected, marked: r.marked });
      if (rows.length < o.pageSize) break;
    }
    if (stopped) break;
  }
  r.complete = !stopped;
  return r;
}

export function epochRepairArgs(argv: readonly string[]): { apply: boolean; pageSize: number; maxPages?: number } {
  const known = new Set(["--apply", "--plan", "--page", "--max-pages"]);
  const out: { apply: boolean; pageSize: number; maxPages?: number } = { apply: false, pageSize: 200 };
  let plan = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (!known.has(a)) refuse("args_invalid");
    if (a === "--apply") out.apply = true;
    else if (a === "--plan") plan = true;
    else {
      const v = argv[++i];
      if (v === undefined || !/^\d{1,4}$/.test(v)) refuse("args_invalid");
      const n = Number(v);
      if (a === "--page") { if (n < 1 || n > 1000) refuse("args_invalid"); out.pageSize = n; }
      else { if (n < 1) refuse("args_invalid"); out.maxPages = n; }
    }
  }
  if (plan && out.apply) refuse("args_invalid");
  return out;
}

const MAX_FILE = 1 << 16;
export function epochRepairDatabaseUrl(env: NodeJS.ProcessEnv): string {
  const value = env.EPOCH_EVENT_TIME_REPAIR_DATABASE_URL, file = env.EPOCH_EVENT_TIME_REPAIR_DATABASE_URL_FILE;
  if (value !== undefined && file !== undefined) refuse("secret_source_ambiguous");
  let text = value ?? "";
  if (file !== undefined) {
    if (!isAbsolute(file)) refuse("file_invalid");
    try {
      const st = statSync(file);
      if (!st.isFile() || st.size < 1 || st.size > MAX_FILE) refuse("file_invalid");
      text = readFileSync(file, "utf8").replace(/\r?\n$/, "");
    } catch (e) {
      if (e instanceof EpochRepairRefused) throw e;
      refuse("file_invalid");
    }
  }
  if (!text) refuse("database_url_missing");
  return text;
}

export interface EpochRepairMainOverrides {
  /** Test-only port; the process entry never takes it from input. */
  db?: EpochRepairDb;
  log?: (event: Record<string, unknown>) => void;
}

export async function runEpochRepairMain(env: NodeJS.ProcessEnv, argv: readonly string[], overrides: EpochRepairMainOverrides = {}): Promise<EpochRepairResult> {
  const args = epochRepairArgs(argv);
  let end: (() => Promise<void>) | undefined;
  let db = overrides.db;
  if (!db) {
    const pool = supabasePool(epochRepairDatabaseUrl(env), 1, postgresTlsOptions(env));
    const client = await pool.pool.connect();
    end = async () => { client.release(); await pool.end(); };
    db = { query: async <R>(text: string, params?: unknown[]) => ({ rows: (await client.query(text, params as unknown[])).rows as R[] }) };
  }
  try {
    return await repairEpochEventTimes(db, { ...args, log: overrides.log ?? ((e) => console.error(JSON.stringify(e))) });
  } finally {
    await end?.().catch(() => {});
  }
}

export function exitCodeForResult(r: EpochRepairResult): 0 | 3 {
  return !r.complete || r.refused > 0 ? 3 : 0;
}

export function exitCodeForError(e: unknown): 2 | 4 {
  return e instanceof EpochRepairRefused ? 2 : 4;
}

if (import.meta.main) {
  runEpochRepairMain(process.env, process.argv.slice(2))
    .then((r) => {
      console.log(JSON.stringify({ event: "epoch_event_time_repair", ...r }));
      process.exit(exitCodeForResult(r));
    })
    .catch((e) => {
      const code = e instanceof EpochRepairRefused ? e.code : typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "repair_failed";
      console.error(JSON.stringify({ event: "epoch_event_time_repair", error: code }));
      process.exit(exitCodeForError(e));
    });
}
