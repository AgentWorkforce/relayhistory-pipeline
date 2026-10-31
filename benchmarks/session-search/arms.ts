/**
 * The local engines: one Postgres (or PGlite) holding the real 96BullSessions
 * migrations and the neon_baseline stand-in. Nothing here connects to a hosted
 * service; run.ts refuses non-local URLs before opening one.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_DIRECTORY_SEARCH_SQL } from "../../src/hoopdb/search-index-contract.ts";
import type { CorpusSession } from "./corpus.ts";
import type { Tenant } from "./queries.ts";

const root = join(import.meta.dir, "../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");

/** What both pg and PGlite offer. `exec` takes several statements, no parameters. */
export interface Db {
  query<R = Record<string, unknown>>(sql: string, args?: unknown[]): Promise<{ rows: R[] }>;
  exec(sql: string): Promise<void>;
}

export async function installSchema(db: Db): Promise<void> {
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(read("benchmarks/session-search/neon-baseline.sql"));
}

export const claims = (t: Tenant) => JSON.stringify({
  role: "session_directory_search_worker",
  app_metadata: { org_id: t.orgId, workspace_ids: [t.workspaceId] },
});

/** Written in batches: multi-row INSERT from a JSON document. */
export async function loadBatch(db: Db, sessions: CorpusSession[], withSummary: boolean): Promise<void> {
  await db.query(
    `INSERT INTO sessions.session_directory
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, task_title,
        agent_name, agent_name_normalized, project_id, git_repo, git_branch, model, state, event_count,
        machine_ids, user_ids, deleted_at, short_summary, short_summary_scrub_ruleset,
        short_summary_scrub_version, short_summary_source, short_summary_generated_at, search_text)
     SELECT r.org_id, r.workspace_id, r.harness, r.session_id, r.created_at, r.last_activity_at, r.title,
            r.task_title, r.agent_name, lower(btrim(r.agent_name)), r.project_id, r.git_repo, r.git_branch,
            r.model, r.state, r.event_count, r.machine_ids, r.user_ids,
            CASE WHEN r.deleted THEN r.last_activity_at END, r.summary,
            CASE WHEN r.summary IS NOT NULL THEN 'relayhistory-scrub' END,
            CASE WHEN r.summary IS NOT NULL THEN 1 END,
            CASE WHEN r.summary IS NOT NULL THEN 'benchmark/synthetic/v1' END,
            CASE WHEN r.summary IS NOT NULL THEN r.last_activity_at END,
            CASE WHEN r.deleted THEN '' ELSE sessions.session_directory_search_text(r.title, r.task_title,
              r.agent_name, r.project_id, r.git_repo, r.git_branch, r.model, r.summary) END
       FROM jsonb_to_recordset($1::jsonb) AS r(org_id text, workspace_id text, harness text, session_id text,
         created_at timestamptz, last_activity_at timestamptz, title text, task_title text, agent_name text,
         project_id text, git_repo text, git_branch text, model text, state text, event_count bigint,
         machine_ids text[], user_ids text[], deleted boolean, summary text)`,
    [JSON.stringify(sessions.map((s) => ({
      org_id: s.orgId, workspace_id: s.workspaceId, harness: s.harness, session_id: s.sessionId,
      created_at: s.createdAt, last_activity_at: s.lastActivityAt, title: s.title, task_title: s.taskTitle,
      agent_name: s.agentName, project_id: s.projectId, git_repo: s.gitRepo, git_branch: s.gitBranch,
      model: s.model, state: s.state, event_count: s.eventCount,
      machine_ids: `{${s.machineIds.join(",")}}`, user_ids: `{${s.userIds.join(",")}}`,
      deleted: s.deleted, summary: withSummary ? s.shortSummary : null,
    })))],
  );
  await db.query(
    `INSERT INTO sessions.session_directory_users (org_id, workspace_id, harness, session_id, user_id, last_activity_at)
     SELECT r.org_id, r.workspace_id, r.harness, r.session_id, r.user_id, r.at
       FROM jsonb_to_recordset($1::jsonb) AS r(org_id text, workspace_id text, harness text, session_id text, user_id text, at timestamptz)`,
    [JSON.stringify(sessions.filter((s) => !s.deleted).flatMap((s) => s.userIds.map((user_id) => ({
      org_id: s.orgId, workspace_id: s.workspaceId, harness: s.harness, session_id: s.sessionId, user_id, at: s.lastActivityAt,
    }))))],
  );
  await db.query(
    `INSERT INTO sessions.session_directory_machines (org_id, workspace_id, harness, session_id, machine_id, last_activity_at)
     SELECT r.org_id, r.workspace_id, r.harness, r.session_id, r.machine_id, r.at
       FROM jsonb_to_recordset($1::jsonb) AS r(org_id text, workspace_id text, harness text, session_id text, machine_id text, at timestamptz)`,
    [JSON.stringify(sessions.filter((s) => !s.deleted).flatMap((s) => s.machineIds.map((machine_id) => ({
      org_id: s.orgId, workspace_id: s.workspaceId, harness: s.harness, session_id: s.sessionId, machine_id, at: s.lastActivityAt,
    }))))],
  );
}

/** One tombstone in Neon is simply absent: its brief and events are deleted with the session. */
export async function loadNeonBatch(db: Db, sessions: CorpusSession[]): Promise<void> {
  const live = sessions.filter((s) => !s.deleted);
  await db.query(
    `INSERT INTO neon_baseline.convergence_events
       (org_id, workspace_id, machine_id, source, session_id, kind, event_id, ts, content, task_title, project_id)
     SELECT r.org_id, r.workspace_id, r.machine_id, r.source, r.session_id, 'session_event', r.event_id, r.ts, r.content, r.task_title, r.project_id
       FROM jsonb_to_recordset($1::jsonb) AS r(org_id text, workspace_id text, machine_id text, source text,
         session_id text, event_id text, ts timestamptz, content text, task_title text, project_id text)`,
    [JSON.stringify(live.flatMap((s) => s.events.map((e) => ({
      org_id: s.orgId, workspace_id: s.workspaceId, machine_id: s.machineIds[0], source: s.harness, session_id: s.sessionId,
      event_id: e.eventId, ts: e.ts, content: e.content, task_title: e.taskTitle, project_id: s.projectId,
    }))))],
  );
  await db.query(
    `INSERT INTO neon_baseline.session_brief_search
       (org_id, session_id, workspace_id, source, evidence_workspace_ids, title, description, brief_completed_at)
     SELECT r.org_id, r.session_id, r.workspace_id, r.source, jsonb_build_array(r.workspace_id), r.title, r.description, r.at
       FROM jsonb_to_recordset($1::jsonb) AS r(org_id text, session_id text, workspace_id text, source text, title text, description text, at timestamptz)`,
    [JSON.stringify(live.filter((s) => s.shortSummary).map((s) => ({
      org_id: s.orgId, session_id: s.sessionId, workspace_id: s.workspaceId, source: s.harness,
      title: s.title, description: s.shortSummary, at: s.lastActivityAt,
    })))],
  );
}

export interface Hit { harness: string; session_id: string; org_id?: string; workspace_id?: string }

/** The real RPC, as the Worker role, with the role's 2 s timeout (SET ROLE does not apply it). */
export async function directorySearch(db: Db, t: Tenant, term: string): Promise<Hit[]> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [claims(t)]);
  await db.exec("SET ROLE session_directory_search_worker; SET statement_timeout = '2s'");
  try {
    return (await db.query<Hit>("SELECT * FROM sessions.search_session_directory($1, $2)", [t.workspaceId, term])).rows;
  } finally {
    await db.exec("RESET statement_timeout; RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

/** The RPC's own SQL for EXPLAIN, run as owner: the Worker cannot read the table. */
export const directoryPlanSql = SESSION_DIRECTORY_SEARCH_SQL;
export const likeTerm = (term: string) => `%${term.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_")}%`;

/**
 * Hand port of briefKeywordCandidates + briefCandidateScope (session-search.ts,
 * relayhistory-cloud 7343404): the lexical brief pool, workspace-scoped, without RRF.
 * `%` and `_` in the term stay wildcards, as they do in Neon.
 */
export const NEON_BRIEF_SQL = `
SELECT b.session_id, b.source AS harness, b.brief_completed_at
  FROM neon_baseline.session_brief_search AS b
 WHERE (b.title ILIKE $3 OR b.description ILIKE $3)
   AND b.org_id = $1
   AND EXISTS (SELECT 1 FROM neon_baseline.convergence_events e
                WHERE e.org_id = $1 AND e.workspace_id = $2
                  AND e.session_id = b.session_id AND e.source = b.source)
   AND b.evidence_workspace_ids = jsonb_build_array($2::text)
   AND NOT EXISTS (SELECT 1 FROM neon_baseline.convergence_events f
                    WHERE f.org_id = $1 AND f.session_id = b.session_id AND f.workspace_id < $2)
   AND NOT EXISTS (SELECT 1 FROM neon_baseline.convergence_events f
                    WHERE f.org_id = $1 AND f.session_id = b.session_id AND f.workspace_id > $2)
   AND NOT EXISTS (SELECT 1 FROM neon_baseline.conversation_turns t
                    WHERE t.org_id = $1 AND t.session_id = b.session_id AND t.workspace_id IS DISTINCT FROM $2)
 ORDER BY b.brief_completed_at DESC, b.session_id DESC
 LIMIT 1000`;

/** Hand port of GET /v1/events?q=&order=desc: buildWhere's q clause, the MCP search path. */
export const NEON_EVENTS_SQL = `
SELECT e.session_id, e.source AS harness, e.org_id, e.workspace_id, e.ts
  FROM neon_baseline.convergence_events AS e
 WHERE e.org_id = $1 AND e.workspace_id = $2
   AND (left(e.content, 4096) ILIKE $3 OR e.task_title ILIKE $3)
 ORDER BY e.ts DESC, e.event_id DESC
 LIMIT 21`;

export const neonBriefSearch = async (db: Db, t: Tenant, term: string) =>
  (await db.query<Hit>(NEON_BRIEF_SQL, [t.orgId, t.workspaceId, `%${term}%`])).rows;
export const neonEventSearch = async (db: Db, t: Tenant, term: string) =>
  (await db.query<Hit>(NEON_EVENTS_SQL, [t.orgId, t.workspaceId, `%${term}%`])).rows;
