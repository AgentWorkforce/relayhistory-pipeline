/**
 * The session LIST and activity from the Supabase logical-session directory, for
 * `GET /v1/sessions` and `GET /v1/sessions/activity`. Without `q`: list_session_directory_for /
 * session_directory_activity_for. With `q` (READ_API_SESSION_QUERY=true only; list-query.ts
 * parses it): query_session_directory_for / query_session_directory_activity_for
 * (supabase/migrations/20261105046010_session_directory_read_api_query.sql), whose free words
 * match the directory's search_text (title, task title, agent name, project id, git repo and
 * branch, model, scrubbed short summary; never transcript content). A q page's cursor is
 * `b96q1:` and carries the query's fingerprint.
 *
 * Calls sessions.list_session_directory_for / session_directory_activity_for
 * (supabase/migrations/20261105034000_session_directory_read_api.sql) as the
 * `bulls96_read_api` role, inside the read API's own READ ONLY transaction with the
 * tenant bound from the verified RelayAuth assertion (liveness.ts TENANT_SETTINGS_SQL).
 * The functions pin the tenant to that binding; every returned row is rechecked here
 * too. No Supabase JWT secret is involved, and there is no Neon fallback anywhere:
 * when the migration is not applied (the functions do not exist) the route answers
 * 501 `directory_not_migrated` and Cloud keeps its current path.
 *
 * The row -> SessionSummary mapping is relayhistory-cloud's reviewed one
 * (packages/relayhistory/src/lib/session-directory-list.ts sessionFromDirectoryRow,
 * the LIST_BACKEND=supabase path), minus `shortSummary`, so the response parses under
 * the strict @relayhistory/cloud-client 0.2.0 `sessionsResponse` Cloud validates with.
 * Activity uses the directory's SPAN semantics (a session counts on every UTC day from
 * its first to its last activity), as relayhistory's LIST_BACKEND=supabase does.
 */
import type { TenantRunner } from "./liveness.ts";
import type { SessionQueryTerms } from "./list-query.ts";

/** One list_session_directory_for row (its RETURNS TABLE). pg returns timestamptz as Date. */
export interface DirectoryListRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  created_at: Date | string;
  last_activity_at: Date | string;
  title: string | null;
  task_title: string | null;
  agent_names: string[] | null;
  project_id: string | null;
  project_key: string | null;
  project_key_method: string | null;
  git_repo: string | null;
  git_branch: string | null;
  initial_commit: string | null;
  originator: string | null;
  agent_version: string | null;
  models: string[] | null;
  state: string | null;
  event_count: number | string;
  input_tokens: number | string;
  output_tokens: number | string;
  cost_usd_micros: number | string | null;
  machine_ids: string[] | null;
  user_ids: string[] | null;
  parent_session_ids: string[] | null;
  child_session_count: number | string;
  short_summary: string | null;
  short_summary_generated_at: Date | string | null;
  catalog_projected_at: Date | string | null;
  projected_at: Date | string | null;
}

export interface DirectoryActivityRow {
  day: Date | string;
  harness: string;
  sessions: number | string;
}

/** One session_directory_users_for row (GET /v1/sessions/users). */
export interface DirectoryUserRow {
  user_id: string;
  sessions: number | string;
  last_session_at: Date | string;
  top_projects: unknown;
}

export interface DirectoryListArgs {
  sources?: string[];
  notSources?: string[];
  projectContains?: string;
  userIds?: string[];
  agent?: string;
  lastTsAfter?: Date;
  lastTsBefore?: Date;
  firstTsAfter?: Date;
  firstTsBefore?: Date;
  cursor?: DirectoryCursor;
  /** Exactly these (harness, session id) keys, 1..200 (the RPC's p_session_harnesses/p_session_ids). */
  sessionKeys?: Array<{ harness: string; sessionId: string }>;
  /** Page size, 1..200; the function returns up to limit + 1 rows. */
  limit: number;
}

export interface DirectoryCursor {
  /** ISO timestamp with up to six fractional digits (microseconds preserved). */
  lastActivityAt: string;
  sessionId: string;
  harness: string;
}

/** Null: the functions are not installed (the migration has not been applied). */
export interface Directory {
  list(orgId: string, workspaceId: string, args: DirectoryListArgs): Promise<DirectoryListRow[] | null>;
  activity(orgId: string, workspaceId: string, since: Date, until: Date): Promise<DirectoryActivityRow[] | null>;
  /** The list filtered by a parsed `q` (sessionKeys is not accepted here). */
  query(orgId: string, workspaceId: string, args: DirectoryQueryArgs): Promise<DirectoryListRow[] | null>;
  /** The SPAN activity of the sessions a parsed `q` and the list filters select. */
  queryActivity(orgId: string, workspaceId: string, since: Date, until: Date, args: DirectoryQueryActivityArgs): Promise<DirectoryActivityRow[] | null>;
  /**
   * Per-user session counts over [lastTsAfter, lastTsBefore) (the list's bounds), at most
   * limit + 1 rows ordered by sessions desc, user id. Absent: this backend cannot answer.
   */
  users?(orgId: string, workspaceId: string, lastTsAfter: Date, lastTsBefore: Date, limit: number): Promise<DirectoryUserRow[] | null>;
}

/** A q list read: the list filters plus the parsed terms (their dates already intersected into lastTs*). */
export type DirectoryQueryArgs = Omit<DirectoryListArgs, "sessionKeys"> & { terms: SessionQueryTerms };
export type DirectoryQueryActivityArgs = Omit<DirectoryQueryArgs, "cursor" | "limit">;

/** Postgres refused for a reason the caller can name (0A000, 22023), never quoting values. */
export class DirectoryError extends Error {
  constructor(readonly reason: "agent_filter_not_ready" | "invalid_argument" | "tenant_mismatch" | "parse" | "refused") {
    super(`session directory: ${reason}`);
    this.name = "DirectoryError";
  }
}

export const LIST_FOR_SIGNATURE =
  "sessions.list_session_directory_for(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)";
export const ACTIVITY_FOR_SIGNATURE = "sessions.session_directory_activity_for(text, timestamptz, timestamptz)";

/** The directory list page bound (the function's p_limit 1..200). */
export const DIRECTORY_LIST_MAX_LIMIT = 200;

// Timestamps travel as text so microseconds survive the cursor round trip.
export const DIRECTORY_LIST_SQL = `SELECT org_id, workspace_id, harness, session_id,
       created_at, last_activity_at,
       to_char(last_activity_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_ts,
       title, task_title, agent_names, project_id, project_key, project_key_method, git_repo, git_branch,
       initial_commit, originator, agent_version, models, state, event_count, input_tokens, output_tokens,
       cost_usd_micros, machine_ids, user_ids, parent_session_ids, child_session_count, short_summary,
       short_summary_generated_at, catalog_projected_at, projected_at
  FROM sessions.list_session_directory_for(
    p_workspace_id => $1, p_sources => $2, p_not_sources => $3, p_project_contains => $4,
    p_user_ids => $5, p_agent => $6, p_last_ts_after => $7, p_last_ts_before => $8,
    p_first_ts_after => $9, p_first_ts_before => $10,
    p_cursor_last_activity_at => $11, p_cursor_session_id => $12, p_cursor_harness => $13,
    p_limit => $14, p_session_harnesses => $15, p_session_ids => $16)`;

export const DIRECTORY_ACTIVITY_SQL = `SELECT to_char(day, 'YYYY-MM-DD') AS day, harness, sessions
  FROM sessions.session_directory_activity_for($1, $2, $3)`;

export const QUERY_FOR_SIGNATURE =
  "sessions.query_session_directory_for(text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, timestamptz, text, text, integer)";
export const QUERY_ACTIVITY_FOR_SIGNATURE =
  "sessions.query_session_directory_activity_for(text, timestamptz, timestamptz, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz)";

const QUERY_TERM_ARGS = `p_free => $2, p_not_free => $3, p_title => $4, p_not_title => $5,
    p_project => $6, p_not_project => $7, p_harness => $8, p_not_harness => $9,
    p_agent_terms => $10, p_not_agent_terms => $11, p_id => $12, p_not_id => $13, p_active => $14,
    p_sources => $15, p_not_sources => $16, p_project_contains => $17, p_user_ids => $18, p_agent => $19,
    p_last_ts_after => $20, p_last_ts_before => $21, p_first_ts_after => $22, p_first_ts_before => $23`;

export const DIRECTORY_QUERY_SQL = `SELECT org_id, workspace_id, harness, session_id,
       created_at, last_activity_at,
       to_char(last_activity_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_ts,
       title, task_title, agent_names, project_id, project_key, project_key_method, git_repo, git_branch,
       initial_commit, originator, agent_version, models, state, event_count, input_tokens, output_tokens,
       cost_usd_micros, machine_ids, user_ids, parent_session_ids, child_session_count, short_summary,
       short_summary_generated_at, catalog_projected_at, projected_at
  FROM sessions.query_session_directory_for(
    p_workspace_id => $1, ${QUERY_TERM_ARGS},
    p_cursor_last_activity_at => $24, p_cursor_session_id => $25, p_cursor_harness => $26, p_limit => $27)`;

export const DIRECTORY_QUERY_ACTIVITY_SQL = `SELECT to_char(day, 'YYYY-MM-DD') AS day, harness, sessions
  FROM sessions.query_session_directory_activity_for(
    p_workspace_id => $1, ${QUERY_TERM_ARGS},
    p_since => $24, p_until => $25)`;

export const USERS_FOR_SIGNATURE = "sessions.session_directory_users_for(text, timestamptz, timestamptz, integer)";
/** The most users GET /v1/sessions/users returns (the function's p_limit 1..1000). */
export const DIRECTORY_USERS_MAX = 1000;
export const DIRECTORY_USERS_SQL = `SELECT user_id, sessions, last_session_at, top_projects
  FROM sessions.session_directory_users_for($1, $2, $3, $4)`;

export const DIRECTORY_INSTALLED_SQL = `SELECT to_regprocedure($1) IS NOT NULL AS installed`;

function mapPgError(error: unknown): never {
  const code = (error as { code?: string })?.code;
  const message = (error as { message?: string })?.message ?? "";
  if (code === "0A000" && message.includes("agent_filter_not_ready")) throw new DirectoryError("agent_filter_not_ready");
  if (code === "22023") throw new DirectoryError("invalid_argument");
  if (code === "42501") throw new DirectoryError("refused");
  throw error;
}

const isoOrNull = (d: Date | undefined) => (d ? d.toISOString() : null);
const listOrNull = (v: string[] | undefined) => (v?.length ? v : null);

/** $2..$23 of DIRECTORY_QUERY_SQL / DIRECTORY_QUERY_ACTIVITY_SQL. */
function queryParams(a: DirectoryQueryActivityArgs): unknown[] {
  const t = a.terms;
  return [
    listOrNull(t.free), listOrNull(t.notFree), listOrNull(t.title), listOrNull(t.notTitle),
    listOrNull(t.project), listOrNull(t.notProject), listOrNull(t.harness), listOrNull(t.notHarness),
    listOrNull(t.agent), listOrNull(t.notAgent), listOrNull(t.id), listOrNull(t.notId), t.active ?? null,
    listOrNull(a.sources), listOrNull(a.notSources), a.projectContains ?? null, listOrNull(a.userIds), a.agent ?? null,
    isoOrNull(a.lastTsAfter), isoOrNull(a.lastTsBefore), isoOrNull(a.firstTsAfter), isoOrNull(a.firstTsBefore),
  ];
}

/** The directory over the read API's tenant runner: one READ ONLY transaction per call. */
export function sqlDirectory(run: TenantRunner): Directory {
  return {
    async query(orgId, workspaceId, a) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [QUERY_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const r = await q
          .query<DirectoryListRow>(DIRECTORY_QUERY_SQL, [
            workspaceId,
            ...queryParams(a),
            a.cursor?.lastActivityAt ?? null,
            a.cursor?.sessionId ?? null,
            a.cursor?.harness ?? null,
            a.limit,
          ])
          .catch(mapPgError);
        return r.rows;
      });
    },
    async queryActivity(orgId, workspaceId, since, until, a) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [QUERY_ACTIVITY_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const r = await q
          .query<DirectoryActivityRow>(DIRECTORY_QUERY_ACTIVITY_SQL, [workspaceId, ...queryParams(a), since.toISOString(), until.toISOString()])
          .catch(mapPgError);
        return r.rows;
      });
    },
    async list(orgId, workspaceId, a) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [LIST_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const iso = (d: Date | undefined) => (d ? d.toISOString() : null);
        const r = await q
          .query<DirectoryListRow>(DIRECTORY_LIST_SQL, [
            workspaceId,
            a.sources?.length ? a.sources : null,
            a.notSources?.length ? a.notSources : null,
            a.projectContains ?? null,
            a.userIds?.length ? a.userIds : null,
            a.agent ?? null,
            iso(a.lastTsAfter),
            iso(a.lastTsBefore),
            iso(a.firstTsAfter),
            iso(a.firstTsBefore),
            a.cursor?.lastActivityAt ?? null,
            a.cursor?.sessionId ?? null,
            a.cursor?.harness ?? null,
            a.limit,
            a.sessionKeys?.length ? a.sessionKeys.map((k) => k.harness) : null,
            a.sessionKeys?.length ? a.sessionKeys.map((k) => k.sessionId) : null,
          ])
          .catch(mapPgError);
        return r.rows;
      });
    },
    async users(orgId, workspaceId, lastTsAfter, lastTsBefore, limit) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [USERS_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const r = await q
          .query<DirectoryUserRow>(DIRECTORY_USERS_SQL, [workspaceId, lastTsAfter.toISOString(), lastTsBefore.toISOString(), limit])
          .catch(mapPgError);
        return r.rows;
      });
    },
    async activity(orgId, workspaceId, since, until) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(DIRECTORY_INSTALLED_SQL, [ACTIVITY_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const r = await q
          .query<DirectoryActivityRow>(DIRECTORY_ACTIVITY_SQL, [workspaceId, since.toISOString(), until.toISOString()])
          .catch(mapPgError);
        return r.rows;
      });
    },
  };
}

/**
 * List cursors, namespaced like the event cursors (cursor.ts `b96e1:`): a cursor from
 * this backend is never valid at relayhistory and a relayhistory `sessions-v2:` cursor
 * is refused here (`foreign_cursor`), so Cloud keeps a pagination on one backend.
 */
export const LIST_CURSOR_PREFIX = "b96s1:";
const CURSOR_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/u;
const HARNESS_NAME = /^[a-z0-9][a-z0-9._-]{0,63}$/u;

export function encodeListCursor(c: DirectoryCursor): string {
  return LIST_CURSOR_PREFIX + btoa(JSON.stringify([c.lastActivityAt, c.sessionId, c.harness])).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "");
}

/**
 * The owner-buffered list position (RELAYHISTORY_READ_OWNER_BUFFERED): the last buffered row
 * consumed, as (cursor_ts, sessionId, source). Rides as an optional 4th element of the same
 * `b96s1:` cursor, so nextCursor stays one opaque string.
 */
export interface BufferedListCursor {
  ts: string;
  sessionId: string;
  source: string;
}

/**
 * `[cts|null, sid|null, harness|null, [bts, bsid, bsource]]`; the plain 3-tuple when there is
 * no buffered position. A null canonical part means no canonical row was consumed yet.
 */
export function encodeCompositeListCursor(canonical: DirectoryCursor | undefined, buffered: BufferedListCursor | undefined): string {
  if (!buffered) {
    if (!canonical) throw new Error("list cursor needs a position");
    return encodeListCursor(canonical);
  }
  const v = [canonical?.lastActivityAt ?? null, canonical?.sessionId ?? null, canonical?.harness ?? null, [buffered.ts, buffered.sessionId, buffered.source]];
  return LIST_CURSOR_PREFIX + btoa(JSON.stringify(v)).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "");
}

const validCanonical = (v: unknown[]): boolean =>
  typeof v[0] === "string" && CURSOR_TS.test(v[0]) && Number.isFinite(Date.parse(v[0])) &&
  typeof v[1] === "string" && v[1].length >= 1 && v[1].length <= 255 &&
  typeof v[2] === "string" && HARNESS_NAME.test(v[2]);
const BUFFERED_CURSOR_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/u;
const validBuffered = (b: unknown): b is [string, string, string] =>
  Array.isArray(b) && b.length === 3 &&
  typeof b[0] === "string" && BUFFERED_CURSOR_TS.test(b[0]) && Number.isFinite(Date.parse(b[0])) &&
  typeof b[1] === "string" && b[1].length >= 1 && b[1].length <= 512 &&
  typeof b[2] === "string" && b[2].length >= 1 && b[2].length <= 255;

/**
 * Both parts of a list cursor (owner-buffered list only). undefined: no cursor; "foreign"/"invalid"
 * as decodeListCursor. A 4-element cursor's canonical part may be all-null (no canonical row
 * consumed yet).
 */
export function decodeCompositeListCursor(raw: string | undefined): { canonical?: DirectoryCursor; buffered?: BufferedListCursor } | undefined | "foreign" | "invalid" {
  if (raw === undefined || raw === "") return undefined;
  if (!raw.startsWith(LIST_CURSOR_PREFIX)) return "foreign";
  try {
    const b64 = raw.slice(LIST_CURSOR_PREFIX.length).replace(/-/gu, "+").replace(/_/gu, "/");
    const v = JSON.parse(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)));
    if (!Array.isArray(v)) return "invalid";
    if (v.length === 3) return validCanonical(v) ? { canonical: { lastActivityAt: v[0], sessionId: v[1], harness: v[2] } } : "invalid";
    if (v.length !== 4 || !validBuffered(v[3])) return "invalid";
    const buffered = { ts: v[3][0], sessionId: v[3][1], source: v[3][2] };
    if (v[0] === null && v[1] === null && v[2] === null) return { buffered };
    return validCanonical(v) ? { canonical: { lastActivityAt: v[0], sessionId: v[1], harness: v[2] }, buffered } : "invalid";
  } catch {
    return "invalid";
  }
}

/**
 * undefined: no cursor; "foreign": not ours; "invalid": ours but malformed. Exactly main's
 * canonical-only decoder: a composite (owner-buffered, 4-element) cursor is "invalid" here; only
 * the list route with owner-buffered reads on decodes it (decodeCompositeListCursor).
 */
export function decodeListCursor(raw: string | undefined): DirectoryCursor | undefined | "foreign" | "invalid" {
  if (raw === undefined || raw === "") return undefined;
  if (!raw.startsWith(LIST_CURSOR_PREFIX)) return "foreign";
  try {
    const b64 = raw.slice(LIST_CURSOR_PREFIX.length).replace(/-/gu, "+").replace(/_/gu, "/");
    const v = JSON.parse(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)));
    if (
      Array.isArray(v) && v.length === 3 &&
      typeof v[0] === "string" && CURSOR_TS.test(v[0]) && Number.isFinite(Date.parse(v[0])) &&
      typeof v[1] === "string" && v[1].length >= 1 && v[1].length <= 255 &&
      typeof v[2] === "string" && HARNESS_NAME.test(v[2])
    )
      return { lastActivityAt: v[0], sessionId: v[1], harness: v[2] };
  } catch {
    // fall through
  }
  return "invalid";
}

/**
 * q list cursors (READ_API_SESSION_QUERY): `b96q1:` + `[lastActivityAt, sessionId, harness,
 * fingerprint]`, the fingerprint being list-query.ts queryFingerprint of the query that made the
 * page. The route honours one only for the same query.
 */
export const QUERY_CURSOR_PREFIX = "b96q1:";
const FINGERPRINT = /^[0-9a-f]{32}$/u;
export interface DirectoryQueryCursor extends DirectoryCursor {
  fingerprint: string;
}

export function encodeQueryCursor(c: DirectoryQueryCursor): string {
  return QUERY_CURSOR_PREFIX + btoa(JSON.stringify([c.lastActivityAt, c.sessionId, c.harness, c.fingerprint])).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "");
}

/** undefined: no cursor; "foreign": not a `b96q1:` cursor; "invalid": ours but malformed. */
export function decodeQueryCursor(raw: string | undefined): DirectoryQueryCursor | undefined | "foreign" | "invalid" {
  if (raw === undefined || raw === "") return undefined;
  if (!raw.startsWith(QUERY_CURSOR_PREFIX)) return "foreign";
  try {
    const b64 = raw.slice(QUERY_CURSOR_PREFIX.length).replace(/-/gu, "+").replace(/_/gu, "/");
    const v = JSON.parse(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)));
    if (Array.isArray(v) && v.length === 4 && validCanonical(v) && typeof v[3] === "string" && FINGERPRINT.test(v[3]))
      return { lastActivityAt: v[0], sessionId: v[1], harness: v[2], fingerprint: v[3] };
  } catch {
    // fall through
  }
  return "invalid";
}

/** relayhistory lib/recall.ts MAX_SESSION_SUMMARY_CHARS and condenseTurn. */
export const MAX_SESSION_SUMMARY_CHARS = 240;
export function condenseTurn(content: string): string | null {
  const flat = content.replace(/\s+/gu, " ").trim();
  if (!flat) return null;
  if (flat.length <= MAX_SESSION_SUMMARY_CHARS) return flat;
  const cut = flat.slice(0, MAX_SESSION_SUMMARY_CHARS);
  const lastSpace = cut.lastIndexOf(" ");
  const body = lastSpace > MAX_SESSION_SUMMARY_CHARS * 0.6 ? cut.slice(0, lastSpace) : cut;
  return `${body.trimEnd()}…`;
}
/** relayhistory summaryFor(task_title, undefined): the directory has no opening turn. */
function summaryFor(taskTitle: string | null): { summary: string | null; summarySource: "task_title" | null } {
  if (typeof taskTitle === "string" && taskTitle.trim() !== "") return { summary: condenseTurn(taskTitle), summarySource: "task_title" };
  return { summary: null, summarySource: null };
}

const iso = (value: Date | string) => {
  const d = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(d.getTime())) throw new DirectoryError("parse");
  return d.toISOString();
};
const num = (value: unknown) => {
  if (typeof value !== "number" && !(typeof value === "string" && /^\d+$/u.test(value))) throw new DirectoryError("parse");
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0) throw new DirectoryError("parse");
  return n;
};
const strings = (value: unknown) =>
  Array.isArray(value) ? value.filter((item): item is string => typeof item === "string" && item !== "") : [];

export type WorkState = { status: "active" | "idle" | "finished"; updatedAt: string } | null;
function workState(row: DirectoryListRow): WorkState {
  const status = row.state === "active" ? "active" : row.state === "done" ? "finished" : row.state === "abandoned" ? "idle" : null;
  return status ? { status, updatedAt: iso(row.last_activity_at) } : null;
}

/** An explicitly negotiated self-host list extension; absent preserves cloud-client 0.2.0. */
export interface DirectoryListMetadata {
  includeAgentName?: "recorded";
}

/** Catalog names are a recorded set, not an ordering or a current/primary-agent claim. */
function recordedAgentName(row: DirectoryListRow): string | undefined {
  if (!row.catalog_projected_at || !Array.isArray(row.agent_names) || row.agent_names.length !== 1) return;
  const [name] = row.agent_names;
  if (typeof name !== "string" || name === "" || name !== name.trim() ||
      Array.from(name).length > 200 || /[\u0000-\u001f\u007f]/u.test(name)) return;
  return name;
}

/** One directory row in the cloud-client shape, with only explicitly negotiated metadata. */
export function sessionFromDirectoryRow(row: DirectoryListRow, metadata: DirectoryListMetadata = {}) {
  const models = strings(row.models);
  const catalog = row.catalog_projected_at
    ? {
        workspaceId: row.workspace_id,
        title: row.title,
        gitBranch: row.git_branch,
        repoUrl: row.git_repo,
        initialCommit: row.initial_commit,
        projectKey: row.project_key,
        projectKeyMethod: row.project_key_method,
        models,
        originator: row.originator,
        agentVersion: row.agent_version,
        firstActivityAt: iso(row.created_at),
        lastActivityAt: iso(row.last_activity_at),
        parentSessionIds: strings(row.parent_session_ids),
        childSessionCount: num(row.child_session_count),
      }
    : undefined;
  // sessionSummary bound (cloud-client 0.2.0 and later): title at most 90 characters.
  if (row.title !== null && row.title !== undefined && row.title.length > 90) throw new DirectoryError("parse");
  const agentName = metadata.includeAgentName === "recorded" ? recordedAgentName(row) : undefined;
  return {
    title: row.title ?? null,
    // shortSummary is deliberately not sent: Cloud validates with the strict
    // @relayhistory/cloud-client 0.2.0 sessionSummary, which has no such key. Add it
    // when Cloud moves to a contract version that has it.
    sessionId: row.session_id,
    userIds: strings(row.user_ids).sort(),
    source: row.harness,
    projectId: row.project_id ?? null,
    machineIds: strings(row.machine_ids),
    firstTs: iso(row.created_at),
    lastTs: iso(row.last_activity_at),
    workState: workState(row),
    eventCount: num(row.event_count),
    kinds: [] as string[],
    models,
    taskTitle: row.task_title ?? null,
    ...summaryFor(row.task_title ?? null),
    taskRefs: [] as unknown[],
    totalCostUsdMicros: row.cost_usd_micros == null ? null : num(row.cost_usd_micros),
    totalInputTokens: num(row.input_tokens),
    totalOutputTokens: num(row.output_tokens),
    totalReasoningTokens: 0,
    totalCacheReadTokens: 0,
    totalCacheCreateTokens: 0,
    totalCacheCreate5mTokens: null,
    totalCacheCreate1hTokens: null,
    ...(catalog ? { catalog } : {}),
    ...(agentName === undefined ? {} : { agentName }),
  };
}

export type DirectorySession = ReturnType<typeof sessionFromDirectoryRow>;

/** One page: rows rechecked against the tenant, limit + 1 trimmed into a cursor (a `b96q1:` one for a q page). */
export function directoryPage(rows: Array<DirectoryListRow & { cursor_ts?: string }>, tenant: { orgId: string; workspaceId: string }, limit: number, queryFingerprint?: string, metadata: DirectoryListMetadata = {}) {
  if (rows.length > limit + 1) throw new DirectoryError("parse");
  for (const row of rows) if (row.org_id !== tenant.orgId || row.workspace_id !== tenant.workspaceId) throw new DirectoryError("tenant_mismatch");
  const hasMore = rows.length > limit;
  const visible = hasMore ? rows.slice(0, limit) : rows;
  const last = visible[visible.length - 1];
  let nextCursor: string | null = null;
  if (hasMore && last) {
    const ts = last.cursor_ts;
    if (typeof ts !== "string" || !CURSOR_TS.test(ts)) throw new DirectoryError("parse");
    const position = { lastActivityAt: ts, sessionId: last.session_id, harness: last.harness };
    nextCursor = queryFingerprint === undefined ? encodeListCursor(position) : encodeQueryCursor({ ...position, fingerprint: queryFingerprint });
  }
  return { sessions: visible.map((row) => sessionFromDirectoryRow(row, metadata)), nextCursor };
}

/** UTC dates (YYYY-MM-DD) from since through until, inclusive (relayhistory activityDates). */
export function activityDates(since: Date, until: Date): string[] {
  const out: string[] = [];
  const day = Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate());
  const last = Date.UTC(until.getUTCFullYear(), until.getUTCMonth(), until.getUTCDate());
  for (let t = day; t <= last; t += 86_400_000) out.push(new Date(t).toISOString().slice(0, 10));
  return out;
}

export interface ActivityDay {
  date: string;
  total: number;
  bySource: Record<string, number>;
}

/** Every UTC day in the window, ascending and zero-filled (cloud-client sessionActivityResponse). */
export function activityDays(rows: DirectoryActivityRow[], since: Date, until: Date): ActivityDay[] {
  const byDate = new Map<string, ActivityDay>(activityDates(since, until).map((date) => [date, { date, total: 0, bySource: {} }]));
  for (const row of rows) {
    const key = row.day instanceof Date ? row.day.toISOString().slice(0, 10) : String(row.day).slice(0, 10);
    const entry = byDate.get(key);
    if (!entry || typeof row.harness !== "string") continue;
    const sessions = num(row.sessions);
    if (sessions === 0) continue;
    entry.bySource[row.harness] = sessions;
    entry.total += sessions;
  }
  return [...byDate.values()];
}

export interface SessionUserAggregate {
  userId: string;
  sessions: number;
  lastSessionAt: string;
  topProjects: Array<{ name: string; sessions: number }>;
}

const count = (v: unknown): number => {
  const n = typeof v === "string" && /^\d+$/u.test(v) ? Number(v) : v;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) throw new DirectoryError("parse");
  return n;
};

/**
 * session_directory_users_for rows -> GET /v1/sessions/users users (at most `limit`, with
 * `truncated` when the function returned limit + 1). Only the user id, counts, the latest
 * activity and project names leave; a row outside that shape is a parse error.
 */
export function sessionUsersPage(rows: DirectoryUserRow[], limit: number): { users: SessionUserAggregate[]; truncated: boolean } {
  if (rows.length > limit + 1) throw new DirectoryError("parse");
  const users = rows.slice(0, limit).map((row): SessionUserAggregate => {
    if (typeof row.user_id !== "string" || row.user_id === "") throw new DirectoryError("parse");
    const last = row.last_session_at instanceof Date ? row.last_session_at : new Date(String(row.last_session_at));
    if (Number.isNaN(last.getTime())) throw new DirectoryError("parse");
    let raw = row.top_projects;
    if (typeof raw === "string") {
      try { raw = JSON.parse(raw); } catch { throw new DirectoryError("parse"); }
    }
    if (!Array.isArray(raw) || raw.length > 3) throw new DirectoryError("parse");
    const topProjects = raw.map((p: unknown) => {
      const { name, sessions } = (p ?? {}) as { name?: unknown; sessions?: unknown };
      if (typeof name !== "string" || name === "") throw new DirectoryError("parse");
      return { name, sessions: count(sessions) };
    });
    return { userId: row.user_id, sessions: count(row.sessions), lastSessionAt: last.toISOString(), topProjects };
  });
  return { users, truncated: rows.length > limit };
}
