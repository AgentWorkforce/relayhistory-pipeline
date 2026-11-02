/**
 * Directory search for the session read API (GET /v1/directory/search): a keyword
 * match over `sessions.session_directory` (title, task_title, git_repo,
 * search_text) in ONE workspace, answered with pointers only:
 *
 *   session_id, workspace_id, title, git_repo, last_activity_at, state
 *
 * No transcript, no summary, no search document is ever returned. It runs as the
 * `bulls96_read_api` role, which may SELECT only the eleven directory columns
 * this statement names (supabase/bootstrap/session_read_api_directory.sql).
 *
 * Visibility, all enforced in the one statement below:
 *   - tenant: org and workspace come only from the verified token, bound both as
 *     statement parameters and as the transaction's tenant settings, which the
 *     role's RLS policies on session_directory AND session_index match (so a row
 *     of another tenant is invisible whatever the statement says);
 *   - tombstones: `deleted_at IS NULL` on the directory row;
 *   - erasure: the session must have at least one live pointer in session_index.
 *     The projector derives every directory row from live pointers and tombstones
 *     it when the last one goes, so this only closes the window between a pointer
 *     tombstone or erasure and the projector running. It is stricter than the event
 *     routes' rule (which treat "no pointer at all" as live because ClickHouse
 *     history can predate pointers): a directory row without a live pointer is
 *     never served.
 *   - sharing mode: not modelled server-side. Desktop sharing modes (all / new /
 *     selected) decide what is uploaded; everything the directory holds was shared
 *     with the workspace, which is the visibility the `authenticated` member policy
 *     already gives every workspace member. See docs/session-read-api-design.md.
 *
 * Paging is a keyset over (last_activity_at, session_id, harness) DESC, the
 * workspace recency index's order, with an opaque `b96d1:` cursor.
 */
import type { TenantRunner } from "./liveness.ts";
import { CursorError } from "./cursor.ts";

export const DIRECTORY_CURSOR_PREFIX = "b96d1:";
export const DIRECTORY_DEFAULT_LIMIT = 20;
export const DIRECTORY_MAX_LIMIT = 50;
/** The same 3-200 character bound as sessions.search_session_directory. */
export const DIRECTORY_MIN_Q_CHARS = 3;
export const DIRECTORY_MAX_Q_CHARS = 200;

/** The pointer a directory hit returns, and nothing else. */
export interface DirectoryPointer {
  session_id: string;
  workspace_id: string;
  title: string | null;
  git_repo: string | null;
  last_activity_at: string;
  state: string;
}

export interface DirectoryPosition {
  /** Microsecond ISO UTC timestamp, exactly as the database rendered it. */
  lastActivityAt: string;
  sessionId: string;
  harness: string;
}

export interface DirectoryRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  title: string | null;
  git_repo: string | null;
  last_activity_at: string;
  state: string;
}

export interface DirectorySearchInput {
  orgId: string;
  workspaceId: string;
  /** Normalized query (normalizeDirectoryQuery). */
  q: string;
  /** Rows to fetch: the page size plus one. */
  fetch: number;
  cursor?: DirectoryPosition;
}

export interface DirectorySearch {
  search(input: DirectorySearchInput): Promise<DirectoryRow[]>;
}

const ISO_MICROS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;
const HARNESS = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export function encodeDirectoryCursor(p: DirectoryPosition): string {
  return DIRECTORY_CURSOR_PREFIX + Buffer.from(JSON.stringify([p.lastActivityAt, p.sessionId, p.harness]), "utf8").toString("base64url");
}

/** `undefined` for no cursor; CursorError("invalid_cursor") for anything else that is not ours. */
export function decodeDirectoryCursor(raw: string | undefined): DirectoryPosition | undefined {
  if (raw === undefined || raw === "") return undefined;
  if (!raw.startsWith(DIRECTORY_CURSOR_PREFIX) || raw.length > 1024) throw new CursorError("invalid_cursor");
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(raw.slice(DIRECTORY_CURSOR_PREFIX.length), "base64url").toString("utf8"));
  } catch {
    throw new CursorError("invalid_cursor");
  }
  if (!Array.isArray(value) || value.length !== 3) throw new CursorError("invalid_cursor");
  const [lastActivityAt, sessionId, harness] = value as unknown[];
  if (typeof lastActivityAt !== "string" || !ISO_MICROS.test(lastActivityAt) || Number.isNaN(Date.parse(lastActivityAt)))
    throw new CursorError("invalid_cursor");
  if (typeof sessionId !== "string" || sessionId.length < 1 || sessionId.length > 255) throw new CursorError("invalid_cursor");
  if (typeof harness !== "string" || !HARNESS.test(harness)) throw new CursorError("invalid_cursor");
  return { lastActivityAt, sessionId, harness };
}

/** Trim and collapse whitespace; `undefined` when outside 3-200 characters. */
export function normalizeDirectoryQuery(raw: string | undefined): string | undefined {
  if (raw === undefined) return undefined;
  const q = raw.normalize("NFC").replace(/[\u0000-\u001f\u007f]+/g, " ").replace(/\s+/g, " ").trim();
  const n = [...q].length;
  return n >= DIRECTORY_MIN_Q_CHARS && n <= DIRECTORY_MAX_Q_CHARS ? q : undefined;
}

/** A literal substring pattern for ILIKE ... ESCAPE '\'. */
export function likePattern(q: string): string {
  return `%${q.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_")}%`;
}

export const DIRECTORY_SEARCH_SQL = `SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.title, d.git_repo, d.state,
       to_char(d.last_activity_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS last_activity_at
  FROM sessions.session_directory AS d
 WHERE d.org_id = $1 AND d.workspace_id = $2
   AND d.deleted_at IS NULL
   AND (d.title ILIKE $3 ESCAPE '\\' OR d.task_title ILIKE $3 ESCAPE '\\' OR d.git_repo ILIKE $3 ESCAPE '\\'
        OR (d.search_text <> '' AND d.search_text ILIKE $3 ESCAPE '\\'))
   AND EXISTS (SELECT 1 FROM sessions.session_index AS p
                WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
                  AND p.harness = d.harness AND p.session_id = d.session_id
                  AND p.deleted_at IS NULL)
   AND ($4::timestamptz IS NULL OR (d.last_activity_at, d.session_id, d.harness) < ($4::timestamptz, $5::text, $6::text))
 ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC
 LIMIT $7`;

export function sqlDirectorySearch(run: TenantRunner): DirectorySearch {
  return {
    async search(input) {
      const { rows } = await run(input.orgId, input.workspaceId, (q) =>
        q.query<DirectoryRow>(DIRECTORY_SEARCH_SQL, [
          input.orgId,
          input.workspaceId,
          likePattern(input.q),
          input.cursor?.lastActivityAt ?? null,
          input.cursor?.sessionId ?? null,
          input.cursor?.harness ?? null,
          input.fetch,
        ]),
      );
      return rows;
    },
  };
}

/** Exactly the pointer fields, so no other column can ever reach a response. */
export function toPointer(r: DirectoryRow): DirectoryPointer {
  return {
    session_id: r.session_id,
    workspace_id: r.workspace_id,
    title: r.title ?? null,
    git_repo: r.git_repo ?? null,
    last_activity_at: r.last_activity_at,
    state: r.state,
  };
}

/**
 * READ_API_DIRECTORY_WORKSPACES: a comma-separated allowlist of workspace ids that
 * may use directory search. Unset or empty: the route is closed for everyone. Any
 * malformed entry closes it too (`invalid`), rather than guessing.
 */
export function parseDirectoryWorkspaces(raw: string | undefined): { workspaces: ReadonlySet<string>; invalid: boolean } {
  if (raw === undefined || raw.trim() === "") return { workspaces: new Set(), invalid: false };
  const ids = raw.split(",").map((s) => s.trim());
  if (ids.some((id) => !/^[^\s,\p{Cc}]{1,128}$/u.test(id))) return { workspaces: new Set(), invalid: true };
  return { workspaces: new Set(ids), invalid: false };
}
