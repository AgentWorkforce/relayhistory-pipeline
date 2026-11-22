/**
 * GET /v1/sessions/summaries/search over the Postgres session directory, behind
 * READ_API_SUMMARY_SEARCH_ENABLED=true and READ_API_SUMMARY_SEARCH_BACKEND=postgres (self-host
 * service only; lambda.ts summarySearchBackend). No migration: it reads through the existing
 * directory query (Directory.query, i.e. sessions.query_session_directory_for, migration
 * 20261105046010) as the read role, inside one READ ONLY tenant transaction per page.
 *
 * - `q` is normalised with normalizeSearchText (the directory search normalisation) and sent as ONE
 *   phrase free term: `search_text ILIKE '%q%'` with LIKE wildcards escaped, over the tenant's live
 *   rows only (`deleted_at IS NULL`; an erased or tombstoned session has blank search_text too).
 * - search_text also holds task title, agent, project, repo, branch and model, and can be stale, so
 *   the SQL rows are a superset: every row is re-checked with summaryMatches (the ClickHouse path's
 *   authoritative re-check, the same fold and the same two columns) and only title / short summary
 *   matches are served. The re-check can only drop rows, never add them.
 * - Keyset pages of DIRECTORY_LIST_MAX_LIMIT rows, newest activity first, at most
 *   SUMMARY_SEARCH_PG_MAX_PAGES pages, stopping once limit + 1 matches are found, and no further
 *   page is started once SUMMARY_SEARCH_PG_BUDGET_MS have passed since the first query started.
 *   Best-effort like the ClickHouse path: past either budget older matches are missed and
 *   `truncated` is true, possibly with fewer than `limit` (even zero) sessions. The page bound is
 *   on rows RETURNED per page (the database may read more); each statement is also bounded by the
 *   read pool's statement timeout.
 * - Every returned row, including a page's extra (limit + 1) row, is checked against the verified
 *   tenant. A (harness, session_id) seen on an earlier page is not counted or served again (each
 *   page is its own transaction, so a row updated between pages can be returned again).
 * No cursor, no rank: the served page is ordered as the ClickHouse path orders it
 * (last activity DESC, source DESC, session_id DESC).
 */
import { normalizeSearchText, SEARCH_QUERY_MAX_CHARS, SEARCH_QUERY_MIN_CHARS } from "../../hoopdb/search-query.ts";
import { summaryMatches } from "../clickhouse/summary-search-query.ts";
import { DIRECTORY_LIST_MAX_LIMIT, DirectoryError, type Directory, type DirectoryCursor, type DirectoryListRow } from "../directory.ts";
import type { SessionQueryTerms } from "../list-query.ts";

/** Pages scanned per request: at most SUMMARY_SEARCH_PG_MAX_PAGES * DIRECTORY_LIST_MAX_LIMIT rows. */
export const SUMMARY_SEARCH_PG_MAX_PAGES = 3;
/** No further directory page starts once this many milliseconds have passed since the first query started. */
export const SUMMARY_SEARCH_PG_BUDGET_MS = 2000;
/** The directory term bounds (046010): at least 3 meaningful characters, at most 200 code points. */
export const SUMMARY_SEARCH_PG_MIN_CHARS = SEARCH_QUERY_MIN_CHARS;
export const SUMMARY_SEARCH_PG_MAX_CHARS = SEARCH_QUERY_MAX_CHARS;

const CURSOR_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/u;

export type SummarySearchPgRow = DirectoryListRow & { cursor_ts: string };

/** The normalised phrase, or why it is refused (a 400 at the route). */
export function normalizeSummarySearchQ(q: string): { ok: true; text: string } | { ok: false; message: string } {
  const n = normalizeSearchText(q);
  if (n.meaningfulChars < SUMMARY_SEARCH_PG_MIN_CHARS) return { ok: false, message: `q must have at least ${SUMMARY_SEARCH_PG_MIN_CHARS} visible characters` };
  if (n.codePoints > SUMMARY_SEARCH_PG_MAX_CHARS) return { ok: false, message: `q must be ${SUMMARY_SEARCH_PG_MAX_CHARS} characters or fewer` };
  return { ok: true, text: n.text };
}

function phraseTerms(text: string): SessionQueryTerms {
  return {
    free: [text], notFree: [], title: [], notTitle: [], project: [], notProject: [],
    harness: [], notHarness: [], agent: [], notAgent: [], id: [], notId: [],
  };
}

/** The ClickHouse path's order: last activity DESC, source DESC, session_id DESC (cursor_ts is fixed-width UTC text). */
export function compareSummaryRows(a: SummarySearchPgRow, b: SummarySearchPgRow): number {
  if (a.cursor_ts !== b.cursor_ts) return a.cursor_ts < b.cursor_ts ? 1 : -1;
  if (a.harness !== b.harness) return a.harness < b.harness ? 1 : -1;
  if (a.session_id !== b.session_id) return a.session_id < b.session_id ? 1 : -1;
  return 0;
}

/**
 * At most `limit` matching rows in the ClickHouse order, the rows scanned, and whether more may
 * exist. Null: the directory query function is not installed. `now` is injectable for tests.
 */
export async function searchSummariesPostgres(
  directory: Directory,
  tenant: { orgId: string; workspaceId: string },
  phrase: string,
  limit: number,
  now: () => number = Date.now,
): Promise<{ rows: SummarySearchPgRow[]; scanned: number; truncated: boolean } | null> {
  const terms = phraseTerms(phrase);
  const matches: SummarySearchPgRow[] = [];
  let scanned = 0;
  let cursor: DirectoryCursor | undefined;
  let more = false;
  const seen = new Set<string>();
  const started = now();
  for (let page = 0; page < SUMMARY_SEARCH_PG_MAX_PAGES; page++) {
    // Time budget: `more` is still true here, so the answer is truncated.
    if (page > 0 && now() - started >= SUMMARY_SEARCH_PG_BUDGET_MS) break;
    const rows = await directory.query(tenant.orgId, tenant.workspaceId, { terms, limit: DIRECTORY_LIST_MAX_LIMIT, ...(cursor ? { cursor } : {}) });
    if (rows === null) return null;
    if (rows.length > DIRECTORY_LIST_MAX_LIMIT + 1) throw new DirectoryError("parse");
    for (const row of rows) if (row.org_id !== tenant.orgId || row.workspace_id !== tenant.workspaceId) throw new DirectoryError("tenant_mismatch");
    more = rows.length > DIRECTORY_LIST_MAX_LIMIT;
    const visible = (more ? rows.slice(0, DIRECTORY_LIST_MAX_LIMIT) : rows) as Array<DirectoryListRow & { cursor_ts?: unknown }>;
    for (const row of visible) {
      if (typeof row.cursor_ts !== "string" || !CURSOR_TS.test(row.cursor_ts)) throw new DirectoryError("parse");
      const key = `${row.harness}\u0000${row.session_id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      scanned++;
      if (summaryMatches(row, phrase)) matches.push(row as SummarySearchPgRow);
      if (matches.length > limit) break;
    }
    if (matches.length > limit || !more) break;
    const last = visible[visible.length - 1] as SummarySearchPgRow;
    cursor = { lastActivityAt: last.cursor_ts, sessionId: last.session_id, harness: last.harness };
  }
  const truncated = matches.length > limit || more;
  return { rows: matches.slice(0, limit).sort(compareSummaryRows), scanned, truncated };
}
