/**
 * Session-list brief titles (feature F2) for `GET /v1/sessions/titles`: the cached
 * model-written brief title of up to 100 sessions, from sessions.session_briefs.
 *
 * Calls sessions.session_brief_titles_for
 * (supabase/migrations/20261105043000_brief_titles.sql) as `bulls96_read_api`, inside the
 * read API's READ ONLY transaction with the tenant bound from the verified RelayAuth
 * assertion (liveness.ts TENANT_SETTINGS_SQL), exactly as the directory list does. The
 * function pins the tenant to that binding; every returned row is rechecked here too.
 * Not installed (the migration is not applied): null, and the route answers 501
 * `brief_titles_not_migrated`. Never Neon.
 *
 * This replaces Cloud's direct Neon read (packages/web/app/dashboard/sessions/
 * session-brief-titles.ts): the latest completed brief job's `result.title` per
 * (source, session) for the workspace. Like that read it never enqueues generation.
 */
import type { TenantRunner } from "./liveness.ts";

/** One requested session: `source` is the harness (the list's `source`). */
export interface BriefTitleKey {
  source: string;
  sessionId: string;
}

export interface BriefTitleRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  title: string;
  origin: string;
  generated_at: Date | string | null;
}

/** Null: the function is not installed (the migration has not been applied). */
export interface BriefTitles {
  titles(orgId: string, workspaceId: string, keys: readonly BriefTitleKey[]): Promise<BriefTitleRow[] | null>;
}

export class BriefTitlesError extends Error {
  constructor(readonly reason: "invalid_argument" | "refused" | "tenant_mismatch" | "parse") {
    super(`brief titles: ${reason}`);
    this.name = "BriefTitlesError";
  }
}

export const BRIEF_TITLES_FOR_SIGNATURE = "sessions.session_brief_titles_for(text, text[], text[])";
/** The function's bound, and the route's. */
export const BRIEF_TITLES_MAX_KEYS = 100;
/** The cloud-client sessionSummary title bound (the list's), and session_briefs' CHECK. */
export const BRIEF_TITLE_MAX_CHARS = 90;

const INSTALLED_SQL = "SELECT to_regprocedure($1) IS NOT NULL AS installed";
export const BRIEF_TITLES_SQL = `SELECT org_id, workspace_id, harness, session_id, title, origin, generated_at
  FROM sessions.session_brief_titles_for($1, $2::text[], $3::text[])`;

function mapPgError(error: unknown): never {
  const code = (error as { code?: string })?.code;
  if (code === "22023") throw new BriefTitlesError("invalid_argument");
  if (code === "42501") throw new BriefTitlesError("refused");
  throw error;
}

export function sqlBriefTitles(run: TenantRunner): BriefTitles {
  return {
    async titles(orgId, workspaceId, keys) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(INSTALLED_SQL, [BRIEF_TITLES_FOR_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const r = await q
          .query<BriefTitleRow>(BRIEF_TITLES_SQL, [workspaceId, keys.map((k) => k.source), keys.map((k) => k.sessionId)])
          .catch(mapPgError);
        return r.rows;
      });
    },
  };
}

/**
 * The response entries: rows rechecked against the tenant and the requested keys, and
 * bounded like the list's title. A key with no row has no title (a known null).
 */
export function briefTitleEntries(
  rows: readonly BriefTitleRow[],
  tenant: { orgId: string; workspaceId: string },
  keys: readonly BriefTitleKey[],
): Array<{ source: string; sessionId: string; title: string }> {
  const wanted = new Set(keys.map((k) => JSON.stringify([k.source, k.sessionId])));
  const seen = new Set<string>();
  const out: Array<{ source: string; sessionId: string; title: string }> = [];
  for (const row of rows) {
    if (row.org_id !== tenant.orgId || row.workspace_id !== tenant.workspaceId) throw new BriefTitlesError("tenant_mismatch");
    const key = JSON.stringify([row.harness, row.session_id]);
    if (!wanted.has(key) || seen.has(key)) throw new BriefTitlesError("parse");
    seen.add(key);
    if (typeof row.title !== "string" || row.title.length < 1 || row.title.length > BRIEF_TITLE_MAX_CHARS || /[\r\n]/u.test(row.title))
      throw new BriefTitlesError("parse");
    out.push({ source: row.harness, sessionId: row.session_id, title: row.title });
  }
  return out;
}
