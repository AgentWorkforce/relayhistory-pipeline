/**
 * Lane: brief-titles (F2, session-list brief titles off Neon; registry slot 043000).
 * supabase/migrations/20261105043000_brief_titles.sql owns two functions:
 *   session_briefs_provenance   BEFORE trigger on session_briefs; not SECURITY DEFINER; owner-only
 *   session_brief               REPLACED (026000's body plus the backfill containment); ACL kept
 *   session_brief_titles_for    SECURITY DEFINER; EXECUTE to bulls96_read_api (tenant pinned by
 *                               sessions.session_directory_read_api_scope, lane read-api)
 * The prefixes are the exact names plus "(", so they claim nothing else under
 * sessions.session_brief* (brief-documents owns sessions.session_brief_document_*).
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105043000_brief_titles.sql";

export const briefTitles: LaneManifest = {
  lane: "brief-titles",
  owner: "F2 (feat/f2-brief-titles-off-neon)",
  migration: MIGRATION,
  prefixes: ["sessions.session_briefs_provenance(", "sessions.session_brief_titles_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_briefs_provenance()": { secdef: false, grants: [] },
    "sessions.session_brief_titles_for(text,text[],text[])": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {
    // Contained so a backfilled brief reads as no brief on relayhistory-cloud's /brief route.
    "sessions.session_brief(text,text,text)": { secdef: true, original: "supabase/migrations/20261105026000_canonical_history_lineage.sql" },
  },
};
