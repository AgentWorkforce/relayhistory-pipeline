/**
 * Lane: session-long-summary (slot 060000).
 * One SECURITY DEFINER reader of the scrubbed, bounded long summary for
 * GET /v1/sessions/:sessionId/summary?kind=long (read-role tenant scope, EXECUTE to bulls96_read_api only),
 * plus the invoker trigger function that clears the long summary whenever the row loses its short summary
 * or is tombstoned (no grants).
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105060000_session_long_summary.sql";

export const sessionLongSummary: LaneManifest = {
  lane: "session-long-summary",
  owner: "scrubbed long summary projection and read API (slot 060000)",
  migration: MIGRATION,
  prefixes: ["sessions.session_long_summary_for(", "sessions.session_directory_long_summary_follows_short("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_long_summary_for(text,text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.session_directory_long_summary_follows_short()": { secdef: false, grants: [] },
  },
  replaces: {},
};
