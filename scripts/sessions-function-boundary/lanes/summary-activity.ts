/**
 * Lane: summary-activity (slot 056000).
 * One SECURITY DEFINER reader of ready native summary counts per UTC day for
 * GET /v1/sessions/summaries/activity; read-role tenant scope, EXECUTE to bulls96_read_api only.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105056000_session_summary_activity.sql";

export const summaryActivity: LaneManifest = {
  lane: "summary-activity",
  owner: "native summary daily counts read API (slot 056000)",
  migration: MIGRATION,
  prefixes: ["sessions.session_summary_activity_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_summary_activity_for(text,timestamp with time zone,timestamp with time zone)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
