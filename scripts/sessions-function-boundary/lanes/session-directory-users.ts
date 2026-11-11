/**
 * Lane: session-directory-users (slot 057000).
 * One SECURITY DEFINER per-user session aggregate over the directory for GET /v1/sessions/users;
 * the directory list's read-role tenant scope, EXECUTE to bulls96_read_api only.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105057000_session_directory_users_aggregate.sql";

export const sessionDirectoryUsers: LaneManifest = {
  lane: "session-directory-users",
  owner: "per-user session aggregate read API (slot 057000)",
  migration: MIGRATION,
  prefixes: ["sessions.session_directory_users_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_directory_users_for(text,timestamp with time zone,timestamp with time zone,integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
