/**
 * Lane: read-api (P3b read API directory functions, #236).
 * supabase/migrations/20261105034000_session_directory_read_api.sql owns three functions:
 *   session_directory_read_api_scope   tenant pin; not SECURITY DEFINER; owner-only EXECUTE
 *   list_session_directory_for         SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   session_directory_activity_for     SECURITY DEFINER; EXECUTE to bulls96_read_api
 * The prefixes are the exact signatures' names plus "(", so they claim nothing else under
 * sessions.session_directory_* or sessions.list_session_directory*; the list function's prefix
 * is its full 16-argument signature, leaving the 050020 overload to read-api-agent-match.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105034000_session_directory_read_api.sql";
const T = "timestamp with time zone";

export const readApi: LaneManifest = {
  lane: "read-api",
  owner: "#236 (feat/p3b-read-api-directory-for)",
  migration: MIGRATION,
  prefixes: [
    // The exact 16-argument signature: 20261105050020's 17-argument overload belongs to the
    // read-api-agent-match lane.
    `sessions.list_session_directory_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer)`,
    "sessions.session_directory_activity_for(",
    "sessions.session_directory_read_api_scope(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_directory_read_api_scope(text)": { secdef: false, grants: [] },
    [`sessions.list_session_directory_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer)`]: {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
    [`sessions.session_directory_activity_for(text,${T},${T})`]: { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
