/**
 * Lane: read-api-query (self-host Dashboard session list/activity `q`, #473; slot 046010).
 * supabase/migrations/20261105046010_session_directory_read_api_query.sql owns five functions:
 *   session_directory_query_check / _like / _where   helpers; not SECURITY DEFINER; owner-only EXECUTE
 *   query_session_directory_for                      SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   query_session_directory_activity_for             SECURITY DEFINER; EXECUTE to bulls96_read_api
 * The two query prefixes are the exact names plus "(", so they claim nothing else; the helper
 * prefix sessions.session_directory_query_ is claimed by no other lane.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105046010_session_directory_read_api_query.sql";
const T = "timestamp with time zone";
const A12 = Array(12).fill("text[]").join(",");
const FILTERS = `${A12},boolean,text[],text[],text,text[],text,${T},${T},${T},${T}`;

export const readApiQuery: LaneManifest = {
  lane: "read-api-query",
  owner: "#473 (feat/selfhost-session-list-q-046010)",
  migration: MIGRATION,
  prefixes: [
    "sessions.query_session_directory_for(",
    "sessions.query_session_directory_activity_for(",
    "sessions.session_directory_query_",
  ],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    [`sessions.session_directory_query_check(${A12},text[],text[],text,text[],text,${T},${T},${T},${T})`]: { secdef: false, grants: [] },
    "sessions.session_directory_query_like(text[])": { secdef: false, grants: [] },
    [`sessions.session_directory_query_where(${Array(12).fill("integer").join(",")},${Array(10).fill("boolean").join(",")})`]: {
      secdef: false,
      grants: [],
    },
    [`sessions.query_session_directory_for(text,${FILTERS},${T},text,text,integer)`]: { secdef: true, grants: ["bulls96_read_api"] },
    [`sessions.query_session_directory_activity_for(text,${T},${T},${FILTERS})`]: { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
