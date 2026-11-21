/**
 * Lane: session-directory-list-readiness (slot 062200).
 * supabase/migrations/20261105062200_session_directory_list_readiness.sql owns two functions, each beside the list
 * function it extends (034000's list_session_directory_for, 046010's query_session_directory_for), returning that
 * function's columns plus events_ready / event_projected_at and taking p_ready_only:
 *   list_session_directory_readiness_for    SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   query_session_directory_readiness_for   SECURITY DEFINER; EXECUTE to bulls96_read_api
 * The prefixes are the exact names plus "(", so they claim nothing else; the read-api and read-api-query lanes'
 * prefixes ("sessions.list_session_directory_for(", "sessions.query_session_directory_for(") do not claim them.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062200_session_directory_list_readiness.sql";
const T = "timestamp with time zone";
const A12 = Array(12).fill("text[]").join(",");

export const sessionDirectoryListReadiness: LaneManifest = {
  lane: "session-directory-list-readiness",
  owner: "session list readiness (slot 062200)",
  migration: MIGRATION,
  prefixes: ["sessions.list_session_directory_readiness_for(", "sessions.query_session_directory_readiness_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    [`sessions.list_session_directory_readiness_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer,boolean)`]: {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
    [`sessions.query_session_directory_readiness_for(text,${A12},boolean,text[],text[],text,text[],text,${T},${T},${T},${T},${T},text,text,integer,boolean)`]: {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
  },
  replaces: {},
};
