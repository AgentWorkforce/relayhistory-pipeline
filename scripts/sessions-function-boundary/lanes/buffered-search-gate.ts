/**
 * Lane: buffered-search-gate (registry slot 043860).
 * supabase/migrations/20261105043860_buffered_search_gate.sql owns one function:
 *   buffered_search_gate_for   SECURITY DEFINER; role guard (bulls96_read_api) + org/workspace pins;
 *                              per-owner ledger verdicts for buffered search hits; EXECUTE to
 *                              bulls96_read_api only
 * The prefix is the exact name plus "(", so it claims nothing else under sessions.buffered_*.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105043860_buffered_search_gate.sql";

export const bufferedSearchGate: LaneManifest = {
  lane: "buffered-search-gate",
  owner: "PR #338 (feat/read-api-buffered-search-gate)",
  migration: MIGRATION,
  prefixes: ["sessions.buffered_search_gate_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
