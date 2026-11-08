/**
 * Lane: owner-buffered-read (Option C PR A, owner-only read of buffered data; registry slot 043700).
 * supabase/migrations/20261105043700_owner_buffered_read.sql owns four functions:
 *   owner_buffered_read_scope        role guard + bulls96_read.user_id pin + fenced/head-shape
 *                                    refusal + R2 control-head v4 gate; not SECURITY DEFINER; owner-only
 *   owner_buffered_parity_expected   canonical G2 parity digests + receipt contiguity; not SECURITY
 *                                    DEFINER; owner-only; no write
 *   buffered_sessions_for            SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   buffered_session_stages_for      SECURITY DEFINER; EXECUTE to bulls96_read_api
 * The prefixes are the exact names plus "(", so they claim nothing else under sessions.buffered_* or
 * sessions.owner_buffered_*.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105043700_owner_buffered_read.sql";

export const ownerBufferedRead: LaneManifest = {
  lane: "owner-buffered-read",
  owner: "Option C PR A (feat/owner-buffered-read-rpcs-043700)",
  migration: MIGRATION,
  prefixes: [
    "sessions.owner_buffered_read_scope(",
    "sessions.owner_buffered_parity_expected(",
    "sessions.buffered_sessions_for(",
    "sessions.buffered_session_stages_for(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.owner_buffered_read_scope(text,text,text,text,text,boolean)": { secdef: false, grants: [] },
    "sessions.owner_buffered_parity_expected(text,text,text,text,bigint,text)": { secdef: false, grants: [] },
    "sessions.buffered_sessions_for(text,timestamp with time zone,text,text,integer,text,text,text,text,boolean)": {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
    "sessions.buffered_session_stages_for(text,text,text,text,text,text,text,boolean)": {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
  },
  replaces: {},
};
