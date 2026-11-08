/**
 * Lane: read-api-tokens (READ_API_AUTH_MODE=self_issued, default off).
 * supabase/migrations/20261105044000_read_api_tokens.sql owns the token table's functions. All are
 * SECURITY DEFINER with an inline guard for the genuine bulls96_read_api login. The sweep is
 * owner-only and runs from the issue and refresh functions.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105044000_read_api_tokens.sql";

export const readApiTokens: LaneManifest = {
  lane: "read-api-tokens",
  owner: "feat/read-api-self-auth",
  migration: MIGRATION,
  prefixes: ["sessions.read_api_token_"],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.read_api_token_verify(text,timestamp with time zone)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.read_api_token_sweep_expired(timestamp with time zone)": { secdef: true, grants: [] },
    "sessions.read_api_token_issue(text,text,text,text,text,text,text,text,timestamp with time zone,text,timestamp with time zone,text,timestamp with time zone)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.read_api_token_refresh(text,text,text,text,text,text,timestamp with time zone,text,timestamp with time zone,timestamp with time zone)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.read_api_token_delete_by_hash(text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.read_api_token_delete_for(text,text,text,text)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
