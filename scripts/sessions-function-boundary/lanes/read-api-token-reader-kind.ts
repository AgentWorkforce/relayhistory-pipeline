/**
 * Lane: read-api-token-reader-kind (slot 062020, READ_API_MINT_READER_KIND, default off).
 * supabase/migrations/20261105062020_read_api_token_reader_kind.sql adds sessions.read_api_tokens.reader_kind
 * and read_api_token_issue_v2: the 044000 issue function plus p_reader_kind, SECURITY DEFINER with the same
 * inline guard for the genuine bulls96_read_api login. The prefix is the exact name plus "(".
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062020_read_api_token_reader_kind.sql";
const T = "timestamp with time zone";

export const readApiTokenReaderKind: LaneManifest = {
  lane: "read-api-token-reader-kind",
  owner: "feat/read-api-mint-reader-kind (slot 062020)",
  migration: MIGRATION,
  prefixes: ["sessions.read_api_token_issue_v2("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    [`sessions.read_api_token_issue_v2(text,text,text,text,text,text,text,text,${T},text,${T},text,${T},text)`]: { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
