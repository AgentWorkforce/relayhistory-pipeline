/**
 * Lane: portable-reader (self-host portable search reader contract, #300).
 * supabase/migrations/20261105048000_portable_search_reader_contract.sql owns one function:
 *   portable_reader_readiness   SECURITY DEFINER; EXECUTE to bulls96_read_api only. Returns a
 *                               bounded readiness code (ledger membership + marker) to the
 *                               fixed reader, which may not SELECT the ledger or the marker.
 * The prefix is the exact name plus "(", so it claims nothing else under sessions.portable_*.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105048000_portable_search_reader_contract.sql";

export const portableReader: LaneManifest = {
  lane: "portable-reader",
  owner: "#300 (feat/selfhost-reader-contract-048-20261107)",
  migration: MIGRATION,
  prefixes: ["sessions.portable_reader_readiness("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.portable_reader_readiness(text[],text)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
