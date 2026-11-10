/**
 * Lane: coordination-fact-readers (slot 047000).
 * Two sparse SECURITY DEFINER readers for the coordination overlaps endpoint; both retain
 * 045040's tenant scope and grant EXECUTE only to bulls96_read_api.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105047000_coordination_fact_readers.sql";
const SIG = "(text,timestamp with time zone,timestamp with time zone,text,integer)";

export const coordinationFactReaders: LaneManifest = {
  lane: "coordination-fact-readers",
  owner: "coordination overlaps filtered readers (slot 047000)",
  migration: MIGRATION,
  prefixes: ["sessions.coordination_write_facts_for(", "sessions.coordination_output_facts_for("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    [`sessions.coordination_write_facts_for${SIG}`]: { secdef: true, grants: ["bulls96_read_api"] },
    [`sessions.coordination_output_facts_for${SIG}`]: { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
