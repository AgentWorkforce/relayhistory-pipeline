/**
 * Lane: staged-output-facts (Phase 3 large-session search, slice 2c; slot 062140).
 * supabase/migrations/20261105062140_staged_output_facts.sql owns eight functions:
 *   staged_facts_run_v1 / staged_facts_whole_v1 /
 *   staged_facts_reset_v1 (trigger)                             helpers; not SECURITY DEFINER; owner-only
 *   staged_facts_stage_v1 / _seal_v1 / _read_v1 / _status_v1 /
 *   _reconcile_v1                                              SECURITY DEFINER; EXECUTE to search_consumer_runtime
 * (Not under sessions.output_facts_: that prefix is the 045040 lane's.)
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062140_staged_output_facts.sql";

export const stagedOutputFacts: LaneManifest = {
  lane: "staged-output-facts",
  owner: "staged output facts, review/phase3-s2c-staged-facts (slot 062140)",
  migration: MIGRATION,
  prefixes: ["sessions.staged_facts_"],
  grantSources: [{ path: MIGRATION, roles: ["search_consumer_runtime"] }],
  functions: {
    "sessions.staged_facts_run_v1(bigint,uuid,text[])": { secdef: false, grants: [] },
    "sessions.staged_facts_whole_v1(bigint)": { secdef: false, grants: [] },
    "sessions.staged_facts_reset_v1()": { secdef: false, grants: [] },
    "sessions.staged_facts_stage_v1(bigint,uuid,jsonb,integer,boolean)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.staged_facts_seal_v1(bigint,uuid)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.staged_facts_read_v1(bigint,uuid,text,text,text[],bigint,integer)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.staged_facts_status_v1(bigint,uuid)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.staged_facts_reconcile_v1(bigint,uuid,text,text,text,text[],text,boolean,bigint)": { secdef: true, grants: ["search_consumer_runtime"] },
  },
  replaces: {},
};
