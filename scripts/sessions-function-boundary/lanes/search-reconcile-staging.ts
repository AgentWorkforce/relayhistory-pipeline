/**
 * Lane: search-reconcile-staging (Phase 3 large-session search, slice 2a; slot 062110).
 * supabase/migrations/20261105062110_search_reconcile_staging.sql owns nine functions:
 *   search_reconcile_caller_v1 / search_reconcile_lease_v1     helpers; not SECURITY DEFINER; owner-only
 *   search_reconcile_claim_v1 / _stage_batch_v1 / _advance_v1 / _diff_page_v1 / _finish_v1 / _abandon_v1 /
 *   _erase_scope_v1                                            SECURITY DEFINER; EXECUTE to search_consumer_runtime
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062110_search_reconcile_staging.sql";

export const searchReconcileStaging: LaneManifest = {
  lane: "search-reconcile-staging",
  owner: "staged search reconcile, review/phase3-s2a-b-staged-reconcile (slot 062110)",
  migration: MIGRATION,
  prefixes: ["sessions.search_reconcile_"],
  grantSources: [{ path: MIGRATION, roles: ["search_consumer_runtime"] }],
  functions: {
    "sessions.search_reconcile_caller_v1()": { secdef: false, grants: [] },
    "sessions.search_reconcile_lease_v1(bigint,uuid)": { secdef: false, grants: [] },
    "sessions.search_reconcile_claim_v1(text,text,text,text,text,bigint,text,integer)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.search_reconcile_stage_batch_v1(bigint,uuid,text,smallint,jsonb,text)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.search_reconcile_advance_v1(bigint,uuid,text,text,text[],bigint,bigint,boolean)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.search_reconcile_diff_page_v1(bigint,uuid,text,text,integer,text)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.search_reconcile_finish_v1(bigint,uuid)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.search_reconcile_abandon_v1(bigint,uuid,text)": { secdef: true, grants: ["search_consumer_runtime"] },
    "sessions.search_reconcile_erase_scope_v1(text,text,text,text[])": { secdef: true, grants: ["search_consumer_runtime"] },
  },
  replaces: {},
};
