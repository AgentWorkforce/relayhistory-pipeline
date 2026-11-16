/**
 * Lane: summary-regeneration (slot 062040).
 * The append-only sessions.summary_regeneration_audit table: two trigger functions (append-only
 * guard and forget/erase cleanup, no grants) and one SECURITY DEFINER retention purge entry point
 * for the worker (service_role). The runtime role inserts audit rows through its table grant.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062040_summary_regeneration.sql";

export const summaryRegeneration: LaneManifest = {
  lane: "summary-regeneration",
  owner: "summary regeneration audit table and retention purge (slot 062040)",
  migration: MIGRATION,
  prefixes: ["sessions.summary_regeneration_audit_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.summary_regeneration_audit_append_only()": { secdef: false, grants: [] },
    "sessions.summary_regeneration_audit_forget()": { secdef: true, grants: [] },
    "sessions.summary_regeneration_audit_purge_v1(integer)": { secdef: true, grants: ["service_role"] },
  },
  replaces: {},
};
