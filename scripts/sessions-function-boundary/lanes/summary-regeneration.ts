/**
 * Lane: summary-regeneration (slot 062040).
 * The append-only sessions.summary_regeneration_audit table: two trigger functions (append-only
 * guard and forget/erase cleanup, no grants) and two SECURITY DEFINER worker entry points
 * (service_role): the batch append and the retention purge. The runtime has no table write grant.
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
    "sessions.summary_regeneration_audit_append_v1(jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.summary_regeneration_audit_purge_v1(integer)": { secdef: true, grants: ["service_role"] },
  },
  replaces: {},
};
