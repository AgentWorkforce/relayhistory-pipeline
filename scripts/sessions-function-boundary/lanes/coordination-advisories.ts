/**
 * Lane: coordination-advisories (slot 062000).
 * Five SECURITY DEFINER entry points over sessions.coordination_advisories
 * (docs/coordination-live-advisories.md): upsert, mark-delivered and sweep for the worker (service_role);
 * pending-for and ack for bulls96_read_api, pinned to its request scope. No runtime role has a table privilege.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062000_coordination_advisories.sql";

export const coordinationAdvisories: LaneManifest = {
  lane: "coordination-advisories",
  owner: "coordination live advisories table and readers (slot 062000)",
  migration: MIGRATION,
  prefixes: ["sessions.coordination_advisories_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role", "bulls96_read_api"] }],
  functions: {
    "sessions.coordination_advisories_upsert_v1(text,text,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text)": { secdef: true, grants: ["service_role"] },
    "sessions.coordination_advisories_pending_for_v1(text,text,text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.coordination_advisories_ack_v1(text,text,text,text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.coordination_advisories_sweep_v1()": { secdef: true, grants: ["service_role"] },
  },
  replaces: {},
};
