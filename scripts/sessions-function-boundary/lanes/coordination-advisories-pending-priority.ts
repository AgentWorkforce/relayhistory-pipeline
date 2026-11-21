/**
 * Lane: coordination-advisories-pending-priority (slot 062190).
 * The priority-first pull reader, coordination_advisories_pending_for_v2, for bulls96_read_api only, pinned to its
 * request scope (docs/coordination-live-advisories.md, "Priority and delivery"). 062000's v1 is untouched.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062190_coordination_advisories_pending_priority.sql";

export const coordinationAdvisoriesPendingPriority: LaneManifest = {
  lane: "coordination-advisories-pending-priority",
  owner: "coordination advisories priority-first pull reader (slot 062190)",
  migration: MIGRATION,
  // Exact name: the "coordination_advisories_" stem is shared with 062000 and 062070.
  prefixes: ["sessions.coordination_advisories_pending_for_v2("],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.coordination_advisories_pending_for_v2(text,text,text,text,integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
