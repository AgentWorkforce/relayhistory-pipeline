import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105047000_coordination_report_jobs.sql";

export const coordinationReports: LaneManifest = {
  lane: "coordination-reports",
  owner: "#211 (codex/workspace-coordination-reports)",
  migration: MIGRATION,
  prefixes: ["sessions.coordination_report_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.coordination_report_source_changed()": { secdef: true, grants: [] },
  },
  replaces: {},
};
