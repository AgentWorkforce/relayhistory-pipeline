import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062160_session_intel_controls.sql";
export const sessionIntelControls: LaneManifest = {
  lane: "session-intel-controls",
  owner: "session intelligence optional-feature controls (slot 062160)",
  migration: MIGRATION,
  prefixes: ["sessions.session_intel_controls_v1("],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: { "sessions.session_intel_controls_v1(text,text)": { secdef: true, grants: ["service_role"] } },
  replaces: {},
};
