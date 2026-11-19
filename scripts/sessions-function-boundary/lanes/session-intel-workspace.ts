/**
 * Lane: session-intel-workspace (slot 062170, provisional).
 * The two read-only SECURITY DEFINER readers behind GET /v1/session-intel/workspace
 * (docs/session-intel-workspace-api.md): the workspace's open advisories and its sessions' current intent and
 * decision titles, for bulls96_read_api only, pinned to its request scope. No role gets a table privilege.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062170_session_intel_workspace.sql";

export const sessionIntelWorkspace: LaneManifest = {
  lane: "session-intel-workspace",
  owner: "session intelligence workspace view readers (slot 062170)",
  migration: MIGRATION,
  prefixes: ["sessions.session_intel_workspace_"],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api"] }],
  functions: {
    "sessions.session_intel_workspace_alerts_v1(text,text,integer)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.session_intel_workspace_claims_v1(text,text,text[],integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};
