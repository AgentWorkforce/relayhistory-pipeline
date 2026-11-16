/** Lane: relay turn forward activation gates (slot 062020). */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062020_turns_forward_gates.sql";

export const turnsForwardGates: LaneManifest = {
  lane: "turns-forward-gates",
  owner: "relay turn cross-origin fence, capability and source discovery (slot 062020)",
  migration: MIGRATION,
  prefixes: [
    "sessions.ingest_turn_",
    "sessions.buffered_turn_sources_for(",
    "sessions.aa_session_directory_hide_turn_only(",
  ],
  grantSources: [
    {
      path: MIGRATION,
      roles: ["bulls96_session_ingest", "service_role", "bulls96_read_api"],
    },
  ],
  functions: {
    "sessions.ingest_turn_materialize_fence(text,text,text,text)": {
      secdef: false,
      grants: [],
    },
    "sessions.ingest_turn_capture_erasure()": { secdef: false, grants: [] },
    "sessions.ingest_turn_capability(jsonb)": {
      secdef: true,
      grants: ["bulls96_session_ingest"],
    },
    "sessions.ingest_turn_register(jsonb)": {
      secdef: true,
      grants: ["bulls96_session_ingest"],
    },
    "sessions.ingest_turn_fences_import(jsonb)": {
      secdef: true,
      grants: ["service_role"],
    },
    "sessions.buffered_turn_sources_for(text,text,text,text,text,text,boolean,text,text)": {
      secdef: true,
      grants: ["bulls96_read_api"],
    },
    "sessions.aa_session_directory_hide_turn_only()": {
      secdef: false,
      grants: [],
    },
  },
  replaces: {},
};
