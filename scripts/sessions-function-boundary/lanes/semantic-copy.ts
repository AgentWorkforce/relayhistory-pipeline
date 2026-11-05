/** Lane: dormant global semantic-feature copier checkpoints (#191). */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261103100000_semantic_copy_state.sql";

export const semanticCopy: LaneManifest = {
  lane: "semantic-copy",
  owner: "#191 (semantic copy state)",
  migration: MIGRATION,
  prefixes: [
    "sessions.claim_search_feature_copy(",
    "sessions.commit_search_feature_copy(",
    "sessions.finish_search_feature_copy(",
    "sessions.heartbeat_search_feature_copy(",
    "sessions.initialize_search_feature_baseline(",
    "sessions.register_search_feature_source(",
    "sessions.search_feature_copy_",
  ],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.claim_search_feature_copy(text,text,text,uuid)": { secdef: true, grants: ["service_role"] },
    "sessions.commit_search_feature_copy(text,text,uuid,bigint,text,jsonb,timestamp with time zone)": {
      secdef: true,
      grants: ["service_role"],
    },
    "sessions.finish_search_feature_copy(text,text,uuid,bigint,text,text)": { secdef: true, grants: ["service_role"] },
    "sessions.heartbeat_search_feature_copy(text,text,uuid,bigint)": { secdef: true, grants: ["service_role"] },
    "sessions.initialize_search_feature_baseline(text,text,uuid,bigint,text,bigint)": {
      secdef: true,
      grants: ["service_role"],
    },
    "sessions.register_search_feature_source(text,text,text,text,text)": { secdef: true, grants: ["service_role"] },
    "sessions.search_feature_copy_body(text,text)": { secdef: false, grants: [] },
    "sessions.search_feature_copy_state_valid(jsonb)": { secdef: false, grants: [] },
  },
  replaces: {},
};
