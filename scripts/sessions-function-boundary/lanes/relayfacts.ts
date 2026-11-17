/**
 * Lane: relayfacts (slot 062080).
 * Four SECURITY DEFINER entry points over sessions.relayfacts_v1 and sessions.relayfacts_evidence_v1
 * (docs/relayfacts.md): insert and supersede for the worker (service_role); topics and list for
 * bulls96_read_api, pinned to its request scope. No runtime role has a table privilege.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062080_memory_documents.sql";

export const relayfacts: LaneManifest = {
  lane: "relayfacts",
  owner: "relayfacts decision records and readers (slot 062080)",
  migration: MIGRATION,
  // One exact prefix per function: a bare "sessions.relayfacts_" would overlap the erase lane's
  // "sessions.relayfacts_evidence_" (062090), and the generator refuses overlapping prefixes.
  prefixes: ["sessions.relayfacts_insert_", "sessions.relayfacts_supersede_", "sessions.relayfacts_topics_", "sessions.relayfacts_list_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role", "bulls96_read_api"] }],
  functions: {
    "sessions.relayfacts_insert_v1(text,text,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_supersede_v1(text,text,uuid,uuid)": { secdef: true, grants: ["service_role"] },
    "sessions.relayfacts_topics_v1(text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.relayfacts_list_v1(text,text,text,timestamp with time zone,uuid,integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};

const ERASE_MIGRATION = "supabase/migrations/20261105062090_relayfacts_erase.sql";

/**
 * Lane: relayfacts-erase (slot 062090).
 * The operator per-session erase of decision evidence, run by the self-host ingest login
 * (bulls96_session_ingest) inside its erase lease, and the trigger that refuses evidence for a fenced session.
 */
export const relayfactsErase: LaneManifest = {
  lane: "relayfacts-erase",
  owner: "relayfacts operator erase of decision evidence (slot 062090)",
  migration: ERASE_MIGRATION,
  prefixes: ["sessions.relayfacts_evidence_"],
  grantSources: [{ path: ERASE_MIGRATION, roles: ["bulls96_session_ingest"] }],
  functions: {
    "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.relayfacts_evidence_fence_guard_v1()": { secdef: true, grants: [] },
  },
  replaces: {},
};
