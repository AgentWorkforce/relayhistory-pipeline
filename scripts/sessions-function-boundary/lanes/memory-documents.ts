/**
 * Lane: memory-documents (slot 062080).
 * Four SECURITY DEFINER entry points over sessions.decision_records_v1 and sessions.decision_evidence_v1
 * (docs/memory-documents.md): insert and supersede for the worker (service_role); topics and list for
 * bulls96_read_api, pinned to its request scope. No runtime role has a table privilege.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062080_memory_documents.sql";

export const memoryDocuments: LaneManifest = {
  lane: "memory-documents",
  owner: "memory documents decision records and readers (slot 062080)",
  migration: MIGRATION,
  prefixes: ["sessions.decision_records_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role", "bulls96_read_api"] }],
  functions: {
    "sessions.decision_records_insert_v1(text,text,jsonb)": { secdef: true, grants: ["service_role"] },
    "sessions.decision_records_supersede_v1(text,text,uuid,uuid)": { secdef: true, grants: ["service_role"] },
    "sessions.decision_records_topics_v1(text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.decision_records_list_v1(text,text,text,timestamp with time zone,uuid,integer)": { secdef: true, grants: ["bulls96_read_api"] },
  },
  replaces: {},
};

const ERASE_MIGRATION = "supabase/migrations/20261105062090_memory_documents_erase.sql";

/**
 * Lane: memory-documents-erase (slot 062090).
 * The operator per-session erase of decision evidence, run by the self-host ingest login
 * (bulls96_session_ingest) inside its erase lease, and the trigger that refuses evidence for a fenced session.
 */
export const memoryDocumentsErase: LaneManifest = {
  lane: "memory-documents-erase",
  owner: "memory documents operator erase of decision evidence (slot 062090)",
  migration: ERASE_MIGRATION,
  prefixes: ["sessions.decision_evidence_"],
  grantSources: [{ path: ERASE_MIGRATION, roles: ["bulls96_session_ingest"] }],
  functions: {
    "sessions.decision_evidence_erase_session_v1(text,text,text,text,uuid)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.decision_evidence_fence_guard_v1()": { secdef: true, grants: [] },
  },
  replaces: {},
};
