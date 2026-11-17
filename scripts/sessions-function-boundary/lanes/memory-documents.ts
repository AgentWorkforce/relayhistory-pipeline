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
