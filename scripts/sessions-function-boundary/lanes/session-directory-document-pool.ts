/** Lane: the third (directory-document) projection pool finish (#037000). */
import type { LaneManifest } from "../types.ts";

const MIGRATION =
  "supabase/migrations/20261105037000_session_directory_document_pool.sql";

export const sessionDirectoryDocumentPool: LaneManifest = {
  lane: "session-directory-document-pool",
  owner: "supporter-cloud (#document-pool)",
  migration: MIGRATION,
  prefixes: ["sessions.canonical_feature_document_finish"],
  grantSources: [
    { path: MIGRATION, roles: ["bulls96_canonical_feature_producer"] },
  ],
  functions: {
    "sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text)": {
      secdef: true,
      grants: ["bulls96_canonical_feature_producer"],
    },
  },
  replaces: {},
};
