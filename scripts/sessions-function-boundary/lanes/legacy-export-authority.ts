/** Lane (slot 062020): durable legacy-export owner authority and its two eraser readers. */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062020_legacy_export_scope_authority.sql";
const EXPORTER = "bulls96_legacy_export_privacy";
const ERASER = "bulls96_privacy_eraser";

export const legacyExportAuthority: LaneManifest = {
  lane: "legacy-export-authority",
  owner: "Neon exit legacy export/hosted erasure (slot 062020)",
  migration: MIGRATION,
  prefixes: ["sessions.legacy_export_", "sessions.ingest_legacy_export_"],
  grantSources: [{ path: MIGRATION, roles: [EXPORTER, ERASER] }],
  functions: {
    "sessions.ingest_legacy_export_scope_authority_register(jsonb)": { secdef: true, grants: [EXPORTER] },
    "sessions.legacy_export_privacy_authority_ready()": { secdef: true, grants: [EXPORTER] },
    "sessions.legacy_export_privacy_eraser_page(jsonb)": { secdef: true, grants: [ERASER] },
    "sessions.legacy_export_privacy_eraser_still_fenced(jsonb)": { secdef: true, grants: [ERASER] },
  },
  replaces: {},
};
