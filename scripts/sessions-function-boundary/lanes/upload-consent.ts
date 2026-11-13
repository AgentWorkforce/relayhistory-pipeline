/**
 * Lane: upload-consent (slot 057010).
 * Two SECURITY DEFINER entry points over the principal-keyed upload consent tables
 * (src/session-ingest/upload-consent-sql.ts): the per-batch snapshot and the single-revision apply.
 * EXECUTE to bulls96_session_ingest only; containment holds have no function (owner/operator only).
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105057010_upload_consent.sql";

export const uploadConsent: LaneManifest = {
  lane: "upload-consent",
  owner: "server-owned upload consent (slot 057010)",
  migration: MIGRATION,
  prefixes: ["sessions.upload_consent_"],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_session_ingest"] }],
  functions: {
    "sessions.upload_consent_snapshot_v1(text,text,text,text,jsonb,bigint)": { secdef: true, grants: ["bulls96_session_ingest"] },
    "sessions.upload_consent_apply_v1(text,text,text,jsonb,boolean,bigint)": { secdef: true, grants: ["bulls96_session_ingest"] },
  },
  replaces: {},
};
