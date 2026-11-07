/**
 * Lane: analysis-jobs (lane F1, registry slot 046000, session ANALYSIS panel off Neon).
 * supabase/migrations/20261105046000_analysis_jobs.sql owns four functions:
 *   analysis_request_v1       SECURITY DEFINER; EXECUTE to bulls96_analysis_admit (tenant-pinned)
 *   analysis_request_json_v1  not SECURITY DEFINER; owner-only (called by the definers)
 *   analysis_read_v1          SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   session_analysis_v2_forget  erasure trigger; SECURITY DEFINER; owner-only
 *   session_analysis_admit_scope      admit role + tenant pin; not SECURITY DEFINER; owner-only
 *   session_analysis_source_revision  pointer-set + proof-input revision; SECURITY DEFINER; EXECUTE to service_role (worker)
 *   session_analysis_neon_free        never-in-Neon proof; EXECUTE to service_role (worker)
 *   session_analysis_require_caller   caller pin for the definers; not SECURITY DEFINER; owner-only
 *   purge_session_analysis_for_owned  C3 reset/disable purge of the user's owned set; SECURITY DEFINER; owner-only
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105046000_analysis_jobs.sql";

export const analysisJobs: LaneManifest = {
  lane: "analysis-jobs",
  owner: "lane F1 (feat/f1-analysis-jobs-046000)",
  migration: MIGRATION,
  prefixes: [
    "sessions.analysis_read_v1(",
    "sessions.analysis_request_json_v1(",
    "sessions.analysis_request_v1(",
    "sessions.session_analysis_v2_forget(",
    "sessions.session_analysis_admit_scope(",
    "sessions.session_analysis_source_revision(",
    "sessions.session_analysis_neon_free(",
    "sessions.purge_session_analysis_for_owned(",
    "sessions.session_analysis_purge_grant(",
    "sessions.session_analysis_require_caller(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api", "bulls96_analysis_admit", "service_role"] }],
  functions: {
    "sessions.analysis_read_v1(text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2)": { secdef: false, grants: [] },
    "sessions.analysis_request_v1(text,text,text,text)": { secdef: true, grants: ["bulls96_analysis_admit"] },
    "sessions.session_analysis_v2_forget()": { secdef: true, grants: [] },
    "sessions.session_analysis_admit_scope(text,text)": { secdef: false, grants: [] },
    "sessions.session_analysis_source_revision(text,text,text)": { secdef: true, grants: ["service_role"] },
    "sessions.session_analysis_neon_free(text,text,text)": { secdef: true, grants: ["service_role"] },
    "sessions.purge_session_analysis_for_owned(text,text,text,jsonb)": { secdef: true, grants: [] },
    "sessions.purge_session_analysis_for_owned(text,text,text)": { secdef: true, grants: [] },
    "sessions.session_analysis_purge_grant(text,text,text)": { secdef: false, grants: [] },
    "sessions.session_analysis_require_caller(text[])": { secdef: false, grants: [] },
  },
  replaces: {},
};
