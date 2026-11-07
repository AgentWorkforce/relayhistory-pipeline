/**
 * Lane: analysis-jobs (lane F1, registry slot 046000, session ANALYSIS panel off Neon).
 * supabase/migrations/20261105046000_analysis_jobs.sql owns four functions:
 *   analysis_request_v1       SECURITY DEFINER; EXECUTE to bulls96_analysis_admit (tenant-pinned)
 *   analysis_request_json_v1  not SECURITY DEFINER; owner-only (called by the definers)
 *   analysis_read_v1          SECURITY DEFINER; EXECUTE to bulls96_read_api
 *   session_analysis_v2_forget  erasure trigger; SECURITY DEFINER; owner-only
 *   session_analysis_admit_scope      admit role + tenant pin; not SECURITY DEFINER; owner-only
 *   session_analysis_source_revision  live pointer-set revision; EXECUTE to service_role (worker)
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
  ],
  grantSources: [{ path: MIGRATION, roles: ["bulls96_read_api", "bulls96_analysis_admit", "service_role"] }],
  functions: {
    "sessions.analysis_read_v1(text,text)": { secdef: true, grants: ["bulls96_read_api"] },
    "sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2)": { secdef: false, grants: [] },
    "sessions.analysis_request_v1(text,text,text,text)": { secdef: true, grants: ["bulls96_analysis_admit"] },
    "sessions.session_analysis_v2_forget()": { secdef: true, grants: [] },
    "sessions.session_analysis_admit_scope(text,text)": { secdef: false, grants: [] },
    "sessions.session_analysis_source_revision(text,text,text)": { secdef: false, grants: ["service_role"] },
  },
  replaces: {},
};
