/**
 * Lane: claims-scheduler (slot 062180, PROVISIONAL; Session Intelligence M1-3).
 * supabase/migrations/20261105062180_claims_scheduler.sql owns four functions:
 *   session_analysis_schedule_v1             SECURITY DEFINER; EXECUTE to service_role (and the runtime role)
 *   session_analysis_schedule_candidates_v1  SECURITY DEFINER; EXECUTE to service_role (and the runtime role)
 *   session_analysis_admit_live_v1           046000's lock, evidence and purge-fence block; owner-only
 *   session_analysis_admit_job_v1            046000's reuse-or-new-job block; owner-only
 * and REPLACES 046000's analysis_request_v1 (ACL kept: bulls96_analysis_admit) to call the two helpers.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062180_claims_scheduler.sql";

export const claimsScheduler: LaneManifest = {
  lane: "claims-scheduler",
  owner: "session intelligence claims scheduler (slot 062180, provisional)",
  migration: MIGRATION,
  prefixes: [
    "sessions.session_analysis_schedule_",
    "sessions.session_analysis_admit_live_v1(",
    "sessions.session_analysis_admit_job_v1(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.session_analysis_schedule_v1(text,text,text,integer,integer,integer,bigint,bigint)": { secdef: true, grants: ["service_role"] },
    "sessions.session_analysis_schedule_candidates_v1(integer,integer,integer)": { secdef: true, grants: ["service_role"] },
    "sessions.session_analysis_admit_live_v1(text,text,text)": { secdef: false, grants: [] },
    "sessions.session_analysis_admit_job_v1(text,text,text)": { secdef: false, grants: [] },
  },
  replaces: {
    "sessions.analysis_request_v1(text,text,text,text)": { secdef: true, original: "supabase/migrations/20261105046000_analysis_jobs.sql" },
  },
};
