/**
 * Lane: analysis-org-s3-cutover (slot 062120).
 * The session analysis worker's scoped completeness proof (docs/session-analysis-completeness.md,
 * condition (d)): session_analysis_cutover_free, SECURITY DEFINER, EXECUTE to service_role (the
 * worker) only; the owner-only immutability trigger function of sessions.org_s3_cutover and its history;
 * and the owner-only correction function (later-only unless Khaliq-approved lowering).
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062120_analysis_org_s3_cutover.sql";

export const analysisOrgS3Cutover: LaneManifest = {
  lane: "analysis-org-s3-cutover",
  owner: "session analysis org S3 cutover proof (slot 062120)",
  migration: MIGRATION,
  prefixes: ["sessions.session_analysis_cutover_free(", "sessions.session_analysis_first_seen_v1(", "sessions.org_s3_cutover_"],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.session_analysis_cutover_free(text,text,text,text,timestamp with time zone)": { secdef: true, grants: ["service_role"] },
    "sessions.org_s3_cutover_immutable_v1()": { secdef: false, grants: [] },
    "sessions.org_s3_cutover_stamp_v1()": { secdef: false, grants: [] },
    "sessions.session_analysis_first_seen_v1(text,text,text)": { secdef: false, grants: [] },
    "sessions.org_s3_cutover_correct_v1(text,timestamp with time zone,timestamp with time zone,text,text,boolean)": { secdef: false, grants: [] },
  },
  replaces: {},
};

const REVISION_MIGRATION = "supabase/migrations/20261105062130_analysis_cutover_source_revision.sql";

/**
 * Lane: analysis-cutover-source-revision (slot 062130). REPLACES 046000's
 * session_analysis_source_revision (ACL kept: service_role and the runtime role) so the cutover
 * proof's database inputs move the revision. Owns no function of its own.
 */
export const analysisCutoverSourceRevision: LaneManifest = {
  lane: "analysis-cutover-source-revision",
  owner: "session analysis cutover proof in the source revision (slot 062130)",
  migration: REVISION_MIGRATION,
  prefixes: [],
  grantSources: [],
  functions: {},
  replaces: {
    "sessions.session_analysis_source_revision(text,text,text)": { secdef: true, original: "supabase/migrations/20261105046000_analysis_jobs.sql" },
  },
};
