/**
 * Lane: search-index-stage (slot 062300).
 * supabase/migrations/20261105062300_search_index_stage.sql (docs/handoffs/incremental-search-indexing.md, M2) owns:
 *   guard_managed_search_wake_v1        queue guard trigger function (pipeline_jobs, the search-index rows); no grant
 *   guard_managed_search_truncate_v1    queue truncate guard trigger function; no grant
 *   enqueue_managed_search_wake_v1      SECURITY DEFINER; the managed wake (§2). No grant here: the owner grants the
 *                                       qualified writer login in a separate, approved step
 *   managed_search_wake_v1              SECURITY DEFINER; the writer's entry: proves the publish clock, then enqueues or
 *                                       records the clock_unprovable refusal (§2 [rev 7]). No grant here (as above)
 *   search_stage_archive_seal_v1        the archive's append-only seal trigger function; no grant
 *   archive_search_stage_v1             SECURITY DEFINER; the only archive write path (§8); EXECUTE to service_role
 *                                       (the operator) ONLY: never a runtime application function
 *   search_job_claim_attempts_v1        SECURITY DEFINER trigger function: keeps the claim receipt's attempt; no grant
 *   search_letter_id_v1                 dlq.ts deadLetterId in SQL, owner-internal (settle calls it); no grant
 *   claim_search_probe_v1               SECURITY DEFINER; the runtime's probe lane (held jobs, fleet bucket); as claim
 *   search_job_lease_v1                 owner-internal lease/park helper of both claims; no grant
 *   claim_search_jobs_v1                SECURITY DEFINER; THE RUNTIME BOUNDARY (§3.2): EXECUTE to service_role, mirrored
 *   settle_search_job_v1                onto the runtime role (RUNTIME_APPLICATION_FUNCTIONS); the only functions of
 *                                       the stage the runtime role executes (§3.3, the server's claim receipt)
 *   resume_search_contention_v1         SECURITY DEFINER; operator release of a 24 h contention hold; service_role only
 * Operator-only, never runtime: replay, catch-up and park are owner-run TypeScript over the guarded queue (no
 * function); the archive is archive_search_stage_v1 above.
 * The rollback (supabase/rollbacks/20261105062300_search_index_stage.down.sql) is hand-written: it carries §8's
 * refusal rules (no search-index job, event or letter) and keeps the archive.
 */
import type { LaneManifest } from "../types.ts";

const MIGRATION = "supabase/migrations/20261105062300_search_index_stage.sql";

export const searchIndexStage: LaneManifest = {
  lane: "search-index-stage",
  owner: "incremental search indexing M2 (slot 062300)",
  migration: MIGRATION,
  prefixes: [
    "sessions.guard_managed_search_wake_v1(",
    "sessions.guard_managed_search_truncate_v1(",
    "sessions.enqueue_managed_search_wake_v1(",
    "sessions.managed_search_wake_v1(",
    "sessions.search_stage_archive_seal_v1(",
    "sessions.archive_search_stage_v1(",
    "sessions.search_job_claim_attempts_v1(",
    "sessions.search_letter_id_v1(",
    "sessions.claim_search_jobs_v1(",
    "sessions.settle_search_job_v1(",
    "sessions.resume_search_contention_v1(",
    "sessions.claim_search_probe_v1(",
    "sessions.search_job_lease_v1(",
  ],
  grantSources: [{ path: MIGRATION, roles: ["service_role"] }],
  functions: {
    "sessions.guard_managed_search_wake_v1()": { secdef: false, grants: [] },
    "sessions.guard_managed_search_truncate_v1()": { secdef: false, grants: [] },
    "sessions.enqueue_managed_search_wake_v1(jsonb,uuid)": { secdef: true, grants: [] },
    "sessions.managed_search_wake_v1(jsonb,uuid)": { secdef: true, grants: [] },
    "sessions.search_stage_archive_seal_v1()": { secdef: false, grants: [] },
    // Operator-only (review finding 5): service_role EXECUTE, never mirrored onto the runtime role.
    "sessions.archive_search_stage_v1()": { secdef: true, grants: ["service_role"], operatorOnly: true },
    "sessions.search_job_claim_attempts_v1()": { secdef: true, grants: [] },
    "sessions.search_letter_id_v1(text,text,text)": { secdef: false, grants: [] },
    // The runtime boundary: service_role EXECUTE, mirrored onto the runtime role (no operatorOnly).
    "sessions.claim_search_jobs_v1(integer)": { secdef: true, grants: ["service_role"] },
    // The probe lane (held jobs, fleet bucket): runtime boundary like the ordinary claim.
    "sessions.claim_search_probe_v1()": { secdef: true, grants: ["service_role"] },
    // Owner-internal lease helper (called only inside the claim functions): no grant.
    "sessions.search_job_lease_v1(bigint,boolean)": { secdef: false, grants: [] },
    "sessions.settle_search_job_v1(bigint,uuid,text)": { secdef: true, grants: ["service_role"] },
    // The 24 h contention cap's operator release: service_role only, never the runtime role.
    "sessions.resume_search_contention_v1(bigint,text)": { secdef: true, grants: ["service_role"], operatorOnly: true },
  },
  replaces: {},
};
