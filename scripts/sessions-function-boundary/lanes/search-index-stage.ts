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
  },
  replaces: {},
};
