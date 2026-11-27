/**
 * Slot 062340 (per-org erasure history): the 15 per-org erasure sources whose every INSERT, UPDATE and DELETE the
 * recorder trigger writes to sessions.ingest_org_erasure_history. The one shared list: the migration's triggers and
 * its `source` CHECK must equal it (org-erasure-history.pglite.test.ts pins both), and the hold bridge's erasure-window
 * proof (src/session-ingest/erasure-window.ts ERASURE_SOURCES on review/backlog-ops-hold-bridge) binds to the same set.
 * Every table here has an `org_id` column; a row with a NULL org is not per-org and is not recorded.
 */
export const ORG_ERASURE_HISTORY_SOURCES = Object.freeze([
  "operator_user_erasures",
  "session_erasure_fences",
  "session_erasure_receipts",
  "session_erasure_events",
  "session_erasure_runs",
  "session_erasure_scopes",
  "session_erasure_writes",
  "upload_deletions",
  "upload_resets",
  "upload_erasure_rearm",
  "relayfacts_erasure_fences",
  "session_analysis_purge_fences",
  "session_analysis_purge_authority",
  "ingest_frozen_source_deletions",
  "ingest_frozen_source_resets",
] as const);
export type OrgErasureHistorySource = (typeof ORG_ERASURE_HISTORY_SOURCES)[number];
