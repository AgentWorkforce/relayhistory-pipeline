-- Post-apply OPERATOR job for registry slot 20261105062120 (analysis org S3 cutover proof). Not a migration:
-- it is CREATE INDEX CONCURRENTLY, which cannot run inside migrate.ts's BEGIN/COMMIT, so it has no slot
-- (registry rule). Run once, outside a transaction, after 20261105062120 is applied and BEFORE
-- SESSION_ANALYSIS_CUTOVER_PROOF is turned on, as the table owner (Khaliq-gated like any prod change):
--   psql -v ON_ERROR_STOP=1 -X -d <HoopDb> -f supabase/pending/20261105062120_session_index_org_canonical.sql
-- It serves sessions.session_analysis_first_seen_v1's canonical-id lookup (org-wide, live and tombstoned
-- rows; the physical-id lookup is served by si_session). Correctness never depends on it: without the index
-- the lookup reads the org's pointers, slower, never different. Idempotent.
CREATE INDEX CONCURRENTLY IF NOT EXISTS session_index_org_canonical_idx
  ON sessions.session_index (org_id, canonical_session_id)
  WHERE canonical_session_id IS NOT NULL;
