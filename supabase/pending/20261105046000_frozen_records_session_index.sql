-- Post-apply OPERATOR job for registry slot 20261105046000 (lane F1). Not a migration: it is
-- CREATE INDEX CONCURRENTLY, which cannot run inside migrate.ts's BEGIN/COMMIT, so it has no
-- slot (registry rule). Run once, outside a transaction, after 20261105046000 is applied, as
-- the table owner:
--   psql -v ON_ERROR_STOP=1 -X -d <HoopDb> -f supabase/pending/20261105046000_frozen_records_session_index.sql
-- It serves sessions.session_analysis_neon_free's "absent from the frozen copy" probe
-- (org_id, session_id) on a large sealed copy. Correctness never depends on it: without the
-- index the probe is slower, not different. Idempotent.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_frozen_source_records_session_idx
  ON sessions.ingest_frozen_source_records (org_id, session_id);
