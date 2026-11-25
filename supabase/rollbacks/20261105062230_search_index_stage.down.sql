-- Rolls back 20261105062230 (the search-index stage, docs/handoffs/incremental-search-indexing.md §8).
-- Run only on the drain path, AFTER: writers off (SEARCH_WAKE_ENQUEUE, the indexer's
-- PIPELINE_MANAGED_SEARCH_WAKE_ENABLED, the catch-up), the stage drained, the consumer off (SEARCH_JOBS_ENABLED),
-- the code rolled back. After a park, the stage's letters and events still exist: this rollback REFUSES (55000)
-- until the parked letters are replayed, resolved and archived (sessions.archive_search_stage_v1(), §8 [rev 7]).
-- It refuses while ANY search-index row exists in pipeline_jobs, pipeline_events or pipeline_dead_letters.
-- It drops the guards, the managed enqueue, the scope index, the cursor, the archive function and the resolution
-- column, and restores the three stage CHECKs to the 20261031000100 sets. sessions.search_stage_archive and its
-- seal stay while the archive holds any row: it is an audit artifact, removed only by a separate, explicitly
-- approved cleanup (until then this slot cannot be re-applied: the migrator refuses its unledgered objects). An
-- empty archive goes with the rest.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- Code only: hosted execution needs Khaliq's separate explicit approval. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search index stage: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062230_search_index_stage.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search index stage: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062230_search_index_stage.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'ffb00e6d54c3b8500f1bceecb9553debb610425937ce30f21912186304f3eb84'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search index stage: own applied checksum required';END IF;
 -- §8: the additive migration stays until the stage is fully empty, in all three tables.
 LOCK TABLE sessions.pipeline_jobs, sessions.pipeline_events, sessions.pipeline_dead_letters IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.pipeline_jobs WHERE stage='search-index')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search index stage: search-index jobs exist; drain or park them first';END IF;
 IF EXISTS(SELECT 1 FROM sessions.pipeline_dead_letters WHERE stage='search-index')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search index stage: search-index dead letters exist; replay, resolve and archive them first';END IF;
 IF EXISTS(SELECT 1 FROM sessions.pipeline_events WHERE stage='search-index')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search index stage: search-index events exist; archive them first';END IF;
END $rollback_guard$;
DROP TRIGGER guard_search_wake_delete ON sessions.pipeline_jobs;
DROP TRIGGER guard_search_wake_truncate ON sessions.pipeline_jobs;
DROP TRIGGER guard_search_wake_insert ON sessions.pipeline_jobs;
DROP TRIGGER guard_search_wake_update ON sessions.pipeline_jobs;
DROP FUNCTION sessions.guard_managed_search_truncate_v1();
DROP FUNCTION sessions.guard_managed_search_wake_v1();
DROP FUNCTION sessions.enqueue_managed_search_wake_v1(jsonb,uuid);
DROP FUNCTION sessions.archive_search_stage_v1();
DROP INDEX sessions.pipeline_jobs_search_index_scope;
DROP TABLE sessions.search_catchup_cursor;
ALTER TABLE sessions.pipeline_dead_letters DROP COLUMN resolution;
DO $archive$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM sessions.search_stage_archive) THEN
   DROP TABLE sessions.search_stage_archive;
   DROP FUNCTION sessions.search_stage_archive_seal_v1();
 END IF;
END $archive$;
ALTER TABLE sessions.pipeline_events DROP CONSTRAINT pipeline_events_stage_check;
ALTER TABLE sessions.pipeline_events ADD CONSTRAINT pipeline_events_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'fetch'));
ALTER TABLE sessions.pipeline_dead_letters DROP CONSTRAINT pipeline_dead_letters_stage_check;
ALTER TABLE sessions.pipeline_dead_letters ADD CONSTRAINT pipeline_dead_letters_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'fetch'));
ALTER TABLE sessions.pipeline_jobs DROP CONSTRAINT pipeline_jobs_stage_check;
ALTER TABLE sessions.pipeline_jobs ADD CONSTRAINT pipeline_jobs_stage_check
  CHECK (stage IN ('object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation'));
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062230_search_index_stage.sql';
COMMIT;
