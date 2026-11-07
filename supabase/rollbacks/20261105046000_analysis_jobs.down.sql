-- Rollback for registry slot 20261105046000 (lane F1 analysis jobs). Operator-only.
-- Stop the session-analysis worker and remove the read route first. Dropping the tables
-- forgets every v2 request and job row (native and backfilled); the result objects in S3
-- are left in place (they are re-referenced by a re-apply of the backfill, or erased with
-- their session).
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105046000_analysis_jobs.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='analysis jobs: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;
DROP TRIGGER session_analysis_v2_forget_deleted_directory ON sessions.session_directory;
DROP TRIGGER session_analysis_v2_forget_tombstoned_directory ON sessions.session_directory;
DROP TRIGGER session_analysis_v2_forget_deleted_pointer ON sessions.session_index;
DROP TRIGGER session_analysis_v2_forget_tombstoned_pointer ON sessions.session_index;
DROP FUNCTION sessions.analysis_read_v1(text, text);
DROP FUNCTION sessions.analysis_request_v1(text, text, text, text);
DROP FUNCTION sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2);
DROP FUNCTION sessions.session_analysis_v2_forget();
DROP FUNCTION sessions.session_analysis_admit_scope(text, text);
DROP FUNCTION sessions.session_analysis_source_revision(text, text, text);
DROP FUNCTION sessions.session_analysis_neon_free(text, text, text);
-- Outstanding tickets name objects the sweeper still had to delete: list them first
-- (SELECT result_key FROM sessions.session_analysis_result_tickets) and remove them by hand.
DROP TABLE sessions.session_analysis_result_tickets;
DROP TABLE sessions.session_analysis_requests_v2;
DROP TABLE sessions.session_analysis_jobs_v2;
DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105046000_analysis_jobs.sql';
  END IF;
END
$ledger$;
