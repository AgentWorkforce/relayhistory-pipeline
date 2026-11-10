-- Destructive: removes persisted reports and jobs. Disable coordination reports and
-- obtain explicit database-migration approval before applying this rollback.
-- Unwind these triggers before rolling back their event-time source tables.
-- This rollback is valid only while coordination reports are the last applied
-- upstream migration; it deletes its ledger row so the migrator can reapply it.
BEGIN;

DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105047000_coordination_report_jobs.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='coordination reports: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;

DROP TRIGGER coordination_report_pointer_changed ON sessions.session_index;
DROP TRIGGER coordination_report_directory_changed ON sessions.session_directory;
DROP TRIGGER coordination_report_pending_changed ON sessions.session_directory_event_pending;
DROP TRIGGER coordination_report_times_changed ON sessions.session_directory_event_times;
DROP FUNCTION sessions.coordination_report_source_changed();
DROP TABLE sessions.coordination_report_jobs;
DROP TABLE sessions.coordination_report_epochs;

DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105047000_coordination_report_jobs.sql';
  END IF;
END
$ledger$;

COMMIT;
