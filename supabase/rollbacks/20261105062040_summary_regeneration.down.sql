-- Drops the 20261105062040 summary regeneration objects: the forget triggers on session_index and
-- session_directory, the audit table (sessions.summary_regeneration_audit, with its indexes and triggers)
-- and its functions, the session_briefs next_retry_at index and the five session_briefs columns
-- (their retry/coverage values are discarded; brief rows themselves are untouched), and its ledger row.
-- 96bulls 0006 (session_summaries columns) is independent and stays.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='summary regeneration: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062040_summary_regeneration.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='summary regeneration: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062040_summary_regeneration.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'69e8a924743f512540e44cf53852b5429cd41611f38228d236b6b693556eec3c'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='summary regeneration: own applied checksum required';END IF;
END $rollback_guard$;
DROP TRIGGER summary_regeneration_audit_forget_tombstoned_pointer ON sessions.session_index;
DROP TRIGGER summary_regeneration_audit_forget_deleted_pointer ON sessions.session_index;
DROP TRIGGER summary_regeneration_audit_forget_tombstoned_directory ON sessions.session_directory;
DROP TRIGGER summary_regeneration_audit_forget_deleted_directory ON sessions.session_directory;
DROP TABLE sessions.summary_regeneration_audit;
DROP FUNCTION sessions.summary_regeneration_audit_append_only(),
 sessions.summary_regeneration_audit_forget(),
 sessions.summary_regeneration_audit_purge_v1(integer),
 sessions.summary_regeneration_audit_append_v1(jsonb);
DROP INDEX sessions.session_briefs_next_retry;
ALTER TABLE sessions.session_briefs
 DROP COLUMN covered_turns,
 DROP COLUMN retry_attempts,
 DROP COLUMN next_retry_at,
 DROP COLUMN config_hash,
 DROP COLUMN pipeline_version;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062040_summary_regeneration.sql';
COMMIT;
