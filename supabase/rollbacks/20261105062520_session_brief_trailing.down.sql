-- Reverse of 20261105062520: drops sessions.session_brief_wakeups.trailing_since (T144 trailing state). Refused
-- (55000) unless 062520 is applied with this exact checksum, and while a later migration is applied. Guard, the
-- column drop and ledger removal are one transaction under the migrator lock; on an error the caller must ROLLBACK
-- (or disconnect). After it, finishWakeup must be the pre-062520 store.ts (it names the column).
-- Code only: hosted execution needs Khaliq's separate explicit approval.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session brief trailing: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062520_session_brief_trailing.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session brief trailing: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062520_session_brief_trailing.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'6b2e22f7a4dc32c4b5fbf091bb1e12aacec08057569f6425c858426a384a070c'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session brief trailing: own applied checksum required';END IF;
END $rollback_guard$;
ALTER TABLE sessions.session_brief_wakeups DROP COLUMN trailing_since;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062520_session_brief_trailing.sql' AND sha256='6b2e22f7a4dc32c4b5fbf091bb1e12aacec08057569f6425c858426a384a070c';
COMMIT;
