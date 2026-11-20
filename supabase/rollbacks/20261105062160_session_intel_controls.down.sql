-- Removes all optional intelligence controls, so the client fails closed. No
-- claims, summaries or advisories are deleted. Copy operator settings before
-- rollback if they must be restored later. No feature is enabled on reapply.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1);
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session intelligence controls: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062160_session_intel_controls.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session intelligence controls: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062160_session_intel_controls.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'26b633354375eae7f893ccebda8f3edc096936343c88abb0a8a4069b6d64e54f'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session intelligence controls: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_intel_controls_v1(text,text);
DROP TABLE sessions.session_intel_controls;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062160_session_intel_controls.sql';
COMMIT;
