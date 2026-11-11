-- Drops the 056000 summary activity reader and its ledger row; session briefs are retained.
-- Guard, function removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='summary activity: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105056000_session_summary_activity.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='summary activity: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105056000_session_summary_activity.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'002273365c9c3f7d17603ba783456a298012b1e17476d5fca625b7a71740a15d'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='summary activity: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_summary_activity_for(text,timestamptz,timestamptz);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105056000_session_summary_activity.sql';
COMMIT;
