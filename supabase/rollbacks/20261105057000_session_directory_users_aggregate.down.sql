-- Drops the 057000 per-user session aggregate reader and its ledger row; the directory is untouched.
-- Guard, function removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session users aggregate: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105057000_session_directory_users_aggregate.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session users aggregate: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105057000_session_directory_users_aggregate.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'602fd08a8b4255c12cd6c305af29620e84d436b26fc22c5a514baa23c78a4b6d'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session users aggregate: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_directory_users_for(text,timestamptz,timestamptz,integer);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105057000_session_directory_users_aggregate.sql';
COMMIT;
