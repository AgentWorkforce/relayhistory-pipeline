-- Drops the 20261105062100 relayfacts user erase: the guard trigger and the two lock triggers on
-- sessions.relayfacts_v1 and sessions.relayfacts_evidence_v1, their functions, sessions.relayfacts_user_erase_v1,
-- the table sessions.relayfacts_erasure_fences and the ledger row. Dropping the fences means late evidence writes
-- for a user-erased session are no longer refused; erased evidence is not restored.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts user erase: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062100_relayfacts_user_erase.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts user erase: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062100_relayfacts_user_erase.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'b12ed339043ab19c7cdcc21ac6a73fa3ad2b399857608dcc77c75348af99d94a'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts user erase: own applied checksum required';END IF;
END $rollback_guard$;
DROP TRIGGER relayfacts_user_erase_guard ON sessions.relayfacts_evidence_v1;
DROP TRIGGER relayfacts_user_erase_lock_evidence ON sessions.relayfacts_evidence_v1;
DROP TRIGGER relayfacts_user_erase_lock_records ON sessions.relayfacts_v1;
DROP FUNCTION sessions.relayfacts_user_erase_guard_v1(),
 sessions.relayfacts_user_erase_lock_v1(),
 sessions.relayfacts_user_erase_v1(text,text,text,text);
DROP TABLE sessions.relayfacts_erasure_fences;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062100_relayfacts_user_erase.sql';
COMMIT;
