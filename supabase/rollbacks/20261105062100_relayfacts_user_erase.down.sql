-- Drops the 20261105062100 relayfacts user erase: the guard trigger and the two lock triggers on
-- sessions.relayfacts_v1 and sessions.relayfacts_evidence_v1, their functions, sessions.relayfacts_user_erase_v1,
-- the table sessions.relayfacts_erasure_fences and the ledger row. Erased evidence is not restored.
-- Fences: while any fence row exists this refuses (55000). Dropping them means late evidence writes for a
-- user-erased session are accepted again, so the analysis tick can rewrite relayfacts from an erased user's
-- sessions. To drop them anyway, set the override in the same session before running this file (it opens its own
-- transaction, so SET LOCAL cannot precede it), for example
--   psql -v ON_ERROR_STOP=1 -c "SET relayfacts.rollback_drop_fences = 'true'" -f <this file>
-- Anything but exactly 'true' (unset included) refuses. The fence table is locked before the check, so no erase
-- can add a fence between the check and the drop.
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
 IF NOT FOUND OR v_sha<>'4bbb0561afa2ee9ed0c4e6e489aa99a5f5b2e31ec3d29ca86500763ef51ad039'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts user erase: own applied checksum required';END IF;
 LOCK TABLE sessions.relayfacts_erasure_fences IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.relayfacts_erasure_fences)
    AND coalesce(current_setting('relayfacts.rollback_drop_fences',true),'')<>'true'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts user erase: erasure fences exist; set relayfacts.rollback_drop_fences=true to drop them';END IF;
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
