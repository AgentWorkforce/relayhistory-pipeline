-- Drops the 058010 operator per-user erase functions, its ledger table and its ledger row. Erasures already
-- performed are not undone (deleted rows and objects stay deleted); only the front door and its receipts go.
-- Guard, drops and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105058010_operator_user_erase.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105058010_operator_user_erase.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'d807ac22f5994ae5128a8929a34c55af281a576223c45beccd287c1d7c212559'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.operator_user_erase_complete(text);
DROP FUNCTION sessions.operator_user_erase_step(text,integer);
DROP FUNCTION sessions.operator_user_erase_begin(text,text,text,text);
DROP FUNCTION sessions.operator_user_erase_render(text);
DROP FUNCTION sessions.operator_user_erase_count(text,jsonb);
DROP TABLE sessions.operator_user_erasures;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105058010_operator_user_erase.sql';
COMMIT;
