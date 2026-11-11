-- Drops the 20261105057010 upload consent entry points, its four tables (policies, per-session
-- decisions, confirmed origins, containment holds) and its ledger row.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='upload consent: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105057010_upload_consent.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='upload consent: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105057010_upload_consent.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'1e1cfe5cc542c1164182038cbb92adae0e3be081fcfc4c7f404d27d7313a24e3'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='upload consent: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.upload_consent_apply_v1(text,text,text,jsonb,boolean,bigint),
 sessions.upload_consent_snapshot_v1(text,text,text,text,jsonb,bigint);
DROP TABLE sessions.upload_consent_holds, sessions.upload_consent_origins,
 sessions.upload_consent_sessions, sessions.upload_consent_policies;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105057010_upload_consent.sql';
COMMIT;
