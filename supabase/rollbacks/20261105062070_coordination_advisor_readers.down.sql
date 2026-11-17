-- Drops the 20261105062070 coordination advisor readers (three tenant-explicit facts mirrors and
-- coordination_advisories_recent_v1) and its ledger row. It adds no table, so nothing else is removed.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisor readers: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062070_coordination_advisor_readers.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisor readers: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062070_coordination_advisor_readers.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'5d17efbafc208baeb5810381279dcbc26b18f45eac2138deaace19d0ef291249'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisor readers: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.coordination_advisor_write_facts_v1(text,text,timestamptz,timestamptz,text,integer),
 sessions.coordination_advisor_output_facts_v1(text,text,timestamptz,timestamptz,text,integer),
 sessions.coordination_advisor_usage_facts_v1(text,text,timestamptz,timestamptz,text,integer),
 sessions.coordination_advisories_recent_v1(text,text,timestamptz);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062070_coordination_advisor_readers.sql';
COMMIT;
