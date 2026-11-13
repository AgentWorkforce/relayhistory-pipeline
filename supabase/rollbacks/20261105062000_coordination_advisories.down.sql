-- Drops the 20261105062000 coordination advisories readers and writers, its table
-- (sessions.coordination_advisories, with its indexes) and its ledger row.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062000_coordination_advisories.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062000_coordination_advisories.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'ed0189d50b7139b017187ee53eb6b12f66d066e01ee06168fd0d3f3e23f7e5ce'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.coordination_advisories_upsert_v1(text,text,jsonb),
 sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text),
 sessions.coordination_advisories_pending_for_v1(text,text,text,text),
 sessions.coordination_advisories_ack_v1(text,text,text,text,text),
 sessions.coordination_advisories_sweep_v1();
DROP TABLE sessions.coordination_advisories;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062000_coordination_advisories.sql';
COMMIT;
