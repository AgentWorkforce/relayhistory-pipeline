-- Drops the two 047000 coordination readers, their partial indexes, and the ledger row.
-- LOCAL/TEST ONLY. On production the indexes may have been built out of band with CREATE INDEX
-- CONCURRENTLY and adopted. This transactional rollback uses non-concurrent DROP INDEX and takes locks;
-- never run it on a real database without the operator's explicit word.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination fact readers: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105047000_coordination_fact_readers.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination fact readers: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105047000_coordination_fact_readers.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'cf9d69b730fbd98d3157fa3544e1108fe1ad3647c2628a79d2eea3da19ce525a'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination fact readers: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.coordination_output_facts_for(text,timestamptz,timestamptz,text,integer);
DROP FUNCTION sessions.coordination_write_facts_for(text,timestamptz,timestamptz,text,integer);
DROP INDEX sessions.session_output_facts_coordination_outcomes_idx;
DROP INDEX sessions.session_event_facts_coordination_writes_idx;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105047000_coordination_fact_readers.sql';
COMMIT;
