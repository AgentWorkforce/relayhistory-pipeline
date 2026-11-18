-- Drops the 20261105062140 staged output facts: the trigger staged_facts_reset on
-- sessions.search_reconcile_runs_v1, the functions, the tables sessions.staged_facts_v1 and
-- sessions.staged_facts_runs_v1 (with their indexes and foreign keys), and its ledger row.
-- Dropping the tables loses only in-progress staged facts: the run's next attempt stages them again.
-- Refused while any staged run (20261105062110) holds a live lease, whether it staged facts yet or not
-- (a consumer is mid-run and its next facts call would fail): stop the consumer or wait for the lease to expire. The 20261105062110 staging is left exactly as that step alone leaves it.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='staged output facts: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062140_staged_output_facts.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='staged output facts: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062140_staged_output_facts.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'9d9d079dac32bdf2cddda7dcc3f7372f4b7bf59ce6c982d0b0ad62938d8af570'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='staged output facts: own applied checksum required';END IF;
 IF to_regclass('sessions.search_reconcile_runs_v1') IS NOT NULL AND EXISTS(
      SELECT 1 FROM sessions.search_reconcile_runs_v1 AS t WHERE t.lease_nonce IS NOT NULL AND t.lease_expires_at>clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='staged output facts: a staged run holds a live lease';END IF;
END $rollback_guard$;
DROP TRIGGER staged_facts_reset ON sessions.search_reconcile_runs_v1;
DROP FUNCTION sessions.staged_facts_stage_v1(bigint,uuid,jsonb,integer,boolean),
 sessions.staged_facts_seal_v1(bigint,uuid),
 sessions.staged_facts_read_v1(bigint,uuid,text,text,text[],bigint,integer),
 sessions.staged_facts_status_v1(bigint,uuid),
 sessions.staged_facts_reconcile_v1(bigint,uuid,text,text,text,text[],text,boolean,bigint),
 sessions.staged_facts_whole_v1(bigint),
 sessions.staged_facts_run_v1(bigint,uuid,text[]),
 sessions.staged_facts_reset_v1();
DROP TABLE sessions.staged_facts_v1;
DROP TABLE sessions.staged_facts_runs_v1;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062140_staged_output_facts.sql';
COMMIT;
