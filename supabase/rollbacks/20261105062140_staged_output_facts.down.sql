-- Drops the 20261105062140 staged output facts functions, its table sessions.staged_facts_v1 (with its
-- indexes and its foreign key to sessions.search_reconcile_stage_v1), the anchor rows its stage writer added
-- to sessions.search_reconcile_stage_v1 (side 'fold', key_hash '~output_facts'), and its ledger row.
-- Dropping the table loses only in-progress staged facts: the run's next attempt stages them again.
-- Refused while a run that staged facts holds a live lease (a consumer is mid-run): stop the consumer or
-- wait for the lease to expire. The 20261105062110 staging is left exactly as that step alone leaves it.
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
 IF NOT FOUND OR v_sha<>'82a07fef4a941fbcaa56a03b431f8c536adfa1291cb0a1679105d8b0d8bc0627'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='staged output facts: own applied checksum required';END IF;
 IF to_regclass('sessions.staged_facts_v1') IS NOT NULL AND EXISTS(
      SELECT 1 FROM sessions.search_reconcile_runs_v1 AS t
       WHERE t.lease_nonce IS NOT NULL AND t.lease_expires_at>clock_timestamp()
         AND EXISTS(SELECT 1 FROM sessions.search_reconcile_stage_v1 AS s
                     WHERE s.run_id=t.run_id AND s.side='fold' AND s.key_hash='~output_facts'))
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='staged output facts: a staged run holds a live lease';END IF;
END $rollback_guard$;
-- The anchors first (their facts stage rows cascade): no fold row of this step outlives it.
DELETE FROM sessions.search_reconcile_stage_v1 WHERE side='fold' AND key_hash='~output_facts';
DROP FUNCTION sessions.staged_facts_stage_v1(bigint,uuid,jsonb,integer,boolean),
 sessions.staged_facts_seal_v1(bigint,uuid),
 sessions.staged_facts_read_v1(bigint,uuid,text,text,text[],bigint,integer),
 sessions.staged_facts_status_v1(bigint,uuid),
 sessions.staged_facts_reconcile_v1(bigint,uuid,text,text,text,text[],text,boolean,bigint),
 sessions.staged_facts_whole_v1(bigint),
 sessions.staged_facts_run_v1(bigint,uuid,text[]);
DROP TABLE sessions.staged_facts_v1;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062140_staged_output_facts.sql';
COMMIT;
