-- Drops the 20261105062110 search reconcile staging functions, its two tables
-- (sessions.search_reconcile_stage_v1 and sessions.search_reconcile_runs_v1, with their indexes) and its ledger row.
-- Dropping the stage tables loses only in-progress staged runs: their next reconcile tick starts the scope over.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search reconcile staging: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062110_search_reconcile_staging.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search reconcile staging: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062110_search_reconcile_staging.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'2ffc8d71cc716ad67f478324a51c9865e2f2bb73ce326e903bda30c2a56d79e1'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='search reconcile staging: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.search_reconcile_claim_v1(text,text,text,text,text,bigint,text,integer),
 sessions.search_reconcile_stage_batch_v1(bigint,uuid,text,smallint,jsonb,text),
 sessions.search_reconcile_advance_v1(bigint,uuid,text,text,text[],bigint,bigint,boolean),
 sessions.search_reconcile_diff_page_v1(bigint,uuid,text,text,integer,text),
 sessions.search_reconcile_finish_v1(bigint,uuid),
 sessions.search_reconcile_abandon_v1(bigint,uuid,text),
 sessions.search_reconcile_erase_scope_v1(text,text,text,text[]),
 sessions.search_reconcile_lease_v1(bigint,uuid),
 sessions.search_reconcile_caller_v1();
DROP TABLE sessions.search_reconcile_stage_v1;
DROP TABLE sessions.search_reconcile_runs_v1;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062110_search_reconcile_staging.sql';
COMMIT;
