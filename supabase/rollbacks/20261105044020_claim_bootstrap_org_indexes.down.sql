-- Drops the eight claim-bootstrap <table>_org_id_idx indexes of 20261105044020 and its ledger row.
-- LOCAL/TEST ONLY. There are no down-migrations on real databases. On production these indexes are the
-- operator-built ones (CREATE INDEX CONCURRENTLY, adopted by the migration), so running this there would
-- drop the operator's indexes and put the org-scoped claim/bootstrap queries back on sequential scans; it
-- must never run against a real database without the operator's explicit word. DROP INDEX here is not
-- CONCURRENTLY (it cannot be inside this transaction) and takes an ACCESS EXCLUSIVE lock on each table.
-- Explicit transaction: guard, drops and ledger removal are atomic. On an error the caller must
-- ROLLBACK (or disconnect); nothing is committed.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claim bootstrap org indexes: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105044020_claim_bootstrap_org_indexes.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claim bootstrap org indexes: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105044020_claim_bootstrap_org_indexes.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'35787e1db39a74dbc0d3520efac2f1bb920d8e559edc78292649cfddb09a7a25'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claim bootstrap org indexes: own applied checksum required';END IF;
END $rollback_guard$;
-- Reverse build order; only the eight declared names.
DROP INDEX IF EXISTS sessions.ingest_lazy_native_slots_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_custody_records_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_stage_intents_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_publication_generations_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_publication_attempts_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_seed_captures_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_history_plans_org_id_idx;
DROP INDEX IF EXISTS sessions.ingest_history_mask_pages_org_id_idx;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105044020_claim_bootstrap_org_indexes.sql';
COMMIT;
