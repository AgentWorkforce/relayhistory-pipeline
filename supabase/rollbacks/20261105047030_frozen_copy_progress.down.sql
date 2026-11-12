-- Drops the 20261105047030 receipts privacy guard trigger, the owner-only frozen-copy progress table and its ledger row.
-- The progress rows are only the copy operator's resume cursor (page inserts are ON CONFLICT DO NOTHING); without the
-- table the operator refuses frozen_copy_progress_table_missing. Copied frozen rows and receipts are retained.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen copy progress: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105047030_frozen_copy_progress.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen copy progress: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105047030_frozen_copy_progress.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'95cca210dd449c8dab40169fafdba7d0cf2d0e84d925b0d576b80cfa145e86cd'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen copy progress: own applied checksum required';END IF;
END $rollback_guard$;
DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_receipts;
DROP TABLE sessions.__96bulls_frozen_copy_progress;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105047030_frozen_copy_progress.sql';
COMMIT;
