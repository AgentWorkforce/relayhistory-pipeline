-- Reverse of 20261105038000 (assigned slot; drafted as provisional 037000). Only
-- valid after any LATER migration is rolled back, and only while no attempt has
-- been retired: a retired attempt's records live only in its S3 archive, and
-- dropping the guards would let a stage intent reach a NULL authority. Fail closed.
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105038000_claim_attempt_records_retirement.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claim records retirement: a later migration is applied; roll it back first';END IF;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='sessions.ingest_claim_attempts'::regclass AND attname='records_retired_at' AND NOT attisdropped) THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts WHERE records_retired_at IS NOT NULL)' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claim records retirement: retired attempts exist; rollback refused';END IF;
 END IF;
END $rollback_guard$;
DROP TRIGGER IF EXISTS ingest_stage_intent_retired_attempt_guard ON sessions.ingest_stage_intents;
DROP TRIGGER IF EXISTS ingest_claim_attempt_records_guard ON sessions.ingest_claim_attempts;
DROP FUNCTION IF EXISTS sessions.ingest_claim_attempt_records_retire(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_claim_attempt_records_candidates(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_claim_attempt_records_terminal(uuid);
DROP FUNCTION IF EXISTS sessions.ingest_claim_attempt_records_key(text,text,text,timestamptz,uuid,text);
DROP FUNCTION IF EXISTS sessions.ingest_stage_intent_retired_attempt_guard();
DROP FUNCTION IF EXISTS sessions.ingest_claim_attempt_records_guard();
ALTER TABLE sessions.ingest_claim_attempts DROP CONSTRAINT IF EXISTS ingest_claim_attempts_records_retired_shape,
 DROP COLUMN IF EXISTS records_retired_at,DROP COLUMN IF EXISTS records_archive_key,
 DROP COLUMN IF EXISTS records_count,DROP COLUMN IF EXISTS records_sha256;
DO $ledger$BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105038000_claim_attempt_records_retirement.sql';
 END IF;END$ledger$;
