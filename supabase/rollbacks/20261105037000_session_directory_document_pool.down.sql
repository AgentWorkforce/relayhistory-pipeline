-- Reverse of 20261105037000: only valid after any LATER migration is rolled
-- back; narrows the pool CHECK back to the signed-off two-pool contract and
-- drops the v3 finish (a document-pool ACK row must not exist when this runs).
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105037000_session_directory_document_pool.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='directory document pool: a later migration is applied; roll it back first';END IF;
 END IF;
END $rollback_guard$;
DO $no_doc$
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.canonical_feature_page_ack WHERE pool='document')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='directory document pool: document ACK rows exist; clear them first';END IF;
END $no_doc$;
DROP FUNCTION IF EXISTS sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text);
ALTER TABLE sessions.canonical_feature_page_ack
  DROP CONSTRAINT IF EXISTS canonical_feature_page_ack_pool_check;
ALTER TABLE sessions.canonical_feature_page_ack
  ADD CONSTRAINT canonical_feature_page_ack_pool_check
  CHECK (pool IN ('event','brief'));
DO $ledger$BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105037000_session_directory_document_pool.sql';
 END IF;END$ledger$;
