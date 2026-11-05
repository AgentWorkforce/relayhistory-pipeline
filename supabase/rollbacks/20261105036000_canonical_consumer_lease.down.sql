-- Reverse of 20261105036000. Only valid after any LATER migration is rolled
-- back (its rows may reference this schema's state).
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105036000_canonical_consumer_lease.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical consumer lease: a later migration is applied; roll it back first';END IF;
 END IF;
END $rollback_guard$;
DROP TABLE IF EXISTS sessions.canonical_feature_consumer_dead_letter;
DROP TABLE IF EXISTS sessions.canonical_feature_consumer_checkpoint;
DROP TABLE IF EXISTS sessions.canonical_feature_consumer_lease;
DO $ledger$BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105036000_canonical_consumer_lease.sql';
 END IF;END$ledger$;
