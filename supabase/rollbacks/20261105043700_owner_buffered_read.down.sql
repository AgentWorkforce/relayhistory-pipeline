-- Down path for supabase/migrations/20261105043700_owner_buffered_read.sql.
-- Drops the four owner buffered read functions (their grants go with them) and the
-- sessions.owner_buffered_parity proof table (with any owner-written proof rows; they are
-- recomputable and a reapply starts with none, which hides everything). Nothing else is touched:
-- the migration created no other table, no column, index, role or table grant. Revert the
-- read API code first (flag RELAYHISTORY_READ_OWNER_BUFFERED off): once the functions are gone
-- the buffered list/detail calls fail. Exercised by src/hoopdb/__tests__/migration-profile.test.ts
-- (up, down, up). Code only: running it against a hosted database needs the user's OK.
--
-- Refused (55000), before anything is dropped, while a later migration is applied (roll it
-- back first), as 20261105034000's and 20261105043000's downs.
--
-- Removes this file's ledger row so the next migrate reapplies it cleanly. No explicit
-- BEGIN/COMMIT, as in the sibling window downs: run it as one transaction
-- (psql -1 -v ON_ERROR_STOP=1).
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105043700_owner_buffered_read.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'owner buffered read: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.buffered_session_stages_for(text, text, text, text, text, text, text, text, boolean, text);
DROP FUNCTION IF EXISTS sessions.buffered_sessions_for(text, timestamptz, text, text, integer, text, text, text, text, text, boolean, text);
DROP FUNCTION IF EXISTS sessions.owner_buffered_read_scope(text, text, text, text, text, text, boolean, text);
DROP FUNCTION IF EXISTS sessions.owner_buffered_parity_expected(text, text, text, text, bigint, text, text);
DROP TABLE IF EXISTS sessions.owner_buffered_parity;
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043700_owner_buffered_read.sql';
 END IF;
END $ledger$;
