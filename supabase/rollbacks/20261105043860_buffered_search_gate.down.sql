-- Down path for supabase/migrations/20261105043860_buffered_search_gate.sql.
-- Drops sessions.buffered_search_gate_for (its grant goes with it). Nothing else is touched: the
-- migration created no table, column, index, role or table grant. Turn the read API gate off
-- first (READ_API_BUFFERED_SEARCH_GATE unset): once the function is gone every gate call fails and
-- every buffered hit is dropped (fail closed). Exercised by
-- src/hoopdb/__tests__/migration-profile.test.ts (up, down, up). Code only: running it against a
-- hosted database needs the user's OK.
--
-- Refused (55000), before anything is dropped, while a later migration is applied (roll it back
-- first), as 20261105043700's down.
--
-- Removes this file's ledger row so the next migrate reapplies it cleanly. No explicit
-- BEGIN/COMMIT, as in the sibling window downs: run it as one transaction
-- (psql -1 -v ON_ERROR_STOP=1).
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105043860_buffered_search_gate.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'buffered search gate: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb);
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043860_buffered_search_gate.sql';
 END IF;
END $ledger$;
