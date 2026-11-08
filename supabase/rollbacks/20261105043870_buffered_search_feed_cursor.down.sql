-- Down path for supabase/migrations/20261105043870_buffered_search_feed_cursor.sql.
-- Drops the three buffered search feed functions (their grants go with them) and the
-- sessions.buffered_search_feed_cursors table (with its cursor positions; a reapply starts every
-- cursor again from its `since`, and the feeder's re-inserted rows are byte-identical, so
-- ClickHouse folds them), and revokes the schema USAGE the migration granted buffered_search_feed_runtime
-- (the role itself is KEPT, as 20261105043850's down keeps its installer and every lane keeps its cluster
-- roles). Nothing else is touched: the migration created no other table, no column, index, role or table
-- grant. Turn the feed off first (INGEST_BUFFERED_SEARCH_FEED unset or off):
-- once the functions are gone every feed call fails and the feed indexes nothing. Exercised by
-- src/hoopdb/__tests__/migration-profile.test.ts (up, down, up). Code only: running it against a
-- hosted database needs the user's OK.
--
-- Refused (55000), before anything is dropped, while a later migration is applied (roll it back
-- first), as 20261105043860's down.
--
-- Removes this file's ledger row so the next migrate reapplies it cleanly. No explicit
-- BEGIN/COMMIT, as in the sibling window downs: run it as one transaction
-- (psql -1 -v ON_ERROR_STOP=1).
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105043870_buffered_search_feed_cursor.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'buffered search feed: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.buffered_search_feed_advance(jsonb);
DROP FUNCTION IF EXISTS sessions.buffered_search_feed_stage(jsonb);
DROP FUNCTION IF EXISTS sessions.buffered_search_feed_page(jsonb);
DROP TABLE IF EXISTS sessions.buffered_search_feed_cursors;
-- The schema USAGE this migration granted the runtime role goes with it (the role itself is kept).
REVOKE USAGE ON SCHEMA sessions FROM buffered_search_feed_runtime;
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105043870_buffered_search_feed_cursor.sql';
 END IF;
END $ledger$;
