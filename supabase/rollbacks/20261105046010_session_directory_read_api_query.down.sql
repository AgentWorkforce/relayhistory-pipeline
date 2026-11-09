-- Down path for supabase/migrations/20261105046010_session_directory_read_api_query.sql.
-- Drops the five functions it adds; no table, index or role changes. The read API answers
-- `q` with 501 directory_not_migrated once they are gone (READ_API_SESSION_QUERY=true) and
-- 501 param_q with the flag off, as before.
--
-- Like 20261105045040's named down, it removes this file's ledger row so the next migrate
-- reapplies it cleanly, which is only possible while it is the LAST applied upstream file:
-- refused (55000) otherwise. Run as one transaction (psql -1 -v ON_ERROR_STOP=1).
-- Code only: running it against a hosted database needs Khaliq's OK.
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105046010_session_directory_read_api_query.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session directory query: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.query_session_directory_activity_for(
  text, timestamptz, timestamptz, text[], text[], text[], text[], text[], text[], text[], text[], text[],
  text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz,
  timestamptz, timestamptz);
DROP FUNCTION IF EXISTS sessions.query_session_directory_for(
  text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  timestamptz, text, text, integer);
DROP FUNCTION IF EXISTS sessions.session_directory_query_where(
  integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, integer,
  boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean);
DROP FUNCTION IF EXISTS sessions.session_directory_query_like(text[]);
DROP FUNCTION IF EXISTS sessions.session_directory_query_check(
  text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz);
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105046010_session_directory_read_api_query.sql';
 END IF;
END $ledger$;
