-- Down path for supabase/migrations/20261105034000_session_directory_read_api.sql.
-- Additive up (three functions, no data, no role), so the down only drops them. The
-- bulls96_read_api role stays: session_read_api_role.sql owns it. Exercised by
-- src/hoopdb/__tests__/session-directory-read-api.test.ts (up, down, up).
-- Code only: running it against a hosted database needs the user's OK.
--
-- Like 20261105026000's named down, it removes this file's ledger row so the next migrate
-- reapplies it cleanly (no function_acl_drift), which is only possible while it is the LAST
-- applied upstream file (else sequencePreflight would see a ledger gap): refused (55000)
-- otherwise.

-- No explicit BEGIN/COMMIT, as in the sibling window downs: the guard runs first and refuses
-- before anything is dropped (run it as one transaction, e.g. psql -1 -v ON_ERROR_STOP=1).
DO $rollback_guard$ DECLARE v_later boolean; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C" > ''upstream:20261105034000_session_directory_read_api.sql'')' INTO v_later;
  IF v_later THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'read api directory: a later migration is applied; roll it back first'; END IF;
 END IF;
END $rollback_guard$;
DROP FUNCTION IF EXISTS sessions.session_directory_activity_for(text, timestamptz, timestamptz);
DROP FUNCTION IF EXISTS sessions.list_session_directory_for(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer);
DROP FUNCTION IF EXISTS sessions.session_directory_read_api_scope(text);
DO $ledger$ BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
  DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105034000_session_directory_read_api.sql';
 END IF;
END $ledger$;
