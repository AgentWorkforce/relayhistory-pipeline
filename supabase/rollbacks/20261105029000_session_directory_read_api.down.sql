-- Down path for supabase/migrations/20261105029000_session_directory_read_api.sql.
-- Additive up (three functions, no data, no role), so the down only drops them. The
-- bulls96_read_api role stays: session_read_api_role.sql owns it. Exercised by
-- src/hoopdb/__tests__/session-directory-read-api.test.ts (up, down, up).
-- Code only: running it against a hosted database needs the user's OK.

BEGIN;
DROP FUNCTION IF EXISTS sessions.session_directory_activity_for(text, timestamptz, timestamptz);
DROP FUNCTION IF EXISTS sessions.list_session_directory_for(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer);
DROP FUNCTION IF EXISTS sessions.session_directory_read_api_scope(text);
COMMIT;
