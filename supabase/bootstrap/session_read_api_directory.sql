-- The session read API's directory search grant (src/session-read-api/directory.ts,
-- GET /v1/directory/search). Applied on top of session_read_api_role.sql.
--
-- The route searches the logical-session directory of the token's workspace and returns
-- pointers only (session_id, workspace_id, title, git_repo, last_activity_at, state). So
-- the role gains column SELECT on exactly the eleven session_directory columns the
-- statement names, and nothing else:
--
--   tenant / key   org_id, workspace_id, harness, session_id
--   returned       title, git_repo, last_activity_at, state
--   matched only   task_title, search_text
--   liveness       deleted_at
--
-- search_text is a WHERE-only column, but PostgreSQL has no "filter without read"
-- privilege: the role can read it. search_text is the metadata search document
-- (sessions.session_directory_search_text: title, task title, agent, project, repository,
-- branch, model and the scrubbed short_summary, at most 8192 characters); it never holds
-- transcript or event content. The role still cannot read short_summary itself, the
-- token or cost counters, machine_ids, user_ids, or any catalog column.
--
--   RLS     session_directory already has RLS on. The policy below admits only rows of
--           the org and workspace the current transaction bound with
--           set_config('bulls96_read.org_id' / 'bulls96_read.workspace_id', ..., true),
--           the same binding as session_index_read_api. A setting never set is NULL, and
--           after a transaction-local set_config ends it reads as ''; neither matches a
--           row (org_id and workspace_id are never NULL or '').
--
-- No function, no EXECUTE, no role attribute change: the HoopDb sessions function
-- boundary event trigger fires on GRANT but checks only routines (a table GRANT touches
-- none), and CREATE POLICY is not one of its tags.
--
-- Idempotent. Run as `postgres` (Supabase's non-superuser privileged role; nothing here
-- names a role attribute), after session_read_api_role.sql, then verify:
--
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_directory.sql
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_role_verify.sql
--
-- Down path (restores the phase A shape; the verify then reports the missing grant):
--   BEGIN;
--   DROP POLICY IF EXISTS session_directory_read_api ON sessions.session_directory;
--   REVOKE SELECT ON sessions.session_directory FROM bulls96_read_api;
--   COMMIT;
\set ON_ERROR_STOP 1
BEGIN;
DO $preflight$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = 'session read api directory grant: bulls96_read_api does not exist; run session_read_api_role.sql first';
  END IF;
END
$preflight$;

-- A table-level REVOKE also removes every column privilege, so a re-run converges on
-- exactly this column set even if an earlier, wider grant was made by hand.
REVOKE ALL ON sessions.session_directory FROM bulls96_read_api;
GRANT SELECT (org_id, workspace_id, harness, session_id, title, task_title, git_repo,
              search_text, last_activity_at, state, deleted_at)
  ON sessions.session_directory TO bulls96_read_api;

DROP POLICY IF EXISTS session_directory_read_api ON sessions.session_directory;
CREATE POLICY session_directory_read_api ON sessions.session_directory
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (
    org_id = current_setting('bulls96_read.org_id', true)
    AND workspace_id = current_setting('bulls96_read.workspace_id', true)
  );
COMMIT;
