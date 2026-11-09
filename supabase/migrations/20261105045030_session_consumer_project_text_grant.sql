-- Session pipeline consumer grant (045030): EXECUTE on
-- sessions.session_directory_project_text(text, text, text) for the self-host
-- writer (search_consumer_runtime).
--
-- Why: the consumer writes sessions.session_directory directly. The directory
-- projector's upsert (src/hoopdb/session-directory-projector.ts:98, INSERT ...
-- ON CONFLICT DO UPDATE) and its UPDATE (:181) run as search_consumer_runtime,
-- and so does the session_directory_events_changed invoker trigger that
-- re-touches the row after a pointer upsert. The partial expression index
-- session_directory_tenant_project_text_trgm_idx (20261101050000) indexes
-- sessions.session_directory_project_text(project_id, project_key, git_repo)
-- for every live row with a project id, project key or repository. PostgreSQL
-- evaluates an index expression with the privileges of the user running the
-- INSERT/UPDATE, not the index or table owner, so every such write that adds
-- an index entry needs EXECUTE on the function. It was granted only to the
-- owner and service_role, and 043900's twelve consumer grants missed it, so
-- these writes failed with 42501 (permission denied for function
-- session_directory_project_text). The function is IMMUTABLE LANGUAGE sql with
-- a pinned search_path and only concatenates its arguments, so the grant
-- exposes nothing.
--
-- Sweep: every other index, constraint, default, generated column, policy and
-- invoker trigger body on the tables the consumer inserts into or updates was
-- checked for sessions.* calls; this is the only one not already granted.
--
-- The role must exist first, exactly as for 043900: the migrate bootstrap step
-- (supabase/bootstrap/session_consumer_roles.sql) provisions it wherever the
-- migrator may, and on Cloud SQL / RDS / Supabase the operator provisions it
-- before migrate runs. A missing role fails closed (55000) instead of skipping,
-- because a skipped step would be ledgered and never self-repair.
--
-- Source of truth: src/hoopdb/sessions-function-acl.json and its Supabase
-- copy in hoopdb_sessions_function_boundary_supabase.sql (generated through the
-- consumer-project-text-grant lane). GRANT is idempotent, so reruns converge.
DO $consumer_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'search_consumer_runtime') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'session consumer project text grant: bare role search_consumer_runtime is absent; provision it (migrate bootstrap step, Cloud SQL / RDS trampoline, or Supabase runbook) and rerun';
  END IF;
  GRANT EXECUTE ON FUNCTION sessions.session_directory_project_text(text, text, text) TO search_consumer_runtime;
END
$consumer_grants$;
