-- Session pipeline consumer function grants (B2): the twelve EXECUTE grants the
-- self-host writer (search_consumer_runtime) needs for the composed tick path
-- (pointer upsert through the publication-authority retry, directory
-- projection and catalog write, repo catalog, publication lock). On a real
-- migrate the bare role always exists first: the migrate bootstrap step
-- (supabase/bootstrap/session_consumer_roles.sql) provisions it wherever the
-- migrator may, and on Cloud SQL / RDS / Supabase the operator provisions it
-- before migrate runs (trampoline bootstrap or runbook). A missing role fails
-- closed here instead of skipping: the migrator would otherwise ledger this
-- step without installing anything, and provisioning the role later would not
-- self-repair (the step is already ledgered). Hand-rolled test fixtures that
-- loop the migration chain must run the lane bootstrap role file first, like
-- the migrator plan does.
--
-- Source of truth: src/hoopdb/sessions-function-acl.json and its Supabase
-- copy in hoopdb_sessions_function_boundary_supabase.sql. GRANT is
-- idempotent, so reruns converge; nothing here is revoked.
DO $consumer_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'search_consumer_runtime') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'session consumer grants: bare role search_consumer_runtime is absent; provision it (migrate bootstrap step, Cloud SQL / RDS trampoline, or Supabase runbook) and rerun';
  END IF;
  GRANT EXECUTE ON FUNCTION sessions.upsert_session_index(jsonb) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.session_index_batch(jsonb) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.session_publication_is_managed(text, text, text, text, text) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.session_publication_lock(jsonb) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.applied_repo_evidence(jsonb, jsonb) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.canonical_remote(text) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.clean_root_commit(text) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.lock_repo_catalog(text) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.resolve_repo(text, text, text, timestamp with time zone) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.repo_live(text, uuid) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.legacy_remote(text) TO search_consumer_runtime;
  GRANT EXECUTE ON FUNCTION sessions.session_directory_search_text(text, text, text, text, text, text, text, text) TO search_consumer_runtime;
END
$consumer_grants$;
