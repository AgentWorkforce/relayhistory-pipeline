-- Cloud SQL pre-step for session_directory_search_worker on PostgreSQL 16+.
--
-- A customer Cloud SQL admin has CREATEROLE but not SUPERUSER. PostgreSQL grants a
-- CREATEROLE creator ADMIN membership in a newly created role, so this block creates the
-- fixed NOLOGIN role and immediately revokes that creator membership in one transaction.
-- Run the canonical session_directory_search_worker.sql verifier immediately afterward.
-- The migration Job sets HOOPDB_ROLE_BOOTSTRAP=false and never performs this privileged
-- provisioning itself.
DO $cloudsql_search_worker$
DECLARE
  v_me record;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_directory_search_worker') THEN
    RETURN;
  END IF;
  SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
  IF v_me.rolsuper OR NOT v_me.rolcreaterole THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'Cloud SQL search Worker bootstrap requires a non-superuser CREATEROLE migration owner';
  END IF;
  IF NOT pg_has_role(current_user, 'cloudsqlsuperuser', 'MEMBER') THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'Cloud SQL search Worker bootstrap requires cloudsqlsuperuser membership';
  END IF;
  CREATE ROLE session_directory_search_worker
    NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
  -- PostgreSQL 16 records the creator membership with the creator as grantor. Name the
  -- grantor explicitly so an ambient SET ROLE/session authorization cannot cause a
  -- different administrator's grant to be targeted.
  EXECUTE format(
    'REVOKE session_directory_search_worker FROM %I GRANTED BY %I',
    current_user,
    current_user
  );
  IF pg_has_role(current_user, 'session_directory_search_worker', 'MEMBER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'Cloud SQL search Worker bootstrap could not remove creator membership';
  END IF;
END
$cloudsql_search_worker$;
