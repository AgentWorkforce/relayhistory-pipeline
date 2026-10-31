-- Privileged selfhosted bootstrap: the migration owner's future functions are not
-- PUBLIC-executable.
--
-- PostgreSQL gives every new function PUBLIC EXECUTE by built-in default, and a
-- per-schema ALTER DEFAULT PRIVILEGES cannot revoke that. This runs, in the HoopDb
-- database:
--   ALTER DEFAULT PRIVILEGES FOR ROLE <migration owner> REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC
-- GLOBAL FOR THAT OWNER: every function the owner creates later in THIS database, in any
-- schema, starts without PUBLIC EXECUTE. That is safe only because a selfhosted HoopDb
-- database is dedicated to HoopDb (the owner owns nothing else). Explicit grants made in
-- migrations (the Worker's RPCs, authenticated's helpers, service_role's grants) are
-- untouched; the ACL manifest test checks it. Never run this on Supabase: there the owner
-- is postgres and the change would reach every schema of the project (an operator
-- decision, activation dossier #98).
--
-- The migrator, in selfhosted and supabase mode, refuses to run unless the catalog shows
-- this control (HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke).
--
-- Idempotent. Run as a superuser (CNPG postgres) or as the owner itself, before migrate:
--   psql -v ON_ERROR_STOP=1 -X -v migration_owner=hoop -d <the HoopDb database> \
--     -f supabase/bootstrap/hoopdb_function_default_revoke.sql
\set ON_ERROR_STOP 1
SELECT set_config('hoopdb.migration_owner', :'migration_owner', false) AS migration_owner;
DO $fn_default$
DECLARE
  v_owner text := current_setting('hoopdb.migration_owner');
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_owner) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('function default bootstrap: role %I does not exist', v_owner);
  END IF;
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC', v_owner);
  IF NOT EXISTS (
    SELECT 1 FROM pg_default_acl d
     WHERE d.defaclrole = (SELECT oid FROM pg_roles WHERE rolname = v_owner)
       AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
       AND NOT EXISTS (SELECT 1 FROM aclexplode(d.defaclacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('function default bootstrap: the revoke for %I did not take effect', v_owner);
  END IF;
END
$fn_default$;
