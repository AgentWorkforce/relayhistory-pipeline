-- Explicit one-time operator prerequisite for a non-superuser consumer owner.
-- Run as a PostgreSQL superuser in the selected database, AFTER both schemas
-- and the owner's bare consumer role exist. Never run from a migration or app.
-- This grants no catalog SELECT. Only the common schemas' CREATEROLE owner can
-- ask one boolean about one fixed role. No password/hash ever leaves the helper.
-- A pre-existing schema refuses: inspect it; this file never overwrites code.
BEGIN;
SET LOCAL search_path=pg_catalog;
DO $install$
DECLARE owner_oid oid;
BEGIN
  IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user)
     OR session_user <> current_user OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='consumer credential probe: direct superuser operator required';
  END IF;
  SELECT nspowner INTO owner_oid FROM pg_catalog.pg_namespace WHERE nspname='sessions';
  IF owner_oid IS NULL OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname='search_index' AND nspowner=owner_oid)
     OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE oid=owner_oid AND rolcreaterole AND NOT rolsuper AND NOT rolbypassrls)
     OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='search_consumer_runtime' AND NOT rolcanlogin) THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='consumer credential probe: common non-superuser CREATEROLE owner and bare consumer required';
  END IF;
END
$install$;
CREATE SCHEMA session_consumer_bootstrap_private;
REVOKE ALL ON SCHEMA session_consumer_bootstrap_private FROM PUBLIC;
CREATE FUNCTION session_consumer_bootstrap_private.consumer_has_password()
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog
AS $credential_probe$
DECLARE caller_oid oid; target_oid oid; target_login boolean; present boolean;
BEGIN
  SELECT oid INTO caller_oid FROM pg_catalog.pg_roles
    WHERE rolname=session_user AND rolcreaterole AND NOT rolsuper AND NOT rolbypassrls;
  IF caller_oid IS NULL OR current_setting('role') <> 'none'
     OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=pg_backend_pid() AND usename=session_user)
     OR (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname IN ('sessions','search_index') AND nspowner=caller_oid) <> 2 THEN
    RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='consumer credential probe: caller refused';
  END IF;
  SELECT oid,rolcanlogin,rolpassword IS NOT NULL INTO target_oid,target_login,present
    FROM pg_catalog.pg_authid WHERE rolname='search_consumer_runtime';
  IF target_oid IS NULL OR target_login THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='consumer credential probe: bare fixed role required';
  END IF;
  IF current_setting('server_version_num')::integer >= 160000 THEN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid=target_oid AND member=caller_oid
                   AND admin_option AND NOT inherit_option AND NOT set_option) THEN
      RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='consumer credential probe: creator authority required';
    END IF;
  ELSE
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid=target_oid AND member=caller_oid AND admin_option) THEN
      RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='consumer credential probe: creator authority required';
    END IF;
  END IF;
  RETURN present;
END
$credential_probe$;
REVOKE ALL ON FUNCTION session_consumer_bootstrap_private.consumer_has_password() FROM PUBLIC;
DO $grant$
DECLARE owner_name name;
BEGIN
  SELECT r.rolname INTO STRICT owner_name FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles r ON r.oid=n.nspowner WHERE n.nspname='sessions';
  EXECUTE format('GRANT USAGE ON SCHEMA session_consumer_bootstrap_private TO %I',owner_name);
  EXECUTE format('GRANT EXECUTE ON FUNCTION session_consumer_bootstrap_private.consumer_has_password() TO %I',owner_name);
END
$grant$;
COMMIT;
