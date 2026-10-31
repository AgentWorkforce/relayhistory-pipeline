-- Privileged bootstrap for the dedicated HoopDb runtime role (selfhosted mode).
--
-- The services connect as this role (HOOPDB_RUNTIME_ROLE). In selfhosted and supabase
-- mode it must be a dedicated, non-owner role; the migrator validates its exact
-- least-privilege allowlist before any write and grants it only USAGE on schema
-- sessions and SELECT on the ledger and readiness marker (src/hoopdb/migrate-guards.ts).
--
--   selfhosted (CNPG, Cloud SQL)  this file, run by the deploy as a superuser
--                                 (CNPG `postgres`), before migrate, every deploy.
--   supabase                      pre-provisioned by an operator and verified by the
--                                 migrator; see the runbook (a PostgreSQL 16+ project
--                                 has the same creator-ADMIN caveat as the Worker).
--   local                         not needed: the Docker stack runs owner-as-runtime.
--
-- It creates the role, if absent, with NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
-- NOBYPASSRLS and LOGIN or NOLOGIN as configured, and grants it nothing. It never sets a
-- password: the credential is provisioned separately (secret manager), never on disk. If
-- the role exists it only verifies those attributes, a member of no role and no owned
-- objects (55000 otherwise, nothing changed). The migrator checks the rest.
--
-- Database ACL (run it connected to the HoopDb database itself):
--   REVOKE TEMPORARY ON DATABASE <this database> FROM PUBLIC, and
--   GRANT CONNECT ON DATABASE <this database> TO <runtime role>.
-- The migrator refuses a runtime role with effective TEMPORARY, and PUBLIC holds
-- TEMPORARY by default, so revoking it from the role alone is not enough.
-- DATABASE-WIDE SIDE EFFECT: every role in this database that relied on PUBLIC for
-- TEMPORARY loses it (the database owner and superusers keep it). No HoopDb service
-- uses temporary objects (docs/evidence/runtime-temp-usage.md). Grant TEMPORARY
-- explicitly to any other role that needs it. Supabase mode never runs this: the
-- operator owns that database's ACL, and the migrator only detects and refuses.
--
-- Usage:
--   psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -v runtime_login=true \
--     -d <the HoopDb database> -f supabase/bootstrap/hoopdb_runtime_role.sql
\set ON_ERROR_STOP 1
SELECT set_config('hoopdb.runtime_role', :'runtime_role', false) AS runtime_role,
       set_config('hoopdb.runtime_login', :'runtime_login', false) AS runtime_login;
DO $runtime$
DECLARE
  v_role text := current_setting('hoopdb.runtime_role');
  v_login_text text := lower(current_setting('hoopdb.runtime_login'));
  v_login boolean;
  v_me record;
  v_row record;
  v_problems text[] := '{}';
BEGIN
  IF v_role !~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$' OR v_role = 'session_directory_search_worker' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('runtime role bootstrap: invalid role name %L', v_role);
  END IF;
  IF v_login_text NOT IN ('true', 'false') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'runtime role bootstrap: runtime_login must be true or false';
  END IF;
  v_login := v_login_text = 'true';
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
    SELECT rolsuper INTO v_me FROM pg_roles WHERE rolname = current_user;
    IF NOT v_me.rolsuper THEN
      RAISE EXCEPTION USING ERRCODE = '42501',
        MESSAGE = format('runtime role bootstrap: %I may not create %I; run as a superuser', current_user, v_role),
        HINT = 'A non-superuser CREATEROLE creator is granted ADMIN OPTION on the new role (PostgreSQL 16+), which the migrator refuses.';
    END IF;
    EXECUTE format('CREATE ROLE %I %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
                   v_role, CASE WHEN v_login THEN 'LOGIN' ELSE 'NOLOGIN' END);
  END IF;
  SELECT * INTO v_row FROM pg_roles WHERE rolname = v_role;
  IF v_row.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF v_row.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF v_row.rolcanlogin <> v_login THEN
    v_problems := array_append(v_problems, format('LOGIN is %s, configured %s', v_row.rolcanlogin, v_login)::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = v_row.oid) THEN
    v_problems := array_append(v_problems, 'a member of another role'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_shdepend WHERE refclassid = 'pg_authid'::regclass AND refobjid = v_row.oid AND deptype = 'o') THEN
    v_problems := array_append(v_problems, 'owns objects'::text);
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('runtime role bootstrap: %I does not match the spec: %s', v_role, array_to_string(v_problems, ', ')),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
END
$runtime$;
DO $db$
BEGIN
  EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), current_setting('hoopdb.runtime_role'));
END
$db$;
