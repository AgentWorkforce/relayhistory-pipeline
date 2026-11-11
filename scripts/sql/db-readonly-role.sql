-- sessions_readonly_stats, step 1 of 2: the NOLOGIN read-only statistics role.
-- Used by .github/workflows/db-readonly-checks.yml (runbooks/sessions-prod-db-readonly-checks.md).
--
-- OWNER GATE. Applying this file to any real database is Khaliq's own separate go, in his own words
-- (AGENTS.md: credentials, IAM and database changes are outside the agents' standing order). It is NOT a
-- migration and nothing in CI applies it. Run as the migration owner (`postgres`), in one transaction:
--   psql --single-transaction -v ON_ERROR_STOP=1 -f scripts/sql/db-readonly-role.sql
-- Step 2 (LOGIN + password) is scripts/sql/db-readonly-role-login.sql.tmpl, a second, separate go.
-- Rollback: scripts/sql/db-readonly-role-nologin.sql.
--
-- Pattern: the two-phase operator bootstrap of supabase/bootstrap/session_read_api_roles.sql (phase 1,
-- NOLOGIN, idempotent DO block) and supabase/bootstrap/session_read_api_role.sql (phase 2: ALTER ROLE at
-- :55). Kept outside supabase/bootstrap/ on purpose:
-- src/hoopdb/__tests__/rds-activation.test.ts:101-124 requires every role a bootstrap file creates or
-- alters to be part of the RDS activation spec, and this role is a Supabase-only operator tool.
--
-- Idempotent: re-running converges on the same grants and never turns LOGIN on or off.
--
-- What the role can do: read statistics and catalog views through membership of pg_read_all_stats and
-- pg_monitor, nothing else. It has NO privilege on any sessions table (not even SELECT on a column), no
-- schema USAGE on sessions, no RLS policy, and no function EXECUTE; every session defaults to a read-only
-- transaction. The only other grant is USAGE on schema `extensions` when pg_stat_statements lives there
-- (schema lookup only; pg_read_all_stats makes every user's statement rows visible). Counts that need table
-- data arrive later through a count-only SECURITY DEFINER function in a separate, future migration.
--
-- INHERIT (not the NOINHERIT of the runtime roles) is deliberate: the statistics privileges arrive only
-- through membership of pg_read_all_stats/pg_monitor, and the final assertion below refuses any other
-- membership. The runner re-checks the same thing before every run.

DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'sessions_readonly_stats') THEN
    CREATE ROLE sessions_readonly_stats NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$role$;

ALTER ROLE sessions_readonly_stats SET default_transaction_read_only = on;
ALTER ROLE sessions_readonly_stats SET statement_timeout = '15s';
ALTER ROLE sessions_readonly_stats SET lock_timeout = '2s';
ALTER ROLE sessions_readonly_stats SET idle_in_transaction_session_timeout = '60s';

GRANT pg_read_all_stats TO sessions_readonly_stats;
GRANT pg_monitor TO sessions_readonly_stats;

-- pg_stat_statements lives in schema `extensions` on Supabase; grant schema USAGE only when it is there.
DO $pgss$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
              WHERE e.extname = 'pg_stat_statements' AND n.nspname = 'extensions') THEN
    GRANT USAGE ON SCHEMA extensions TO sessions_readonly_stats;
  END IF;
END
$pgss$;

-- Fail closed (and roll back the transaction) unless the role is exactly what the runner expects.
DO $assert$
DECLARE
  v_role oid := (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'sessions_readonly_stats');
  v_bad text;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.oid = v_role
              AND (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'sessions_readonly_stats: elevated role attribute';
  END IF;
  SELECT string_agg(g.rolname, ',') INTO v_bad
    FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles g ON g.oid = m.roleid
   WHERE m.member = v_role AND g.rolname NOT IN ('pg_read_all_stats', 'pg_monitor');
  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'sessions_readonly_stats: unexpected membership ' || v_bad;
  END IF;
  SELECT string_agg(c.oid::regclass::text, ',') INTO v_bad
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm')
     AND (pg_catalog.has_table_privilege(v_role, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
          OR pg_catalog.has_any_column_privilege(v_role, c.oid, 'INSERT, UPDATE, REFERENCES'));
  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'sessions_readonly_stats: write privilege on ' || v_bad;
  END IF;
  -- Stats-only: no SELECT on any sessions table or column either (counts come from a future function).
  SELECT string_agg(c.oid::regclass::text, ',') INTO v_bad
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm')
     AND (pg_catalog.has_table_privilege(v_role, c.oid, 'SELECT')
          OR pg_catalog.has_any_column_privilege(v_role, c.oid, 'SELECT'));
  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'sessions_readonly_stats: SELECT privilege on ' || v_bad;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_policy p WHERE v_role = ANY (p.polroles)) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'sessions_readonly_stats: has an RLS policy';
  END IF;
END
$assert$;
