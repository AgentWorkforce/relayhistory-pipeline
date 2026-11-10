-- Amazon RDS for PostgreSQL entry point for the dedicated HoopDb runtime role
-- (HOOPDB_DB_MODE=rds; RDS for PostgreSQL only, NOT Aurora).
--
-- MASTER-OWNER LAYOUT ONLY (HOOPDB_RDS_LAYOUT unset or master-owner). Not for the
-- infra #107 separate-database layout (hoopdb / hoop / sessions_pipeline_runtime),
-- whose RDS master is provisioning-only. There, rds-activation/v1
-- (src/hoopdb/rds-activation.ts, supabase/bootstrap/rds_activation/,
-- docs/rds-activation-executor.md) creates the runtime role through a
-- transaction-scoped executor that it then drops. So no master creator edge survives,
-- and the password rotation path below does not exist there: each role rotates its
-- own password (rotate-self). The rationale below keeps the master's ADMIN edge on
-- purpose, and applies to the master-owner layout only.
--
-- The selfhosted hoopdb_runtime_role.sql refuses a non-superuser creator (42501), and
-- the RDS master is not a superuser. Unlike the fixed NOLOGIN roles
-- (rds_migration_roles.sql), the runtime LOGIN role must stay manageable by the master
-- so its password can be set and rotated through the approved operator path, with
-- 1Password as the system of record. So the master creates
-- it directly, and PostgreSQL 16+ records the unavoidable creator edge:
--   member = the migration owner, grantor = rdsadmin (SUPERUSER),
--   ADMIN true, INHERIT false, SET false.
-- That edge gives the owner no runtime privilege and the runtime role nothing; the
-- migrator accepts exactly that shape, only in rds mode, only with the connected owner
-- as member (isRdsCreatorEdge in src/hoopdb/migrate-guards.ts).
--
-- Same spec as the selfhosted file otherwise: NOSUPERUSER NOCREATEDB NOCREATEROLE
-- NOREPLICATION NOBYPASSRLS and LOGIN/NOLOGIN as configured, a member of no role, no
-- owned objects, no member except that exact creator edge. No workload role: on RDS its
-- edge would be granted by the non-superuser master, which the migrator refuses
-- (HOOPDB_RUNTIME_WORKLOAD_ROLE is refused in rds mode). It never sets a password: the
-- credential is provisioned separately from 1Password, never on disk. An existing role
-- is only verified (55000 on any difference, nothing changed).
--
-- Database ACL, as in selfhosted: REVOKE TEMPORARY ON DATABASE <this database> FROM
-- PUBLIC and GRANT CONNECT to the runtime role, with a postcondition (a non-owner REVOKE
-- is only a WARNING in PostgreSQL, so the owner must own this database).
-- DATABASE-WIDE SIDE EFFECT: as in selfhosted, every role that relied on PUBLIC for
-- TEMPORARY in this database loses it.
--
-- One transaction: any refusal rolls back role creation and the database ACL together.
-- Run as the migration owner (the RDS master) named explicitly, connected to the HoopDb
-- database, after rds_migration_roles.sql and before migrate:
--   psql -v ON_ERROR_STOP=1 -X -v migration_owner=<the RDS master> \
--     -v runtime_role=bulls96_runtime -v runtime_login=true \
--     -d <the HoopDb database> -f supabase/bootstrap/hoopdb_runtime_role_rds.sql
\set ON_ERROR_STOP 1
BEGIN;
SELECT set_config('hoopdb.db_mode', 'rds', true) AS db_mode,
       set_config('hoopdb.migration_owner', :'migration_owner', true) AS migration_owner,
       set_config('hoopdb.runtime_role', :'runtime_role', true) AS runtime_role,
       set_config('hoopdb.runtime_login', :'runtime_login', true) AS runtime_login;
DO $runtime_rds$
DECLARE
  v_owner text := current_setting('hoopdb.migration_owner');
  v_role text := current_setting('hoopdb.runtime_role');
  v_login_text text := lower(current_setting('hoopdb.runtime_login'));
  v_login boolean;
  v_me record;
  v_oid oid;
  v_row record;
  v_problems text[] := '{}';
  v_version int := current_setting('server_version_num')::int;
  v_public_temp boolean;
BEGIN
  IF v_version < 160000 THEN
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'RDS runtime role bootstrap requires PostgreSQL 16 or later';
  END IF;
  IF v_owner IS DISTINCT FROM current_user::text THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = format('RDS runtime role bootstrap: run as the migration owner %I (connected as %I)', v_owner, current_user);
  END IF;
  SELECT * INTO v_me FROM pg_roles WHERE rolname = current_user;
  IF v_me.rolsuper OR NOT v_me.rolcreaterole
     OR NOT EXISTS (SELECT 1 FROM pg_roles r
                     WHERE r.rolname = 'rds_superuser' AND pg_has_role(v_me.oid, r.oid, 'MEMBER'))
     OR NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rdsadmin' AND rolsuper AND oid <> v_me.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'RDS runtime role bootstrap requires a non-superuser CREATEROLE rds_superuser member and the fixed rdsadmin SUPERUSER';
  END IF;
  IF v_role !~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$'
     OR v_role IN ('session_directory_search_worker', 'session_token_verifier',
                   'bulls96_session_ingest', 'bulls96_session_ingest_seeder',
                   'bulls96_canonical_feature_producer', 'bulls96_read_api',
                   'bulls96_project_events', 'bulls96_privacy_eraser',
                   'bulls96_babysitter_retention', 'bulls96_acquisition_installer',
                   'buffered_search_feed_runtime',
                   'bulls96_rds_role_seed',
                   'anon', 'authenticated', 'service_role', 'rdsadmin', 'rds_superuser')
     OR v_role = v_owner THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('RDS runtime role bootstrap: invalid role name %L', v_role);
  END IF;
  IF v_login_text NOT IN ('true', 'false') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'RDS runtime role bootstrap: runtime_login must be true or false';
  END IF;
  v_login := v_login_text = 'true';
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
    EXECUTE format('CREATE ROLE %I %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
                   v_role, CASE WHEN v_login THEN 'LOGIN' ELSE 'NOLOGIN' END);
  END IF;
  SELECT * INTO v_row FROM pg_roles WHERE rolname = v_role;
  v_oid := v_row.oid;
  IF v_row.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF v_row.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF v_row.rolcanlogin <> v_login THEN
    v_problems := array_append(v_problems, format('LOGIN is %s, configured %s', v_row.rolcanlogin, v_login)::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = v_oid) THEN
    v_problems := array_append(v_problems, 'a member of another role'::text);
  END IF;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT member.rolname AS member_name, grantor.rolname AS grantor_name,
              grantor.rolsuper AS grantor_super, m.admin_option,
              m.inherit_option, m.set_option
         FROM pg_auth_members m
         JOIN pg_roles member ON member.oid = m.member
         JOIN pg_roles grantor ON grantor.oid = m.grantor
        WHERE m.roleid = $1 ORDER BY member.rolname, grantor.rolname'
      USING v_oid LOOP
      IF v_row.member_name = v_owner
         AND v_row.grantor_name = 'rdsadmin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- the unavoidable PG16+ RDS creator edge
      ELSE
        v_problems := array_append(v_problems,
          format('granted to %I by %I with ADMIN=%s INHERIT=%s SET=%s',
                 v_row.member_name, v_row.grantor_name, v_row.admin_option,
                 v_row.inherit_option, v_row.set_option)::text);
      END IF;
    END LOOP;
  ELSIF EXISTS (SELECT 1 FROM pg_auth_members WHERE roleid = v_oid) THEN
    v_problems := array_append(v_problems, 'granted to another role'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_shdepend WHERE refclassid = 'pg_authid'::regclass AND refobjid = v_oid AND deptype = 'o') THEN
    v_problems := array_append(v_problems, 'owns objects'::text);
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('RDS runtime role bootstrap: %I does not match the spec: %s', v_role, array_to_string(v_problems, ', ')),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = current_database() AND datdba = v_me.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = format('RDS runtime role bootstrap: %I must own database %I', current_user, current_database()),
      HINT = 'Nothing was changed. A non-owner REVOKE TEMPORARY FROM PUBLIC would be a silent no-op.';
  END IF;
  EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), v_role);
  SELECT d.datacl IS NULL OR EXISTS (
           SELECT 1 FROM aclexplode(d.datacl) a WHERE a.grantee = 0 AND a.privilege_type = 'TEMPORARY')
    INTO v_public_temp
    FROM pg_database d WHERE d.datname = current_database();
  IF v_public_temp OR NOT has_database_privilege(v_role, current_database(), 'CONNECT')
     OR has_database_privilege(v_role, current_database(), 'TEMPORARY') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS runtime role bootstrap: database ACL postcondition failed (PUBLIC TEMPORARY or runtime CONNECT/TEMPORARY)';
  END IF;
END
$runtime_rds$;
COMMIT;
