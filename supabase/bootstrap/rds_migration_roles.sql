-- Amazon RDS for PostgreSQL 16+ migration compatibility roles (HOOPDB_DB_MODE=rds;
-- RDS for PostgreSQL only, NOT Aurora). Same contract as cloudsql_migration_roles.sql.
--
-- RDS exposes no customer SUPERUSER: the master is a CREATEROLE member of rds_superuser.
-- PostgreSQL 16+ records an automatic ADMIN membership, granted by the bootstrap
-- superuser (rdsadmin on RDS), when a non-superuser CREATEROLE user creates a role, and
-- the creator cannot revoke it. The generic bootstraps (supabase_shim.sql,
-- session_directory_search_worker.sql, session_token_verifier.sql,
-- session_ingest_roles.sql) refuse such a creator (42501), and the shim's BYPASSRLS
-- service_role cannot be created at all without BYPASSRLS.
-- https://www.postgresql.org/docs/17/role-attributes.html
--
-- Use a disposable CREATEROLE trampoline instead: it creates the fixed NOLOGIN
-- roles, the same set as cloudsql_migration_roles.sql (ingest, canonical producer,
-- project-events and read API included; their
-- migrations only verify roles), then the migration owner drops the trampoline. DROP ROLE removes the
-- trampoline's automatic memberships, leaving every target membership-free, so the
-- existing strict verifiers and historical migration guards pass unchanged. The
-- trampoline and every target are created in this one transaction, so any refusal rolls
-- the entire operation back. Consequence (by design): nobody but rdsadmin can later
-- alter or drop these NOLOGIN roles.
--
-- search_consumer_runtime is NOT in that set: the migration owner creates it
-- directly with supabase/bootstrap/session_consumer_roles.sql (like the runtime
-- LOGIN role below), keeping the owner's ADMIN creator edge so the owner can
-- promote it. A trampoline copy would leave it manageable only by rdsadmin.
--
-- service_role is deliberately NOBYPASSRLS, as on Cloud SQL: there is no Supabase or
-- PostgREST service-role runtime on RDS; the role exists only because the shared schema
-- grants reference it.
--
-- The runtime LOGIN role is NOT created here: see hoopdb_runtime_role_rds.sql (it must
-- stay manageable by the master for password rotation).
--
-- Run directly as the RDS migration owner (the master), connected to the HoopDb
-- database, before the first migrate and before the canonical
-- session_directory_search_worker.sql role verifier. Idempotent after a clean success;
-- it never repairs or replaces an existing role.
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> \
--     -f supabase/bootstrap/rds_migration_roles.sql
DO $rds_migration_roles$
DECLARE
  v_owner name := current_user;
  v_seed constant name := 'bulls96_rds_role_seed';
  v_role name;
  v_row record;
  v_problems text[] := '{}';
  v_missing boolean := false;
BEGIN
  IF current_setting('server_version_num')::int < 160000 THEN
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'RDS role bootstrap requires PostgreSQL 16 or later';
  END IF;
  SELECT * INTO v_row FROM pg_roles WHERE rolname = v_owner;
  IF v_row.rolsuper OR NOT v_row.rolcreaterole THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'RDS role bootstrap requires a non-superuser CREATEROLE migration owner';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles r
                  WHERE r.rolname = 'rds_superuser' AND pg_has_role(v_owner, r.oid, 'MEMBER')) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'RDS role bootstrap requires rds_superuser membership';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rdsadmin' AND rolsuper) THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'RDS role bootstrap: the fixed rdsadmin SUPERUSER was not proven';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_seed) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('RDS role bootstrap: reserved trampoline role %I already exists', v_seed),
      HINT = 'Nothing was changed. Inspect and remove the stale role as a separate operator action.';
  END IF;

  FOREACH v_role IN ARRAY ARRAY[
    'anon'::name,
    'authenticated'::name,
    'service_role'::name,
    'session_directory_search_worker'::name,
    'session_token_verifier'::name,
    'bulls96_session_ingest'::name,
    'bulls96_session_ingest_seeder'::name,
    'bulls96_canonical_feature_producer'::name,
    'bulls96_read_api'::name,
    'bulls96_project_events'::name,
    'bulls96_privacy_eraser'::name,
    'bulls96_babysitter_retention'::name,
    'bulls96_acquisition_installer'::name,
    'buffered_search_feed_runtime'::name
  ] LOOP
    SELECT * INTO v_row FROM pg_roles WHERE rolname = v_role;
    IF NOT FOUND THEN
      v_missing := true;
      CONTINUE;
    END IF;
    -- search_consumer_runtime is deliberately NOT in this inventory: the
    -- migration owner creates it directly with
    -- supabase/bootstrap/session_consumer_roles.sql (same pattern as
    -- hoopdb_runtime_role_rds.sql), keeping the unavoidable PG16+ ADMIN
    -- creator edge so the owner can promote it to LOGIN. A trampoline would
    -- create it in the disposable seed context, and the final membership
    -- recheck below would then refuse the owner's promotion path, so the
    -- trampoline must never provision it. Its claimed-LOGIN steady state
    -- converges in session_consumer_roles.sql instead.
    IF v_row.rolcanlogin THEN v_problems := array_append(v_problems, format('%I has LOGIN', v_role)::text); END IF;
    IF v_row.rolinherit THEN v_problems := array_append(v_problems, format('%I has INHERIT', v_role)::text); END IF;
    IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, format('%I has BYPASSRLS', v_role)::text); END IF;
    IF v_row.rolsuper THEN v_problems := array_append(v_problems, format('%I has SUPERUSER', v_role)::text); END IF;
    IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, format('%I has CREATEROLE', v_role)::text); END IF;
    IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, format('%I has CREATEDB', v_role)::text); END IF;
    IF v_row.rolreplication THEN v_problems := array_append(v_problems, format('%I has REPLICATION', v_role)::text); END IF;
    IF EXISTS (
      SELECT 1 FROM pg_auth_members
       WHERE roleid = v_row.oid OR member = v_row.oid
    ) THEN
      v_problems := array_append(v_problems, format('%I has a role membership', v_role)::text);
    END IF;
    IF v_role = 'session_directory_search_worker' AND (
      (SELECT count(*) FROM pg_db_role_setting WHERE setrole = v_row.oid) <> 1 OR
      NOT EXISTS (
        SELECT 1 FROM pg_db_role_setting
         WHERE setrole = v_row.oid AND setdatabase = 0
           AND setconfig = ARRAY['statement_timeout=2s']::text[]
      )
    ) THEN
      v_problems := array_append(v_problems, 'session_directory_search_worker has unexpected role settings'::text);
    END IF;
    IF v_role <> 'session_directory_search_worker' AND EXISTS (
      SELECT 1 FROM pg_db_role_setting WHERE setrole = v_row.oid
    ) THEN
      v_problems := array_append(v_problems, format('%I has role settings', v_role)::text);
    END IF;
  END LOOP;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS role bootstrap refused (nothing changed): ' || array_to_string(v_problems, '; ');
  END IF;

  IF v_missing THEN
    CREATE ROLE bulls96_rds_role_seed
      NOLOGIN NOINHERIT CREATEROLE NOBYPASSRLS NOSUPERUSER NOCREATEDB NOREPLICATION;
    -- The automatic membership has SET FALSE. An ADMIN member may add SET TRUE; this
    -- does not try to modify or revoke the bootstrap-superuser grant.
    EXECUTE format(
      'GRANT bulls96_rds_role_seed TO %I WITH INHERIT FALSE, SET TRUE',
      v_owner
    );
    SET LOCAL ROLE bulls96_rds_role_seed;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
      CREATE ROLE anon NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
      CREATE ROLE authenticated NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
      CREATE ROLE service_role NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_directory_search_worker') THEN
      CREATE ROLE session_directory_search_worker
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
      ALTER ROLE session_directory_search_worker SET statement_timeout = '2s';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_token_verifier') THEN
      CREATE ROLE session_token_verifier
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest') THEN
      CREATE ROLE bulls96_session_ingest
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest_seeder') THEN
      CREATE ROLE bulls96_session_ingest_seeder
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_canonical_feature_producer') THEN
      CREATE ROLE bulls96_canonical_feature_producer
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
      CREATE ROLE bulls96_read_api
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_project_events') THEN
      CREATE ROLE bulls96_project_events
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_privacy_eraser') THEN
      CREATE ROLE bulls96_privacy_eraser
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_babysitter_retention') THEN
      CREATE ROLE bulls96_babysitter_retention
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    -- search_consumer_runtime is never created here (see the inventory note
    -- above): the migration owner provisions it with
    -- session_consumer_roles.sql so the owner's ADMIN creator edge survives.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_acquisition_installer') THEN
      CREATE ROLE bulls96_acquisition_installer
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'buffered_search_feed_runtime') THEN
      CREATE ROLE buffered_search_feed_runtime
        NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    RESET ROLE;
    DROP ROLE bulls96_rds_role_seed;
  END IF;

  -- Recheck the security property that the trampoline exists to establish.
  IF EXISTS (
    SELECT 1
      FROM pg_auth_members membership
      JOIN pg_roles target ON target.oid IN (membership.roleid, membership.member)
     WHERE target.rolname IN (
       'anon', 'authenticated', 'service_role', 'session_directory_search_worker',
       'session_token_verifier',
       'bulls96_session_ingest', 'bulls96_session_ingest_seeder',
       'bulls96_canonical_feature_producer', 'bulls96_read_api', 'bulls96_project_events',
       'bulls96_privacy_eraser', 'bulls96_babysitter_retention',
       'bulls96_acquisition_installer',
       'buffered_search_feed_runtime'
     )
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS role bootstrap left a target role membership';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_seed) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'RDS role bootstrap left its trampoline role behind';
  END IF;
END
$rds_migration_roles$;
