-- Phase 1: privileged bootstrap for the session read API role. Before migrate.
--
-- The migration supabase/migrations/20261105034000_session_directory_read_api.sql
-- grants EXECUTE on the read API's tenant-setting directory functions
-- (sessions.list_session_directory_for, sessions.session_directory_activity_for) to
-- bulls96_read_api, and src/hoopdb/sessions-function-acl.json lists that grant, so the
-- role must exist BEFORE migrate. A migration never creates or alters a role (a CNPG /
-- Cloud SQL migration owner has no CREATEROLE), so this file provisions the bare role,
-- the pattern of supabase/bootstrap/session_ingest_roles.sql:
--
--   role      bulls96_read_api   the session read Worker's Supabase login (Hyperdrive).
--                                Made LOGIN, with its settings, column grant and RLS
--                                policy, AFTER migrate by session_read_api_role.sql.
--   attrs     NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
--   members   member of none.
--   run as    a superuser (CNPG `postgres`); a CREATEROLE role on PostgreSQL 15 and
--             older (Cloud SQL's migration_owner); or Supabase's `postgres` on 16+.
--   when      before every migrate (src/hoopdb/migrate.ts runs it as its bootstrap step
--             where the migrator may; on Supabase the runbook runs it as `postgres`).
--             Production already has the role (LOGIN, from session_read_api_role.sql),
--             so there this file only verifies it.
--   exit      success: the role exists (created now or already) and is not elevated.
--             failure: 42501 (caller may not create it) or 55000 (the existing role is
--             elevated or a member of a role; nothing is changed). It never alters an
--             existing role, so it is safe after session_read_api_role.sql made it LOGIN.
--
-- Idempotent and atomic (one DO block). Roles are cluster-wide.
DO $bootstrap$
DECLARE
  v_name text := 'bulls96_read_api';
  v_me record;
  r pg_roles%ROWTYPE;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  IF NOT FOUND THEN
    SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
    IF NOT v_me.rolsuper
       AND NOT (v_me.rolcreaterole AND v_version < 160000)
       AND NOT (v_version >= 160000 AND current_user = 'postgres' AND v_me.rolcreaterole
                AND (v_mode = 'supabase'
                     OR EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin' AND rolsuper))) THEN
      RAISE EXCEPTION USING ERRCODE = '42501',
        MESSAGE = format('bootstrap %s: %I may not create it', v_name, current_user),
        HINT = 'Run as a superuser, a CREATEROLE role on PostgreSQL 15 or older, or Supabase''s postgres.';
    END IF;
    EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', v_name);
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  END IF;
  IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('bootstrap %s: the role exists elevated', v_name),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('bootstrap %s: the role is a member of another role', v_name),
      HINT = 'Nothing was changed. Revoke the membership by hand, then rerun.';
  END IF;
END
$bootstrap$;

-- The digest narrative writer (lane F4, migration 20261105045000), part of the read Worker's
-- package: bulls96_digest_narrative, the narrative Worker's Supabase login. The identical
-- contract and checks as above (bare, NOLOGIN, not elevated, member of none); made LOGIN after
-- migrate by digest_narrative_role.sql. Provisioned here so every migrate and test that already
-- runs this bootstrap creates it before 20261105045000's fail-closed role check.
DO $bootstrap_narrative$
DECLARE
  v_name text := 'bulls96_digest_narrative';
  v_me record;
  r pg_roles%ROWTYPE;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  IF NOT FOUND THEN
    SELECT rolsuper, rolcreaterole INTO v_me FROM pg_roles WHERE rolname = current_user;
    IF NOT v_me.rolsuper
       AND NOT (v_me.rolcreaterole AND v_version < 160000)
       AND NOT (v_version >= 160000 AND current_user = 'postgres' AND v_me.rolcreaterole
                AND (v_mode = 'supabase'
                     OR EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin' AND rolsuper))) THEN
      RAISE EXCEPTION USING ERRCODE = '42501',
        MESSAGE = format('bootstrap %s: %I may not create it', v_name, current_user),
        HINT = 'Run as a superuser, a CREATEROLE role on PostgreSQL 15 or older, or Supabase''s postgres.';
    END IF;
    EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', v_name);
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  END IF;
  IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('bootstrap %s: the role exists elevated', v_name),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('bootstrap %s: the role is a member of another role', v_name),
      HINT = 'Nothing was changed. Revoke the membership by hand, then rerun.';
  END IF;
END
$bootstrap_narrative$;
