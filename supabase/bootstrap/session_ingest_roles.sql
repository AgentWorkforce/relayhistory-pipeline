-- Phase 1: privileged bootstrap for the session ingest roles. Before migrate.
--
-- The migration (supabase/migrations/20261104022000_session_ingest_ledger.sql) never
-- creates or alters a role: a CNPG/Cloud SQL migration owner has no CREATEROLE. It
-- verifies these roles (42704 if absent, 55000 if they differ) and grants EXECUTE on
-- the ingest functions to them. This file provisions them, the pattern of
-- supabase/bootstrap/session_directory_search_worker.sql:
--
--   roles     bulls96_session_ingest         the ingest Worker's ledger login (made
--                                            LOGIN after migrate by session_ingest_role.sql)
--             bulls96_canonical_feature_producer  the 610 producer: read-only on the canonical
--                                            feature journal and the frozen pointer pages
--             bulls96_project_events         canonical metadata read/pending/ack only
--             bulls96_session_ingest_seeder  the revision seed (granted to an operator
--                                            login for scripts/session-ingest-seed.ts)
--             bulls96_privacy_eraser         the E3 transcript eraser under the Supabase privacy
--                                            authority: EXECUTE on the four eraser read RPCs only
--                                            (20261105041000; made LOGIN by privacy_eraser_role.sql)
--             bulls96_babysitter_retention   the E3 erasure phase's registry login: EXECUTE on the
--                                            retention claim/fence/complete RPCs only (20261105042000;
--                                            made LOGIN by babysitter_retention_role.sql)
--             bulls96_acquisition_installer  the Factory acquisition installer: USAGE on sessions and
--                                            EXECUTE on install/revoke/rebind/status only (20261105043850;
--                                            made LOGIN by acquisition_installer_role.sql)
--   attrs     NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
--   members   member of none. bulls96_acquisition_installer additionally has NO members (no login may
--             SET ROLE to it); the one tolerated inbound edge is Supabase's PG16+ creator edge
--             (postgres, granted by superuser supabase_admin, ADMIN TRUE, INHERIT FALSE, SET FALSE), the
--             exact predicate of acquisition_installer_verify.sql. The other roles keep their own member
--             contracts in their verifiers (the seeder is granted to an operator login by design).
--   run as    a superuser (CNPG `postgres`); a CREATEROLE role on PostgreSQL 15 and
--             older (Cloud SQL's migration_owner); or Supabase's `postgres` on 16+.
--   when      before every migrate (src/hoopdb/migrate.ts runs it as its bootstrap step
--             where the migrator may; on Supabase the runbook runs it as `postgres`).
--   exit      success: both roles exist (created now or already) and are not elevated.
--             failure: 42501 (caller may not create them) or 55000 (an existing role is
--             elevated or a member of a role, or the installer has a member; nothing is changed). It never alters an
--             existing role, so it is safe after session_ingest_role.sql made one LOGIN.
--
-- Idempotent and atomic (one DO block). Roles are cluster-wide.
DO $bootstrap$
DECLARE
  v_name text;
  v_me record;
  r pg_roles%ROWTYPE;
  v_member record;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  FOREACH v_name IN ARRAY ARRAY['bulls96_session_ingest', 'bulls96_session_ingest_seeder', 'bulls96_canonical_feature_producer', 'bulls96_project_events', 'bulls96_privacy_eraser', 'bulls96_babysitter_retention', 'bulls96_acquisition_installer'] LOOP
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
    IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
       OR (v_name = 'bulls96_session_ingest_seeder' AND r.rolcanlogin) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('bootstrap %s: the role exists elevated (or the seeder can log in)', v_name),
        HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('bootstrap %s: the role is a member of another role', v_name),
        HINT = 'Nothing was changed. Revoke the membership by hand, then rerun.';
    END IF;
    -- Inbound direction, installer only (dogpatch P1): a login that is a member OF the installer can
    -- SET ROLE to it and call the SECURITY DEFINER acquisition RPCs directly. Only Supabase's PG16+
    -- creator edge is tolerated, and only where this file itself would have admitted Supabase's postgres
    -- as the creator: it is SET FALSE and INHERIT FALSE, so it can neither SET ROLE nor inherit the
    -- grants. Before PG16 (no inherit_option/set_option) every member is refused.
    IF v_name = 'bulls96_acquisition_installer' THEN
      IF v_version >= 160000 THEN
        FOR v_member IN EXECUTE 'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super, a.admin_option, a.inherit_option, a.set_option
            FROM pg_auth_members a JOIN pg_roles u ON u.oid = a.member JOIN pg_roles g ON g.oid = a.grantor
           WHERE a.roleid = $1' USING r.oid LOOP
          IF (v_mode = 'supabase' OR EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin' AND rolsuper))
             AND v_member.rolname = 'postgres' AND v_member.grantor = 'supabase_admin' AND v_member.grantor_super
             AND v_member.admin_option AND NOT v_member.inherit_option AND NOT v_member.set_option THEN
            NULL; -- Supabase PG16+ creator edge
          ELSE
            RAISE EXCEPTION USING ERRCODE = '55000',
              MESSAGE = format('bootstrap %s: the role has an unexpected member %I', v_name, v_member.rolname),
              HINT = 'Nothing was changed. Revoke the membership by hand (no login may be a member of the installer), then rerun.';
          END IF;
        END LOOP;
      ELSIF EXISTS (SELECT 1 FROM pg_auth_members WHERE roleid = r.oid) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
          MESSAGE = format('bootstrap %s: the role has an unexpected member', v_name),
          HINT = 'Nothing was changed. Revoke the membership by hand (no login may be a member of the installer), then rerun.';
      END IF;
    END IF;
  END LOOP;
END
$bootstrap$;
