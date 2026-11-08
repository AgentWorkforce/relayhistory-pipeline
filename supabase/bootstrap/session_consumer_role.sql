-- Dedicated runtime role for the self-host search-index writer
-- (SERVICE=session-pipeline-consumer).
--
-- The reconciler connects as this role and never as owner, migrator or
-- reader. Least privilege, derived from the statements the tick actually
-- issues (pointer upsert + directory projection + tombstone propagation,
-- search write + erasure, pending/high-water, ledger/marker reads):
--   * USAGE on the sessions and search_index schemas (without it every
--     sessions.* call fails 42501);
--   * exactly the table DML below (session_index_ext and the directory
--     membership tables are written directly, not only through functions);
--   * EXECUTE on the three pointer functions, including the SECURITY DEFINER
--     session_publication_is_managed (the writer must call it; verify
--     allowlists exactly this one and no other SECURITY DEFINER function);
--   * a transparent RLS policy on every sessions table the writer touches
--     (the member-read policies name only `authenticated`, so without
--     these the writer sees 0 rows and reports complete wrongly; same
--     shape as the migrator's trusted policy, scoped to this role). No
--     writer policy on the search_index tables: those writes always run
--     with the tenant GUCs bound per scope, under tenant_only policies.
--
-- Run as the schema owner INSIDE the bootstrap's single transaction (the
-- bootstrap holds the advisory lock and rolls everything back on failure).
-- This file holds no BEGIN/COMMIT of its own: its own COMMIT would end the
-- outer transaction early, drop the lock and lock_timeout, and leave a
-- partial commit behind when verify later fails. Grants converge on rerun:
-- every grant below is revoked first, then granted exactly.
--
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_consumer_role.sql
--   psql -X -d <the HoopDb database> -c '\password search_consumer_runtime'
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_consumer_role_verify.sql
DO $role$
DECLARE
  v_name text := 'search_consumer_runtime';
  r pg_roles%ROWTYPE;
  v_version integer := current_setting('server_version_num')::integer;
BEGIN
  -- Never create or elevate a privileged/built-in name under this file: a
  -- future edit renaming v_name to one of these must fail closed here,
  -- before any role lookup or grant.
  IF v_name IN ('public', 'anon', 'authenticated', 'postgres', 'supabase_admin', 'bulls96_read_api')
     OR left(v_name, 3) = 'pg_' THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('bootstrap %s: refusing a reserved role name', v_name),
      HINT = 'This file manages only the dedicated search_consumer_runtime role.';
  END IF;
  SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  IF NOT FOUND THEN
    CREATE ROLE search_consumer_runtime LOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  END IF;
  -- An existing role is never altered: it must already match the spec.
  IF NOT r.rolcanlogin OR r.rolinherit OR r.rolbypassrls OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('bootstrap %s: the role exists with different attributes', v_name),
      HINT = 'Nothing was changed. Fix or drop the role by hand (an operator decision), then rerun.';
  END IF;
  -- The creator edge: whoever creates the role unavoidably becomes its
  -- member WITH ADMIN OPTION but no INHERIT and no SET (observed on real
  -- PG16+ for CREATEROLE owners and superusers alike; on PG15 the edge
  -- carries only the admin flag, and the catalog lacks the other columns).
  -- Tolerate exactly that row when the member is the invoker; the writer
  -- itself must never be a member of another role, and any other membership
  -- fails closed.
  IF v_version >= 160000 THEN
    IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
               WHERE (a.roleid = r.oid OR a.member = r.oid)
                 AND NOT (a.roleid = r.oid AND m.rolname = current_user AND a.admin_option
                          AND NOT a.inherit_option AND NOT a.set_option)) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('bootstrap %s: the role has memberships beyond the creator edge', v_name),
        HINT = 'Nothing was changed. Revoke the membership by hand (an operator decision), then rerun.';
    END IF;
  ELSE
    IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
               WHERE (a.roleid = r.oid OR a.member = r.oid)
                 AND NOT (a.roleid = r.oid AND m.rolname = current_user AND a.admin_option)) THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('bootstrap %s: the role has memberships beyond the creator edge', v_name),
        HINT = 'Nothing was changed. Revoke the membership by hand (an operator decision), then rerun.';
    END IF;
  END IF;
END
$role$;
-- PostgreSQL 16+ lets only a superuser name SUPERUSER or REPLICATION in ALTER
-- ROLE, even as NOSUPERUSER/NOREPLICATION (and BYPASSRLS needs BYPASSRLS).
-- CREATE ROLE above sets the safe attributes; here only LOGIN NOINHERIT are
-- re-asserted, and session_consumer_role_verify.sql fails if an existing role
-- has SUPERUSER, CREATEDB, CREATEROLE, REPLICATION, BYPASSRLS or INHERIT.
ALTER ROLE search_consumer_runtime LOGIN NOINHERIT;
-- Backstop matching the writer transactions' own SET LOCAL (60s); the pointer
-- publication sets tighter locals per statement.
ALTER ROLE search_consumer_runtime SET statement_timeout = '60s';

-- Schema access (revoke-first so a rerun converges drift).
REVOKE ALL ON SCHEMA sessions FROM search_consumer_runtime;
REVOKE ALL ON SCHEMA search_index FROM search_consumer_runtime;
GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime;
GRANT USAGE ON SCHEMA search_index TO search_consumer_runtime;

-- Ledger + marker reads the runWorker readiness gate needs (read-only, no
-- membership borrowed).
REVOKE ALL ON sessions.__96bulls_migrations FROM search_consumer_runtime;
REVOKE ALL ON sessions.__96bulls_role_validation FROM search_consumer_runtime;
GRANT SELECT ON sessions.__96bulls_migrations TO search_consumer_runtime;
GRANT SELECT ON sessions.__96bulls_role_validation TO search_consumer_runtime;

-- Pointer surface (revoke-first, then exactly what the tick issues).
REVOKE ALL ON sessions.session_index FROM search_consumer_runtime;
REVOKE ALL ON sessions.session_index_ext FROM search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.session_index TO search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE ON sessions.session_index_ext TO search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.upsert_session_index(jsonb) FROM search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.session_index_batch(jsonb) FROM search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.session_publication_is_managed(text, text, text, text, text) FROM search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.upsert_session_index(jsonb) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.session_index_batch(jsonb) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.session_publication_is_managed(text, text, text, text, text) TO search_consumer_runtime;

-- Repo catalog the pointer upsert maintains through these invoker helpers
-- (upsert_session_index calls them as the writer, so they need EXECUTE plus
-- the table DML below, not SECURITY DEFINER).
REVOKE ALL ON FUNCTION sessions.applied_repo_evidence(jsonb, jsonb) FROM search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.canonical_remote(text) FROM search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.clean_root_commit(text) FROM search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.lock_repo_catalog(text) FROM search_consumer_runtime;
REVOKE ALL ON FUNCTION sessions.resolve_repo(text, text, text, timestamp with time zone) FROM search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.applied_repo_evidence(jsonb, jsonb) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.canonical_remote(text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.clean_root_commit(text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.lock_repo_catalog(text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.resolve_repo(text, text, text, timestamp with time zone) TO search_consumer_runtime;
REVOKE ALL ON sessions.repos FROM search_consumer_runtime;
REVOKE ALL ON sessions.repo_names FROM search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE ON sessions.repos TO search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE ON sessions.repo_names TO search_consumer_runtime;

-- Directory projection + tombstone propagation (direct DML, not functions).
REVOKE ALL ON sessions.session_directory FROM search_consumer_runtime;
REVOKE ALL ON sessions.session_directory_users FROM search_consumer_runtime;
REVOKE ALL ON sessions.session_directory_machines FROM search_consumer_runtime;
REVOKE ALL ON sessions.session_briefs FROM search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.session_directory TO search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.session_directory_users TO search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.session_directory_machines TO search_consumer_runtime;
GRANT SELECT ON sessions.session_briefs TO search_consumer_runtime;

-- Directory event watermarks (the erased and live-complete branches). INSERT on
-- event_pending is required because the AFTER trigger on session_index
-- (sessions.session_directory_events_changed, SECURITY INVOKER) inserts the
-- dirty marker as the writer: without INSERT every pointer write fails 42501.
-- Only that invoker trigger writes extra tables from the writer's statements;
-- every other trigger reachable from the writer's writes is SECURITY DEFINER
-- (replay, history sync, seed, publication guard, brief documents) or touches
-- only the NEW row (project_name, tombstone), except event_times_forget,
-- whose DELETE the writer is already granted below.
REVOKE ALL ON sessions.session_directory_event_pending FROM search_consumer_runtime;
REVOKE ALL ON sessions.session_directory_event_times FROM search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE ON sessions.session_directory_event_pending TO search_consumer_runtime;
GRANT SELECT, INSERT, DELETE ON sessions.session_directory_event_times TO search_consumer_runtime;

-- Search write surface. DELETE on transcript_events is required: the adapter
-- erases a scope's rows there (synchronousErasure). DELETE on
-- projection_state is required: the tombstone backstop retires erasure
-- residue atomically (a complete row_count=0 state with no transcript rows
-- left), in one statement, so the same tombstones are not re-sent forever.
REVOKE ALL ON search_index.transcript_events FROM search_consumer_runtime;
REVOKE ALL ON search_index.projection_state FROM search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON search_index.transcript_events TO search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON search_index.projection_state TO search_consumer_runtime;

-- The writer's own durable tables (owner-created by the bootstrap Job).
REVOKE ALL ON sessions.search_consumer_pending FROM search_consumer_runtime;
REVOKE ALL ON sessions.search_consumer_progress FROM search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.search_consumer_pending TO search_consumer_runtime;
GRANT SELECT, INSERT, UPDATE ON sessions.search_consumer_progress TO search_consumer_runtime;

-- Transparent RLS for the writer: PERMISSIVE FOR ALL USING (true), one policy
-- per RLS-enabled table the tick touches. Without these the member-read
-- policies (TO authenticated only) leave the writer blind: reconcile, the
-- freshness self-check and the tombstone backstop all return empty and the
-- tick wrongly reports complete. Recreated on rerun so definition drift
-- converges; verify fails on any other writer-targeted policy.
DO $policies$
DECLARE
  v_tables text[] := ARRAY[
    'sessions.session_index',
    'sessions.session_index_ext',
    'sessions.session_directory',
    'sessions.session_directory_users',
    'sessions.session_directory_machines',
    'sessions.session_briefs',
    'sessions.session_directory_event_pending',
    'sessions.session_directory_event_times',
    'sessions.repos',
    'sessions.repo_names'
  ];
  v_search_tables text[] := ARRAY[
    'search_index.transcript_events',
    'search_index.projection_state'
  ];
  v_table text;
BEGIN
  FOREACH v_table IN ARRAY v_tables LOOP
    EXECUTE format('DROP POLICY IF EXISTS search_consumer_trusted ON %s', v_table);
    EXECUTE format('CREATE POLICY search_consumer_trusted ON %s AS PERMISSIVE FOR ALL TO search_consumer_runtime USING (true) WITH CHECK (true)', v_table);
  END LOOP;
  -- A previous install may have left a writer policy on the search tables;
  -- drop it so the tenant_only policies are the only gate. None is created.
  FOREACH v_table IN ARRAY v_search_tables LOOP
    EXECUTE format('DROP POLICY IF EXISTS search_consumer_trusted ON %s', v_table);
  END LOOP;
END
$policies$;
