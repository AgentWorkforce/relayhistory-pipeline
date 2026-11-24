-- The session read API's Supabase role (src/session-read-api/liveness.ts).
--
-- The read API serves transcript reads from ClickHouse. It asks Supabase
-- whether a session is erased (every pointer row tombstoned) and when it began
-- (the history floor), and, for session timing, where the session's canonical
-- S3 objects are (session-timing-source.ts SESSION_POINTERS_SQL, the query of
-- sessions.session_directory_pointers). So this role may read the id/time and
-- object-pointer columns of the pointer table, plus the id and event-projection
-- epoch columns of sessions.session_directory for the Postgres search backend's
-- completeness check, and nothing else: no content, no titles, no metadata, no
-- other table, no writes. Its function grants are made
-- by migration 20261105034000 (EXECUTE on sessions.list_session_directory_for /
-- session_directory_activity_for, the directory list and activity pinned to the
-- same tenant settings), not here.
--
--   role      LOGIN NOINHERIT NOBYPASSRLS; no SUPERUSER, CREATEDB, CREATEROLE or
--             REPLICATION; read-only transactions; statement_timeout 2s.
--   grants    USAGE on schema sessions; SELECT on sessions.session_index of
--             (org_id, workspace_id, harness, session_id, created_at, deleted_at)
--             and the pointer columns (canonical_session_id, machine_id,
--             canonical_machine_id, user_id, storage_provider, bucket,
--             manifest_key, content_sha256, raw_format, revision).
--             Narrowing back is a REVOKE of the pointer columns; re-running an
--             older copy of this file does not remove them.
--   RLS       session_index already has RLS on. The policy below admits only rows
--             of the org and workspace that the current transaction bound with
--             set_config('bulls96_read.org_id' / 'bulls96_read.workspace_id', ..., true)
--             (TENANT_SETTINGS_SQL). A setting never set on the connection is NULL;
--             after a transaction-local set_config ends it reads as '' on that pooled
--             connection. Neither matches a row: org_id and workspace_id are never NULL or ''.
--
-- The bare role is created NOLOGIN before migrate by session_read_api_roles.sql (the
-- migration's grant needs it); this file then makes it LOGIN. The CREATE below remains
-- for a database whose migrate predates that bootstrap.
--
-- Three transactions, so that no transaction holds a lock on one of the two tables
-- while it waits for the other: (0) the role, its settings and schema USAGE, which
-- lock no table; (1) sessions.session_index: its column grant and its policy;
-- (2) sessions.session_directory: its column grant and its policy. A table's grant
-- and policy commit together. DROP POLICY and CREATE POLICY take ACCESS EXCLUSIVE
-- on their table (DROP POLICY IF EXISTS too, even when the policy is absent), so
-- each policy step first reads the catalog: a policy that already has exactly the
-- wanted definition (PERMISSIVE, FOR SELECT, TO bulls96_read_api only, the same
-- USING expression, no WITH CHECK) is left alone, so that transaction takes no
-- ACCESS EXCLUSIVE lock (ACCESS SHARE at most, which does not block readers or
-- writers); any other difference, or no policy, drops and recreates it. Each transaction
-- sets lock_timeout 3s, so a busy table fails that transaction instead of queueing
-- behind (and blocking) its readers and writers. The file is safe to re-run after
-- such a timeout: transactions that committed are no-ops the second time, and the
-- rest are applied.
--
-- No statement here reads hoopdb.db_mode, so this file needs no PGOPTIONS; only
-- session_read_api_role_verify.sql reads it (on Supabase PG16+ run the verifier with
-- PGOPTIONS='-c hoopdb.db_mode=supabase' so it tolerates the Supabase creator edge).
--
-- Idempotent. Run as `postgres` (Supabase) after the HoopDb migrations; set the
-- password separately (never in this file), then verify:
--
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_role.sql
--   psql -X -d <the HoopDb database> -c '\password bulls96_read_api'
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_role_verify.sql
\set ON_ERROR_STOP 1

-- Transaction 0: the role, its settings and schema USAGE. No table lock.
BEGIN;
SET LOCAL lock_timeout = '3s';
DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    CREATE ROLE bulls96_read_api LOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  END IF;
END
$role$;
-- Supabase's postgres is not a superuser, and PostgreSQL 16+ lets only a superuser name SUPERUSER or
-- REPLICATION in ALTER ROLE, even as NOSUPERUSER/NOREPLICATION (and BYPASSRLS needs BYPASSRLS). CREATE ROLE above
-- sets the safe attributes; here only LOGIN NOINHERIT are re-asserted, and session_read_api_role_verify.sql fails
-- if an existing role has SUPERUSER, CREATEDB, CREATEROLE, REPLICATION, BYPASSRLS or INHERIT.
ALTER ROLE bulls96_read_api LOGIN NOINHERIT;
ALTER ROLE bulls96_read_api SET statement_timeout = '2s';
ALTER ROLE bulls96_read_api SET default_transaction_read_only = on;

GRANT USAGE ON SCHEMA sessions TO bulls96_read_api;
COMMIT;

-- The wanted USING expression of both policies, as pg_get_expr deparses it (the same literal
-- session_read_api_role_verify.sql requires):
--   ((org_id = current_setting('bulls96_read.org_id'::text, true)) AND (workspace_id = current_setting('bulls96_read.workspace_id'::text, true)))
-- Observed on PostgreSQL 18.3 (PGlite 0.5.3). The comparison is exact text equality: if another server
-- version deparsed it differently, the comparison would only fail and the policy would be dropped and
-- recreated (and the verifier, which compares the same literal, would refuse it); a different policy is
-- never kept.

-- Transaction 1: sessions.session_index only.
BEGIN;
SET LOCAL lock_timeout = '3s';
GRANT SELECT (org_id, workspace_id, harness, session_id, created_at, deleted_at,
              canonical_session_id, machine_id, canonical_machine_id, user_id,
              storage_provider, bucket, manifest_key, content_sha256, raw_format, revision)
  ON sessions.session_index TO bulls96_read_api;
DO $session_index_read_api_policy$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_policy p
     WHERE p.polrelid = 'sessions.session_index'::regclass AND p.polname = 'session_index_read_api'
       AND p.polpermissive AND p.polcmd = 'r'
       AND p.polroles = ARRAY[(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = 'bulls96_read_api')]
       AND p.polwithcheck IS NULL
       AND pg_catalog.pg_get_expr(p.polqual, p.polrelid) = '((org_id = current_setting(''bulls96_read.org_id''::text, true)) AND (workspace_id = current_setting(''bulls96_read.workspace_id''::text, true)))'
  ) THEN
    RETURN; -- already exactly the wanted policy: no DROP or CREATE, so no ACCESS EXCLUSIVE lock
  END IF;
  DROP POLICY IF EXISTS session_index_read_api ON sessions.session_index;
  CREATE POLICY session_index_read_api ON sessions.session_index
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (
    org_id = current_setting('bulls96_read.org_id', true)
    AND workspace_id = current_setting('bulls96_read.workspace_id', true)
  );
END
$session_index_read_api_policy$;
COMMIT;

-- Transaction 2: sessions.session_directory only.
BEGIN;
SET LOCAL lock_timeout = '3s';
-- The Postgres search backend's completeness check (postgres-search-state.ts partials) compares the
-- search projection's epoch with the directory's. Ids and epoch columns only, tenant-bound below; no
-- title, summary, metadata or other content column. Everything else about the directory stays behind
-- the two definer functions.
GRANT SELECT (org_id, workspace_id, harness, session_id, event_projection_ready, event_projection_epoch)
  ON sessions.session_directory TO bulls96_read_api;
DO $session_directory_read_api_policy$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_policy p
     WHERE p.polrelid = 'sessions.session_directory'::regclass AND p.polname = 'session_directory_read_api'
       AND p.polpermissive AND p.polcmd = 'r'
       AND p.polroles = ARRAY[(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = 'bulls96_read_api')]
       AND p.polwithcheck IS NULL
       AND pg_catalog.pg_get_expr(p.polqual, p.polrelid) = '((org_id = current_setting(''bulls96_read.org_id''::text, true)) AND (workspace_id = current_setting(''bulls96_read.workspace_id''::text, true)))'
  ) THEN
    RETURN; -- already exactly the wanted policy: no DROP or CREATE, so no ACCESS EXCLUSIVE lock
  END IF;
  DROP POLICY IF EXISTS session_directory_read_api ON sessions.session_directory;
  CREATE POLICY session_directory_read_api ON sessions.session_directory
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (org_id = current_setting('bulls96_read.org_id', true) AND workspace_id = current_setting('bulls96_read.workspace_id', true));
END
$session_directory_read_api_policy$;
COMMIT;
