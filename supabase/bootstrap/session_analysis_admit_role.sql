-- Lane F1 (migration 20261105046000): the session-analysis ADMISSION principal of the read
-- API, bulls96_analysis_admit. Post-migrate operator step (needs Khaliq's OK, like every role
-- change): makes the bare NOLOGIN role (created before migrate by session_read_api_roles.sql)
-- able to log in, and gives it exactly what sessions.analysis_request_v1 needs.
--   role      LOGIN NOINHERIT NOBYPASSRLS; no SUPERUSER, CREATEDB, CREATEROLE or REPLICATION;
--             statement_timeout 2s; read-write (it inserts one request/job per admission).
--   grants    USAGE on schema sessions. Nothing else here: its only function grant is the
--             migration's EXECUTE on sessions.analysis_request_v1 (SECURITY DEFINER, tenant
--             pinned by sessions.session_analysis_admit_scope). No table, column or sequence.
-- Idempotent. Run as `postgres` (Supabase) after the HoopDb migrations; set the password
-- separately (never in this file; 1Password is the system of record), then verify:
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_analysis_admit_role.sql
--   psql -X -d <the HoopDb database> -c '\password bulls96_analysis_admit'
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_analysis_admit_role_verify.sql
-- Nothing in code names this role yet (no analysis_admit_url reader): it is the
-- reserved admission principal for the future read-API analysis path.
\set ON_ERROR_STOP 1
BEGIN;
DO $role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_analysis_admit') THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = 'bulls96_analysis_admit does not exist; run session_read_api_roles.sql and migrate first';
  END IF;
END
$role$;
-- As session_read_api_role.sql: only LOGIN NOINHERIT are re-asserted (Supabase's postgres may not
-- name SUPERUSER/REPLICATION); the verifier refuses any elevated attribute.
ALTER ROLE bulls96_analysis_admit LOGIN NOINHERIT;
ALTER ROLE bulls96_analysis_admit SET statement_timeout = '2s';
-- Read-write by design (it inserts the request and job), stated explicitly so no inherited or
-- copied default_transaction_read_only=on can make every admission fail.
ALTER ROLE bulls96_analysis_admit SET default_transaction_read_only = off;
GRANT USAGE ON SCHEMA sessions TO bulls96_analysis_admit;
COMMIT;
