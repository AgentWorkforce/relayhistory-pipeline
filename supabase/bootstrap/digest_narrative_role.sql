-- The digest narrative Worker's Supabase login (lane F4; packages/session-read-worker
-- src/digest-narrative.ts). After migrate, operator-only. The password is set separately
-- from 1Password (never in this file or on disk).
--
--   role      LOGIN NOINHERIT; no SUPERUSER, CREATEDB, CREATEROLE, REPLICATION or BYPASSRLS
--             (the bare role comes from session_read_api_roles.sql); statement_timeout 5s.
--   grants    USAGE on schema sessions only. Its single function grant, EXECUTE on
--             sessions.digest_narrative_upsert, is made by migration 20261105045000. No
--             table privilege: the table is reached only through that SECURITY DEFINER.
--
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/digest_narrative_role.sql
--   psql -X -d <the HoopDb database> -c '\password bulls96_digest_narrative'
\set ON_ERROR_STOP 1
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_digest_narrative')
 THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'digest narrative: bootstrap and migrate first'; END IF;
END $role$;
ALTER ROLE bulls96_digest_narrative LOGIN NOINHERIT;
ALTER ROLE bulls96_digest_narrative SET statement_timeout = '5s';
ALTER ROLE bulls96_digest_narrative SET lock_timeout = '2s';
ALTER ROLE bulls96_digest_narrative SET idle_in_transaction_session_timeout = '10s';
GRANT USAGE ON SCHEMA sessions TO bulls96_digest_narrative;
COMMIT;
