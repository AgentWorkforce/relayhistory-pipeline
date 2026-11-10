-- Operator-only phase after migrations. Password is set separately from
-- 1Password in memory; never place it here or in a connection file.
-- Run canonical_project_events_verify.sql before and after activation.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_project_events')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE bulls96_project_events LOGIN NOINHERIT;
ALTER ROLE bulls96_project_events SET statement_timeout='5s';
ALTER ROLE bulls96_project_events SET lock_timeout='2s';
ALTER ROLE bulls96_project_events SET idle_in_transaction_session_timeout='10s';
COMMIT;
