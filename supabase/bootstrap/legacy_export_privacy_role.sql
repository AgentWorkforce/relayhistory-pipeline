-- Operator-only phase after 20261105062030. The password is set separately
-- from 1Password in memory; never put it in this file or a connection file.
-- This role has SELECT on exactly the six export fence ledgers and EXECUTE on
-- the one authority-row registration RPC. It has no authority-table write.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_legacy_export_privacy')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE bulls96_legacy_export_privacy LOGIN NOINHERIT;
ALTER ROLE bulls96_legacy_export_privacy SET statement_timeout='20s';
ALTER ROLE bulls96_legacy_export_privacy SET lock_timeout='5s';
ALTER ROLE bulls96_legacy_export_privacy SET idle_in_transaction_session_timeout='30s';
COMMIT;
