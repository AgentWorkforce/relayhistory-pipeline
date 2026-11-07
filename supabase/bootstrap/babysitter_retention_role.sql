-- Operator-only phase after migrations (20261105042000): make the E3 erasure phase's
-- registry login able to log in. Password is set separately from 1Password in memory;
-- never place it here or in a connection file. Run babysitter_retention_verify.sql
-- before and after activation. The role may EXECUTE only the retention claim/fence/complete
-- RPCs (no ingest, register, reset or commit RPC, and no table), so the erasure host
-- holds no cross-tenant write credential.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_babysitter_retention')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='babysitter retention: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE bulls96_babysitter_retention LOGIN NOINHERIT;
ALTER ROLE bulls96_babysitter_retention SET statement_timeout='5s';
ALTER ROLE bulls96_babysitter_retention SET lock_timeout='2s';
ALTER ROLE bulls96_babysitter_retention SET idle_in_transaction_session_timeout='10s';
COMMIT;
