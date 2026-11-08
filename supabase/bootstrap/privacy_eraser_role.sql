-- Operator-only phase after migrations (20261105041000): make the E3 transcript eraser's login able to log in.
-- Run only under an explicit apply OK, after the Supabase privacy authority is activated. The password is set
-- separately from 1Password in memory; never place it here or in a connection file. Run privacy_eraser_verify.sql
-- before and after. The role may EXECUTE only the four eraser read RPCs (state, groups, page, still-fenced) and holds
-- no table right, so the host that holds the destructive S3 eraser key holds no owner or BYPASSRLS database credential.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_privacy_eraser')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy eraser: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE bulls96_privacy_eraser LOGIN NOINHERIT;
ALTER ROLE bulls96_privacy_eraser SET statement_timeout='5s';
ALTER ROLE bulls96_privacy_eraser SET lock_timeout='2s';
ALTER ROLE bulls96_privacy_eraser SET idle_in_transaction_session_timeout='10s';
COMMIT;
