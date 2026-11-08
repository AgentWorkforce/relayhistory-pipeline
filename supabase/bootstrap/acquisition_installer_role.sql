-- Operator-only phase after migrations (20261105043850): make the Factory acquisition installer's login
-- able to log in. Run only under an explicit apply OK from the owner; the migration leaves the switch
-- disabled and enrolls no source, so a login alone admits nothing. The password is set separately from
-- 1Password in memory; never place it here or in a connection file. Run acquisition_installer_verify.sql
-- before and after. The role may EXECUTE only sessions.ingest_acquisition_install / _revoke /
-- _rebind_origin / _status and holds no table right and no enrollment right.
BEGIN;
DO $role$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_acquisition_installer')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='acquisition installer: bootstrap and migrate first';END IF;
END $role$;
ALTER ROLE bulls96_acquisition_installer LOGIN NOINHERIT;
ALTER ROLE bulls96_acquisition_installer SET statement_timeout='5s';
ALTER ROLE bulls96_acquisition_installer SET lock_timeout='2s';
ALTER ROLE bulls96_acquisition_installer SET idle_in_transaction_session_timeout='10s';
COMMIT;
