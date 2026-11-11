-- Explicit operator bootstrap AFTER the search index/state schema exists. Never run by the service.
-- Role creation, LOGIN/credentials, and applying this to a real database require Khaliq's approval.
-- The service accepts only a genuine LOGIN of this dedicated role, on the same metadata database.
\set ON_ERROR_STOP 1
BEGIN;
DO $role$ DECLARE r pg_roles%ROWTYPE;BEGIN
 IF session_user<>current_user OR current_setting('role')<>'none'
  OR NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='operator search erase: direct superuser bootstrap required';END IF;
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_session_erase_search';
 IF NOT FOUND THEN
  CREATE ROLE bulls96_session_erase_search NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
 ELSE
  IF r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolinherit
   OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid OR roleid=r.oid) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator search erase: unsafe role attributes or membership';END IF;
 END IF;
 IF to_regclass('search_index.transcript_events') IS NULL OR to_regclass('search_index.projection_state') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator search erase: search schema must exist first';END IF;
 IF(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='search_index'
  AND c.relname IN('transcript_events','projection_state') AND c.relrowsecurity AND c.relforcerowsecurity)<>2 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator search erase: forced tenant RLS required';END IF;
END $role$;
ALTER ROLE bulls96_session_erase_search SET statement_timeout='5s';
ALTER ROLE bulls96_session_erase_search SET lock_timeout='2s';
ALTER ROLE bulls96_session_erase_search SET idle_in_transaction_session_timeout='10s';
GRANT USAGE ON SCHEMA search_index TO bulls96_session_erase_search;
GRANT DELETE ON search_index.transcript_events,search_index.projection_state TO bulls96_session_erase_search;
GRANT SELECT(org_id,workspace_id,source,session_id) ON search_index.transcript_events TO bulls96_session_erase_search;
GRANT SELECT(org_id,workspace_id,source,session_id,canonical_session_id) ON search_index.projection_state TO bulls96_session_erase_search;
COMMIT;
