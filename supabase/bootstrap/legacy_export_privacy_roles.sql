-- Privileged pre-migration bootstrap for the one-shot legacy export's privacy
-- reader. The migration owner never creates roles. The role may later be made
-- LOGIN by legacy_export_privacy_role.sql; this bootstrap never changes an
-- existing role and accepts that operator-owned login/settings state.
DO $bootstrap$
DECLARE
 r pg_roles%ROWTYPE;
 me record;
 v_mode text:=coalesce(current_setting('hoopdb.db_mode',true),'');
 v_version int:=current_setting('server_version_num')::int;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_legacy_export_privacy';
 IF NOT FOUND THEN
  SELECT rolsuper,rolcreaterole INTO me FROM pg_roles WHERE rolname=current_user;
  IF NOT me.rolsuper
    AND NOT (me.rolcreaterole AND v_version<160000)
    AND NOT (v_version>=160000 AND current_user='postgres' AND me.rolcreaterole
      AND (v_mode='supabase' OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname='supabase_admin' AND rolsuper)))
  THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='bootstrap bulls96_legacy_export_privacy: caller may not create it';END IF;
  CREATE ROLE bulls96_legacy_export_privacy NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  SELECT * INTO STRICT r FROM pg_roles WHERE rolname='bulls96_legacy_export_privacy';
 END IF;
 IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bootstrap bulls96_legacy_export_privacy: role exists elevated';END IF;
 IF EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='bootstrap bulls96_legacy_export_privacy: role is a member of another role';END IF;
END $bootstrap$;
