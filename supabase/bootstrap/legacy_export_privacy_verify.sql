-- Read-only phase-2 verifier for the dedicated legacy-export privacy login.
-- Run after 062020 and 062030, before and after legacy_export_privacy_role.sql.
-- It accepts the bare NOLOGIN role or the one approved LOGIN timeout profile.
DO $verify$
DECLARE
 r pg_roles%ROWTYPE;v record;m record;v_oid oid;
 v_mode text:=coalesce(current_setting('hoopdb.db_mode',true),'');
 v_tables oid[]:=ARRAY[
  'sessions.session_erasure_receipts'::regclass,
  'sessions.upload_deletions'::regclass,
  'sessions.ingest_frozen_source_deletions'::regclass,
  'sessions.upload_resets'::regclass,
  'sessions.turn_session_fences'::regclass,
  'sessions.operator_user_erasures'::regclass
 ];
 v_fns oid[]:=ARRAY[
  'sessions.ingest_legacy_export_scope_authority_register(jsonb)'::regprocedure,
  'sessions.legacy_export_privacy_authority_ready()'::regprocedure
 ];
 v_table text:='SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'||
  CASE WHEN current_setting('server_version_num')::int>=170000 THEN ',MAINTAIN' ELSE '' END;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_legacy_export_privacy';
 IF NOT FOUND OR r.rolinherit OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy role attributes differ';END IF;
 IF EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) OR
    EXISTS(SELECT 1 FROM pg_db_role_setting WHERE setrole=r.oid AND setdatabase<>0)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy role membership or settings differ';END IF;
 IF r.rolcanlogin THEN
  IF coalesce((SELECT array_agg(x ORDER BY x) FROM unnest(r.rolconfig)x),'{}') IS DISTINCT FROM
    ARRAY['idle_in_transaction_session_timeout=30s','lock_timeout=5s','statement_timeout=20s']::text[]
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy LOGIN timeout profile differs';END IF;
 ELSIF coalesce(cardinality(r.rolconfig),0)<>0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy bare-role settings differ';
 END IF;
 IF current_setting('server_version_num')::int>=160000 THEN
  FOR m IN EXECUTE 'SELECT u.rolname,g.rolname AS grantor,g.rolsuper AS grantor_super,a.admin_option,a.inherit_option,a.set_option FROM pg_auth_members a JOIN pg_roles u ON u.oid=a.member JOIN pg_roles g ON g.oid=a.grantor WHERE a.roleid=$1' USING r.oid LOOP
   IF v_mode='supabase' AND m.rolname='postgres' AND m.grantor='supabase_admin' AND m.grantor_super AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option THEN NULL;
   ELSE RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy has an unexpected member';END IF;
  END LOOP;
 ELSIF EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid=r.oid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy has an unexpected member';
 END IF;
 IF NOT has_schema_privilege(r.oid,'sessions','USAGE') OR has_schema_privilege(r.oid,'sessions','CREATE')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy schema privileges differ';END IF;
 FOR v IN SELECT c.oid,c.relkind,c.relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' LOOP
  IF v.relkind IN('r','p','v','m','f') THEN
   IF v.oid=ANY(v_tables) THEN
    IF NOT has_table_privilege(r.oid,v.oid,'SELECT') OR has_table_privilege(r.oid,v.oid,replace(v_table,'SELECT,',''))
    THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('legacy export privacy: ledger privilege on %s',v.oid::regclass);END IF;
    IF NOT v.relrowsecurity OR NOT EXISTS(
      SELECT 1 FROM pg_policy p WHERE p.polrelid=v.oid AND p.polcmd='r' AND p.polpermissive
       AND r.oid=ANY(p.polroles) AND pg_get_expr(p.polqual,p.polrelid) IN('true','(true)')) OR EXISTS(
      SELECT 1 FROM pg_policy p WHERE p.polrelid=v.oid AND p.polcmd IN('r','*') AND NOT p.polpermissive
       AND (0=ANY(p.polroles) OR r.oid=ANY(p.polroles)))
    THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('legacy export privacy: ledger RLS on %s',v.oid::regclass);END IF;
   ELSIF has_table_privilege(r.oid,v.oid,v_table) OR has_any_column_privilege(r.oid,v.oid,'SELECT,INSERT,UPDATE,REFERENCES') THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('legacy export privacy: privilege on %s',v.oid::regclass);
   END IF;
  ELSIF v.relkind='S' AND has_sequence_privilege(r.oid,v.oid,'USAGE,SELECT,UPDATE') THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy has effective sequence privilege';
  END IF;
 END LOOP;
 FOREACH v_oid IN ARRAY v_fns LOOP
  IF NOT has_function_privilege(r.oid,v_oid,'EXECUTE') OR NOT (SELECT prosecdef FROM pg_proc WHERE oid=v_oid)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export privacy approved RPC differs';END IF;
 END LOOP;
 FOR v IN SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' LOOP
  IF has_function_privilege(r.oid,v.oid,'EXECUTE') IS DISTINCT FROM (v.oid=ANY(v_fns))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('legacy export privacy: unexpected EXECUTE on %s',v.oid::regprocedure);END IF;
 END LOOP;
END $verify$;
