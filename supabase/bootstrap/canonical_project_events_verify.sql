-- Read-only phase2: bare NOLOGIN or approved LOGIN timeout profile.
-- Exact ingest RPC OIDs derive from the combined ACL manifest; no effective
-- table/column/sequence rights, including PUBLIC and PG17 MAINTAIN.
DO $verify$
DECLARE r pg_roles%ROWTYPE; m record;v record;v_mode text:=coalesce(current_setting('hoopdb.db_mode',true),'');
 v_allowed text[] := ARRAY[
 'sessions.canonical_project_event_ack_v1(jsonb)',
 'sessions.canonical_project_event_read_v1(jsonb)',
 'sessions.canonical_project_events_pending_v1(jsonb)'
 ]::text[];
 v_oids oid[];
 v_table text:='SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'||
  CASE WHEN current_setting('server_version_num')::int>=170000 THEN ',MAINTAIN' ELSE '' END;
BEGIN
 SELECT array_agg(to_regprocedure(fn)::oid) INTO v_oids FROM unnest(v_allowed) fn;
 IF cardinality(v_oids)<>cardinality(v_allowed) OR array_position(v_oids,NULL) IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events RPC missing';END IF;
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_project_events';
 IF NOT FOUND OR r.rolinherit OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events role attributes differ';END IF;
 IF EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) OR
    EXISTS(SELECT 1 FROM pg_db_role_setting WHERE setrole=r.oid AND setdatabase<>0)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events role membership or settings differ';END IF;
 IF r.rolcanlogin THEN
  IF coalesce((SELECT array_agg(x ORDER BY x) FROM unnest(r.rolconfig)x),'{}') IS DISTINCT FROM
    ARRAY['idle_in_transaction_session_timeout=10s','lock_timeout=2s','statement_timeout=5s']::text[]
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events LOGIN timeout profile differs';END IF;
 ELSIF coalesce(cardinality(r.rolconfig),0)<>0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events bare-role settings differ';
 END IF;
 IF current_setting('server_version_num')::int>=160000 THEN
  FOR m IN EXECUTE 'SELECT u.rolname,u.rolinherit,u.rolcanlogin,u.rolsuper,u.rolcreaterole,u.rolcreatedb,u.rolreplication,u.rolbypassrls,g.rolname AS grantor,g.rolsuper AS grantor_super,a.admin_option,a.inherit_option,a.set_option FROM pg_auth_members a JOIN pg_roles u ON u.oid=a.member JOIN pg_roles g ON g.oid=a.grantor WHERE a.roleid=$1' USING r.oid LOOP
   IF v_mode='supabase' AND m.rolname='postgres' AND m.grantor='supabase_admin' AND m.grantor_super AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option THEN NULL;
   ELSE RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events has an unexpected member';END IF;
  END LOOP;
 ELSE
  IF EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid=r.oid)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events has an unexpected member';END IF;
 END IF;
 IF NOT has_schema_privilege(r.oid,'sessions','USAGE') OR has_schema_privilege(r.oid,'sessions','CREATE')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events schema privileges differ';END IF;
 FOR v IN SELECT c.oid,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' LOOP
  IF v.relkind IN('r','p','v','m','f') AND
   (has_table_privilege(r.oid,v.oid,v_table) OR has_any_column_privilege(r.oid,v.oid,'SELECT,INSERT,UPDATE,REFERENCES'))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('canonical project events: privilege on %s',v.oid::regclass);END IF;
  IF v.relkind='S' AND has_sequence_privilege(r.oid,v.oid,'USAGE,SELECT,UPDATE')
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events has effective sequence privilege';END IF;
 END LOOP;
 FOR v IN SELECT p.oid,p.oid::regprocedure::text AS fn,p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' LOOP
  IF has_function_privilege(r.oid,v.oid,'EXECUTE') IS DISTINCT FROM (v.oid=ANY(v_oids))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('canonical project events: unexpected EXECUTE on %s',v.oid::regprocedure);END IF;
  IF v.oid=ANY(v_oids) AND NOT v.prosecdef
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events RPC is not SECURITY DEFINER';END IF;
 END LOOP;
 IF (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.oid=ANY(v_oids))<>cardinality(v_allowed)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical project events RPC missing';END IF;
END $verify$;
