-- Verifier for supabase/bootstrap/session_analysis_admit_role.sql (lane F1). Read-only; fails
-- (55000) naming every violation. bulls96_analysis_admit must be exactly:
--   LOGIN NOINHERIT, no SUPERUSER/CREATEDB/CREATEROLE/REPLICATION/BYPASSRLS, member of nothing;
--   USAGE on schema sessions and no CREATE there;
--   EXECUTE on exactly one SECURITY DEFINER sessions function: analysis_request_v1;
--   no privilege on any sessions table, column or sequence.
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  r pg_roles%ROWTYPE;
  v_problems text[] := '{}';
  v_row record;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = 'bulls96_analysis_admit';
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '42704', MESSAGE = 'bulls96_analysis_admit does not exist';
  END IF;
  IF NOT r.rolcanlogin THEN v_problems := array_append(v_problems, 'NOLOGIN'::text); END IF;
  IF r.rolinherit THEN v_problems := array_append(v_problems, 'INHERIT'::text); END IF;
  IF r.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF r.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF r.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF r.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  IF r.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members AS m JOIN pg_roles AS g ON g.oid = m.roleid WHERE m.member = r.oid LOOP
    v_problems := array_append(v_problems, format('member of %I', v_row.rolname)::text);
  END LOOP;
  -- Exactly the reviewed role settings (pg_db_role_setting, every database): statement_timeout
  -- 2s and default_transaction_read_only off; nothing else, nothing missing.
  IF (SELECT coalesce(array_agg(cfg ORDER BY cfg), '{}') FROM pg_db_role_setting AS d, unnest(d.setconfig) AS cfg
       WHERE d.setrole = r.oid) IS DISTINCT FROM ARRAY['default_transaction_read_only=off', 'statement_timeout=2s']::text[] THEN
    v_problems := array_append(v_problems, 'role settings are not exactly statement_timeout=2s, default_transaction_read_only=off'::text);
  END IF;
  IF NOT has_schema_privilege(r.oid, 'sessions', 'USAGE') THEN v_problems := array_append(v_problems, 'no USAGE on sessions'::text); END IF;
  IF has_schema_privilege(r.oid, 'sessions', 'CREATE') THEN v_problems := array_append(v_problems, 'CREATE on sessions'::text); END IF;
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef AND has_function_privilege(r.oid, p.oid, 'EXECUTE')
       AND p.oid::regprocedure::text <> 'sessions.analysis_request_v1(text,text,text,text)'
  LOOP
    v_problems := array_append(v_problems, format('EXECUTE on %s', v_row.fn)::text);
  END LOOP;
  IF NOT has_function_privilege(r.oid, 'sessions.analysis_request_v1(text,text,text,text)', 'EXECUTE') THEN
    v_problems := array_append(v_problems, 'no EXECUTE on analysis_request_v1'::text);
  END IF;
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
       AND (has_table_privilege(r.oid, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
            OR has_any_column_privilege(r.oid, c.oid, 'SELECT,INSERT,UPDATE,REFERENCES'))
  LOOP
    v_problems := array_append(v_problems, format('privilege on %s', v_row.rel)::text);
  END LOOP;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'bulls96_analysis_admit is not the reviewed admission principal: ' || array_to_string(v_problems, '; ');
  END IF;
END
$verify$;
