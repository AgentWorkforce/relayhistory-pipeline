-- Read-only verification of the session ingest role (session_ingest_role.sql).
-- Raises, listing every problem, unless the role has exactly the approved shape:
-- attributes and settings, no memberships, no table or column privilege in schema
-- sessions, schema USAGE, and EXECUTE on exactly the eight ingest functions (claim, commit, selection, reset,
-- acquire and release a slot) among the
-- SECURITY DEFINER sessions functions.
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_allowed text[] := ARRAY['sessions.ingest_claim_batch(jsonb)', 'sessions.ingest_claim_buffered_batch(jsonb)', 'sessions.ingest_prepare_buffer_stage(jsonb)', 'sessions.ingest_commit_buffered_batch(jsonb)', 'sessions.ingest_commit_batch(jsonb)',
                            'sessions.ingest_acquire_slot(jsonb)', 'sessions.ingest_release_slot(jsonb)',
                            'sessions.ingest_set_session_uploads(jsonb)', 'sessions.ingest_reset_uploaded_sessions(jsonb)',
                            'sessions.ingest_fanout_fence(jsonb)', 'sessions.ingest_commit_revisions(jsonb)'];
BEGIN
  SELECT * INTO v_role FROM pg_roles WHERE rolname = 'bulls96_session_ingest';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'session ingest role: bulls96_session_ingest does not exist';
  END IF;
  IF NOT v_role.rolcanlogin THEN v_problems := array_append(v_problems, ('role cannot log in')::text); END IF;
  IF v_role.rolinherit THEN v_problems := array_append(v_problems, ('role has INHERIT')::text); END IF;
  IF v_role.rolbypassrls THEN v_problems := array_append(v_problems, ('role has BYPASSRLS')::text); END IF;
  IF v_role.rolsuper OR v_role.rolcreatedb OR v_role.rolcreaterole OR v_role.rolreplication THEN
    v_problems := array_append(v_problems, ('role has SUPERUSER, CREATEDB, CREATEROLE or REPLICATION')::text);
  END IF;
  IF NOT coalesce(v_role.rolconfig @> ARRAY['statement_timeout=5s'], false) THEN
    v_problems := array_append(v_problems, ('statement_timeout is not 5s')::text);
  END IF;
  IF NOT coalesce(v_role.rolconfig @> ARRAY['lock_timeout=2s'], false) THEN
    v_problems := array_append(v_problems, ('lock_timeout is not 2s')::text);
  END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = v_role.oid LOOP
    v_problems := array_append(v_problems, (format('member of %I', v_row.rolname))::text);
  END LOOP;

  -- No table, view or column privilege of any kind in sessions, however obtained.
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
       AND (has_table_privilege(v_role.oid, c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
            OR has_any_column_privilege(v_role.oid, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))
  LOOP
    v_problems := array_append(v_problems, (format('privilege on %s', v_row.rel))::text);
  END LOOP;
  IF NOT has_schema_privilege(v_role.oid, 'sessions', 'USAGE') THEN
    v_problems := array_append(v_problems, ('no USAGE on schema sessions')::text);
  END IF;
  IF has_schema_privilege(v_role.oid, 'sessions', 'CREATE') THEN
    v_problems := array_append(v_problems, ('CREATE on schema sessions')::text);
  END IF;

  -- EXECUTE on exactly the two ingest functions among SECURITY DEFINER sessions
  -- functions. An invoker function runs with the role's own privileges, which are
  -- none above, so it cannot widen what the role reaches.
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn,
           has_function_privilege(v_role.oid, p.oid, 'EXECUTE') AS can
      FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef
  LOOP
    IF v_row.can AND NOT (v_row.fn = ANY (v_allowed)) THEN
      v_problems := array_append(v_problems, (format('EXECUTE on %s', v_row.fn))::text);
    ELSIF NOT v_row.can AND v_row.fn = ANY (v_allowed) THEN
      v_problems := array_append(v_problems, (format('no EXECUTE on %s', v_row.fn))::text);
    END IF;
  END LOOP;
  IF (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'sessions' AND p.oid::regprocedure::text = ANY (v_allowed) AND p.prosecdef) <> cardinality(v_allowed) THEN
    v_problems := array_append(v_problems, ('the eight ingest functions are missing or not SECURITY DEFINER')::text);
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION 'session ingest role is not in the approved shape: %', array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'session ingest role: PASS';
END
$verify$;
