-- Read-only verification of the session read API role (session_read_api_role.sql and
-- session_read_api_directory.sql). Raises, listing every problem, unless the role has
-- exactly the approved shape: attributes; no memberships either way (only Supabase's
-- creator edge to postgres may name it); as its effective privileges in schema sessions,
-- schema USAGE, exactly the six-column SELECT on session_index and exactly the
-- eleven-column SELECT on session_directory; no EXECUTE on any sessions security definer
-- function; and the tenant-bound RLS policy on each of those two tables, and no other.
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_qual text;
  v_table text;
  v_policy text;
  v_cols text[];
BEGIN
  SELECT * INTO v_role FROM pg_roles WHERE rolname = 'bulls96_read_api';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'session read api role: bulls96_read_api does not exist';
  END IF;
  IF NOT v_role.rolcanlogin THEN v_problems := array_append(v_problems, ('role cannot log in')::text); END IF;
  IF v_role.rolinherit THEN v_problems := array_append(v_problems, ('role has INHERIT')::text); END IF;
  IF v_role.rolbypassrls THEN v_problems := array_append(v_problems, ('role has BYPASSRLS')::text); END IF;
  IF v_role.rolsuper OR v_role.rolcreatedb OR v_role.rolcreaterole OR v_role.rolreplication THEN
    v_problems := array_append(v_problems, ('role has SUPERUSER, CREATEDB, CREATEROLE or REPLICATION')::text);
  END IF;
  IF NOT coalesce(v_role.rolconfig @> ARRAY['statement_timeout=2s'], false) THEN
    v_problems := array_append(v_problems, ('statement_timeout is not 2s')::text);
  END IF;
  IF NOT coalesce(v_role.rolconfig @> ARRAY['default_transaction_read_only=on'], false) THEN
    v_problems := array_append(v_problems, ('default_transaction_read_only is not on')::text);
  END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = v_role.oid LOOP
    v_problems := array_append(v_problems, (format('member of %I', v_row.rolname))::text);
  END LOOP;
  -- Nobody may act as this role: a member such as authenticator could SET ROLE to it
  -- (PostgREST with a forged role claim) and read the directory with tenant settings of
  -- its own choosing. The one accepted edge is PostgreSQL 16+'s automatic grant of a new
  -- role to its creator, postgres (Supabase's bootstrap runner, which owns these tables).
  FOR v_row IN SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member
                WHERE m.roleid = v_role.oid AND r.rolname <> 'postgres' LOOP
    v_problems := array_append(v_problems, (format('granted to %I', v_row.rolname))::text);
  END LOOP;

  -- Effective table privileges in sessions, however obtained.
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel,
           has_table_privilege(v_role.oid, c.oid, 'SELECT') AS sel,
           has_table_privilege(v_role.oid, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') AS write,
           has_any_column_privilege(v_role.oid, c.oid, 'SELECT') AS col_sel,
           has_any_column_privilege(v_role.oid, c.oid, 'INSERT, UPDATE, REFERENCES') AS col_write
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  LOOP
    IF v_row.write OR v_row.col_write THEN
      v_problems := array_append(v_problems, (format('write privilege on %s', v_row.rel))::text);
    ELSIF v_row.rel IN ('sessions.session_index', 'sessions.session_directory') THEN
      IF v_row.sel THEN v_problems := array_append(v_problems, (format('whole-table SELECT on %s', v_row.rel))::text); END IF;
      IF NOT v_row.col_sel THEN v_problems := array_append(v_problems, (format('no column SELECT on %s', v_row.rel))::text); END IF;
    ELSIF v_row.sel OR v_row.col_sel THEN
      v_problems := array_append(v_problems, (format('SELECT on %s', v_row.rel))::text);
    END IF;
  END LOOP;
  FOREACH v_table IN ARRAY ARRAY['sessions.session_index', 'sessions.session_directory'] LOOP
    v_cols := CASE v_table
      WHEN 'sessions.session_index' THEN
        ARRAY['org_id', 'workspace_id', 'harness', 'session_id', 'created_at', 'deleted_at']
      ELSE
        ARRAY['org_id', 'workspace_id', 'harness', 'session_id', 'title', 'task_title', 'git_repo',
              'search_text', 'last_activity_at', 'state', 'deleted_at']
    END;
    FOR v_row IN
      SELECT a.attname,
             has_column_privilege(v_role.oid, v_table::regclass, a.attnum, 'SELECT') AS sel
        FROM pg_attribute a
       WHERE a.attrelid = v_table::regclass AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
      IF v_row.sel <> (v_row.attname = ANY (v_cols)) THEN
        v_problems := array_append(v_problems, (format('%s column %s: SELECT is %s', v_table, v_row.attname, CASE WHEN v_row.sel THEN 'granted' ELSE 'missing' END))::text);
      END IF;
    END LOOP;
  END LOOP;
  -- No callable sessions function: the claim-bound RPCs refuse other roles anyway,
  -- but a definer function without such a check must not be reachable.
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef AND has_function_privilege(v_role.oid, p.oid, 'EXECUTE')
  LOOP
    v_problems := array_append(v_problems, (format('EXECUTE on security definer %s', v_row.fn))::text);
  END LOOP;

  FOREACH v_table IN ARRAY ARRAY['sessions.session_index', 'sessions.session_directory'] LOOP
    v_policy := CASE v_table WHEN 'sessions.session_index' THEN 'session_index_read_api' ELSE 'session_directory_read_api' END;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = v_table::regclass) THEN
      v_problems := array_append(v_problems, (format('RLS is off on %s', v_table))::text);
    END IF;
    SELECT pg_get_expr(p.polqual, p.polrelid) INTO v_qual
      FROM pg_policy p
     WHERE p.polrelid = v_table::regclass AND p.polname = v_policy
       AND p.polpermissive AND p.polcmd = 'r' AND p.polroles = ARRAY[v_role.oid];
    IF v_qual IS NULL THEN
      v_problems := array_append(v_problems, (format('policy %s is missing or not FOR SELECT TO bulls96_read_api only', v_policy))::text);
    -- Exactly the tenant binding, as PostgreSQL deparses it: a substring match would also
    -- accept the binding OR'ed with anything else.
    ELSIF v_qual IS DISTINCT FROM
          '((org_id = current_setting(''bulls96_read.org_id''::text, true)) AND (workspace_id = current_setting(''bulls96_read.workspace_id''::text, true)))' THEN
      v_problems := array_append(v_problems, (format('policy %s is not bound to the tenant settings', v_policy))::text);
    END IF;
    -- A permissive policy for PUBLIC would admit this role too, OR'ed with its own.
    FOR v_row IN
      SELECT p.polname FROM pg_policy p
       WHERE p.polrelid = v_table::regclass AND p.polpermissive AND p.polcmd IN ('r', '*')
         AND 0::oid = ANY (p.polroles)
    LOOP
      v_problems := array_append(v_problems, (format('policy %s on %s applies to PUBLIC', v_row.polname, v_table))::text);
    END LOOP;
  END LOOP;
  FOR v_row IN
    SELECT p.polrelid::regclass::text AS rel, p.polname FROM pg_policy p
     WHERE v_role.oid = ANY(p.polroles)
       AND (p.polrelid::regclass::text, p.polname) NOT IN
           (('sessions.session_index', 'session_index_read_api'), ('sessions.session_directory', 'session_directory_read_api'))
  LOOP
    v_problems := array_append(v_problems, (format('unapproved policy %s on %s', v_row.polname, v_row.rel))::text);
  END LOOP;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION 'session read api role is not in the approved shape: %', array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'session read api role: PASS';
END
$verify$;
