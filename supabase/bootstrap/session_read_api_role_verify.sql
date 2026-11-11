-- Read-only verification of the session read API role (session_read_api_role.sql).
-- Raises, listing every problem, unless the role has exactly the approved shape:
-- attributes, no memberships and no members (inbound, below), exactly the approved column SELECT on session_index and
-- schema USAGE as its effective privileges in schema sessions, EXECUTE on no sessions
-- SECURITY DEFINER function but the two tenant-setting directory reads of
-- 20261105034000 (sessions.list_session_directory_for, session_directory_activity_for;
-- allowed, not required, so this also passes before that migration), the two q-filtered
-- directory reads of 20261105046010 (sessions.query_session_directory_for,
-- query_session_directory_activity_for; likewise allowed, not required), the other
-- allow-listed read API functions below, and the
-- tenant-bound RLS policy on session_index.
--
-- Inbound membership: any role granted bulls96_read_api could SET ROLE to it and call the
-- read API's SECURITY DEFINER functions (e.g. probe the owner gate status through
-- sessions.buffered_search_gate_for), so the role must have no member. On PG16+ the only
-- tolerated member row is the Supabase creator edge that PostgreSQL adds itself when
-- Supabase's non-superuser `postgres` (CREATEROLE) creates the role: member `postgres`,
-- grantor the superuser `supabase_admin`, admin_option true, inherit_option false,
-- set_option false, and only with hoopdb.db_mode=supabase bound (on Supabase run this file
-- with PGOPTIONS='-c hoopdb.db_mode=supabase'). That edge can neither SET ROLE to the role
-- nor inherit its privileges. It is the same predicate as
-- supabase/bootstrap/acquisition_installer_verify.sql (#332) and session_read_api_roles.sql.
-- Before PG16 pg_auth_members has no inherit_option / set_option columns, so any member fails.
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_qual text;
  v_directory_qual constant text := '((org_id = current_setting(''bulls96_read.org_id''::text, true)) AND (workspace_id = current_setting(''bulls96_read.workspace_id''::text, true)))';
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
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
  IF current_setting('server_version_num')::int >= 160000 THEN
    FOR v_row IN EXECUTE 'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super, a.admin_option, a.inherit_option, a.set_option FROM pg_auth_members a JOIN pg_roles u ON u.oid = a.member JOIN pg_roles g ON g.oid = a.grantor WHERE a.roleid = $1' USING v_role.oid LOOP
      IF v_mode = 'supabase' AND v_row.rolname = 'postgres' AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- Supabase PG16+ creator edge: ADMIN only, no SET, no INHERIT
      ELSE
        v_problems := array_append(v_problems, (format('granted to %I', v_row.rolname))::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT u.rolname FROM pg_auth_members m JOIN pg_roles u ON u.oid = m.member WHERE m.roleid = v_role.oid LOOP
      v_problems := array_append(v_problems, (format('granted to %I', v_row.rolname))::text);
    END LOOP;
  END IF;

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
    ELSIF v_row.rel = 'sessions.session_index' THEN
      IF v_row.sel THEN v_problems := array_append(v_problems, ('whole-table SELECT on sessions.session_index')::text); END IF;
      IF NOT v_row.col_sel THEN v_problems := array_append(v_problems, ('no column SELECT on sessions.session_index')::text); END IF;
    ELSIF v_row.rel = 'sessions.session_directory' THEN
      -- Optional (a role from before this grant still passes); columns are pinned in the loop below.
      IF v_row.sel THEN v_problems := array_append(v_problems, ('whole-table SELECT on sessions.session_directory')::text); END IF;
    ELSIF v_row.sel OR v_row.col_sel THEN
      v_problems := array_append(v_problems, (format('SELECT on %s', v_row.rel))::text);
    END IF;
  END LOOP;
  FOR v_row IN
    SELECT a.attname,
           has_column_privilege(v_role.oid, 'sessions.session_index'::regclass, a.attnum, 'SELECT') AS sel
      FROM pg_attribute a
     WHERE a.attrelid = 'sessions.session_index'::regclass AND a.attnum > 0 AND NOT a.attisdropped
  LOOP
    IF v_row.sel <> (v_row.attname IN ('org_id', 'workspace_id', 'harness', 'session_id', 'created_at', 'deleted_at',
                                       'canonical_session_id', 'machine_id', 'canonical_machine_id', 'user_id',
                                       'storage_provider', 'bucket', 'manifest_key', 'content_sha256', 'raw_format', 'revision')) THEN
      v_problems := array_append(v_problems, (format('column %s: SELECT is %s', v_row.attname, CASE WHEN v_row.sel THEN 'granted' ELSE 'missing' END))::text);
    END IF;
  END LOOP;
  FOR v_row IN
    SELECT a.attname
      FROM pg_attribute a
     WHERE a.attrelid = 'sessions.session_directory'::regclass AND a.attnum > 0 AND NOT a.attisdropped
       AND has_column_privilege(v_role.oid, 'sessions.session_directory'::regclass, a.attnum, 'SELECT')
       AND a.attname NOT IN ('org_id', 'workspace_id', 'harness', 'session_id', 'event_projection_ready', 'event_projection_epoch')
  LOOP
    v_problems := array_append(v_problems, (format('column %s of sessions.session_directory: SELECT is granted', v_row.attname))::text);
  END LOOP;
  -- No other callable sessions definer function: the claim-bound RPCs refuse other roles
  -- anyway, but a definer function without such a check must not be reachable. The two
  -- allowed ones pin the tenant to this role's transaction settings
  -- (sessions.session_directory_read_api_scope).
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef AND has_function_privilege(v_role.oid, p.oid, 'EXECUTE')
       AND p.oid::regprocedure::text NOT IN (
         'sessions.analysis_read_v1(text,text)',
         'sessions.babysitter_evidence_page_for(text,jsonb)',
         'sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)',
         'sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)',
         'sessions.buffered_sessions_for(text,timestamp with time zone,text,text,integer,text,text,text,text,text,boolean,text)',
         'sessions.coordination_output_facts_for(text,timestamp with time zone,timestamp with time zone,text,integer)',
         'sessions.coordination_write_facts_for(text,timestamp with time zone,timestamp with time zone,text,integer)',
         'sessions.digest_narrative_read(text,date,text,text)',
         'sessions.list_session_directory_for(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer)',
         'sessions.output_facts_coverage_for(text,timestamp with time zone,timestamp with time zone)',
         'sessions.output_facts_events_for(text,timestamp with time zone,timestamp with time zone,text,integer)',
         'sessions.output_facts_outputs_for(text,timestamp with time zone,timestamp with time zone,text,integer)',
         'sessions.query_session_directory_activity_for(text,timestamp with time zone,timestamp with time zone,text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],boolean,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone)',
         'sessions.query_session_directory_for(text,text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],text[],boolean,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text,text,integer)',
         'sessions.read_api_token_delete_by_hash(text,text)',
         'sessions.read_api_token_delete_for(text,text,text,text)',
         'sessions.read_api_token_issue(text,text,text,text,text,text,text,text,timestamp with time zone,text,timestamp with time zone,text,timestamp with time zone)',
         'sessions.read_api_token_refresh(text,text,text,text,text,text,timestamp with time zone,text,timestamp with time zone,timestamp with time zone)',
         'sessions.read_api_token_verify(text,timestamp with time zone)',
         'sessions.session_brief_summary_for(text,text,text)',
         'sessions.session_brief_titles_for(text,text[],text[])',
         'sessions.session_directory_activity_for(text,timestamp with time zone,timestamp with time zone)',
         'sessions.session_summary_activity_for(text,timestamp with time zone,timestamp with time zone)')
  LOOP
    v_problems := array_append(v_problems, (format('EXECUTE on security definer %s', v_row.fn))::text);
  END LOOP;

  IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'sessions.session_index'::regclass) THEN
    v_problems := array_append(v_problems, ('RLS is off on sessions.session_index')::text);
  END IF;
  SELECT pg_get_expr(p.polqual, p.polrelid) INTO v_qual
    FROM pg_policy p
   WHERE p.polrelid = 'sessions.session_index'::regclass AND p.polname = 'session_index_read_api'
     AND p.polpermissive AND p.polcmd = 'r' AND p.polroles = ARRAY[v_role.oid];
  IF v_qual IS NULL THEN
    v_problems := array_append(v_problems, ('policy session_index_read_api is missing or not FOR SELECT TO bulls96_read_api only')::text);
  ELSIF v_qual IS DISTINCT FROM v_directory_qual THEN
    -- Same exact tenant predicate as the directory policy (session_read_api_role.sql):
    -- a LIKE on the setting names also accepts an OR/true bypass.
    v_problems := array_append(v_problems, ('policy session_index_read_api differs from the tenant predicate')::text);
  END IF;
  -- Column grants expose directory metadata directly, so the policy must
  -- actually enforce both tenant settings. Merely mentioning their names
  -- also accepts an OR/true bypass. Older roles without these optional grants
  -- still pass, but any granted directory access requires enabled RLS and
  -- the exact predicate installed by session_read_api_role.sql.
  IF EXISTS (SELECT 1 FROM pg_attribute a
              WHERE a.attrelid = 'sessions.session_directory'::regclass
                AND a.attnum > 0 AND NOT a.attisdropped
                AND has_column_privilege(v_role.oid, a.attrelid, a.attnum, 'SELECT')) THEN
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'sessions.session_directory'::regclass) THEN
      v_problems := array_append(v_problems, ('RLS is off on sessions.session_directory')::text);
    END IF;
    SELECT pg_get_expr(p.polqual, p.polrelid) INTO v_qual
      FROM pg_policy p
     WHERE p.polrelid = 'sessions.session_directory'::regclass AND p.polname = 'session_directory_read_api'
       AND p.polpermissive AND p.polcmd = 'r' AND p.polroles = ARRAY[v_role.oid];
    IF v_qual IS DISTINCT FROM v_directory_qual THEN
      v_problems := array_append(v_problems, ('policy session_directory_read_api is missing or differs from the tenant predicate')::text);
    END IF;
  END IF;
  FOR v_row IN
    SELECT p.polrelid::regclass::text AS rel, p.polname FROM pg_policy p
     WHERE v_role.oid = ANY(p.polroles)
       AND NOT (p.polname = 'session_index_read_api' AND p.polrelid = 'sessions.session_index'::regclass)
       AND NOT (p.polname = 'session_directory_read_api' AND p.polrelid = 'sessions.session_directory'::regclass
                AND p.polpermissive AND p.polcmd = 'r' AND p.polroles = ARRAY[v_role.oid]
                AND pg_get_expr(p.polqual, p.polrelid) = v_directory_qual)
  LOOP
    v_problems := array_append(v_problems, (format('unapproved policy %s on %s', v_row.polname, v_row.rel))::text);
  END LOOP;
  -- A PUBLIC policy (polroles {0}) also applies to bulls96_read_api and is
  -- OR-ed with the tenant policy, so any on the two tenant tables is refused.
  FOR v_row IN
    SELECT p.polrelid::regclass::text AS rel, p.polname FROM pg_policy p
     WHERE 0::oid = ANY(p.polroles)
       AND p.polrelid IN ('sessions.session_index'::regclass, 'sessions.session_directory'::regclass)
  LOOP
    v_problems := array_append(v_problems, (format('PUBLIC policy %s on %s', v_row.polname, v_row.rel))::text);
  END LOOP;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION 'session read api role is not in the approved shape: %', array_to_string(v_problems, '; ');
  END IF;
  RAISE NOTICE 'session read api role: PASS';
END
$verify$;
