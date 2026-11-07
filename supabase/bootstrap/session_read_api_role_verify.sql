-- Read-only verification of the session read API role (session_read_api_role.sql).
-- Raises, listing every problem, unless the role has exactly the approved shape:
-- attributes, no memberships, exactly the approved column SELECT on session_index and
-- schema USAGE as its effective privileges in schema sessions, EXECUTE on no sessions
-- SECURITY DEFINER function but the two tenant-setting directory reads of
-- 20261105034000 (sessions.list_session_directory_for, session_directory_activity_for;
-- allowed, not required, so this also passes before that migration), and the
-- tenant-bound RLS policy on session_index. When migration 20261105048000 (portable
-- search reader contract) is present (either of its policies exists), it requires exactly its column SELECT on
-- sessions.search_projection_state (9 columns) and sessions.session_directory (6
-- columns) and its two exact tenant FOR SELECT policies; before that, both are refused.
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role pg_roles%ROWTYPE;
  v_problems text[] := ARRAY[]::text[];
  v_row record;
  v_qual text;
  -- 20261105048000 (portable search reader contract): all-or-nothing (see the body).
  v_portable boolean := false;
  -- regclass/regprocedure render without the schema when it is on search_path; pin a
  -- catalog-only path for this body so every rendered comparison is caller-independent,
  -- and restore the caller's value afterwards (transaction-local either way).
  v_saved_path text := current_setting('search_path');
  v_portable_cols constant jsonb := jsonb_build_object(
    'sessions.search_projection_state', jsonb_build_array('backend_id', 'canonical_session_id', 'complete', 'coverage_from_ms', 'org_id', 'session_id', 'source', 'source_epoch', 'workspace_id'),
    'sessions.session_directory', jsonb_build_array('event_projection_epoch', 'event_projection_ready', 'harness', 'org_id', 'session_id', 'workspace_id'));
  v_tenant_qual constant text := '((org_id = current_setting(''bulls96_read.org_id''::text, true)) AND (workspace_id = current_setting(''bulls96_read.workspace_id''::text, true)))';
BEGIN
  PERFORM set_config('search_path', 'pg_catalog, pg_temp', true);
  -- All-or-nothing: 048000 is present iff either of its two policies exists; then its
  -- complete exact surface is required, otherwise any part of it is refused. Independent of
  -- the ledger (test fixtures apply migrations without one) and of search_path.
  v_portable := EXISTS (SELECT 1 FROM pg_policy
                         WHERE (polname = 'search_projection_state_read_api' AND polrelid = to_regclass('sessions.search_projection_state'))
                            OR (polname = 'session_directory_portable_read_api' AND polrelid = to_regclass('sessions.session_directory')));
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

  -- Effective table privileges in sessions, however obtained.
  FOR v_row IN
    -- Explicit schema-qualified name: regclass rendering drops the schema when sessions is
    -- on the caller's search_path, which would misclassify approved grants.
    SELECT format('%I.%I', n.nspname, c.relname) AS rel,
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
    ELSIF v_portable AND v_portable_cols ? v_row.rel THEN
      IF v_row.sel THEN v_problems := array_append(v_problems, (format('whole-table SELECT on %s', v_row.rel))::text); END IF;
    ELSIF v_row.sel OR v_row.col_sel THEN
      v_problems := array_append(v_problems, (format('SELECT on %s', v_row.rel))::text);
    END IF;
  END LOOP;
  -- Exact column sets of the portable reader contract (only when 048000 is applied).
  IF v_portable THEN
    -- Every required contract column must exist (not dropped) and be granted; enumerated
    -- from the contract, not from surviving attributes.
    FOR v_row IN
      SELECT t.rel, c.col
        FROM jsonb_object_keys(v_portable_cols) AS t(rel), jsonb_array_elements_text(v_portable_cols -> t.rel) AS c(col)
       WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a
                          WHERE a.attrelid = to_regclass(t.rel) AND a.attname = c.col AND a.attnum > 0 AND NOT a.attisdropped)
    LOOP
      v_problems := array_append(v_problems, (format('%s required column %s is missing', v_row.rel, v_row.col))::text);
    END LOOP;
    FOR v_row IN
      SELECT t.rel, a.attname, has_column_privilege(v_role.oid, t.rel::regclass, a.attnum, 'SELECT') AS sel
        FROM jsonb_object_keys(v_portable_cols) AS t(rel)
        JOIN pg_attribute a ON a.attrelid = to_regclass(t.rel) AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
      IF v_row.sel <> (v_portable_cols -> v_row.rel) ? v_row.attname::text THEN
        v_problems := array_append(v_problems, (format('%s column %s: SELECT is %s', v_row.rel, v_row.attname,
          CASE WHEN v_row.sel THEN 'granted' ELSE 'missing' END))::text);
      END IF;
    END LOOP;
  END IF;
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
  -- No other callable sessions definer function: the claim-bound RPCs refuse other roles
  -- anyway, but a definer function without such a check must not be reachable. The two
  -- allowed ones pin the tenant to this role's transaction settings
  -- (sessions.session_directory_read_api_scope).
  FOR v_row IN
    SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.prosecdef AND has_function_privilege(v_role.oid, p.oid, 'EXECUTE')
       AND p.oid::regprocedure::text NOT IN (
         'sessions.list_session_directory_for(text,text[],text[],text,text[],text,timestamp with time zone,timestamp with time zone,timestamp with time zone,timestamp with time zone,text[],text[],timestamp with time zone,text,text,integer)',
         'sessions.portable_reader_readiness(text[],text)',
         'sessions.session_directory_activity_for(text,timestamp with time zone,timestamp with time zone)')
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
  ELSIF v_qual NOT LIKE '%current_setting(''bulls96_read.org_id''::text, true)%'
     OR v_qual NOT LIKE '%current_setting(''bulls96_read.workspace_id''::text, true)%' THEN
    v_problems := array_append(v_problems, ('policy session_index_read_api is not bound to the tenant settings')::text);
  END IF;
  FOR v_row IN
    SELECT p.polrelid, p.polname, p.polpermissive, p.polcmd, p.polroles,
           pg_get_expr(p.polqual, p.polrelid) AS q, p.polwithcheck IS NULL AS nocheck
      FROM pg_policy p
     WHERE (v_role.oid = ANY(p.polroles)
            -- Once 048000 is present the read role reads these two relations directly, so a
            -- policy applying to PUBLIC there applies to it too: include, never filter out.
            OR (v_portable AND 0 = ANY(p.polroles)
                AND p.polrelid IN (to_regclass('sessions.session_directory'), to_regclass('sessions.search_projection_state'))))
       AND NOT (p.polname = 'session_index_read_api' AND p.polrelid = 'sessions.session_index'::regclass)
  LOOP
    -- Relation identity by OID, never by search_path-dependent regclass rendering.
    IF v_portable AND v_row.polpermissive AND v_row.polcmd = 'r' AND v_row.polroles = ARRAY[v_role.oid]
       AND v_row.nocheck AND v_row.q = v_tenant_qual
       AND ((v_row.polname = 'search_projection_state_read_api' AND v_row.polrelid = to_regclass('sessions.search_projection_state'))
         OR (v_row.polname = 'session_directory_portable_read_api' AND v_row.polrelid = to_regclass('sessions.session_directory'))) THEN
      NULL;
    ELSE
      v_problems := array_append(v_problems, (format('unapproved policy %s on %s', v_row.polname, v_row.polrelid::regclass))::text);
    END IF;
  END LOOP;
  IF v_portable THEN
    -- Exact, not merely present by name: PERMISSIVE FOR SELECT TO the read role only, the
    -- tenant qual, no WITH CHECK.
    FOR v_row IN
      SELECT x.name, x.rel, EXISTS (
               SELECT 1 FROM pg_policy p
                WHERE p.polname = x.name AND p.polrelid = to_regclass(x.rel)
                  AND p.polpermissive AND p.polcmd = 'r' AND p.polroles = ARRAY[v_role.oid]
                  AND p.polwithcheck IS NULL AND pg_get_expr(p.polqual, p.polrelid) = v_tenant_qual) AS ok
        FROM (VALUES ('search_projection_state_read_api', 'sessions.search_projection_state'),
                     ('session_directory_portable_read_api', 'sessions.session_directory')) AS x(name, rel)
    LOOP
      IF NOT v_row.ok THEN
        v_problems := array_append(v_problems, (format('policy %s on %s is missing or not exactly FOR SELECT TO bulls96_read_api on the tenant settings', v_row.name, v_row.rel))::text);
      END IF;
    END LOOP;
    -- The readiness RPC is part of the 048000 unit: exactly owner-owned, SECURITY DEFINER,
    -- pinned search_path, EXECUTE for exactly the owner and the reader (no GRANT OPTION).
    IF to_regprocedure('sessions.portable_reader_readiness(text[],text)') IS NULL THEN
      v_problems := array_append(v_problems, ('readiness function sessions.portable_reader_readiness(text[],text) is missing')::text);
    ELSE
      IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
         WHERE p.oid = to_regprocedure('sessions.portable_reader_readiness(text[],text)')
           AND p.prosecdef AND p.provolatile = 'v' AND p.prorettype = 'text'::regtype
           AND p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
           AND p.proconfig = ARRAY['search_path=pg_catalog, pg_temp']
           -- The migration owner: owner of the 047000 table this contract extends.
           AND p.proowner = (SELECT relowner FROM pg_class WHERE oid = to_regclass('sessions.search_projection_state'))) THEN
        v_problems := array_append(v_problems, ('readiness function is not owner-owned VOLATILE SECURITY DEFINER with the pinned search_path')::text);
      END IF;
      -- The exact reviewed body, pinned by the sha256 of the committed definition (never
      -- inferred from the live catalog): a body replaced with an always-pass is refused.
      IF (SELECT encode(sha256(convert_to(p.prosrc, 'UTF8')), 'hex') FROM pg_proc p
           WHERE p.oid = to_regprocedure('sessions.portable_reader_readiness(text[],text)'))
         IS DISTINCT FROM '6f198ba75dc55787c7af275752540c0ee494976110687575cb3020ce93ef6795' THEN
        v_problems := array_append(v_problems, ('readiness function body differs from the reviewed definition')::text);
      END IF;
      -- Sorted by the computed grant key itself (an aggregate ORDER BY cannot name an
      -- output column; ORDER BY 1 would sort by a constant and keep physical ACL order),
      -- under COLLATE "C" so the customer database's locale/punctuation rules cannot reorder
      -- '<owner>' against role names and falsely refuse the exact set.
      IF (SELECT string_agg(g.k, ',' ORDER BY g.k COLLATE "C")
            FROM (SELECT CASE WHEN x.grantee = p.proowner THEN '<owner>' WHEN x.grantee = 0 THEN 'PUBLIC' ELSE x.grantee::regrole::text END
                         || ':' || x.privilege_type || CASE WHEN x.is_grantable THEN '*' ELSE '' END AS k
                    FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
                   WHERE p.oid = to_regprocedure('sessions.portable_reader_readiness(text[],text)')) AS g)
         IS DISTINCT FROM '<owner>:EXECUTE,bulls96_read_api:EXECUTE' THEN
        v_problems := array_append(v_problems, ('readiness function EXECUTE grants are not exactly the owner and bulls96_read_api')::text);
      END IF;
    END IF;
    -- No GRANT OPTION anywhere in the reader's column grants on the two relations.
    IF EXISTS (SELECT 1 FROM pg_attribute a, aclexplode(a.attacl) x
                WHERE a.attrelid IN (to_regclass('sessions.session_directory'), to_regclass('sessions.search_projection_state'))
                  AND a.attnum > 0 AND a.attacl IS NOT NULL AND x.grantee = v_role.oid AND x.is_grantable) THEN
      v_problems := array_append(v_problems, ('reader column SELECT WITH GRANT OPTION on the portable relations')::text);
    END IF;
    -- Row security must be on wherever the read role now reads directly.
    IF NOT coalesce((SELECT relrowsecurity FROM pg_class WHERE oid = to_regclass('sessions.session_directory')), false) THEN
      v_problems := array_append(v_problems, ('RLS is off on sessions.session_directory')::text);
    END IF;
    IF NOT coalesce((SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid = to_regclass('sessions.search_projection_state')), false) THEN
      v_problems := array_append(v_problems, ('RLS is off or not FORCEd on sessions.search_projection_state')::text);
    END IF;
  END IF;

  IF NOT v_portable AND to_regprocedure('sessions.portable_reader_readiness(text[],text)') IS NOT NULL THEN
    v_problems := array_append(v_problems, ('readiness function exists without the 048000 reader contract')::text);
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION 'session read api role is not in the approved shape: %', array_to_string(v_problems, '; ');
  END IF;
  PERFORM set_config('search_path', v_saved_path, true);
  RAISE NOTICE 'session read api role: PASS';
END
$verify$;
