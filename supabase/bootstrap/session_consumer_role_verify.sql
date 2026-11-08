-- Exact-shape verification for the self-host search-index writer role.
-- Fails on any drift (attributes, membership, missing or extra grants,
-- missing or extra sessions-table writer policies, any search-table policy
-- that is not exactly the tenant gate (writer-targeted or permissive TO
-- PUBLIC), PUBLIC or anonymous access, direct database or default or column
-- grants, wrong ownership). Emits a NOTICE pass line on success.
--
-- Run as the schema owner after session_consumer_role.sql:
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_consumer_role_verify.sql
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role text := 'search_consumer_runtime';
  r pg_roles%ROWTYPE;
  v_oid oid;
  v_bad text;
  v_extra text;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = v_role;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role is missing', v_role);
  END IF;
  v_oid := r.oid;
  IF NOT r.rolcanlogin OR r.rolinherit OR r.rolbypassrls OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role attributes drifted', v_role);
  END IF;
  -- Same creator-edge tolerance as the role file: exactly the invoker's
  -- admin-only membership, nothing else. pg_auth_members only carries the
  -- inherit/set columns on PG16+, so older servers check the admin flag.
  IF current_setting('server_version_num')::integer >= 160000 THEN
    IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
               WHERE (a.roleid = r.oid OR a.member = r.oid)
                 AND NOT (a.roleid = r.oid AND m.rolname = current_user AND a.admin_option
                          AND NOT a.inherit_option AND NOT a.set_option)) THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role membership drifted', v_role);
    END IF;
  ELSE
    IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
               WHERE (a.roleid = r.oid OR a.member = r.oid)
                 AND NOT (a.roleid = r.oid AND m.rolname = current_user AND a.admin_option)) THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role membership drifted', v_role);
    END IF;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role AND rolconfig IS NOT NULL
                  AND EXISTS (SELECT 1 FROM unnest(rolconfig) u WHERE u = 'statement_timeout=60s')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: statement_timeout is not 60s', v_role);
  END IF;

  -- Schema access: USAGE and nothing else.
  FOR v_bad IN SELECT 'sessions' UNION ALL SELECT 'search_index' LOOP
    IF NOT has_schema_privilege(v_role, v_bad, 'USAGE') THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: %s USAGE is missing', v_role, v_bad);
    END IF;
    IF has_schema_privilege(v_role, v_bad, 'CREATE') THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: %s CREATE must not be granted', v_role, v_bad);
    END IF;
  END LOOP;

  -- Exact per-privilege table grants: has_table_privilege with a privilege
  -- LIST is an OR (any one passes), so every required privilege is asserted
  -- alone; the unlisted-grant enumeration below refuses anything extra.
  -- event_pending carries INSERT: the session_index trigger inserts the
  -- dirty marker as the writer (SECURITY INVOKER), so the pointer path fails
  -- 42501 without it.
  FOR v_bad IN
    SELECT x.t || ' ' || x.p FROM (VALUES
      ('sessions.session_index', 'SELECT'), ('sessions.session_index', 'INSERT'),
      ('sessions.session_index', 'UPDATE'), ('sessions.session_index', 'DELETE'),
      ('sessions.session_index_ext', 'SELECT'), ('sessions.session_index_ext', 'INSERT'),
      ('sessions.session_index_ext', 'UPDATE'),
      ('sessions.session_directory', 'SELECT'), ('sessions.session_directory', 'INSERT'),
      ('sessions.session_directory', 'UPDATE'), ('sessions.session_directory', 'DELETE'),
      ('sessions.session_directory_users', 'SELECT'), ('sessions.session_directory_users', 'INSERT'),
      ('sessions.session_directory_users', 'UPDATE'), ('sessions.session_directory_users', 'DELETE'),
      ('sessions.session_directory_machines', 'SELECT'), ('sessions.session_directory_machines', 'INSERT'),
      ('sessions.session_directory_machines', 'UPDATE'), ('sessions.session_directory_machines', 'DELETE'),
      ('sessions.session_briefs', 'SELECT'),
      ('sessions.repos', 'SELECT'), ('sessions.repos', 'INSERT'), ('sessions.repos', 'UPDATE'),
      ('sessions.repo_names', 'SELECT'), ('sessions.repo_names', 'INSERT'), ('sessions.repo_names', 'UPDATE'),
      ('sessions.session_directory_event_pending', 'SELECT'), ('sessions.session_directory_event_pending', 'INSERT'),
      ('sessions.session_directory_event_pending', 'UPDATE'),
      ('sessions.session_directory_event_times', 'SELECT'), ('sessions.session_directory_event_times', 'INSERT'),
      ('sessions.session_directory_event_times', 'DELETE'),
      ('sessions.__96bulls_migrations', 'SELECT'), ('sessions.__96bulls_role_validation', 'SELECT'),
      ('search_index.transcript_events', 'SELECT'), ('search_index.transcript_events', 'INSERT'),
      ('search_index.transcript_events', 'UPDATE'), ('search_index.transcript_events', 'DELETE'),
      ('search_index.projection_state', 'SELECT'), ('search_index.projection_state', 'INSERT'),
      ('search_index.projection_state', 'UPDATE'), ('search_index.projection_state', 'DELETE'),
      ('sessions.search_consumer_pending', 'SELECT'), ('sessions.search_consumer_pending', 'INSERT'),
      ('sessions.search_consumer_pending', 'UPDATE'), ('sessions.search_consumer_pending', 'DELETE'),
      ('sessions.search_consumer_progress', 'SELECT'), ('sessions.search_consumer_progress', 'INSERT'),
      ('sessions.search_consumer_progress', 'UPDATE')
    ) AS x(t, p)
    WHERE NOT has_table_privilege(v_role, x.t, x.p)
  LOOP
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: missing grant %s', v_role, v_bad);
  END LOOP;
  -- Privileges the writer must never hold (the enumeration below is the
  -- backstop; these pin the dangerous ones by name).
  FOR v_bad IN
    SELECT x.t || ' ' || x.p FROM (VALUES
      ('sessions.session_index', 'TRUNCATE'), ('sessions.session_index', 'REFERENCES'), ('sessions.session_index', 'TRIGGER'),
      ('sessions.session_index_ext', 'DELETE'), ('sessions.session_index_ext', 'TRUNCATE'),
      ('sessions.session_index_ext', 'REFERENCES'), ('sessions.session_index_ext', 'TRIGGER'),
      ('sessions.session_directory', 'TRUNCATE'), ('sessions.session_directory', 'REFERENCES'), ('sessions.session_directory', 'TRIGGER'),
      ('sessions.session_directory_users', 'TRUNCATE'), ('sessions.session_directory_users', 'REFERENCES'), ('sessions.session_directory_users', 'TRIGGER'),
      ('sessions.session_directory_machines', 'TRUNCATE'), ('sessions.session_directory_machines', 'REFERENCES'), ('sessions.session_directory_machines', 'TRIGGER'),
      ('sessions.session_briefs', 'INSERT'), ('sessions.session_briefs', 'UPDATE'), ('sessions.session_briefs', 'DELETE'),
      ('sessions.session_briefs', 'TRUNCATE'), ('sessions.session_briefs', 'REFERENCES'), ('sessions.session_briefs', 'TRIGGER'),
      ('sessions.repos', 'DELETE'), ('sessions.repos', 'TRUNCATE'), ('sessions.repos', 'REFERENCES'), ('sessions.repos', 'TRIGGER'),
      ('sessions.repo_names', 'DELETE'), ('sessions.repo_names', 'TRUNCATE'), ('sessions.repo_names', 'REFERENCES'), ('sessions.repo_names', 'TRIGGER'),
      ('sessions.session_directory_event_pending', 'DELETE'), ('sessions.session_directory_event_pending', 'TRUNCATE'),
      ('sessions.session_directory_event_pending', 'REFERENCES'), ('sessions.session_directory_event_pending', 'TRIGGER'),
      ('sessions.session_directory_event_times', 'UPDATE'), ('sessions.session_directory_event_times', 'TRUNCATE'),
      ('sessions.session_directory_event_times', 'REFERENCES'), ('sessions.session_directory_event_times', 'TRIGGER'),
      ('sessions.__96bulls_migrations', 'INSERT'), ('sessions.__96bulls_migrations', 'UPDATE'), ('sessions.__96bulls_migrations', 'DELETE'),
      ('sessions.__96bulls_migrations', 'TRUNCATE'), ('sessions.__96bulls_migrations', 'REFERENCES'), ('sessions.__96bulls_migrations', 'TRIGGER'),
      ('sessions.__96bulls_role_validation', 'INSERT'), ('sessions.__96bulls_role_validation', 'UPDATE'), ('sessions.__96bulls_role_validation', 'DELETE'),
      ('sessions.__96bulls_role_validation', 'TRUNCATE'), ('sessions.__96bulls_role_validation', 'REFERENCES'), ('sessions.__96bulls_role_validation', 'TRIGGER'),
      ('search_index.transcript_events', 'TRUNCATE'), ('search_index.transcript_events', 'REFERENCES'), ('search_index.transcript_events', 'TRIGGER'),
      ('search_index.projection_state', 'TRUNCATE'), ('search_index.projection_state', 'REFERENCES'), ('search_index.projection_state', 'TRIGGER'),
      ('sessions.search_consumer_pending', 'TRUNCATE'), ('sessions.search_consumer_pending', 'REFERENCES'), ('sessions.search_consumer_pending', 'TRIGGER'),
      ('sessions.search_consumer_progress', 'DELETE'), ('sessions.search_consumer_progress', 'TRUNCATE'),
      ('sessions.search_consumer_progress', 'REFERENCES'), ('sessions.search_consumer_progress', 'TRIGGER')
    ) AS x(t, p)
    WHERE has_table_privilege(v_role, x.t, x.p)
  LOOP
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: forbidden grant %s', v_role, v_bad);
  END LOOP;
  IF NOT has_schema_privilege(v_role, 'search_index', 'USAGE') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: search_index USAGE is missing', v_role);
  END IF;

  -- No table, sequence, function or schema privilege anywhere else: catches
  -- extra TRUNCATE, grants on other tables, and any future GRANT the role
  -- file does not list. aclexplode (not composite field access) so the
  -- check runs on every Postgres. The writer's own grants are matched
  -- database-wide (an extra grant anywhere is drift); PUBLIC grants are
  -- matched inside the writer's two schemas only (a PUBLIC grant there
  -- silently widens the writer, while other schemas' PUBLIC defaults are
  -- out of scope). Sessions functions with a NULL ACL carry the built-in
  -- PUBLIC EXECUTE default and are pinned separately below.
  FOR v_extra IN
    WITH held AS (
      SELECT c.oid::regclass::text AS obj, a.privilege_type AS priv, COALESCE(gr.rolname, 'PUBLIC') AS grantee
        FROM pg_class c CROSS JOIN aclexplode(c.relacl) a LEFT JOIN pg_roles gr ON gr.oid = a.grantee
       WHERE gr.rolname = v_role
          OR (a.grantee = 0 AND c.oid::regclass::text LIKE ANY (ARRAY['sessions.%', 'search_index.%']))
      UNION ALL
      SELECT p.oid::regprocedure::text, a.privilege_type, COALESCE(gr.rolname, 'PUBLIC')
        FROM pg_proc p CROSS JOIN aclexplode(p.proacl) a LEFT JOIN pg_roles gr ON gr.oid = a.grantee
       WHERE gr.rolname = v_role
          OR (a.grantee = 0 AND p.pronamespace = to_regnamespace('sessions'))
      UNION ALL
      SELECT n.nspname, a.privilege_type, COALESCE(gr.rolname, 'PUBLIC')
        FROM pg_namespace n CROSS JOIN aclexplode(n.nspacl) a LEFT JOIN pg_roles gr ON gr.oid = a.grantee
       WHERE (gr.rolname = v_role OR a.grantee = 0) AND n.nspname IN ('sessions', 'search_index')
      UNION ALL
      SELECT p.oid::regprocedure::text, 'EXECUTE', 'PUBLIC'
        FROM pg_proc p WHERE p.pronamespace = to_regnamespace('sessions') AND p.proacl IS NULL
    ), allowed(obj, priv) AS (
      VALUES
        ('sessions', 'USAGE'), ('search_index', 'USAGE'),
        ('sessions.session_index', 'SELECT'), ('sessions.session_index', 'INSERT'),
        ('sessions.session_index', 'UPDATE'), ('sessions.session_index', 'DELETE'),
        ('sessions.session_index_ext', 'SELECT'), ('sessions.session_index_ext', 'INSERT'),
        ('sessions.session_index_ext', 'UPDATE'),
        ('sessions.session_directory', 'SELECT'), ('sessions.session_directory', 'INSERT'),
        ('sessions.session_directory', 'UPDATE'), ('sessions.session_directory', 'DELETE'),
        ('sessions.session_directory_users', 'SELECT'), ('sessions.session_directory_users', 'INSERT'),
        ('sessions.session_directory_users', 'UPDATE'), ('sessions.session_directory_users', 'DELETE'),
        ('sessions.session_directory_machines', 'SELECT'), ('sessions.session_directory_machines', 'INSERT'),
        ('sessions.session_directory_machines', 'UPDATE'), ('sessions.session_directory_machines', 'DELETE'),
        ('sessions.session_briefs', 'SELECT'),
        ('sessions.repos', 'SELECT'), ('sessions.repos', 'INSERT'), ('sessions.repos', 'UPDATE'),
        ('sessions.repo_names', 'SELECT'), ('sessions.repo_names', 'INSERT'), ('sessions.repo_names', 'UPDATE'),
        ('sessions.session_directory_event_pending', 'SELECT'), ('sessions.session_directory_event_pending', 'INSERT'),
        ('sessions.session_directory_event_pending', 'UPDATE'),
        ('sessions.session_directory_event_times', 'SELECT'), ('sessions.session_directory_event_times', 'INSERT'),
        ('sessions.session_directory_event_times', 'DELETE'),
        ('sessions.__96bulls_migrations', 'SELECT'), ('sessions.__96bulls_role_validation', 'SELECT'),
        ('search_index.transcript_events', 'SELECT'), ('search_index.transcript_events', 'INSERT'),
        ('search_index.transcript_events', 'UPDATE'), ('search_index.transcript_events', 'DELETE'),
        ('search_index.projection_state', 'SELECT'), ('search_index.projection_state', 'INSERT'),
        ('search_index.projection_state', 'UPDATE'), ('search_index.projection_state', 'DELETE'),
        ('sessions.search_consumer_pending', 'SELECT'), ('sessions.search_consumer_pending', 'INSERT'),
        ('sessions.search_consumer_pending', 'UPDATE'), ('sessions.search_consumer_pending', 'DELETE'),
        ('sessions.search_consumer_progress', 'SELECT'), ('sessions.search_consumer_progress', 'INSERT'),
        ('sessions.search_consumer_progress', 'UPDATE'),
        ('sessions.upsert_session_index(jsonb)', 'EXECUTE'),
        ('sessions.session_index_batch(jsonb)', 'EXECUTE'),
        ('sessions.session_publication_is_managed(text,text,text,text,text)', 'EXECUTE'),
        ('sessions.applied_repo_evidence(jsonb,jsonb)', 'EXECUTE'),
        ('sessions.canonical_remote(text)', 'EXECUTE'),
        ('sessions.clean_root_commit(text)', 'EXECUTE'),
        ('sessions.lock_repo_catalog(text)', 'EXECUTE'),
        ('sessions.resolve_repo(text,text,text,timestamp with time zone)', 'EXECUTE')
    )
    -- held.grantee is a role name (or 'PUBLIC' for grantee 0): the writer's
    -- own rows must all be allowlisted, and any PUBLIC row in its schemas is
    -- drift on its own (PUBLIC is never allowlisted).
    SELECT DISTINCT h.obj || ' ' || h.priv || ' to ' || h.grantee FROM held h
      WHERE (h.grantee = v_role AND NOT EXISTS (SELECT 1 FROM allowed a WHERE a.obj = h.obj AND a.priv = h.priv))
         OR h.grantee = 'PUBLIC'
  LOOP
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: unlisted grant %s', v_role, v_extra);
  END LOOP;

  -- No direct database privilege for the writer: it connects through the
  -- PUBLIC CONNECT baseline like every role, and any grant naming it is
  -- drift. (Database-level grants are invisible to the table/function
  -- enumeration above.)
  IF EXISTS (SELECT 1 FROM pg_database d, aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a
             WHERE d.datname = current_database() AND a.grantee = v_oid) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: direct database grant', v_role);
  END IF;
  -- No default privileges may name the writer, as the role or as a grantee:
  -- a default would silently grant on every future object.
  IF EXISTS (SELECT 1 FROM pg_default_acl d
             WHERE d.defaclrole = v_oid
                OR EXISTS (SELECT 1 FROM aclexplode(d.defaclacl) a WHERE a.grantee = v_oid)) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: default privilege names the role', v_role);
  END IF;
  -- No column privilege may name the writer in its two schemas: the grant
  -- contract is table-level only.
  IF EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('sessions', 'search_index') AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attacl IS NOT NULL
               AND EXISTS (SELECT 1 FROM aclexplode(a.attacl) x WHERE x.grantee = v_oid)) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: column grant names the role', v_role);
  END IF;

  -- Named function execution only: the allowlisted pointer functions plus the
  -- repo-catalog helpers the pointer upsert calls as the writer. The
  -- publication guard is SECURITY DEFINER and the writer must call it, so it
  -- is allowlisted exactly; no other SECURITY DEFINER function may execute.
  FOR v_bad IN
    SELECT p.oid::regprocedure::text FROM pg_proc p
    WHERE p.pronamespace = to_regnamespace('sessions')
      AND p.proname IN ('session_index_batch', 'session_publication_is_managed', 'upsert_session_index',
        'applied_repo_evidence', 'canonical_remote', 'clean_root_commit', 'lock_repo_catalog', 'resolve_repo')
      AND NOT has_function_privilege(v_role, p.oid, 'EXECUTE')
  LOOP
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: missing EXECUTE on %s', v_role, v_bad);
  END LOOP;
  SELECT p.oid::regprocedure::text INTO v_bad FROM pg_proc p
    WHERE p.pronamespace IN (to_regnamespace('sessions'), to_regnamespace('search_index')) AND p.prosecdef
      AND p.proname <> 'session_publication_is_managed'
      AND has_function_privilege(v_role, p.oid, 'EXECUTE') LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: executes SECURITY DEFINER %s', v_role, v_bad);
  END IF;

  -- Transparent writer RLS on the sessions tables: exactly the permissive
  -- writer policy by definition, and nothing else targeting the role. The
  -- member-read policies stay untouched (TO authenticated).
  FOR v_bad IN
    SELECT format('%s.%s', n.nspname, c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'sessions' AND c.relname IN ('session_index', 'session_index_ext', 'session_directory',
        'session_directory_users', 'session_directory_machines', 'session_briefs',
        'session_directory_event_pending', 'session_directory_event_times',
        'repos', 'repo_names',
        'search_consumer_pending', 'search_consumer_progress')
  LOOP
    IF v_bad IN ('sessions.search_consumer_pending', 'sessions.search_consumer_progress') THEN
      -- Writer tables are never RLS-gated: RLS would blind the durable queue.
      IF EXISTS (SELECT 1 FROM pg_class c2 JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                 WHERE format('%s.%s', n2.nspname, c2.relname) = v_bad AND c2.relrowsecurity) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: RLS must stay off %s', v_role, v_bad);
      END IF;
      CONTINUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname = split_part(v_bad, '.', 1) AND tablename = split_part(v_bad, '.', 2)
                     AND policyname = 'search_consumer_trusted' AND roles = ARRAY[v_role]::name[]
                     AND cmd = 'ALL' AND permissive = 'PERMISSIVE'
                     AND qual = 'true' AND with_check = 'true') THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: writer policy missing on %s', v_role, v_bad);
    END IF;
    -- Pre-existing TO authenticated / TO PUBLIC member policies stay
    -- untouched; only a second writer-targeted policy is drift.
    IF EXISTS (SELECT 1 FROM pg_policies
               WHERE schemaname = split_part(v_bad, '.', 1) AND tablename = split_part(v_bad, '.', 2)
                 AND policyname <> 'search_consumer_trusted'
                 AND roles @> ARRAY[v_role]::name[]) THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: extra writer-targeted policy on %s', v_role, v_bad);
    END IF;
  END LOOP;
  -- Every policy on the search tables must be exactly the tenant gate. A
  -- writer-targeted policy would collide with #330's exact-one-policy check
  -- while widening the gate, and a permissive TO PUBLIC policy ORs with
  -- tenant_only and gives the writer cross-tenant visibility without ever
  -- naming the role (roles @> ARRAY[v_role] does not match PUBLIC). Zero
  -- policies passes too: pre-#330 transcript_events has no row gate yet,
  -- and the serve-time tenant gate refuses to tick until it lands.
  FOR v_bad IN
    SELECT format('%s.%s', p.schemaname, p.tablename) || ' ' || p.policyname FROM pg_policies p
    WHERE p.schemaname = 'search_index' AND p.tablename IN ('transcript_events', 'projection_state')
  LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_policies q
                   WHERE format('%s.%s', q.schemaname, q.tablename) || ' ' || q.policyname = v_bad
                     AND q.policyname = 'tenant_only' AND q.cmd = 'ALL'
                     AND q.permissive = 'PERMISSIVE' AND q.roles = '{public}'::name[]
                     AND q.qual LIKE '%bulls96_read.org_id%' AND q.qual LIKE '%bulls96_read.workspace_id%'
                     AND q.with_check LIKE '%bulls96_read.org_id%' AND q.with_check LIKE '%bulls96_read.workspace_id%') THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: non-tenant policy %s', v_role, v_bad);
    END IF;
  END LOOP;

  -- Ownership and catalog shape: owner-created tables owned by neither the
  -- writer nor the reader; no PUBLIC or anonymous grants on the writer tables.
  SELECT c.oid::regclass::text INTO v_bad FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'search_index' AND c.relname IN ('transcript_events', 'projection_state')
       OR n.nspname = 'sessions' AND c.relname IN ('search_consumer_pending', 'search_consumer_progress')
    ORDER BY 1 LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: a search table is missing', v_role);
  END IF;
  -- Only on the bootstrap-owned tables: the pointer/directory/member tables
  -- legitimately keep their pre-existing SELECT TO authenticated grants from
  -- the app migrations, which this file never touches.
  FOR v_bad IN
    SELECT DISTINCT c.oid::regclass::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN LATERAL aclexplode(c.relacl) a ON true
      LEFT JOIN pg_roles gr ON gr.oid = a.grantee
      WHERE ((n.nspname = 'search_index' AND c.relname IN ('transcript_events', 'projection_state'))
          OR (n.nspname = 'sessions' AND c.relname IN ('search_consumer_pending', 'search_consumer_progress')))
        AND (a.grantee = 0 OR gr.rolname IN ('anon', 'authenticated'))
  LOOP
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: public/anonymous grant on %s', v_role, v_bad);
  END LOOP;
  SELECT r2.rolname INTO v_bad FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_roles r2 ON r2.oid = c.relowner
    WHERE ((n.nspname = 'search_index' AND c.relname IN ('transcript_events', 'projection_state'))
        OR (n.nspname = 'sessions' AND c.relname IN ('search_consumer_pending', 'search_consumer_progress')))
      AND r2.rolname IN ('search_consumer_runtime', 'bulls96_read_api') LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: table owned by runtime role', v_bad);
  END IF;
  -- The bootstrap-owned writer tables are owned by the invoker: the owner
  -- bootstrap is the only writer of these tables, and a table owned by any
  -- other role means a different bootstrap (or hand DDL) ran.
  SELECT r2.rolname INTO v_bad FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_roles r2 ON r2.oid = c.relowner
    WHERE n.nspname = 'sessions' AND c.relname IN ('search_consumer_pending', 'search_consumer_progress')
      AND r2.rolname <> current_user LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: writer table owned by %s, expected the bootstrap invoker', v_role, v_bad);
  END IF;

  RAISE NOTICE 'verify search_consumer_runtime: PASS';
END
$verify$;
