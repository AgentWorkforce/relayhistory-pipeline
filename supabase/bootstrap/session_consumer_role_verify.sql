-- Exact-shape verification for the self-host search-index writer role.
-- Fails on any drift (attributes, membership, missing or extra grants,
-- missing or extra policies, PUBLIC or anonymous access, wrong ownership).
-- Emits a NOTICE pass line on success.
--
-- Run as the schema owner after session_consumer_role.sql:
--   psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_consumer_role_verify.sql
\set ON_ERROR_STOP 1
DO $verify$
DECLARE
  v_role text := 'search_consumer_runtime';
  r pg_roles%ROWTYPE;
  v_bad text;
  v_extra text;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = v_role;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role is missing', v_role);
  END IF;
  IF NOT r.rolcanlogin OR r.rolinherit OR r.rolbypassrls OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role attributes drifted', v_role);
  END IF;
  -- Same creator-edge tolerance as the role file: exactly the invoker's
  -- admin-only membership, nothing else.
  IF EXISTS (SELECT 1 FROM pg_auth_members a JOIN pg_roles m ON m.oid = a.member
             WHERE (a.roleid = r.oid OR a.member = r.oid)
               AND NOT (a.roleid = r.oid AND m.rolname = current_user AND a.admin_option
                        AND NOT a.inherit_option AND NOT a.set_option)) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: role membership drifted', v_role);
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

  -- Exact table grants: no more (least privilege), no less (the writer path).
  IF NOT has_table_privilege(v_role, 'sessions.session_index', 'SELECT, INSERT, UPDATE, DELETE') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: session_index grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.session_index_ext', 'SELECT, INSERT, UPDATE') OR
     has_table_privilege(v_role, 'sessions.session_index_ext', 'DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: session_index_ext grant drifted', v_role);
  END IF;
  FOR v_bad IN SELECT t FROM (VALUES ('sessions.session_directory'), ('sessions.session_directory_users'),
      ('sessions.session_directory_machines')) AS x(t) LOOP
    IF NOT has_table_privilege(v_role, v_bad, 'SELECT, INSERT, UPDATE, DELETE') THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: %s grant drifted', v_role, v_bad);
    END IF;
  END LOOP;
  FOR v_bad IN SELECT t FROM (VALUES ('sessions.repos'), ('sessions.repo_names')) AS x(t) LOOP
    IF NOT has_table_privilege(v_role, v_bad, 'SELECT, INSERT, UPDATE') OR
       has_table_privilege(v_role, v_bad, 'DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: %s grant drifted', v_role, v_bad);
    END IF;
  END LOOP;
  IF NOT has_table_privilege(v_role, 'sessions.session_briefs', 'SELECT') OR
     has_table_privilege(v_role, 'sessions.session_briefs', 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: session_briefs grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.session_directory_event_pending', 'SELECT, UPDATE') OR
     has_table_privilege(v_role, 'sessions.session_directory_event_pending', 'INSERT, DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: event_pending grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.session_directory_event_times', 'SELECT, INSERT, DELETE') OR
     has_table_privilege(v_role, 'sessions.session_directory_event_times', 'UPDATE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: event_times grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.__96bulls_migrations', 'SELECT') OR
     has_table_privilege(v_role, 'sessions.__96bulls_migrations', 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: ledger grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.__96bulls_role_validation', 'SELECT') OR
     has_table_privilege(v_role, 'sessions.__96bulls_role_validation', 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: marker grant drifted', v_role);
  END IF;
  IF NOT has_schema_privilege(v_role, 'search_index', 'USAGE') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: search_index USAGE is missing', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'search_index.transcript_events', 'SELECT, INSERT, UPDATE, DELETE') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: transcript_events grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'search_index.projection_state', 'SELECT, INSERT, UPDATE, DELETE') OR
     has_table_privilege(v_role, 'search_index.projection_state', 'TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: projection_state grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.search_consumer_pending', 'SELECT, INSERT, UPDATE, DELETE') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: pending grant drifted', v_role);
  END IF;
  IF NOT has_table_privilege(v_role, 'sessions.search_consumer_progress', 'SELECT, INSERT, UPDATE') OR
     has_table_privilege(v_role, 'sessions.search_consumer_progress', 'DELETE, TRUNCATE, REFERENCES, TRIGGER') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: progress grant drifted', v_role);
  END IF;

  -- No table, sequence, function or schema privilege anywhere else in the
  -- database: catches extra TRUNCATE, grants on other tables, and any future
  -- GRANT the role file does not list. aclexplode (not composite field
  -- access) so the check runs on every Postgres. Column privileges are not
  -- used.
  FOR v_extra IN
    WITH held AS (
      SELECT c.oid::regclass::text AS obj, a.privilege_type AS priv, a.grantee AS grantee
        FROM pg_class c CROSS JOIN aclexplode(c.relacl) a
      UNION ALL
      SELECT p.oid::regprocedure::text, a.privilege_type, a.grantee
        FROM pg_proc p CROSS JOIN aclexplode(p.proacl) a
      UNION ALL
      SELECT n.nspname, a.privilege_type, a.grantee
        FROM pg_namespace n CROSS JOIN aclexplode(n.nspacl) a
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
        ('sessions.session_directory_event_pending', 'SELECT'), ('sessions.session_directory_event_pending', 'UPDATE'),
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
    SELECT DISTINCT h.obj || ' ' || h.priv FROM held h
      JOIN pg_roles gr ON gr.oid = h.grantee
      WHERE gr.rolname = v_role
        AND NOT EXISTS (SELECT 1 FROM allowed a WHERE a.obj = h.obj AND a.priv = h.priv)
  LOOP
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: unlisted grant %s', v_role, v_extra);
  END LOOP;

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
    WHERE p.pronamespace = to_regnamespace('sessions') AND p.prosecdef
      AND p.proname <> 'session_publication_is_managed'
      AND has_function_privilege(v_role, p.oid, 'EXECUTE') LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: executes SECURITY DEFINER %s', v_role, v_bad);
  END IF;

  -- Transparent writer RLS: exactly one policy per touched RLS table, the
  -- permissive writer policy and nothing else targeting the role. The
  -- member-read policies stay untouched (TO authenticated).
  FOR v_bad IN
    SELECT format('%s.%s', n.nspname, c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE (n.nspname = 'sessions' AND c.relname IN ('session_index', 'session_index_ext', 'session_directory',
        'session_directory_users', 'session_directory_machines', 'session_briefs',
        'session_directory_event_pending', 'session_directory_event_times',
        'repos', 'repo_names',
        'search_consumer_pending', 'search_consumer_progress'))
       OR (n.nspname = 'search_index' AND c.relname IN ('transcript_events', 'projection_state'))
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
    -- Pre-existing TO authenticated / TO PUBLIC member policies (e.g. the
    -- tenant_only policy) stay untouched; only a second writer-targeted
    -- policy is drift.
    IF EXISTS (SELECT 1 FROM pg_policies
               WHERE schemaname = split_part(v_bad, '.', 1) AND tablename = split_part(v_bad, '.', 2)
                 AND policyname <> 'search_consumer_trusted'
                 AND roles @> ARRAY[v_role]::name[]) THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('verify %s: extra writer-targeted policy on %s', v_role, v_bad);
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

  RAISE NOTICE 'verify search_consumer_runtime: PASS';
END
$verify$;
