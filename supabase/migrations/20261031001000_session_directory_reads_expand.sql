-- G6 expand phase: claim-bound replacements for the last direct reads RelayHistory #226
-- makes (pointers on session_index, and the directory readiness marker). ADDITIVE ONLY:
-- it revokes nothing, adds no index and does not change sessions.search_session_directory.
--
-- Expand/contract (docs/session-search-index-contract.md, "Direct surface: expand then
-- contract"): this migration only adds the RPC capabilities. Direct grants are revoked in a
-- later, separate contract migration, after #226's consumer cutover and direct-use proof;
-- it is not part of this change.
--
-- Both functions follow the search RPC's rules: SECURITY DEFINER with a pinned
-- search_path, fixed SQL with bound values, the caller must be the
-- session_directory_search_worker role, the tenant comes only from the signed JWT claims,
-- and EXECUTE goes to that role alone.
--
-- Code only; nothing here is applied by this repository. Down path:
-- supabase/rollbacks/20261031001000_session_directory_reads_expand.down.sql.

-- The Worker's tenant from the signed claims, refusing everything else (42501). With a
-- workspace, it must be one the claims list; without one, the claims must list at least
-- one. Not callable directly: only the SECURITY DEFINER functions below use it.
CREATE FUNCTION sessions.session_directory_worker_scope(p_workspace_id text)
RETURNS TABLE (org_id text, workspace_ids text[])
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_claims jsonb;
  v_org text;
  v_workspaces text[];
BEGIN
  -- Inside SECURITY DEFINER the `role` setting still names the caller's SET ROLE.
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'session_directory_search_worker' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: caller is not the search worker role';
  END IF;
  BEGIN
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  EXCEPTION WHEN others THEN
    v_claims := NULL;
  END;
  IF v_claims IS NULL OR jsonb_typeof(v_claims) <> 'object'
     OR (v_claims ->> 'role') IS DISTINCT FROM 'session_directory_search_worker'
     OR jsonb_typeof(v_claims -> 'app_metadata') IS DISTINCT FROM 'object'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'org_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'workspace_ids') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant claims missing or invalid';
  END IF;
  v_org := v_claims -> 'app_metadata' ->> 'org_id';
  SELECT coalesce(array_agg(w.value #>> '{}'), '{}') INTO v_workspaces
    FROM jsonb_array_elements(v_claims -> 'app_metadata' -> 'workspace_ids') AS w
   WHERE jsonb_typeof(w.value) = 'string';
  IF v_org = '' OR cardinality(v_workspaces) = 0
     OR (p_workspace_id IS NOT NULL AND NOT (p_workspace_id = ANY (v_workspaces))) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant claims missing or invalid';
  END IF;
  org_id := v_org;
  workspace_ids := v_workspaces;
  RETURN NEXT;
END
$scope$;

-- Pointers for one logical session in a claimed workspace: exactly RelayHistory #226's
-- POINTER_COLUMNS, live pointers only, ordered by machine_id, at most 64 rows.
CREATE FUNCTION sessions.session_directory_pointers(
  p_workspace_id text, p_harness text, p_session_id text
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text, machine_id text, user_id text,
  storage_provider text, bucket text, manifest_key text, content_sha256 bytea,
  raw_format text, revision bigint, deleted_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $pointers$
DECLARE
  v_org text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: workspace is required';
  END IF;
  SELECT s.org_id INTO v_org FROM sessions.session_directory_worker_scope(p_workspace_id) AS s;
  IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: harness is invalid';
  END IF;
  IF p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session directory: session id is invalid';
  END IF;
  RETURN QUERY
    SELECT p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.user_id,
           p.storage_provider, p.bucket, p.manifest_key, p.content_sha256,
           p.raw_format, p.revision, p.deleted_at
      FROM sessions.session_index AS p
     WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
       AND p.harness = p_harness AND p.session_id = p_session_id
       AND p.deleted_at IS NULL
     ORDER BY p.machine_id
     LIMIT 64;
END
$pointers$;

-- The directory readiness marker (completed_at only), for a valid Worker caller.
CREATE FUNCTION sessions.session_directory_readiness()
RETURNS TABLE (completed_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $readiness$
BEGIN
  PERFORM 1 FROM sessions.session_directory_worker_scope(NULL);
  RETURN QUERY SELECT r.completed_at FROM sessions.session_directory_rollout AS r WHERE r.singleton;
END
$readiness$;

REVOKE ALL ON FUNCTION sessions.session_directory_worker_scope(text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_pointers(text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_readiness()
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_pointers(text, text, text)
  TO session_directory_search_worker;
GRANT EXECUTE ON FUNCTION sessions.session_directory_readiness()
  TO session_directory_search_worker;

-- As in 20261030020000: strip EXECUTE from every grantee present at creation (default
-- privileges, a pre-existing runtime role such as relayhistory_app), whatever the order.
-- A grant added later is still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_directory_worker_scope(text)'::regprocedure,
    'sessions.session_directory_pointers(text, text, text)'::regprocedure,
    'sessions.session_directory_readiness()'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'session_directory_search_worker'
                  AND v_fn <> 'sessions.session_directory_worker_scope(text)'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
