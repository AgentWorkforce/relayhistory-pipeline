-- The session list and activity for the 96Bulls session read API (P3b, Cloud reads off
-- Neon). bulls96-session-read-api (sessions-read.agentrelay.com) serves `GET /v1/sessions`
-- and `GET /v1/sessions/activity` from the Supabase directory as the `bulls96_read_api`
-- role (Hyperdrive). It never holds a Supabase JWT secret: the existing
-- sessions.list_session_directory / session_directory_activity (20261101050000) take the
-- tenant from signed request.jwt.claims for session_directory_search_worker, and minting
-- those claims needs the project JWT secret, which can mint any role (rejected).
--
-- Instead the read API binds the tenant per READ ONLY transaction from the verified
-- RelayAuth assertion (src/session-read-api/liveness.ts TENANT_SETTINGS_SQL:
-- set_config('bulls96_read.org_id' / 'bulls96_read.workspace_id', $, true)), the same
-- binding the role's RLS policy on session_index already trusts
-- (supabase/bootstrap/session_read_api_role.sql). This migration adds:
--
--   1. sessions.session_directory_read_api_scope(p_workspace_id): the role guard and
--      tenant pin. Refuses 42501 unless the caller is bulls96_read_api (the `role`
--      setting, else session_user, as session_directory_worker_scope reads it), both
--      tenant settings are non-empty, and p_workspace_id equals the bound workspace.
--      Returns the bound org. Not SECURITY DEFINER; owner-only EXECUTE (called by the
--      two definer functions below as their owner).
--   2. sessions.list_session_directory_for(<the 16 arguments of list_session_directory>)
--      and sessions.session_directory_activity_for(text, timestamptz, timestamptz):
--      bodies copied verbatim from 20261101050000 except the scope call. Same
--      validations and refusals, the same statement builder
--      (sessions.session_directory_list_sql), the same return columns (including
--      short_summary), the same cursor and limits. SECURITY DEFINER, pinned
--      search_path, EXECUTE for bulls96_read_api alone.
--
-- Trust: a bulls96_read_api login can set any tenant setting, so these functions trust
-- the read Worker to bind only the verified assertion's tenant, exactly as the
-- session_index RLS policy already does. The role is NOINHERIT, has no members and
-- logs in only from the Worker (Hyperdrive; its password lives in 1Password).
--
-- Roles. This migration never creates or alters a role. The bare role is provisioned
-- before migrate by supabase/bootstrap/session_read_api_roles.sql (production already
-- has it, LOGIN, from session_read_api_role.sql); here it is verified, fail closed:
-- absent is 42704, elevated attributes or memberships are 55000. It may be LOGIN. It may
-- have no members but Supabase PG16+'s creator edge (postgres, granted by the superuser
-- supabase_admin, ADMIN without INHERIT or SET).
--
-- Code only; nothing here is applied by this repository. It changes the migration-set
-- generation, so it ships only in a reviewed generation window. Down path:
-- supabase/rollbacks/20261105029000_session_directory_read_api.down.sql.

DO $role$
DECLARE
  v_name text := 'bulls96_read_api';
  r pg_roles%ROWTYPE;
  v_row record;
  v_problems text[] := '{}';
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = format('role %s does not exist; this migration does not create it', v_name),
      HINT = 'Run supabase/bootstrap/session_read_api_roles.sql as a privileged role first, then rerun.';
  END IF;
  IF r.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF r.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF r.rolinherit THEN v_problems := array_append(v_problems, 'INHERIT'::text); END IF;
  IF r.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF r.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF r.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;
  FOR v_row IN SELECT g.rolname FROM pg_auth_members AS m JOIN pg_roles AS g ON g.oid = m.roleid
                WHERE m.member = r.oid LOOP
    v_problems := array_append(v_problems, format('member of %I', v_row.rolname)::text);
  END LOOP;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT u.rolname, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS u ON u.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1' USING r.oid LOOP
      IF v_row.rolname = 'postgres' AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL; -- Supabase PG16+ creator edge
      ELSE
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT u.rolname FROM pg_auth_members AS m
                   JOIN pg_roles AS u ON u.oid = m.member WHERE m.roleid = r.oid LOOP
      v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
    END LOOP;
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = format('role %s does not match the bootstrap spec: %s', v_name, array_to_string(v_problems, ', ')),
      HINT = 'Fix or drop the role and rerun supabase/bootstrap/session_read_api_roles.sql. The migration never alters a role.';
  END IF;
END
$role$;

-- The read API's tenant pin. Inside SECURITY DEFINER the `role` setting still names the
-- caller's SET ROLE, and session_user names the login (session_directory_worker_scope).
-- A transaction-local set_config reads '' on that pooled connection after its
-- transaction ends, and NULL where it was never set: both refuse.
CREATE FUNCTION sessions.session_directory_read_api_scope(p_workspace_id text)
RETURNS text
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_org text := nullif(current_setting('bulls96_read.org_id', true), '');
  v_workspace text := nullif(current_setting('bulls96_read.workspace_id', true), '');
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'bulls96_read_api' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: caller is not the read api role';
  END IF;
  IF v_org IS NULL OR v_workspace IS NULL
     OR p_workspace_id IS DISTINCT FROM v_workspace THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session directory: tenant binding missing or not this workspace';
  END IF;
  RETURN v_org;
END
$scope$;

-- sessions.list_session_directory (20261101050000) for the read API: the same arguments,
-- validations, statement and result; only the tenant comes from
-- session_directory_read_api_scope. Errors:
--   42501  caller is not bulls96_read_api; or the tenant binding is missing or names
--          another workspace
--   22023  an invalid value (as list_session_directory)
--   0A000  agent_filter_not_ready (as list_session_directory)
CREATE FUNCTION sessions.list_session_directory_for(
  p_workspace_id text,
  p_sources text[] DEFAULT NULL,
  p_not_sources text[] DEFAULT NULL,
  p_project_contains text DEFAULT NULL,
  p_user_ids text[] DEFAULT NULL,
  p_agent text DEFAULT NULL,
  p_last_ts_after timestamptz DEFAULT NULL,
  p_last_ts_before timestamptz DEFAULT NULL,
  p_first_ts_after timestamptz DEFAULT NULL,
  p_first_ts_before timestamptz DEFAULT NULL,
  p_session_harnesses text[] DEFAULT NULL,
  p_session_ids text[] DEFAULT NULL,
  p_cursor_last_activity_at timestamptz DEFAULT NULL,
  p_cursor_session_id text DEFAULT NULL,
  p_cursor_harness text DEFAULT NULL,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  created_at timestamptz, last_activity_at timestamptz,
  title text, task_title text, agent_names text[], project_id text,
  project_key text, project_key_method text, git_repo text, git_branch text,
  initial_commit text, originator text, agent_version text, models text[],
  state text, event_count bigint, input_tokens bigint, output_tokens bigint,
  cost_usd_micros bigint, machine_ids text[], user_ids text[],
  parent_session_ids text[], child_session_count integer,
  short_summary text, short_summary_generated_at timestamptz,
  catalog_projected_at timestamptz, projected_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $list$
DECLARE
  v_org text;
  v_sources text[];
  v_not_sources text[];
  v_users text[];
  v_agent text;
  v_project text;
  v_has_cursor boolean;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: workspace is required';
  END IF;
  -- Role guard and tenant pin; refuses (42501) before any value is read.
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);

  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 200 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: limit must be 1 to 200';
  END IF;
  IF (p_session_harnesses IS NULL) <> (p_session_ids IS NULL)
     OR (p_session_ids IS NOT NULL AND (
          cardinality(p_session_ids) NOT BETWEEN 1 AND 200
          OR cardinality(p_session_harnesses) <> cardinality(p_session_ids)
          OR EXISTS (SELECT 1 FROM unnest(p_session_harnesses, p_session_ids) AS k(h, s)
                      WHERE h IS NULL OR h !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
                         OR s IS NULL OR char_length(s) NOT BETWEEN 1 AND 255))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023',
      MESSAGE = 'session list: session keys must be 1 to 200 (harness, session id) pairs';
  END IF;
  IF p_sources IS NOT NULL AND cardinality(p_sources) > 0 THEN
    IF cardinality(p_sources) > 16 OR EXISTS (
         SELECT 1 FROM unnest(p_sources) AS s(v)
          WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$') THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: sources must be 1 to 16 valid harness names';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_sources FROM unnest(p_sources) AS s(v);
  END IF;
  IF p_not_sources IS NOT NULL AND cardinality(p_not_sources) > 0 THEN
    IF cardinality(p_not_sources) > 16 OR EXISTS (
         SELECT 1 FROM unnest(p_not_sources) AS s(v)
          WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$') THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: not_sources must be 1 to 16 valid harness names';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_not_sources FROM unnest(p_not_sources) AS s(v);
  END IF;
  IF p_user_ids IS NOT NULL AND cardinality(p_user_ids) > 0 THEN
    IF cardinality(p_user_ids) > 20 OR EXISTS (
         SELECT 1 FROM unnest(p_user_ids) AS u(v)
          WHERE v IS NULL OR char_length(v) NOT BETWEEN 1 AND 255) THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: users must be 1 to 20 ids of 1 to 255 characters';
    END IF;
    SELECT array_agg(DISTINCT v) INTO v_users FROM unnest(p_user_ids) AS u(v);
  END IF;
  IF p_agent IS NOT NULL THEN
    v_agent := lower(btrim(p_agent));
    IF v_agent = '' OR char_length(v_agent) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: agent must be 1 to 200 characters';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM sessions.session_directory_catalog_rollout AS r
                    WHERE r.singleton AND r.completed_at IS NOT NULL) THEN
      RAISE EXCEPTION USING ERRCODE = '0A000',
        MESSAGE = 'session list: agent_filter_not_ready: the directory catalog backfill is not complete';
    END IF;
  END IF;
  IF p_project_contains IS NOT NULL THEN
    v_project := btrim(p_project_contains);
    IF v_project = '' OR char_length(v_project) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: project_contains must be 1 to 200 characters';
    END IF;
  END IF;
  IF (p_last_ts_after IS NOT NULL AND p_last_ts_before IS NOT NULL AND p_last_ts_after > p_last_ts_before)
     OR (p_first_ts_after IS NOT NULL AND p_first_ts_before IS NOT NULL AND p_first_ts_after > p_first_ts_before) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: an after bound is later than its before bound';
  END IF;
  v_has_cursor := p_cursor_last_activity_at IS NOT NULL OR p_cursor_session_id IS NOT NULL
    OR p_cursor_harness IS NOT NULL;
  IF v_has_cursor AND (p_cursor_last_activity_at IS NULL OR p_cursor_session_id IS NULL
       OR p_cursor_harness IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session list: cursor is incomplete';
  END IF;

  RETURN QUERY EXECUTE sessions.session_directory_list_sql(
      coalesce(cardinality(v_sources), 0), v_project IS NOT NULL, coalesce(cardinality(v_users), 0),
      v_agent IS NOT NULL, p_last_ts_after IS NOT NULL, p_last_ts_before IS NOT NULL,
      p_first_ts_after IS NOT NULL, p_first_ts_before IS NOT NULL, p_session_ids IS NOT NULL,
      v_has_cursor, v_not_sources IS NOT NULL)
  USING v_org, p_workspace_id, v_sources,
        CASE WHEN v_project IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_project, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        v_users, v_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before,
        p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_limit + 1,
        p_session_harnesses, p_session_ids, v_not_sources;
END
$list$;

-- sessions.session_directory_activity (20261101050000) for the read API: the same SPAN
-- semantics, window bound and refusals; only the tenant comes from
-- session_directory_read_api_scope.
CREATE FUNCTION sessions.session_directory_activity_for(
  p_workspace_id text, p_since timestamptz, p_until timestamptz
)
RETURNS TABLE (day date, harness text, sessions bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $activity$
DECLARE
  v_org text;
  v_first date;
  v_last date;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: workspace is required';
  END IF;
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_since IS NULL OR p_until IS NULL OR p_until < p_since THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: since and until are required, since first';
  END IF;
  v_first := (p_since AT TIME ZONE 'UTC')::date;
  v_last := (p_until AT TIME ZONE 'UTC')::date;
  IF v_last - v_first + 1 > 366 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: at most 366 UTC days';
  END IF;
  RETURN QUERY
    WITH spans AS MATERIALIZED (
      SELECT d.harness,
             greatest((least(d.created_at, d.last_activity_at) AT TIME ZONE 'UTC')::date, v_first) AS lo,
             least((greatest(d.created_at, d.last_activity_at) AT TIME ZONE 'UTC')::date, v_last) AS hi
        FROM sessions.session_directory AS d
       WHERE d.org_id = v_org AND d.workspace_id = p_workspace_id AND d.deleted_at IS NULL
         AND greatest(d.created_at, d.last_activity_at) >= p_since
         AND least(d.created_at, d.last_activity_at) <= p_until
    ), deltas AS (
      SELECT s.harness, s.lo AS day, 1::bigint AS delta FROM spans AS s
      UNION ALL
      SELECT s.harness, s.hi + 1, -1::bigint FROM spans AS s WHERE s.hi < v_last
    ), per_day AS (
      SELECT x.harness, x.day, sum(x.delta) AS delta FROM deltas AS x GROUP BY x.harness, x.day
    ), grid AS (
      SELECT h.harness, g.day::date AS day
        FROM (SELECT DISTINCT s.harness FROM spans AS s) AS h,
             generate_series(v_first::timestamp, v_last::timestamp, interval '1 day') AS g(day)
    ), running AS (
      SELECT g.day, g.harness,
             sum(coalesce(p.delta, 0)) OVER (PARTITION BY g.harness ORDER BY g.day)::bigint AS sessions
        FROM grid AS g
        LEFT JOIN per_day AS p ON p.harness = g.harness AND p.day = g.day
    )
    SELECT r.day, r.harness, r.sessions FROM running AS r WHERE r.sessions > 0
     ORDER BY r.day, r.harness;
END
$activity$;

REVOKE ALL ON FUNCTION sessions.session_directory_read_api_scope(text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.list_session_directory_for(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_activity_for(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.list_session_directory_for(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.session_directory_activity_for(text, timestamptz, timestamptz)
  TO bulls96_read_api;

-- As in 20261101050000: strip EXECUTE from every grantee present at creation (default
-- privileges, a pre-existing runtime role), whatever the order. A grant added later is
-- still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_directory_read_api_scope(text)'::regprocedure,
    'sessions.list_session_directory_for(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)'::regprocedure,
    'sessions.session_directory_activity_for(text, timestamptz, timestamptz)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api'
                  AND v_fn IN ('sessions.list_session_directory_for(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer)'::regprocedure,
                               'sessions.session_directory_activity_for(text, timestamptz, timestamptz)'::regprocedure))
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
