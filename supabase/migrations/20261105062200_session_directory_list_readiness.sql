-- Slot 20261105062200 (assigned). Session list readiness for the read API's GET /v1/sessions.
-- Cloud lists sessions from the read API and must be able to tell, or filter, the sessions whose
-- search (event) projection is complete. No list or query function returns or checks that state
-- today (20261101050000, 20261105034000, 20261105046010), and a RETURNS TABLE cannot change in
-- place, so this adds two NEW functions, one beside each list function the read API calls:
--
--   1. sessions.list_session_directory_readiness_for(<the 16 arguments of
--      list_session_directory_for>, p_ready_only boolean), beside
--      sessions.list_session_directory_for (20261105034000).
--   2. sessions.query_session_directory_readiness_for(<the 27 arguments of
--      query_session_directory_for>, p_ready_only boolean), beside
--      sessions.query_session_directory_for (20261105046010).
--
-- Unlike their neighbours, the arguments take no DEFAULTs: every caller (directory.ts) passes all of
-- them by name, and the migration guards read a function header without DEFAULT clauses.
--
-- Each returns its neighbour's columns, in the same order, plus two:
--   events_ready        d.event_projection_ready AND d.event_projection_version = 1
--   event_projected_at  d.event_projected_at as stored (the last projection; it can be set on a
--                       row that a later pointer change made not ready again)
-- With p_ready_only = true the same predicate (d.event_projection_ready AND
-- d.event_projection_version = 1) joins the WHERE, before ORDER BY, the keyset cursor and
-- LIMIT p_limit + 1, so a page and its cursor are exact. With p_ready_only false (or NULL) the
-- rows are exactly the neighbour's rows.
--
-- Readiness is search-projection state only: it does not prove exact or context readability
-- (those use pointer liveness). A ready-only list hides, for good, sessions too large to index,
-- scopes over budget, sessions with persistent canonical metadata errors and every session on an
-- install with no indexer; and briefly every active session after each upload (a pointer write
-- makes the row not ready until the next index pass).
--
-- 1 reuses 20261101050000's statement builder (sessions.session_directory_list_sql) unchanged, so
-- every filter means exactly what it means for list_session_directory_for: it adds the two columns
-- after the builder's last column and the predicate after the builder's last WHERE term. Both
-- anchors must appear exactly once (the ORDER BY at the very end) or the call refuses 55000; the
-- builder is IMMUTABLE and shipped, so that refusal only fires if a later migration replaces it.
-- 2 composes 20261105046010's helpers (session_directory_query_check / _like / _where) exactly as
-- query_session_directory_for does.
--
-- Both are SECURITY DEFINER, STABLE, with search_path pinned to pg_catalog, sessions, pg_temp; the
-- tenant comes from sessions.session_directory_read_api_scope (42501 for any caller but
-- bulls96_read_api, or a missing or other-workspace binding); EXECUTE is revoked from PUBLIC, anon,
-- authenticated and service_role and granted to bulls96_read_api alone, as their neighbours.
-- The read API calls them only with READ_API_LIST_READINESS_ENABLED=true and a caller opt-in
-- (includeReadiness=true or readyOnly=true); otherwise it calls the neighbours, as before.
--
-- No existing function is replaced; no table, index, role or table grant changes. Transactional.
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK. Down path:
-- supabase/rollbacks/20261105062200_session_directory_list_readiness.down.sql.

-- sessions.list_session_directory_for (20261105034000) plus readiness. The same arguments,
-- validations (copied verbatim), refusals and statement. Errors:
--   42501  caller is not bulls96_read_api; or the tenant binding is missing or names
--          another workspace
--   22023  an invalid value (as list_session_directory_for)
--   0A000  agent_filter_not_ready (as list_session_directory_for)
--   55000  the 20261101050000 statement builder no longer has the shape this function extends
CREATE FUNCTION sessions.list_session_directory_readiness_for(
  p_workspace_id text,
  p_sources text[],
  p_not_sources text[],
  p_project_contains text,
  p_user_ids text[],
  p_agent text,
  p_last_ts_after timestamptz,
  p_last_ts_before timestamptz,
  p_first_ts_after timestamptz,
  p_first_ts_before timestamptz,
  p_session_harnesses text[],
  p_session_ids text[],
  p_cursor_last_activity_at timestamptz,
  p_cursor_session_id text,
  p_cursor_harness text,
  p_limit integer,
  p_ready_only boolean
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
  catalog_projected_at timestamptz, projected_at timestamptz,
  events_ready boolean, event_projected_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $list_readiness$
DECLARE
  v_org text;
  v_sources text[];
  v_not_sources text[];
  v_users text[];
  v_agent text;
  v_project text;
  v_has_cursor boolean;
  v_sql text;
  -- The builder's last column and its FROM/WHERE (20261101050000), and its ORDER BY/LIMIT tail.
  v_columns_anchor constant text :=
    ' d.short_summary, d.short_summary_generated_at, d.catalog_projected_at, d.projected_at'
    || ' FROM sessions.session_directory AS d WHERE ';
  v_order_anchor constant text :=
    ' ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT $14';
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

  v_sql := sessions.session_directory_list_sql(
      coalesce(cardinality(v_sources), 0), v_project IS NOT NULL, coalesce(cardinality(v_users), 0),
      v_agent IS NOT NULL, p_last_ts_after IS NOT NULL, p_last_ts_before IS NOT NULL,
      p_first_ts_after IS NOT NULL, p_first_ts_before IS NOT NULL, p_session_ids IS NOT NULL,
      v_has_cursor, v_not_sources IS NOT NULL);
  IF (char_length(v_sql) - char_length(replace(v_sql, v_columns_anchor, ''))) <> char_length(v_columns_anchor)
     OR (char_length(v_sql) - char_length(replace(v_sql, v_order_anchor, ''))) <> char_length(v_order_anchor)
     OR right(v_sql, char_length(v_order_anchor)) <> v_order_anchor THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'session list: the directory list statement does not have the shape the readiness list extends';
  END IF;
  -- Every builder WHERE term is a top-level AND, so one more AND before ORDER BY narrows the
  -- set before the keyset cursor's order and the LIMIT.
  v_sql := replace(v_sql, v_columns_anchor,
      ' d.short_summary, d.short_summary_generated_at, d.catalog_projected_at, d.projected_at,'
      || ' (d.event_projection_ready AND d.event_projection_version = 1) AS events_ready,'
      || ' d.event_projected_at'
      || ' FROM sessions.session_directory AS d WHERE ');
  v_sql := left(v_sql, char_length(v_sql) - char_length(v_order_anchor))
    || CASE WHEN p_ready_only THEN ' AND d.event_projection_ready AND d.event_projection_version = 1' ELSE '' END
    || v_order_anchor;

  RETURN QUERY EXECUTE v_sql
  USING v_org, p_workspace_id, v_sources,
        CASE WHEN v_project IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_project, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        v_users, v_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before,
        p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_limit + 1,
        p_session_harnesses, p_session_ids, v_not_sources;
END
$list_readiness$;

-- sessions.query_session_directory_for (20261105046010) plus readiness. The same arguments,
-- checks, helpers and statement. Errors as query_session_directory_for:
--   42501  caller is not bulls96_read_api; or the tenant binding is missing or names
--          another workspace
--   22023  an invalid value (sessions.session_directory_query_check), a limit outside
--          1..200 or an incomplete cursor
--   0A000  agent_filter_not_ready (the agent= parameter only)
CREATE FUNCTION sessions.query_session_directory_readiness_for(
  p_workspace_id text,
  p_free text[],
  p_not_free text[],
  p_title text[],
  p_not_title text[],
  p_project text[],
  p_not_project text[],
  p_harness text[],
  p_not_harness text[],
  p_agent_terms text[],
  p_not_agent_terms text[],
  p_id text[],
  p_not_id text[],
  p_active boolean,
  p_sources text[],
  p_not_sources text[],
  p_project_contains text,
  p_user_ids text[],
  p_agent text,
  p_last_ts_after timestamptz,
  p_last_ts_before timestamptz,
  p_first_ts_after timestamptz,
  p_first_ts_before timestamptz,
  p_cursor_last_activity_at timestamptz,
  p_cursor_session_id text,
  p_cursor_harness text,
  p_limit integer,
  p_ready_only boolean
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
  catalog_projected_at timestamptz, projected_at timestamptz,
  events_ready boolean, event_projected_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $query_readiness$
DECLARE
  v_org text;
  v_has_cursor boolean;
  v_project text;
  v_agent text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: workspace is required';
  END IF;
  -- Role guard and tenant pin; refuses (42501) before any value is read.
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);

  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 200 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: limit must be 1 to 200';
  END IF;
  v_has_cursor := p_cursor_last_activity_at IS NOT NULL OR p_cursor_session_id IS NOT NULL
    OR p_cursor_harness IS NOT NULL;
  IF v_has_cursor AND (p_cursor_last_activity_at IS NULL OR p_cursor_session_id IS NULL
       OR p_cursor_harness IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: cursor is incomplete';
  END IF;
  PERFORM sessions.session_directory_query_check(
    p_free, p_not_free, p_title, p_not_title, p_project, p_not_project, p_harness, p_not_harness,
    p_agent_terms, p_not_agent_terms, p_id, p_not_id, p_sources, p_not_sources, p_project_contains,
    p_user_ids, p_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before);
  v_project := btrim(p_project_contains);
  v_agent := lower(btrim(p_agent));

  RETURN QUERY EXECUTE
    'SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,'
    || ' d.title, d.task_title, d.agent_names, d.project_id, d.project_key, d.project_key_method,'
    || ' d.git_repo, d.git_branch, d.initial_commit, d.originator, d.agent_version,'
    || ' ARRAY(SELECT DISTINCT x FROM unnest(d.catalog_models || ARRAY[d.model]) AS x'
    ||       ' WHERE x IS NOT NULL AND x <> '''' ORDER BY x) AS models,'
    || ' d.state, d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,'
    || ' d.machine_ids, d.user_ids,'
    || ' ARRAY(SELECT DISTINCT r.parent_session_id FROM sessions.session_directory_relationships AS r'
    ||       ' WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness'
    ||       ' AND r.child_session_id = d.session_id ORDER BY 1 LIMIT 64) AS parent_session_ids,'
    || ' (SELECT count(*) FROM sessions.session_directory_relationships AS r'
    ||       ' WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness'
    ||       ' AND r.parent_session_id = d.session_id)::integer AS child_session_count,'
    || ' d.short_summary, d.short_summary_generated_at, d.catalog_projected_at, d.projected_at,'
    || ' (d.event_projection_ready AND d.event_projection_version = 1) AS events_ready,'
    || ' d.event_projected_at'
    || ' FROM sessions.session_directory AS d'
    || ' WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL'
    || sessions.session_directory_query_where(
         cardinality(p_free), cardinality(p_not_free), cardinality(p_title), cardinality(p_not_title),
         cardinality(p_project), cardinality(p_not_project), cardinality(p_harness), cardinality(p_not_harness),
         cardinality(p_agent_terms), cardinality(p_not_agent_terms), cardinality(p_id), cardinality(p_not_id),
         p_active IS NOT NULL, coalesce(cardinality(p_sources), 0) > 0, coalesce(cardinality(p_not_sources), 0) > 0,
         v_project IS NOT NULL, coalesce(cardinality(p_user_ids), 0) > 0, v_agent IS NOT NULL,
         p_last_ts_after IS NOT NULL, p_last_ts_before IS NOT NULL,
         p_first_ts_after IS NOT NULL, p_first_ts_before IS NOT NULL)
    || CASE WHEN p_ready_only THEN ' AND d.event_projection_ready AND d.event_projection_version = 1' ELSE '' END
    || CASE WHEN v_has_cursor THEN ' AND (d.last_activity_at, d.session_id, d.harness) < ($25, $26, $27)' ELSE '' END
    || ' ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT $28'
  USING v_org, p_workspace_id,
        sessions.session_directory_query_like(p_free), sessions.session_directory_query_like(p_not_free),
        sessions.session_directory_query_like(p_title), sessions.session_directory_query_like(p_not_title),
        sessions.session_directory_query_like(p_project), sessions.session_directory_query_like(p_not_project),
        p_harness, p_not_harness,
        sessions.session_directory_query_like(p_agent_terms), sessions.session_directory_query_like(p_not_agent_terms),
        sessions.session_directory_query_like(p_id), sessions.session_directory_query_like(p_not_id),
        p_active, p_sources, p_not_sources,
        CASE WHEN v_project IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_project, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        p_user_ids, v_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before,
        p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_limit + 1;
END
$query_readiness$;

REVOKE ALL ON FUNCTION sessions.list_session_directory_readiness_for(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.query_session_directory_readiness_for(
  text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  timestamptz, text, text, integer, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.list_session_directory_readiness_for(
  text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  text[], text[], timestamptz, text, text, integer, boolean)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.query_session_directory_readiness_for(
  text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  timestamptz, text, text, integer, boolean)
  TO bulls96_read_api;

-- As in 20261105034000 and 20261105046010: strip EXECUTE from every grantee present at creation
-- (default privileges, a pre-existing runtime role), whatever the order. A grant added later is
-- still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.list_session_directory_readiness_for(text, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, text[], text[], timestamptz, text, text, integer, boolean)'::regprocedure,
    'sessions.query_session_directory_readiness_for(text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, timestamptz, text, text, integer, boolean)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND r.rolname <> 'bulls96_read_api'
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
