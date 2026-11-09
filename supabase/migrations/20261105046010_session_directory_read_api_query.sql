-- Session list and activity `q` for the self-host Dashboard (#473; registry slot 046010,
-- taken after 046000 (#292), which has not landed on main; nothing here depends on it).
-- The read API's `GET /v1/sessions` and `GET /v1/sessions/activity` answer `q` (the
-- Dashboard's GitHub-like session query: free words, `-word`, `field:value`, `@agent`,
-- dates) from the session directory, behind READ_API_SESSION_QUERY=true (default off;
-- src/session-read-api/lambda.ts). The query is parsed by src/session-read-api/list-query.ts;
-- this migration receives the terms already split by category and maps them onto the
-- directory row:
--
--   free word      d.search_text <> '' AND d.search_text ILIKE '%word%'   (the tenant-scoped
--                  trigram index session_directory_tenant_search_trgm_idx). search_text is
--                  sessions.session_directory_search_text: title, task title, agent name,
--                  project id, git repo and branch, model and the scrubbed short summary;
--                  never transcript or event content.
--   title:         d.title or d.task_title ILIKE
--   project:       sessions.session_directory_project_text(...) ILIKE; several OR together
--   harness:/source:  d.harness = ANY; several OR together
--   agent: / @     an element of d.agent_names ILIKE
--   id:            d.session_id ILIKE
--   is:active / is:inactive   d.state = 'active' / <> 'active'
--   `-term`        NOT (the same predicate; NULL counts as not matching)
-- Every other term ANDs. Dates (updated:/after:/before:) arrive as last-activity bounds,
-- already intersected with lastTsAfter/lastTsBefore. path:, upload: and is:on-relay /
-- is:off-relay are not representable here and are refused (501) by the read API.
--
--   1. sessions.session_directory_query_check(...): every value validation, 22023 (and the
--      0A000 agent_filter_not_ready of the agent= list parameter). Not SECURITY DEFINER;
--      owner-only.
--   2. sessions.session_directory_query_like(text[]): the '%value%' patterns, `\ % _`
--      escaped exactly as 20261105034000 escapes project_contains. Owner-only.
--   3. sessions.session_directory_query_where(<shape>): the WHERE fragments for one query
--      shape. Fixed fragments only; every value is a bound parameter (EXECUTE ... USING),
--      a term is addressed by a builder-generated integer index ($n)[i]. IMMUTABLE,
--      owner-only.
--   4. sessions.query_session_directory_for(...): the list, the RETURNS TABLE of
--      sessions.list_session_directory_for, keyset (last_activity_at, session_id, harness)
--      DESC, p_limit 1..200, at most p_limit + 1 rows.
--   5. sessions.query_session_directory_activity_for(...): the SPAN activity of
--      sessions.session_directory_activity_for (same CTEs, at most 366 UTC days) over the
--      filtered sessions.
--   4 and 5 are SECURITY DEFINER with a pinned search_path, EXECUTE for bulls96_read_api
--   alone; the tenant comes from sessions.session_directory_read_api_scope (20261105034000),
--   42501 for any other caller or workspace.
--
-- Bind map shared by 3, 4 and 5:
--   $1 org  $2 workspace  $3/$4 free-word patterns / excluded  $5/$6 title patterns
--   $7/$8 project patterns  $9/$10 harnesses  $11/$12 agent patterns  $13/$14 id patterns
--   $15 is:active (boolean)  $16 sources  $17 not_sources  $18 project_contains pattern
--   $19 users  $20 agent (normalized)  $21 last_ts_after  $22 last_ts_before
--   $23 first_ts_after  $24 first_ts_before
--   list: $25 cursor last_activity_at  $26 cursor session_id  $27 cursor harness  $28 limit + 1
--   activity: $25 since  $26 until  $27 first UTC day  $28 last UTC day
--
-- Roles: never creates or alters a role; bulls96_read_api is verified exactly as in
-- 20261105034000 (42704 absent, 55000 elevated). Transactional; no index or table change.
-- Code only; nothing here is applied by this repository, and applying it to a real
-- database needs Khaliq's explicit OK. Down path:
-- supabase/rollbacks/20261105046010_session_directory_read_api_query.down.sql.

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

-- Every value the two query functions accept, checked before any statement is built.
-- 22023 for an invalid value; 0A000 agent_filter_not_ready for the agent= parameter
-- before the catalog backfill (as list_session_directory_for).
CREATE FUNCTION sessions.session_directory_query_check(
  p_free text[], p_not_free text[], p_title text[], p_not_title text[],
  p_project text[], p_not_project text[], p_harness text[], p_not_harness text[],
  p_agent_terms text[], p_not_agent_terms text[], p_id text[], p_not_id text[],
  p_sources text[], p_not_sources text[], p_project_contains text, p_user_ids text[],
  p_agent text, p_last_ts_after timestamptz, p_last_ts_before timestamptz,
  p_first_ts_after timestamptz, p_first_ts_before timestamptz
) RETURNS void
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $query_check$
DECLARE
  v_terms jsonb := jsonb_build_object(
    'free', to_jsonb(p_free), 'not_free', to_jsonb(p_not_free),
    'title', to_jsonb(p_title), 'not_title', to_jsonb(p_not_title),
    'project', to_jsonb(p_project), 'not_project', to_jsonb(p_not_project),
    'harness', to_jsonb(p_harness), 'not_harness', to_jsonb(p_not_harness),
    'agent', to_jsonb(p_agent_terms), 'not_agent', to_jsonb(p_not_agent_terms),
    'id', to_jsonb(p_id), 'not_id', to_jsonb(p_not_id));
  v_key text;
  v_list jsonb;
  v_value jsonb;
  v_total integer := 0;
  v_agent text;
  v_project text;
BEGIN
  FOR v_key, v_list IN SELECT t.key, t.value FROM jsonb_each(v_terms) AS t WHERE jsonb_typeof(t.value) <> 'null' LOOP
    v_total := v_total + jsonb_array_length(v_list);
    FOR v_value IN SELECT e.value FROM jsonb_array_elements(v_list) AS e LOOP
      -- A string of 1 to 200 characters (a nested array or a NULL element is refused).
      IF jsonb_typeof(v_value) <> 'string' OR char_length(v_value #>> '{}') NOT BETWEEN 1 AND 200 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: a term must be 1 to 200 characters';
      END IF;
      IF v_key IN ('harness', 'not_harness') AND (v_value #>> '{}') !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: harness terms must be valid harness names';
      END IF;
      -- Free words are already normalized by the read API (search-query.ts), the same rule.
      IF v_key IN ('free', 'not_free')
         AND sessions.session_directory_search_query(v_value #>> '{}') IS DISTINCT FROM (v_value #>> '{}') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: a search word needs 3 to 200 meaningful characters, normalized';
      END IF;
    END LOOP;
  END LOOP;
  IF v_total > 16 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: at most 16 terms';
  END IF;

  -- The list parameters, as list_session_directory_for validates them.
  IF p_sources IS NOT NULL AND cardinality(p_sources) > 0 AND (cardinality(p_sources) > 16 OR array_ndims(p_sources) > 1
     OR EXISTS (SELECT 1 FROM unnest(p_sources) AS s(v) WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$')) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: sources must be 1 to 16 valid harness names';
  END IF;
  IF p_not_sources IS NOT NULL AND cardinality(p_not_sources) > 0 AND (cardinality(p_not_sources) > 16 OR array_ndims(p_not_sources) > 1
     OR EXISTS (SELECT 1 FROM unnest(p_not_sources) AS s(v) WHERE v IS NULL OR v !~ '^[a-z0-9][a-z0-9._-]{0,63}$')) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: not_sources must be 1 to 16 valid harness names';
  END IF;
  IF p_user_ids IS NOT NULL AND cardinality(p_user_ids) > 0 AND (cardinality(p_user_ids) > 20 OR array_ndims(p_user_ids) > 1
     OR EXISTS (SELECT 1 FROM unnest(p_user_ids) AS u(v) WHERE v IS NULL OR char_length(v) NOT BETWEEN 1 AND 255)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: users must be 1 to 20 ids of 1 to 255 characters';
  END IF;
  IF p_agent IS NOT NULL THEN
    v_agent := lower(btrim(p_agent));
    IF v_agent = '' OR char_length(v_agent) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: agent must be 1 to 200 characters';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM sessions.session_directory_catalog_rollout AS r
                    WHERE r.singleton AND r.completed_at IS NOT NULL) THEN
      RAISE EXCEPTION USING ERRCODE = '0A000',
        MESSAGE = 'session query: agent_filter_not_ready: the directory catalog backfill is not complete';
    END IF;
  END IF;
  IF p_project_contains IS NOT NULL THEN
    v_project := btrim(p_project_contains);
    IF v_project = '' OR char_length(v_project) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: project_contains must be 1 to 200 characters';
    END IF;
  END IF;
  IF (p_last_ts_after IS NOT NULL AND p_last_ts_before IS NOT NULL AND p_last_ts_after > p_last_ts_before)
     OR (p_first_ts_after IS NOT NULL AND p_first_ts_before IS NOT NULL AND p_first_ts_after > p_first_ts_before) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: an after bound is later than its before bound';
  END IF;
END
$query_check$;

-- '%value%' ILIKE patterns in the input order, with `\`, `%` and `_` escaped (ESCAPE '\'),
-- exactly as 20261105034000 escapes project_contains. Always 1-based, whatever the input's
-- bounds, so the builder's ($n)[i] addresses term i.
CREATE FUNCTION sessions.session_directory_query_like(p_values text[])
RETURNS text[] LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $query_like$
  SELECT CASE WHEN p_values IS NULL THEN NULL ELSE ARRAY(
    SELECT '%' || replace(replace(replace(t.v, '\', '\\'), '%', '\%'), '_', '\_') || '%'
      FROM unnest(p_values) WITH ORDINALITY AS t(v, o) ORDER BY t.o) END
$query_like$;

-- The WHERE fragments (each starting ' AND') for one query shape: term counts per
-- category and which list parameters are set. Fixed fragments only; a term is addressed by
-- its 1-based index, generated here as an integer. One ILIKE per term, never ILIKE ANY, so
-- the trigram indexes can serve each one.
CREATE FUNCTION sessions.session_directory_query_where(
  p_free integer, p_not_free integer, p_title integer, p_not_title integer,
  p_project integer, p_not_project integer, p_harness integer, p_not_harness integer,
  p_agent_terms integer, p_not_agent_terms integer, p_id integer, p_not_id integer,
  p_has_active boolean, p_has_sources boolean, p_has_not_sources boolean,
  p_has_project_contains boolean, p_has_users boolean, p_has_agent boolean,
  p_has_last_after boolean, p_has_last_before boolean,
  p_has_first_after boolean, p_has_first_before boolean
) RETURNS text LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, pg_temp AS $query_where$
DECLARE
  v_counts integer[] := ARRAY[coalesce(p_free, 0), coalesce(p_not_free, 0), coalesce(p_title, 0),
    coalesce(p_not_title, 0), coalesce(p_project, 0), coalesce(p_not_project, 0),
    coalesce(p_harness, 0), coalesce(p_not_harness, 0), coalesce(p_agent_terms, 0),
    coalesce(p_not_agent_terms, 0), coalesce(p_id, 0), coalesce(p_not_id, 0)];
  v text := '';
  v_any text[];
  i integer;
BEGIN
  IF EXISTS (SELECT 1 FROM unnest(v_counts) AS c(n) WHERE c.n < 0 OR c.n > 16)
     OR (SELECT sum(c.n) FROM unnest(v_counts) AS c(n)) > 16 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query: at most 16 terms';
  END IF;
  -- Free words: the partial trigram index's predicate, then one ILIKE per word.
  IF v_counts[1] > 0 THEN
    v := v || $f$ AND d.search_text <> ''$f$;
  END IF;
  FOR i IN 1..v_counts[1] LOOP
    v := v || format($f$ AND d.search_text ILIKE ($3)[%s] ESCAPE '\'$f$, i);
  END LOOP;
  FOR i IN 1..v_counts[2] LOOP
    v := v || format($f$ AND NOT (d.search_text ILIKE ($4)[%s] ESCAPE '\')$f$, i);
  END LOOP;
  FOR i IN 1..v_counts[3] LOOP
    v := v || format($f$ AND coalesce(d.title ILIKE ($5)[%1$s] ESCAPE '\' OR d.task_title ILIKE ($5)[%1$s] ESCAPE '\', false)$f$, i);
  END LOOP;
  FOR i IN 1..v_counts[4] LOOP
    v := v || format($f$ AND NOT coalesce(d.title ILIKE ($6)[%1$s] ESCAPE '\' OR d.task_title ILIKE ($6)[%1$s] ESCAPE '\', false)$f$, i);
  END LOOP;
  -- project: alternatives OR together (the Dashboard's rule for project, harness, source).
  IF v_counts[5] > 0 THEN
    v_any := '{}';
    FOR i IN 1..v_counts[5] LOOP
      v_any := v_any || format($f$sessions.session_directory_project_text(d.project_id, d.project_key, d.git_repo) ILIKE ($7)[%s] ESCAPE '\'$f$, i);
    END LOOP;
    v := v || ' AND (d.project_id IS NOT NULL OR d.project_key IS NOT NULL OR d.git_repo IS NOT NULL)'
      || ' AND (' || array_to_string(v_any, ' OR ') || ')';
  END IF;
  FOR i IN 1..v_counts[6] LOOP
    v := v || format($f$ AND NOT (sessions.session_directory_project_text(d.project_id, d.project_key, d.git_repo) ILIKE ($8)[%s] ESCAPE '\')$f$, i);
  END LOOP;
  IF v_counts[7] > 0 THEN
    v := v || ' AND d.harness = ANY ($9::text[])';
  END IF;
  IF v_counts[8] > 0 THEN
    v := v || ' AND NOT (d.harness = ANY ($10::text[]))';
  END IF;
  FOR i IN 1..v_counts[9] LOOP
    v := v || format($f$ AND EXISTS (SELECT 1 FROM unnest(d.agent_names) AS n(v) WHERE n.v ILIKE ($11)[%s] ESCAPE '\')$f$, i);
  END LOOP;
  FOR i IN 1..v_counts[10] LOOP
    v := v || format($f$ AND NOT EXISTS (SELECT 1 FROM unnest(d.agent_names) AS n(v) WHERE n.v ILIKE ($12)[%s] ESCAPE '\')$f$, i);
  END LOOP;
  FOR i IN 1..v_counts[11] LOOP
    v := v || format($f$ AND d.session_id ILIKE ($13)[%s] ESCAPE '\'$f$, i);
  END LOOP;
  FOR i IN 1..v_counts[12] LOOP
    v := v || format($f$ AND NOT (d.session_id ILIKE ($14)[%s] ESCAPE '\')$f$, i);
  END LOOP;
  IF p_has_active THEN
    v := v || $f$ AND (d.state = 'active') = $15$f$;
  END IF;
  -- The list parameters, as sessions.session_directory_list_sql (20261101050000) builds them.
  IF p_has_sources THEN
    v := v || ' AND d.harness = ANY ($16::text[])';
  END IF;
  IF p_has_not_sources THEN
    v := v || ' AND NOT (d.harness = ANY ($17::text[]))';
  END IF;
  IF p_has_project_contains THEN
    v := v || ' AND (d.project_id IS NOT NULL OR d.project_key IS NOT NULL OR d.git_repo IS NOT NULL)'
      || $f$ AND sessions.session_directory_project_text(d.project_id, d.project_key, d.git_repo) ILIKE $18 ESCAPE '\'$f$;
  END IF;
  IF p_has_users THEN
    v := v || ' AND (d.org_id, d.workspace_id, d.harness, d.session_id) IN (SELECT u.org_id, u.workspace_id, u.harness, u.session_id'
      || ' FROM sessions.session_directory_users AS u'
      || ' WHERE u.org_id = $1 AND u.workspace_id = $2 AND u.user_id = ANY ($19::text[]))';
  END IF;
  IF p_has_agent THEN
    v := v || ' AND (d.org_id, d.workspace_id, d.harness, d.session_id) IN (SELECT a.org_id, a.workspace_id, a.harness, a.session_id'
      || ' FROM sessions.session_directory_agents AS a'
      || ' WHERE a.org_id = $1 AND a.workspace_id = $2 AND a.agent_name_normalized = $20)';
  END IF;
  IF p_has_last_after THEN v := v || ' AND d.last_activity_at >= $21'; END IF;
  IF p_has_last_before THEN v := v || ' AND d.last_activity_at < $22'; END IF;
  IF p_has_first_after THEN v := v || ' AND d.created_at >= $23'; END IF;
  IF p_has_first_before THEN v := v || ' AND d.created_at < $24'; END IF;
  RETURN v;
END
$query_where$;

-- The session list with `q` for the read API. Errors:
--   42501  caller is not bulls96_read_api; or the tenant binding is missing or names
--          another workspace
--   22023  an invalid value (sessions.session_directory_query_check), a limit outside
--          1..200 or an incomplete cursor
--   0A000  agent_filter_not_ready (the agent= parameter only)
CREATE FUNCTION sessions.query_session_directory_for(
  p_workspace_id text,
  p_free text[] DEFAULT NULL,
  p_not_free text[] DEFAULT NULL,
  p_title text[] DEFAULT NULL,
  p_not_title text[] DEFAULT NULL,
  p_project text[] DEFAULT NULL,
  p_not_project text[] DEFAULT NULL,
  p_harness text[] DEFAULT NULL,
  p_not_harness text[] DEFAULT NULL,
  p_agent_terms text[] DEFAULT NULL,
  p_not_agent_terms text[] DEFAULT NULL,
  p_id text[] DEFAULT NULL,
  p_not_id text[] DEFAULT NULL,
  p_active boolean DEFAULT NULL,
  p_sources text[] DEFAULT NULL,
  p_not_sources text[] DEFAULT NULL,
  p_project_contains text DEFAULT NULL,
  p_user_ids text[] DEFAULT NULL,
  p_agent text DEFAULT NULL,
  p_last_ts_after timestamptz DEFAULT NULL,
  p_last_ts_before timestamptz DEFAULT NULL,
  p_first_ts_after timestamptz DEFAULT NULL,
  p_first_ts_before timestamptz DEFAULT NULL,
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
SET search_path = pg_catalog, sessions, pg_temp AS $query$
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
    || ' d.short_summary, d.short_summary_generated_at, d.catalog_projected_at, d.projected_at'
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
$query$;

-- The session activity with `q`: sessions.session_directory_activity_for's SPAN semantics
-- (a session counts on every UTC day from its first to its last activity, clipped to the
-- window; at most 366 UTC days), over the sessions the filter keeps. Errors as
-- query_session_directory_for.
CREATE FUNCTION sessions.query_session_directory_activity_for(
  p_workspace_id text,
  p_since timestamptz,
  p_until timestamptz,
  p_free text[] DEFAULT NULL,
  p_not_free text[] DEFAULT NULL,
  p_title text[] DEFAULT NULL,
  p_not_title text[] DEFAULT NULL,
  p_project text[] DEFAULT NULL,
  p_not_project text[] DEFAULT NULL,
  p_harness text[] DEFAULT NULL,
  p_not_harness text[] DEFAULT NULL,
  p_agent_terms text[] DEFAULT NULL,
  p_not_agent_terms text[] DEFAULT NULL,
  p_id text[] DEFAULT NULL,
  p_not_id text[] DEFAULT NULL,
  p_active boolean DEFAULT NULL,
  p_sources text[] DEFAULT NULL,
  p_not_sources text[] DEFAULT NULL,
  p_project_contains text DEFAULT NULL,
  p_user_ids text[] DEFAULT NULL,
  p_agent text DEFAULT NULL,
  p_last_ts_after timestamptz DEFAULT NULL,
  p_last_ts_before timestamptz DEFAULT NULL,
  p_first_ts_after timestamptz DEFAULT NULL,
  p_first_ts_before timestamptz DEFAULT NULL
)
RETURNS TABLE (day date, harness text, sessions bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $query_activity$
DECLARE
  v_org text;
  v_first date;
  v_last date;
  v_project text;
  v_agent text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query activity: workspace is required';
  END IF;
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_since IS NULL OR p_until IS NULL OR p_until < p_since THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query activity: since and until are required, since first';
  END IF;
  v_first := (p_since AT TIME ZONE 'UTC')::date;
  v_last := (p_until AT TIME ZONE 'UTC')::date;
  IF v_last - v_first + 1 > 366 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session query activity: at most 366 UTC days';
  END IF;
  PERFORM sessions.session_directory_query_check(
    p_free, p_not_free, p_title, p_not_title, p_project, p_not_project, p_harness, p_not_harness,
    p_agent_terms, p_not_agent_terms, p_id, p_not_id, p_sources, p_not_sources, p_project_contains,
    p_user_ids, p_agent, p_last_ts_after, p_last_ts_before, p_first_ts_after, p_first_ts_before);
  v_project := btrim(p_project_contains);
  v_agent := lower(btrim(p_agent));

  RETURN QUERY EXECUTE
    $sql$WITH spans AS MATERIALIZED (
      SELECT d.harness,
             greatest((least(d.created_at, d.last_activity_at) AT TIME ZONE 'UTC')::date, $27) AS lo,
             least((greatest(d.created_at, d.last_activity_at) AT TIME ZONE 'UTC')::date, $28) AS hi
        FROM sessions.session_directory AS d
       WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL
         AND greatest(d.created_at, d.last_activity_at) >= $25
         AND least(d.created_at, d.last_activity_at) <= $26$sql$
    || sessions.session_directory_query_where(
         cardinality(p_free), cardinality(p_not_free), cardinality(p_title), cardinality(p_not_title),
         cardinality(p_project), cardinality(p_not_project), cardinality(p_harness), cardinality(p_not_harness),
         cardinality(p_agent_terms), cardinality(p_not_agent_terms), cardinality(p_id), cardinality(p_not_id),
         p_active IS NOT NULL, coalesce(cardinality(p_sources), 0) > 0, coalesce(cardinality(p_not_sources), 0) > 0,
         v_project IS NOT NULL, coalesce(cardinality(p_user_ids), 0) > 0, v_agent IS NOT NULL,
         p_last_ts_after IS NOT NULL, p_last_ts_before IS NOT NULL,
         p_first_ts_after IS NOT NULL, p_first_ts_before IS NOT NULL)
    || $sql$
    ), deltas AS (
      SELECT s.harness, s.lo AS day, 1::bigint AS delta FROM spans AS s
      UNION ALL
      SELECT s.harness, s.hi + 1, -1::bigint FROM spans AS s WHERE s.hi < $28
    ), per_day AS (
      SELECT x.harness, x.day, sum(x.delta) AS delta FROM deltas AS x GROUP BY x.harness, x.day
    ), grid AS (
      SELECT h.harness, g.day::date AS day
        FROM (SELECT DISTINCT s.harness FROM spans AS s) AS h,
             generate_series($27::timestamp, $28::timestamp, interval '1 day') AS g(day)
    ), running AS (
      SELECT g.day, g.harness,
             sum(coalesce(p.delta, 0)) OVER (PARTITION BY g.harness ORDER BY g.day)::bigint AS sessions
        FROM grid AS g
        LEFT JOIN per_day AS p ON p.harness = g.harness AND p.day = g.day
    )
    SELECT r.day, r.harness, r.sessions FROM running AS r WHERE r.sessions > 0
     ORDER BY r.day, r.harness$sql$
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
        p_since, p_until, v_first, v_last;
END
$query_activity$;

REVOKE ALL ON FUNCTION sessions.session_directory_query_check(
  text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_query_like(text[])
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_directory_query_where(
  integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, integer,
  boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.query_session_directory_for(
  text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  timestamptz, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.query_session_directory_activity_for(
  text, timestamptz, timestamptz, text[], text[], text[], text[], text[], text[], text[], text[], text[],
  text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz,
  timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.query_session_directory_for(
  text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[],
  boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz,
  timestamptz, text, text, integer)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.query_session_directory_activity_for(
  text, timestamptz, timestamptz, text[], text[], text[], text[], text[], text[], text[], text[], text[],
  text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz,
  timestamptz, timestamptz)
  TO bulls96_read_api;

-- As in 20261105034000: strip EXECUTE from every grantee present at creation (default
-- privileges, a pre-existing runtime role), whatever the order. A grant added later is
-- still refused at call time by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
  v_query regprocedure := 'sessions.query_session_directory_for(text, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz, timestamptz, text, text, integer)'::regprocedure;
  v_activity regprocedure := 'sessions.query_session_directory_activity_for(text, timestamptz, timestamptz, text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], boolean, text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz)'::regprocedure;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_directory_query_check(text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text[], text, text[], text, timestamptz, timestamptz, timestamptz, timestamptz)'::regprocedure,
    'sessions.session_directory_query_like(text[])'::regprocedure,
    'sessions.session_directory_query_where(integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, integer, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)'::regprocedure,
    v_query, v_activity
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api' AND v_fn IN (v_query, v_activity))
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
