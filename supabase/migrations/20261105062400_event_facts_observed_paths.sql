-- Observed paths on event facts (registry slot 062400). A tool result that is exactly a `git status --short` listing
-- names the paths its session SAW as changed: observations, not writes, kept apart
-- from files_touched (docs/event-facts-observed-paths.md). Two lines listing the same dirty paths can suggest a
-- shared checkout, or one line picking up another's uncommitted work; this field lets that be measured.
--
-- Additive and dormant. sessions.session_event_facts gains observed_paths text[] (empty by default, at most 64
-- entries), appended as its last column. output_facts_reconcile (045040) and staged_facts_reconcile_v1 (062140) are
-- replaced with identical signatures, ACLs and behaviour except that they carry observed_paths through (a row
-- without the key stores '{}'). No writer emits the key until the facts builders are given observedPaths, which no
-- runtime does yet; no reader or detector reads it. Applying this to any real database needs Khaliq's separate,
-- explicit go. The rollback (supabase/rollbacks/20261105062400_event_facts_observed_paths.down.sql) restores the
-- exact 045040 and 062140 bodies and drops the column (its values are derived and can be rebuilt).
SET LOCAL lock_timeout = '5s';

-- A constant default adds the column without rewriting the table. The bound is added NOT VALID: checking it here
-- would scan every row under this ACCESS EXCLUSIVE lock, and every existing row holds the default '{}', which
-- meets it. It is enforced for every row inserted or updated from here on.
ALTER TABLE sessions.session_event_facts ADD COLUMN observed_paths text[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions.session_event_facts
  ADD CONSTRAINT session_event_facts_observed_paths_bounded CHECK (cardinality(observed_paths) <= 64) NOT VALID;

CREATE OR REPLACE FUNCTION sessions.output_facts_reconcile(
  p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[], p_canonical_id text,
  p_events jsonb, p_facts jsonb, p_complete boolean, p_source_version bigint
)
RETURNS TABLE (applied boolean, inserted bigint, deleted bigint)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $reconcile$
DECLARE
  v_stored bigint;
  v_n bigint;
  v_inserted bigint := 0;
  v_deleted bigint := 0;
BEGIN
  PERFORM sessions.output_facts_writer_scope(p_org_id, p_workspace_id, p_harness, p_session_ids, p_canonical_id);
  IF p_canonical_id IS NULL OR p_complete IS NULL OR p_source_version IS NULL OR p_source_version < 0
     OR jsonb_typeof(p_events) IS DISTINCT FROM 'array' OR jsonb_typeof(p_facts) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid reconcile arguments';
  END IF;
  v_stored := sessions.output_facts_stored_version(p_org_id, p_workspace_id, p_harness, p_canonical_id);
  IF v_stored IS NOT NULL AND v_stored > p_source_version THEN
    RETURN QUERY SELECT false, 0::bigint, 0::bigint;
    RETURN;
  END IF;

  -- Every row must belong to this scope: never a write outside the tenant, harness or sessions.
  -- (No temp tables: a SECURITY DEFINER body must not trust caller-creatable pg_temp objects.)
  IF EXISTS (SELECT 1 FROM jsonb_to_recordset(p_events) AS e(org_id text, workspace_id text, source text, session_id text)
              WHERE e.org_id IS DISTINCT FROM p_org_id OR e.workspace_id IS DISTINCT FROM p_workspace_id
                 OR e.source IS DISTINCT FROM p_harness OR e.session_id IS NULL
                 OR NOT (e.session_id = ANY (p_session_ids)))
     OR EXISTS (SELECT 1 FROM jsonb_to_recordset(p_facts) AS f(org_id text, workspace_id text, scope_source text, session_id text)
              WHERE f.org_id IS DISTINCT FROM p_org_id OR f.workspace_id IS DISTINCT FROM p_workspace_id
                 OR f.scope_source IS DISTINCT FROM p_harness OR f.session_id IS NULL
                 OR NOT (f.session_id = ANY (p_session_ids))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: a row lies outside the scope';
  END IF;

  INSERT INTO sessions.session_event_facts AS t
  SELECT DISTINCT ON (e.row_id)
      e.org_id, e.workspace_id, e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id,
      e.project_id, e.task_ref_id, coalesce(e.task_ref_empty, 0), coalesce(e.task_ref_json, '{}'), e.task_title,
      e.user_id, e.branch, e.model, e.provider, e.input_tokens, e.output_tokens, e.reasoning_tokens,
      e.cache_read_tokens, e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens,
      e.cost_usd_micros, coalesce(e.files_touched, '{}'), coalesce(e.retention_days, 0), e.row_id, e.fingerprint,
      p_source_version, coalesce(e.observed_paths, '{}')
    FROM jsonb_to_recordset(p_events) AS e(
      org_id text, workspace_id text, ts timestamptz, session_id text, source text, machine_id text, kind text,
      event_id text, project_id text, task_ref_id text, task_ref_empty smallint, task_ref_json text,
      task_title text, user_id text, branch text, model text, provider text, input_tokens bigint,
      output_tokens bigint, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint,
      cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, cost_usd_micros bigint,
      files_touched text[], retention_days integer, row_id text, fingerprint text,
      observed_paths text[])
    ORDER BY e.row_id, e.fingerprint
  ON CONFLICT (org_id, workspace_id, row_id) DO UPDATE SET
    ts = EXCLUDED.ts, session_id = EXCLUDED.session_id, source = EXCLUDED.source,
    machine_id = EXCLUDED.machine_id, kind = EXCLUDED.kind, event_id = EXCLUDED.event_id,
    project_id = EXCLUDED.project_id, task_ref_id = EXCLUDED.task_ref_id,
    task_ref_empty = EXCLUDED.task_ref_empty, task_ref_json = EXCLUDED.task_ref_json,
    task_title = EXCLUDED.task_title, user_id = EXCLUDED.user_id, branch = EXCLUDED.branch,
    model = EXCLUDED.model, provider = EXCLUDED.provider, input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens, reasoning_tokens = EXCLUDED.reasoning_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens, cache_create_tokens = EXCLUDED.cache_create_tokens,
    cache_create_5m_tokens = EXCLUDED.cache_create_5m_tokens, cache_create_1h_tokens = EXCLUDED.cache_create_1h_tokens,
    cost_usd_micros = EXCLUDED.cost_usd_micros, files_touched = EXCLUDED.files_touched, observed_paths = EXCLUDED.observed_paths,
    retention_days = EXCLUDED.retention_days, fingerprint = EXCLUDED.fingerprint,
    source_version = EXCLUDED.source_version
  WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
    AND t.source = p_harness AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_inserted := v_inserted + v_n;

  INSERT INTO sessions.session_output_facts AS t
  SELECT DISTINCT ON (f.row_id)
      f.org_id, f.workspace_id, f.fact_kind, f.ts, f.scope_source, f.source, f.session_id, f.fact_key,
      f.origin, f.row_key, f.added, f.removed, coalesce(f.reverted, 0), coalesce(f.retention_days, 0),
      f.row_id, f.fingerprint, p_source_version
    FROM jsonb_to_recordset(p_facts) AS f(
      org_id text, workspace_id text, fact_kind text, ts timestamptz, scope_source text, source text,
      session_id text, fact_key text, origin text, row_key text, added bigint, removed bigint,
      reverted smallint, retention_days integer, row_id text, fingerprint text)
    ORDER BY f.row_id, f.fingerprint
  ON CONFLICT (org_id, workspace_id, row_id) DO UPDATE SET
    fact_kind = EXCLUDED.fact_kind, ts = EXCLUDED.ts, scope_source = EXCLUDED.scope_source,
    source = EXCLUDED.source, session_id = EXCLUDED.session_id, fact_key = EXCLUDED.fact_key,
    origin = EXCLUDED.origin, row_key = EXCLUDED.row_key, added = EXCLUDED.added,
    removed = EXCLUDED.removed, reverted = EXCLUDED.reverted, retention_days = EXCLUDED.retention_days,
    fingerprint = EXCLUDED.fingerprint, source_version = EXCLUDED.source_version
  WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
    AND t.scope_source = p_harness AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_inserted := v_inserted + v_n;

  -- Only an exact pass deletes: the scope's rows it no longer carries.
  IF p_complete THEN
    DELETE FROM sessions.session_event_facts AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.source = p_harness
       AND t.session_id = ANY (p_session_ids)
       AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_events) AS x WHERE x->>'row_id' = t.row_id);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_deleted := v_deleted + v_n;
    DELETE FROM sessions.session_output_facts AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.scope_source = p_harness
       AND t.session_id = ANY (p_session_ids)
       AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_facts) AS x WHERE x->>'row_id' = t.row_id);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_deleted := v_deleted + v_n;
  END IF;
  RETURN QUERY SELECT true, v_inserted, v_deleted;
END
$reconcile$;

CREATE OR REPLACE FUNCTION sessions.staged_facts_reconcile_v1(
  p_run_id bigint, p_nonce uuid, p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[],
  p_canonical_id text, p_complete boolean, p_source_version bigint
)
RETURNS TABLE (applied boolean, inserted bigint, deleted bigint)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $reconcile$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_stored bigint;
  v_n bigint;
  v_inserted bigint := 0;
  v_deleted bigint := 0;
BEGIN
  r := sessions.staged_facts_run_v1(p_run_id, p_nonce, ARRAY['final']);
  PERFORM sessions.output_facts_writer_scope(p_org_id, p_workspace_id, p_harness, p_session_ids, p_canonical_id);
  IF p_canonical_id IS NULL OR p_complete IS NULL OR p_source_version IS NULL OR p_source_version < 0 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid reconcile arguments';
  END IF;
  -- The run's facts reconcile the run's own scope only.
  IF r.org_id IS DISTINCT FROM p_org_id OR r.workspace_id IS DISTINCT FROM p_workspace_id OR r.harness IS DISTINCT FROM p_harness
     OR r.canonical_id IS DISTINCT FROM p_canonical_id OR NOT (r.session_id = ANY (p_session_ids)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'staged facts: the run belongs to another scope';
  END IF;
  -- A stage that does not cover the whole want pass is never read as the scope's facts: nothing is
  -- written, so a partial stage can never delete what an earlier pass wrote.
  IF NOT sessions.staged_facts_whole_v1(p_run_id) THEN
    RETURN QUERY SELECT false, 0::bigint, 0::bigint;
    RETURN;
  END IF;
  v_stored := sessions.output_facts_stored_version(p_org_id, p_workspace_id, p_harness, p_canonical_id);
  IF v_stored IS NOT NULL AND v_stored > p_source_version THEN
    RETURN QUERY SELECT false, 0::bigint, 0::bigint;
    RETURN;
  END IF;

  -- Every row must belong to this scope: never a write outside the tenant, harness or sessions.
  IF EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
               CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS e(org_id text, workspace_id text, source text, session_id text)
              WHERE s.run_id = p_run_id AND s.item_kind = 'event'
                AND (e.org_id IS DISTINCT FROM p_org_id OR e.workspace_id IS DISTINCT FROM p_workspace_id
                 OR e.source IS DISTINCT FROM p_harness OR e.session_id IS NULL
                 OR NOT (e.session_id = ANY (p_session_ids))))
     OR EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
               CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS f(org_id text, workspace_id text, scope_source text, session_id text)
              WHERE s.run_id = p_run_id AND s.item_kind = 'fact'
                AND (f.org_id IS DISTINCT FROM p_org_id OR f.workspace_id IS DISTINCT FROM p_workspace_id
                 OR f.scope_source IS DISTINCT FROM p_harness OR f.session_id IS NULL
                 OR NOT (f.session_id = ANY (p_session_ids)))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: a row lies outside the scope';
  END IF;

  INSERT INTO sessions.session_event_facts AS t
  SELECT DISTINCT ON (e.row_id)
      e.org_id, e.workspace_id, e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id,
      e.project_id, e.task_ref_id, coalesce(e.task_ref_empty, 0), coalesce(e.task_ref_json, '{}'), e.task_title,
      e.user_id, e.branch, e.model, e.provider, e.input_tokens, e.output_tokens, e.reasoning_tokens,
      e.cache_read_tokens, e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens,
      e.cost_usd_micros, coalesce(e.files_touched, '{}'), coalesce(e.retention_days, 0), e.row_id, e.fingerprint,
      p_source_version, coalesce(e.observed_paths, '{}')
    FROM sessions.staged_facts_v1 AS s
    CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS e(
      org_id text, workspace_id text, ts timestamptz, session_id text, source text, machine_id text, kind text,
      event_id text, project_id text, task_ref_id text, task_ref_empty smallint, task_ref_json text,
      task_title text, user_id text, branch text, model text, provider text, input_tokens bigint,
      output_tokens bigint, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint,
      cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, cost_usd_micros bigint,
      files_touched text[], retention_days integer, row_id text, fingerprint text,
      observed_paths text[])
   WHERE s.run_id = p_run_id AND s.item_kind = 'event'
    ORDER BY e.row_id, e.fingerprint
  ON CONFLICT (org_id, workspace_id, row_id) DO UPDATE SET
    ts = EXCLUDED.ts, session_id = EXCLUDED.session_id, source = EXCLUDED.source,
    machine_id = EXCLUDED.machine_id, kind = EXCLUDED.kind, event_id = EXCLUDED.event_id,
    project_id = EXCLUDED.project_id, task_ref_id = EXCLUDED.task_ref_id,
    task_ref_empty = EXCLUDED.task_ref_empty, task_ref_json = EXCLUDED.task_ref_json,
    task_title = EXCLUDED.task_title, user_id = EXCLUDED.user_id, branch = EXCLUDED.branch,
    model = EXCLUDED.model, provider = EXCLUDED.provider, input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens, reasoning_tokens = EXCLUDED.reasoning_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens, cache_create_tokens = EXCLUDED.cache_create_tokens,
    cache_create_5m_tokens = EXCLUDED.cache_create_5m_tokens, cache_create_1h_tokens = EXCLUDED.cache_create_1h_tokens,
    cost_usd_micros = EXCLUDED.cost_usd_micros, files_touched = EXCLUDED.files_touched, observed_paths = EXCLUDED.observed_paths,
    retention_days = EXCLUDED.retention_days, fingerprint = EXCLUDED.fingerprint,
    source_version = EXCLUDED.source_version
  WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
    AND t.source = p_harness AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_inserted := v_inserted + v_n;

  INSERT INTO sessions.session_output_facts AS t
  SELECT DISTINCT ON (f.row_id)
      f.org_id, f.workspace_id, f.fact_kind, f.ts, f.scope_source, f.source, f.session_id, f.fact_key,
      f.origin, f.row_key, f.added, f.removed, coalesce(f.reverted, 0), coalesce(f.retention_days, 0),
      f.row_id, f.fingerprint, p_source_version
    FROM sessions.staged_facts_v1 AS s
    CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS f(
      org_id text, workspace_id text, fact_kind text, ts timestamptz, scope_source text, source text,
      session_id text, fact_key text, origin text, row_key text, added bigint, removed bigint,
      reverted smallint, retention_days integer, row_id text, fingerprint text)
   WHERE s.run_id = p_run_id AND s.item_kind = 'fact'
    ORDER BY f.row_id, f.fingerprint
  ON CONFLICT (org_id, workspace_id, row_id) DO UPDATE SET
    fact_kind = EXCLUDED.fact_kind, ts = EXCLUDED.ts, scope_source = EXCLUDED.scope_source,
    source = EXCLUDED.source, session_id = EXCLUDED.session_id, fact_key = EXCLUDED.fact_key,
    origin = EXCLUDED.origin, row_key = EXCLUDED.row_key, added = EXCLUDED.added,
    removed = EXCLUDED.removed, reverted = EXCLUDED.reverted, retention_days = EXCLUDED.retention_days,
    fingerprint = EXCLUDED.fingerprint, source_version = EXCLUDED.source_version
  WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
    AND t.scope_source = p_harness AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_inserted := v_inserted + v_n;

  -- Only an exact pass deletes: the scope's rows it no longer carries.
  IF p_complete THEN
    DELETE FROM sessions.session_event_facts AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.source = p_harness
       AND t.session_id = ANY (p_session_ids)
       AND NOT EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
                        WHERE s.run_id = p_run_id AND s.item_kind = 'event' AND s.row_id = t.row_id);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_deleted := v_deleted + v_n;
    DELETE FROM sessions.session_output_facts AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.scope_source = p_harness
       AND t.session_id = ANY (p_session_ids)
       AND NOT EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
                        WHERE s.run_id = p_run_id AND s.item_kind = 'fact' AND s.row_id = t.row_id);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_deleted := v_deleted + v_n;
  END IF;
  RETURN QUERY SELECT true, v_inserted, v_deleted;
END
$reconcile$;
