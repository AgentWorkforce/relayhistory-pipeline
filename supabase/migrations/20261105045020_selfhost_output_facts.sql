-- Self-host output facts in Postgres (PR1 of the self-host digest/output plan; registry slot
-- 045020 by the lead's ruling, ordered after 045000 digest_narrative_cache and 045010).
-- The ClickHouse fact tables (src/session-pipeline/output-facts-schema.ts, schema v4) have no
-- home in a ClickHouse-free self-host stack. These tables hold the same rows, built by the same
-- builder (output-facts.ts outputFactsFromScope), written by the self-host consumer
-- (search_consumer_runtime) inside its directory-events transaction, behind
-- RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true (default off; consumer-config.ts).
--
--   1. sessions.session_event_facts / sessions.session_output_facts: the v4 columns.
--      DateTime64(6) -> timestamptz, Array(Nullable(String)) -> text[], UInt8/UInt16/Int64/UInt64
--      -> smallint/integer/bigint. ReplacingMergeTree `version` is replaced by `source_version`
--      (the session directory event_projection_epoch the pass read). PK (org_id, workspace_id,
--      row_id): row_id is sha256 of the identity WITHOUT the tenant, so a tenant-less PK would
--      let one workspace's pass overwrite another's row with the same identity.
--   2. sessions.output_facts_session_state: one row per (org, workspace, source, session_id),
--      complete / min_ts / max_ts / source_version / synced_at. Tenant-qualified for the same
--      reason as 1.
--   3. sessions.output_facts_workspace_state: (org_id, workspace_id) -> complete_since. Written
--      by the operator bootstrap (a later PR) as the table owner; no function writes it here.
--   Plain indexes on (org_id, workspace_id, ts) and (org_id, workspace_id, session_id): the
--   tables are new and empty, so no CONCURRENTLY (the migrator is transactional).
--   RLS on, no policy, no table grant: reachable only through the functions below.
--
-- Writer functions (SECURITY DEFINER, EXECUTE to search_consumer_runtime alone, which each
-- checks itself, 42501 otherwise):
--   output_facts_state(org, workspace, harness, session_ids, canonical_id)
--       the scope's recorded span and the canonical row's source_version
--   output_facts_reconcile(org, workspace, harness, session_ids, canonical_id, events, facts,
--                          complete, source_version)
--       upsert by PK WHERE fingerprint IS DISTINCT FROM; a complete pass also deletes the
--       scope's rows it did not carry. A pass older than the stored canonical source_version
--       is a no-op (applied = false).
--   output_facts_record_state(org, workspace, harness, session_ids, canonical_id, complete,
--                             min_ts, max_ts, source_version, synced_at)
--       same stale rule; returns whether it wrote
--   output_facts_erase(org, workspace, harness, session_ids)
--       erasure: every fact and state row of the scope goes
-- Reader functions (SECURITY DEFINER, EXECUTE to bulls96_read_api alone; the tenant comes from
-- session_directory_read_api_scope, 20261105034000):
--   output_facts_events_for / output_facts_outputs_for(workspace, from, to, after_row_id, limit)
--       keyset pages of one window, ordered by row_id, limit 1..10000
--   output_facts_coverage_for(workspace, from, to)
--       complete_since plus the count of state rows (and incomplete ones) overlapping the window
--
-- Roles: never created or altered here. search_consumer_runtime (session_consumer_roles.sql)
-- and bulls96_read_api (session_read_api_roles.sql) are provisioned before migrate and checked
-- here, fail closed (42704 absent, 55000 elevated). The consumer verifier
-- (supabase/bootstrap/session_consumer_role_verify.sql) allowlists the four writer functions.
--
-- Not covered here (follow-ups): the workspace-level privacy purge
-- (ingest_privacy_authority_purge_derived, 041000) does not purge these tables; sessions
-- erased while the flag is off keep any facts written while it was on.
--
-- Transactional (no CONCURRENTLY, no enum change): migrate.ts wraps it with its ledger row.
-- Code only; applying it to a real database needs Khaliq's explicit OK. Down path:
-- supabase/rollbacks/20261105045020_selfhost_output_facts.down.sql.

DO $roles$
DECLARE
  v_name text;
  r pg_roles%ROWTYPE;
BEGIN
  FOREACH v_name IN ARRAY ARRAY['search_consumer_runtime', 'bulls96_read_api'] LOOP
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE = '42704',
        MESSAGE = format('role %s does not exist; this migration does not create it', v_name),
        HINT = 'Run the role bootstrap (session_consumer_roles.sql, session_read_api_roles.sql) as a privileged role first, then rerun.';
    END IF;
    IF r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolinherit THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('role %s exists elevated', v_name);
    END IF;
  END LOOP;
END
$roles$;

CREATE TABLE sessions.session_event_facts (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 255),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 255),
  ts timestamptz NOT NULL,
  session_id text NOT NULL,
  source text NOT NULL,
  machine_id text NOT NULL,
  kind text NOT NULL,
  event_id text NOT NULL,
  project_id text,
  task_ref_id text,
  task_ref_empty smallint NOT NULL DEFAULT 0,
  task_ref_json text NOT NULL DEFAULT '{}',
  task_title text,
  user_id text,
  branch text,
  model text,
  provider text,
  input_tokens bigint NOT NULL,
  output_tokens bigint NOT NULL,
  reasoning_tokens bigint NOT NULL,
  cache_read_tokens bigint NOT NULL,
  cache_create_tokens bigint NOT NULL,
  cache_create_5m_tokens bigint,
  cache_create_1h_tokens bigint,
  cost_usd_micros bigint,
  files_touched text[] NOT NULL DEFAULT '{}',
  retention_days integer NOT NULL DEFAULT 0,
  row_id text NOT NULL CHECK (row_id ~ '^[0-9a-f]{64}$'),
  fingerprint text NOT NULL,
  source_version bigint NOT NULL CHECK (source_version >= 0),
  PRIMARY KEY (org_id, workspace_id, row_id)
);
CREATE INDEX session_event_facts_window ON sessions.session_event_facts (org_id, workspace_id, ts);
CREATE INDEX session_event_facts_scope ON sessions.session_event_facts (org_id, workspace_id, session_id);
ALTER TABLE sessions.session_event_facts ENABLE ROW LEVEL SECURITY;

CREATE TABLE sessions.session_output_facts (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 255),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 255),
  fact_kind text NOT NULL CHECK (fact_kind IN ('message', 'legacy_turn_message', 'commit', 'outcome')),
  ts timestamptz NOT NULL,
  scope_source text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  fact_key text NOT NULL,
  origin text NOT NULL,
  row_key text NOT NULL,
  added bigint CHECK (added IS NULL OR added >= 0),
  removed bigint CHECK (removed IS NULL OR removed >= 0),
  reverted smallint NOT NULL DEFAULT 0,
  retention_days integer NOT NULL DEFAULT 0,
  row_id text NOT NULL CHECK (row_id ~ '^[0-9a-f]{64}$'),
  fingerprint text NOT NULL,
  source_version bigint NOT NULL CHECK (source_version >= 0),
  PRIMARY KEY (org_id, workspace_id, row_id)
);
CREATE INDEX session_output_facts_window ON sessions.session_output_facts (org_id, workspace_id, ts);
CREATE INDEX session_output_facts_scope ON sessions.session_output_facts (org_id, workspace_id, session_id);
ALTER TABLE sessions.session_output_facts ENABLE ROW LEVEL SECURITY;

CREATE TABLE sessions.output_facts_session_state (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 255),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 255),
  source text NOT NULL,
  session_id text NOT NULL,
  complete boolean NOT NULL,
  synced_at timestamptz NOT NULL,
  min_ts timestamptz,
  max_ts timestamptz,
  source_version bigint NOT NULL CHECK (source_version >= 0),
  PRIMARY KEY (org_id, workspace_id, source, session_id)
);
ALTER TABLE sessions.output_facts_session_state ENABLE ROW LEVEL SECURITY;

CREATE TABLE sessions.output_facts_workspace_state (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 255),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 255),
  complete_since timestamptz NOT NULL,
  marked_at timestamptz NOT NULL DEFAULT now(),
  marked_by text NOT NULL CHECK (char_length(marked_by) BETWEEN 1 AND 255),
  PRIMARY KEY (org_id, workspace_id)
);
ALTER TABLE sessions.output_facts_workspace_state ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON sessions.session_event_facts, sessions.session_output_facts,
  sessions.output_facts_session_state, sessions.output_facts_workspace_state
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;

-- The writer's scope check, shared by every writer function: the caller, the tenant, the
-- harness and a bounded session id set that contains the canonical id.
CREATE FUNCTION sessions.output_facts_writer_scope(
  p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[], p_canonical_id text
)
RETURNS void
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp AS $scope$
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'search_consumer_runtime' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'output facts: caller is not the consumer runtime role';
  END IF;
  IF p_org_id IS NULL OR p_org_id = '' OR p_workspace_id IS NULL OR p_workspace_id = ''
     OR p_harness IS NULL OR p_harness = ''
     OR p_session_ids IS NULL OR cardinality(p_session_ids) NOT BETWEEN 1 AND 1000
     OR array_position(p_session_ids, NULL) IS NOT NULL
     OR (p_canonical_id IS NOT NULL AND NOT (p_canonical_id = ANY (p_session_ids))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid scope';
  END IF;
END
$scope$;

-- The stored source_version of the scope's canonical state row, locked for the rest of the
-- transaction (NULL when the scope has none).
CREATE FUNCTION sessions.output_facts_stored_version(
  p_org_id text, p_workspace_id text, p_harness text, p_canonical_id text
)
RETURNS bigint
LANGUAGE sql VOLATILE
SET search_path = pg_catalog, pg_temp AS $stored$
  SELECT s.source_version FROM sessions.output_facts_session_state AS s
   WHERE s.org_id = p_org_id AND s.workspace_id = p_workspace_id AND s.source = p_harness
     AND s.session_id = p_canonical_id
   FOR UPDATE;
$stored$;

CREATE FUNCTION sessions.output_facts_state(
  p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[], p_canonical_id text
)
RETURNS TABLE (min_ts timestamptz, max_ts timestamptz, source_version bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $state$
BEGIN
  PERFORM sessions.output_facts_writer_scope(p_org_id, p_workspace_id, p_harness, p_session_ids, p_canonical_id);
  RETURN QUERY
    SELECT min(s.min_ts) FILTER (WHERE s.min_ts IS NOT NULL AND s.max_ts IS NOT NULL),
           max(s.max_ts) FILTER (WHERE s.min_ts IS NOT NULL AND s.max_ts IS NOT NULL),
           max(s.source_version) FILTER (WHERE s.session_id = p_canonical_id)
      FROM sessions.output_facts_session_state AS s
     WHERE s.org_id = p_org_id AND s.workspace_id = p_workspace_id AND s.source = p_harness
       AND s.session_id = ANY (p_session_ids);
END
$state$;

CREATE FUNCTION sessions.output_facts_reconcile(
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
      p_source_version
    FROM jsonb_to_recordset(p_events) AS e(
      org_id text, workspace_id text, ts timestamptz, session_id text, source text, machine_id text, kind text,
      event_id text, project_id text, task_ref_id text, task_ref_empty smallint, task_ref_json text,
      task_title text, user_id text, branch text, model text, provider text, input_tokens bigint,
      output_tokens bigint, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint,
      cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, cost_usd_micros bigint,
      files_touched text[], retention_days integer, row_id text, fingerprint text)
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
    cost_usd_micros = EXCLUDED.cost_usd_micros, files_touched = EXCLUDED.files_touched,
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

CREATE FUNCTION sessions.output_facts_record_state(
  p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[], p_canonical_id text,
  p_complete boolean, p_min_ts timestamptz, p_max_ts timestamptz, p_source_version bigint,
  p_synced_at timestamptz
)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $record$
DECLARE v_stored bigint;
BEGIN
  PERFORM sessions.output_facts_writer_scope(p_org_id, p_workspace_id, p_harness, p_session_ids, p_canonical_id);
  IF p_canonical_id IS NULL OR p_complete IS NULL OR p_source_version IS NULL OR p_source_version < 0
     OR p_synced_at IS NULL OR ((p_min_ts IS NULL) <> (p_max_ts IS NULL)) OR p_min_ts > p_max_ts THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid state arguments';
  END IF;
  v_stored := sessions.output_facts_stored_version(p_org_id, p_workspace_id, p_harness, p_canonical_id);
  IF v_stored IS NOT NULL AND v_stored > p_source_version THEN
    RETURN false;
  END IF;
  INSERT INTO sessions.output_facts_session_state AS s
    (org_id, workspace_id, source, session_id, complete, synced_at, min_ts, max_ts, source_version)
  SELECT p_org_id, p_workspace_id, p_harness, i.session_id, p_complete, p_synced_at, p_min_ts, p_max_ts, p_source_version
    FROM (SELECT DISTINCT unnest(p_session_ids) AS session_id) AS i
  ON CONFLICT (org_id, workspace_id, source, session_id) DO UPDATE SET
    complete = EXCLUDED.complete, synced_at = EXCLUDED.synced_at, min_ts = EXCLUDED.min_ts,
    max_ts = EXCLUDED.max_ts, source_version = EXCLUDED.source_version;
  RETURN true;
END
$record$;

CREATE FUNCTION sessions.output_facts_erase(
  p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[]
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $erase$
DECLARE
  v_n bigint;
  v_total bigint := 0;
BEGIN
  PERFORM sessions.output_facts_writer_scope(p_org_id, p_workspace_id, p_harness, p_session_ids, NULL);
  DELETE FROM sessions.session_event_facts AS t
   WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.source = p_harness
     AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT; v_total := v_total + v_n;
  DELETE FROM sessions.session_output_facts AS t
   WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.scope_source = p_harness
     AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT; v_total := v_total + v_n;
  -- The state rows go too: a re-upload starts from a fresh directory epoch, which a leftover
  -- source_version would otherwise refuse as stale.
  DELETE FROM sessions.output_facts_session_state AS s
   WHERE s.org_id = p_org_id AND s.workspace_id = p_workspace_id AND s.source = p_harness
     AND s.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT; v_total := v_total + v_n;
  RETURN v_total;
END
$erase$;

CREATE FUNCTION sessions.output_facts_events_for(
  p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
)
RETURNS TABLE (
  ts timestamptz, session_id text, source text, machine_id text, kind text, event_id text,
  project_id text, task_ref_id text, task_ref_empty smallint, task_ref_json text, task_title text,
  user_id text, branch text, model text, provider text, input_tokens bigint, output_tokens bigint,
  reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint,
  cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, cost_usd_micros bigint,
  files_touched text[], row_id text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $events$
DECLARE v_org text;
BEGIN
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid window or limit';
  END IF;
  RETURN QUERY
    SELECT e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id, e.project_id, e.task_ref_id,
           e.task_ref_empty, e.task_ref_json, e.task_title, e.user_id, e.branch, e.model, e.provider,
           e.input_tokens, e.output_tokens, e.reasoning_tokens, e.cache_read_tokens, e.cache_create_tokens,
           e.cache_create_5m_tokens, e.cache_create_1h_tokens, e.cost_usd_micros, e.files_touched, e.row_id
      FROM sessions.session_event_facts AS e
     WHERE e.org_id = v_org AND e.workspace_id = p_workspace_id AND e.ts >= p_from AND e.ts < p_to
       AND (p_after_row_id IS NULL OR e.row_id > p_after_row_id)
     ORDER BY e.row_id
     LIMIT p_limit;
END
$events$;

CREATE FUNCTION sessions.output_facts_outputs_for(
  p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
)
RETURNS TABLE (
  fact_kind text, ts timestamptz, scope_source text, source text, session_id text, fact_key text,
  origin text, row_key text, added bigint, removed bigint, reverted smallint, row_id text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $outputs$
DECLARE v_org text;
BEGIN
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid window or limit';
  END IF;
  RETURN QUERY
    SELECT f.fact_kind, f.ts, f.scope_source, f.source, f.session_id, f.fact_key, f.origin, f.row_key,
           f.added, f.removed, f.reverted, f.row_id
      FROM sessions.session_output_facts AS f
     WHERE f.org_id = v_org AND f.workspace_id = p_workspace_id AND f.ts >= p_from AND f.ts < p_to
       AND (p_after_row_id IS NULL OR f.row_id > p_after_row_id)
     ORDER BY f.row_id
     LIMIT p_limit;
END
$outputs$;

CREATE FUNCTION sessions.output_facts_coverage_for(p_workspace_id text, p_from timestamptz, p_to timestamptz)
RETURNS TABLE (complete_since timestamptz, sessions bigint, incomplete bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coverage$
DECLARE v_org text;
BEGIN
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid window';
  END IF;
  RETURN QUERY
    SELECT (SELECT w.complete_since FROM sessions.output_facts_workspace_state AS w
             WHERE w.org_id = v_org AND w.workspace_id = p_workspace_id),
           count(*), count(*) FILTER (WHERE NOT s.complete)
      FROM sessions.output_facts_session_state AS s
     WHERE s.org_id = v_org AND s.workspace_id = p_workspace_id
       AND (s.min_ts IS NULL OR (s.min_ts < p_to AND s.max_ts >= p_from));
END
$coverage$;

REVOKE ALL ON FUNCTION sessions.output_facts_writer_scope(text, text, text, text[], text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_stored_version(text, text, text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_state(text, text, text, text[], text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_reconcile(text, text, text, text[], text, jsonb, jsonb, boolean, bigint)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_record_state(text, text, text, text[], text, boolean, timestamptz, timestamptz, bigint, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_erase(text, text, text, text[]) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_events_for(text, timestamptz, timestamptz, text, integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_outputs_for(text, timestamptz, timestamptz, text, integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.output_facts_coverage_for(text, timestamptz, timestamptz) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.output_facts_state(text, text, text, text[], text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.output_facts_reconcile(text, text, text, text[], text, jsonb, jsonb, boolean, bigint)
  TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.output_facts_record_state(text, text, text, text[], text, boolean, timestamptz, timestamptz, bigint, timestamptz)
  TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.output_facts_erase(text, text, text, text[]) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.output_facts_events_for(text, timestamptz, timestamptz, text, integer) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.output_facts_outputs_for(text, timestamptz, timestamptz, text, integer) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.output_facts_coverage_for(text, timestamptz, timestamptz) TO bulls96_read_api;
