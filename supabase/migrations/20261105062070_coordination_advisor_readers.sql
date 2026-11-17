-- Slot 20261105062070. Coordination live advisories, PR 4: the worker's own readers on the postgres facts backend
-- (docs/coordination-live-advisories.md). Enables nothing: the worker stays off unless
-- COORDINATION_ADVISOR_ENABLED is exactly "true", and it uses these readers only when
-- COORDINATION_ADVISOR_FACTS_BACKEND=postgres. Code only: applying this to any real database needs Khaliq's
-- separate, explicit OK.
--
-- The overlaps readers (047000 coordination_write_facts_for / coordination_output_facts_for, 045040
-- output_facts_events_for) are pinned to the read API's request scope (session_directory_read_api_scope) and
-- granted to bulls96_read_api only. The worker runs as service_role and scans every enabled workspace, so it
-- gets its own mirrors here. They take the tenant explicitly, check its bounds (the 062000 tenant check), and
-- filter every row on org_id AND workspace_id. Columns, window and page rules match the originals exactly.
--
-- coordination_advisories_recent_v1 makes the cooldown and the hourly cap durable. For one tenant it returns:
--   'pair' rows: each (recipient, peer, urgency) with the latest created_at at or after p_pair_since (clamped
--   to the 7-day retention), so the worker keeps its cooldown per urgency;
--   'recipient' rows: each recipient's advisory count and latest created_at in the last hour.
-- Every status counts: an acked or expired row was still sent.
--
-- No table, index, column or backfill. Every function is SECURITY DEFINER with search_path pinned. EXECUTE is
-- revoked from PUBLIC and every runtime role and then granted to service_role only (the self-host runtime role
-- through RUNTIME_APPLICATION_FUNCTIONS). No transcript content is read or returned.

CREATE FUNCTION sessions.coordination_advisor_write_facts_v1(
  p_org_id text, p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
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
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisor_writes$
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisor facts: invalid tenant';
  END IF;
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisor facts: invalid write window or limit';
  END IF;
  RETURN QUERY
    SELECT e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id, e.project_id, e.task_ref_id,
           e.task_ref_empty, e.task_ref_json, e.task_title, e.user_id, e.branch, e.model, e.provider,
           e.input_tokens, e.output_tokens, e.reasoning_tokens, e.cache_read_tokens, e.cache_create_tokens,
           e.cache_create_5m_tokens, e.cache_create_1h_tokens, e.cost_usd_micros, e.files_touched, e.row_id
      FROM sessions.session_event_facts AS e
     WHERE e.org_id = p_org_id AND e.workspace_id = p_workspace_id AND e.ts >= p_from AND e.ts < p_to
       AND cardinality(e.files_touched) > 0
       AND (p_after_row_id IS NULL OR e.row_id > p_after_row_id)
     ORDER BY e.row_id
     LIMIT p_limit;
END
$coordination_advisor_writes$;

CREATE FUNCTION sessions.coordination_advisor_output_facts_v1(
  p_org_id text, p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
)
RETURNS TABLE (
  fact_kind text, ts timestamptz, scope_source text, source text, session_id text, fact_key text,
  origin text, row_key text, added bigint, removed bigint, reverted smallint, row_id text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisor_outputs$
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisor facts: invalid tenant';
  END IF;
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisor facts: invalid output window or limit';
  END IF;
  RETURN QUERY
    SELECT f.fact_kind, f.ts, f.scope_source, f.source, f.session_id, f.fact_key, f.origin, f.row_key,
           f.added, f.removed, f.reverted, f.row_id
      FROM sessions.session_output_facts AS f
     WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id AND f.ts >= p_from AND f.ts < p_to
       AND f.fact_kind = ANY (ARRAY['commit'::text, 'outcome'::text])
       AND (p_after_row_id IS NULL OR f.row_id > p_after_row_id)
     ORDER BY f.row_id
     LIMIT p_limit;
END
$coordination_advisor_outputs$;

CREATE FUNCTION sessions.coordination_advisor_usage_facts_v1(
  p_org_id text, p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
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
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisor_usage$
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisor facts: invalid tenant';
  END IF;
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisor facts: invalid usage window or limit';
  END IF;
  RETURN QUERY
    SELECT e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id, e.project_id, e.task_ref_id,
           e.task_ref_empty, e.task_ref_json, e.task_title, e.user_id, e.branch, e.model, e.provider,
           e.input_tokens, e.output_tokens, e.reasoning_tokens, e.cache_read_tokens, e.cache_create_tokens,
           e.cache_create_5m_tokens, e.cache_create_1h_tokens, e.cost_usd_micros, e.files_touched, e.row_id
      FROM sessions.session_event_facts AS e
     WHERE e.org_id = p_org_id AND e.workspace_id = p_workspace_id AND e.ts >= p_from AND e.ts < p_to
       AND (p_after_row_id IS NULL OR e.row_id > p_after_row_id)
     ORDER BY e.row_id
     LIMIT p_limit;
END
$coordination_advisor_usage$;

-- Worker: durable cooldown and cap state for one tenant (see the header). At most 10000 rows of each kind,
-- newest first.
CREATE FUNCTION sessions.coordination_advisories_recent_v1(p_org_id text, p_workspace_id text, p_pair_since timestamptz)
RETURNS TABLE (
  row_kind text, harness text, session_id text, peer_harness text, peer_session_id text, urgency text,
  latest_at timestamptz, sent_last_hour integer
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_recent$
DECLARE v_since timestamptz;
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisories: invalid tenant';
  END IF;
  IF p_pair_since IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisories: pair window required';
  END IF;
  v_since := greatest(p_pair_since, now() - interval '7 days');
  RETURN QUERY
    (SELECT 'pair'::text, a.harness, a.session_id, a.peer_harness, a.peer_session_id, a.urgency, max(a.created_at),
            NULL::integer
       FROM sessions.coordination_advisories AS a
      WHERE a.org_id = p_org_id AND a.workspace_id = p_workspace_id AND a.created_at >= v_since
      GROUP BY a.harness, a.session_id, a.peer_harness, a.peer_session_id, a.urgency
      ORDER BY max(a.created_at) DESC
      LIMIT 10000)
    UNION ALL
    (SELECT 'recipient'::text, a.harness, a.session_id, NULL::text, NULL::text, NULL::text, max(a.created_at),
            count(*)::integer
       FROM sessions.coordination_advisories AS a
      WHERE a.org_id = p_org_id AND a.workspace_id = p_workspace_id AND a.created_at >= now() - interval '1 hour'
      GROUP BY a.harness, a.session_id
      ORDER BY max(a.created_at) DESC
      LIMIT 10000);
END
$coordination_advisories_recent$;

REVOKE ALL ON FUNCTION sessions.coordination_advisor_write_facts_v1(text,text,timestamptz,timestamptz,text,integer),
  sessions.coordination_advisor_output_facts_v1(text,text,timestamptz,timestamptz,text,integer),
  sessions.coordination_advisor_usage_facts_v1(text,text,timestamptz,timestamptz,text,integer),
  sessions.coordination_advisories_recent_v1(text,text,timestamptz)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.coordination_advisor_write_facts_v1(text,text,timestamptz,timestamptz,text,integer),
  sessions.coordination_advisor_output_facts_v1(text,text,timestamptz,timestamptz,text,integer),
  sessions.coordination_advisor_usage_facts_v1(text,text,timestamptz,timestamptz,text,integer),
  sessions.coordination_advisories_recent_v1(text,text,timestamptz)
  TO service_role;
