-- 20261103070000_session_aggregates: GET /v1/sessions/output and /v1/sessions/activity
-- (non-q) on the Supabase lane, as claim-bound SECURITY DEFINER functions the
-- bulls96_read_api role executes through the tenant runner
-- (TENANT_SETTINGS_SQL binds bulls96_read.org_id/workspace_id; the functions
-- refuse when either setting is missing or the workspace does not match).
--
-- The SQL bodies are the relayhistory oracles verbatim, tenancy-bound instead of
-- Drizzle-scoped: session-output.ts getSessionOutput (messages + deduplicated commits
-- + normalized line counts over convergence_events, conversation_turns,
-- session_commit_links, session_outcomes) and session-activity.ts getSessionActivity
-- (rollup spans + per-day existence probes, events aggregate when the rollup
-- backfill is not marked complete). No request-time transcript reads anywhere.

-- Deterministic summary metrics, as SessionOutput in the recall contract.
CREATE FUNCTION sessions.session_output(
  p_workspace_id text, p_since timestamptz, p_until timestamptz
)
RETURNS TABLE (
  agent_messages bigint, commits bigint, lines_added bigint,
  lines_removed bigint, commits_without_line_counts bigint
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $output$
DECLARE
  v_org text;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session output: workspace is required';
  END IF;
  IF p_since IS NULL OR p_until IS NULL OR p_until < p_since THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session output: since and until are required';
  END IF;
  v_org := current_setting('bulls96_read.org_id', true);
  IF v_org IS NULL OR v_org = ''
     OR p_workspace_id IS DISTINCT FROM current_setting('bulls96_read.workspace_id', true) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session output: tenant scope is required';
  END IF;
  RETURN QUERY
  WITH events AS MATERIALIZED (
    SELECT * FROM sessions.convergence_events
     WHERE org_id = v_org AND workspace_id = p_workspace_id
       AND ts >= p_since AND ts <= p_until
  ), messages AS (
    SELECT DISTINCT source, session_id,
      COALESCE(NULLIF(request_key, ''), NULLIF(record->'payload'->>'message_id', ''),
        delivery_record_id, machine_id || ':' || event_id) AS message_id
    FROM events
    WHERE actor_role = 'assistant' AND btrim(COALESCE(content, '')) <> ''
      AND kind IN ('session_event', 'assistant', 'text')
      AND (record->'payload'->>'kind' = 'text'
        OR (record->'payload'->>'kind' IS NULL AND type IN ('assistant', 'text')))
  ), legacy_messages AS (
    SELECT t.id FROM sessions.conversation_turns t
    WHERE org_id = v_org AND workspace_id = p_workspace_id
      AND ts >= p_since AND ts <= p_until
      AND role = 'assistant' AND btrim(content) <> ''
      AND NOT EXISTS (
        SELECT 1 FROM messages m WHERE m.source = t.source AND m.session_id = t.session_id
      )
  ), commit_candidates AS (
    SELECT lower(commit_sha) AS sha, COALESCE(linked_at, received_at) AS ts, numstat AS stats
    FROM sessions.session_commit_links
    WHERE org_id = v_org AND workspace_id = p_workspace_id
    UNION ALL
    SELECT lower(commit_sha) AS sha, COALESCE(shipped_at, created_at) AS ts, numstat_json AS stats
    FROM sessions.session_outcomes
    WHERE org_id = v_org AND workspace_id = p_workspace_id
    UNION ALL
    SELECT matched.parts[1], e.ts,
      jsonb_build_object('added', COALESCE(matched.parts[2], '0'), 'deleted', COALESCE(matched.parts[3], '0'))
    FROM events e
    CROSS JOIN LATERAL regexp_matches(e.content,
      '(?:^|\n)\[[^\]\n]+ ([0-9a-f]{7,40})\][^\n]*\n[ \t]*[0-9]+ files? changed(?:, ([0-9]+) insertions?\(\+\))?(?:, ([0-9]+) deletions?\(-\))?',
      'g') AS matched(parts)
    WHERE e.actor_role = 'tool_result' AND EXISTS (
      SELECT 1 FROM sessions.convergence_events call
      WHERE call.org_id = e.org_id AND call.workspace_id = e.workspace_id
        AND call.source = e.source AND call.session_id = e.session_id
        AND call.kind = 'tool_call' AND call.ts <= e.ts
        AND NULLIF(call.record->'payload'->>'tool_use_id', '') = NULLIF(e.record->'payload'->>'tool_use_id', '')
        AND (NULLIF(e.record->'payload'->>'parent_id', '') IS NULL
          OR e.record->'payload'->>'parent_id' = call.record->'payload'->>'message_id'
          OR NULLIF(e.record->'payload'->>'message_id', '') = NULLIF(call.record->'payload'->>'message_id', ''))
        AND (NULLIF(e.record->'payload'->>'turn_id', '') IS NULL OR EXISTS (
          SELECT 1 FROM sessions.convergence_events parent
          WHERE parent.org_id = call.org_id AND parent.workspace_id = call.workspace_id
            AND parent.source = call.source AND parent.session_id = call.session_id
            AND parent.kind = 'session_event'
            AND NULLIF(parent.record->'payload'->>'message_id', '') = NULLIF(call.record->'payload'->>'message_id', '')
            AND parent.record->'payload'->>'turn_id' = e.record->'payload'->>'turn_id'
        ))
      HAVING count(DISTINCT jsonb_build_array(
        call.record->'payload'->>'message_id', call.tool_name,
        call.record->'payload'->>'args_json',
        CASE WHEN NULLIF(call.record->'payload'->>'message_id', '') IS NULL THEN call.ts END
      )) = 1
        AND bool_and(COALESCE(
          call.tool_name IN ('exec_command', 'exec', 'Bash', 'bash', 'shell', 'run_shell_command')
          AND call.record->'payload'->>'args_json' ~ '\mgit\M[^\n]*\mcommit\M', false))
    )
  ), commit_hashes AS MATERIALIZED (
    SELECT DISTINCT sha FROM commit_candidates
  ), short_lengths AS (
    SELECT DISTINCT length(sha) AS size FROM commit_hashes WHERE length(sha) BETWEEN 1 AND 39
  ), unique_prefixes AS MATERIALIZED (
    SELECT left(full_id.sha, short.size) AS prefix, min(full_id.sha) AS sha
    FROM commit_hashes full_id CROSS JOIN short_lengths short
    WHERE length(full_id.sha) = 40
    GROUP BY left(full_id.sha, short.size) HAVING count(*) = 1
  ), canonical_commits AS (
    SELECT COALESCE(prefix.sha, c.sha) AS sha, c.ts, c.stats
    FROM commit_candidates c LEFT JOIN unique_prefixes prefix ON prefix.prefix = c.sha
  ), stat_entries AS (
    SELECT sha, ts, CASE
      WHEN jsonb_typeof(stats) = 'array' THEN stats
      WHEN jsonb_typeof(stats) = 'object' AND EXISTS (
        SELECT 1 FROM jsonb_each(stats) field
        WHERE field.key IN ('added', 'deleted', 'removed', 'additions', 'deletions')
          AND jsonb_typeof(field.value) NOT IN ('object', 'array')
      ) THEN jsonb_build_array(stats)
      WHEN jsonb_typeof(stats) = 'object' THEN (SELECT jsonb_agg(value) FROM jsonb_each(stats))
    END AS entries
    FROM canonical_commits
  ), normalized_stats AS (
    SELECT sha, ts,
      CASE WHEN entries IS NOT NULL THEN (
        SELECT CASE WHEN COALESCE(bool_and(COALESCE(COALESCE(value->>'additions', value->>'added') ~ '^[0-9]{1,12}$' OR COALESCE(value->>'additions', value->>'added') = '-' OR (jsonb_typeof(value->'path') = 'string'
  AND value->'additions' = 'null'::jsonb AND value->'deletions' = 'null'::jsonb), false)), true)
          THEN COALESCE(sum(CASE WHEN COALESCE(value->>'additions', value->>'added') ~ '^[0-9]{1,12}$' THEN COALESCE(value->>'additions', value->>'added')::bigint ELSE 0 END), 0) END
        FROM jsonb_array_elements(entries)
      ) END AS added,
      CASE WHEN entries IS NOT NULL THEN (
        SELECT CASE WHEN COALESCE(bool_and(COALESCE(COALESCE(value->>'deletions', value->>'removed', value->>'deleted') ~ '^[0-9]{1,12}$' OR COALESCE(value->>'deletions', value->>'removed', value->>'deleted') = '-' OR (jsonb_typeof(value->'path') = 'string'
  AND value->'additions' = 'null'::jsonb AND value->'deletions' = 'null'::jsonb), false)), true)
          THEN COALESCE(sum(CASE WHEN COALESCE(value->>'deletions', value->>'removed', value->>'deleted') ~ '^[0-9]{1,12}$' THEN COALESCE(value->>'deletions', value->>'removed', value->>'deleted')::bigint ELSE 0 END), 0) END
        FROM jsonb_array_elements(entries)
      ) END AS removed
    FROM stat_entries
  ), commit_rows AS (
    SELECT sha, min(ts) AS ts, max(added) AS added, max(removed) AS removed
    FROM normalized_stats GROUP BY sha
  )
  SELECT ((SELECT count(*) FROM messages) + (SELECT count(*) FROM legacy_messages)) AS agent_messages,
    count(*) AS commits, COALESCE(sum(added)::bigint, 0::bigint) AS lines_added, COALESCE(sum(removed)::bigint, 0::bigint) AS lines_removed,
    count(*) FILTER (WHERE added IS NULL OR removed IS NULL) AS commits_without_line_counts
  FROM commit_rows WHERE ts >= p_since AND ts <= p_until;
END;
$output$;

-- Distinct sessions with at least one event on each UTC day of the window,
-- by source. The rollup path (spans + one index probe per session-day) runs only
-- when the rollout marker is complete; otherwise the events aggregate answers.
CREATE FUNCTION sessions.session_activity(
  p_workspace_id text, p_since timestamptz, p_until timestamptz
)
RETURNS TABLE (day text, source text, sessions integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $activity$
DECLARE
  v_org text;
  v_ready boolean;
BEGIN
  IF p_workspace_id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: workspace is required';
  END IF;
  IF p_since IS NULL OR p_until IS NULL OR p_until < p_since THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session activity: since and until are required';
  END IF;
  v_org := current_setting('bulls96_read.org_id', true);
  IF v_org IS NULL OR v_org = ''
     OR p_workspace_id IS DISTINCT FROM current_setting('bulls96_read.workspace_id', true) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session activity: tenant scope is required';
  END IF;
  SELECT (r.completed_at IS NOT NULL) INTO v_ready FROM sessions.session_rollup_rollout r;
  IF v_ready IS NULL THEN v_ready := false; END IF;

  IF v_ready THEN
    RETURN QUERY
    WITH spans AS MATERIALIZED (
      SELECT r.source, r.session_id,
             greatest(min(r.first_ts), p_since) AS lo,
             least(max(r.last_ts), p_until) AS hi
        FROM sessions.session_rollups AS r
       WHERE r.org_id = v_org AND r.workspace_id = p_workspace_id
         AND r.last_ts >= p_since AND r.first_ts <= p_until
       GROUP BY r.source, r.session_id
    ),
    candidates AS (
      SELECT s.source, s.session_id, d.day
        FROM spans AS s,
             generate_series(date_trunc('day', s.lo, 'UTC'),
                             date_trunc('day', s.hi, 'UTC'),
                             interval '24 hours') AS d(day)
    )
    SELECT to_char(c.day at time zone 'UTC', 'YYYY-MM-DD') AS day,
           c.source,
           count(*)::int AS sessions
      FROM candidates AS c
     CROSS JOIN LATERAL (
       SELECT 1 FROM sessions.convergence_events e
        WHERE e.org_id = v_org AND e.workspace_id = p_workspace_id
          AND e.ts >= p_since AND e.ts <= p_until
          AND e.session_id = c.session_id
          AND e.source = c.source
          AND e.ts >= c.day
          AND e.ts < c.day + interval '24 hours'
        LIMIT 1
     ) AS hit
     GROUP BY 1, 2
     ORDER BY 1, 2;
  ELSE
    RETURN QUERY
    SELECT to_char(e.ts at time zone 'UTC', 'YYYY-MM-DD') AS day,
           e.source,
           count(distinct e.session_id)::int AS sessions
      FROM sessions.convergence_events e
     WHERE e.org_id = v_org AND e.workspace_id = p_workspace_id
       AND e.ts >= p_since AND e.ts <= p_until
     GROUP BY 1, 2
     ORDER BY 1, 2;
  END IF;
END;
$activity$;

-- Fail closed when the read lane's role is not provisioned yet: a grant into a
-- vacuum would leave the function callable by nobody, or worse, lazily re-granted.
DO $agg$
BEGIN
  PERFORM 1 FROM pg_roles WHERE rolname = 'bulls96_read_api';
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = '20261103070000_session_aggregates requires bulls96_read_api (run supabase/bootstrap/session_read_api_role.sql first)';
  END IF;
END
$agg$;
REVOKE ALL ON FUNCTION sessions.session_output(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_activity(text, timestamptz, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_output(text, timestamptz, timestamptz)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.session_activity(text, timestamptz, timestamptz)
  TO bulls96_read_api;
