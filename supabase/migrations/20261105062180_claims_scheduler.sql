-- Slot 20261105062180 (PROVISIONAL: confirm with the migration registry owner before landing). Session
-- Intelligence M1-3: the live claims scheduler's store side (docs/session-analysis-runner.md, Claims scheduler).
-- Enables nothing: the runner calls these only with SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED=true and its other
-- gates. Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- analysis_request_v1 (046000) is replaced with the same signature, ACL, outcomes and order: its lock, evidence
-- and purge-fence block moves verbatim into session_analysis_admit_live_v1, and its reuse-or-new-job block into
-- session_analysis_admit_job_v1, so the scheduler reuses them instead of copying them. Both helpers are owner-only
-- (called by the definers as their owner); neither is SECURITY DEFINER.
--
-- session_analysis_schedule_ledger_v1 (new, empty): one row per job the scheduler enqueued, with its worst-case
-- token reservation. It is the scheduler's durable budget (a restart keeps the counts). analysis_job_id references
-- the job ON DELETE CASCADE, so every path that erases a session's analysis jobs (the per-session operator purge,
-- the per-user erase, the C3 purge, the v2 forget trigger) also erases its ledger rows. Rows older than 8 days are
-- deleted by session_analysis_schedule_v1, opportunistically.
--
-- session_analysis_schedule_v1 (service_role and the runtime role): under the same per-org and per-scope locks as
-- analysis_request_v1, plus one global scheduler lock taken after them, it answers not_found / in_flight /
-- over_budget (workspace_hourly, global_daily, session_interval, daily_tokens) / reused / enqueued. Over budget is
-- a reason, never an error, and writes nothing. It never writes a request row, so it never spends the user
-- request limits (20 sessions per org and 30 per session per UTC day).
-- session_analysis_schedule_candidates_v1 (service_role and the runtime role): live sessions of workspaces whose
-- 'claims' control is on (062160), with no job in flight, activity newer than their last completed analysis, and no
-- completed analysis or scheduled job within the minimum interval.

CREATE TABLE sessions.session_analysis_schedule_ledger_v1 (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512 AND btrim(org_id) <> ''),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id) <> ''),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512 AND btrim(session_id) <> ''),
  analysis_job_id uuid NOT NULL REFERENCES sessions.session_analysis_jobs_v2 (id) ON DELETE CASCADE,
  reserved_tokens bigint NOT NULL CHECK (reserved_tokens BETWEEN 1 AND 1000000),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX session_analysis_schedule_ledger_v1_workspace ON sessions.session_analysis_schedule_ledger_v1 (org_id, workspace_id, created_at);
CREATE INDEX session_analysis_schedule_ledger_v1_session ON sessions.session_analysis_schedule_ledger_v1 (org_id, workspace_id, session_id, created_at);
CREATE INDEX session_analysis_schedule_ledger_v1_day ON sessions.session_analysis_schedule_ledger_v1 (created_at);
CREATE INDEX session_analysis_schedule_ledger_v1_job ON sessions.session_analysis_schedule_ledger_v1 (analysis_job_id);
ALTER TABLE sessions.session_analysis_schedule_ledger_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_schedule_ledger_v1
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder, bulls96_analysis_admit;

-- 046000's lock, evidence and purge-fence block, verbatim; false = not found.
CREATE FUNCTION sessions.session_analysis_admit_live_v1(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS boolean LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $admit_live$
BEGIN
  -- Serializes admission per org, as relayhistory does; the evidence check follows it.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  -- The per-scope lock C3's purge takes first: an admission racing a purge either sees its
  -- fence (blocked until the purge commits) or commits before it (and is purged).
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-scope:' || p_org_id || '/' || p_workspace_id || '/' || p_session_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                  WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
                    AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
                    AND p.deleted_at IS NULL) THEN
    RETURN false;
  END IF;
  -- Purged by a C3 reset/disable and not re-uploaded since: as if erased.
  IF EXISTS (SELECT 1 FROM sessions.session_analysis_purge_fences AS f
                  WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id AND f.session_id = p_session_id
                    AND NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                                     WHERE p.org_id = f.org_id AND p.workspace_id = f.workspace_id
                                       AND (coalesce(p.canonical_session_id, p.session_id) = f.session_id OR p.session_id = f.session_id)
                                       AND p.deleted_at IS NULL AND p.created_at > f.purged_at)) THEN
    RETURN false;
  END IF;
  RETURN true;
END
$admit_live$;

-- 046000's reuse-or-new-job block, verbatim; reused = an existing completed job answers.
CREATE FUNCTION sessions.session_analysis_admit_job_v1(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS TABLE (job_id uuid, reused boolean) LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $admit_job$
DECLARE
  c_analysis_version constant text := 'session-analysis-v2.4:96bulls-s3-v1';
  c_schema_version constant integer := 2;
  c_model_version constant text := 'gpt-5.4';
  c_summary_version constant text := 'session-summary-v3:workspace-output-v2';
  c_summary_model_version constant text := 'gpt-5.6-luna';
  v_revision text;
  v_job_id uuid;
BEGIN
  -- A native analysis of this engine completed in the last 30 minutes over the SAME live
  -- pointer set is reused (relayhistory reused by source fingerprint; this is its revision).
  v_revision := sessions.session_analysis_source_revision(p_org_id, p_workspace_id, p_session_id);
  SELECT id INTO v_job_id FROM sessions.session_analysis_jobs_v2
   WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND session_id = p_session_id
     AND origin = 'native' AND status = 'completed'
     AND source_revision IS NOT DISTINCT FROM v_revision AND v_revision IS NOT NULL
     AND analysis_version = c_analysis_version AND schema_version = c_schema_version
     AND model_version = c_model_version AND summary_version = c_summary_version
     AND summary_model_version = c_summary_model_version
     AND completed_at > now() - interval '30 minutes'
   ORDER BY completed_at DESC, id DESC LIMIT 1;
  IF v_job_id IS NULL THEN
    INSERT INTO sessions.session_analysis_jobs_v2
      (org_id, workspace_id, session_id, analysis_version, schema_version, model_version,
       summary_version, summary_model_version)
    VALUES (p_org_id, p_workspace_id, p_session_id, c_analysis_version, c_schema_version, c_model_version,
            c_summary_version, c_summary_model_version)
    RETURNING id INTO v_job_id;
    RETURN QUERY SELECT v_job_id, false;
    RETURN;
  END IF;
  RETURN QUERY SELECT v_job_id, true;
END
$admit_job$;

-- 046000's admission, with the two blocks above called instead of inlined.
CREATE OR REPLACE FUNCTION sessions.analysis_request_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request$
DECLARE
  v_day timestamptz := date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
  v_latest sessions.session_analysis_requests_v2%ROWTYPE;
  v_request sessions.session_analysis_requests_v2%ROWTYPE;
  v_job sessions.session_analysis_jobs_v2%ROWTYPE;
  v_job_id uuid;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  PERFORM sessions.session_analysis_admit_scope(p_org_id, p_workspace_id);
  -- The locks, the evidence check and the purge fence: session_analysis_admit_live_v1 (062180).
  IF NOT sessions.session_analysis_admit_live_v1(p_org_id, p_workspace_id, p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  SELECT * INTO v_latest FROM sessions.session_analysis_requests_v2
   WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;

  -- An analysis already queued or running for this session answers the request.
  SELECT r.* INTO v_request FROM sessions.session_analysis_requests_v2 AS r
    JOIN sessions.session_analysis_jobs_v2 AS j ON j.id = r.analysis_job_id
   WHERE r.org_id = p_org_id AND r.workspace_id = p_workspace_id AND r.session_id = p_session_id
     AND j.origin = 'native' AND j.status IN ('queued', 'running')
   ORDER BY r.created_at DESC, r.id DESC LIMIT 1;
  IF v_request.id IS NULL THEN
    -- Throttled (a request in the last 2 minutes): the session's latest request.
    IF v_latest.id IS NOT NULL AND v_latest.created_at > now() - interval '2 minutes' THEN
      v_request := v_latest;
    END IF;
  END IF;
  IF v_request.id IS NOT NULL THEN
    RETURN jsonb_build_object('outcome', 'accepted',
      'request', sessions.analysis_request_json_v1(v_request));
  END IF;

  IF (SELECT count(*) FROM sessions.session_analysis_requests_v2
       WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND session_id = p_session_id
         AND created_at >= v_day) >= 30
     OR (SELECT count(DISTINCT (workspace_id, session_id)) FROM sessions.session_analysis_requests_v2
          WHERE org_id = p_org_id AND origin = 'native' AND created_at >= v_day
            AND NOT (workspace_id = p_workspace_id AND session_id = p_session_id)) >= 20 THEN
    RETURN jsonb_build_object('outcome', 'rate_limited');
  END IF;

  -- Reuse or a new job: session_analysis_admit_job_v1 (062180).
  SELECT a.job_id INTO v_job_id FROM sessions.session_analysis_admit_job_v1(p_org_id, p_workspace_id, p_session_id) AS a;
  INSERT INTO sessions.session_analysis_requests_v2
    (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id)
  VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, 'resolved', v_job_id)
  RETURNING * INTO v_request;
  RETURN jsonb_build_object('outcome', 'accepted',
    'request', sessions.analysis_request_json_v1(v_request));
END
$request$;

-- The scheduler's admission. Budgets are arguments, clamped: workspace hourly 1..100, global daily 1..1000,
-- session interval 60..86400 s, daily token ceiling 1..50000000, the job's reservation 1..1000000. Days are UTC.
CREATE FUNCTION sessions.session_analysis_schedule_v1(
  p_org_id text, p_workspace_id text, p_session_id text, p_workspace_hourly_cap integer, p_global_daily_cap integer,
  p_min_interval_seconds integer, p_daily_token_ceiling bigint, p_reserve_tokens bigint
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $schedule$
DECLARE
  v_day timestamptz := date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
  v_hourly integer; v_daily integer; v_interval integer; v_ceiling bigint; v_reserve bigint;
  v_job_id uuid; v_reused boolean; v_spent bigint;
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>']);
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = ''
    OR p_workspace_hourly_cap IS NULL OR p_global_daily_cap IS NULL OR p_min_interval_seconds IS NULL
    OR p_daily_token_ceiling IS NULL OR p_reserve_tokens IS NULL THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  v_hourly := least(greatest(p_workspace_hourly_cap, 1), 100);
  v_daily := least(greatest(p_global_daily_cap, 1), 1000);
  v_interval := least(greatest(p_min_interval_seconds, 60), 86400);
  v_ceiling := least(greatest(p_daily_token_ceiling, 1), 50000000);
  v_reserve := least(greatest(p_reserve_tokens, 1), 1000000);
  IF NOT sessions.session_analysis_admit_live_v1(p_org_id, p_workspace_id, p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  -- After the per-org and per-scope locks (the same order analysis_request_v1 takes them): one lock for the
  -- global counts, which span orgs. Only this function takes it.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-schedule:global', 0));
  DELETE FROM sessions.session_analysis_schedule_ledger_v1 WHERE created_at < now() - interval '8 days';
  IF EXISTS (SELECT 1 FROM sessions.session_analysis_jobs_v2 AS j
              WHERE j.org_id = p_org_id AND j.workspace_id = p_workspace_id AND j.session_id = p_session_id
                AND j.origin = 'native' AND j.status IN ('queued', 'running')) THEN
    RETURN jsonb_build_object('outcome', 'in_flight');
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.session_analysis_schedule_ledger_v1 AS l
              WHERE l.org_id = p_org_id AND l.workspace_id = p_workspace_id AND l.session_id = p_session_id
                AND l.created_at > now() - make_interval(secs => v_interval)) THEN
    RETURN jsonb_build_object('outcome', 'over_budget', 'reason', 'session_interval');
  END IF;
  IF (SELECT count(*) FROM sessions.session_analysis_schedule_ledger_v1 AS l
       WHERE l.org_id = p_org_id AND l.workspace_id = p_workspace_id AND l.created_at > now() - interval '1 hour') >= v_hourly THEN
    RETURN jsonb_build_object('outcome', 'over_budget', 'reason', 'workspace_hourly');
  END IF;
  IF (SELECT count(*) FROM sessions.session_analysis_schedule_ledger_v1 AS l WHERE l.created_at >= v_day) >= v_daily THEN
    RETURN jsonb_build_object('outcome', 'over_budget', 'reason', 'global_daily');
  END IF;
  SELECT coalesce(sum(l.reserved_tokens), 0) INTO v_spent FROM sessions.session_analysis_schedule_ledger_v1 AS l
   WHERE l.created_at >= v_day;
  IF v_spent + v_reserve > v_ceiling THEN
    RETURN jsonb_build_object('outcome', 'over_budget', 'reason', 'daily_tokens');
  END IF;
  SELECT a.job_id, a.reused INTO v_job_id, v_reused FROM sessions.session_analysis_admit_job_v1(p_org_id, p_workspace_id, p_session_id) AS a;
  IF v_reused THEN
    RETURN jsonb_build_object('outcome', 'reused');
  END IF;
  INSERT INTO sessions.session_analysis_schedule_ledger_v1 (org_id, workspace_id, session_id, analysis_job_id, reserved_tokens)
  VALUES (p_org_id, p_workspace_id, p_session_id, v_job_id, v_reserve);
  RETURN jsonb_build_object('outcome', 'enqueued', 'reservedTokens', v_reserve);
END
$schedule$;

-- Live sessions to schedule, newest activity first, at most p_limit (1..200): sessions of workspaces whose
-- 'claims' control is on globally and for the workspace (062160), with last_activity_at within p_active_seconds
-- (60..86400) by their canonical id, no native job queued or running, activity newer than their last completed
-- native analysis, and neither a completed analysis nor a scheduled job within p_min_interval_seconds (60..86400).
-- User turns since the last analysis are not derivable from analysis data (the jobs carry no turn coverage), so
-- this is the documented fallback: newer activity, and the interval.
CREATE FUNCTION sessions.session_analysis_schedule_candidates_v1(
  p_active_seconds integer, p_min_interval_seconds integer, p_limit integer
) RETURNS TABLE (org_id text, workspace_id text, session_id text, last_activity_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $schedule_candidates$
DECLARE v_active integer; v_interval integer; v_limit integer;
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>']);
  IF p_active_seconds IS NULL OR p_min_interval_seconds IS NULL OR p_limit IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'claims scheduler: window, interval and limit are required';
  END IF;
  v_active := least(greatest(p_active_seconds, 60), 86400);
  v_interval := least(greatest(p_min_interval_seconds, 60), 86400);
  v_limit := least(greatest(p_limit, 1), 200);
  RETURN QUERY
    WITH ws AS (
      SELECT w.org_id, w.workspace_id FROM sessions.session_intel_controls AS w
       WHERE w.scope = 'workspace' AND w.feature = 'claims' AND w.enabled
         AND EXISTS (SELECT 1 FROM sessions.session_intel_controls AS g
                      WHERE g.scope = 'global' AND g.feature = 'claims' AND g.enabled)
    ), live AS (
      SELECT s.org_id, s.workspace_id, s.sid AS session_id, max(s.last_activity_at) AS last_activity_at
        FROM ws CROSS JOIN LATERAL (
          SELECT si.org_id, si.workspace_id, coalesce(si.canonical_session_id, si.session_id) AS sid, si.last_activity_at
            FROM sessions.session_index AS si
           WHERE si.org_id = ws.org_id AND si.workspace_id = ws.workspace_id AND si.deleted_at IS NULL
             AND si.last_activity_at >= now() - make_interval(secs => v_active)
           ORDER BY si.last_activity_at DESC LIMIT 1000) AS s
       GROUP BY s.org_id, s.workspace_id, s.sid
    )
    SELECT l.org_id, l.workspace_id, l.session_id, l.last_activity_at FROM live AS l
     WHERE NOT EXISTS (SELECT 1 FROM sessions.session_analysis_jobs_v2 AS j
                        WHERE j.org_id = l.org_id AND j.workspace_id = l.workspace_id AND j.session_id = l.session_id
                          AND j.origin = 'native'
                          AND (j.status IN ('queued', 'running')
                               OR (j.status = 'completed' AND (j.completed_at >= l.last_activity_at
                                                              OR j.completed_at > now() - make_interval(secs => v_interval)))))
       AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_schedule_ledger_v1 AS x
                        WHERE x.org_id = l.org_id AND x.workspace_id = l.workspace_id AND x.session_id = l.session_id
                          AND x.created_at > now() - make_interval(secs => v_interval))
     ORDER BY l.last_activity_at DESC, l.org_id, l.workspace_id, l.session_id
     LIMIT v_limit;
END
$schedule_candidates$;

REVOKE ALL ON FUNCTION sessions.session_analysis_admit_live_v1(text,text,text),
  sessions.session_analysis_admit_job_v1(text,text,text),
  sessions.session_analysis_schedule_v1(text,text,text,integer,integer,integer,bigint,bigint),
  sessions.session_analysis_schedule_candidates_v1(integer,integer,integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder, bulls96_analysis_admit;
GRANT EXECUTE ON FUNCTION sessions.session_analysis_schedule_v1(text,text,text,integer,integer,integer,bigint,bigint),
  sessions.session_analysis_schedule_candidates_v1(integer,integer,integer)
  TO service_role;
