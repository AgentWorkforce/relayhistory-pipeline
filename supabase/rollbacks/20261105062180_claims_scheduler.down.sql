-- Rolls back 20261105062180 claims scheduler: restores 20261105046000's sessions.analysis_request_v1 body exactly
-- (CREATE OR REPLACE keeps its owner and its single grant to bulls96_analysis_admit), drops
-- session_analysis_schedule_v1, session_analysis_schedule_candidates_v1, the two admission helpers and the table
-- sessions.session_analysis_schedule_ledger_v1, and removes the ledger row.
-- Analysis jobs the scheduler enqueued are ordinary jobs and stay. The scheduler ledger is its durable budget:
-- while any ledger row exists this refuses (55000), because dropping it forgets today's spend and a re-applied
-- scheduler would start from zero. To drop it anyway, set the override in the same session first (this file
-- opens its own transaction), for example
--   psql -v ON_ERROR_STOP=1 -c "SET claims_scheduler.rollback_drop_ledger = 'true'" -f <this file>
-- Anything but exactly 'true' (unset included) refuses. Turn SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED off first:
-- without 062180 the scheduler's calls fail and are only counted.
-- Guard, restore and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claims scheduler: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062180_claims_scheduler.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claims scheduler: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062180_claims_scheduler.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'56cb14743be633925992aee3c88dd6cec1ca08eb334363cdb6ea38ecbd69a969'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claims scheduler: own applied checksum required';END IF;
 LOCK TABLE sessions.session_analysis_schedule_ledger_v1 IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.session_analysis_schedule_ledger_v1)
    AND coalesce(current_setting('claims_scheduler.rollback_drop_ledger',true),'')<>'true'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='claims scheduler: ledger rows exist; set claims_scheduler.rollback_drop_ledger=true to drop them';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.analysis_request_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request$
DECLARE
  c_analysis_version constant text := 'session-analysis-v2.4:96bulls-s3-v1';
  c_schema_version constant integer := 2;
  c_model_version constant text := 'gpt-5.4';
  c_summary_version constant text := 'session-summary-v3:workspace-output-v2';
  c_summary_model_version constant text := 'gpt-5.6-luna';
  v_revision text;
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
  -- Serializes admission per org, as relayhistory does; the evidence check follows it.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  -- The per-scope lock C3's purge takes first: an admission racing a purge either sees its
  -- fence (blocked until the purge commits) or commits before it (and is purged).
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-scope:' || p_org_id || '/' || p_workspace_id || '/' || p_session_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                  WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
                    AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
                    AND p.deleted_at IS NULL) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  -- Purged by a C3 reset/disable and not re-uploaded since: as if erased.
  IF EXISTS (SELECT 1 FROM sessions.session_analysis_purge_fences AS f
                  WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id AND f.session_id = p_session_id
                    AND NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                                     WHERE p.org_id = f.org_id AND p.workspace_id = f.workspace_id
                                       AND (coalesce(p.canonical_session_id, p.session_id) = f.session_id OR p.session_id = f.session_id)
                                       AND p.deleted_at IS NULL AND p.created_at > f.purged_at)) THEN
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
  END IF;
  INSERT INTO sessions.session_analysis_requests_v2
    (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id)
  VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, 'resolved', v_job_id)
  RETURNING * INTO v_request;
  RETURN jsonb_build_object('outcome', 'accepted',
    'request', sessions.analysis_request_json_v1(v_request));
END
$request$;

DROP FUNCTION sessions.session_analysis_schedule_v1(text,text,text,integer,integer,integer,bigint,bigint),
 sessions.session_analysis_schedule_candidates_v1(integer,integer,integer),
 sessions.session_analysis_admit_job_v1(text,text,text),
 sessions.session_analysis_admit_live_v1(text,text,text);
DROP TABLE sessions.session_analysis_schedule_ledger_v1;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062180_claims_scheduler.sql';
COMMIT;
