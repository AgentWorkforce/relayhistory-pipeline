-- Registry slot 046000 (lane F1, session ANALYSIS panel off Neon). The full-analysis
-- request/job lifecycle that relayhistory ran on Neon (sessions.session_analysis_requests,
-- sessions.session_analysis_jobs; RHC migrations 0015, 0016, 0024, 0031, 0043, 0062), owned
-- by sessions-pipeline. The engine is src/session-analysis (relayhistory's, ported as-is).
--
-- Differences from relayhistory, all deliberate:
--   * One job kind. The brief half is sessions.session_briefs (src/session-brief, lane F2);
--     relayhistory's per-request brief job is not recreated, so there is no second brief
--     generator. A request links at most one analysis job.
--   * No evidence or result in Postgres (the 0004 no-text rule). A job carries its scope;
--     the worker reads canonical S3 evidence when it runs, and the result is an object under
--     the session's manifest directory, recorded here by key, SHA-256 and size.
--   * Scope is (org, workspace, session): the requesting workspace, not the whole org.
--   * Backfilled Neon history (origin 'neon_backfill') keeps its public v1 values
--     (status, error, v1 job id, versions) so the read is byte-equal to Neon's
--     read_session_analysis_v1 for those rows.
--
-- Admission limits are relayhistory's (0024 session_request_limit, 0043
-- sessions_admitted_today, 0062 request_session_analysis_v1): one request per session per
-- 2 minutes (a throttled request returns the latest), 30 per session per UTC day, 20
-- distinct other sessions admitted per org per UTC day; an analysis completed in the last
-- 30 minutes is reused (relayhistory 0024 collect: "defers its analysis").
--
-- Functions:
--   sessions.analysis_request_v1(org, workspace, user, session)  SECURITY DEFINER, EXECUTE
--     bulls96_analysis_admit only: the read Lambda's separate admission principal (NOLOGIN
--     until its operator step; provisioned before migrate by session_read_api_roles.sql).
--     The tenant must equal the transaction's bound bulls96_read.* settings (the verified
--     assertion), as session_directory_read_api_scope pins the read role.
--   sessions.session_analysis_source_revision(org, workspace, session)  the live pointer-set
--     revision (sha256 over the live pointers); reuse and publication compare it.
--   sessions.session_analysis_result_tickets  erase-safe publication: a ticket is recorded
--     (lease + liveness + revision checked) before a result object is written and retired
--     with the completion; the forget trigger tickets every result key it orphans; the
--     worker's sweeper deletes the object of an orphan or expired ticket.
--   sessions.analysis_read_v1(workspace, session)  SECURITY DEFINER, EXECUTE bulls96_read_api;
--     the tenant comes from sessions.session_directory_read_api_scope (034000).
--   sessions.session_analysis_v2_forget()  erasure trigger (session_briefs_forget pattern).
-- The worker (claim, complete, fail) uses the tables as service_role, as session-brief does.
--
-- Retention-ready: every row carries org_id and time columns; results sit under the
-- org/date-prefixed session keys. Code only; nothing here is applied by this repository.
-- Down path: supabase/rollbacks/20261105046000_analysis_jobs.down.sql.

CREATE TABLE sessions.session_analysis_jobs_v2 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512 AND btrim(org_id) <> ''),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id) <> ''),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512 AND btrim(session_id) <> ''),
  -- The harness the evidence was read from; NULL until the worker resolved it.
  harness text CHECK (harness ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
  status text NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_token uuid,
  lease_expires_at timestamptz,
  analysis_version text NOT NULL CHECK (analysis_version ~ '^[a-z0-9][a-z0-9._:-]{0,127}$'),
  schema_version integer NOT NULL CHECK (schema_version >= 0),
  model_version text NOT NULL CHECK (model_version ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$'),
  -- relayhistory's public v1 brief versions; '' for native jobs (the brief is session_briefs).
  summary_version text NOT NULL DEFAULT '' CHECK (char_length(summary_version) <= 128),
  summary_model_version text NOT NULL DEFAULT '' CHECK (char_length(summary_model_version) <= 128),
  source_fingerprint text CHECK (char_length(source_fingerprint) BETWEEN 1 AND 2048),
  -- canonicalSourceGeneration of the pointer set the evidence was read from.
  source_generation text CHECK (source_generation ~ '^[0-9a-f]{64}$'),
  -- session_analysis_source_revision at claim; equal at reserve and completion.
  source_revision text CHECK (source_revision ~ '^[0-9a-f]{64}$'),
  result_s3_key text CHECK (char_length(result_s3_key) BETWEEN 1 AND 1024),
  result_sha256 text CHECK (result_sha256 ~ '^[0-9a-f]{64}$'),
  result_bytes integer CHECK (result_bytes BETWEEN 1 AND 8388608),
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  -- A controlled code (native), or relayhistory's stored error (backfill, bounded).
  last_error text CHECK (char_length(last_error) BETWEEN 1 AND 1000),
  origin text NOT NULL DEFAULT 'native' CHECK (origin IN ('native', 'neon_backfill')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CONSTRAINT session_analysis_jobs_v2_lease CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
  CONSTRAINT session_analysis_jobs_v2_running_leased CHECK (status <> 'running' OR origin <> 'native' OR lease_token IS NOT NULL),
  CONSTRAINT session_analysis_jobs_v2_result_whole CHECK (
    (result_s3_key IS NULL) = (result_sha256 IS NULL) AND (result_s3_key IS NULL) = (result_bytes IS NULL)),
  CONSTRAINT session_analysis_jobs_v2_completed_result CHECK (status <> 'completed' OR result_s3_key IS NOT NULL),
  CONSTRAINT session_analysis_jobs_v2_native_error CHECK (
    origin <> 'native' OR last_error IS NULL OR last_error ~ '^[a-z][a-z0-9_]{0,63}$')
);
CREATE INDEX session_analysis_jobs_v2_due_idx ON sessions.session_analysis_jobs_v2 (available_at)
  WHERE status IN ('queued', 'running');
CREATE INDEX session_analysis_jobs_v2_scope_idx
  ON sessions.session_analysis_jobs_v2 (org_id, workspace_id, session_id, created_at DESC, id DESC);

CREATE TABLE sessions.session_analysis_requests_v2 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512 AND btrim(org_id) <> ''),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id) <> ''),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512 AND btrim(session_id) <> ''),
  requested_by_user_id text NOT NULL CHECK (char_length(requested_by_user_id) BETWEEN 1 AND 512),
  -- Native: 'resolved' once linked (the public status is the job's). Backfill: the public
  -- v1 status Neon served.
  status text NOT NULL CHECK (status IN ('queued', 'running', 'resolved', 'completed', 'failed')),
  analysis_job_id uuid REFERENCES sessions.session_analysis_jobs_v2 (id) ON DELETE SET NULL,
  -- Backfill: Neon's public v1 job id (md5('v1-job:' || request id) for brief-era requests).
  v1_job_id uuid,
  last_error text CHECK (char_length(last_error) BETWEEN 1 AND 1000),
  origin text NOT NULL DEFAULT 'native' CHECK (origin IN ('native', 'neon_backfill')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT session_analysis_requests_v2_native CHECK (
    origin <> 'native' OR (status = 'resolved' AND analysis_job_id IS NOT NULL AND v1_job_id IS NULL))
);
CREATE INDEX session_analysis_requests_v2_scope_idx
  ON sessions.session_analysis_requests_v2 (org_id, workspace_id, session_id, created_at DESC, id DESC);
CREATE INDEX session_analysis_requests_v2_day_idx
  ON sessions.session_analysis_requests_v2 (org_id, created_at);
CREATE INDEX session_analysis_requests_v2_job_idx
  ON sessions.session_analysis_requests_v2 (analysis_job_id);

ALTER TABLE sessions.session_analysis_jobs_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_analysis_requests_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_jobs_v2, sessions.session_analysis_requests_v2
  FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_analysis_jobs_v2, sessions.session_analysis_requests_v2 TO service_role;

-- Erase-safe publication tickets. Not deleted by erasure: they are how the sweeper finds
-- an object written for (or orphaned by) a forgotten session.
CREATE TABLE sessions.session_analysis_result_tickets (
  result_key text PRIMARY KEY CHECK (char_length(result_key) BETWEEN 1 AND 1024),
  job_id uuid NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  session_id text NOT NULL,
  reason text NOT NULL CHECK (reason IN ('publishing', 'forgotten')),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX session_analysis_result_tickets_created_idx ON sessions.session_analysis_result_tickets (created_at);
ALTER TABLE sessions.session_analysis_result_tickets ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_result_tickets FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_analysis_result_tickets TO service_role;

-- The live pointer set of a session as the read API matches it (canonical id, else raw
-- id), signed over every field canonicalSourceGeneration (src/session-read-api/canonical.ts)
-- binds: the tenant and canonical session id, and per pointer the harness, canonical and
-- physical machine id, user id, storage provider, bucket, manifest key, revision, raw
-- content digest and raw format. Any change to any of them moves it, so a reuse or a
-- publication can never outlive a pointer correction. NULL when no pointer is live.
CREATE FUNCTION sessions.session_analysis_source_revision(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS text LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $revision$
  SELECT CASE WHEN count(*) = 0 THEN NULL ELSE encode(sha256(convert_to(
           concat_ws('|', p_org_id, p_workspace_id, p_session_id) || E'\n' || string_agg(
           concat_ws('|', p.harness, coalesce(p.canonical_session_id, p.session_id),
             coalesce(p.canonical_machine_id, p.machine_id), p.machine_id, p.user_id,
             p.storage_provider, p.bucket, p.manifest_key, p.revision::text,
             encode(p.content_sha256, 'hex'), p.raw_format),
           E'\n' ORDER BY p.manifest_key, p.harness, p.machine_id), 'UTF8')), 'hex') END
    FROM sessions.session_index AS p
   WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
     AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
     AND p.deleted_at IS NULL;
$revision$;

-- Proof that a session never had Neon evidence, from sessions-pipeline's own records, so
-- its analysis is complete from S3 without the legacy export's coverage marker. True only
-- when ALL hold (any doubt is false, and false keeps the marker requirement):
--   1. the frozen Neon copy is sealed (20261105010000 ingest_frozen_source_completeness);
--   2. the sealed copy holds no delivery record for the session id in the org (any
--      workspace, physical or canonical id): it was never a Neon delivery session;
--   3. it has a live pointer, and every pointer (live or tombstoned) was created after the
--      seal: it was born after the freeze, so it is not a pre-freeze copy or continuation.
-- The worker additionally requires that no manifest carries a Neon export/backfill marker
-- (src/session-analysis/worker.ts), which catches a turns-only Neon session copied later.
-- Index for (2) on large frozen copies: supabase/pending/20261105046000_frozen_records_session_index.sql.
CREATE FUNCTION sessions.session_analysis_neon_free(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS boolean LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $neon_free$
  WITH seal AS (SELECT sealed_at FROM sessions.ingest_frozen_source_completeness WHERE singleton),
  ptr AS (
    SELECT p.session_id, p.created_at, p.deleted_at FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
  )
  SELECT EXISTS (SELECT 1 FROM seal)
     AND EXISTS (SELECT 1 FROM ptr WHERE ptr.deleted_at IS NULL)
     AND NOT EXISTS (SELECT 1 FROM ptr, seal WHERE ptr.created_at <= seal.sealed_at)
     AND NOT EXISTS (
       SELECT 1 FROM sessions.ingest_frozen_source_records AS r
        WHERE r.org_id = p_org_id
          AND (r.session_id = p_session_id OR r.session_id IN (SELECT ptr.session_id FROM ptr)));
$neon_free$;

-- The same proof for the read API's turns/metadata routes (src/shared/never-in-neon.ts), as
-- bulls96_read_api: the org is the transaction's bound tenant (session_directory_read_api_scope),
-- never a caller argument. SECURITY DEFINER only to read the frozen copy and seal, which the read
-- role cannot; it returns one boolean.
CREATE FUNCTION sessions.session_neon_free_for(p_workspace_id text, p_session_id text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $neon_free_for$
DECLARE
  v_org text := sessions.session_directory_read_api_scope(p_workspace_id);
BEGIN
  IF p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 THEN RETURN false; END IF;
  RETURN sessions.session_analysis_neon_free(v_org, p_workspace_id, p_session_id);
END
$neon_free_for$;

-- The admission principal's tenant pin (session_directory_read_api_scope's pattern):
-- refuses 42501 unless the caller is bulls96_analysis_admit and the transaction's bound
-- tenant (the verified assertion) is exactly (p_org_id, p_workspace_id).
CREATE FUNCTION sessions.session_analysis_admit_scope(p_org_id text, p_workspace_id text)
RETURNS void LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_org text := nullif(current_setting('bulls96_read.org_id', true), '');
  v_workspace text := nullif(current_setting('bulls96_read.workspace_id', true), '');
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'bulls96_analysis_admit' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session analysis: caller is not the admission role';
  END IF;
  IF v_org IS NULL OR v_workspace IS NULL
     OR p_org_id IS DISTINCT FROM v_org OR p_workspace_id IS DISTINCT FROM v_workspace THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session analysis: tenant binding missing or not this tenant';
  END IF;
END
$scope$;

-- Erasure: a tombstoned or deleted pointer or directory row forgets the session's analysis
-- rows, as session_briefs_forget does, and tickets every result object it orphans so the
-- sweeper deletes it even if the session's object erasure already ran.
CREATE FUNCTION sessions.session_analysis_v2_forget()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
DECLARE
  -- A request names the session as the read API does (canonical id, else raw id).
  v_ids text[] := array_remove(ARRAY[OLD.session_id, to_jsonb(OLD) ->> 'canonical_session_id'], NULL);
BEGIN
  INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason)
  SELECT j.result_s3_key, j.id, j.org_id, j.workspace_id, j.session_id, 'forgotten'
    FROM sessions.session_analysis_jobs_v2 AS j
   WHERE j.org_id = OLD.org_id AND j.workspace_id = OLD.workspace_id AND j.session_id = ANY (v_ids)
     AND j.result_s3_key IS NOT NULL
  ON CONFLICT (result_key) DO UPDATE SET reason = 'forgotten';
  DELETE FROM sessions.session_analysis_requests_v2 AS r
   WHERE r.org_id = OLD.org_id AND r.workspace_id = OLD.workspace_id AND r.session_id = ANY (v_ids);
  DELETE FROM sessions.session_analysis_jobs_v2 AS j
   WHERE j.org_id = OLD.org_id AND j.workspace_id = OLD.workspace_id AND j.session_id = ANY (v_ids);
  RETURN NULL;
END
$forget$;

CREATE TRIGGER session_analysis_v2_forget_tombstoned_pointer
  AFTER UPDATE OF deleted_at ON sessions.session_index
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_analysis_v2_forget();
CREATE TRIGGER session_analysis_v2_forget_deleted_pointer
  AFTER DELETE ON sessions.session_index
  FOR EACH ROW EXECUTE FUNCTION sessions.session_analysis_v2_forget();
CREATE TRIGGER session_analysis_v2_forget_tombstoned_directory
  AFTER UPDATE OF deleted_at ON sessions.session_directory
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_analysis_v2_forget();
CREATE TRIGGER session_analysis_v2_forget_deleted_directory
  AFTER DELETE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_analysis_v2_forget();

-- relayhistory request_session_analysis_v1 (0062:413) over the v2 tables. Same outcome
-- enum and request object: accepted {request} | not_found | rate_limited | invalid_request.
-- The engine identity below is src/session-analysis/versions.ts (a test pins them equal).
CREATE FUNCTION sessions.analysis_request_v1(
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
  IF NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                  WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
                    AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
                    AND p.deleted_at IS NULL) THEN
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

-- One request as relayhistory's v1 request object (sessionAnalysisRequest), with the
-- public status/error/job id of the 0031 projection. Owner-only; called by the definer
-- functions as their owner.
CREATE FUNCTION sessions.analysis_request_json_v1(r sessions.session_analysis_requests_v2)
RETURNS jsonb LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $json$
  SELECT CASE WHEN r.origin = 'neon_backfill' THEN
      jsonb_build_object('id', r.id, 'sessionId', r.session_id, 'status', r.status,
        'analysisId', r.v1_job_id, 'error', r.last_error,
        'createdAt', r.created_at, 'updatedAt', r.updated_at)
    ELSE
      jsonb_build_object('id', r.id, 'sessionId', r.session_id,
        'status', COALESCE(j.status, 'failed'),
        'analysisId', j.id,
        'error', CASE WHEN j.id IS NULL THEN 'analysis_unavailable'
                      WHEN j.status = 'failed' THEN j.last_error END,
        'createdAt', r.created_at, 'updatedAt', GREATEST(r.updated_at, j.updated_at))
    END
    FROM (SELECT 1) AS one
    LEFT JOIN sessions.session_analysis_jobs_v2 AS j
      ON j.id = r.analysis_job_id AND j.org_id = r.org_id
     AND j.workspace_id = r.workspace_id AND j.session_id = r.session_id;
$json$;

-- relayhistory read_session_analysis_v1 (0024:537 over the 0031 projection) for the read
-- Worker. `analyses[].result` is replaced by `resultRef` {key, sha256, bytes} plus `origin`
-- and `harness`; the Worker reads and verifies the object and assembles the v1 `result`
-- (with the brief from session_briefs for native rows). 50-request window, as relayhistory.
CREATE FUNCTION sessions.analysis_read_v1(p_workspace_id text, p_session_id text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $read$
DECLARE
  v_org text := sessions.session_directory_read_api_scope(p_workspace_id);
  v_requests jsonb;
  v_analyses jsonb;
BEGIN
  IF p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                  WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
                    AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
                    AND p.deleted_at IS NULL)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_requests_v2
                      WHERE org_id = v_org AND workspace_id = p_workspace_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  WITH recent AS (
    SELECT r FROM sessions.session_analysis_requests_v2 AS r
     WHERE r.org_id = v_org AND r.workspace_id = p_workspace_id AND r.session_id = p_session_id
     ORDER BY r.created_at DESC, r.id DESC LIMIT 50
  )
  SELECT COALESCE(jsonb_agg(sessions.analysis_request_json_v1((recent.r)::sessions.session_analysis_requests_v2)
           ORDER BY (recent.r).created_at DESC, (recent.r).id DESC), '[]'::jsonb)
    INTO v_requests FROM recent;

  WITH recent AS (
    SELECT * FROM sessions.session_analysis_requests_v2 AS r
     WHERE r.org_id = v_org AND r.workspace_id = p_workspace_id AND r.session_id = p_session_id
     ORDER BY r.created_at DESC, r.id DESC LIMIT 50
  ), linked AS (
    SELECT DISTINCT ON (j.id) j.*, COALESCE(recent.v1_job_id, j.id) AS public_id
      FROM recent JOIN sessions.session_analysis_jobs_v2 AS j
        ON j.id = recent.analysis_job_id AND j.org_id = recent.org_id
       AND j.workspace_id = recent.workspace_id AND j.session_id = recent.session_id
     ORDER BY j.id, recent.created_at DESC, recent.id DESC
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', l.public_id, 'sessionId', l.session_id, 'sourceProvider', COALESCE(l.harness, ''),
      'sourceFingerprint', COALESCE(l.source_fingerprint, ''), 'status', l.status,
      'attempts', l.attempts, 'analysisVersion', l.analysis_version,
      'schemaVersion', l.schema_version, 'summaryVersion', l.summary_version,
      'modelVersion', l.model_version, 'summaryModelVersion', l.summary_model_version,
      'resultRef', CASE WHEN l.result_s3_key IS NULL THEN NULL ELSE jsonb_build_object(
        'key', l.result_s3_key, 'sha256', l.result_sha256, 'bytes', l.result_bytes) END,
      'origin', l.origin, 'harness', l.harness,
      'error', CASE WHEN l.status = 'failed' THEN l.last_error END,
      'createdAt', l.created_at, 'updatedAt', l.updated_at, 'completedAt', l.completed_at
    ) ORDER BY l.created_at DESC, l.public_id DESC), '[]'::jsonb)
    INTO v_analyses FROM linked AS l;

  RETURN jsonb_build_object('outcome', 'ok', 'sessionId', p_session_id,
    'requests', v_requests, 'analyses', v_analyses);
END
$read$;

REVOKE ALL ON FUNCTION sessions.session_analysis_v2_forget() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_admit_scope(text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_source_revision(text, text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_neon_free(text, text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_analysis_neon_free(text, text, text) TO service_role;
REVOKE ALL ON FUNCTION sessions.session_neon_free_for(text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_neon_free_for(text, text) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.session_analysis_source_revision(text, text, text) TO service_role;
REVOKE ALL ON FUNCTION sessions.analysis_request_v1(text, text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.analysis_read_v1(text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.analysis_read_v1(text, text) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.analysis_request_v1(text, text, text, text) TO bulls96_analysis_admit;

-- As 034000: strip EXECUTE from every grantee present at creation (default privileges),
-- keeping only bulls96_read_api on analysis_read_v1.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_analysis_v2_forget()'::regprocedure,
    'sessions.session_analysis_admit_scope(text, text)'::regprocedure,
    'sessions.session_analysis_source_revision(text, text, text)'::regprocedure,
    'sessions.session_analysis_neon_free(text, text, text)'::regprocedure,
    'sessions.session_neon_free_for(text, text)'::regprocedure,
    'sessions.analysis_request_v1(text, text, text, text)'::regprocedure,
    'sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2)'::regprocedure,
    'sessions.analysis_read_v1(text, text)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api' AND v_fn = 'sessions.analysis_read_v1(text, text)'::regprocedure)
         AND NOT (r.rolname = 'bulls96_analysis_admit' AND v_fn = 'sessions.analysis_request_v1(text, text, text, text)'::regprocedure)
         AND NOT (r.rolname = 'service_role' AND v_fn = 'sessions.session_analysis_source_revision(text, text, text)'::regprocedure)
         AND NOT (r.rolname = 'service_role' AND v_fn = 'sessions.session_analysis_neon_free(text, text, text)'::regprocedure)
         AND NOT (r.rolname = 'bulls96_read_api' AND v_fn = 'sessions.session_neon_free_for(text, text)'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
