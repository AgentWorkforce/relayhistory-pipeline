-- Queue access for the file-normalize worker (docs/event-count-writeback.md, #58).
--
-- The worker's credential has no table access. It holds EXECUTE on these functions
-- only, and each is fixed to its stage: claim, renew, start, ack and retry on
-- file-normalize, enqueue on normalized-ready, and events, quarantine and resolve
-- for the job it holds. Semantics follow pollPostgresStage and
-- postgresDeadLetterStore.
--
-- Everything after the claim takes the job id and its lease. Tenant identity is read
-- from that job's row, never from the caller, so this role can only write about work
-- it has claimed. It can't name another tenant's session, and it can't read a job it
-- hasn't claimed. It can claim any tenant's file-normalize job: that is the worker's
-- function, and the queue has one worker pool.
--
-- Code only: applying it to a hosted project needs Khaliq's go.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'file_normalize_worker') THEN
    CREATE ROLE file_normalize_worker NOLOGIN NOINHERIT NOBYPASSRLS;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA sessions TO file_normalize_worker;

CREATE FUNCTION sessions.file_normalize_assert_visible()
RETURNS void LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
  IF row_security_active('sessions.pipeline_jobs')
     OR row_security_active('sessions.pipeline_events')
     OR row_security_active('sessions.pipeline_dead_letters') THEN
    RAISE EXCEPTION 'row security filters the pipeline tables' USING ERRCODE = 'FN001';
  END IF;
END
$fn$;

-- The job this lease holds, locked. Internal: granted to no one.
CREATE FUNCTION sessions.file_normalize_held_job(p_job_id bigint, p_lease_id uuid)
RETURNS sessions.pipeline_jobs LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.pipeline_jobs%ROWTYPE;
BEGIN
  PERFORM sessions.file_normalize_assert_visible();
  SELECT * INTO v FROM sessions.pipeline_jobs AS j
   WHERE j.id = p_job_id AND j.lease_id = p_lease_id AND j.stage = 'file-normalize'
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'this lease does not hold that job' USING ERRCODE = 'FN002';
  END IF;
  RETURN v;
END
$fn$;

CREATE FUNCTION sessions.file_normalize_claim(p_batch integer, p_lease_seconds integer, p_lease_id uuid)
RETURNS TABLE (id bigint) LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
#variable_conflict use_column
BEGIN
  PERFORM sessions.file_normalize_assert_visible();
  IF p_lease_id IS NULL OR p_batch NOT BETWEEN 1 AND 100 OR p_lease_seconds NOT BETWEEN 1 AND 3600 THEN
    RAISE EXCEPTION 'invalid claim' USING ERRCODE = '22023';
  END IF;
  RETURN QUERY
  WITH next AS (
    SELECT j.id FROM sessions.pipeline_jobs AS j
     WHERE j.stage = 'file-normalize' AND j.visible_at <= now()
     ORDER BY j.visible_at, j.id
     LIMIT p_batch
     FOR UPDATE SKIP LOCKED)
  UPDATE sessions.pipeline_jobs AS j
     SET visible_at = now() + make_interval(secs => p_lease_seconds), lease_id = p_lease_id
    FROM next
   WHERE j.id = next.id
  RETURNING j.id;
END
$fn$;

-- Extends the claim on jobs still waiting behind the one being handled.
CREATE FUNCTION sessions.file_normalize_renew(p_job_ids bigint[], p_lease_id uuid, p_lease_seconds integer)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
BEGIN
  PERFORM sessions.file_normalize_assert_visible();
  IF p_lease_seconds NOT BETWEEN 1 AND 3600 THEN
    RAISE EXCEPTION 'invalid lease' USING ERRCODE = '22023';
  END IF;
  UPDATE sessions.pipeline_jobs AS j
     SET visible_at = now() + make_interval(secs => p_lease_seconds)
   WHERE j.id = ANY(p_job_ids) AND j.lease_id = p_lease_id AND j.stage = 'file-normalize';
END
$fn$;

-- Counts the attempt. A main-queue job past its budget starts as a DLQ delivery, so a
-- job that kills its consumer still reaches quarantine. No row: the lease was lost.
-- The budget is STAGE_REGISTRY's maxAttempts for file-normalize, pinned by a test.
CREATE FUNCTION sessions.file_normalize_start(p_job_id bigint, p_lease_id uuid, p_lease_seconds integer)
RETURNS TABLE (id bigint, dlq boolean, attempts integer, message jsonb)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
#variable_conflict use_column
BEGIN
  PERFORM sessions.file_normalize_assert_visible();
  IF p_lease_seconds NOT BETWEEN 1 AND 3600 THEN
    RAISE EXCEPTION 'invalid lease' USING ERRCODE = '22023';
  END IF;
  RETURN QUERY
  UPDATE sessions.pipeline_jobs AS j
     SET dlq = j.dlq OR j.attempts >= 6,
         attempts = CASE WHEN NOT j.dlq AND j.attempts >= 6 THEN 1 ELSE j.attempts + 1 END,
         visible_at = now() + make_interval(secs => p_lease_seconds)
   WHERE j.id = p_job_id AND j.lease_id = p_lease_id AND j.stage = 'file-normalize'
  RETURNING j.id, j.dlq, j.attempts, j.message;
END
$fn$;

CREATE FUNCTION sessions.file_normalize_ack(p_job_id bigint, p_lease_id uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
BEGIN
  PERFORM sessions.file_normalize_assert_visible();
  DELETE FROM sessions.pipeline_jobs AS j
   WHERE j.id = p_job_id AND j.lease_id = p_lease_id AND j.stage = 'file-normalize';
  RETURN FOUND;
END
$fn$;

-- Returns retried, exhausted or lost. An exhausted job becomes a DLQ delivery right
-- away, with its own attempt count; a DLQ delivery retries until quarantine succeeds.
CREATE FUNCTION sessions.file_normalize_retry(p_job_id bigint, p_lease_id uuid, p_delay_seconds integer)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v_exhausted boolean;
BEGIN
  PERFORM sessions.file_normalize_assert_visible();
  IF p_delay_seconds NOT BETWEEN 0 AND 900 THEN
    RAISE EXCEPTION 'invalid delay' USING ERRCODE = '22023';
  END IF;
  SELECT NOT j.dlq AND j.attempts >= 6 INTO v_exhausted FROM sessions.pipeline_jobs AS j
   WHERE j.id = p_job_id AND j.lease_id = p_lease_id AND j.stage = 'file-normalize'
   FOR UPDATE;
  IF NOT FOUND THEN
    RETURN 'lost';
  END IF;
  UPDATE sessions.pipeline_jobs AS j
     SET visible_at = now() + make_interval(secs => CASE WHEN v_exhausted THEN 0 ELSE p_delay_seconds END),
         lease_id = NULL,
         dlq = j.dlq OR v_exhausted,
         attempts = CASE WHEN v_exhausted THEN 0 ELSE j.attempts END
   WHERE j.id = p_job_id;
  RETURN CASE WHEN v_exhausted THEN 'exhausted' ELSE 'retried' END;
END
$fn$;

-- Sends the normalized-ready record for the held job. The stage is fixed here and the
-- identity must be the job's own, so a record can't name any other session. A retry
-- can send it twice; record_normalized_event_count makes that a replay.
CREATE FUNCTION sessions.file_normalize_enqueue_ready(p_job_id bigint, p_lease_id uuid, p_message jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.pipeline_jobs%ROWTYPE;
BEGIN
  v := sessions.file_normalize_held_job(p_job_id, p_lease_id);
  IF jsonb_typeof(p_message) IS DISTINCT FROM 'object'
     OR jsonb_typeof(p_message -> 'payload') IS DISTINCT FROM 'object'
     OR p_message ->> 'stage' IS DISTINCT FROM 'normalized-ready'
     OR p_message ->> 'workKey' IS DISTINCT FROM v.work_key
     OR p_message -> 'ctx' IS DISTINCT FROM v.message -> 'ctx'
     OR p_message -> 'revision' IS DISTINCT FROM v.message -> 'revision'
     OR octet_length(p_message::text) > 16384 THEN
    RAISE EXCEPTION 'record does not match the held job' USING ERRCODE = 'FN003';
  END IF;
  INSERT INTO sessions.pipeline_jobs
    (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message)
  VALUES ('normalized-ready', v.work_key, v.trace_id, v.org_id, v.workspace_id,
          v.session_id, v.machine_id, p_message);
END
$fn$;

-- One stage event for the held job. Identity and stage come from the job. Of the
-- error, only its code, name and retryable flag are kept: no message text.
CREATE FUNCTION sessions.file_normalize_record_event(
  p_job_id bigint, p_lease_id uuid, p_status text, p_attempt integer, p_error jsonb,
  p_attrs jsonb, p_duration_ms integer, p_idempotency_key text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.pipeline_jobs%ROWTYPE;
BEGIN
  v := sessions.file_normalize_held_job(p_job_id, p_lease_id);
  IF (p_attrs IS NOT NULL AND (jsonb_typeof(p_attrs) <> 'object' OR octet_length(p_attrs::text) > 4096))
     OR (p_error IS NOT NULL AND jsonb_typeof(p_error) <> 'object')
     OR p_idempotency_key IS NULL OR char_length(p_idempotency_key) > 1024 THEN
    RAISE EXCEPTION 'invalid event' USING ERRCODE = '22023';
  END IF;
  INSERT INTO sessions.pipeline_events
    (trace_id, workspace_id, org_id, machine_id, session_id, harness, stage, status,
     attempt, error, attrs, duration_ms, idempotency_key)
  SELECT v.trace_id, v.workspace_id, v.org_id, v.machine_id, v.session_id,
         v.message -> 'ctx' ->> 'harness', 'file-normalize', p_status, p_attempt,
         CASE WHEN p_error IS NULL THEN NULL
              ELSE jsonb_strip_nulls(jsonb_build_object(
                     'code', coalesce(p_error ->> 'code', 'unknown'),
                     'name', p_error ->> 'name',
                     'retryable', p_error -> 'retryable')) END,
         coalesce(p_attrs, '{}'), p_duration_ms, p_idempotency_key
   WHERE NOT EXISTS (SELECT 1 FROM sessions.pipeline_events AS e
                      WHERE e.trace_id = v.trace_id AND e.idempotency_key = p_idempotency_key);
END
$fn$;

-- encodeURIComponent, so a letter's id equals deadLetterId() in dlq.ts.
CREATE FUNCTION sessions.file_normalize_uri_component(p_value text)
RETURNS text LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
  v_bytes bytea := convert_to(p_value, 'UTF8');
  v_out text := '';
  v_byte integer;
BEGIN
  FOR i IN 0 .. length(v_bytes) - 1 LOOP
    v_byte := get_byte(v_bytes, i);
    IF v_byte < 128 AND chr(v_byte) ~ '[A-Za-z0-9_.!~*''()-]' THEN
      v_out := v_out || chr(v_byte);
    ELSE
      v_out := v_out || '%' || upper(lpad(to_hex(v_byte), 2, '0'));
    END IF;
  END LOOP;
  RETURN v_out;
END
$fn$;

CREATE FUNCTION sessions.file_normalize_letter_id(v sessions.pipeline_jobs)
RETURNS text LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
  SELECT 'file-normalize:' || sessions.file_normalize_uri_component(v.org_id) || ':'
         || sessions.file_normalize_uri_component(v.workspace_id) || ':' || v.work_key
$fn$;

-- The letter is built from the job row: the caller supplies neither message nor id.
CREATE FUNCTION sessions.file_normalize_quarantine(p_job_id bigint, p_lease_id uuid)
RETURNS TABLE (letter_id text, letter_dead_count integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.pipeline_jobs%ROWTYPE;
  v_id text;
BEGIN
  v := sessions.file_normalize_held_job(p_job_id, p_lease_id);
  v_id := sessions.file_normalize_letter_id(v);
  INSERT INTO sessions.pipeline_dead_letters AS d
    (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message)
  VALUES (v_id, 'file-normalize', v.trace_id, v.org_id, v.workspace_id,
          v.session_id, v.machine_id, v.message)
  ON CONFLICT (id) DO UPDATE SET
    message = EXCLUDED.message,
    state = 'dead',
    last_dead_at = now(),
    dead_count = d.dead_count + 1,
    resolved_at = NULL,
    session_id = coalesce(EXCLUDED.session_id, d.session_id),
    machine_id = coalesce(EXCLUDED.machine_id, d.machine_id)
  RETURNING d.id, d.dead_count INTO letter_id, letter_dead_count;
  RETURN NEXT;
END
$fn$;

-- A held job that succeeded resolves its letter, if it has one.
CREATE FUNCTION sessions.file_normalize_resolve(p_job_id bigint, p_lease_id uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.pipeline_jobs%ROWTYPE;
BEGIN
  v := sessions.file_normalize_held_job(p_job_id, p_lease_id);
  UPDATE sessions.pipeline_dead_letters AS d
     SET state = 'resolved', resolved_at = now()
   WHERE d.id = sessions.file_normalize_letter_id(v) AND d.state <> 'resolved';
END
$fn$;

REVOKE ALL ON FUNCTION sessions.file_normalize_assert_visible(),
  sessions.file_normalize_held_job(bigint, uuid),
  sessions.file_normalize_claim(integer, integer, uuid),
  sessions.file_normalize_renew(bigint[], uuid, integer),
  sessions.file_normalize_start(bigint, uuid, integer),
  sessions.file_normalize_ack(bigint, uuid),
  sessions.file_normalize_retry(bigint, uuid, integer),
  sessions.file_normalize_enqueue_ready(bigint, uuid, jsonb),
  sessions.file_normalize_record_event(bigint, uuid, text, integer, jsonb, jsonb, integer, text),
  sessions.file_normalize_uri_component(text),
  sessions.file_normalize_letter_id(sessions.pipeline_jobs),
  sessions.file_normalize_quarantine(bigint, uuid),
  sessions.file_normalize_resolve(bigint, uuid)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION
  sessions.file_normalize_claim(integer, integer, uuid),
  sessions.file_normalize_renew(bigint[], uuid, integer),
  sessions.file_normalize_start(bigint, uuid, integer),
  sessions.file_normalize_ack(bigint, uuid),
  sessions.file_normalize_retry(bigint, uuid, integer),
  sessions.file_normalize_enqueue_ready(bigint, uuid, jsonb),
  sessions.file_normalize_record_event(bigint, uuid, text, integer, jsonb, jsonb, integer, text),
  sessions.file_normalize_quarantine(bigint, uuid),
  sessions.file_normalize_resolve(bigint, uuid)
  TO file_normalize_worker;

-- Enqueue is the only way a normalized-ready job is created, so it runs as the role the
-- guard admits. Its helpers run as that role too. The membership exists only for this
-- ALTER, so the migrating role can't become it afterwards.
GRANT EXECUTE ON FUNCTION sessions.file_normalize_held_job(bigint, uuid),
  sessions.file_normalize_assert_visible() TO normalized_ready_writer;
GRANT normalized_ready_writer TO CURRENT_USER;
ALTER FUNCTION sessions.file_normalize_enqueue_ready(bigint, uuid, jsonb) OWNER TO normalized_ready_writer;
REVOKE normalized_ready_writer FROM CURRENT_USER;
