-- The authoritative event_count write-back (docs/event-count-writeback.md, #58).
--
-- session_index.event_count is not a real count for a session with a backfill or
-- resumed epoch. The real one is known after normalization, by a transform with
-- no database access. It arrives through record_normalized_event_count, and its
-- provenance is kept beside it. A value is authoritative only when
-- event_count_revision = revision AND deleted_at IS NULL.
--
-- Code only: applying it to a hosted project needs Khaliq's go.

ALTER TABLE sessions.session_index
  ADD COLUMN event_count_revision bigint,
  ADD COLUMN event_count_normalized_version integer,
  ADD COLUMN event_count_work_key text,
  ADD COLUMN event_count_object_sha256 bytea
    CHECK (octet_length(event_count_object_sha256) = 32),
  ADD COLUMN event_count_recorded_at timestamptz;

COMMENT ON COLUMN sessions.session_index.event_count_revision IS
  'The manifest revision of the authoritative event_count. NULL: never counted. Authoritative only while it equals revision.';

-- The index stage sends a chunk sum for each live revision. Once a count has
-- provenance, only a write that changes the provenance may change it. A trigger,
-- so the rule holds for every writer, as session_index_scrub_sticky does.
CREATE FUNCTION sessions.session_index_event_count_sticky()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $sticky$
BEGIN
  IF OLD.event_count_revision IS NOT NULL
     AND NEW.event_count_revision IS NOT DISTINCT FROM OLD.event_count_revision
     AND NEW.event_count_normalized_version IS NOT DISTINCT FROM OLD.event_count_normalized_version
     AND NEW.event_count_object_sha256 IS NOT DISTINCT FROM OLD.event_count_object_sha256 THEN
    NEW.event_count := OLD.event_count;
  END IF;
  RETURN NEW;
END
$sticky$;

CREATE TRIGGER session_index_event_count_sticky
  BEFORE UPDATE ON sessions.session_index
  FOR EACH ROW EXECUTE FUNCTION sessions.session_index_event_count_sticky();

-- The ordering rules, in the order of the design doc's table. Returns applied,
-- replayed, stale or deleted. Raises CN001 (the index hasn't reached this revision:
-- retry) or CN002 (a mismatch: never retry). Internal: granted to no one, and only
-- reachable through record_normalized_event_count.
CREATE FUNCTION sessions.apply_normalized_event_count(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text, p_machine_id text,
  p_revision bigint, p_normalized_version integer, p_work_key text,
  p_content_sha256 bytea, p_object_sha256 bytea, p_event_count integer)
RETURNS text LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.session_index%ROWTYPE;
BEGIN
  SELECT * INTO v FROM sessions.session_index AS s
   WHERE s.org_id = p_org_id AND s.workspace_id = p_workspace_id AND s.harness = p_harness
     AND s.session_id = p_session_id AND s.machine_id = p_machine_id
   FOR UPDATE;

  IF NOT FOUND OR p_revision > v.revision THEN
    RAISE EXCEPTION 'the index has not reached this revision' USING ERRCODE = 'CN001';
  END IF;
  IF v.deleted_at IS NOT NULL THEN
    RETURN 'deleted';
  END IF;
  IF p_revision < v.revision THEN
    RETURN 'stale';
  END IF;
  IF v.manifest_key IS DISTINCT FROM regexp_replace(p_work_key, '@[^@]*$', '')
     OR v.content_sha256 IS DISTINCT FROM p_content_sha256 THEN
    RAISE EXCEPTION 'count does not match the indexed revision' USING ERRCODE = 'CN002';
  END IF;
  IF v.event_count_revision = p_revision THEN
    IF v.event_count_normalized_version > p_normalized_version THEN
      RETURN 'stale';
    END IF;
    IF v.event_count_normalized_version = p_normalized_version THEN
      IF v.event_count_object_sha256 = p_object_sha256 AND v.event_count = p_event_count THEN
        RETURN 'replayed';
      END IF;
      RAISE EXCEPTION 'one revision and version has two outputs' USING ERRCODE = 'CN002';
    END IF;
  END IF;

  UPDATE sessions.session_index AS s
     SET event_count = p_event_count,
         event_count_revision = p_revision,
         event_count_normalized_version = p_normalized_version,
         event_count_work_key = p_work_key,
         event_count_object_sha256 = p_object_sha256,
         event_count_recorded_at = now()
   WHERE s.org_id = p_org_id AND s.workspace_id = p_workspace_id AND s.harness = p_harness
     AND s.session_id = p_session_id AND s.machine_id = p_machine_id;
  RETURN 'applied';
END
$fn$;

-- The capability form, and the only entry point. The caller names a job and proves
-- it holds the lease. Tenant, session, revision, provenance and count all come from
-- that job's row, so no caller can name or override them. The job is acked in this
-- transaction with the count, whatever the outcome, so a lost response can't apply
-- twice and a failure leaves both unchanged. A job in the DLQ path is never applied.
-- Raises CN003 if RLS would hide the rows, and CN004 if this lease doesn't hold a
-- live normalized-ready job. Expired means visible_at has passed: a live lease always
-- holds it in the future.
CREATE FUNCTION sessions.record_normalized_event_count(p_job_id bigint, p_lease_id uuid)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  j sessions.pipeline_jobs%ROWTYPE;
  r jsonb;
  v_outcome text;
BEGIN
  IF row_security_active('sessions.session_index') OR row_security_active('sessions.pipeline_jobs') THEN
    RAISE EXCEPTION 'row security filters the count tables' USING ERRCODE = 'CN003';
  END IF;
  SELECT * INTO j FROM sessions.pipeline_jobs AS q
   WHERE q.id = p_job_id AND q.lease_id = p_lease_id AND q.stage = 'normalized-ready'
     AND NOT q.dlq AND q.visible_at > now()
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'this lease does not hold a live normalized-ready job' USING ERRCODE = 'CN004';
  END IF;

  r := j.message -> 'payload';
  IF jsonb_typeof(r) IS DISTINCT FROM 'object'
     OR jsonb_typeof(r -> 'manifest_revision') IS DISTINCT FROM 'number'
     OR jsonb_typeof(r -> 'normalized_version') IS DISTINCT FROM 'number'
     OR jsonb_typeof(r -> 'convergence_event_count') IS DISTINCT FROM 'number'
     OR r ->> 'manifest_revision' !~ '^[1-9][0-9]{0,15}$'
     OR r ->> 'normalized_version' !~ '^[1-9][0-9]{0,8}$'
     OR r ->> 'convergence_event_count' !~ '^(0|[1-9][0-9]{0,8})$'
     OR (r ->> 'source_content_sha256') !~ '^[0-9a-f]{64}$' IS NOT FALSE
     OR (r ->> 'output_sha256') !~ '^[0-9a-f]{64}$' IS NOT FALSE
     OR jsonb_typeof(r -> 'output_divergent') IS DISTINCT FROM 'boolean' THEN
    RAISE EXCEPTION 'invalid normalized-ready record' USING ERRCODE = '22023';
  END IF;

  -- The record must be about the job it sits in: a substituted payload stops here.
  IF r ->> 'org_id' IS DISTINCT FROM j.org_id
     OR r ->> 'workspace_id' IS DISTINCT FROM j.workspace_id
     OR r ->> 'session_id' IS DISTINCT FROM j.session_id
     OR r ->> 'machine_id' IS DISTINCT FROM j.machine_id
     OR r ->> 'harness' IS DISTINCT FROM j.message -> 'ctx' ->> 'harness'
     OR r ->> 'work_key' IS DISTINCT FROM j.work_key
     OR r ->> 'manifest_revision' IS DISTINCT FROM j.message ->> 'revision'
     OR r ->> 'manifest_key' IS DISTINCT FROM regexp_replace(j.work_key, '@[^@]*$', '')
     OR (r ->> 'output_divergent')::boolean THEN
    RAISE EXCEPTION 'record does not match its job' USING ERRCODE = 'CN002';
  END IF;

  v_outcome := sessions.apply_normalized_event_count(
    j.org_id, j.workspace_id, j.message -> 'ctx' ->> 'harness', j.session_id, j.machine_id,
    (r ->> 'manifest_revision')::bigint, (r ->> 'normalized_version')::integer, j.work_key,
    decode(r ->> 'source_content_sha256', 'hex'), decode(r ->> 'output_sha256', 'hex'),
    (r ->> 'convergence_event_count')::integer);
  DELETE FROM sessions.pipeline_jobs AS q WHERE q.id = j.id;
  RETURN v_outcome;
END
$fn$;

-- The only role that may create or change a normalized-ready row. NOLOGIN and
-- granted to no login, so a workload can't become it. It owns the guard and the narrow
-- functions, so no workload can replace them. BYPASSRLS because these tables have RLS
-- on and no policy. The guard names this role rather than the table owner: the
-- shared login's identity isn't known here, and "is the owner" would fail open.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'normalized_ready_writer') THEN
    CREATE ROLE normalized_ready_writer NOLOGIN NOINHERIT BYPASSRLS;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA sessions TO normalized_ready_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.pipeline_jobs, sessions.pipeline_dead_letters
  TO normalized_ready_writer;
GRANT SELECT, UPDATE ON sessions.session_index TO normalized_ready_writer;

-- Invoker on purpose: current_user is who is writing. Lease columns (lease_id,
-- visible_at, attempts, dlq) stay writable so the normal poller works.
-- Superusers are outside any grant, so refusing them would only break maintenance.
CREATE FUNCTION sessions.normalized_ready_guard()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $guard$
DECLARE
  v_protected boolean;
BEGIN
  IF current_user = 'normalized_ready_writer'
     OR (SELECT r.rolsuper FROM pg_roles AS r WHERE r.rolname = current_user) THEN
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
  END IF;
  IF TG_OP = 'TRUNCATE' THEN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s WHERE stage = %L)',
                   TG_RELID::regclass, 'normalized-ready') INTO v_protected;
    IF v_protected THEN
      RAISE EXCEPTION 'normalized-ready rows can''t be truncated' USING ERRCODE = 'CN005';
    END IF;
    RETURN NULL;
  END IF;
  IF TG_OP = 'INSERT' THEN
    v_protected := NEW.stage = 'normalized-ready';
  ELSIF TG_OP = 'DELETE' THEN
    v_protected := OLD.stage = 'normalized-ready';
    -- Retention's sweep. A letter only becomes resolved through a narrow function.
    IF TG_TABLE_NAME = 'pipeline_dead_letters' THEN
      v_protected := v_protected AND OLD.state <> 'resolved';
    END IF;
  ELSIF OLD.stage = 'normalized-ready' OR NEW.stage = 'normalized-ready' THEN
    v_protected := true;
    IF TG_TABLE_NAME = 'pipeline_jobs' THEN
      v_protected := (to_jsonb(NEW) - ARRAY['lease_id', 'visible_at', 'attempts', 'dlq'])
                  <> (to_jsonb(OLD) - ARRAY['lease_id', 'visible_at', 'attempts', 'dlq']);
    END IF;
  ELSE
    v_protected := false;
  END IF;
  IF v_protected THEN
    RAISE EXCEPTION 'normalized-ready rows change only through their functions' USING ERRCODE = 'CN005';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$guard$;

CREATE TRIGGER pipeline_jobs_normalized_ready_guard
  BEFORE INSERT OR UPDATE OR DELETE ON sessions.pipeline_jobs
  FOR EACH ROW EXECUTE FUNCTION sessions.normalized_ready_guard();
CREATE TRIGGER pipeline_jobs_normalized_ready_truncate
  BEFORE TRUNCATE ON sessions.pipeline_jobs
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.normalized_ready_guard();
CREATE TRIGGER pipeline_dead_letters_normalized_ready_guard
  BEFORE INSERT OR UPDATE OR DELETE ON sessions.pipeline_dead_letters
  FOR EACH ROW EXECUTE FUNCTION sessions.normalized_ready_guard();
CREATE TRIGGER pipeline_dead_letters_normalized_ready_truncate
  BEFORE TRUNCATE ON sessions.pipeline_dead_letters
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.normalized_ready_guard();

-- encodeURIComponent, so a letter's id equals deadLetterId() in dlq.ts.
CREATE FUNCTION sessions.pipeline_uri_component(p_value text)
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

-- Moves a held job to the dead letter store in one transaction, from the job's own
-- row, so quarantine never needs a direct DELETE or a caller-supplied message.
CREATE FUNCTION sessions.normalized_ready_quarantine(p_job_id bigint, p_lease_id uuid)
RETURNS SETOF sessions.pipeline_dead_letters LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  j sessions.pipeline_jobs%ROWTYPE;
BEGIN
  SELECT * INTO j FROM sessions.pipeline_jobs AS q
   WHERE q.id = p_job_id AND q.lease_id = p_lease_id AND q.stage = 'normalized-ready'
     AND q.visible_at > now()
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'this lease does not hold a live normalized-ready job' USING ERRCODE = 'CN004';
  END IF;
  DELETE FROM sessions.pipeline_jobs AS q WHERE q.id = j.id;
  RETURN QUERY
  INSERT INTO sessions.pipeline_dead_letters AS d
    (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message)
  VALUES ('normalized-ready:' || sessions.pipeline_uri_component(j.org_id) || ':'
            || sessions.pipeline_uri_component(j.workspace_id) || ':' || j.work_key,
          'normalized-ready', j.trace_id, j.org_id, j.workspace_id, j.session_id, j.machine_id,
          j.message)
  ON CONFLICT (id) DO UPDATE SET
    message = EXCLUDED.message,
    state = 'dead',
    last_dead_at = now(),
    dead_count = d.dead_count + 1,
    resolved_at = NULL
  RETURNING d.*;
END
$fn$;

-- Replay never re-sends a stored count. It queues file-normalize for the same manifest
-- version, so the count is recomputed from the object. Letters of this stage can only
-- be written by the function above, so the identity here comes from a real job.
-- Returns false if the letter isn't dead, so concurrent replays send it once.
CREATE FUNCTION sessions.normalized_ready_replay(p_letter_id text, p_replay_id uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  d sessions.pipeline_dead_letters%ROWTYPE;
  m jsonb;
BEGIN
  SELECT * INTO d FROM sessions.pipeline_dead_letters AS l
   WHERE l.id = p_letter_id AND l.stage = 'normalized-ready' AND l.state = 'dead'
   FOR UPDATE;
  IF NOT FOUND THEN
    RETURN false;
  END IF;
  m := d.message;
  INSERT INTO sessions.pipeline_jobs
    (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message)
  VALUES ('file-normalize', m ->> 'workKey', d.trace_id, d.org_id, d.workspace_id,
          d.session_id, d.machine_id,
          jsonb_build_object(
            'v', 1, 'stage', 'file-normalize', 'ctx', m -> 'ctx', 'workKey', m -> 'workKey',
            'revision', m -> 'revision',
            'replay', jsonb_build_object('id', p_replay_id, 'at', now()),
            'payload', jsonb_build_object(
              'traceId', m -> 'ctx' -> 'traceId', 'workspaceId', m -> 'ctx' -> 'workspaceId',
              'machineId', m -> 'ctx' -> 'machineId', 'harness', m -> 'ctx' -> 'harness',
              'sessionId', m -> 'ctx' -> 'sessionId',
              'manifestKey', m -> 'payload' -> 'manifest_key')));
  UPDATE sessions.pipeline_dead_letters AS l
     SET state = 'resolved', resolved_at = now(), replay_count = l.replay_count + 1,
         last_replay_id = p_replay_id, last_replay_at = now()
   WHERE l.id = p_letter_id;
  RETURN true;
END
$fn$;

-- NOLOGIN, no BYPASSRLS, no table privileges. service_role is what the runtime
-- connects as, and it is NOINHERIT, so it has no EXECUTE through this: the store takes
-- the role with SET LOCAL ROLE for the one call.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_count_writer') THEN
    CREATE ROLE session_count_writer NOLOGIN NOINHERIT NOBYPASSRLS;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA sessions TO session_count_writer;
GRANT session_count_writer TO service_role;

REVOKE ALL ON FUNCTION sessions.session_index_event_count_sticky(),
  sessions.normalized_ready_guard(), sessions.pipeline_uri_component(text),
  sessions.apply_normalized_event_count(text, text, text, text, text, bigint,
    integer, text, bytea, bytea, integer),
  sessions.record_normalized_event_count(bigint, uuid),
  sessions.normalized_ready_quarantine(bigint, uuid),
  sessions.normalized_ready_replay(text, uuid)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.record_normalized_event_count(bigint, uuid) TO session_count_writer;
-- Bound to a held job, or to a real letter, and neither can carry a count.
GRANT EXECUTE ON FUNCTION sessions.normalized_ready_quarantine(bigint, uuid),
  sessions.normalized_ready_replay(text, uuid) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.apply_normalized_event_count(text, text, text, text, text, bigint,
    integer, text, bytea, bytea, integer),
  sessions.pipeline_uri_component(text) TO normalized_ready_writer;

-- Hands the guard and the narrow functions to the isolated role. The membership
-- exists only for this ALTER, so the migrating role can't become it afterwards.
GRANT normalized_ready_writer TO CURRENT_USER;
ALTER FUNCTION sessions.normalized_ready_guard() OWNER TO normalized_ready_writer;
ALTER FUNCTION sessions.pipeline_uri_component(text) OWNER TO normalized_ready_writer;
ALTER FUNCTION sessions.apply_normalized_event_count(text, text, text, text, text, bigint,
  integer, text, bytea, bytea, integer) OWNER TO normalized_ready_writer;
ALTER FUNCTION sessions.record_normalized_event_count(bigint, uuid) OWNER TO normalized_ready_writer;
ALTER FUNCTION sessions.normalized_ready_quarantine(bigint, uuid) OWNER TO normalized_ready_writer;
ALTER FUNCTION sessions.normalized_ready_replay(text, uuid) OWNER TO normalized_ready_writer;
REVOKE normalized_ready_writer FROM CURRENT_USER;

-- Only the narrow functions may write provenance, revision or content_sha256. A
-- count is authoritative at a revision and hash, so a role that could rewrite those
-- could make a forged record match. service_role holds GRANT ALL, and a column REVOKE
-- doesn't subtract from a table grant, so its table-level privileges go and every
-- other column is granted back. Nothing deletes or truncates this table. A column
-- added later is unwritable by service_role until a migration grants it.
-- upsert_session_index becomes SECURITY DEFINER so the index stage still sets them.
-- A migration that replaces it must restate SECURITY DEFINER, or writes fail closed.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON sessions.session_index FROM service_role;
DO $$
DECLARE
  v_columns text;
BEGIN
  SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) INTO v_columns
    FROM pg_attribute
   WHERE attrelid = 'sessions.session_index'::regclass AND attnum > 0 AND NOT attisdropped
     AND attgenerated = ''
     AND attname NOT IN ('event_count_revision', 'event_count_normalized_version',
                         'event_count_work_key', 'event_count_object_sha256',
                         'event_count_recorded_at', 'revision', 'content_sha256');
  EXECUTE format('GRANT INSERT (%1$s), UPDATE (%1$s) ON sessions.session_index TO service_role', v_columns);
END
$$;
ALTER FUNCTION sessions.upsert_session_index(jsonb) SECURITY DEFINER;

-- The stage that consumes the typed normalized-ready record.
-- NOT VALID: every new row is checked, and pipeline_events isn't scanned under
-- this lock. Each list only grows, so existing rows already satisfy it.
ALTER TABLE sessions.pipeline_events DROP CONSTRAINT pipeline_events_stage_check;
ALTER TABLE sessions.pipeline_dead_letters DROP CONSTRAINT pipeline_dead_letters_stage_check;
ALTER TABLE sessions.pipeline_jobs DROP CONSTRAINT pipeline_jobs_stage_check;
ALTER TABLE sessions.pipeline_events ADD CONSTRAINT pipeline_events_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'normalized-ready', 'fetch')) NOT VALID;
ALTER TABLE sessions.pipeline_dead_letters ADD CONSTRAINT pipeline_dead_letters_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'normalized-ready', 'fetch')) NOT VALID;
ALTER TABLE sessions.pipeline_jobs ADD CONSTRAINT pipeline_jobs_stage_check
  CHECK (stage IN ('object-landed', 'session-indexed', 'summary-short', 'summary-long',
    'status-classify', 'file-normalize', 'file-maintenance', 'normalized-ready')) NOT VALID;
