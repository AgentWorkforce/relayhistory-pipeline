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

-- Returns applied, replayed, stale or deleted. Raises CN001 (the index hasn't
-- reached this revision: retry), CN002 (a mismatch: never retry) or CN003 (RLS
-- would hide the row, which must not read as CN001). The rule order is the table
-- in the design doc.
CREATE FUNCTION sessions.record_normalized_event_count(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text, p_machine_id text,
  p_revision bigint, p_normalized_version integer, p_work_key text,
  p_content_sha256 bytea, p_object_sha256 bytea, p_event_count integer)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
DECLARE
  v sessions.session_index%ROWTYPE;
BEGIN
  IF row_security_active('sessions.session_index') THEN
    RAISE EXCEPTION 'row security filters the session index' USING ERRCODE = 'CN003';
  END IF;
  IF p_revision IS NULL OR p_revision < 1
     OR p_normalized_version IS NULL OR p_normalized_version < 1
     OR p_event_count IS NULL OR p_event_count < 0
     OR p_work_key IS NULL OR p_work_key !~ '@'
     OR octet_length(p_content_sha256) IS DISTINCT FROM 32
     OR octet_length(p_object_sha256) IS DISTINCT FROM 32 THEN
    RAISE EXCEPTION 'invalid normalized count' USING ERRCODE = '22023';
  END IF;

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

-- NOLOGIN, no BYPASSRLS, no table privileges. A login is a separate approved step.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'session_count_writer') THEN
    CREATE ROLE session_count_writer NOLOGIN NOINHERIT NOBYPASSRLS;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA sessions TO session_count_writer;

REVOKE ALL ON FUNCTION sessions.session_index_event_count_sticky() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION sessions.record_normalized_event_count(text, text, text, text, text, bigint,
  integer, text, bytea, bytea, integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION sessions.record_normalized_event_count(text, text, text, text, text, bigint,
  integer, text, bytea, bytea, integer) TO session_count_writer;

-- Only record_normalized_event_count may write provenance. service_role holds GRANT
-- ALL, and a column REVOKE doesn't subtract from a table grant, so the table-level
-- INSERT and UPDATE go and every other column is granted back. A column added later
-- is unwritable by service_role until a migration grants it. The owner, a superuser
-- and members of pg_write_all_data aren't limited by this; the sticky trigger is
-- what still protects the count from them.
REVOKE INSERT, UPDATE ON sessions.session_index FROM service_role;
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
                         'event_count_recorded_at');
  EXECUTE format('GRANT INSERT (%1$s), UPDATE (%1$s) ON sessions.session_index TO service_role', v_columns);
END
$$;

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
