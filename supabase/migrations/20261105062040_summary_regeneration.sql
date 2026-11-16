-- Slot 20261105062040. Summary regeneration, push A of 4 (docs: the summaries regenerate scope): the
-- regeneration audit table and the session_briefs retry/coverage columns. Enables nothing by itself:
-- every reader and writer is behind default-off flags (SUMMARY_REGEN_TURNS_ENABLED,
-- SUMMARY_RETRY_BACKOFF_ENABLED, SUMMARY_RETRY_CONFIG_RESET_ENABLED, SUMMARY_REGEN_AUDIT_ENABLED), and the
-- workers feature-detect these objects, so code may ship before this applies. Code only: applying this to
-- any real database needs Khaliq's separate, explicit OK.
--
-- sessions.summary_regeneration_audit: one row per regeneration enqueue (turn/idle trigger, retry backoff,
-- config change; push B adds operator requeue). Ids, closed reason codes and versions only: never summary
-- text, a prompt, a model key or a config value. request_id/scope_hash are the push-B operator idempotency
-- key and are NULL for sweep rows. The self-host Bob Costas lane (96bulls 0006) uses this same table and
-- does not require it: the lanes may apply in either order, so the writers feature-detect it at runtime.
--
-- Writers insert through sessions.summary_regeneration_audit_append_v1 (SECURITY DEFINER, EXECUTE to
-- service_role); the runtime has no table write grant. Append-only: UPDATE and TRUNCATE are refused; DELETE is refused unless it comes from one of the two
-- SECURITY DEFINER functions below (the table owner, with their transaction-local marker set): the forget
-- trigger (pointer or directory row tombstoned or deleted, which includes operator erase) and the retention
-- purge. The operator erase metadata guard refuses (skips) a row for a fenced session.
--
-- session_briefs: covered_turns, retry_attempts, next_retry_at, config_hash, pipeline_version. Nullable or
-- constant-defaulted, no CHECK and no backfill, so no table rewrite or validation scan. The partial
-- next_retry_at index starts empty (every existing row is NULL), but it is built non-concurrently inside
-- the transactional migrator: CREATE INDEX holds a SHARE lock and blocks writes to session_briefs while it
-- scans the table, briefly on a large one.
--
-- Rollout order: re-apply supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql BEFORE this
-- step (the same pattern as 20261105062000). Its embedded manifest and function lists now name the four
-- functions below, so the installed event-trigger function's prosrc (and its sha) changes; with the old
-- bootstrap installed the function boundary refuses this step. With SUMMARY_REGEN_TURNS_ENABLED first
-- turned on, legacy Bob Costas summaries (hoopdb 0006 covered_turns NULL) that are current by revision are
-- stamped with their turn count (no model call), not regenerated; a legacy brief is stamped by its next write.

CREATE TABLE sessions.summary_regeneration_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512),
  harness text NOT NULL CHECK (char_length(harness) BETWEEN 1 AND 64),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512),
  system text NOT NULL CHECK (system IN ('bob_costas','brief')),
  kind text NOT NULL CHECK (kind IN ('short','long','brief')),
  reason text NOT NULL CHECK (reason IN ('turns_gained','idle_uncovered','ended_uncovered','retry_backoff',
    'config_changed','operator_requeue','version_backfill')),
  prior_status text CHECK (prior_status ~ '^[a-z][a-z0-9_]{0,31}$'),
  prior_failure_code text CHECK (prior_failure_code ~ '^[a-z][a-z0-9_]{0,63}$'),
  prior_pipeline_version text CHECK (char_length(prior_pipeline_version) BETWEEN 1 AND 128),
  request_id text CHECK (char_length(request_id) BETWEEN 1 AND 128),
  scope_hash text CHECK (scope_hash ~ '^[0-9a-f]{64}$'),
  requested_by text CHECK (char_length(requested_by) BETWEEN 1 AND 256),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((system = 'brief') = (kind = 'brief')),
  CHECK ((request_id IS NULL) = (scope_hash IS NULL))
);
-- Operator requeue idempotency (push B).
CREATE UNIQUE INDEX summary_regeneration_audit_request ON sessions.summary_regeneration_audit (request_id, scope_hash)
  WHERE request_id IS NOT NULL;
-- The forget trigger and per-session reads; the retention purge.
CREATE INDEX summary_regeneration_audit_session ON sessions.summary_regeneration_audit (org_id, workspace_id, harness, session_id);
CREATE INDEX summary_regeneration_audit_created ON sessions.summary_regeneration_audit (created_at);

ALTER TABLE sessions.summary_regeneration_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.summary_regeneration_audit
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
-- Rows are written only through summary_regeneration_audit_append_v1 (below): no runtime table grant, so a
-- release-target run that holds this step never has to grant on a relation it has not created yet.
GRANT SELECT ON sessions.summary_regeneration_audit TO service_role;

CREATE FUNCTION sessions.summary_regeneration_audit_append_only() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $append_only$
BEGIN
 IF TG_OP='DELETE' AND current_setting('sessions.summary_regeneration_audit_delete',true)='on'
  AND current_user=(SELECT r.rolname FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE c.oid=TG_RELID) THEN
  RETURN OLD;
 END IF;
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('summary regeneration audit: %s on %s is refused',TG_OP,TG_TABLE_NAME);
END $append_only$;
CREATE TRIGGER summary_regeneration_audit_append_only BEFORE UPDATE OR DELETE ON sessions.summary_regeneration_audit
  FOR EACH ROW EXECUTE FUNCTION sessions.summary_regeneration_audit_append_only();
CREATE TRIGGER summary_regeneration_audit_no_truncate BEFORE TRUNCATE ON sessions.summary_regeneration_audit
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.summary_regeneration_audit_append_only();

-- Deletion propagates to the audit, as for the brief (20261102000000): any machine's pointer tombstoned
-- or deleted, or the logical directory row tombstoned or deleted (erasure).
CREATE FUNCTION sessions.summary_regeneration_audit_forget()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
BEGIN
  PERFORM set_config('sessions.summary_regeneration_audit_delete', 'on', true);
  DELETE FROM sessions.summary_regeneration_audit AS a
   WHERE a.org_id = OLD.org_id AND a.workspace_id = OLD.workspace_id
     AND a.harness = OLD.harness AND a.session_id = OLD.session_id;
  PERFORM set_config('sessions.summary_regeneration_audit_delete', '', true);
  RETURN NULL;
END
$forget$;

CREATE TRIGGER summary_regeneration_audit_forget_tombstoned_pointer
  AFTER UPDATE OF deleted_at ON sessions.session_index
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.summary_regeneration_audit_forget();
CREATE TRIGGER summary_regeneration_audit_forget_deleted_pointer
  AFTER DELETE ON sessions.session_index
  FOR EACH ROW EXECUTE FUNCTION sessions.summary_regeneration_audit_forget();
CREATE TRIGGER summary_regeneration_audit_forget_tombstoned_directory
  AFTER UPDATE OF deleted_at ON sessions.session_directory
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.summary_regeneration_audit_forget();
CREATE TRIGGER summary_regeneration_audit_forget_deleted_directory
  AFTER DELETE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.summary_regeneration_audit_forget();

-- Retention: deletes at most 10000 rows older than p_days (1..3650) per call; returns the count. Called
-- from the brief worker's existing prune tick when SUMMARY_REGEN_AUDIT_ENABLED is on.
CREATE FUNCTION sessions.summary_regeneration_audit_purge_v1(p_days integer) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $purge$
DECLARE v_count bigint;
BEGIN
  IF p_days IS NULL OR p_days < 1 OR p_days > 3650 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='summary regeneration audit: retention days out of range';
  END IF;
  PERFORM set_config('sessions.summary_regeneration_audit_delete', 'on', true);
  DELETE FROM sessions.summary_regeneration_audit
   WHERE id IN (SELECT id FROM sessions.summary_regeneration_audit
                 WHERE created_at < now() - make_interval(days => p_days)
                 ORDER BY created_at LIMIT 10000);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  PERFORM set_config('sessions.summary_regeneration_audit_delete', '', true);
  RETURN v_count;
END
$purge$;

-- The writer entry point (Bob Costas sweep, brief sweep): one batch per sweep, at most 1000 rows, each
-- checked by the table's CHECK constraints and the operator erase guard. Returns the rows written.
-- A forgotten session never gains a row, by the same keys the forget triggers use: a row is written only
-- while the session has a live pointer (session_index, deleted_at NULL) and no pointer or directory row
-- of that key is tombstoned. A key with no pointer at all (deleted) writes nothing. A missing directory
-- row does not block: the directory is a projection that may lag or carry a canonical id that differs
-- from the physical one; its tombstone or deletion is caught by the forget triggers. The live pointer and
-- directory rows are locked FOR SHARE (key order) first, so a concurrent tombstone or delete waits for
-- this transaction and its forget trigger then removes what this call wrote; one that committed first
-- is seen by the INSERT (READ COMMITTED takes a new snapshot per statement) and the row is skipped.
CREATE FUNCTION sessions.summary_regeneration_audit_append_v1(p_rows jsonb) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $append$
DECLARE v_count integer;
BEGIN
  IF p_rows IS NULL OR jsonb_typeof(p_rows) <> 'array' OR jsonb_array_length(p_rows) > 1000 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='summary regeneration audit: rows must be a JSON array of at most 1000';
  END IF;
  PERFORM 1
     FROM sessions.session_index AS i
     JOIN (SELECT DISTINCT k.org_id, k.workspace_id, k.harness, k.session_id
             FROM jsonb_to_recordset(p_rows) AS k(org_id text, workspace_id text, harness text, session_id text)) AS k
       ON i.org_id = k.org_id AND i.workspace_id = k.workspace_id AND i.harness = k.harness AND i.session_id = k.session_id
    WHERE i.deleted_at IS NULL
    ORDER BY i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id
      FOR SHARE OF i;
  PERFORM 1
     FROM sessions.session_directory AS d
     JOIN (SELECT DISTINCT k.org_id, k.workspace_id, k.harness, k.session_id
             FROM jsonb_to_recordset(p_rows) AS k(org_id text, workspace_id text, harness text, session_id text)) AS k
       ON d.org_id = k.org_id AND d.workspace_id = k.workspace_id AND d.harness = k.harness AND d.session_id = k.session_id
    WHERE d.deleted_at IS NULL
    ORDER BY d.org_id, d.workspace_id, d.harness, d.session_id
      FOR SHARE OF d;
  INSERT INTO sessions.summary_regeneration_audit
    (org_id, workspace_id, harness, session_id, system, kind, reason, prior_status, prior_failure_code,
     prior_pipeline_version, request_id, scope_hash, requested_by)
  SELECT r.org_id, r.workspace_id, r.harness, r.session_id, r.system, r.kind, r.reason, r.prior_status,
         r.prior_failure_code, r.prior_pipeline_version, r.request_id, r.scope_hash, r.requested_by
    FROM jsonb_to_recordset(p_rows) AS r(org_id text, workspace_id text, harness text, session_id text, system text,
         kind text, reason text, prior_status text, prior_failure_code text, prior_pipeline_version text,
         request_id text, scope_hash text, requested_by text)
   WHERE EXISTS (SELECT 1 FROM sessions.session_index AS i
                  WHERE i.org_id = r.org_id AND i.workspace_id = r.workspace_id AND i.harness = r.harness
                    AND i.session_id = r.session_id AND i.deleted_at IS NULL)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_index AS i
                      WHERE i.org_id = r.org_id AND i.workspace_id = r.workspace_id AND i.harness = r.harness
                        AND i.session_id = r.session_id AND i.deleted_at IS NOT NULL)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_directory AS d
                      WHERE d.org_id = r.org_id AND d.workspace_id = r.workspace_id AND d.harness = r.harness
                        AND d.session_id = r.session_id AND d.deleted_at IS NOT NULL);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END
$append$;

REVOKE ALL ON FUNCTION sessions.summary_regeneration_audit_append_only(),
  sessions.summary_regeneration_audit_forget(),
  sessions.summary_regeneration_audit_purge_v1(integer),
  sessions.summary_regeneration_audit_append_v1(jsonb)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.summary_regeneration_audit_purge_v1(integer),
  sessions.summary_regeneration_audit_append_v1(jsonb) TO service_role;

-- An erased (fenced) session never gains an audit row, as for the other optional per-session tables.
DO $guard$ BEGIN
 IF to_regprocedure('sessions.ingest_operator_runtime_metadata_guard()') IS NOT NULL THEN
  CREATE TRIGGER operator_erase_metadata_guard BEFORE INSERT OR UPDATE ON sessions.summary_regeneration_audit
   FOR EACH ROW EXECUTE FUNCTION sessions.ingest_operator_runtime_metadata_guard();
 END IF;
END $guard$;

ALTER TABLE sessions.session_briefs
  ADD COLUMN covered_turns integer,
  ADD COLUMN retry_attempts smallint NOT NULL DEFAULT 0,
  ADD COLUMN next_retry_at timestamptz,
  ADD COLUMN config_hash text,
  ADD COLUMN pipeline_version text;
CREATE INDEX session_briefs_next_retry ON sessions.session_briefs (next_retry_at) WHERE next_retry_at IS NOT NULL;
