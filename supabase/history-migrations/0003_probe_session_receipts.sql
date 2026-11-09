-- Zero-Neon lane D / W4: the session counts behind relayhistory-cloud GET /v1/onboarding/status
-- (sessionCount, lastReceivedAt, sources for one (org, workspace, user)) move off Neon
-- sessions.onboarding_session_rollups / convergence_events.
--
-- Two sources, both Supabase:
--   1. sessions.ingest_record_revisions, the ingest ledger: every record the session ingest
--      accepted since the delivery cutover, with its uploader, source, session and
--      updated_at. Deletions and resets already remove this user's rows from it.
--   2. history.probe_session_imports (below): a one-time, owner-only copy of Neon's per-session
--      onboarding rollups, for sessions received before the cutover. A deletion
--      (sessions.upload_deletions) or a reset (sessions.upload_resets) recorded after the copy
--      hides the copied row, so the copy never resurrects a removed session.
--
-- Reached only through history.probe_session_receipt (SECURITY DEFINER, the ingest Worker's
-- role). It is plpgsql so this file applies on a database where the ingest track is absent
-- (tests); at call time a missing ingest table is an error, which the route answers 503.
-- The definer (the history-track owner) must be able to SELECT the three sessions tables;
-- the operator checks that at plan time (runbook), it is not granted here.
CREATE TABLE history.probe_session_imports (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  last_received_at timestamptz NOT NULL,
  imported_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, user_id, source, session_id)
);
REVOKE ALL ON history.probe_session_imports FROM PUBLIC;

-- {sessionCount, lastReceivedAt, sources}: the exact counters relayhistory's status returns.
-- A session is one distinct (source, session_id) of this user in this workspace.
CREATE FUNCTION history.probe_session_receipt(p_org text, p_ws text, p_user text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $receipt$
DECLARE
  v_reset timestamptz;
  v_out jsonb;
BEGIN
  IF p_org IS NULL OR p_org = '' OR p_ws IS NULL OR p_ws = '' OR p_user IS NULL OR p_user = '' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'onboarding receipt: tenant is required';
  END IF;
  SELECT max(r.reset_at) INTO v_reset FROM sessions.upload_resets AS r
   WHERE r.org_id = p_org AND r.workspace_id = p_ws AND r.user_id = p_user;
  WITH live AS (
    SELECT l.source, l.session_id, max(l.updated_at) AS at
      FROM sessions.ingest_record_revisions AS l
     WHERE l.org_id = p_org AND l.workspace_id = p_ws AND l.user_id = p_user AND l.session_id IS NOT NULL
     GROUP BY l.source, l.session_id
  ), imported AS (
    SELECT i.source, i.session_id, i.last_received_at AS at
      FROM history.probe_session_imports AS i
     WHERE i.org_id = p_org AND i.workspace_id = p_ws AND i.user_id = p_user
       AND (v_reset IS NULL OR i.last_received_at > v_reset)
       AND NOT EXISTS (
         SELECT 1 FROM sessions.upload_deletions AS d
          WHERE d.org_id = p_org AND d.workspace_id = p_ws AND d.user_id = p_user
            AND d.source = i.source AND d.session_id = i.session_id)
  ), sessions_seen AS (
    SELECT u.source, u.session_id, max(u.at) AS at
      FROM (SELECT * FROM live UNION ALL SELECT * FROM imported) AS u
     GROUP BY u.source, u.session_id
  )
  SELECT jsonb_build_object(
           'sessionCount', count(*)::integer,
           'lastReceivedAt', max(s.at),
           'sources', to_jsonb(coalesce(array_agg(DISTINCT s.source ORDER BY s.source), ARRAY[]::text[])))
    INTO v_out FROM sessions_seen AS s;
  RETURN v_out;
END
$receipt$;

-- Owner-only backfill from Neon sessions.onboarding_session_rollups: rows
-- [{org_id, workspace_id, user_id, source, session_id, last_received_at}]. Idempotent; a
-- re-import only ever moves last_received_at later.
CREATE FUNCTION history.probe_session_imports_import(p_rows jsonb)
RETURNS integer LANGUAGE sql VOLATILE
SET search_path = pg_catalog, history AS $import$
  WITH r AS (
    SELECT x->>'org_id' AS org_id, x->>'workspace_id' AS workspace_id, x->>'user_id' AS user_id,
           x->>'source' AS source, x->>'session_id' AS session_id,
           (x->>'last_received_at')::timestamptz AS last_received_at
    FROM jsonb_array_elements(p_rows) x
  ), up AS (
    INSERT INTO history.probe_session_imports (org_id, workspace_id, user_id, source, session_id, last_received_at)
    SELECT org_id, workspace_id, user_id, source, session_id, last_received_at FROM r
    ON CONFLICT (org_id, workspace_id, user_id, source, session_id) DO UPDATE
      SET last_received_at = EXCLUDED.last_received_at, imported_at = now()
      WHERE history.probe_session_imports.last_received_at < EXCLUDED.last_received_at
    RETURNING 1
  ) SELECT count(*)::integer FROM up;
$import$;

REVOKE ALL ON FUNCTION history.probe_session_receipt(text, text, text),
  history.probe_session_imports_import(jsonb) FROM PUBLIC;
DO $grant$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_session_ingest') THEN
    GRANT EXECUTE ON FUNCTION history.probe_session_receipt(text, text, text) TO bulls96_session_ingest;
  END IF;
END
$grant$;
