-- Slot 20261105062000. Coordination live advisories, PR 1 of 3: the table and its SECURITY DEFINER
-- readers and writers (docs/coordination-live-advisories.md). Enables nothing: no worker, no route, no
-- flag. Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- One row per (advisory, recipient): advisory_id is the finding id (stableHex, 16 lowercase hex), and
-- (harness, session_id) is the recipient session; the peer is the other session of the pair. No transcript
-- content: artifact is a path, migration name or commit sha; evidence is at most 10
-- {sessionId, eventId, ts, kind} string objects. kind is a format and length bound, not a closed list:
-- HIGH kinds are FindingKind values, but LOW pre-filter findings use their own kind.
--
-- Status: pending -> delivered -> acked; pending/delivered -> expired (sweep only). Nothing moves a row
-- backwards: an acked row stays acked, HIGH is never downgraded to LOW.
--
-- Roles: the worker (service_role, and the runtime role through RUNTIME_APPLICATION_FUNCTIONS) gets
-- upsert, mark-delivered and sweep; the read API (bulls96_read_api) gets pending-for and ack, pinned to its
-- request scope (session_directory_read_api_scope). Neither gets any table privilege.
-- Retention is a fixed 7-day constant inside the functions, not a parameter, so no caller can widen
-- the sweep. bulls96_read_api defaults to read-only transactions: the ack caller must open READ WRITE.

CREATE TABLE sessions.coordination_advisories (
  advisory_id text NOT NULL CHECK (advisory_id ~ '^[0-9a-f]{16,64}$'),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512),
  harness text NOT NULL CHECK (char_length(harness) BETWEEN 1 AND 64),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512),
  peer_harness text NOT NULL CHECK (char_length(peer_harness) BETWEEN 1 AND 64),
  peer_session_id text NOT NULL CHECK (char_length(peer_session_id) BETWEEN 1 AND 512),
  urgency text NOT NULL CHECK (urgency IN ('high','low')),
  kind text NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]{0,63}$'),
  artifact text NOT NULL CHECK (char_length(artifact) BETWEEN 1 AND 512),
  evidence jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (CASE WHEN jsonb_typeof(evidence) = 'array' THEN
    jsonb_array_length(evidence) <= 10 AND char_length(evidence::text) <= 4096
    -- strict: no array unwrapping. Each element is an object of exactly the four keys, every value a string.
    AND NOT jsonb_path_exists(evidence, 'strict $[*] ? (@.type() != "object"
      || exists(@.keyvalue() ? (!(@.key == "sessionId" || @.key == "eventId" || @.key == "ts" || @.key == "kind") || @.value.type() != "string"))
      || !exists(@.keyvalue() ? (@.key == "sessionId")) || !exists(@.keyvalue() ? (@.key == "eventId"))
      || !exists(@.keyvalue() ? (@.key == "ts")) || !exists(@.keyvalue() ? (@.key == "kind")))')
    ELSE false END),
  created_at timestamptz NOT NULL DEFAULT now(),
  delivered_at timestamptz,
  acked_at timestamptz,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','delivered','acked','expired')),
  CHECK ((harness, session_id) <> (peer_harness, peer_session_id)),
  CHECK ((status = 'acked') = (acked_at IS NOT NULL)),
  CHECK (CASE status WHEN 'pending' THEN delivered_at IS NULL WHEN 'expired' THEN true ELSE delivered_at IS NOT NULL END),
  PRIMARY KEY (advisory_id, harness, session_id)
);
-- The pull path (pending-for) and the sweep.
CREATE INDEX coordination_advisories_pull ON sessions.coordination_advisories (org_id, workspace_id, harness, session_id, status, created_at);
CREATE INDEX coordination_advisories_sweep ON sessions.coordination_advisories (created_at);

ALTER TABLE sessions.coordination_advisories ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.coordination_advisories
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;

-- Worker: insert a batch of advisories for one tenant. p_advisories is a JSON array (at most 500) of
-- {advisory_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact, evidence}.
-- New rows start pending. An existing row of the same tenant is only ever upgraded LOW -> HIGH while it is
-- still pending or delivered; status, timestamps, artifact and evidence never change, and an acked or
-- expired row is untouched. A key held by another tenant is never touched and counts as skipped.
CREATE FUNCTION sessions.coordination_advisories_upsert_v1(p_org_id text, p_workspace_id text, p_advisories jsonb)
RETURNS TABLE (inserted integer, upgraded integer, skipped integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_upsert$
DECLARE v_total integer; v_ins integer; v_upg integer;
BEGIN
  IF p_org_id IS NULL OR char_length(p_org_id) NOT BETWEEN 1 AND 512
     OR p_workspace_id IS NULL OR char_length(p_workspace_id) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisories: invalid tenant';
  END IF;
  IF p_advisories IS NULL OR jsonb_typeof(p_advisories) <> 'array' OR jsonb_array_length(p_advisories) > 500 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisories: advisories must be an array of at most 500';
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_advisories) AS a(v)
              WHERE jsonb_typeof(a.v) <> 'object'
                 OR jsonb_typeof(a.v -> 'evidence') IS DISTINCT FROM 'array'
                 OR jsonb_array_length(a.v -> 'evidence') > 10) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination advisories: each advisory needs an evidence array of at most 10 entries';
  END IF;
  v_total := jsonb_array_length(p_advisories);
  WITH src AS (
    SELECT DISTINCT ON (r.advisory_id, r.harness, r.session_id)
           r.advisory_id, r.harness, r.session_id, r.peer_harness, r.peer_session_id, r.urgency, r.kind, r.artifact, r.evidence
      FROM jsonb_to_recordset(p_advisories) AS r(advisory_id text, harness text, session_id text, peer_harness text,
                                                  peer_session_id text, urgency text, kind text, artifact text, evidence jsonb)
     ORDER BY r.advisory_id, r.harness, r.session_id, (r.urgency = 'high') DESC
  ), ins AS (
    INSERT INTO sessions.coordination_advisories AS t
           (advisory_id, org_id, workspace_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact, evidence)
    SELECT s.advisory_id, p_org_id, p_workspace_id, s.harness, s.session_id, s.peer_harness, s.peer_session_id, s.urgency, s.kind, s.artifact, s.evidence
      FROM src AS s
    ON CONFLICT (advisory_id, harness, session_id) DO UPDATE SET urgency = 'high'
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
       AND t.status IN ('pending','delivered') AND t.urgency = 'low' AND excluded.urgency = 'high'
    RETURNING (t.xmax::text = '0') AS fresh
  )
  SELECT count(*) FILTER (WHERE fresh)::integer, count(*) FILTER (WHERE NOT fresh)::integer INTO v_ins, v_upg FROM ins;
  RETURN QUERY SELECT v_ins, v_upg, v_total - v_ins - v_upg;
END
$coordination_advisories_upsert$;

-- Worker: record a delivery (the HIGH DM was sent). Sets delivered_at once; pending -> delivered; an acked
-- row stays acked and an expired row is not matched. Returns whether a row matched.
CREATE FUNCTION sessions.coordination_advisories_mark_delivered_v1(
  p_org_id text, p_workspace_id text, p_advisory_id text, p_harness text, p_session_id text
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_mark_delivered$
BEGIN
  UPDATE sessions.coordination_advisories AS t
     SET delivered_at = coalesce(t.delivered_at, now()),
         status = CASE WHEN t.status = 'pending' THEN 'delivered' ELSE t.status END
   WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
     AND t.advisory_id = p_advisory_id AND t.harness = p_harness AND t.session_id = p_session_id
     AND t.status <> 'expired';
  RETURN FOUND;
END
$coordination_advisories_mark_delivered$;

-- Read API: the recipient session's open advisories (pending or delivered, younger than the 7-day
-- retention), oldest first, at most 10. The org must match the read API's request scope.
CREATE FUNCTION sessions.coordination_advisories_pending_for_v1(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text
) RETURNS TABLE (
  advisory_id text, harness text, session_id text, peer_harness text, peer_session_id text, urgency text,
  kind text, artifact text, evidence jsonb, created_at timestamptz, delivered_at timestamptz, status text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_pending_for$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'coordination advisories: org outside the read scope';
  END IF;
  RETURN QUERY
    SELECT a.advisory_id, a.harness, a.session_id, a.peer_harness, a.peer_session_id, a.urgency,
           a.kind, a.artifact, a.evidence, a.created_at, a.delivered_at, a.status
      FROM sessions.coordination_advisories AS a
     WHERE a.org_id = p_org_id AND a.workspace_id = p_workspace_id
       AND a.harness = p_harness AND a.session_id = p_session_id
       AND a.status IN ('pending','delivered') AND a.created_at >= now() - interval '7 days'
     ORDER BY a.created_at, a.advisory_id
     LIMIT 10;
END
$coordination_advisories_pending_for$;

-- Read API: the recipient acks its advisory. Idempotent: acking an acked row matches again and changes
-- nothing. Another recipient (or tenant) matches nothing; an expired row matches nothing.
CREATE FUNCTION sessions.coordination_advisories_ack_v1(
  p_org_id text, p_workspace_id text, p_advisory_id text, p_harness text, p_session_id text
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_ack$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'coordination advisories: org outside the read scope';
  END IF;
  UPDATE sessions.coordination_advisories AS t
     SET acked_at = coalesce(t.acked_at, now()),
         delivered_at = coalesce(t.delivered_at, now()),
         status = 'acked'
   WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
     AND t.advisory_id = p_advisory_id AND t.harness = p_harness AND t.session_id = p_session_id
     AND t.status IN ('pending','delivered','acked');
  RETURN FOUND;
END
$coordination_advisories_ack$;

-- Worker: retention. Unacked rows older than 7 days become expired, then every expired row and every row
-- older than 7 days is deleted. The 7 days is a fixed constant, not a parameter. Returns counts only.
CREATE FUNCTION sessions.coordination_advisories_sweep_v1()
RETURNS TABLE (expired integer, deleted integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_sweep$
DECLARE v_cutoff timestamptz := now() - interval '7 days'; v_exp integer; v_del integer;
BEGIN
  UPDATE sessions.coordination_advisories AS t SET status = 'expired'
   WHERE t.status IN ('pending','delivered') AND t.created_at < v_cutoff;
  GET DIAGNOSTICS v_exp = ROW_COUNT;
  DELETE FROM sessions.coordination_advisories AS t
   WHERE t.status = 'expired' OR t.created_at < v_cutoff;
  GET DIAGNOSTICS v_del = ROW_COUNT;
  RETURN QUERY SELECT v_exp, v_del;
END
$coordination_advisories_sweep$;

REVOKE ALL ON FUNCTION sessions.coordination_advisories_upsert_v1(text,text,jsonb),
  sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text),
  sessions.coordination_advisories_pending_for_v1(text,text,text,text),
  sessions.coordination_advisories_ack_v1(text,text,text,text,text),
  sessions.coordination_advisories_sweep_v1()
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.coordination_advisories_upsert_v1(text,text,jsonb),
  sessions.coordination_advisories_mark_delivered_v1(text,text,text,text,text),
  sessions.coordination_advisories_sweep_v1()
  TO service_role;
GRANT EXECUTE ON FUNCTION sessions.coordination_advisories_pending_for_v1(text,text,text,text),
  sessions.coordination_advisories_ack_v1(text,text,text,text,text)
  TO bulls96_read_api;
