-- Slot 20261105062220 (assigned). Coordination advisories, the advisory INSTANCE (incarnation) and the atomic ack.
-- An advisory id (packages/coordination-detect findingId: stableHex of workspace, kind, sorted lanes, artifact) has no
-- epoch: the sweep deletes a row 7 days after created_at and the next scan re-inserts the SAME id, and a LOW -> HIGH
-- promotion keeps it. A recipient keyed by the id alone mistakes the re-created (or promoted) advisory for the one it
-- already saw or acked. This adds the instance: sessions.coordination_advisories.incarnation uuid NULL, written only by
-- the new upsert_v2 and read only by pending_for_v3 and ack_v2.
-- Applying this changes no response and no write: the advisor calls upsert_v2 only with
-- COORDINATION_ADVISOR_INCARNATION_ENABLED=true and the read API calls pending_for_v3 / ack_v2 only with
-- READ_API_ADVISORY_INSTANCE_ENABLED=true; both default off, and upsert_v1 keeps inserting NULL.
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- The column: nullable, NO default and NO backfill. NULL is a legacy row (written by upsert_v1, before or after this
-- migration); a UUID is an instance-protected row. The writer decides, never a default. Adding a nullable column
-- without a default rewrites nothing, but it takes the table's ACCESS EXCLUSIVE lock briefly: lock_timeout (5s) bounds
-- the wait, and a timeout fails the migration (55P03) without a partial change.
--
-- upsert_v2 (same grants as upsert_v1: service_role, and the runtime role): a new row gets gen_random_uuid(). On a
-- LOW -> HIGH promotion (v1's DO UPDATE condition: same tenant, status pending or delivered, LOW -> HIGH) it ROTATES
-- the instance in the same statement: a new incarnation, status 'pending', delivered_at NULL. An acked or expired row
-- is never resurrected (the status guard is kept). Otherwise the incarnation is never touched.
--
-- The v1 mutators are FENCED to legacy rows (incarnation IS NULL), replaced here with the one added predicate and the
-- same signatures and grants: upsert_v1's match and promotion (a protected row is skipped and counted as skipped),
-- ack_v1 (false on a protected row) and mark_delivered_v1 (false on a protected row: never marked). So a delayed
-- old-client ack, an old v1 writer during a rolling deploy, or a stale DM receipt can never touch an instance.
-- mark_delivered / DM semantics: there is no mark_delivered_v2 yet. Instance mode has NO DM: the advisor refuses to
-- start with COORDINATION_ADVISOR_INCARNATION_ENABLED and COORDINATION_ADVISOR_RELAY_DM_ENABLED both on, and a
-- generation-aware mark_delivered is a later slice. delivered_at on a protected row is therefore only ever cleared
-- (by a promotion) or set by ack_v2.
--
-- ack_v2 (same grants as ack_v1: bulls96_read_api, pinned to its read scope) returns 'acked', 'already_acked' or
-- 'no_match'. The state change is ONE UPDATE matching tenant + id + recipient (harness, session_id) + incarnation +
-- status pending or delivered. When it matches nothing, a second statement (a fresh READ COMMITTED snapshot, after
-- the UPDATE waited out any concurrent writer of the row) asks whether that exact instance is already acked; acked is
-- terminal, so the answer cannot go stale. A stale incarnation (the row was promoted, or swept and re-created) matches
-- nothing. Expired rows never match.
--
-- pending_for_v3 (bulls96_read_api only): 062190's pending_for_v2 (priority-first, same scope check, filters and
-- clamp) plus the incarnation column. 062190 and v2 are untouched.

SET LOCAL lock_timeout='5s';

ALTER TABLE sessions.coordination_advisories ADD COLUMN incarnation uuid NULL;

CREATE OR REPLACE FUNCTION sessions.coordination_advisories_upsert_v1(p_org_id text, p_workspace_id text, p_advisories jsonb)
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
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.incarnation IS NULL
       AND t.status IN ('pending','delivered') AND t.urgency = 'low' AND excluded.urgency = 'high'
    RETURNING (t.xmax::text = '0') AS fresh
  )
  SELECT count(*) FILTER (WHERE fresh)::integer, count(*) FILTER (WHERE NOT fresh)::integer INTO v_ins, v_upg FROM ins;
  RETURN QUERY SELECT v_ins, v_upg, v_total - v_ins - v_upg;
END
$coordination_advisories_upsert$;

CREATE OR REPLACE FUNCTION sessions.coordination_advisories_mark_delivered_v1(
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
     AND t.status <> 'expired' AND t.incarnation IS NULL;
  RETURN FOUND;
END
$coordination_advisories_mark_delivered$;

CREATE OR REPLACE FUNCTION sessions.coordination_advisories_ack_v1(
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
     AND t.status IN ('pending','delivered','acked') AND t.incarnation IS NULL;
  RETURN FOUND;
END
$coordination_advisories_ack$;

-- Worker: upsert_v1's batch contract (same bounds, same tenant rule, same counts) with the instance. A new row gets a
-- new incarnation; a LOW -> HIGH promotion rotates it and resets the row to pending (delivered_at NULL); acked and
-- expired rows are untouched; any other conflict changes nothing (skipped).
CREATE FUNCTION sessions.coordination_advisories_upsert_v2(p_org_id text, p_workspace_id text, p_advisories jsonb)
RETURNS TABLE (inserted integer, upgraded integer, skipped integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_upsert_instance$
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
           (advisory_id, org_id, workspace_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact, evidence,
            incarnation)
    SELECT s.advisory_id, p_org_id, p_workspace_id, s.harness, s.session_id, s.peer_harness, s.peer_session_id, s.urgency, s.kind, s.artifact, s.evidence,
           gen_random_uuid()
      FROM src AS s
    ON CONFLICT (advisory_id, harness, session_id) DO UPDATE
       SET urgency = 'high', incarnation = gen_random_uuid(), status = 'pending', delivered_at = NULL
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
       AND t.status IN ('pending','delivered') AND t.urgency = 'low' AND excluded.urgency = 'high'
    RETURNING (t.xmax::text = '0') AS fresh
  )
  SELECT count(*) FILTER (WHERE fresh)::integer, count(*) FILTER (WHERE NOT fresh)::integer INTO v_ins, v_upg FROM ins;
  RETURN QUERY SELECT v_ins, v_upg, v_total - v_ins - v_upg;
END
$coordination_advisories_upsert_instance$;

-- Read API: the recipient acks one INSTANCE of its advisory. 'acked' when this call moved it from pending or
-- delivered to acked, 'already_acked' when that exact instance was acked before, 'no_match' otherwise (unknown id,
-- another recipient or tenant, a stale or NULL incarnation, a legacy row, an expired row).
CREATE FUNCTION sessions.coordination_advisories_ack_v2(
  p_org_id text, p_workspace_id text, p_advisory_id text, p_harness text, p_session_id text, p_incarnation uuid
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_ack_instance$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'coordination advisories: org outside the read scope';
  END IF;
  UPDATE sessions.coordination_advisories AS t
     SET acked_at = now(),
         delivered_at = coalesce(t.delivered_at, now()),
         status = 'acked'
   WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
     AND t.advisory_id = p_advisory_id AND t.harness = p_harness AND t.session_id = p_session_id
     AND t.incarnation = p_incarnation AND t.status IN ('pending','delivered');
  IF FOUND THEN RETURN 'acked'; END IF;
  IF EXISTS (SELECT 1 FROM sessions.coordination_advisories AS t
              WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
                AND t.advisory_id = p_advisory_id AND t.harness = p_harness AND t.session_id = p_session_id
                AND t.incarnation = p_incarnation AND t.status = 'acked') THEN
    RETURN 'already_acked';
  END IF;
  RETURN 'no_match';
END
$coordination_advisories_ack_instance$;

-- Read API: 062190's priority-first pull (pending_for_v2) plus each row's incarnation (NULL on a legacy row).
CREATE FUNCTION sessions.coordination_advisories_pending_for_v3(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text, p_limit integer
) RETURNS TABLE (
  advisory_id text, harness text, session_id text, peer_harness text, peer_session_id text, urgency text,
  kind text, artifact text, evidence jsonb, created_at timestamptz, delivered_at timestamptz, status text,
  incarnation uuid
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_pending_instance$
BEGIN
  IF sessions.session_directory_read_api_scope(p_workspace_id) IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'coordination advisories: org outside the read scope';
  END IF;
  RETURN QUERY
    SELECT a.advisory_id, a.harness, a.session_id, a.peer_harness, a.peer_session_id, a.urgency,
           a.kind, a.artifact, a.evidence, a.created_at, a.delivered_at, a.status, a.incarnation
      FROM sessions.coordination_advisories AS a
     WHERE a.org_id = p_org_id AND a.workspace_id = p_workspace_id
       AND a.harness = p_harness AND a.session_id = p_session_id
       AND a.status IN ('pending','delivered') AND a.created_at >= now() - interval '7 days'
     ORDER BY CASE
                WHEN a.urgency = 'high' AND a.kind IN ('constraint_violation','intent_conflict','duplicate_work','superseded_work') THEN 1
                WHEN a.urgency = 'high' AND a.kind <> 'planned_fleet' THEN 2
                ELSE 3
              END,
              a.created_at, a.advisory_id
     LIMIT least(greatest(coalesce(p_limit, 10), 1), 50);
END
$coordination_advisories_pending_instance$;

REVOKE ALL ON FUNCTION sessions.coordination_advisories_upsert_v2(text,text,jsonb),
  sessions.coordination_advisories_ack_v2(text,text,text,text,text,uuid),
  sessions.coordination_advisories_pending_for_v3(text,text,text,text,integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.coordination_advisories_upsert_v2(text,text,jsonb)
  TO service_role;
GRANT EXECUTE ON FUNCTION sessions.coordination_advisories_ack_v2(text,text,text,text,text,uuid),
  sessions.coordination_advisories_pending_for_v3(text,text,text,text,integer)
  TO bulls96_read_api;
