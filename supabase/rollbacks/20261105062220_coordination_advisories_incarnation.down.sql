-- Generated coordination advisories incarnation rollback (scripts/sessions-function-boundary/lanes/coordination-advisories-incarnation.ts).
-- Restores 062000's exact upsert_v1, mark_delivered_v1 and ack_v1 bodies (ACLs kept), drops upsert_v2, ack_v2,
-- pending_for_v3 and the incarnation column: every row is a legacy row again, served and acked by v1.
-- Turn READ_API_ADVISORY_INSTANCE_ENABLED and COORDINATION_ADVISOR_INCARNATION_ENABLED off first.
-- Code only: hosted execution needs Khaliq's separate explicit approval. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1);
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories incarnation: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062220_coordination_advisories_incarnation.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories incarnation: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062220_coordination_advisories_incarnation.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha IS DISTINCT FROM '3f173e8b90da9eb4f491c92bb33616c45c15e2c8fda18b60fb8c60a0494b7356'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories incarnation: own applied checksum required';END IF;
END $rollback_guard$;
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
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id
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
     AND t.status <> 'expired';
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
     AND t.status IN ('pending','delivered','acked');
  RETURN FOUND;
END
$coordination_advisories_ack$;
DROP FUNCTION sessions.coordination_advisories_upsert_v2(text,text,jsonb);
DROP FUNCTION sessions.coordination_advisories_ack_v2(text,text,text,text,text,uuid);
DROP FUNCTION sessions.coordination_advisories_pending_for_v3(text,text,text,text,integer);
ALTER TABLE sessions.coordination_advisories DROP COLUMN incarnation;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062220_coordination_advisories_incarnation.sql' AND sha256='3f173e8b90da9eb4f491c92bb33616c45c15e2c8fda18b60fb8c60a0494b7356';
COMMIT;
