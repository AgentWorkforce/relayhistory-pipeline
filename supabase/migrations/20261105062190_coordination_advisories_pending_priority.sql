-- Slot 20261105062190 (assigned). Coordination advisories, the pull path ranked by
-- priority (Session Intelligence M3-7 follow-up): 062000's coordination_advisories_pending_for_v1 is ORDER BY
-- created_at, advisory_id LIMIT 10, so a new HIGH (P1) advisory can sit behind 10 older unacked LOW rows and never
-- reach the recipient's turn-end pull. This adds a v2 reader that returns the recipient's open advisories
-- priority-first. v1 is untouched (same body, same grants): a read API without the v2 route keeps working.
-- Applying this changes no response: the read API calls v2 only with READ_API_ADVISORY_PRIORITY_ENABLED=true.
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- The priority rank is the read API's advisoryPriority (src/session-read-api/advisory-priority.ts), and a shared
-- test fixture asserts the two agree on every (urgency, kind): 1 = HIGH constraint_violation, intent_conflict,
-- duplicate_work or superseded_work; 2 = any other HIGH kind except planned_fleet; 3 = every LOW, and planned_fleet.
-- Then oldest first (created_at, advisory_id), as v1. p_limit is clamped to 1..50 (NULL is 10, v1's bound).
-- Same tenant and read-scope check and the same filters as v1: pending or delivered, younger than 7 days.
-- SECURITY DEFINER, read-only (STABLE), search_path pinned, PUBLIC EXECUTE revoked, EXECUTE for bulls96_read_api
-- alone. No table, index or table grant.

CREATE FUNCTION sessions.coordination_advisories_pending_for_v2(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text, p_limit integer
) RETURNS TABLE (
  advisory_id text, harness text, session_id text, peer_harness text, peer_session_id text, urgency text,
  kind text, artifact text, evidence jsonb, created_at timestamptz, delivered_at timestamptz, status text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_advisories_pending_priority$
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
     ORDER BY CASE
                WHEN a.urgency = 'high' AND a.kind IN ('constraint_violation','intent_conflict','duplicate_work','superseded_work') THEN 1
                WHEN a.urgency = 'high' AND a.kind <> 'planned_fleet' THEN 2
                ELSE 3
              END,
              a.created_at, a.advisory_id
     LIMIT least(greatest(coalesce(p_limit, 10), 1), 50);
END
$coordination_advisories_pending_priority$;

REVOKE ALL ON FUNCTION sessions.coordination_advisories_pending_for_v2(text,text,text,text,integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api, bulls96_session_ingest,
       bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.coordination_advisories_pending_for_v2(text,text,text,text,integer)
  TO bulls96_read_api;
