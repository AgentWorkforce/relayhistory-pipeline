-- Owner-only parameterized recovery, never a service RPC. No real database action is authorized.
-- $1 org_id, $2 workspace_id, $3 writer UUID, $4 stopped-and-reconciled attestation.
-- First stop EVERY process capable of completing this writer, wait for outstanding
-- storage requests, and reconcile its writes. A timeout or failed_at is NOT evidence.
-- Release the entire batch obligation only after that proof; no age-based expiry.
-- The owner must obtain Khaliq's explicit approval before executing on a real database.
WITH confirmed AS (
 SELECT $1::text AS org_id,$2::text AS workspace_id,$3::uuid AS writer_id
 WHERE $4::boolean IS TRUE
), recovered AS (
 DELETE FROM sessions.session_erasure_writes AS w USING confirmed AS c
 WHERE w.org_id=c.org_id AND w.workspace_id=c.workspace_id AND w.writer_id=c.writer_id
 RETURNING w.org_id,w.workspace_id,w.source,w.session_id
), logged AS (
 INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,
   kind,step,state,count,error_code,actor_kind,actor_id)
 SELECT r.org_id,r.workspace_id,r.source,r.session_id,r.request_id,
   'step','fence','writer_recovered',count(*),'writer_stopped_verified','bootstrap',current_user
 FROM recovered AS w JOIN sessions.session_erasure_scopes AS s
 ON s.org_id=w.org_id AND s.workspace_id=w.workspace_id AND s.source=w.source AND s.target_session_id=w.session_id
 JOIN sessions.session_erasure_receipts AS r ON r.org_id=s.org_id AND r.workspace_id=s.workspace_id
  AND r.source=s.source AND r.session_id=s.session_id
 GROUP BY r.org_id,r.workspace_id,r.source,r.session_id,r.request_id
 RETURNING event_id
)
SELECT (SELECT count(*)::integer FROM recovered) AS recovered,
 (SELECT count(*)::integer FROM logged) AS audit_events;
