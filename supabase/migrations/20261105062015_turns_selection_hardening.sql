-- SLOT 062015. Defense in depth before 062020 installs the relay-turn privacy tables.
-- Code only: applying this migration or changing any runtime flag requires Khaliq's
-- separate explicit approval.
--
-- Every ordinary selection takes the shared side of the global privacy lock before
-- its per-user fence lock or upload_deletions write. The reviewed 062020 rollback
-- wrapper takes the exclusive side first, so it can never hold turn_session_fences
-- while an ordinary erasure holds upload_deletions and waits for that turn fence.
--
-- The reserved turns origin is owned only by /v1/turns/batches. Rejecting it here is
-- the SQL boundary behind the matching HTTP validation and protects every current
-- or future caller of ingest_apply_selection.

SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='120s';

CREATE OR REPLACE FUNCTION sessions.ingest_apply_selection(
 p_org text,p_ws text,p_user text,p_origin text,p_sessions jsonb,p_enabled boolean
) RETURNS integer
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $apply$
DECLARE v_n integer;
BEGIN
 IF p_origin='relayhistory:conversation_turns:v1' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: turns origin is reserved';
 END IF;
 PERFORM pg_advisory_xact_lock_shared(961996,50000);
 PERFORM sessions.ingest_lock_fences(p_org,p_ws,p_user);
 IF p_enabled THEN
  DELETE FROM sessions.upload_deletions AS d
   USING sessions.ingest_fence_pairs(p_sessions) AS s
   WHERE d.org_id=p_org AND d.workspace_id=p_ws AND d.user_id=p_user
    AND d.origin_id=p_origin AND d.source=s.source AND d.session_id=s.session_id;
 ELSE
  INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
  SELECT DISTINCT p_org,p_ws,p_user,p_origin,x.source,x.session_id,1
   FROM sessions.ingest_fence_pairs(p_sessions) AS x
  ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id)
  DO UPDATE SET epoch=f.epoch+1;
  INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT DISTINCT p_org,p_ws,p_user,p_origin,s.source,s.session_id
   FROM sessions.ingest_fence_pairs(p_sessions) AS s
  ON CONFLICT DO NOTHING;
  DELETE FROM sessions.ingest_record_revisions AS t
   USING sessions.ingest_fence_pairs(p_sessions) AS s
   WHERE t.org_id=p_org AND t.workspace_id=p_ws AND t.user_id=p_user
    AND t.origin_id=p_origin AND t.source=s.source AND t.session_id=s.session_id;
  GET DIAGNOSTICS v_n=ROW_COUNT;
  DELETE FROM sessions.ingest_session_ownership AS o
   USING sessions.ingest_fence_pairs(p_sessions) AS s
   WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=p_user
    AND o.origin_id=p_origin AND o.source=s.source AND o.session_id=s.session_id;
  RETURN v_n;
 END IF;
 GET DIAGNOSTICS v_n=ROW_COUNT;
 RETURN v_n;
END
$apply$;
