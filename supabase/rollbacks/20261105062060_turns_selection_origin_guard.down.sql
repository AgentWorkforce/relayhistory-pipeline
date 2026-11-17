-- Generated turns selection origin-guard rollback. Restores 030000's exact ingest_apply_selection body.
-- Code only: hosted execution needs Khaliq's separate explicit approval.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='120s';
SELECT pg_advisory_xact_lock(961996,1);
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062060_turns_selection_origin_guard.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062060_turns_selection_origin_guard.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha IS DISTINCT FROM 'd8457cc296da41b0dc8e5c796acbc62e72f81d21be345550fe2f261db6bba48f'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: own applied checksum required';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.ingest_apply_selection(p_org text, p_ws text, p_user text, p_origin text,
                                                p_sessions jsonb, p_enabled boolean)
RETURNS integer
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $apply$
DECLARE
  v_n integer;
BEGIN
  PERFORM sessions.ingest_lock_fences(p_org,p_ws,p_user);
  IF p_enabled THEN
    DELETE FROM sessions.upload_deletions AS d
     USING sessions.ingest_fence_pairs(p_sessions) AS s
     WHERE d.org_id = p_org AND d.workspace_id = p_ws AND d.user_id = p_user
       AND d.origin_id = p_origin AND d.source = s.source AND d.session_id = s.session_id;
  ELSE
    INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
    SELECT DISTINCT p_org,p_ws,p_user,p_origin,x.source,x.session_id,1 FROM sessions.ingest_fence_pairs(p_sessions) AS x
    ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1;
    INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id)
    SELECT DISTINCT p_org, p_ws, p_user, p_origin, s.source, s.session_id
      FROM sessions.ingest_fence_pairs(p_sessions) AS s
    ON CONFLICT DO NOTHING;
    DELETE FROM sessions.ingest_record_revisions AS t
     USING sessions.ingest_fence_pairs(p_sessions) AS s
     WHERE t.org_id = p_org AND t.workspace_id = p_ws AND t.user_id = p_user
       AND t.origin_id = p_origin AND t.source = s.source AND t.session_id = s.session_id;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    DELETE FROM sessions.ingest_session_ownership AS o
     USING sessions.ingest_fence_pairs(p_sessions) AS s
     WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=p_user
       AND o.origin_id=p_origin AND o.source=s.source AND o.session_id=s.session_id;
    RETURN v_n;
  END IF;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END
$apply$;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062060_turns_selection_origin_guard.sql' AND sha256='d8457cc296da41b0dc8e5c796acbc62e72f81d21be345550fe2f261db6bba48f';
COMMIT;
