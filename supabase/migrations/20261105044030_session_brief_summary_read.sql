-- Native model descriptions for the service read API. No table grants, no generation,
-- no backfilled text. The existing read-role tenant binding is the only authority.
CREATE FUNCTION sessions.session_brief_summary_for(
 p_workspace_id text, p_harness text, p_session_id text
) RETURNS TABLE(org_id text, workspace_id text, harness text, session_id text,
 description text, model text, generated_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $summary$
DECLARE v_org text;
BEGIN
 v_org := sessions.session_directory_read_api_scope(p_workspace_id);
 IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
    OR p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='brief summary: invalid session key';
 END IF;
 -- A brief is published per physical source. Until publication can atomically bind
 -- all aliases, never present one physical source as a complete logical summary.
 IF (SELECT count(DISTINCT p.session_id) FROM sessions.session_index p
     WHERE p.org_id=v_org AND p.workspace_id=p_workspace_id AND p.harness=p_harness
       AND coalesce(p.canonical_session_id,p.session_id)=p_session_id AND p.deleted_at IS NULL)>1 THEN
  RAISE EXCEPTION USING ERRCODE='0A000', MESSAGE='brief summary: multiple physical aliases unsupported';
 END IF;
 -- Managed history needs qualified metadata, exactly as the existing brief RPC.
 IF sessions.ingest_history_live() AND EXISTS(
  SELECT 1 FROM sessions.ingest_history_scope_current h
  WHERE h.org_id=v_org AND h.workspace_id=p_workspace_id AND h.harness=p_harness
   AND (h.session_id=p_session_id OR h.session_id IN (
    SELECT p.session_id FROM sessions.session_index p WHERE p.org_id=v_org
     AND p.workspace_id=p_workspace_id AND p.harness=p_harness
     AND coalesce(p.canonical_session_id,p.session_id)=p_session_id))) THEN
  RAISE EXCEPTION USING ERRCODE='0A000', MESSAGE='history: qualified brief metadata not ready';
 END IF;
 RETURN QUERY
 SELECT v_org, p_workspace_id, p_harness, p_session_id, b.description, b.model, b.generated_at
 FROM sessions.session_briefs b
 WHERE b.org_id=v_org AND b.workspace_id=p_workspace_id AND b.harness=p_harness
  AND b.status='ready' AND b.origin='native'
  AND b.engine_version NOT LIKE 'neon-backfill:%'
  AND b.description IS NOT NULL AND btrim(b.description)<>''
  AND b.model IS NOT NULL AND btrim(b.model)<>'' AND b.generated_at IS NOT NULL
  AND EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=v_org
    AND p.workspace_id=p_workspace_id AND p.harness=p_harness AND p.session_id=b.session_id
    AND coalesce(p.canonical_session_id,p.session_id)=p_session_id AND p.deleted_at IS NULL)
  AND b.brief_source_revisions IS NOT NULL
  AND b.brief_source_revisions IS NOT DISTINCT FROM (
   SELECT jsonb_object_agg(p.machine_id,p.revision) FROM sessions.session_index p
    WHERE p.org_id=v_org AND p.workspace_id=p_workspace_id AND p.harness=p_harness
      AND p.session_id=b.session_id AND p.deleted_at IS NULL AND p.manifest_key IS NOT NULL)
 ORDER BY b.generated_at DESC, b.session_id LIMIT 1;
END
$summary$;
REVOKE ALL ON FUNCTION sessions.session_brief_summary_for(text,text,text)
 FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_brief_summary_for(text,text,text) TO bulls96_read_api;
DO $revoke$
DECLARE v_grantee text; v_fn regprocedure := 'sessions.session_brief_summary_for(text,text,text)'::regprocedure;
BEGIN
 FOR v_grantee IN SELECT r.rolname FROM pg_proc p
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
  JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=v_fn AND a.privilege_type='EXECUTE'
   AND a.grantee<>p.proowner AND r.rolname<>'bulls96_read_api'
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',v_fn,v_grantee); END LOOP;
END
$revoke$;
