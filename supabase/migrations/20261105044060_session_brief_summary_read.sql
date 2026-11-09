-- Native model descriptions and recorded catalog title fallback. No new table grants
-- or model calls. Existing read-role tenant binding and consumer authority are kept.
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
 IF EXISTS(SELECT 1 FROM sessions.session_index a JOIN sessions.session_index p
   ON p.org_id=a.org_id AND p.workspace_id=a.workspace_id AND p.harness=a.harness AND p.session_id=a.session_id
   WHERE a.org_id=v_org AND a.workspace_id=p_workspace_id AND a.harness=p_harness
    AND coalesce(a.canonical_session_id,a.session_id)=p_session_id AND a.deleted_at IS NULL
    AND p.deleted_at IS NULL AND coalesce(p.canonical_session_id,p.session_id)<>p_session_id) THEN
  RAISE EXCEPTION USING ERRCODE='0A000', MESSAGE='brief summary: inconsistent canonical mapping';
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
 FROM PUBLIC;
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

-- Persist recorded scrubbed catalog titles separately from model/pointer titles.
ALTER TABLE sessions.session_directory
  ADD COLUMN recorded_catalog_title text
  CHECK (recorded_catalog_title IS NULL OR
    (char_length(recorded_catalog_title) BETWEEN 1 AND 90
      AND recorded_catalog_title = btrim(recorded_catalog_title)
      AND recorded_catalog_title !~ '[[:cntrl:]]')),
  ADD CONSTRAINT session_directory_recorded_title_live CHECK
    (recorded_catalog_title IS NULL OR (deleted_at IS NULL AND event_projection_ready));

COMMENT ON COLUMN sessions.session_directory.recorded_catalog_title IS
  'Bounded already-scrubbed recorded session catalog title, written only after a complete canonical projection; below native brief and pointer title in display precedence.';

CREATE FUNCTION sessions.session_directory_recorded_title_invalidate()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $recorded$
DECLARE invalidated boolean;
BEGIN
  invalidated := NEW.deleted_at IS NOT NULL OR NEW.event_projection_ready IS NOT TRUE;
  IF TG_OP = 'UPDATE' THEN
    invalidated := invalidated OR NEW.event_projection_epoch IS DISTINCT FROM OLD.event_projection_epoch;
  END IF;
  IF invalidated THEN
    NEW.recorded_catalog_title := NULL;
    -- A derived title may not retain invalidated recorded content. Never retain an
    -- old model title here as a fallback; normal projectors recompute priority.
    IF TG_OP = 'UPDATE' AND OLD.recorded_catalog_title IS NOT NULL
      AND NEW.title IS NOT DISTINCT FROM OLD.recorded_catalog_title THEN
      NEW.title := NULL;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.recorded_catalog_title IS NOT NULL THEN
      NEW.search_text := '';
    END IF;
  END IF;
  RETURN NEW;
END
$recorded$;
CREATE TRIGGER session_directory_recorded_title_invalidate
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_recorded_title_invalidate();
REVOKE ALL ON FUNCTION sessions.session_directory_recorded_title_invalidate() FROM PUBLIC;
DO $optional_roles$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['anon','authenticated','service_role','search_consumer_runtime'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=role_name) THEN
      EXECUTE format('REVOKE ALL ON FUNCTION sessions.session_directory_recorded_title_invalidate() FROM %I', role_name);
    END IF;
  END LOOP;
END
$optional_roles$;

-- Default ACLs may include other roles. The trigger function remains owner-only.
DO $recorded_acl$
DECLARE v_grantee text; v_fn regprocedure := 'sessions.session_directory_recorded_title_invalidate()'::regprocedure;
BEGIN
 FOR v_grantee IN SELECT r.rolname FROM pg_catalog.pg_proc p
  CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
  JOIN pg_catalog.pg_roles r ON r.oid=a.grantee
  WHERE p.oid=v_fn AND a.privilege_type='EXECUTE' AND a.grantee<>p.proowner
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',v_fn,v_grantee); END LOOP;
END
$recorded_acl$;
