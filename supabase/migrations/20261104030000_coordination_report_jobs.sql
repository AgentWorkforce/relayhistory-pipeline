-- Service-only historical reports. Publication rechecks exact window membership and
-- pointer dependencies; unrelated current activity does not erase historical reports.
-- Publication sequence counter only; ingestion never writes this table.
CREATE TABLE sessions.coordination_report_epochs (
 org_id text NOT NULL, workspace_id text NOT NULL, generation bigint NOT NULL DEFAULT 1,
 PRIMARY KEY(org_id,workspace_id)
);
CREATE TABLE sessions.coordination_report_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), org_id text NOT NULL,workspace_id text NOT NULL,
 idempotency_key text NOT NULL CHECK(char_length(idempotency_key) BETWEEN 1 AND 256),
 request jsonb NOT NULL CHECK(jsonb_typeof(request)='object'),
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','ready','failed','invalidated')),
 lease_id uuid, lease_expires_at timestamptz, attempts integer NOT NULL DEFAULT 0,
 dependency_sessions jsonb NOT NULL DEFAULT '[]'::jsonb CHECK(jsonb_typeof(dependency_sessions)='array'),
 publication_sequence bigint, result jsonb CHECK(result IS NULL OR octet_length(result::text)<=16777216),
 error_code text CHECK(error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_]{0,63}$'),
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(org_id,workspace_id,idempotency_key),
 CHECK((status='ready')=(result IS NOT NULL))
);
ALTER TABLE sessions.coordination_report_epochs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.coordination_report_jobs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.coordination_report_epochs,sessions.coordination_report_jobs FROM PUBLIC,anon,authenticated;
GRANT ALL ON sessions.coordination_report_epochs,sessions.coordination_report_jobs TO service_role;
CREATE FUNCTION sessions.coordination_report_source_changed() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $fn$
DECLARE changed jsonb; scope_org text; scope_workspace text; source_name text; session_name text; changed_time timestamptz;
BEGIN
 -- Each source mutation locks the same workspace fence before touching reports.
 -- Running jobs retain their lease and perform an exact dependency recheck at publication.
 FOR changed IN SELECT value FROM jsonb_array_elements(
  CASE WHEN TG_OP='INSERT' THEN jsonb_build_array(to_jsonb(NEW))
       WHEN TG_OP='DELETE' THEN jsonb_build_array(to_jsonb(OLD))
       ELSE jsonb_build_array(to_jsonb(OLD),to_jsonb(NEW)) END)
 ORDER BY value->>'org_id',value->>'workspace_id'
 LOOP
  scope_org:=changed->>'org_id'; scope_workspace:=changed->>'workspace_id';
  source_name:=changed->>'harness';
  session_name:=coalesce(changed->>'canonical_session_id',changed->>'session_id',changed->>'source_session_id');
  changed_time:=CASE WHEN TG_TABLE_NAME='session_directory_event_times' THEN (changed->>'event_ts')::timestamptz END;
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('coordination-report:'||length(scope_org)::text||':'||scope_org||':'||scope_workspace,0));
  UPDATE sessions.coordination_report_jobs j SET status='invalidated',result=NULL,publication_sequence=NULL,
   lease_id=NULL,lease_expires_at=NULL,updated_at=now()
   WHERE j.org_id=scope_org AND j.workspace_id=scope_workspace AND j.status='ready' AND (
    -- Pointer edits/erasure of a cited session invalidate synchronously, even if
    -- its directory metadata has already disappeared. Same-session future appends
    -- are conservatively invalidating because old canonical content may change.
    EXISTS(SELECT 1 FROM jsonb_array_elements(j.dependency_sessions) dep
      WHERE dep->>'source'=source_name AND (
       dep->>'sessionId'=session_name OR
       (TG_TABLE_NAME='session_directory_event_pending' AND EXISTS(
        SELECT 1 FROM sessions.session_index i WHERE i.org_id=scope_org AND i.workspace_id=scope_workspace
         AND i.harness=source_name AND i.session_id=session_name
         AND coalesce(i.canonical_session_id,i.session_id)=dep->>'sessionId'))))
    OR (changed_time IS NOT NULL AND changed_time >= (j.request->'window'->>'since')::timestamptz
        AND changed_time < (j.request->'window'->>'until')::timestamptz)
    OR (TG_TABLE_NAME IN ('session_directory','session_directory_event_pending') AND EXISTS(
      SELECT 1 FROM sessions.session_directory_event_times t WHERE t.org_id=scope_org AND t.workspace_id=scope_workspace
       AND t.harness=source_name AND t.session_id=session_name
       AND t.event_ts >= (j.request->'window'->>'since')::timestamptz
       AND t.event_ts < (j.request->'window'->>'until')::timestamptz))
   );
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $fn$;
-- Trigger-only entrypoint. The migration owner supplies its narrowly pinned authority;
-- ingestion callers need no privilege on report rows or this function.
REVOKE ALL ON FUNCTION sessions.coordination_report_source_changed() FROM PUBLIC,anon,authenticated,service_role;
CREATE TRIGGER coordination_report_pointer_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.coordination_report_source_changed();
CREATE TRIGGER coordination_report_directory_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.coordination_report_source_changed();
CREATE TRIGGER coordination_report_pending_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_directory_event_pending
 FOR EACH ROW EXECUTE FUNCTION sessions.coordination_report_source_changed();
CREATE TRIGGER coordination_report_times_changed AFTER INSERT OR UPDATE OR DELETE ON sessions.session_directory_event_times
 FOR EACH ROW EXECUTE FUNCTION sessions.coordination_report_source_changed();
