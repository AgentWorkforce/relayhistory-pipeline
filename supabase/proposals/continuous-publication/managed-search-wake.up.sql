-- Optional M2 producer proposal. Install before opting a HoopDb indexer into enqueue.
BEGIN;
-- Provision the consumer role separately. This proposal grants no memberships.
DO $role$ BEGIN
 IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname='bulls96_search_wake_consumer'
   AND NOT (rolcanlogin OR rolsuper OR rolbypassrls OR rolcreatedb OR rolcreaterole OR rolreplication)) THEN
   RAISE EXCEPTION 'search wake: dedicated unprivileged NOLOGIN consumer role required';
 END IF;
END $role$;
-- Keep exactly the installed stage sets, including site-specific extensions.
LOCK TABLE sessions.pipeline_jobs,sessions.pipeline_events,sessions.pipeline_dead_letters IN ACCESS EXCLUSIVE MODE;
CREATE TABLE publication_private.search_stage_checks (
  relation regclass PRIMARY KEY, original_definition text NOT NULL, installed_definition text NOT NULL
);
REVOKE ALL ON publication_private.search_stage_checks FROM PUBLIC;
DO $checks$
DECLARE rel regclass; cname text; expr text; before_def text; after_def text;
BEGIN
 FOREACH rel IN ARRAY ARRAY['sessions.pipeline_jobs'::regclass,'sessions.pipeline_events'::regclass,'sessions.pipeline_dead_letters'::regclass] LOOP
   SELECT relname||'_stage_check' INTO STRICT cname FROM pg_class WHERE oid=rel;
   SELECT pg_get_expr(conbin,conrelid), pg_get_constraintdef(oid) INTO STRICT expr,before_def
   FROM pg_constraint WHERE conrelid=rel AND conname=cname AND contype='c' AND convalidated;
   EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, ADD CONSTRAINT %I CHECK ((%s) OR stage = %L)',rel,cname,cname,expr,'search-index');
   SELECT pg_get_constraintdef(oid) INTO STRICT after_def FROM pg_constraint WHERE conrelid=rel AND conname=cname;
   INSERT INTO publication_private.search_stage_checks VALUES(rel,before_def,after_def);
 END LOOP;
END $checks$;
CREATE UNIQUE INDEX pipeline_jobs_search_index_scope ON sessions.pipeline_jobs(work_key)
WHERE stage='search-index';

-- Ordinary queue writers can enqueue every existing stage, but cannot invent
-- a managed scope, replace its envelope or move another row into this stage.
CREATE FUNCTION sessions.guard_managed_search_wake_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF current_user=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure)) THEN
   IF TG_OP='DELETE' THEN RETURN OLD; END IF;
   RETURN NEW;
 END IF;
 IF current_user='bulls96_search_wake_consumer' THEN
   IF TG_OP='DELETE' THEN RETURN OLD; END IF;
   IF TG_OP='UPDATE' AND OLD.stage='search-index' AND NEW.stage='search-index'
    AND (NEW.work_key,NEW.trace_id,NEW.org_id,NEW.workspace_id,NEW.session_id,NEW.machine_id,NEW.message)
      IS NOT DISTINCT FROM (OLD.work_key,OLD.trace_id,OLD.org_id,OLD.workspace_id,OLD.session_id,OLD.machine_id,OLD.message)
 THEN RETURN NEW; END IF;
 END IF;
 RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='search wake: committed managed authority required';
END $$;
REVOKE ALL ON FUNCTION sessions.guard_managed_search_wake_v1() FROM PUBLIC;
CREATE TRIGGER guard_search_wake_delete BEFORE DELETE ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (OLD.stage='search-index') EXECUTE FUNCTION sessions.guard_managed_search_wake_v1();
-- Queue writers must never have authority to truncate stage history.
CREATE FUNCTION sessions.guard_managed_search_truncate_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF current_user<>pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure)) THEN
   RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='search wake: queue truncate requires owner';
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION sessions.guard_managed_search_truncate_v1() FROM PUBLIC;
CREATE TRIGGER guard_search_wake_truncate BEFORE TRUNCATE ON sessions.pipeline_jobs FOR EACH STATEMENT
 EXECUTE FUNCTION sessions.guard_managed_search_truncate_v1();
CREATE TRIGGER guard_search_wake_insert BEFORE INSERT ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (NEW.stage='search-index') EXECUTE FUNCTION sessions.guard_managed_search_wake_v1();
CREATE TRIGGER guard_search_wake_update BEFORE UPDATE ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (OLD.stage='search-index' OR NEW.stage='search-index') EXECUTE FUNCTION sessions.guard_managed_search_wake_v1();

CREATE FUNCTION sessions.enqueue_managed_search_wake_v1(evidence jsonb, trace uuid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE
 authority jsonb; h sessions.ingest_publication_heads; epoch bigint; canonical_id text;
 key text; wake jsonb; publication_time timestamptz;
BEGIN
 authority:=sessions.session_publication_lock(evidence);
 IF authority->'managed' IS DISTINCT FROM 'true'::jsonb OR authority->'current' IS DISTINCT FROM 'true'::jsonb THEN
   RAISE EXCEPTION 'search wake: publication no longer current';
 END IF;
 SELECT * INTO STRICT h FROM sessions.ingest_publication_heads
 WHERE generation=(authority->>'generation')::uuid AND current_key=evidence->>'manifestKey' AND state='live';
 IF h.source='_none' AND h.session_id='_origin' THEN RETURN; END IF;
 SELECT d.event_projection_epoch, d.session_id INTO STRICT epoch, canonical_id
 FROM sessions.session_index p JOIN sessions.session_directory d
   ON (d.org_id,d.workspace_id,d.harness,d.session_id)=(p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id))
 WHERE (p.org_id,p.workspace_id,p.harness,p.session_id,p.machine_id)=(h.org_id,h.workspace_id,h.physical_source,h.physical_session,h.machine_id)
   AND p.manifest_key=h.current_key AND p.revision=h.pointer_revision AND p.deleted_at IS NULL AND p.scrubbed IS TRUE AND NOT p.scrub_quarantined AND d.deleted_at IS NULL;
 -- Resolve the exact image, stage identity and successful attempt that made
 -- this head current. Neither the queue clock nor the pointer clock is a substitute.
 SELECT a.published_at INTO STRICT publication_time
 FROM sessions.ingest_publication_images i
 JOIN sessions.ingest_publication_attempts a ON a.attempt_token=i.attempt_token
 JOIN sessions.ingest_stage_intents s ON s.stage_token=a.stage_token
 WHERE i.manifest_key=h.current_key AND i.generation=h.generation AND i.pointer_revision=h.pointer_revision
   AND i.raw_sha256=h.current_raw_sha256 AND a.schema_generation=evidence->>'schemaGeneration'
   AND (a.org_id,a.workspace_id,a.user_id,a.origin_id)=(h.org_id,h.workspace_id,h.user_id,h.origin_id)
   AND (s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest)=(a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest)
   AND a.state='published' AND a.published_at IS NOT NULL;
 IF trace IS NULL OR epoch IS NULL OR epoch NOT BETWEEN 0 AND 9007199254740991 OR h.pointer_revision>9007199254740991 OR canonical_id<>h.session_id
    OR position(chr(31) IN concat(h.org_id,h.workspace_id,h.physical_source,canonical_id))>0
 THEN RAISE EXCEPTION 'search wake: ambiguous directory scope'; END IF;
 key:=concat_ws(chr(31),'search',h.org_id,h.workspace_id,h.physical_source,canonical_id);
 wake:=jsonb_build_object('v',1,'stage','search-index','workKey',key,'revision',h.pointer_revision,
   'ctx',jsonb_build_object('traceId',trace,'orgId',h.org_id,'workspaceId',h.workspace_id,'harness',h.physical_source,'sessionId',canonical_id),
   'payload',jsonb_build_object('epoch',epoch,'generation',h.generation,'schema_generation',evidence->>'schemaGeneration','published_at',to_char(publication_time AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
 INSERT INTO sessions.pipeline_jobs AS jobs(stage,work_key,trace_id,org_id,workspace_id,session_id,message,visible_at)
 VALUES('search-index',key,trace,h.org_id,h.workspace_id,canonical_id,wake,now())
 ON CONFLICT(work_key) WHERE stage='search-index'
 DO UPDATE SET message=CASE WHEN epoch>(jobs.message->'payload'->>'epoch')::bigint THEN excluded.message ELSE jobs.message END,
   trace_id=CASE WHEN epoch>(jobs.message->'payload'->>'epoch')::bigint THEN excluded.trace_id ELSE jobs.trace_id END,
   visible_at=CASE WHEN jobs.lease_id IS NULL AND NOT jobs.dlq THEN least(jobs.visible_at,excluded.visible_at) ELSE jobs.visible_at END;
END $$;
REVOKE ALL ON FUNCTION sessions.enqueue_managed_search_wake_v1(jsonb,uuid) FROM PUBLIC;
-- The queue/schema owner grants EXECUTE to the independently qualified indexer
-- login. Its grant is intentionally separate from this proposal.
COMMIT;
