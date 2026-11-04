-- Generated from620 owned definitions + authoritative560 originals. Stop all620/630 workers first.
-- Operational rollback retains installed authority/object ledgers; never forget late PUT obligations.
DO $rollback_guard$ BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_plans)OR EXISTS(SELECT 1 FROM sessions.ingest_history_reconcile_outbox)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_privacy_outbox)OR EXISTS(SELECT 1 FROM sessions.ingest_history_required_scopes)OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_versions)
  OR EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects)OR to_regprocedure('sessions.session_history_model_claim(jsonb)')IS NOT NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: retain authority/object ledgers; stop consumers for operational rollback';END IF;
END $rollback_guard$;
DROP TRIGGER IF EXISTS ingest_history_pointer_sync ON sessions.session_index;
DROP TRIGGER IF EXISTS ingest_history_native_insert ON sessions.ingest_lazy_native_slots;
DROP TRIGGER IF EXISTS ingest_history_native_update ON sessions.ingest_lazy_native_slots;
DROP TRIGGER IF EXISTS ingest_history_control_changed ON sessions.ingest_control_receipts;
DROP TRIGGER IF EXISTS ingest_history_directory_state ON sessions.session_directory;
CREATE OR REPLACE FUNCTION sessions.session_replay_source_snapshot(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $snapshot$
 SELECT jsonb_build_object('epoch',a.epoch::text,
 'directory', (SELECT jsonb_build_object('epoch',d.event_projection_epoch::text,'deleted_at',d.deleted_at) FROM sessions.session_directory d
  WHERE d.org_id=p_org AND d.workspace_id=p_workspace AND d.harness=p_harness AND d.session_id=p_session),
 'pending', coalesce((SELECT jsonb_agg(jsonb_build_object('source_session_id',m.source_session_id,'epoch',m.epoch::text) ORDER BY m.source_session_id COLLATE "C")
  FROM sessions.session_directory_event_pending m WHERE m.org_id=p_org AND m.workspace_id=p_workspace AND m.harness=p_harness
   AND m.source_session_id IN(SELECT p.session_id FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness AND coalesce(p.canonical_session_id,p.session_id)=p_session AND p.deleted_at IS NULL)),'[]'::jsonb),
 'pointers',coalesce((
  SELECT jsonb_agg(jsonb_build_object(
   'org_id',p.org_id,'workspace_id',p.workspace_id,'harness',p.harness,
   'session_id',coalesce(p.canonical_session_id,p.session_id),'machine_id',coalesce(p.canonical_machine_id,p.machine_id),
   'source_session_id',p.session_id,'source_machine_id',p.machine_id,
   'canonical_session_id',p.canonical_session_id,'canonical_machine_id',p.canonical_machine_id,
   'user_id',p.user_id,'storage_provider',p.storage_provider,'bucket',p.bucket,
   'manifest_key',p.manifest_key,'content_sha256',encode(p.content_sha256,'hex'),
   'raw_format',p.raw_format,'format_version',p.format_version,'manifest_etag',p.manifest_etag,'raw_bytes',p.raw_bytes::text,'chunk_count',p.chunk_count,
   'revision',p.revision::text,'deleted_at',p.deleted_at)
   ORDER BY p.manifest_key COLLATE "C",p.session_id COLLATE "C",p.machine_id COLLATE "C")
  FROM sessions.session_index p WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
   AND coalesce(p.canonical_session_id,p.session_id)=p_session AND p.deleted_at IS NULL
 ),'[]'::jsonb)) FROM sessions.session_replay_publications a
 WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session;
$snapshot$;
CREATE OR REPLACE FUNCTION sessions.publish_session_replay_pages(
 p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_snapshot jsonb,
 p_generation text,p_source_generation text,p_prefix text,p_index_key text,p_index_sha256 text,p_index_bytes integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE live jsonb; fp text; anchor jsonb; expected_prefix text;
BEGIN
 -- Row lock is also acquired by every pointer mutation trigger. A pointer txn
 -- already in progress invalidates this publication before that txn can commit.
 PERFORM 1 FROM sessions.session_replay_publications a WHERE a.org_id=p_org AND a.workspace_id=p_workspace
  AND a.harness=p_harness AND a.session_id=p_session AND a.epoch=p_epoch FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 live:=sessions.session_replay_source_snapshot(p_org,p_workspace,p_harness,p_session);
 fp:=encode(sha256(convert_to(live::text,'UTF8')),'hex');
 IF jsonb_typeof(live->'directory') IS DISTINCT FROM 'object' OR coalesce(live->'directory'->>'deleted_at','')<>'' OR live IS DISTINCT FROM p_snapshot OR fp IS DISTINCT FROM p_fingerprint OR jsonb_array_length(live->'pointers')=0
  OR p_generation IS NULL OR p_source_generation IS NULL OR p_index_sha256 IS NULL OR p_index_bytes IS NULL OR p_prefix IS NULL OR p_index_key IS NULL
  OR p_generation !~ '^[0-9a-f]{64}$' OR p_source_generation !~ '^[0-9a-f]{64}$'
  OR p_index_sha256 !~ '^[0-9a-f]{64}$' OR p_index_bytes NOT BETWEEN 1 AND 262144 THEN RETURN false; END IF;
 anchor:=live->'pointers'->0;
 IF (anchor->>'manifest_key') !~ '/manifest\.json$' OR (anchor->>'content_sha256') !~ '^[0-9a-f]{64}$' THEN RETURN false; END IF;
 expected_prefix:=regexp_replace(anchor->>'manifest_key','manifest\.json$','')||'normalized/read-projection-v1/'||(anchor->>'content_sha256')||'/'||p_generation||'/';
 IF p_prefix IS DISTINCT FROM expected_prefix OR p_index_key IS DISTINCT FROM p_prefix||p_index_sha256||'.json' THEN RETURN false; END IF;
 UPDATE sessions.session_replay_publications SET published_epoch=p_epoch,source_fingerprint=fp,source_snapshot=live,
  generation=p_generation,source_generation=p_source_generation,artifact_prefix=p_prefix,index_key=p_index_key,index_sha256=p_index_sha256,index_bytes=p_index_bytes,published_at=now()
  WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session AND epoch=p_epoch;
 RETURN FOUND;
END $publish$;
CREATE OR REPLACE FUNCTION sessions.session_publication_is_managed(p_org text,p_ws text,p_harness text,p_session text,p_machine text)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $managed$
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=p_org AND workspace_id=p_ws
  AND physical_source=p_harness AND physical_session=p_session AND machine_id=p_machine);
$managed$;
CREATE OR REPLACE FUNCTION sessions.claim_session_large_read(p_nonce uuid,p_lease_seconds integer DEFAULT 120)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE j sessions.session_large_read_jobs%ROWTYPE;c jsonb;
BEGIN
 IF p_nonce IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 30 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='large read: invalid lease'; END IF;
 FOR j IN SELECT * FROM sessions.session_large_read_jobs WHERE
 (state='queued' AND available_at<=clock_timestamp()) OR (state='running' AND lease_expires_at<=clock_timestamp())
 ORDER BY available_at,updated_at LIMIT 100 FOR UPDATE SKIP LOCKED LOOP
  IF j.attempts>=5 THEN
   UPDATE sessions.session_large_read_jobs SET state='failed',lease_nonce=NULL,lease_expires_at=NULL,last_error='attempts_exhausted',updated_at=now() WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END IF;
  c:=sessions.capture_session_replay_pages(j.org_id,j.workspace_id,j.harness,j.session_id);
  IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean OR c->'source'->>'epoch' IS DISTINCT FROM j.source_epoch::text
   OR c->'source'->>'fingerprint' IS DISTINCT FROM j.source_fingerprint THEN
   UPDATE sessions.session_large_read_jobs SET state='superseded',lease_nonce=NULL,lease_expires_at=NULL,updated_at=now()
    WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END IF;
  UPDATE sessions.session_large_read_jobs SET state='running',lease_nonce=p_nonce,lease_expires_at=clock_timestamp()+make_interval(secs=>p_lease_seconds),attempts=attempts+1,last_error=NULL,updated_at=now()
   WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
  RETURN jsonb_build_object('identity',c->'identity','source',c->'source','leaseNonce',p_nonce::text);
 END LOOP;
 RETURN NULL;
END $claim$;
DROP FUNCTION IF EXISTS
 sessions.session_history_large_job(jsonb),
 sessions.ingest_history_directory_state(),
 sessions.ingest_history_control_changed(),
 sessions.ingest_history_invalidate_scope(text,text,text,text),
 sessions.session_history_register_artifact(jsonb),
 sessions.ingest_history_mask_get(jsonb),
 sessions.session_history_replay_descriptor(jsonb),
 sessions.session_history_replay_publish(jsonb),
 sessions.session_history_summary_get(jsonb),
 sessions.session_history_capture(jsonb),
 sessions.session_history_state_publish(jsonb),
 sessions.session_history_summary_publish(jsonb),
 sessions.ingest_history_require_model_permit(jsonb),
 sessions.session_history_register_object(jsonb),
 sessions.session_history_mask_page(jsonb),
 sessions.session_history_source_lock(jsonb),
 sessions.ingest_history_source_current(jsonb,boolean),
 sessions.ingest_history_mask_rows(text,bigint,integer),
 sessions.ingest_history_mask_register(jsonb),
 sessions.ingest_history_native_changed(),
 sessions.ingest_history_plan_refs_page(jsonb),
 sessions.ingest_history_plan_carriers_page(jsonb),
 sessions.ingest_history_plan_heartbeat(jsonb),
 sessions.ingest_history_plan_locked(uuid,uuid,text),
 sessions.ingest_history_plan_claim(jsonb),
 sessions.ingest_history_native_snapshot(uuid),
 sessions.ingest_history_inventory_heartbeat(jsonb),
 sessions.ingest_history_inventory_finish(jsonb),
 sessions.ingest_history_inventory_page(jsonb),
 sessions.ingest_history_inventory_refs(jsonb),
 sessions.ingest_history_inventory_locked(jsonb),
 sessions.ingest_history_inventory_claim(jsonb),
 sessions.ingest_history_sync_pointer(),
 sessions.ingest_history_pointer(sessions.session_index),
 sessions.ingest_history_root(text);
DROP TABLE IF EXISTS
 sessions.ingest_history_graph_scan,
 sessions.ingest_history_privacy_outbox,
 sessions.ingest_history_scope_owners,
 sessions.ingest_history_states,
 sessions.ingest_history_summaries,
 sessions.ingest_history_derived_objects,
 sessions.ingest_history_scope_keys,
 sessions.ingest_history_scope_current,
 sessions.ingest_history_scope_versions,
 sessions.ingest_history_required_scopes,
 sessions.ingest_history_mask_pages,
 sessions.ingest_history_reconcile_outbox,
 sessions.ingest_history_plan_refs,
 sessions.ingest_history_plan_carriers,
 sessions.ingest_history_plans,
 sessions.ingest_history_native_locations,
 sessions.ingest_history_scan_pages,
 sessions.ingest_history_scan_refs,
 sessions.ingest_history_pointer_refs,
 sessions.ingest_history_carriers;
DROP SEQUENCE IF EXISTS sessions.ingest_history_inventory_epoch;
ALTER TABLE sessions.session_large_read_jobs DROP COLUMN history_generation;
