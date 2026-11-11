-- Operator erase runtime for canonical synchronous self-host ingest. Inert until explicitly called.
-- No production apply is authorized. 050000/050010 must precede this slot.
-- Open write obligations deliberately have no timeout: a crashed or ambiguous writer must never
-- disappear merely because a clock elapsed. Operator recovery must establish that writer is stopped.
CREATE TABLE sessions.session_erasure_writes (
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 writer_id uuid NOT NULL,started_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,
 PRIMARY KEY(org_id,workspace_id,source,session_id,writer_id)
);
CREATE TABLE sessions.session_erasure_scopes (
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 target_session_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,source,session_id,target_session_id),
 FOREIGN KEY(org_id,workspace_id,source,session_id) REFERENCES sessions.session_erasure_receipts
);
CREATE TABLE sessions.session_erasure_runs (
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 bucket text NOT NULL,key_root text NOT NULL,store_ref text NOT NULL CHECK(store_ref~'^[0-9a-f]{64}$'),
 lease_id uuid,lease_until timestamptz,available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,source,session_id),
 FOREIGN KEY(org_id,workspace_id,source,session_id) REFERENCES sessions.session_erasure_receipts
);
ALTER TABLE sessions.session_erasure_writes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_scopes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_runs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_erasure_writes,sessions.session_erasure_scopes,sessions.session_erasure_runs
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest;

CREATE FUNCTION sessions.ingest_operator_runtime_ids(p_org text,p_ws text,p_source text,p_session text) RETURNS text[]
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $ids$
 WITH RECURSIVE related(id) AS (
  SELECT p_session UNION
  SELECT n.id FROM related AS rel JOIN sessions.session_index AS s
   ON s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
    AND (s.session_id=rel.id OR s.canonical_session_id=rel.id)
   CROSS JOIN LATERAL(VALUES(s.session_id),(s.canonical_session_id)) AS n(id) WHERE n.id IS NOT NULL
 ) SELECT array_agg(id ORDER BY id COLLATE "C") FROM(SELECT id FROM related LIMIT 257) AS bounded
$ids$;

CREATE FUNCTION sessions.ingest_operator_runtime_write_begin(p jsonb) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $write_begin$
DECLARE v_org text;v_ws text;v_writer uuid;v_pair record;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['orgId','workspaceId','writerId','sessions']);
 v_org:=sessions.ingest_identifier(p->'orgId',false);v_ws:=sessions.ingest_identifier(p->'workspaceId',true);
 v_writer:=(p->>'writerId')::uuid;
 IF v_writer IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator write: writer required';END IF;
 -- Serialize registration with erase, in the same order in every process. These locks last only
 -- for this short transaction; the persistent obligation, not a held connection, covers S3 I/O.
 FOR v_pair IN SELECT source,session_id FROM sessions.ingest_session_pairs(p->'sessions') ORDER BY source COLLATE "C",session_id COLLATE "C" LOOP
  PERFORM pg_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:operator-write',v_org,v_ws,v_pair.source,v_pair.session_id),0));
 END LOOP;
 IF EXISTS(SELECT 1 FROM sessions.ingest_session_pairs(p->'sessions') AS x JOIN sessions.session_erasure_fences AS f
  ON f.org_id=v_org AND f.workspace_id=v_ws AND f.source=x.source AND f.session_id=x.session_id) THEN RETURN false;END IF;
 INSERT INTO sessions.session_erasure_writes(org_id,workspace_id,source,session_id,writer_id)
 SELECT v_org,v_ws,x.source,x.session_id,v_writer FROM sessions.ingest_session_pairs(p->'sessions') AS x;
 RETURN true;
END $write_begin$;

CREATE FUNCTION sessions.ingest_operator_runtime_write_finish(p_org text,p_ws text,p_writer uuid) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $write_finish$
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 UPDATE sessions.session_erasure_writes SET finished_at=coalesce(finished_at,clock_timestamp())
 WHERE org_id=p_org AND workspace_id=p_ws AND writer_id=p_writer;
END $write_finish$;

CREATE FUNCTION sessions.ingest_operator_runtime_location_valid(p_root text,p_org text,p_ws text,p_source text,p_session text,p_machine text,p_manifest text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $location$
DECLARE v_prefix text;v_parts text[];
BEGIN
 IF p_machine IS NULL OR p_machine!~'^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$' OR p_manifest IS NULL THEN RETURN false;END IF;
 v_prefix:=p_root||'/'||p_org||'/v1/ws='||p_ws||'/m='||p_machine||'/h='||p_source||'/';
 IF starts_with(p_manifest,v_prefix) THEN
  v_parts:=regexp_match(substr(p_manifest,length(v_prefix)+1),'^d=[0-9]{4}/[0-9]{2}/[0-9]{2}/s=([^/]+)/manifest[.]json$');
  RETURN coalesce(v_parts[1]=p_session,false);
 END IF;
 v_prefix:=p_root||'/'||p_org||'/upload-generations/';
 IF NOT starts_with(p_manifest,v_prefix) THEN RETURN false;END IF;
 v_parts:=regexp_match(substr(p_manifest,length(v_prefix)+1),'^g=[0-9a-f-]{36}/v1/ws=([^/]+)/m=([^/]+)/h=([^/]+)/d=[0-9]{4}/[0-9]{2}/[0-9]{2}/s=([^/]+)/publications/[^/]+/manifest[.]json$');
 RETURN coalesce(v_parts=ARRAY[p_ws,p_machine,p_source,p_session],false);
END $location$;

CREATE FUNCTION sessions.ingest_operator_runtime_begin(p_org text,p_ws text,p_source text,p_session text,p_request text,p_actor_kind text,p_actor_id text,p_bucket text,p_key_root text,p_store_ref text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $runtime_begin$
DECLARE v_ids text[];v_id text;v_receipt jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 -- Resume before consulting metadata already deleted by a prior attempt.
 v_receipt:=sessions.ingest_operator_erase_receipt(p_org,p_ws,p_source,p_session);
 IF v_receipt IS NOT NULL AND v_receipt->>'status'<>'failed' THEN RETURN v_receipt;END IF;
 v_ids:=sessions.ingest_operator_runtime_ids(p_org,p_ws,p_source,p_session);
 IF cardinality(v_ids)>256 THEN RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: alias cap exceeded';END IF;
 FOREACH v_id IN ARRAY v_ids LOOP
  PERFORM pg_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:operator-write',p_org,p_ws,p_source,v_id),0));
 END LOOP;
 IF v_ids IS DISTINCT FROM sessions.ingest_operator_runtime_ids(p_org,p_ws,p_source,p_session) THEN
  RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: alias set changed while locking';END IF;
 IF p_bucket IS NULL OR p_bucket='' OR p_key_root IS NULL OR p_key_root!~'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
  OR p_store_ref IS NULL OR p_store_ref!~'^[0-9a-f]{64}$'
  OR EXISTS(SELECT 1 FROM unnest(ARRAY[p_org,p_ws,p_source]||v_ids) AS i WHERE i IS NULL OR i!~'^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$')
  OR EXISTS(SELECT 1 FROM sessions.session_index AS s WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
   AND(s.session_id=ANY(v_ids) OR s.canonical_session_id=ANY(v_ids)) AND(
    s.storage_provider NOT IN('s3','r2') OR s.bucket IS DISTINCT FROM p_bucket
    OR NOT sessions.ingest_operator_runtime_location_valid(p_key_root,p_org,p_ws,p_source,s.session_id,s.machine_id,s.manifest_key))) THEN
  RAISE EXCEPTION USING ERRCODE='RH013',MESSAGE='operator erase: unsupported or ambiguous object location';END IF;
 -- Source-agnostic analysis/queue tables cannot safely distinguish colliding source ids.
 -- Refuse before tombstone triggers rather than deleting another source's analysis.
 IF EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=p_org AND workspace_id=p_ws AND harness<>p_source
   AND (session_id=ANY(v_ids) OR canonical_session_id=ANY(v_ids))) THEN
  RAISE EXCEPTION USING ERRCODE='RH013',MESSAGE='operator erase: source collision requires scoped analysis support';END IF;
 -- These pipelines have independent writers or mixed-session objects. They need their own drain
 -- and redaction protocols; absence is verified, never assumed from a runtime flag.
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_scope_versions WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source AND session_id=ANY(v_ids))
 OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=ANY(v_ids))
 OR EXISTS(SELECT 1 FROM sessions.babysitter_retention_objects WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=ANY(v_ids))
 OR EXISTS(SELECT 1 FROM sessions.session_analysis_jobs_v2 WHERE org_id=p_org AND workspace_id=p_ws AND session_id=ANY(v_ids))
 OR EXISTS(SELECT 1 FROM sessions.session_analysis_requests_v2 WHERE org_id=p_org AND workspace_id=p_ws AND session_id=ANY(v_ids))
 OR EXISTS(SELECT 1 FROM sessions.session_analysis_result_tickets WHERE org_id=p_org AND workspace_id=p_ws AND session_id=ANY(v_ids))
 OR EXISTS(SELECT 1 FROM sessions.session_large_read_jobs WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source AND session_id=ANY(v_ids) AND state='running')
 OR EXISTS(SELECT 1 FROM sessions.session_analysis_wakeups WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source AND session_id=ANY(v_ids) AND lease_nonce IS NOT NULL)
 OR EXISTS(SELECT 1 FROM sessions.session_brief_document_control WHERE live) THEN
  RAISE EXCEPTION USING ERRCODE='RH013',MESSAGE='operator erase: independent artifact writer requires a drain protocol';END IF;
 v_receipt:=sessions.ingest_operator_erase_session(p_org,p_ws,p_source,p_session,p_request,p_actor_kind,p_actor_id);
 INSERT INTO sessions.session_erasure_scopes(org_id,workspace_id,source,session_id,target_session_id)
 SELECT p_org,p_ws,p_source,p_session,i FROM unnest(v_ids) AS i ON CONFLICT DO NOTHING;
 INSERT INTO sessions.session_erasure_runs(org_id,workspace_id,source,session_id,bucket,key_root,store_ref)
 VALUES(p_org,p_ws,p_source,p_session,p_bucket,p_key_root,p_store_ref)
 ON CONFLICT(org_id,workspace_id,source,session_id) DO UPDATE SET available_at=clock_timestamp();
 RETURN v_receipt;
END $runtime_begin$;

CREATE FUNCTION sessions.ingest_operator_runtime_next(p_lease uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $next$
DECLARE v_run sessions.session_erasure_runs%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 IF p_lease IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: lease required';END IF;
 SELECT q.* INTO v_run FROM sessions.session_erasure_runs AS q JOIN sessions.session_erasure_receipts AS r
 USING(org_id,workspace_id,source,session_id)
 WHERE r.status='in_progress' AND r.uploads_fenced AND q.available_at<=clock_timestamp()
  AND(q.lease_until IS NULL OR q.lease_until<=clock_timestamp())
 ORDER BY q.available_at,q.org_id,q.workspace_id,q.source,q.session_id FOR UPDATE OF q SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN NULL;END IF;
 UPDATE sessions.session_erasure_runs SET lease_id=p_lease,lease_until=clock_timestamp()+interval '60 seconds'
 WHERE org_id=v_run.org_id AND workspace_id=v_run.workspace_id AND source=v_run.source AND session_id=v_run.session_id;
 RETURN jsonb_build_object('orgId',v_run.org_id,'workspaceId',v_run.workspace_id,'source',v_run.source,'sessionId',v_run.session_id,
  'bucket',v_run.bucket,'keyRoot',v_run.key_root,'storeRef',v_run.store_ref,
  'sessionIds',(SELECT jsonb_agg(target_session_id ORDER BY target_session_id COLLATE "C") FROM sessions.session_erasure_scopes
   WHERE org_id=v_run.org_id AND workspace_id=v_run.workspace_id AND source=v_run.source AND session_id=v_run.session_id));
END $next$;

CREATE FUNCTION sessions.ingest_operator_runtime_authorize(p_org text,p_ws text,p_source text,p_session text,p_lease uuid) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $authorize$
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 UPDATE sessions.session_erasure_runs AS q SET lease_until=clock_timestamp()+interval '60 seconds'
 WHERE q.org_id=p_org AND q.workspace_id=p_ws AND q.source=p_source AND q.session_id=p_session
  AND q.lease_id=p_lease AND q.lease_until>clock_timestamp()
  AND EXISTS(SELECT 1 FROM sessions.session_erasure_receipts AS r WHERE r.org_id=q.org_id AND r.workspace_id=q.workspace_id
   AND r.source=q.source AND r.session_id=q.session_id AND r.status='in_progress' AND r.uploads_fenced);
 IF NOT FOUND THEN RETURN false;END IF;
 IF to_regclass('sessions.pipeline_jobs') IS NOT NULL THEN
  IF EXISTS(SELECT 1 FROM sessions.pipeline_jobs AS j JOIN sessions.session_erasure_scopes AS s
   ON j.org_id=s.org_id AND j.workspace_id=s.workspace_id AND j.session_id=s.target_session_id
   WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.source=p_source AND s.session_id=p_session AND j.lease_id IS NOT NULL)
  THEN RETURN false;END IF;
 END IF;
 RETURN NOT EXISTS(SELECT 1 FROM sessions.session_erasure_writes AS w JOIN sessions.session_erasure_scopes AS s
  ON w.org_id=s.org_id AND w.workspace_id=s.workspace_id AND w.source=s.source AND w.session_id=s.target_session_id
  WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.source=p_source AND s.session_id=p_session AND w.finished_at IS NULL);
END $authorize$;

CREATE FUNCTION sessions.ingest_operator_runtime_release(p_org text,p_ws text,p_source text,p_session text,p_lease uuid) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $release$
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 UPDATE sessions.session_erasure_runs SET lease_id=NULL,lease_until=NULL,available_at=clock_timestamp()+interval '5 seconds'
 WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session AND lease_id=p_lease;
END $release$;

CREATE FUNCTION sessions.ingest_operator_runtime_step(p_org text,p_ws text,p_source text,p_session text,p_lease uuid,p_step text,p_state text,p_count bigint,p_error text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $step$
DECLARE v_count bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 IF NOT sessions.ingest_operator_runtime_authorize(p_org,p_ws,p_source,p_session,p_lease) THEN
  RAISE EXCEPTION USING ERRCODE='RH014',MESSAGE='operator erase: writer or lease still pending';END IF;
 SELECT greatest(p_count,coalesce((artifacts->p_step->>'count')::bigint,0)) INTO v_count FROM sessions.session_erasure_receipts
 WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 RETURN sessions.ingest_operator_erase_record_step(p_org,p_ws,p_source,p_session,p_step,p_state,v_count,p_error);
END $step$;

-- The indexer's original upsert does not accept canonical identity fields. This wrapper checks
-- both identities under the erase registration lock and installs canonical identity atomically,
-- before the caller writes ext/catalog/fanout metadata. It is used only with the enforcement flag.
CREATE FUNCTION sessions.ingest_operator_runtime_upsert(p_rows jsonb,p_canonical text,p_canonical_machine text)
RETURNS TABLE(applied boolean,revision bigint,fenced boolean)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $upsert$
DECLARE v_row jsonb;v_org text;v_ws text;v_source text;v_session text;v_id text;v_result record;
BEGIN
 PERFORM sessions.ingest_require_role('search_consumer_runtime');
 IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' OR jsonb_array_length(p_rows)<>1 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator pointer: one row required';END IF;
 v_row:=p_rows->0;v_org:=sessions.ingest_identifier(v_row->'org_id',false);v_ws:=sessions.ingest_identifier(v_row->'workspace_id',true);
 v_source:=sessions.ingest_identifier(v_row->'harness',false);v_session:=sessions.ingest_identifier(v_row->'session_id',false);
 p_canonical:=sessions.ingest_identifier(to_jsonb(p_canonical),false);
 p_canonical_machine:=sessions.ingest_identifier(to_jsonb(p_canonical_machine),false);
 FOR v_id IN SELECT id FROM(SELECT DISTINCT unnest(ARRAY[v_session,p_canonical]) AS id) AS pairs ORDER BY id COLLATE "C" LOOP
  PERFORM pg_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:operator-write',v_org,v_ws,v_source,v_id),0));
 END LOOP;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_fences WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=ANY(ARRAY[v_session,p_canonical])) THEN
  RETURN QUERY SELECT false,0::bigint,true;RETURN;END IF;
 SELECT x.applied,x.revision INTO v_result FROM sessions.upsert_session_index(p_rows) AS x;
 UPDATE sessions.session_index SET canonical_session_id=p_canonical,canonical_machine_id=p_canonical_machine
 WHERE org_id=v_org AND workspace_id=v_ws AND harness=v_source AND session_id=v_session AND machine_id=v_row->>'machine_id'
  AND manifest_key=v_row->>'manifest_key' AND deleted_at IS NULL;
 RETURN QUERY SELECT coalesce(v_result.applied,false),coalesce(v_result.revision,0)::bigint,false;
END $upsert$;

CREATE FUNCTION sessions.ingest_operator_runtime_erased(p_org text,p_ws text,p_source text,p_session text) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $read_fence$
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_read_api');
 IF p_org IS NULL OR p_ws IS NULL OR p_org IS DISTINCT FROM current_setting('bulls96_read.org_id',true)
  OR p_ws IS DISTINCT FROM current_setting('bulls96_read.workspace_id',true) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='operator read fence: tenant binding required';END IF;
 RETURN EXISTS(SELECT 1 FROM sessions.session_erasure_fences WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session);
END $read_fence$;

-- No table-wide grant is added to the ingest login. The fixed catalog below is the entire purge
-- authority. Optional 96bulls tables are handled only when installed; names never come from input.
CREATE FUNCTION sessions.ingest_operator_runtime_catalog() RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $catalog$
 SELECT '[
 ["pointerRows","session_index","harness"],["pointerRows","session_index_ext","harness"],["pointerRows","session_verdicts","harness"],
 ["directoryRows","session_directory","harness"],["directoryRows","session_directory_users","harness"],["directoryRows","session_directory_machines","harness"],
 ["catalogRows","session_directory_agents","harness"],["catalogRows","session_directory_event_times","harness"],["catalogRows","session_desktop_catalog_reproject","harness"],
 ["largeReadJobs","session_large_read_jobs","harness"],["largeReadJobs","session_large_read_scope_errors","harness"],
 ["summaries","session_summaries","harness"],["briefs","session_briefs","harness"],["briefs","session_brief_wakeups","harness"],["briefs","session_brief_attempts","harness"],
 ["briefDocuments","session_brief_document_outbox","harness"],["replayPublications","session_replay_publications","harness"],
 ["analysisPublications","session_analysis_publications","harness"],["analysisPublications","session_analysis_wakeups","harness"],
 ["searchDocuments","session_event_facts","source"],["searchDocuments","session_output_facts","scope_source"],["searchDocuments","output_facts_session_state","source"],
 ["ledgerRows","ingest_record_revisions","source"],["ledgerRows","ingest_session_ownership","source"],["ledgerRows","ingest_claim_sessions","source"],
 ["catalogRows","canonical_project_events","harness"],
 ["catalogRows","pipeline_events","harness"]
 ]'::jsonb
$catalog$;

CREATE FUNCTION sessions.ingest_operator_runtime_purge(p_org text,p_ws text,p_source text,p_session text,p_lease uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $purge$
DECLARE v_ids text[];v_entry jsonb;v_table text;v_col text;v_artifact text;v_count bigint;v_left bigint;v_counts jsonb:='{}';v_pass integer;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 IF NOT sessions.ingest_operator_runtime_authorize(p_org,p_ws,p_source,p_session,p_lease) THEN
  RAISE EXCEPTION USING ERRCODE='RH014',MESSAGE='operator erase: writer or lease still pending';END IF;
 SELECT array_agg(target_session_id) INTO v_ids FROM sessions.session_erasure_scopes
 WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 DELETE FROM sessions.session_directory_relationships WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source
  AND(parent_session_id=ANY(v_ids) OR child_session_id=ANY(v_ids));
 IF to_regclass('search_index.projection_state') IS NOT NULL THEN
  PERFORM set_config('bulls96_read.org_id',p_org,true),set_config('bulls96_read.workspace_id',p_ws,true);
  DELETE FROM search_index.projection_state WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source
   AND(session_id=ANY(v_ids) OR canonical_session_id=ANY(v_ids));
  IF EXISTS(SELECT 1 FROM search_index.projection_state WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source
   AND(session_id=ANY(v_ids) OR canonical_session_id=ANY(v_ids))) THEN
   RAISE EXCEPTION USING ERRCODE='RH015',MESSAGE='operator erase: search state remains';END IF;
 END IF;
 -- A digest mixes summaries from several sessions and cannot be selectively redacted. Evict
 -- this workspace cache; another workspace is never affected. The guard below prevents a
 -- previously started narrative writer from restoring text from before the fence.
 DELETE FROM sessions.daily_digest_narratives WHERE org_id=p_org AND workspace_id=p_ws;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 v_counts:=jsonb_build_object('summaries',v_count);
 -- Derived publications can be re-queued by pointer/directory deletion triggers. Two passes
 -- remove those generated rows too; the final independent counts must be zero.
 FOR v_pass IN 1..2 LOOP
  FOR v_entry IN SELECT value FROM jsonb_array_elements(sessions.ingest_operator_runtime_catalog()) LOOP
   v_artifact:=v_entry->>0;v_table:=v_entry->>1;v_col:=v_entry->>2;
   IF to_regclass('sessions.'||v_table) IS NULL THEN CONTINUE;END IF;
   EXECUTE format('DELETE FROM sessions.%I WHERE org_id=$1 AND workspace_id=$2 AND session_id=ANY($3)%s',v_table,
    CASE WHEN v_col IS NULL THEN '' ELSE format(' AND %I=$4',v_col) END) USING p_org,p_ws,v_ids,p_source;
   GET DIAGNOSTICS v_count=ROW_COUNT;
   v_counts:=v_counts||jsonb_build_object(v_artifact,coalesce((v_counts->>v_artifact)::bigint,0)+v_count);
  END LOOP;
 END LOOP;
 FOR v_entry IN SELECT value FROM jsonb_array_elements(sessions.ingest_operator_runtime_catalog()) LOOP
  v_table:=v_entry->>1;v_col:=v_entry->>2;
  IF to_regclass('sessions.'||v_table) IS NULL THEN CONTINUE;END IF;
  EXECUTE format('SELECT count(*) FROM sessions.%I WHERE org_id=$1 AND workspace_id=$2 AND session_id=ANY($3)%s',v_table,
   CASE WHEN v_col IS NULL THEN '' ELSE format(' AND %I=$4',v_col) END) INTO v_left USING p_org,p_ws,v_ids,p_source;
  IF v_left<>0 THEN RAISE EXCEPTION USING ERRCODE='RH015',MESSAGE='operator erase: metadata remains';END IF;
 END LOOP;
 -- Keep counts and verification in the purge transaction, so a crash before the caller receives
 -- its answer cannot lose the audit evidence or report zero on the next attempt.
 FOR v_artifact IN SELECT DISTINCT value->>0 FROM jsonb_array_elements(sessions.ingest_operator_runtime_catalog()) LOOP
  SELECT greatest(coalesce((v_counts->>v_artifact)::bigint,0),coalesce((artifacts->v_artifact->>'count')::bigint,0))
  INTO v_count FROM sessions.session_erasure_receipts
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
  PERFORM sessions.ingest_operator_erase_record_step(p_org,p_ws,p_source,p_session,v_artifact,'done',v_count,NULL);
 END LOOP;
 RETURN v_counts;
END $purge$;

-- Late metadata writers must not restore pointers, directory text, summaries or publications after
-- their source object is erased. Operational fences/receipts and cleanup tickets are not guarded.
CREATE FUNCTION sessions.ingest_operator_runtime_metadata_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $metadata_guard$
DECLARE v_row jsonb:=to_jsonb(NEW);
BEGIN
 IF TG_TABLE_NAME='daily_digest_narratives' THEN
  IF EXISTS(SELECT 1 FROM sessions.session_erasure_fences WHERE org_id=v_row->>'org_id' AND workspace_id=v_row->>'workspace_id') THEN RETURN NULL;END IF;
  RETURN NEW;
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_fences AS f
  WHERE f.org_id=v_row->>'org_id' AND f.workspace_id=v_row->>'workspace_id'
   AND f.source=coalesce(v_row->>'harness',v_row->>'scope_source',v_row->>'source') AND f.session_id=v_row->>'session_id') THEN
  -- Let the initial fence tombstone an existing directory row. Subsequent writes, including
  -- dead-row inserts after physical purge, are refused.
  IF TG_TABLE_NAME='session_directory' AND TG_OP='UPDATE'
   AND(to_jsonb(OLD)->>'deleted_at') IS NULL AND(v_row->>'deleted_at') IS NOT NULL THEN RETURN NEW;END IF;
  RETURN NULL;
 END IF;
 RETURN NEW;
END $metadata_guard$;
DO $guards$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['session_directory','session_directory_agents','session_directory_event_times','session_desktop_catalog_reproject',
  'session_large_read_jobs','session_index_ext','session_verdicts','session_summaries','session_briefs','session_brief_wakeups','session_brief_attempts',
  'session_brief_document_outbox','session_replay_publications','session_analysis_publications','session_analysis_wakeups',
  'session_event_facts','session_output_facts','output_facts_session_state','session_analysis_jobs_v2','daily_digest_narratives'] LOOP
  IF to_regclass('sessions.'||t) IS NOT NULL THEN
   EXECUTE format('CREATE TRIGGER operator_erase_metadata_guard BEFORE INSERT OR UPDATE ON sessions.%I FOR EACH ROW EXECUTE FUNCTION sessions.ingest_operator_runtime_metadata_guard()',t);
  END IF;
 END LOOP;
END $guards$;

REVOKE ALL ON FUNCTION sessions.ingest_operator_runtime_ids(text,text,text,text),sessions.ingest_operator_runtime_catalog(),
 sessions.ingest_operator_runtime_location_valid(text,text,text,text,text,text,text),
 sessions.ingest_operator_runtime_metadata_guard(),sessions.ingest_operator_runtime_write_begin(jsonb),
 sessions.ingest_operator_runtime_write_finish(text,text,uuid),sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text),
 sessions.ingest_operator_runtime_next(uuid),sessions.ingest_operator_runtime_authorize(text,text,text,text,uuid),
 sessions.ingest_operator_runtime_release(text,text,text,text,uuid),sessions.ingest_operator_runtime_purge(text,text,text,text,uuid),
 sessions.ingest_operator_runtime_step(text,text,text,text,uuid,text,text,bigint,text),sessions.ingest_operator_runtime_upsert(jsonb,text,text),
 sessions.ingest_operator_runtime_erased(text,text,text,text)
 FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.ingest_operator_runtime_write_begin(jsonb),sessions.ingest_operator_runtime_write_finish(text,text,uuid),
 sessions.ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text),sessions.ingest_operator_runtime_next(uuid),
 sessions.ingest_operator_runtime_authorize(text,text,text,text,uuid),sessions.ingest_operator_runtime_release(text,text,text,text,uuid),
 sessions.ingest_operator_runtime_purge(text,text,text,text,uuid),sessions.ingest_operator_runtime_step(text,text,text,text,uuid,text,text,bigint,text) TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.ingest_operator_runtime_upsert(jsonb,text,text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.ingest_operator_runtime_erased(text,text,text,text) TO bulls96_read_api;
