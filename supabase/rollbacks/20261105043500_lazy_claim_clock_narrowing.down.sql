-- Generated: restore the exact 055000 lazy claim / ACK clock locking; owner/ACL stay unchanged.
-- Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.
-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy claim clock: migration ledger/marker required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105043500_lazy_claim_clock_narrowing.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy claim clock: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105043500_lazy_claim_clock_narrowing.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'4c5c9c8904e28e2ca544ade05b15ba429aa696b8f34f0aa0f5789c406c2b0325'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy claim clock: own applied checksum required';END IF;
 UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='lazy claim clock: validation marker required';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.ingest_lazy_records(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_records$
DECLARE w jsonb;v_org text;v_ws text;v_user text;v_origin text;v_items jsonb;v_reset bigint;
 c sessions.ingest_receipt_seat_clock%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;
 z record;v_key text;v_material jsonb;cur jsonb;v_legacy text;v_meta jsonb;v_fence bigint;seq jsonb;
BEGIN
 w:=p#>'{custody,witness}';v_org:=p#>>'{tenant,orgId}';v_ws:=p#>>'{tenant,workspaceId}';v_user:=p#>>'{tenant,userId}';
 v_origin:=p->>'originId';v_items:=sessions.ingest_records(p->'records');
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 SELECT count(*) INTO v_reset FROM sessions.upload_resets WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 -- Source true is an additional authoritative suppression. False never clears
 -- a target fence; only the ordered control may re-enable a scope.
 WITH pairs AS MATERIALIZED(
  SELECT DISTINCT r.source,r.session_id FROM jsonb_to_recordset(v_items)r(ord integer,source text,session_id text)
   JOIN jsonb_array_elements(w->'records')q(e) ON r.ord=(e->>'preparedOrd')::integer
  WHERE (e->>'candidateFenced')::boolean AND r.session_id IS NOT NULL
 ), fresh AS MATERIALIZED(
  SELECT p.* FROM pairs p WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=v_org
   AND d.workspace_id=v_ws AND d.user_id=v_user AND d.origin_id=v_origin AND d.source=p.source AND d.session_id=p.session_id)
 ), versioned AS(
  INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
  SELECT v_org,v_ws,v_user,v_origin,x.source,x.session_id,1 FROM fresh x
  ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1
 )
 INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
 SELECT v_org,v_ws,v_user,v_origin,x.source,x.session_id FROM fresh x;
 FOR z IN SELECT r.*,e AS witness_record FROM jsonb_to_recordset(v_items)
  r(ord integer,record_id text,revision bigint,revision_id text,digest text,source text,session_id text)
  JOIN jsonb_array_elements(w->'records')q(e) ON r.ord=(e->>'preparedOrd')::integer
  WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=v_org AND d.workspace_id=v_ws
   AND d.user_id=v_user AND d.origin_id=v_origin AND d.source=r.source AND d.session_id=r.session_id)
  ORDER BY (e->>'processingRank')::integer
 LOOP
  v_material:=jsonb_build_array('global-native-custody-v1',v_org,v_ws,v_origin,z.record_id);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF FOUND THEN
   IF slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256 OR slot.control_epoch<>c.control_epoch THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native custody identity changed';END IF;
   IF slot.current_owner IS NOT NULL AND slot.current_owner<>v_user THEN
    RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
   -- An initialized slot is authoritative, even when empty. The original
   -- source copy may be obsolete after target updates or authorized erasure.
   CONTINUE;
  END IF;
  cur:=z.witness_record->'current';v_legacy:=NULL;v_fence:=0;
  IF cur<>'null'::jsonb THEN
   PERFORM sessions.ingest_exact_keys(cur,ARRAY['userId','revision','digest','revisionId','kind','source','sessionId','operation','catalogAcceptedSeq','projectionAcceptedSeq']);
   IF EXISTS(SELECT 1 FROM unnest(ARRAY['userId','revisionId','kind','source'])k WHERE jsonb_typeof(cur->k) IS DISTINCT FROM 'string')
    OR jsonb_typeof(cur->'revision') IS DISTINCT FROM 'string' OR cur->>'revision'!~'^[1-9][0-9]{0,18}$'
    OR jsonb_typeof(cur->'sessionId') NOT IN('string','null') OR cur->>'operation' NOT IN('upsert','delete') THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid current custody metadata';END IF;
   IF cur->>'userId' IS DISTINCT FROM v_user THEN
    RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: source native key belongs to another user';END IF;
   PERFORM sessions.ingest_digest(cur->'digest');
   PERFORM (cur->>'revision')::bigint;
   FOREACH seq IN ARRAY ARRAY[cur->'catalogAcceptedSeq',cur->'projectionAcceptedSeq'] LOOP
    IF seq<>'null'::jsonb THEN
     IF jsonb_typeof(seq)<>'string' OR (seq#>>'{}')!~'^(0|[1-9][0-9]{0,18})$' THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid current custody sequence';END IF;
     PERFORM (seq#>>'{}')::bigint;
    END IF;
   END LOOP;
   IF (z.witness_record->>'currentFenced')::boolean THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source current row is already privacy fenced';END IF;
   IF nullif(cur->>'sessionId','') IS NOT NULL THEN
    SELECT coalesce((SELECT epoch FROM sessions.ingest_fence_versions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
     AND origin_id=v_origin AND source=cur->>'source' AND session_id=cur->>'sessionId'),0) INTO v_fence;
    IF EXISTS(SELECT 1 FROM sessions.upload_deletions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
     AND origin_id=v_origin AND source=cur->>'source' AND session_id=cur->>'sessionId') THEN
     RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: inconsistent current custody privacy';END IF;
   END IF;
   v_meta:=cur||jsonb_build_object('workspaceId',v_ws,'originId',v_origin,'recordId',z.record_id);
   v_legacy:=encode(sha256(convert_to(jsonb_build_array('live-current-custody-v1',c.source_identity_sha256,c.control_epoch,
    v_org,v_ws,v_user,v_origin,z.record_id,v_reset::text,p#>>'{custody,sourceDigest}',v_meta)::text,'UTF8')),'hex');
   INSERT INTO sessions.ingest_custody_records(key_sha256,import_token,page,source_key_sha256,org_id,workspace_id,user_id,origin_id,record_id,
    revision,digest,source,session_id,metadata,reset_epoch,fence_epoch,baseline_fingerprint,
    live_source_identity_sha256,live_control_epoch,live_control_version,live_witness_sha256)
   SELECT v_legacy,NULL,NULL,encode(sha256(convert_to(jsonb_build_array(v_ws,v_origin,z.record_id)::text,'UTF8')),'hex'),
    v_org,v_ws,v_user,v_origin,z.record_id,(cur->>'revision')::bigint,cur->>'digest',cur->>'source',nullif(cur->>'sessionId',''),v_meta,
    v_reset,v_fence,s.privacy_fingerprint,c.source_identity_sha256,c.control_epoch,(w->>'sourceControlVersion')::bigint,p#>>'{custody,sourceDigest}'
   FROM sessions.ingest_lazy_user_seals s WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.user_id=v_user;
   IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: user privacy seal missing';END IF;
  ELSIF (z.witness_record->>'currentFenced')::boolean THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: absent current custody cannot have a scope fence';
  END IF;
  INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,
   source_identity_sha256,control_epoch,initialized_witness_sha256,current_owner,current_source,current_session,current_revision,current_digest,
   current_reset_epoch,current_fence_epoch,current_legacy_key)
  VALUES(v_key,v_material,v_org,v_ws,v_origin,z.record_id,c.source_identity_sha256,c.control_epoch,p#>>'{custody,sourceDigest}',
   CASE WHEN v_legacy IS NULL THEN NULL ELSE v_user END,cur->>'source',nullif(cur->>'sessionId',''),
   (cur->>'revision')::bigint,cur->>'digest',CASE WHEN v_legacy IS NULL THEN NULL ELSE v_reset END,
   CASE WHEN v_legacy IS NULL THEN NULL ELSE v_fence END,v_legacy);
 END LOOP;
END $lazy_records$;
CREATE OR REPLACE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_batch$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 t sessions.ingest_stage_intents%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 d sessions.ingest_lazy_ack_decisions%ROWTYPE;r record;v_last bigint;v_key text;v_material jsonb;v_accept boolean;v_first text;v_new_lineage bigint;
BEGIN
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_lazy_user_seals u JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE u.org_id=p->>'org_id' AND u.workspace_id=p->>'workspace_id' AND u.user_id=p->>'user_id' AND u.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: real user seal required before any receipt';END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;v_last:=c.last_seat;
 IF p->>'buffer_receipt_kind'='suppressed' THEN RETURN;END IF;
 IF p->>'buffer_receipt_kind' IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unexpected receipt authority';END IF;
 SELECT i.* INTO t FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token
  WHERE o.org_id=p->>'org_id' AND o.workspace_id=p->>'workspace_id' AND o.user_id=p->>'user_id' AND o.origin_id=p->>'origin_id'
   AND o.batch_id=p->>'batch_id' AND o.digest=p->>'digest' AND o.state='buffered' FOR UPDATE OF i;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live'
  OR t.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current original stage required';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id
  OR a.batch_id<>t.batch_id OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch
  OR jsonb_array_length(a.source_processing_ranks)<>jsonb_array_length(a.records) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original candidate order changed';END IF;
 FOR r IN SELECT q.* FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q
  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP
  IF a.source_processing_ranks->>r.ord IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source processing rank missing';END IF;
  v_material:=jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,r.key_material->>7);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF NOT FOUND OR slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256
   OR slot.control_epoch<>c.control_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: initialized native custody slot required';END IF;
  IF slot.current_owner IS NOT NULL AND slot.current_owner<>a.user_id THEN
   RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
  v_accept:=slot.current_owner IS NULL OR slot.current_revision<(r.key_material->>8)::bigint;
  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;
  -- Every ignored old-source candidate receives a real target order reference;
  -- it never fabricates the source row's publication seat.
  IF NOT v_accept AND slot.current_seat_key IS NOT NULL THEN
   SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=slot.current_seat_key;
   IF s.first_digest<>v_first THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
  ELSE
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
   VALUES(encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
    r.key_material,v_last,v_first,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;
  END IF;
  v_new_lineage:=slot.lineage;
  IF v_accept THEN
   IF slot.lineage=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native lineage exhausted';END IF;
   v_new_lineage:=slot.lineage+1;
   UPDATE sessions.ingest_lazy_native_slots SET lineage=v_new_lineage,current_owner=a.user_id,
    current_source=r.key_material->>10,current_session=r.key_material->>11,current_revision=(r.key_material->>8)::bigint,
    current_digest=r.candidate_digest,current_reset_epoch=r.reset_epoch,current_fence_epoch=r.fence_epoch,
    current_seat_key=s.key_sha256,current_legacy_key=NULL WHERE key_sha256=v_key;
  END IF;
  INSERT INTO sessions.ingest_lazy_ack_decisions(claim_token,ord,native_key_sha256,native_lineage,candidate_key_sha256,candidate_digest,
   first_digest,body_accepted,seat_key_sha256,reset_epoch,fence_epoch,generation,source_control_version,source_witness_sha256)
  VALUES(t.claim_token,r.ord,v_key,v_new_lineage,r.key_sha256,r.candidate_digest,v_first,v_accept,s.key_sha256,r.reset_epoch,r.fence_epoch,
   a.schema_generation,a.source_control_version,a.source_witness_sha256) ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT d FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=t.claim_token AND ord=r.ord;
  IF d.native_key_sha256<>v_key OR d.native_lineage<>v_new_lineage OR d.candidate_key_sha256<>r.key_sha256 OR d.candidate_digest<>r.candidate_digest
   OR d.first_digest<>v_first OR d.body_accepted<>v_accept OR d.seat_key_sha256<>s.key_sha256 OR d.reset_epoch<>r.reset_epoch OR d.fence_epoch<>r.fence_epoch
   OR d.generation<>a.schema_generation OR d.source_control_version<>a.source_control_version OR d.source_witness_sha256<>a.source_witness_sha256 THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: immutable ACK decision adoption refused';END IF;
 END LOOP;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF t.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: stage expired while assigning seats';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
END $lazy_ack_batch$;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105043500_lazy_claim_clock_narrowing.sql' AND sha256='4c5c9c8904e28e2ca544ade05b15ba429aa696b8f34f0aa0f5789c406c2b0325';
COMMIT;
