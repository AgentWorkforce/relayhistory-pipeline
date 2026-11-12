-- Reverse of 20261105058010. Restores 20261104030000's exact sessions.ingest_lock_fences (and its ACL),
-- 20261105050010's exact sessions.ingest_buffer_gc_claim (owner, SECURITY DEFINER and ACL unchanged) and
-- 20261105041000's exact sessions.ingest_privacy_authority_begin and sessions.ingest_source_control_begin (SECURITY
-- DEFINER, EXECUTE for bulls96_session_ingest only, as before), then drops the heads trigger, the operator per-user
-- erase functions, its ledger table and its ledger row. Erasures already performed are not undone
-- (deleted rows and objects stay deleted; lazy slot tombstones stay and keep refusing a reseed of their keys).
-- Refused (55000) while any erase is not bytes_verified: dropping the fence mid-erase would readmit the user's
-- uploads before the purge and the byte verification finished. Completed erases have already deleted their fence rows.
-- Guard, restores, drops and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105058010_operator_user_erase.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105058010_operator_user_erase.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'b0ce9a9c37b9c9fc11e07189610bd1e8074f1442a7f54dbe46a3b76b9e48c08f'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: own applied checksum required';END IF;
 PERFORM pg_advisory_xact_lock(961996,50000);
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures WHERE state<>'bytes_verified')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: an erase is still active; finish it first';END IF;
END $rollback_guard$;
-- sessions.ingest_lock_fences(text,text,text): restored from 20261104030000.
CREATE OR REPLACE FUNCTION sessions.ingest_lock_fences(p_org text,p_ws text,p_user text)
RETURNS void LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lock$
BEGIN
 INSERT INTO sessions.ingest_fence_owners VALUES(p_org,p_ws,p_user) ON CONFLICT DO NOTHING;
 PERFORM 1 FROM sessions.ingest_fence_owners AS f
 WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.user_id=p_user FOR UPDATE;
END
$lock$;
REVOKE ALL ON FUNCTION sessions.ingest_lock_fences(text,text,text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- sessions.ingest_buffer_gc_claim(jsonb): restored from 20261105050010.
CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE v_limit int;v_generation text;candidate record;s sessions.ingest_stage_intents%ROWTYPE;
 child sessions.ingest_stage_intents%ROWTYPE;parent sessions.ingest_stage_intents%ROWTYPE;
 v_keep jsonb;v_ordinals jsonb;v_phase text;v_ticket jsonb;v_tickets jsonb:='[]';v_has_outbox boolean;v_abort boolean;v_token uuid;v_outbox_version bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM pg_advisory_xact_lock_shared(961996,50000);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 -- Marker first: metadata operations never wait for it while holding user/table
 -- locks. The migrator's PENDING update waits for this short operation to finish.
 PERFORM sessions.ingest_require_generation(v_generation);
 v_limit:=sessions.ingest_bounded(p->'limit',1,100);
 FOR candidate IN SELECT stage_token,org_id,workspace_id,user_id,origin_id,batch_id
  FROM sessions.ingest_stage_intents
  WHERE cleanup_state<>'redacted' AND (gc_lease_until IS NULL OR gc_lease_until<=clock_timestamp())
  ORDER BY coalesce(gc_checked_at,created_at),stage_token LIMIT 4
 LOOP
  -- GC-only admission hint; legacy writers do not take this advisory lock.
  -- The existing USER row lock below remains the authority shared with them.
  -- An acquisition-only subtransaction releases EVERY lock on a busy skip.
  -- No retention error, timeout, generation failure or mutation is swallowed.
  BEGIN
   IF NOT pg_try_advisory_xact_lock(hashtextextended(jsonb_build_array(
      '96bulls:buffer-gc-user-v1',candidate.org_id,candidate.workspace_id,candidate.user_id)::text,0)) THEN
    RAISE lock_not_available USING MESSAGE='buffer gc: user busy';
   END IF;
   PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=candidate.org_id
    AND workspace_id=candidate.workspace_id AND user_id=candidate.user_id FOR UPDATE NOWAIT;
   IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: user fence missing';
   END IF;
   PERFORM 1 FROM sessions.ingest_batches WHERE org_id=candidate.org_id AND workspace_id=candidate.workspace_id
    AND origin_id=candidate.origin_id AND batch_id=candidate.batch_id FOR UPDATE NOWAIT;
   IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: batch missing';
   END IF;
   SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=candidate.stage_token FOR UPDATE NOWAIT;
   IF NOT FOUND OR s.cleanup_state='redacted' OR s.gc_lease_until>clock_timestamp() THEN
    RAISE lock_not_available USING MESSAGE='buffer gc: candidate changed';
   END IF;
   PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
    AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE NOWAIT;
  EXCEPTION WHEN lock_not_available THEN
   CONTINUE;
  END;
  IF ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id) IS DISTINCT FROM
    ROW(candidate.org_id,candidate.workspace_id,candidate.user_id,candidate.origin_id,candidate.batch_id)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: identity changed';END IF;
  -- A prospective child cannot be independently aborted during its parent's copy.
  IF s.parent_stage_token IS NOT NULL THEN
   SELECT * INTO parent FROM sessions.ingest_stage_intents WHERE stage_token=s.parent_stage_token;
   -- Prospective bytes have a deterministic key. They remain owned through
   -- repeated parent crashes; retiring them merely because a lease expired would
   -- make that same replacement key permanently unusable after its barrier.
   IF parent.gc_replacement_stage_token=s.stage_token AND parent.cleanup_state='live' AND parent.receipt_state='acknowledged'
    AND ROW(parent.org_id,parent.workspace_id,parent.user_id,parent.origin_id,parent.batch_id,parent.claim_token,parent.digest)
      IS NOT DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.claim_token,s.digest)
    AND EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=parent.org_id AND o.workspace_id=parent.workspace_id
      AND o.user_id=parent.user_id AND o.origin_id=parent.origin_id AND o.batch_id=parent.batch_id AND o.digest=parent.digest
      AND o.state='buffered' AND o.active_stage_token=parent.stage_token)
   THEN
    UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
    EXIT;
   END IF;
  END IF;
  PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
   AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
  SELECT row_version INTO v_outbox_version FROM sessions.ingest_buffer_outbox WHERE active_stage_token=s.stage_token;
  v_has_outbox:=FOUND;
  v_abort:=false;
  IF s.cleanup_state='retired' THEN
   IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: current object cannot be cleaned';END IF;
   v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
  ELSE
   v_ordinals:=sessions.ingest_buffer_gc_ordinals(s.stage_token);
   SELECT coalesce(jsonb_agg(idx-1 ORDER BY idx),'[]') INTO v_keep
    FROM jsonb_array_elements(s.group_ordinals) WITH ORDINALITY g(value,idx)
    WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(value)o WHERE v_ordinals @> jsonb_build_array(o));
   IF s.receipt_state='preparing' THEN
    IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: preparing current object';END IF;
    v_abort:=s.expires_at<=clock_timestamp() OR jsonb_array_length(v_ordinals)<>(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o);
    IF NOT v_abort THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     EXIT;
    END IF;
    v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
   ELSE
    IF NOT v_has_outbox THEN
     -- Acknowledged but unreferenced intent is a known retired object.
     v_abort:=true;v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
    ELSIF jsonb_array_length(v_ordinals)=(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o) THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     EXIT;
    ELSE v_phase:='plan';END IF;
   END IF;
  END IF;
  -- A stale prospective child is never adopted into another keep set.
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token FOR UPDATE;
   IF NOT FOUND OR child.cleanup_state<>'live' OR child.group_ordinals IS DISTINCT FROM
     sessions.ingest_buffer_gc_groups(s.group_ordinals,v_ordinals)
   THEN
    IF FOUND AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE active_stage_token=child.stage_token) THEN
     UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1,
      gc_lease_token=NULL,gc_lease_until=NULL,gc_lease_version=NULL WHERE stage_token=child.stage_token;
    END IF;
    s.gc_replacement_stage_token:=NULL;
   END IF;
  END IF;
  v_token:=gen_random_uuid();
  UPDATE sessions.ingest_stage_intents SET gc_generation=v_generation,gc_lease_token=v_token,gc_lease_until=clock_timestamp()+interval '5 minutes',
   row_version=row_version+1,gc_lease_version=row_version+1,gc_checked_at=clock_timestamp(),gc_keep_groups=v_keep,gc_keep_ordinals=v_ordinals,
   gc_replacement_stage_token=s.gc_replacement_stage_token,gc_outbox_version=v_outbox_version,
   cleanup_state=CASE WHEN v_abort THEN 'retired' ELSE cleanup_state END
   WHERE stage_token=s.stage_token RETURNING * INTO s;
  v_ticket:=jsonb_build_object('stageToken',s.stage_token,'claimToken',s.claim_token,'leaseToken',v_token,
   'rowVersion',s.row_version::text,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'keepGroupIndexes',v_keep,'keepOrdinals',v_ordinals,'receiptState',s.receipt_state,'phase',v_phase,
   'resetEpoch',s.reset_epoch::text,'cleanupState',s.cleanup_state);
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token;
   v_ticket:=v_ticket||jsonb_build_object('replacement',jsonb_build_object('stageToken',child.stage_token,
    'key',child.stage_key,'sha256',child.stage_sha256,'bytes',child.stage_bytes,'groupOrdinals',child.group_ordinals));
  END IF;
  v_tickets:=v_tickets||jsonb_build_array(v_ticket);
  EXIT; -- At most ONE fully evaluated candidate per transaction, including healthy rotations.
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_tickets)t JOIN sessions.ingest_stage_intents i
   ON i.stage_token=(t->>'stageToken')::uuid WHERE i.gc_lease_token IS DISTINCT FROM (t->>'leaseToken')::uuid
    OR i.gc_lease_until<=clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: claim lease expired';END IF;
 IF octet_length(v_tickets::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='buffer gc: ticket byte bound';END IF;
 RETURN jsonb_build_object('tickets',v_tickets);
END $claim$;
-- sessions.ingest_privacy_authority_begin(jsonb): restored from 20261105041000 (exact body; CREATE OR REPLACE keeps owner and ACL).
CREATE OR REPLACE FUNCTION sessions.ingest_privacy_authority_begin(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $authority_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_kind text;v_epoch text;v_payload jsonb;d text;fresh boolean;v_deleted bigint;s jsonb;
BEGIN
 -- The writer marker the statement guard checks: transaction-local, set here (a function-level SET of a custom
 -- parameter needs superuser on PG15+) and cleared before every normal return. It is not a trust boundary.
 PERFORM set_config('bulls96.privacy_authority_writer','authority',true);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','kind','payload']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_kind:=p->>'kind';v_epoch:=p->>'epoch';v_payload:=p->'payload';
 IF v_kind IS NULL OR v_kind NOT IN('selection','reset') OR jsonb_typeof(v_payload) IS DISTINCT FROM 'object' OR octet_length(v_payload::text)>2113536 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 IF v_kind='selection' THEN
  PERFORM sessions.ingest_exact_keys(v_payload,ARRAY['origin_id','sessions','enabled']);
  PERFORM sessions.ingest_identifier(v_payload->'origin_id',false);
  IF jsonb_typeof(v_payload->'enabled') IS DISTINCT FROM 'boolean' OR jsonb_typeof(v_payload->'sessions') IS DISTINCT FROM 'array'
   OR jsonb_array_length(v_payload->'sessions') NOT BETWEEN 1 AND 2048 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid selection';END IF;
  FOR s IN SELECT value FROM jsonb_array_elements(v_payload->'sessions') LOOP
   PERFORM sessions.ingest_exact_keys(s,ARRAY['source','session_id']);
   PERFORM sessions.ingest_identifier(s->'source',false);PERFORM sessions.ingest_identifier(s->'session_id',false);
  END LOOP;
 ELSE
  PERFORM sessions.ingest_exact_keys(v_payload,ARRAY['request_id']);PERFORM sessions.ingest_identifier(v_payload->'request_id',false);
 END IF;
 PERFORM sessions.ingest_privacy_authority_lock(false);
 pol:=sessions.ingest_frozen_source_policy();
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority WHERE state='active' AND control_epoch=pol.control_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: supabase privacy authority not active';END IF;
 IF v_epoch IS DISTINCT FROM pol.control_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',v_epoch,v_org,v_ws,v_user,v_kind,v_payload)::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
  VALUES(v_org,v_ws,v_user,v_epoch,1,d,v_kind,v_payload,true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF coalesce(fresh,false) THEN
  -- PRE before the apply below can change it (Neon persists PRE before selection/reset create fences).
  UPDATE sessions.ingest_source_control_heads SET before_privacy=sessions.ingest_frozen_source_privacy(v_org,v_ws,v_user),privacy_version=0
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 IF h.epoch IS DISTINCT FROM v_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 IF NOT coalesce(fresh,false) THEN
  IF h.pending THEN
   IF h.digest IS DISTINCT FROM d OR h.kind IS DISTINCT FROM v_kind THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control pending';END IF;
   PERFORM set_config('bulls96.privacy_authority_writer','',true);
   RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,
    'deleted',h.deleted::text,'apply',false);
  END IF;
  IF h.version=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control version exhausted';END IF;
  UPDATE sessions.ingest_source_control_heads SET version=h.version+1,digest=d,kind=v_kind,payload=v_payload,pending=true,
   before_privacy=NULL,privacy_version=h.version,deleted=NULL,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 v_deleted:=sessions.ingest_privacy_authority_apply(v_org,v_ws,v_user,v_kind,v_payload);
 IF v_kind='reset' THEN
  UPDATE sessions.ingest_source_control_heads SET deleted=v_deleted WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 END IF;
 PERFORM set_config('bulls96.privacy_authority_writer','',true);
 RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,
  'deleted',v_deleted::text,'apply',true);
END $authority_begin$;
-- sessions.ingest_source_control_begin(jsonb): restored from 20261105041000 (exact body; CREATE OR REPLACE keeps owner and ACL).
CREATE OR REPLACE FUNCTION sessions.ingest_source_control_begin(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $control_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_kind text;v_epoch text;d text;fresh boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','kind','payload']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_kind:=p->>'kind';v_epoch:=p->>'epoch';
 IF v_kind IS NULL OR v_kind NOT IN('selection','reset') OR p->'payload' IS NULL OR jsonb_typeof(p->'payload')='null'
  OR octet_length((p->'payload')::text)>2113536 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 PERFORM sessions.ingest_privacy_authority_lock(false);
 IF EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: privacy authority moved; legacy control refused';END IF;
 pol:=sessions.ingest_frozen_source_policy();
 IF v_epoch IS DISTINCT FROM pol.control_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',v_epoch,v_org,v_ws,v_user,v_kind,p->'payload')::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
  VALUES(v_org,v_ws,v_user,v_epoch,1,d,v_kind,p->'payload',true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF coalesce(fresh,false) THEN
  UPDATE sessions.ingest_source_control_heads SET before_privacy=sessions.ingest_frozen_source_privacy(v_org,v_ws,v_user),privacy_version=0
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 IF h.epoch IS DISTINCT FROM v_epoch THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control epoch';END IF;
 IF NOT coalesce(fresh,false) THEN
  IF h.pending THEN
   IF h.digest IS DISTINCT FROM d OR h.kind IS DISTINCT FROM v_kind THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control pending';END IF;
   RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',false);
  END IF;
  IF h.version=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control version exhausted';END IF;
  UPDATE sessions.ingest_source_control_heads SET version=h.version+1,digest=d,kind=v_kind,payload=p->'payload',pending=true,
   before_privacy=NULL,privacy_version=h.version,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user RETURNING * INTO h;
 END IF;
 RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',true);
END $control_begin$;
DROP TRIGGER operator_user_erase_guard ON sessions.ingest_source_control_heads;
DROP FUNCTION sessions.operator_user_erase_heads_guard();
DROP FUNCTION sessions.operator_user_erase_control_gate(text,text,text);
DROP FUNCTION sessions.operator_user_erase_complete(text);
DROP FUNCTION sessions.operator_user_erase_step(text,integer);
DROP FUNCTION sessions.operator_user_erase_begin(text,text,text,text);
DROP FUNCTION sessions.operator_user_erase_purge(text,text,text,text[],boolean);
DROP FUNCTION sessions.operator_user_erase_remaining(text,text,text,text[]);
DROP FUNCTION sessions.operator_user_erase_seat_ambiguity(text,text,text,text[]);
DROP FUNCTION sessions.operator_user_erase_render(text);
DROP FUNCTION sessions.operator_user_erase_count(text,jsonb);
DROP TABLE sessions.operator_user_erasures;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105058010_operator_user_erase.sql';
COMMIT;
