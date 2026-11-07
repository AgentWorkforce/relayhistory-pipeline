-- One complete GC retention decision per transaction; no schema/ACL expansion.
-- Busy acquisitions are conservative skips with no checked/lease advancement.
-- Same retention, privacy, ACK, row-version and generation authority as G2.
-- Released claim(limit) still receives <=limit tickets; every call returns <=1.
-- A new derived generation and normal coordinated rollout are required.

CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE v_limit int;v_generation text;candidate record;s sessions.ingest_stage_intents%ROWTYPE;
 child sessions.ingest_stage_intents%ROWTYPE;parent sessions.ingest_stage_intents%ROWTYPE;
 v_keep jsonb;v_ordinals jsonb;v_phase text;v_ticket jsonb;v_tickets jsonb:='[]';v_has_outbox boolean;v_abort boolean;v_token uuid;v_outbox_version bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
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
