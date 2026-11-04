-- Durable logical-source model continuation. Metadata ONLY; no transcript/model
-- text, raw revision seed, broad role grant, model call or cloud IO.
-- Depends on the exact same-creator620 source authority, never duplicates it.
DO $preflight$
BEGIN
 IF to_regprocedure('sessions.ingest_history_source_current(jsonb,boolean)') IS NULL
  OR (SELECT proowner FROM pg_proc WHERE oid='sessions.ingest_history_source_current(jsonb,boolean)'::regprocedure)
     IS DISTINCT FROM (SELECT oid FROM pg_roles WHERE rolname=current_user) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='model work requires same-creator history source authority';
 END IF;
END $preflight$;
CREATE TABLE sessions.history_model_work(
 work_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 work_key text NOT NULL UNIQUE CHECK(work_key~'^[0-9a-f]{64}$'),
 evidence jsonb NOT NULL CHECK(jsonb_typeof(evidence)='object'),
 captured jsonb NOT NULL CHECK(jsonb_typeof(captured)='object' AND octet_length(captured::text)<=16384),
 config jsonb NOT NULL CHECK(jsonb_typeof(config)='object' AND octet_length(config::text)<=65536),
 checkpoint jsonb NOT NULL CHECK(jsonb_typeof(checkpoint)='object' AND octet_length(checkpoint::text)<=65536),
 checkpoint_version bigint NOT NULL DEFAULT 0 CHECK(checkpoint_version>=0),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN('pending','complete','failed','canceled')),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 32),last_error_code text CHECK(last_error_code~'^[a-z0-9_]{1,64}$'),
 lease_token uuid,lease_until timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX history_model_work_pending ON sessions.history_model_work(next_attempt_at,created_at,work_id) WHERE status='pending';
ALTER TABLE sessions.history_model_work ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.history_model_work FROM PUBLIC,anon,authenticated,service_role;

-- Coalesced metadata wake AFTER complete masked graph publication, not after
-- unqualified physical-index events. Provider/config secrets never enter SQL.
CREATE TABLE sessions.history_model_wakeups(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 authority_id uuid NOT NULL,source_epoch bigint NOT NULL,source_fingerprint text NOT NULL,mask_sha256 text NOT NULL,
 wake_token uuid NOT NULL DEFAULT gen_random_uuid(),lease_until timestamptz,
 queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
CREATE UNIQUE INDEX history_model_wakeups_token ON sessions.history_model_wakeups(wake_token);
CREATE INDEX history_model_wakeups_due ON sessions.history_model_wakeups(queued_at,org_id,workspace_id,harness,session_id);
ALTER TABLE sessions.history_model_wakeups ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.history_model_wakeups FROM PUBLIC,anon,authenticated,service_role;
CREATE FUNCTION sessions.ingest_history_model_graph_wake() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $wake$
DECLARE a uuid;v sessions.ingest_history_scope_versions;
BEGIN
 IF NEW.epoch IS DISTINCT FROM NEW.published_epoch OR NEW.index_key IS NULL
  OR NEW.source_snapshot->'history'->>'required' IS DISTINCT FROM 'true'
  OR NEW.source_snapshot->'history'->>'ready' IS DISTINCT FROM 'true' THEN RETURN NEW;END IF;
 BEGIN a:=(NEW.source_snapshot->'history'->>'authorityId')::uuid;EXCEPTION WHEN invalid_text_representation THEN RETURN NEW;END;
 SELECT h.* INTO v FROM sessions.ingest_history_scope_versions h JOIN sessions.ingest_history_scope_current c ON c.authority_id=h.authority_id
  WHERE h.authority_id=a AND c.ready AND h.state='ready' AND h.org_id=NEW.org_id AND h.workspace_id=NEW.workspace_id
   AND h.harness=NEW.harness AND h.session_id=NEW.session_id AND h.source_epoch=NEW.epoch
   AND h.source_fingerprint=NEW.source_fingerprint;
 IF NOT FOUND THEN RETURN NEW;END IF;
 INSERT INTO sessions.history_model_wakeups AS q(org_id,workspace_id,harness,session_id,authority_id,source_epoch,source_fingerprint,mask_sha256)
  VALUES(v.org_id,v.workspace_id,v.harness,v.session_id,v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256)
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET authority_id=excluded.authority_id,source_epoch=excluded.source_epoch,
  source_fingerprint=excluded.source_fingerprint,mask_sha256=excluded.mask_sha256,wake_token=gen_random_uuid(),lease_until=NULL,queued_at=clock_timestamp()
 WHERE ROW(q.authority_id,q.source_epoch,q.source_fingerprint,q.mask_sha256)
  IS DISTINCT FROM ROW(excluded.authority_id,excluded.source_epoch,excluded.source_fingerprint,excluded.mask_sha256);
 RETURN NEW;
END $wake$;
CREATE TRIGGER history_model_graph_wake AFTER INSERT OR UPDATE ON sessions.session_replay_publications
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_history_model_graph_wake();
-- One metadata-only initial population from exact already-qualified graphs.
-- Future graph publications coalesce through the trigger. No raw/body backfill.
INSERT INTO sessions.history_model_wakeups(org_id,workspace_id,harness,session_id,authority_id,source_epoch,source_fingerprint,mask_sha256)
 SELECT v.org_id,v.workspace_id,v.harness,v.session_id,v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256
 FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v ON v.authority_id=c.authority_id
 JOIN sessions.session_replay_publications a ON a.org_id=v.org_id AND a.workspace_id=v.workspace_id AND a.harness=v.harness AND a.session_id=v.session_id
 WHERE c.ready AND v.state='ready' AND a.epoch=a.published_epoch AND v.source_epoch=a.epoch
  AND v.source_fingerprint=a.source_fingerprint AND a.index_key IS NOT NULL
  AND a.source_snapshot#>>'{history,ready}'='true' AND a.source_snapshot#>>'{history,authorityId}'=v.authority_id::text
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO NOTHING;

CREATE FUNCTION sessions.session_history_model_discover(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $discover$
DECLARE candidate record;q sessions.history_model_wakeups;e jsonb;items jsonb:='[]'::jsonb;started timestamptz:=clock_timestamp();
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 -- A schema upgrade never adopts an old lease/frontier. Once expired, an old
 -- PENDING row is only a bounded scope hint: lock/recheck its original source
 -- under this generation, cancel that exact old work, then create a fresh wake.
 -- Completed/terminal failures remain unchanged. No whole-corpus read/scan.
 FOR candidate IN SELECT work_id,evidence FROM sessions.history_model_work
  WHERE status='pending' AND evidence->>'generation' IS DISTINCT FROM p->>'generation'
   AND(lease_until IS NULL OR lease_until<=clock_timestamp())
  ORDER BY next_attempt_at,created_at,work_id LIMIT 32 LOOP
  e:=jsonb_set(candidate.evidence,'{generation}',p->'generation',false);
  IF sessions.ingest_history_source_current(e,true) IS NULL THEN
   UPDATE sessions.history_model_work SET status='canceled',last_error_code='source_or_graph_changed',
    lease_token=NULL,lease_until=NULL,checkpoint_version=checkpoint_version+1
    WHERE work_id IN(SELECT work_id FROM sessions.history_model_work WHERE work_id=candidate.work_id
     AND status='pending' AND evidence=candidate.evidence
     AND(lease_until IS NULL OR lease_until<=clock_timestamp())FOR UPDATE SKIP LOCKED);
   CONTINUE;
  END IF;
  UPDATE sessions.history_model_work SET status='canceled',last_error_code='schema_generation_changed',
   lease_token=NULL,lease_until=NULL,checkpoint_version=checkpoint_version+1
   WHERE work_id IN(SELECT work_id FROM sessions.history_model_work WHERE work_id=candidate.work_id
    AND status='pending' AND evidence=candidate.evidence
    AND(lease_until IS NULL OR lease_until<=clock_timestamp())FOR UPDATE SKIP LOCKED);
  IF NOT FOUND THEN CONTINUE;END IF;
  INSERT INTO sessions.history_model_wakeups(org_id,workspace_id,harness,session_id,authority_id,source_epoch,source_fingerprint,mask_sha256)
   VALUES(e->'identity'->>'orgId',e->'identity'->>'workspaceId',e->'identity'->>'source',e->'identity'->>'sessionId',
    (e->>'authorityId')::uuid,(e->'source'->>'epoch')::bigint,e->'source'->>'fingerprint',e->>'maskSha256')
   ON CONFLICT(org_id,workspace_id,harness,session_id)DO NOTHING;
 END LOOP;
 FOR candidate IN SELECT * FROM sessions.history_model_wakeups WHERE lease_until IS NULL OR lease_until<=clock_timestamp()
  ORDER BY queued_at,org_id,workspace_id,harness,session_id LIMIT 32 LOOP
  e:=jsonb_build_object('generation',p->>'generation','identity',jsonb_build_object('orgId',candidate.org_id,'workspaceId',candidate.workspace_id,'source',candidate.harness,'sessionId',candidate.session_id),
   'source',jsonb_build_object('epoch',candidate.source_epoch::text,'fingerprint',candidate.source_fingerprint),'authorityId',candidate.authority_id,'maskSha256',candidate.mask_sha256);
  IF sessions.ingest_history_source_current(e,true) IS NULL THEN
   DELETE FROM sessions.history_model_wakeups WHERE wake_token=candidate.wake_token;
   CONTINUE;
  END IF;
  SELECT * INTO q FROM sessions.history_model_wakeups WHERE wake_token=candidate.wake_token
   AND(lease_until IS NULL OR lease_until<=clock_timestamp())FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN CONTINUE;END IF;
  UPDATE sessions.history_model_wakeups SET lease_until=started+interval '60 seconds' WHERE wake_token=q.wake_token;
  items:=items||jsonb_build_array(jsonb_build_object('wakeToken',q.wake_token,'identity',e->'identity','source',e->'source','authorityId',q.authority_id,'maskSha256',q.mask_sha256));
  IF jsonb_array_length(items)>=20 THEN EXIT;END IF;
 END LOOP;
 IF started+interval '60 seconds'<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model discovery lease expired';END IF;
 RETURN jsonb_build_object('scopes',items);
EXCEPTION WHEN SQLSTATE 'RH063'THEN RETURN jsonb_build_object('scopes','[]'::jsonb);
END $discover$;

-- Exact metadata grammar prevents provider text or unbounded frontiers being
-- smuggled into the durable row. Final summaries live only in encrypted objects.
CREATE FUNCTION sessions.ingest_history_model_checkpoint_valid(c jsonb,a jsonb,cfg jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $valid$
DECLARE r jsonb;refs jsonb;cursor_at numeric:=0;first_at numeric;count_at numeric;names text[];
BEGIN
 IF jsonb_typeof(c) IS DISTINCT FROM 'object' OR octet_length(c::text)>65536
  OR (SELECT array_agg(k ORDER BY k)FROM jsonb_object_keys(c)k) IS DISTINCT FROM ARRAY['actualModel','calls','carry','configSha256','cursor','frontier','inputComplete','protocol','sourceBinding','tokensIn','tokensOut','units']::text[]
  OR NOT(c->'actualModel'='null'::jsonb OR jsonb_typeof(c->'actualModel')='string' AND length(c->>'actualModel') BETWEEN 1 AND 200)
  OR c->>'protocol' IS DISTINCT FROM 'bob-full-work-v1' OR c->>'configSha256' IS DISTINCT FROM cfg->>'protocolSha256'
  OR c->>'sourceBinding' IS NULL OR c->>'sourceBinding'!~'^[0-9a-f]{64}$'
  OR jsonb_typeof(c->'inputComplete') IS DISTINCT FROM 'boolean'
  OR jsonb_typeof(c->'frontier') IS DISTINCT FROM 'array' OR jsonb_array_length(c->'frontier')>63
  OR jsonb_typeof(c->'cursor') IS DISTINCT FROM 'object'
  OR c->'cursor'->>'rowOrdinal' IS NULL OR c->'cursor'->>'rowOrdinal'!~'^(0|[1-9][0-9]{0,18})$'
  OR jsonb_typeof(c->'cursor'->'utf16Offset') IS DISTINCT FROM 'number'
  OR c->'cursor'->>'utf16Offset'!~'^[0-9]{1,9}$'
  OR (c->'cursor'->>'utf16Offset')::numeric>67108864 THEN RETURN false;END IF;
 SELECT array_agg(k ORDER BY k) INTO names FROM jsonb_object_keys(c->'cursor')k;
 IF names IS DISTINCT FROM ARRAY['rowOrdinal','utf16Offset']::text[] AND names IS DISTINCT FROM ARRAY['rowOrdinal','rowSha256','utf16Offset']::text[] THEN RETURN false;END IF;
 IF (c->'cursor'->>'utf16Offset')::numeric>0 AND (c->'cursor'->>'rowSha256' IS NULL OR c->'cursor'->>'rowSha256'!~'^[0-9a-f]{64}$') THEN RETURN false;END IF;
 FOREACH names SLICE 1 IN ARRAY ARRAY[['units'],['calls'],['tokensIn'],['tokensOut']] LOOP
  IF c->>names[1] IS NULL OR c->>names[1]!~'^(0|[1-9][0-9]{0,18})$' OR (c->>names[1])::numeric>9223372036854775807 THEN RETURN false;END IF;
 END LOOP;
 IF c->'carry'='null'::jsonb THEN refs:=c->'frontier';
 ELSIF jsonb_typeof(c->'carry')='object' THEN refs:=(c->'frontier')||jsonb_build_array(c->'carry');
 ELSE RETURN false;END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(refs) ORDER BY(value->>'first')::numeric LOOP
  IF jsonb_typeof(r) IS DISTINCT FROM 'object'
   OR (SELECT array_agg(k ORDER BY k)FROM jsonb_object_keys(r)k) IS DISTINCT FROM ARRAY['bytes','count','first','id','level','sha256']::text[]
   OR r->>'id' IS NULL OR r->>'id'!~'^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
   OR r->>'sha256' IS NULL OR r->>'sha256'!~'^[0-9a-f]{64}$'
   OR jsonb_typeof(r->'bytes') IS DISTINCT FROM 'number' OR r->>'bytes'!~'^[0-9]{1,7}$'
   OR (r->>'bytes')::numeric NOT BETWEEN 1 AND 1048576
   OR jsonb_typeof(r->'level') IS DISTINCT FROM 'number' OR r->>'level'!~'^[0-9]{1,3}$' OR (r->>'level')::numeric>126
   OR r->>'first' IS NULL OR r->>'first'!~'^(0|[1-9][0-9]{0,18})$'
   OR r->>'count' IS NULL OR r->>'count'!~'^[1-9][0-9]{0,18}$' THEN RETURN false;END IF;
  first_at:=(r->>'first')::numeric;count_at:=(r->>'count')::numeric;
  IF first_at<>cursor_at THEN RETURN false;END IF;cursor_at:=cursor_at+count_at;
 END LOOP;
 RETURN cursor_at=(c->>'units')::numeric;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $valid$;

CREATE FUNCTION sessions.ingest_history_model_ticket(w sessions.history_model_work)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $ticket$
 SELECT jsonb_build_object('version',1,'workId',w.work_id,'leaseToken',w.lease_token,
  'leaseUntil',to_char(w.lease_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'checkpointVersion',w.checkpoint_version::text,'captured',w.captured,'config',w.config,'checkpoint',w.checkpoint)
$ticket$;

CREATE FUNCTION sessions.session_history_model_enqueue(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue$
DECLARE e jsonb:=p->'evidence';w sessions.history_model_work;key text;g jsonb;v sessions.ingest_history_scope_versions;captured jsonb;
BEGIN
 -- Acquire SourceScope before ANY ModelWork row, including idempotent retries.
 v:=sessions.ingest_history_source_current(e,true);
 IF v IS NULL THEN RETURN jsonb_build_object('current',false);END IF;
 captured:=jsonb_build_object('identity',e->'identity','source',e->'source','authorityId',e->'authorityId','maskSha256',e->'maskSha256',
  'recordCount',v.record_count::text,'derivedAnchorKey',v.derived_anchor_key,'schemaGeneration',e->>'generation');
 g:=sessions.session_history_replay_descriptor(e);
 IF g->>'current' IS DISTINCT FROM 'true' OR jsonb_typeof(p->'config'->'graph') IS DISTINCT FROM 'object' OR g->'publication' IS DISTINCT FROM p->'config'->'graph' THEN RETURN jsonb_build_object('current',false);END IF;
 IF p->'captured' IS DISTINCT FROM captured OR jsonb_typeof(p->'captured') IS DISTINCT FROM 'object' OR octet_length((p->'captured')::text)>16384
  OR(SELECT array_agg(k ORDER BY k)FROM jsonb_object_keys(p->'config')k)IS DISTINCT FROM ARRAY['graph','keyId','model','protocolSha256']::text[]
  OR jsonb_typeof(p->'config') IS DISTINCT FROM 'object' OR octet_length((p->'config')::text)>65536
  OR p->'config'->>'protocolSha256' IS NULL OR p->'config'->>'protocolSha256'!~'^[0-9a-f]{64}$'
  OR coalesce(p->'config'->>'model','')='' OR coalesce(p->'config'->>'keyId','')=''
  OR jsonb_typeof(p->'checkpoint') IS DISTINCT FROM 'object' OR octet_length((p->'checkpoint')::text)>65536
  OR p->'checkpoint'->>'configSha256' IS DISTINCT FROM p->'config'->>'protocolSha256'
  OR p->'captured'->'identity' IS DISTINCT FROM e->'identity'
  OR p->'captured'->'source' IS DISTINCT FROM e->'source'
  OR p->'captured'->>'authorityId' IS DISTINCT FROM e->>'authorityId'
  OR p->'captured'->>'maskSha256' IS DISTINCT FROM e->>'maskSha256'
  OR p->'captured'->>'schemaGeneration' IS DISTINCT FROM e->>'generation'
  OR NOT sessions.ingest_history_model_checkpoint_valid(p->'checkpoint',p->'captured',p->'config')
  OR p->'checkpoint'->>'units' IS DISTINCT FROM '0'
  OR p->'checkpoint'->>'calls' IS DISTINCT FROM '0'
  OR p->'checkpoint'->'cursor' IS DISTINCT FROM '{"rowOrdinal":"0","utf16Offset":0}'::jsonb
  OR p->'checkpoint'->>'inputComplete' IS DISTINCT FROM 'false' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid model work metadata';
 END IF;
 key:=encode(sha256(convert_to(jsonb_build_array('history-model-work-v1',e,p->'config')::text,'UTF8')),'hex');
 INSERT INTO sessions.history_model_work(work_key,evidence,captured,config,checkpoint)
  VALUES(key,e,p->'captured',p->'config',p->'checkpoint') ON CONFLICT(work_key)DO NOTHING;
 SELECT * INTO STRICT w FROM sessions.history_model_work WHERE work_key=key FOR UPDATE;
 IF p->>'wakeToken' IS NOT NULL THEN
  DELETE FROM sessions.history_model_wakeups WHERE wake_token=(p->>'wakeToken')::uuid
   AND org_id=e->'identity'->>'orgId' AND workspace_id=e->'identity'->>'workspaceId'
   AND harness=e->'identity'->>'source' AND session_id=e->'identity'->>'sessionId'
   AND authority_id::text=e->>'authorityId' AND source_epoch::text=e->'source'->>'epoch'
   AND source_fingerprint=e->'source'->>'fingerprint' AND mask_sha256=e->>'maskSha256';
 END IF;
 -- Do not overwrite retained checkpoint/captured source on wake or crash retry.
 RETURN jsonb_build_object('current',true,'workId',w.work_id,'status',w.status);
END $enqueue$;

CREATE FUNCTION sessions.session_history_model_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE candidate record;w sessions.history_model_work;seconds integer;token uuid;started timestamptz:=clock_timestamp();
BEGIN
 IF p->>'model' IS NULL OR length(p->>'model') NOT BETWEEN 1 AND 200 OR p->>'generation' IS NULL OR p->>'generation'!~'^[0-9a-f]{64}$' OR p->>'leaseSeconds' IS NULL
  OR p->>'leaseSeconds'!~'^[0-9]{1,3}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid model claim';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 seconds:=(p->>'leaseSeconds')::integer;
 IF seconds NOT BETWEEN 30 AND 120 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid model lease';END IF;
 -- No ModelWork lock is held while obtaining SourceScope. Bounded candidate
 -- scan; stale authorities are deferred, never mistaken for current/complete.
 FOR candidate IN SELECT work_id,evidence,config FROM sessions.history_model_work
  WHERE status='pending' AND evidence->>'generation'=p->>'generation'
   AND config->>'model'=p->>'model'
   AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
  ORDER BY next_attempt_at,created_at,work_id LIMIT 32 LOOP
  IF sessions.ingest_history_source_current(candidate.evidence,true) IS NULL OR sessions.session_history_replay_descriptor(candidate.evidence)->'publication' IS DISTINCT FROM candidate.config->'graph' THEN
   -- Backlog cannot be starved by the same first32 stale source generations.
   -- No source authority is changed, and a live lease is never adopted.
   UPDATE sessions.history_model_work SET status='canceled',last_error_code='source_or_graph_changed',next_attempt_at=clock_timestamp()+interval '5 minutes'
    WHERE work_id IN(SELECT work_id FROM sessions.history_model_work
      WHERE work_id=candidate.work_id AND status='pending' AND (lease_until IS NULL OR lease_until<=clock_timestamp())
      FOR UPDATE SKIP LOCKED);
   CONTINUE;
  END IF;
  SELECT * INTO w FROM sessions.history_model_work WHERE work_id=candidate.work_id AND status='pending'
   AND evidence=candidate.evidence AND (lease_until IS NULL OR lease_until<=clock_timestamp()) FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN CONTINUE;END IF;
  IF w.attempts>=12 THEN
   UPDATE sessions.history_model_work SET status='failed',last_error_code='model_attempts_exhausted',lease_token=NULL,lease_until=NULL WHERE work_id=w.work_id;
   CONTINUE;
  END IF;
  token:=gen_random_uuid();
  UPDATE sessions.history_model_work SET attempts=attempts+1,lease_token=token,lease_until=started+make_interval(secs=>seconds),updated_at=clock_timestamp()
   WHERE work_id=w.work_id RETURNING * INTO w;
  IF w.lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model lease expired during claim';END IF;
  RETURN jsonb_build_object('ticket',sessions.ingest_history_model_ticket(w));
 END LOOP;
 RETURN jsonb_build_object('ticket',null);
EXCEPTION WHEN SQLSTATE 'RH063' THEN RETURN jsonb_build_object('ticket',null);
END $claim$;

CREATE FUNCTION sessions.session_history_model_heartbeat(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE w sessions.history_model_work;started timestamptz:=clock_timestamp();prior_until timestamptz;
BEGIN
 -- Original immutable evidence comes from the row, never new client context.
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=(p->>'workId')::uuid;
 IF NOT FOUND OR w.evidence->>'generation' IS DISTINCT FROM p->>'generation'
  OR sessions.ingest_history_source_current(w.evidence,true) IS NULL THEN RETURN jsonb_build_object('renewed',false);END IF;
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=w.work_id FOR UPDATE;
 IF sessions.session_history_replay_descriptor(w.evidence)->'publication' IS DISTINCT FROM w.config->'graph' THEN RETURN jsonb_build_object('renewed',false);END IF;
 IF w.status<>'pending' OR w.lease_token IS DISTINCT FROM (p->>'leaseToken')::uuid
  OR w.checkpoint_version::text IS DISTINCT FROM p->>'checkpointVersion'
  OR w.lease_until IS NULL OR w.lease_until<=clock_timestamp() THEN RETURN jsonb_build_object('renewed',false);END IF;
 prior_until:=w.lease_until;
 UPDATE sessions.history_model_work SET lease_until=started+interval '120 seconds',updated_at=clock_timestamp()
  WHERE work_id=w.work_id RETURNING * INTO w;
 IF prior_until<=clock_timestamp() OR w.lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model heartbeat expired during write';END IF;
 RETURN jsonb_build_object('renewed',true,'leaseUntil',to_char(w.lease_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
EXCEPTION WHEN SQLSTATE 'RH063' THEN RETURN jsonb_build_object('renewed',false);
END $heartbeat$;

CREATE FUNCTION sessions.session_history_model_checkpoint(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $checkpoint$
DECLARE w sessions.history_model_work;until_at timestamptz;locked jsonb;pointer jsonb;completed boolean;canceled boolean;failed boolean;retryable boolean;error_code text;
BEGIN
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=(p->>'workId')::uuid;
 IF NOT FOUND OR w.evidence->>'generation' IS DISTINCT FROM p->>'generation'
  OR sessions.ingest_history_source_current(w.evidence,true) IS NULL THEN RETURN jsonb_build_object('advanced',false);END IF;
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=w.work_id FOR UPDATE;
 IF sessions.session_history_replay_descriptor(w.evidence)->'publication' IS DISTINCT FROM w.config->'graph' THEN RETURN jsonb_build_object('advanced',false);END IF;
 IF w.status<>'pending' OR w.lease_token IS DISTINCT FROM (p->>'leaseToken')::uuid
  OR w.checkpoint_version::text IS DISTINCT FROM p->>'checkpointVersion'
  OR w.lease_until IS NULL OR w.lease_until<=clock_timestamp() THEN RETURN jsonb_build_object('advanced',false);END IF;
 until_at:=w.lease_until;
 IF NOT sessions.ingest_history_model_checkpoint_valid(p->'next',w.captured,w.config) OR jsonb_typeof(p->'next') IS DISTINCT FROM 'object' OR octet_length((p->'next')::text)>65536
  OR p->'next'->>'sourceBinding' IS DISTINCT FROM w.checkpoint->>'sourceBinding'
  OR p->'next'->>'configSha256' IS DISTINCT FROM w.config->>'protocolSha256'
  OR p->'next'->>'protocol' IS DISTINCT FROM w.checkpoint->>'protocol'
  OR jsonb_typeof(p->'done') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid model checkpoint';
 END IF;
 canceled:=coalesce((p->>'cancel')::boolean,false);
 IF canceled AND ((p->>'done')::boolean OR p->>'cancelReason' IS DISTINCT FROM 'config_changed' OR p->'next' IS DISTINCT FROM w.checkpoint) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid model cancel';END IF;
 failed:=p->'error' IS NOT NULL;
 IF failed THEN
  IF (p->>'done')::boolean OR canceled OR p->'next' IS DISTINCT FROM w.checkpoint
   OR jsonb_typeof(p->'error') IS DISTINCT FROM 'object' OR p->'error'->>'code' IS NULL
   OR p->'error'->>'code'!~'^[a-z0-9_]{1,64}$' OR jsonb_typeof(p->'error'->'retryable') IS DISTINCT FROM 'boolean' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid model failure';END IF;
  error_code:=p->'error'->>'code';retryable:=(p->'error'->>'retryable')::boolean;
 END IF;
 completed:=(p->>'done')::boolean;
 IF completed THEN
  IF p->'next'->>'inputComplete' IS DISTINCT FROM 'true' OR jsonb_typeof(p->'pointers') IS DISTINCT FROM 'array'
   OR jsonb_array_length(p->'pointers')<>2
   OR (SELECT array_agg(x->>'kind' ORDER BY x->>'kind') FROM jsonb_array_elements(p->'pointers')x)
       IS DISTINCT FROM ARRAY['long','short']::text[]
   OR NOT(p->'state'='null'::jsonb OR p->>'state' IN('active','done','abandoned','unknown')) OR p->>'classifiedAt' IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid complete model metadata';END IF;
  locked:=sessions.session_history_source_lock(w.evidence);
  IF locked->>'current' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('advanced',false);END IF;
  FOR pointer IN SELECT value FROM jsonb_array_elements(p->'pointers') LOOP
   IF sessions.session_history_summary_publish(w.evidence||jsonb_build_object('pointer',pointer))->>'applied' IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model summary authority changed';END IF;
  END LOOP;
  IF p->'state'<>'null'::jsonb AND sessions.session_history_state_publish(w.evidence||jsonb_build_object('state',p->>'state','classifiedAt',p->>'classifiedAt'))->>'applied' IS DISTINCT FROM 'true' THEN
   RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model state authority changed';END IF;
 END IF;
 UPDATE sessions.history_model_work SET checkpoint=p->'next',checkpoint_version=checkpoint_version+1,
  status=CASE WHEN canceled THEN 'canceled' WHEN completed THEN 'complete' WHEN failed AND(NOT retryable OR attempts>=12)THEN 'failed' ELSE 'pending' END,
  attempts=CASE WHEN failed THEN attempts ELSE 0 END,last_error_code=CASE WHEN failed THEN error_code ELSE NULL END,
  lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()+CASE WHEN failed AND retryable THEN make_interval(secs=>least(300,5*power(2,least(attempts,6)))::integer)ELSE interval '0 seconds'END,updated_at=clock_timestamp()
 WHERE work_id=w.work_id RETURNING * INTO w;
 -- A trigger/row wait/callback may consume the lease. Raising here rolls back
 -- BOTH pointers, state and checkpoint. Never return success after expiry.
 IF until_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model checkpoint authority expired during writes';END IF;
 RETURN jsonb_build_object('advanced',true,'checkpointVersion',w.checkpoint_version::text,'complete',completed);
EXCEPTION WHEN SQLSTATE 'RH063' THEN RETURN jsonb_build_object('advanced',false);
END $checkpoint$;

-- Explicit operator retry through a reviewed source entry, not table surgery.
-- Resume only the SAME still-current source/graph/config; old leases are revoked.
CREATE FUNCTION sessions.ingest_history_model_retry(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $retry$
DECLARE w sessions.history_model_work;
BEGIN
 IF current_user IS DISTINCT FROM(SELECT r.rolname FROM pg_proc f JOIN pg_roles r ON r.oid=f.proowner WHERE f.oid='sessions.ingest_history_model_retry(jsonb)'::regprocedure)THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='model retry requires coherent creator';END IF;
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=(p->>'workId')::uuid;
 IF NOT FOUND OR w.evidence->>'generation'IS DISTINCT FROM p->>'generation' OR sessions.ingest_history_source_current(w.evidence,true)IS NULL THEN RETURN jsonb_build_object('retried',false);END IF;
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=w.work_id FOR UPDATE;
 IF w.status NOT IN('failed','canceled')OR sessions.session_history_replay_descriptor(w.evidence)->'publication'IS DISTINCT FROM w.config->'graph'THEN RETURN jsonb_build_object('retried',false);END IF;
 UPDATE sessions.history_model_work SET status='pending',attempts=0,last_error_code=NULL,checkpoint_version=checkpoint_version+1,lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()WHERE work_id=w.work_id;
 RETURN jsonb_build_object('retried',true);
END $retry$;

-- Exact caller-role decision is a separate reviewed bootstrap prerequisite.
-- No service_role/owner credential fallback and no table grants are introduced.
DO $acl$
DECLARE f regprocedure;grantee text;t regclass:='sessions.history_model_work'::regclass;
BEGIN
 FOR f IN SELECT oid::regprocedure FROM pg_proc WHERE pronamespace='sessions'::regnamespace
  AND proname IN('ingest_history_model_retry','ingest_history_model_graph_wake','session_history_model_discover','ingest_history_model_ticket','ingest_history_model_checkpoint_valid','session_history_model_enqueue','session_history_model_claim','session_history_model_heartbeat','session_history_model_checkpoint') LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated,service_role',f);
  FOR grantee IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner)))a
   JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=f AND a.grantee<>p.proowner LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,grantee);END LOOP;
 END LOOP;
 FOR grantee IN SELECT r.rolname FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner)))a
  JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=t AND a.grantee<>c.relowner LOOP
  EXECUTE format('REVOKE ALL ON TABLE %s FROM %I',t,grantee);END LOOP;
END $acl$;

-- BEGIN GENERATED MODEL CAPABILITIES
-- PROPOSED source contract, not an operator approval. Bare role must be created
-- through its reviewed privileged prerequisite; this migrator never creates it.
DO $model_grants$
DECLARE r pg_roles%ROWTYPE;f text;p record;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_session_model';
 IF NOT FOUND OR r.rolsuper OR r.rolinherit OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication
  OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='model role prerequisite absent or unsafe';END IF;
 FOREACH f IN ARRAY ARRAY['sessions.session_history_capture(jsonb)','sessions.session_history_model_checkpoint(jsonb)','sessions.session_history_model_claim(jsonb)','sessions.session_history_model_enqueue(jsonb)','sessions.session_history_model_discover(jsonb)','sessions.session_history_model_heartbeat(jsonb)','sessions.session_history_register_object(jsonb)','sessions.session_history_replay_descriptor(jsonb)']::text[] LOOP
  SELECT proowner,prosecdef INTO p FROM pg_proc WHERE oid=to_regprocedure(f);
  IF NOT FOUND OR NOT p.prosecdef OR p.proowner IS DISTINCT FROM(SELECT oid FROM pg_roles WHERE rolname=current_user)
   OR p.proowner=r.oid THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='model RPC creator mismatch';END IF;
 END LOOP;
END $model_grants$;
GRANT USAGE ON SCHEMA sessions TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_capture(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_model_checkpoint(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_model_claim(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_model_enqueue(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_model_discover(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_model_heartbeat(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_register_object(jsonb) TO bulls96_session_model;
GRANT EXECUTE ON FUNCTION sessions.session_history_replay_descriptor(jsonb) TO bulls96_session_model;
-- END GENERATED MODEL CAPABILITIES
