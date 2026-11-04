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
 config jsonb NOT NULL CHECK(jsonb_typeof(config)='object' AND octet_length(config::text)<=4096),
 checkpoint jsonb NOT NULL CHECK(jsonb_typeof(checkpoint)='object' AND octet_length(checkpoint::text)<=65536),
 checkpoint_version bigint NOT NULL DEFAULT 0 CHECK(checkpoint_version>=0),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN('pending','complete','canceled')),
 lease_token uuid,lease_until timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX history_model_work_pending ON sessions.history_model_work(next_attempt_at,created_at,work_id) WHERE status='pending';
ALTER TABLE sessions.history_model_work ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.history_model_work FROM PUBLIC,anon,authenticated,service_role;

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
   OR (SELECT array_agg(k ORDER BY k)FROM jsonb_object_keys(r)k) IS DISTINCT FROM ARRAY['bytes','count','first','key','keyId','level','sha256']::text[]
   OR r->>'key' IS NULL OR r->>'key'!~'^[^[:space:]%?#]+/derived/bob-slice/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\.bin$'
   OR r->>'key' NOT LIKE left(a->>'derivedAnchorKey',length(a->>'derivedAnchorKey')-length('manifest.json'))||'derived/bob-slice/%'
   OR r->>'keyId' IS DISTINCT FROM cfg->>'keyId'
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
DECLARE e jsonb:=p->'evidence';w sessions.history_model_work;key text;
BEGIN
 -- Acquire SourceScope before ANY ModelWork row, including idempotent retries.
 IF sessions.ingest_history_source_current(e,true) IS NULL THEN RETURN jsonb_build_object('current',false);END IF;
 IF jsonb_typeof(p->'captured') IS DISTINCT FROM 'object' OR octet_length((p->'captured')::text)>16384
  OR jsonb_typeof(p->'config') IS DISTINCT FROM 'object' OR octet_length((p->'config')::text)>4096
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
 FOR candidate IN SELECT work_id,evidence FROM sessions.history_model_work
  WHERE status='pending' AND evidence->>'generation'=p->>'generation'
   AND config->>'model'=p->>'model'
   AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
  ORDER BY next_attempt_at,created_at,work_id LIMIT 32 LOOP
  IF sessions.ingest_history_source_current(candidate.evidence,true) IS NULL THEN
   -- Backlog cannot be starved by the same first32 stale source generations.
   -- No source authority is changed, and a live lease is never adopted.
   UPDATE sessions.history_model_work SET next_attempt_at=clock_timestamp()+interval '5 minutes'
    WHERE work_id IN(SELECT work_id FROM sessions.history_model_work
      WHERE work_id=candidate.work_id AND status='pending' AND (lease_until IS NULL OR lease_until<=clock_timestamp())
      FOR UPDATE SKIP LOCKED);
   CONTINUE;
  END IF;
  SELECT * INTO w FROM sessions.history_model_work WHERE work_id=candidate.work_id AND status='pending'
   AND evidence=candidate.evidence AND (lease_until IS NULL OR lease_until<=clock_timestamp()) FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN CONTINUE;END IF;
  token:=gen_random_uuid();
  UPDATE sessions.history_model_work SET lease_token=token,lease_until=started+make_interval(secs=>seconds),updated_at=clock_timestamp()
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
DECLARE w sessions.history_model_work;until_at timestamptz;locked jsonb;pointer jsonb;completed boolean;canceled boolean;
BEGIN
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=(p->>'workId')::uuid;
 IF NOT FOUND OR w.evidence->>'generation' IS DISTINCT FROM p->>'generation'
  OR sessions.ingest_history_source_current(w.evidence,true) IS NULL THEN RETURN jsonb_build_object('advanced',false);END IF;
 SELECT * INTO w FROM sessions.history_model_work WHERE work_id=w.work_id FOR UPDATE;
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
 completed:=(p->>'done')::boolean;
 IF completed THEN
  IF p->'next'->>'inputComplete' IS DISTINCT FROM 'true' OR jsonb_typeof(p->'pointers') IS DISTINCT FROM 'array'
   OR jsonb_array_length(p->'pointers')<>2
   OR (SELECT array_agg(x->>'kind' ORDER BY x->>'kind') FROM jsonb_array_elements(p->'pointers')x)
       IS DISTINCT FROM ARRAY['long','short']::text[]
   OR p->>'state' NOT IN('active','done','abandoned','unknown') OR p->>'classifiedAt' IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='invalid complete model metadata';END IF;
  locked:=sessions.session_history_source_lock(w.evidence);
  IF locked->>'current' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('advanced',false);END IF;
  FOR pointer IN SELECT value FROM jsonb_array_elements(p->'pointers') LOOP
   IF sessions.session_history_summary_publish(w.evidence||jsonb_build_object('pointer',pointer))->>'applied' IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model summary authority changed';END IF;
  END LOOP;
  IF sessions.session_history_state_publish(w.evidence||jsonb_build_object('state',p->>'state','classifiedAt',p->>'classifiedAt'))->>'applied' IS DISTINCT FROM 'true' THEN
   RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model state authority changed';END IF;
 END IF;
 UPDATE sessions.history_model_work SET checkpoint=p->'next',checkpoint_version=checkpoint_version+1,
  status=CASE WHEN canceled THEN 'canceled' WHEN completed THEN 'complete' ELSE 'pending' END,
  lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
 WHERE work_id=w.work_id RETURNING * INTO w;
 -- A trigger/row wait/callback may consume the lease. Raising here rolls back
 -- BOTH pointers, state and checkpoint. Never return success after expiry.
 IF until_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='RH063',MESSAGE='model checkpoint authority expired during writes';END IF;
 RETURN jsonb_build_object('advanced',true,'checkpointVersion',w.checkpoint_version::text,'complete',completed);
EXCEPTION WHEN SQLSTATE 'RH063' THEN RETURN jsonb_build_object('advanced',false);
END $checkpoint$;

-- Exact caller-role decision is a separate reviewed bootstrap prerequisite.
-- No service_role/owner credential fallback and no table grants are introduced.
DO $acl$
DECLARE f regprocedure;grantee text;t regclass:='sessions.history_model_work'::regclass;
BEGIN
 FOR f IN SELECT oid::regprocedure FROM pg_proc WHERE pronamespace='sessions'::regnamespace
  AND proname IN('ingest_history_model_ticket','ingest_history_model_checkpoint_valid','session_history_model_enqueue','session_history_model_claim','session_history_model_heartbeat','session_history_model_checkpoint') LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated,service_role',f);
  FOR grantee IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner)))a
   JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=f AND a.grantee<>p.proowner LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,grantee);END LOOP;
 END LOOP;
 FOR grantee IN SELECT r.rolname FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner)))a
  JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=t AND a.grantee<>c.relowner LOOP
  EXECUTE format('REVOKE ALL ON TABLE %s FROM %I',t,grantee);END LOOP;
END $acl$;
