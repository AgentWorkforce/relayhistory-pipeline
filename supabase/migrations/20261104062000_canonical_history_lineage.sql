-- Canonical History lineage. This source remains dormant until the full source
-- frontier, logical consumers and permanent retirement transport are proven.
--040600 remains provenance authority for generation object IO, not History moves.
DO $creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid IN('sessions.ingest_require_generation(text)'::regprocedure,
  'sessions.ingest_lazy_stage_seats(uuid)'::regprocedure,'sessions.ingest_publication_claim(jsonb)'::regprocedure,
  'sessions.session_replay_source_snapshot(text,text,text,text)'::regprocedure,
  'sessions.publish_session_replay_pages(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer)'::regprocedure,
  'sessions.publish_session_large_read(text,text,text,text,bigint,text,jsonb,text,text,text,text,text,integer,uuid)'::regprocedure) AND proowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: original coherent creator required';END IF;
END $creator$;

CREATE SEQUENCE sessions.ingest_history_inventory_epoch AS bigint NO CYCLE;
CREATE TABLE sessions.ingest_history_carriers(
 carrier_sha256 text PRIMARY KEY CHECK(carrier_sha256~'^[a-f0-9]{64}$'),
 storage_provider text NOT NULL,bucket text NOT NULL,physical_root text NOT NULL,layout_known boolean NOT NULL,
 epoch bigint NOT NULL DEFAULT nextval('sessions.ingest_history_inventory_epoch'),scanned_epoch bigint,
 scan_token uuid,scan_lease_token uuid,scan_lease_until timestamptz,scan_generation text,
 next_page bigint NOT NULL DEFAULT 0,scanned_records bigint NOT NULL DEFAULT 0 CHECK(scanned_records BETWEEN 0 AND 2000000),
 scan_complete boolean NOT NULL DEFAULT false,scan_descriptor_sha256 text,
 UNIQUE(storage_provider,bucket,physical_root),
 CHECK((scan_token IS NULL)=(scan_lease_token IS NULL)),
 CHECK((scan_token IS NULL)=(scan_lease_until IS NULL))
);
CREATE INDEX ingest_history_unscanned ON sessions.ingest_history_carriers(carrier_sha256)
 WHERE scanned_epoch IS DISTINCT FROM epoch OR NOT scan_complete;
CREATE TABLE sessions.ingest_history_pointer_refs(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,machine_id text NOT NULL,
 carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers ON DELETE RESTRICT,
 pointer jsonb NOT NULL CHECK(jsonb_typeof(pointer)='object'),pointer_sha256 text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id,machine_id)
);
CREATE INDEX ingest_history_pointer_root ON sessions.ingest_history_pointer_refs(carrier_sha256);
CREATE TABLE sessions.ingest_history_scan_refs(
 scan_token uuid NOT NULL,ref_ordinal bigint NOT NULL,carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers,
 pointer jsonb NOT NULL,pointer_sha256 text NOT NULL,PRIMARY KEY(scan_token,ref_ordinal)
);
CREATE TABLE sessions.ingest_history_scan_pages(
 scan_token uuid NOT NULL,page bigint NOT NULL,page_sha256 text NOT NULL CHECK(page_sha256~'^[a-f0-9]{64}$'),
 records bigint NOT NULL CHECK(records>=0),PRIMARY KEY(scan_token,page)
);
CREATE TABLE sessions.ingest_history_native_locations(
 carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers,
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
 max_revision bigint NOT NULL CHECK(max_revision>0),records bigint NOT NULL CHECK(records>0),
 PRIMARY KEY(carrier_sha256,org_id,workspace_id,origin_id,record_id)
);
CREATE INDEX ingest_history_native_lookup ON sessions.ingest_history_native_locations(org_id,workspace_id,origin_id,record_id,carrier_sha256);
ALTER TABLE sessions.ingest_history_carriers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_pointer_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scan_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scan_pages ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_native_locations ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_carriers,sessions.ingest_history_pointer_refs,sessions.ingest_history_scan_refs,
 sessions.ingest_history_scan_pages,sessions.ingest_history_native_locations FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
REVOKE ALL ON SEQUENCE sessions.ingest_history_inventory_epoch FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- Physical root, NOT logical source/session, identifies shared raw chunk keys.
CREATE FUNCTION sessions.ingest_history_root(p_key text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $root$
 SELECT (regexp_match(p_key,'^(.*/v1/ws=[A-Za-z0-9._-]+/m=[A-Za-z0-9._-]+/h=[A-Za-z0-9._-]+/d=[0-9]{4}/[0-9]{2}/[0-9]{2}/s=[A-Za-z0-9._-]+/)(?:manifest[.]json|publications/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}/manifest[.]json)$'))[1]
$root$;
CREATE FUNCTION sessions.ingest_history_pointer(p sessions.session_index) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $pointer$
 -- indexed_at/fanout operational clocks do not change admitted raw authority.
 SELECT to_jsonb(p)-ARRAY['indexed_at','state','state_classified_at']
$pointer$;
CREATE FUNCTION sessions.ingest_history_sync_pointer() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $sync$
DECLARE r sessions.session_index%ROWTYPE;old_key text;v_root text;v_key text;v_pointer jsonb;v_known boolean;
BEGIN
 IF TG_OP<>'INSERT' THEN
  SELECT carrier_sha256 INTO old_key FROM sessions.ingest_history_pointer_refs
   WHERE org_id=OLD.org_id AND workspace_id=OLD.workspace_id AND harness=OLD.harness AND session_id=OLD.session_id AND machine_id=OLD.machine_id;
 END IF;
 IF TG_OP='UPDATE' AND sessions.ingest_history_pointer(OLD)=sessions.ingest_history_pointer(NEW) THEN RETURN NEW;END IF;
 IF TG_OP<>'INSERT' THEN
  DELETE FROM sessions.ingest_history_pointer_refs WHERE org_id=OLD.org_id AND workspace_id=OLD.workspace_id
   AND harness=OLD.harness AND session_id=OLD.session_id AND machine_id=OLD.machine_id;
  UPDATE sessions.ingest_history_carriers SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false
   WHERE carrier_sha256=old_key;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;
 r:=NEW;
 -- Keep tombstoned/nonpublic refs too; they can still carry physical authority.
 -- Unknown layouts are recorded as unscannable, not omitted or imposed as a new
 -- failure on the dormant legacy indexer. They block full inventory admission.
 IF r.manifest_key IS NULL OR r.storage_provider IS NULL OR r.bucket IS NULL THEN RETURN NEW;END IF;
 v_root:=sessions.ingest_history_root(r.manifest_key);
 v_known:=v_root IS NOT NULL;v_root:=coalesce(v_root,r.manifest_key);
 v_key:=encode(sha256(convert_to(jsonb_build_array(r.storage_provider,r.bucket,v_root)::text,'UTF8')),'hex');
 v_pointer:=sessions.ingest_history_pointer(r);
 INSERT INTO sessions.ingest_history_carriers(carrier_sha256,storage_provider,bucket,physical_root,layout_known)
 VALUES(v_key,r.storage_provider,r.bucket,v_root,v_known)
 ON CONFLICT(carrier_sha256)DO UPDATE SET epoch=nextval('sessions.ingest_history_inventory_epoch'),scan_complete=false
 WHERE sessions.ingest_history_carriers.storage_provider=excluded.storage_provider AND sessions.ingest_history_carriers.bucket=excluded.bucket
  AND sessions.ingest_history_carriers.physical_root=excluded.physical_root;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: carrier hash collision';END IF;
 INSERT INTO sessions.ingest_history_pointer_refs VALUES(r.org_id,r.workspace_id,r.harness,r.session_id,r.machine_id,v_key,v_pointer,
  encode(sha256(convert_to(v_pointer::text,'UTF8')),'hex'));
 RETURN NEW;
END $sync$;
CREATE TRIGGER ingest_history_pointer_sync AFTER INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_history_sync_pointer();
-- Seed the reference index without changing stored pointers or applied epochs.
DO $initial_refs$
DECLARE r sessions.session_index%ROWTYPE;v_root text;v_key text;v_pointer jsonb;v_known boolean;
BEGIN
 FOR r IN SELECT * FROM sessions.session_index WHERE manifest_key IS NOT NULL AND storage_provider IS NOT NULL AND bucket IS NOT NULL LOOP
  v_root:=sessions.ingest_history_root(r.manifest_key);
  v_known:=v_root IS NOT NULL;v_root:=coalesce(v_root,r.manifest_key);
  v_key:=encode(sha256(convert_to(jsonb_build_array(r.storage_provider,r.bucket,v_root)::text,'UTF8')),'hex');
  v_pointer:=sessions.ingest_history_pointer(r);
  INSERT INTO sessions.ingest_history_carriers(carrier_sha256,storage_provider,bucket,physical_root,layout_known)VALUES(v_key,r.storage_provider,r.bucket,v_root,v_known)ON CONFLICT DO NOTHING;
  INSERT INTO sessions.ingest_history_pointer_refs VALUES(r.org_id,r.workspace_id,r.harness,r.session_id,r.machine_id,v_key,v_pointer,encode(sha256(convert_to(v_pointer::text,'UTF8')),'hex'));
 END LOOP;
END $initial_refs$;

CREATE FUNCTION sessions.ingest_history_inventory_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_token uuid:=gen_random_uuid();v_lease uuid:=gen_random_uuid();v_count bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','bucket']);PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'bucket')IS DISTINCT FROM'string' OR octet_length(p->>'bucket')NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid inventory target';END IF;
 SELECT q.* INTO c FROM sessions.ingest_history_carriers q
  WHERE q.storage_provider='s3' AND q.layout_known AND q.bucket=p->>'bucket' AND(q.scanned_epoch IS DISTINCT FROM q.epoch OR NOT q.scan_complete)
   AND(q.scan_lease_until IS NULL OR q.scan_lease_until<=clock_timestamp())
   AND EXISTS(SELECT 1 FROM sessions.ingest_history_pointer_refs r WHERE r.carrier_sha256=q.carrier_sha256)
  ORDER BY q.carrier_sha256 FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN jsonb_build_object('ticket',NULL);END IF;
 INSERT INTO sessions.ingest_history_scan_refs(scan_token,ref_ordinal,carrier_sha256,pointer,pointer_sha256)
 SELECT v_token,row_number()OVER(ORDER BY r.org_id COLLATE"C",r.workspace_id COLLATE"C",r.harness COLLATE"C",r.session_id COLLATE"C",r.machine_id COLLATE"C")-1,
  r.carrier_sha256,r.pointer,r.pointer_sha256 FROM sessions.ingest_history_pointer_refs r WHERE r.carrier_sha256=c.carrier_sha256;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count=0 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: carrier lost references';END IF;
 DELETE FROM sessions.ingest_history_native_locations WHERE carrier_sha256=c.carrier_sha256;
 UPDATE sessions.ingest_history_carriers SET scan_token=v_token,scan_lease_token=v_lease,scan_lease_until=clock_timestamp()+interval'5 minutes',
  scan_generation=p->>'generation',next_page=0,scanned_records=0,scanned_epoch=c.epoch,scan_complete=false,scan_descriptor_sha256=NULL
 WHERE carrier_sha256=c.carrier_sha256;
 RETURN jsonb_build_object('ticket',jsonb_build_object('carrierSha256',c.carrier_sha256,'physicalRoot',c.physical_root,'bucket',c.bucket,
  'scanToken',v_token,'leaseToken',v_lease,'epoch',c.epoch::text,'referenceCount',v_count::text,'leaseUntil',clock_timestamp()+interval'5 minutes'));
END $claim$;
CREATE FUNCTION sessions.ingest_history_inventory_locked(p jsonb) RETURNS sessions.ingest_history_carriers
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $locked$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 SELECT * INTO c FROM sessions.ingest_history_carriers WHERE carrier_sha256=p->>'carrierSha256' FOR UPDATE;
 IF NOT FOUND OR c.scan_token IS DISTINCT FROM(p->>'scanToken')::uuid OR c.scan_lease_token IS DISTINCT FROM(p->>'leaseToken')::uuid
  OR c.scan_generation IS DISTINCT FROM p->>'generation' OR c.epoch IS DISTINCT FROM(p->>'epoch')::bigint
  OR c.scanned_epoch IS DISTINCT FROM c.epoch OR c.scan_lease_until<=clock_timestamp() OR c.scan_complete THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory lease/source changed';END IF;
 RETURN c;
END $locked$;
CREATE FUNCTION sessions.ingest_history_inventory_refs(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $refs$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_after bigint;v_rows jsonb;v_total bigint;v_next bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch','after']);
 c:=sessions.ingest_history_inventory_locked(p);v_after:=(p->>'after')::bigint;
 IF v_after<0 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid reference cursor';END IF;
 SELECT count(*)INTO v_total FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token;
 SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',r.ref_ordinal::text,'pointer',r.pointer,'pointerSha256',r.pointer_sha256)ORDER BY r.ref_ordinal),'[]'),max(r.ref_ordinal)+1
 INTO v_rows,v_next FROM(SELECT *FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token AND ref_ordinal>=v_after ORDER BY ref_ordinal LIMIT 100)r;
 IF octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history: reference packet limit';END IF;
 IF c.scan_lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory lease expired';END IF;
 RETURN jsonb_build_object('references',v_rows,'total',v_total::text,'next',CASE WHEN v_next<v_total THEN v_next::text ELSE NULL END);
END $refs$;
CREATE FUNCTION sessions.ingest_history_inventory_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;r jsonb;v_page bigint;v_sha text;prior sessions.ingest_history_scan_pages%ROWTYPE;v_records bigint:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch','page','records']);
 c:=sessions.ingest_history_inventory_locked(p);v_page:=(p->>'page')::bigint;
 IF v_page<0 OR jsonb_typeof(p->'records')IS DISTINCT FROM'array' OR jsonb_array_length(p->'records')NOT BETWEEN 1 AND 500
  OR octet_length(p::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: inventory page bound';END IF;
 v_sha:=encode(sha256(convert_to((p->'records')::text,'UTF8')),'hex');
 SELECT *INTO prior FROM sessions.ingest_history_scan_pages WHERE scan_token=c.scan_token AND page=v_page;
 IF FOUND THEN
  IF prior.page_sha256<>v_sha THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: replayed page changed';END IF;
  RETURN jsonb_build_object('stored',true,'replay',true);END IF;
 IF v_page<>c.next_page THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: missing inventory page';END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(p->'records') LOOP
  PERFORM sessions.ingest_exact_keys(r,ARRAY['orgId','workspaceId','originId','recordId','revision','count']);
  IF EXISTS(SELECT 1 FROM unnest(ARRAY['orgId','workspaceId','originId','recordId'])k
   WHERE jsonb_typeof(r->k)IS DISTINCT FROM'string' OR char_length(r->>k)NOT BETWEEN 1 AND 1024 OR octet_length(r->>k)>4096)
   OR NOT EXISTS(SELECT 1 FROM sessions.ingest_history_scan_refs f WHERE f.scan_token=c.scan_token AND f.pointer->>'org_id'=r->>'orgId'
    AND f.pointer->>'workspace_id'=r->>'workspaceId') OR jsonb_typeof(r->'revision')IS DISTINCT FROM'string'
   OR r->>'revision'!~'^[1-9][0-9]{0,18}$' OR(r->>'revision')::numeric>9223372036854775807
   OR jsonb_typeof(r->'count')IS DISTINCT FROM'number' OR(r->>'count')::numeric<>trunc((r->>'count')::numeric)
   OR(r->>'count')::numeric NOT BETWEEN 1 AND 2000000 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid carrier native record';END IF;
  INSERT INTO sessions.ingest_history_native_locations AS n VALUES(c.carrier_sha256,r->>'orgId',r->>'workspaceId',r->>'originId',r->>'recordId',(r->>'revision')::bigint,(r->>'count')::bigint)
  ON CONFLICT(carrier_sha256,org_id,workspace_id,origin_id,record_id)DO UPDATE SET
   max_revision=greatest(n.max_revision,excluded.max_revision),records=n.records+excluded.records;
  v_records:=v_records+(r->>'count')::bigint;
 END LOOP;
 INSERT INTO sessions.ingest_history_scan_pages VALUES(c.scan_token,v_page,v_sha,v_records);
 UPDATE sessions.ingest_history_carriers SET next_page=next_page+1,scanned_records=scanned_records+v_records
 WHERE carrier_sha256=c.carrier_sha256;
 IF c.scan_lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory lease expired after page';END IF;
 RETURN jsonb_build_object('stored',true,'replay',false);
END $page$;
CREATE FUNCTION sessions.ingest_history_inventory_finish(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $finish$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_refs bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch','pages','records','descriptorSha256']);
 c:=sessions.ingest_history_inventory_locked(p);
 IF jsonb_typeof(p->'pages')IS DISTINCT FROM'string' OR jsonb_typeof(p->'records')IS DISTINCT FROM'string'
  OR p->>'pages'!~'^(0|[1-9][0-9]*)$' OR p->>'records'!~'^(0|[1-9][0-9]*)$'
  OR (p->>'pages')::bigint<>c.next_page OR(p->>'records')::bigint<>c.scanned_records
  OR jsonb_typeof(p->'descriptorSha256')IS DISTINCT FROM'string' OR p->>'descriptorSha256'!~'^[a-f0-9]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: inventory incomplete';END IF;
 -- Exact GLOBAL reference inventory, including users other than the claimant.
 IF EXISTS((SELECT pointer_sha256,pointer FROM sessions.ingest_history_pointer_refs WHERE carrier_sha256=c.carrier_sha256
   EXCEPT ALL SELECT pointer_sha256,pointer FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token)
  UNION ALL(SELECT pointer_sha256,pointer FROM sessions.ingest_history_scan_refs WHERE scan_token=c.scan_token
   EXCEPT ALL SELECT pointer_sha256,pointer FROM sessions.ingest_history_pointer_refs WHERE carrier_sha256=c.carrier_sha256)) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: global carrier references changed';END IF;
 UPDATE sessions.ingest_history_carriers SET scan_complete=true,scan_descriptor_sha256=p->>'descriptorSha256',scan_lease_token=NULL,scan_token=NULL,scan_lease_until=NULL
 WHERE carrier_sha256=c.carrier_sha256;
 IF c.scan_lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: final inventory expired';END IF;
 RETURN jsonb_build_object('complete',true,'records',c.scanned_records::text);
END $finish$;
CREATE FUNCTION sessions.ingest_history_inventory_heartbeat(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE c sessions.ingest_history_carriers%ROWTYPE;v_until timestamptz;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','carrierSha256','scanToken','leaseToken','epoch']);
 c:=sessions.ingest_history_inventory_locked(p);v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_history_carriers SET scan_lease_until=v_until WHERE carrier_sha256=c.carrier_sha256 AND scan_lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: expired inventory cannot renew';END IF;
 RETURN jsonb_build_object('leaseUntil',v_until);
END $heartbeat$;
REVOKE ALL ON FUNCTION sessions.ingest_history_root(text),sessions.ingest_history_pointer(sessions.session_index),sessions.ingest_history_sync_pointer(),
 sessions.ingest_history_inventory_claim(jsonb),sessions.ingest_history_inventory_locked(jsonb),sessions.ingest_history_inventory_refs(jsonb),
 sessions.ingest_history_inventory_page(jsonb),sessions.ingest_history_inventory_finish(jsonb),sessions.ingest_history_inventory_heartbeat(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_inventory_claim(jsonb),sessions.ingest_history_inventory_refs(jsonb),
 sessions.ingest_history_inventory_page(jsonb),sessions.ingest_history_inventory_finish(jsonb),sessions.ingest_history_inventory_heartbeat(jsonb)TO bulls96_session_ingest;


-- Plans are immutable capture records. S3 IO never runs in these transactions.
CREATE TABLE sessions.ingest_history_plans(
 plan_token uuid PRIMARY KEY,lease_token uuid NOT NULL,schema_generation text NOT NULL,lease_until timestamptz NOT NULL,
 stage_token uuid NOT NULL REFERENCES sessions.ingest_stage_intents,stage_version bigint NOT NULL,outbox_version bigint NOT NULL,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,
 target_bucket text NOT NULL,target_region text NOT NULL,key_root text NOT NULL,
 accepted_ordinals jsonb NOT NULL,receipt_seats jsonb NOT NULL,native_snapshot jsonb NOT NULL,
 frontier jsonb NOT NULL,state text NOT NULL CHECK(state IN('admitted','prepared','published','rejected')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),published_at timestamptz,completion_sha256 text
);
CREATE TABLE sessions.ingest_history_plan_carriers(
 plan_token uuid NOT NULL REFERENCES sessions.ingest_history_plans,carrier_sha256 text NOT NULL REFERENCES sessions.ingest_history_carriers,
 source_epoch bigint NOT NULL,reference_count bigint NOT NULL CHECK(reference_count>0),
 destination_generation uuid NOT NULL,destination_prefix text NOT NULL,
 PRIMARY KEY(plan_token,carrier_sha256),UNIQUE(destination_generation)
);
CREATE TABLE sessions.ingest_history_plan_refs(
 plan_token uuid NOT NULL,carrier_sha256 text NOT NULL,ordinal bigint NOT NULL,
 pointer jsonb NOT NULL,pointer_sha256 text NOT NULL,
 PRIMARY KEY(plan_token,carrier_sha256,ordinal),FOREIGN KEY(plan_token,carrier_sha256)REFERENCES sessions.ingest_history_plan_carriers
);
ALTER TABLE sessions.ingest_history_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_plan_carriers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_plan_refs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_plans,sessions.ingest_history_plan_carriers,sessions.ingest_history_plan_refs
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_native_snapshot(p_stage uuid)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $native$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;v jsonb;v_expected bigint;v_wanted bigint;
BEGIN
 SELECT *INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT *INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 WITH keep AS MATERIALIZED(SELECT DISTINCT(o::text)::integer ord FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o),wanted AS MATERIALIZED(
  SELECT DISTINCT x->>'record_id' record_id,encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,x->>'record_id')::text,'UTF8')),'hex') key
  FROM jsonb_array_elements(a.records)x JOIN keep k ON k.ord=(x->>'ord')::integer
 )SELECT coalesce(jsonb_agg(jsonb_build_object('recordId',w.record_id,'slot',to_jsonb(n))ORDER BY w.record_id COLLATE"C"),'[]'),count(*),(SELECT count(*)FROM wanted)
 INTO v,v_expected,v_wanted FROM wanted w JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=w.key AND n.org_id=a.org_id
  AND n.workspace_id=a.workspace_id AND n.origin_id=a.origin_id AND n.record_id=w.record_id AND n.current_owner=a.user_id;
 IF v_expected=0 OR v_expected<>v_wanted THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: no current qualified native custody';END IF;
 RETURN v;
END $native$;
CREATE FUNCTION sessions.ingest_history_plan_claim(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;o sessions.ingest_buffer_outbox%ROWTYPE;
 f sessions.ingest_publication_frontiers%ROWTYPE;c sessions.ingest_history_carriers%ROWTYPE;
 v_plan uuid:=gen_random_uuid();v_lease uuid:=gen_random_uuid();v_until timestamptz;v_org text;v_ws text;v_user text;
 v_keep jsonb;v_all jsonb;v_native jsonb;v_seats jsonb;v_carriers bigint:=0;v_refs bigint;v_gen uuid;v_root text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','stageToken','bucket','region','keyRoot']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'keyRoot')IS DISTINCT FROM'string' OR p->>'keyRoot'!~'^[a-z0-9][a-z0-9-]{0,62}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid key root';END IF;
 SELECT org_id,workspace_id,user_id INTO v_org,v_ws,v_user FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: stage absent';END IF;
 PERFORM sessions.ingest_require_buffer_admission(v_org);PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT *INTO s FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id AND b.user_id=s.user_id
  AND b.origin_id=s.origin_id AND b.batch_id=s.batch_id AND b.digest=s.digest AND b.state='committed' AND b.buffer_receipt_kind='staged' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: durable staged receipt absent';END IF;
 SELECT *INTO s FROM sessions.ingest_stage_intents WHERE stage_token=s.stage_token FOR UPDATE;
 SELECT *INTO o FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
 IF NOT FOUND OR o.active_stage_token IS DISTINCT FROM s.stage_token OR o.state<>'buffered' OR o.user_id<>s.user_id OR o.digest<>s.digest
  OR s.receipt_state<>'acknowledged' OR s.cleanup_state<>'live' OR s.gc_lease_until>clock_timestamp() OR s.gc_replacement_stage_token IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: stage busy or changed';END IF;
 SELECT *INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 -- Current target custody/lifecycle dominates unqualified historical S3 MAX.
 -- All chosen records have immutable true ACK decisions and current exact seats.
 v_keep:=sessions.ingest_buffer_gc_ordinals(s.stage_token);
 SELECT coalesce(jsonb_agg(x ORDER BY x::text::integer),'[]')INTO v_all FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)x;
 IF v_keep='[]'OR v_keep IS DISTINCT FROM v_all THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: stage requires original-ordinal retention rewrite';END IF;
 v_seats:=sessions.ingest_receipt_seats_for_stage(s.stage_token);v_native:=sessions.ingest_history_native_snapshot(s.stage_token);
 SELECT *INTO f FROM sessions.ingest_publication_frontiers WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND user_id=s.user_id AND origin_id=s.origin_id
  AND schema_generation=p->>'generation' AND target_bucket=p->>'bucket' AND target_region=p->>'region' AND key_root=p->>'keyRoot' AND valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: qualified complete source frontier required';END IF;
 -- Complete coverage is a database predicate, never a limited candidate page.
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_pointer_refs r JOIN sessions.ingest_history_carriers q USING(carrier_sha256)
   WHERE r.org_id=s.org_id AND r.workspace_id=s.workspace_id AND(q.storage_provider<>'s3' OR q.bucket<>f.target_bucket OR NOT q.layout_known OR NOT q.scan_complete OR q.scanned_epoch IS DISTINCT FROM q.epoch)) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: complete verified carrier inventory required';END IF;
 v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_stage_intents SET row_version=row_version+1,gc_lease_version=row_version+1,gc_lease_token=v_lease,
  gc_generation=p->>'generation',gc_lease_until=v_until,gc_keep_groups=NULL,gc_keep_ordinals=NULL,gc_outbox_version=o.row_version WHERE stage_token=s.stage_token RETURNING*INTO s;
 INSERT INTO sessions.ingest_history_plans(plan_token,lease_token,schema_generation,lease_until,stage_token,stage_version,outbox_version,
  org_id,workspace_id,user_id,origin_id,batch_id,digest,target_bucket,target_region,key_root,accepted_ordinals,receipt_seats,native_snapshot,frontier,state)
 VALUES(v_plan,v_lease,p->>'generation',v_until,s.stage_token,s.row_version,o.row_version,s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest,
  f.target_bucket,f.target_region,f.key_root,v_keep,v_seats,v_native,to_jsonb(f),'admitted');
 -- All carriers containing any selected native key, plus destination scope
 -- carriers whose unrelated content must be preserved. The indexed native
 -- lookup includes older locations, not only the latest numeric revision.
 FOR c IN SELECT q.*FROM sessions.ingest_history_carriers q WHERE q.carrier_sha256 IN(
  SELECT n.carrier_sha256 FROM sessions.ingest_history_native_locations n JOIN jsonb_array_elements(a.records)x ON n.record_id=x->>'record_id'
   WHERE n.org_id=s.org_id AND n.workspace_id=s.workspace_id AND n.origin_id=s.origin_id AND v_keep@>to_jsonb((x->>'ord')::integer)
  UNION SELECT r.carrier_sha256 FROM sessions.ingest_history_pointer_refs r JOIN jsonb_array_elements(a.records)x
   ON r.harness=CASE WHEN x->>'session_id'IS NULL THEN '_none'ELSE x->>'source'END
    AND coalesce(r.pointer->>'canonical_session_id',r.session_id)=coalesce(x->>'session_id','_origin')
   WHERE r.org_id=s.org_id AND r.workspace_id=s.workspace_id AND v_keep@>to_jsonb((x->>'ord')::integer))
  ORDER BY q.carrier_sha256 LOOP
  IF NOT c.scan_complete OR c.scanned_epoch IS DISTINCT FROM c.epoch OR c.storage_provider<>'s3' OR c.bucket<>f.target_bucket OR NOT c.layout_known THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: selected carrier inventory changed';END IF;
  SELECT count(*)INTO v_refs FROM sessions.ingest_history_pointer_refs WHERE carrier_sha256=c.carrier_sha256;
  IF v_refs=0 THEN CONTINUE;END IF;
  v_gen:=gen_random_uuid();v_root:=f.key_root||'/'||s.org_id||'/upload-generations/g='||v_gen;
  INSERT INTO sessions.ingest_history_plan_carriers VALUES(v_plan,c.carrier_sha256,c.epoch,v_refs,v_gen,v_root);
  INSERT INTO sessions.ingest_history_plan_refs SELECT v_plan,c.carrier_sha256,
   row_number()OVER(ORDER BY r.org_id COLLATE"C",r.workspace_id COLLATE"C",r.harness COLLATE"C",r.session_id COLLATE"C",r.machine_id COLLATE"C")-1,r.pointer,r.pointer_sha256
   FROM sessions.ingest_history_pointer_refs r WHERE r.carrier_sha256=c.carrier_sha256;
  v_carriers:=v_carriers+1;
 END LOOP;
 -- Baseline closure is irrevocable BEFORE the first generation PUT.
 UPDATE sessions.ingest_batches SET publication_state='canonical' WHERE org_id=s.org_id AND workspace_id=s.workspace_id AND origin_id=s.origin_id AND batch_id=s.batch_id;
 PERFORM sessions.ingest_require_buffer_admission(s.org_id);
 IF v_until<=clock_timestamp()OR f.valid_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: plan expired after capture';END IF;
 RETURN jsonb_build_object('schemaGeneration',p->>'generation','planToken',v_plan,'leaseToken',v_lease,'leaseUntil',v_until,
  'stageToken',s.stage_token,'stageVersion',s.row_version::text,'outboxVersion',o.row_version::text,'carrierCount',v_carriers::text,
  'tenant',jsonb_build_object('orgId',s.org_id,'workspaceId',s.workspace_id,'userId',s.user_id),'originId',s.origin_id,
  'batchId',s.batch_id,'batchDigest',s.digest,'claimToken',s.claim_token,'records',a.records,'acceptedOrdinals',v_keep,'receiptSeats',v_seats,
  'stage',jsonb_build_object('key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,'groupOrdinals',s.group_ordinals));
END $claim$;
CREATE FUNCTION sessions.ingest_history_plan_locked(p_plan uuid,p_lease uuid,p_generation text)RETURNS sessions.ingest_history_plans
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $locked$
DECLARE a sessions.ingest_history_plans%ROWTYPE;s sessions.ingest_stage_intents%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_generation(p_generation);
 SELECT *INTO a FROM sessions.ingest_history_plans WHERE plan_token=p_plan;
 IF NOT FOUND OR a.lease_token IS DISTINCT FROM p_lease OR a.schema_generation IS DISTINCT FROM p_generation THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: unknown plan lease';END IF;
 PERFORM sessions.ingest_require_buffer_admission(a.org_id);s:=sessions.ingest_buffer_gc_locked(a.stage_token,p_lease);
 SELECT *INTO a FROM sessions.ingest_history_plans WHERE plan_token=p_plan FOR UPDATE;
 IF a.state NOT IN('admitted','prepared')OR a.stage_version<>s.row_version OR s.gc_keep_groups IS NOT NULL OR s.gc_keep_ordinals IS NOT NULL
  OR s.cleanup_state<>'live'OR s.receipt_state<>'acknowledged'OR a.lease_until<=clock_timestamp()
  OR NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=a.org_id AND o.workspace_id=a.workspace_id AND o.user_id=a.user_id
   AND o.origin_id=a.origin_id AND o.batch_id=a.batch_id AND o.active_stage_token=a.stage_token AND o.state='buffered'AND o.row_version=a.outbox_version)
  OR sessions.ingest_buffer_gc_ordinals(a.stage_token)IS DISTINCT FROM a.accepted_ordinals
  OR sessions.ingest_history_native_snapshot(a.stage_token)IS DISTINCT FROM a.native_snapshot THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: original receipt/custody changed';END IF;
 PERFORM 1 FROM sessions.ingest_publication_frontiers f WHERE to_jsonb(f)=a.frontier AND f.valid_until>clock_timestamp()FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: frontier changed';END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_plan_carriers pc JOIN sessions.ingest_history_carriers c USING(carrier_sha256)
  WHERE pc.plan_token=a.plan_token AND(c.epoch<>pc.source_epoch OR c.scanned_epoch IS DISTINCT FROM c.epoch OR NOT c.scan_complete))THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: captured carrier changed';END IF;
 IF a.lease_until<=clock_timestamp()OR s.gc_lease_until<=clock_timestamp()THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: final plan deadline expired';END IF;
 RETURN a;
END $locked$;
CREATE FUNCTION sessions.ingest_history_plan_heartbeat(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE a sessions.ingest_history_plans%ROWTYPE;v_until timestamptz;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','planToken','leaseToken']);
 a:=sessions.ingest_history_plan_locked((p->>'planToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');v_until:=clock_timestamp()+interval'5 minutes';
 UPDATE sessions.ingest_stage_intents SET gc_lease_until=v_until WHERE stage_token=a.stage_token AND gc_lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: expired stage cannot renew';END IF;
 UPDATE sessions.ingest_history_plans SET lease_until=v_until WHERE plan_token=a.plan_token AND lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: expired plan cannot renew';END IF;
 RETURN jsonb_build_object('leaseUntil',v_until);
END $heartbeat$;
REVOKE ALL ON FUNCTION sessions.ingest_history_native_snapshot(uuid),sessions.ingest_history_plan_claim(jsonb),sessions.ingest_history_plan_locked(uuid,uuid,text),sessions.ingest_history_plan_heartbeat(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_plan_claim(jsonb),sessions.ingest_history_plan_heartbeat(jsonb)TO bulls96_session_ingest;

CREATE FUNCTION sessions.ingest_history_plan_carriers_page(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $carriers$
DECLARE a sessions.ingest_history_plans%ROWTYPE;v_after text;v_rows jsonb;v_last text;v_total bigint;v_more boolean;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','planToken','leaseToken','after']);
 a:=sessions.ingest_history_plan_locked((p->>'planToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');v_after:=p->>'after';
 IF jsonb_typeof(p->'after')IS DISTINCT FROM'string'OR(v_after<>''AND v_after!~'^[a-f0-9]{64}$')THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid carrier cursor';END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('carrierSha256',pc.carrier_sha256,'epoch',pc.source_epoch::text,'referenceCount',pc.reference_count::text,
  'generation',pc.destination_generation,'prefix',pc.destination_prefix,'physicalRoot',c.physical_root,'bucket',c.bucket)ORDER BY pc.carrier_sha256),'[]'),max(pc.carrier_sha256)
 INTO v_rows,v_last FROM(SELECT *FROM sessions.ingest_history_plan_carriers WHERE plan_token=a.plan_token AND carrier_sha256>v_after COLLATE"C" ORDER BY carrier_sha256 COLLATE"C" LIMIT 20)pc
 JOIN sessions.ingest_history_carriers c USING(carrier_sha256);
 SELECT count(*),coalesce(bool_or(carrier_sha256>v_last COLLATE"C"),false)INTO v_total,v_more FROM sessions.ingest_history_plan_carriers WHERE plan_token=a.plan_token;
 IF octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history: carrier packet bound';END IF;
 IF a.lease_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: carrier page expired';END IF;
 RETURN jsonb_build_object('carriers',v_rows,'total',v_total::text,'next',CASE WHEN v_more THEN v_last ELSE NULL END);
END $carriers$;
CREATE FUNCTION sessions.ingest_history_plan_refs_page(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $refs$
DECLARE a sessions.ingest_history_plans%ROWTYPE;c sessions.ingest_history_plan_carriers%ROWTYPE;v_after bigint;v_rows jsonb;v_next bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','planToken','leaseToken','carrierSha256','after']);
 a:=sessions.ingest_history_plan_locked((p->>'planToken')::uuid,(p->>'leaseToken')::uuid,p->>'generation');
 SELECT *INTO c FROM sessions.ingest_history_plan_carriers WHERE plan_token=a.plan_token AND carrier_sha256=p->>'carrierSha256';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: carrier outside immutable plan';END IF;
 v_after:=(p->>'after')::bigint;IF v_after<0 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid plan reference cursor';END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',r.ordinal::text,'pointer',r.pointer,'pointerSha256',r.pointer_sha256)ORDER BY r.ordinal),'[]'),max(r.ordinal)+1
 INTO v_rows,v_next FROM(SELECT *FROM sessions.ingest_history_plan_refs WHERE plan_token=a.plan_token AND carrier_sha256=c.carrier_sha256 AND ordinal>=v_after ORDER BY ordinal LIMIT 100)r;
 IF octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='history: plan reference packet bound';END IF;
 IF a.lease_until<=clock_timestamp()THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: plan reference page expired';END IF;
 RETURN jsonb_build_object('references',v_rows,'total',c.reference_count::text,'next',CASE WHEN v_next<c.reference_count THEN v_next::text ELSE NULL END);
END $refs$;
REVOKE ALL ON FUNCTION sessions.ingest_history_plan_carriers_page(jsonb),sessions.ingest_history_plan_refs_page(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_plan_carriers_page(jsonb),sessions.ingest_history_plan_refs_page(jsonb)TO bulls96_session_ingest;


-- Privacy/move work survives source-stage GC. A known-empty slot is still a
-- native-key erasure obligation; no surviving new body is required to remove
-- every older carrier copy. Original ACK/claim metadata remains immutable.
CREATE TABLE sessions.ingest_history_reconcile_outbox(
 native_key_sha256 text PRIMARY KEY REFERENCES sessions.ingest_lazy_native_slots(key_sha256),
 requested_lineage bigint NOT NULL CHECK(requested_lineage>=0),request_version bigint NOT NULL DEFAULT 1,
 source_identity_sha256 text NOT NULL,control_epoch text NOT NULL,
 prior_owner text,current_owner text,state text NOT NULL DEFAULT'pending'CHECK(state IN('pending','leased','complete')),
 lease_token uuid,lease_until timestamptz,lease_generation text,
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
 CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
CREATE INDEX ingest_history_reconcile_pending ON sessions.ingest_history_reconcile_outbox(requested_at,native_key_sha256)WHERE state<>'complete';
ALTER TABLE sessions.ingest_history_reconcile_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_reconcile_outbox FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE FUNCTION sessions.ingest_history_native_changed()RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE v_scope record;
BEGIN
 IF EXISTS(SELECT 1 FROM history_new n JOIN sessions.ingest_history_reconcile_outbox o ON o.native_key_sha256=n.key_sha256
  WHERE o.source_identity_sha256<>n.source_identity_sha256 OR o.control_epoch<>n.control_epoch)THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: native reconcile source changed';END IF;
 IF TG_OP='INSERT'THEN
  INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,current_owner)
  SELECT key_sha256,lineage,source_identity_sha256,control_epoch,current_owner FROM history_new
  ON CONFLICT(native_key_sha256)DO UPDATE SET requested_lineage=excluded.requested_lineage,request_version=sessions.ingest_history_reconcile_outbox.request_version+1,
   current_owner=excluded.current_owner,state='pending',lease_token=NULL,lease_until=NULL,lease_generation=NULL,requested_at=clock_timestamp(),completed_at=NULL;
 ELSE
  INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,prior_owner,current_owner)
  SELECT n.key_sha256,n.lineage,n.source_identity_sha256,n.control_epoch,o.current_owner,n.current_owner FROM history_new n JOIN history_old o USING(key_sha256)
  WHERE ROW(o.lineage,o.current_owner,o.current_source,o.current_session,o.current_revision,o.current_digest,o.current_reset_epoch,o.current_fence_epoch,o.current_seat_key,o.current_legacy_key)
   IS DISTINCT FROM ROW(n.lineage,n.current_owner,n.current_source,n.current_session,n.current_revision,n.current_digest,n.current_reset_epoch,n.current_fence_epoch,n.current_seat_key,n.current_legacy_key)
  ON CONFLICT(native_key_sha256)DO UPDATE SET requested_lineage=excluded.requested_lineage,request_version=sessions.ingest_history_reconcile_outbox.request_version+1,
   prior_owner=coalesce(excluded.prior_owner,sessions.ingest_history_reconcile_outbox.prior_owner),current_owner=excluded.current_owner,
   state='pending',lease_token=NULL,lease_until=NULL,lease_generation=NULL,requested_at=clock_timestamp(),completed_at=NULL;
 END IF;
 -- A native winner can change without any physical pointer changing. Invalidate
 -- every previously registered logical consumer in this SAME ACK/control txn.
 -- Sorted nonblocking scope locks avoid USER->scope / scope->model deadlocks.
 FOR v_scope IN
  SELECT z.*FROM(
   SELECT DISTINCT k.org_id,k.workspace_id,k.harness,k.session_id
   FROM history_new n JOIN sessions.ingest_history_scope_keys k
    ON k.org_id=n.org_id AND k.workspace_id=n.workspace_id AND k.record_id=n.record_id
  )z ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
 LOOP
  PERFORM sessions.ingest_history_invalidate_scope(v_scope.org_id,v_scope.workspace_id,v_scope.harness,v_scope.session_id);
 END LOOP;
 RETURN NULL;
END $changed$;
-- Transition tables batch resets/large ACKs; never one nested SPI mutation per
-- native row while the existing short USER/ACK critical section is held.
CREATE TRIGGER ingest_history_native_insert AFTER INSERT ON sessions.ingest_lazy_native_slots
 REFERENCING NEW TABLE AS history_new FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_history_native_changed();
CREATE TRIGGER ingest_history_native_update AFTER UPDATE ON sessions.ingest_lazy_native_slots
 REFERENCING OLD TABLE AS history_old NEW TABLE AS history_new FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_history_native_changed();
-- Install AFTER initial-buffer operation without losing controls that occurred
-- while620 was dormant/uninstalled. Cleared slots are deliberately included.
INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,current_owner)
 SELECT key_sha256,lineage,source_identity_sha256,control_epoch,current_owner FROM sessions.ingest_lazy_native_slots;
REVOKE ALL ON FUNCTION sessions.ingest_history_native_changed()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;


-- Immutable copy-on-write mask pages make one-record moves cheap even when a
-- logical scope contains a million retained records. Tree fanout16/depth64;
-- counts support ordinal seeks without OFFSET scans or full-mask JSONB copies.
CREATE TABLE sessions.ingest_history_mask_pages(
 sha256 text PRIMARY KEY CHECK(sha256~'^[a-f0-9]{64}$'),org_id text NOT NULL,workspace_id text NOT NULL,
 prefix text NOT NULL CHECK(prefix~'^[a-f0-9]{0,64}$'),kind text NOT NULL CHECK(kind IN('leaf','node')),
 record_count bigint NOT NULL CHECK(record_count BETWEEN 0 AND 2000000),document jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions.ingest_history_scope_versions(
 authority_id uuid PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 source_epoch bigint NOT NULL CHECK(source_epoch>0),source_fingerprint text CHECK(source_fingerprint~'^[a-f0-9]{64}$'),
 mask_sha256 text NOT NULL REFERENCES sessions.ingest_history_mask_pages,record_count bigint NOT NULL CHECK(record_count BETWEEN 0 AND 2000000),
 derived_anchor_key text NOT NULL,bucket text NOT NULL,region text NOT NULL,key_root text NOT NULL,
 state text NOT NULL CHECK(state IN('building','ready','retired')),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),retired_at timestamptz
);
CREATE INDEX ingest_history_scope_versions_lookup ON sessions.ingest_history_scope_versions(org_id,workspace_id,harness,session_id,authority_id);
CREATE TABLE sessions.ingest_history_scope_current(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,ready boolean NOT NULL DEFAULT false,
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
CREATE TABLE sessions.ingest_history_scope_keys(
 org_id text NOT NULL,workspace_id text NOT NULL,record_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,record_id,harness,session_id)
);
CREATE TABLE sessions.ingest_history_derived_objects(
 object_key text PRIMARY KEY,authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,
 sha256 text NOT NULL CHECK(sha256~'^[a-f0-9]{64}$'),bytes bigint NOT NULL CHECK(bytes BETWEEN 1 AND 16777216),
 cleanup_state text NOT NULL DEFAULT'live'CHECK(cleanup_state IN('live','retired','redacted')),
 row_version bigint NOT NULL DEFAULT 1,lease_token uuid,lease_until timestamptz,barrier_version_id text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ingest_history_derived_authority_cleanup ON sessions.ingest_history_derived_objects(authority_id,cleanup_state);
CREATE INDEX ingest_history_derived_pending_cleanup ON sessions.ingest_history_derived_objects(created_at,object_key)WHERE cleanup_state='retired';
CREATE TABLE sessions.ingest_history_summaries(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,kind text NOT NULL CHECK(kind IN('short','long')),
 authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,source_epoch bigint NOT NULL,source_fingerprint text NOT NULL,
 mask_sha256 text NOT NULL,status text NOT NULL CHECK(status IN('ready','disabled','quarantined','empty','unsupported')),
 object_key text,object_sha256 text,object_bytes bigint,key_id text,model text,built_at timestamptz NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id,kind),
 CHECK(status<>'ready'OR(object_key IS NOT NULL AND object_sha256~'^[a-f0-9]{64}$'AND object_bytes BETWEEN 1 AND 16777216 AND key_id IS NOT NULL))
);
CREATE TABLE sessions.ingest_history_states(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 authority_id uuid NOT NULL REFERENCES sessions.ingest_history_scope_versions,source_epoch bigint NOT NULL,source_fingerprint text NOT NULL,mask_sha256 text NOT NULL,
 state text NOT NULL CHECK(state IN('active','done','abandoned','unknown')),classified_at timestamptz NOT NULL,
 PRIMARY KEY(org_id,workspace_id,harness,session_id)
);
ALTER TABLE sessions.ingest_history_mask_pages ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scope_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scope_current ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_scope_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_derived_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_summaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_states ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_mask_pages,sessions.ingest_history_scope_versions,sessions.ingest_history_scope_current,sessions.ingest_history_scope_keys,
 sessions.ingest_history_derived_objects,sessions.ingest_history_summaries,sessions.ingest_history_states FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_mask_register(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $mask$
DECLARE d jsonb;r jsonb;w jsonb;child sessions.ingest_history_mask_pages%ROWTYPE;v_count bigint;v_sum bigint:=0;v_prefix text;v_kind text;v_sha text;last_key text:='';v_key text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','document']);PERFORM sessions.ingest_require_generation(p->>'generation');
 d:=p->'document';PERFORM sessions.ingest_exact_keys(d,ARRAY['protocol','orgId','workspaceId','prefix','kind','count','items']);
 IF d->>'protocol'IS DISTINCT FROM'history-mask-v1'OR octet_length(d::text)>16777216
  OR EXISTS(SELECT 1 FROM unnest(ARRAY['orgId','workspaceId'])k WHERE jsonb_typeof(d->k)IS DISTINCT FROM'string'OR char_length(d->>k)NOT BETWEEN 1 AND 255)
  OR jsonb_typeof(d->'prefix')IS DISTINCT FROM'string'OR d->>'prefix'!~'^[a-f0-9]{0,64}$'
  OR jsonb_typeof(d->'count')IS DISTINCT FROM'string'OR d->>'count'!~'^(0|[1-9][0-9]{0,6})$'OR(d->>'count')::bigint>2000000
  OR jsonb_typeof(d->'items')IS DISTINCT FROM'array'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid mask page';END IF;
 v_prefix:=d->>'prefix';v_kind:=d->>'kind';v_count:=(d->>'count')::bigint;
 IF v_kind='leaf'THEN
  IF jsonb_array_length(d->'items')>500 OR jsonb_array_length(d->'items')<>v_count THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask leaf count';END IF;
  FOR r IN SELECT value FROM jsonb_array_elements(d->'items')LOOP
   PERFORM sessions.ingest_exact_keys(r,ARRAY['recordId','winner']);
   IF jsonb_typeof(r->'recordId')IS DISTINCT FROM'string'OR char_length(r->>'recordId')NOT BETWEEN 1 AND 1024 OR octet_length(r->>'recordId')>4096 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask record identity';END IF;
   v_key:=encode(sha256(convert_to(r->>'recordId','UTF8')),'hex');
   IF left(v_key,length(v_prefix))<>v_prefix OR v_key<=last_key COLLATE"C"THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask hash order/alias';END IF;last_key:=v_key;
   w:=r->'winner';
   IF w IS DISTINCT FROM'null'::jsonb THEN
    PERFORM sessions.ingest_exact_keys(w,ARRAY['originId','recordId','kind','revision','acceptedSeq','operation','userId','source','sessionId','contentSha256','lineage']);
    IF w->>'recordId'IS DISTINCT FROM r->>'recordId'OR EXISTS(SELECT 1 FROM unnest(ARRAY['originId','recordId','kind','userId','source'])k WHERE jsonb_typeof(w->k)IS DISTINCT FROM'string'OR char_length(w->>k)NOT BETWEEN 1 AND 1024 OR octet_length(w->>k)>4096)
     OR EXISTS(SELECT 1 FROM unnest(ARRAY['revision','acceptedSeq'])k WHERE jsonb_typeof(w->k)IS DISTINCT FROM'string'OR w->>k!~'^[1-9][0-9]{0,18}$'OR(w->>k)::numeric>9223372036854775807)
     OR jsonb_typeof(w->'lineage')IS DISTINCT FROM'string'OR w->>'lineage'!~'^(0|[1-9][0-9]{0,18})$'OR(w->>'lineage')::numeric>9223372036854775807
     OR jsonb_typeof(w->'operation')IS DISTINCT FROM'string'OR w->>'operation'NOT IN('upsert','delete')OR jsonb_typeof(w->'contentSha256')IS DISTINCT FROM'string'OR w->>'contentSha256'!~'^[a-f0-9]{64}$'
     OR(w->'sessionId'IS DISTINCT FROM'null'::jsonb AND(jsonb_typeof(w->'sessionId')IS DISTINCT FROM'string'OR char_length(w->>'sessionId')NOT BETWEEN 1 AND 1024))THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask current custody';END IF;
   END IF;
  END LOOP;
 ELSIF v_kind='node'THEN
  IF length(v_prefix)>=64 OR jsonb_array_length(d->'items')NOT BETWEEN 1 AND 16 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask node bound';END IF;
  FOR r IN SELECT value FROM jsonb_array_elements(d->'items')LOOP
   PERFORM sessions.ingest_exact_keys(r,ARRAY['digit','sha256']);
   IF jsonb_typeof(r->'digit')IS DISTINCT FROM'string'OR r->>'digit'!~'^[a-f0-9]$'OR r->>'digit'<=last_key COLLATE"C"OR jsonb_typeof(r->'sha256')IS DISTINCT FROM'string'OR r->>'sha256'!~'^[a-f0-9]{64}$'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask child order';END IF;
   SELECT *INTO child FROM sessions.ingest_history_mask_pages WHERE sha256=r->>'sha256';
   IF NOT FOUND OR child.org_id<>d->>'orgId'OR child.workspace_id<>d->>'workspaceId'OR child.prefix<>v_prefix||(r->>'digit')OR child.record_count=0 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: mask child proof';END IF;
   last_key:=r->>'digit';v_sum:=v_sum+child.record_count;
  END LOOP;
  IF v_sum<>v_count THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask node count';END IF;
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: unknown mask page kind';END IF;
 v_sha:=encode(sha256(convert_to(d::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_history_mask_pages VALUES(v_sha,d->>'orgId',d->>'workspaceId',v_prefix,v_kind,v_count,d,clock_timestamp())ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_mask_pages WHERE sha256=v_sha AND document=d)THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: mask hash collision';END IF;
 RETURN jsonb_build_object('registered',true,'sha256',v_sha,'count',v_count::text);
END $mask$;

CREATE FUNCTION sessions.ingest_history_mask_rows(p_root text,p_after bigint,p_limit integer)RETURNS TABLE(ordinal bigint,record_id text,winner jsonb)
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $rows$
 WITH RECURSIVE tree AS(
  SELECT p.sha256,p.document,p.kind,0::bigint AS base,0 AS depth FROM sessions.ingest_history_mask_pages p WHERE p.sha256=p_root
  UNION ALL
  SELECT c.sha256,c.document,c.kind,t.base+x.before_count,t.depth+1 FROM tree t
  CROSS JOIN LATERAL(
   SELECT c0.*,coalesce(sum(c0.record_count)OVER(ORDER BY c0.digit COLLATE"C"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)::bigint before_count
   FROM(SELECT c1.*,i->>'digit' digit FROM jsonb_array_elements(CASE WHEN t.kind='node'THEN t.document->'items'ELSE'[]'::jsonb END)i JOIN sessions.ingest_history_mask_pages c1 ON c1.sha256=i->>'sha256')c0
  )x JOIN sessions.ingest_history_mask_pages c ON c.sha256=x.sha256
  WHERE t.kind='node'AND t.depth<64 AND t.base+x.before_count<p_after+p_limit AND t.base+x.before_count+x.record_count>p_after
 )SELECT t.base+r.n-1,r.v->>'recordId',r.v->'winner'FROM tree t CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN t.kind='leaf'THEN t.document->'items'ELSE'[]'::jsonb END)WITH ORDINALITY r(v,n)
 WHERE t.base+r.n-1>=p_after AND t.base+r.n-1<p_after+p_limit ORDER BY 1
$rows$;

ALTER TABLE sessions.ingest_history_scope_versions ADD COLUMN pointer_snapshot_sha256 text NOT NULL CHECK(pointer_snapshot_sha256~'^[a-f0-9]{64}$');

-- A source snapshot names the immutable mask root, not all million rows. Existing
-- replay epoch/fingerprint remains the sole logical read/cache authority.
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
 ),'[]'::jsonb)) || coalesce((SELECT jsonb_build_object('history',jsonb_build_object('version',1,'required',true,'ready',c.ready AND v.source_epoch=a.epoch,
  'authorityId',v.authority_id,'maskSha256',v.mask_sha256,'recordCount',v.record_count::text,'derivedAnchorKey',v.derived_anchor_key))
  FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v USING(authority_id)
  WHERE c.org_id=p_org AND c.workspace_id=p_workspace AND c.harness=p_harness AND c.session_id=p_session),'{}'::jsonb) FROM sessions.session_replay_publications a
 WHERE a.org_id=p_org AND a.workspace_id=p_workspace AND a.harness=p_harness AND a.session_id=p_session;
$snapshot$;

CREATE FUNCTION sessions.ingest_history_source_current(p jsonb,p_lock boolean)RETURNS sessions.ingest_history_scope_versions
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $current$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;live jsonb;o text;w text;h text;sid text;
BEGIN
 PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM sessions.ingest_exact_keys(p->'identity',ARRAY['orgId','workspaceId','source','sessionId']);
 PERFORM sessions.ingest_exact_keys(p->'source',ARRAY['epoch','fingerprint']);
 o:=sessions.ingest_identifier(p#>'{identity,orgId}',false);w:=sessions.ingest_identifier(p#>'{identity,workspaceId}',false);
 h:=sessions.ingest_identifier(p#>'{identity,source}',false);sid:=sessions.ingest_identifier(p#>'{identity,sessionId}',false);
 IF jsonb_typeof(p->'authorityId')IS DISTINCT FROM'string'OR p->>'authorityId'!~'^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
  OR jsonb_typeof(p#>'{source,epoch}')IS DISTINCT FROM'string'OR p#>>'{source,epoch}'!~'^[1-9][0-9]{0,18}$'OR(p#>>'{source,epoch}')::numeric>9223372036854775807
  OR jsonb_typeof(p#>'{source,fingerprint}')IS DISTINCT FROM'string'OR p#>>'{source,fingerprint}'!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p->'maskSha256')IS DISTINCT FROM'string'OR p->>'maskSha256'!~'^[a-f0-9]{64}$'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: invalid logical source evidence';END IF;
 IF p_lock THEN
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',o,w,h,sid),0))THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: logical scope busy';END IF;
  SELECT v0.*INTO v FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v0 USING(authority_id)
   WHERE c.org_id=o AND c.workspace_id=w AND c.harness=h AND c.session_id=sid AND c.ready AND v0.state='ready'
    AND v0.authority_id=(p->>'authorityId')::uuid FOR SHARE OF c,v0;
 ELSE
  SELECT v0.*INTO v FROM sessions.ingest_history_scope_current c JOIN sessions.ingest_history_scope_versions v0 USING(authority_id)
   WHERE c.org_id=o AND c.workspace_id=w AND c.harness=h AND c.session_id=sid AND c.ready AND v0.state='ready'AND v0.authority_id=(p->>'authorityId')::uuid;
 END IF;
 IF NOT FOUND OR v.mask_sha256 IS DISTINCT FROM p->>'maskSha256'OR v.source_epoch::text IS DISTINCT FROM p#>>'{source,epoch}'
  OR v.source_fingerprint IS DISTINCT FROM p#>>'{source,fingerprint}'THEN RETURN NULL;END IF;
 -- Re-read AFTER any blocking row lock. Identical pointers cannot defeat the
 -- monotonic replay epoch or current mask/owner lifecycle.
 live:=sessions.session_replay_source_snapshot(o,w,h,sid);
 IF live->>'epoch'IS DISTINCT FROM v.source_epoch::text OR encode(sha256(convert_to(live::text,'UTF8')),'hex')IS DISTINCT FROM v.source_fingerprint
  OR live#>>'{history,authorityId}'IS DISTINCT FROM v.authority_id::text OR live#>>'{history,ready}'IS DISTINCT FROM'true'
  OR live#>>'{directory,deleted_at}'IS NOT NULL THEN RETURN NULL;END IF;
 RETURN v;
END $current$;
CREATE FUNCTION sessions.session_history_source_lock(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $lock$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256']);v:=sessions.ingest_history_source_current(p,true);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('current',false);END IF;
 PERFORM set_config('relay_history.source_permit',p::text,true);
 RETURN jsonb_build_object('current',true);
END $lock$;
CREATE FUNCTION sessions.session_history_mask_page(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;v_after bigint;v_rows jsonb;v_n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','after']);v:=sessions.ingest_history_source_current(p,false);
 IF v.authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: mask source changed';END IF;
 IF jsonb_typeof(p->'after')IS DISTINCT FROM'string'OR p->>'after'!~'^(0|[1-9][0-9]{0,18})$'OR(p->>'after')::numeric>v.record_count THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask ordinal bound';END IF;
 v_after:=(p->>'after')::bigint;
 SELECT coalesce(jsonb_agg(jsonb_build_object('ordinal',r.ordinal::text,'recordId',r.record_id,'winner',r.winner)ORDER BY r.ordinal),'[]'),count(*)INTO v_rows,v_n FROM sessions.ingest_history_mask_rows(v.mask_sha256,v_after,500)r;
 IF v_n<>least(500,v.record_count-v_after)OR octet_length(v_rows::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: incomplete mask tree';END IF;
 IF(sessions.ingest_history_source_current(p,false)).authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: mask source changed after read';END IF;
 RETURN jsonb_build_object('authorityId',v.authority_id,'records',v_rows,'total',v.record_count::text,'next',CASE WHEN v_after+v_n<v.record_count THEN(v_after+v_n)::text ELSE NULL END);
END $page$;
CREATE FUNCTION sessions.session_history_register_object(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $register$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;v_prefix text;v_tail text;v_key text;v_sha text;v_bytes bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','key','sha256','bytes']);v:=sessions.ingest_history_source_current(p,true);
 IF v.authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: derived source changed';END IF;
 v_prefix:=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/';
 v_key:=p->>'key';v_tail:=substr(v_key,length(v_prefix)+1);v_sha:=p->>'sha256';
 IF jsonb_typeof(p->'key')IS DISTINCT FROM'string'OR left(v_key,length(v_prefix))<>v_prefix OR v_tail!~'^[a-z][a-z0-9-]{0,63}/[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}[.]bin$'
  OR jsonb_typeof(p->'sha256')IS DISTINCT FROM'string'OR v_sha!~'^[a-f0-9]{64}$'OR jsonb_typeof(p->'bytes')IS DISTINCT FROM'number'
  OR(p->>'bytes')::numeric<>trunc((p->>'bytes')::numeric)OR(p->>'bytes')::numeric NOT BETWEEN 1 AND 16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: derived object proof';END IF;
 v_bytes:=(p->>'bytes')::bigint;
 INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes)VALUES(v_key,v.authority_id,v_sha,v_bytes)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects WHERE object_key=v_key AND authority_id=v.authority_id AND sha256=v_sha AND bytes=v_bytes AND cleanup_state='live')THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: immutable object intent changed';END IF;
 RETURN jsonb_build_object('registered',true);
END $register$;
CREATE FUNCTION sessions.ingest_history_require_model_permit(p jsonb)RETURNS sessions.ingest_history_scope_versions
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $permit$
DECLARE e jsonb;v sessions.ingest_history_scope_versions%ROWTYPE;
BEGIN
 e:=jsonb_build_object('generation',p->'generation','identity',p->'identity','source',p->'source','authorityId',p->'authorityId','maskSha256',p->'maskSha256');
 IF nullif(current_setting('relay_history.source_permit',true),'')IS NULL OR current_setting('relay_history.source_permit',true)::jsonb IS DISTINCT FROM e THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: final source transaction permit required';END IF;
 v:=sessions.ingest_history_source_current(e,true);RETURN v;
END $permit$;
CREATE FUNCTION sessions.session_history_summary_publish(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $summary$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;x jsonb;v_at timestamptz;v_n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','pointer']);v:=sessions.ingest_history_require_model_permit(p);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('applied',false);END IF;
 x:=p->'pointer';PERFORM sessions.ingest_exact_keys(x,ARRAY['kind','status','objectKey','objectSha256','objectBytes','keyId','model','builtAt']);
 IF x->>'kind'NOT IN('short','long')OR x->>'status'NOT IN('ready','disabled','quarantined','empty','unsupported')OR jsonb_typeof(x->'builtAt')IS DISTINCT FROM'string'
  OR NOT(x->'model'='null'::jsonb OR(jsonb_typeof(x->'model')='string'AND octet_length(x->>'model')BETWEEN 1 AND 1024))THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: summary metadata';END IF;
 v_at:=(x->>'builtAt')::timestamptz;
 IF x->>'status'='ready'THEN
  IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects o WHERE o.object_key=x->>'objectKey'AND o.authority_id=v.authority_id
   AND o.sha256=x->>'objectSha256'AND o.bytes=(x->>'objectBytes')::bigint AND o.cleanup_state='live'
   AND left(o.object_key,length(v.derived_anchor_key)-length('manifest.json')+length('derived/summary-'||(x->>'kind')||'/'))=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/summary-'||(x->>'kind')||'/')OR jsonb_typeof(x->'keyId')IS DISTINCT FROM'string'OR octet_length(x->>'keyId')NOT BETWEEN 1 AND 1024 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: registered encrypted summary required';END IF;
 ELSE
  IF x->'objectKey'IS DISTINCT FROM'null'::jsonb OR x->'objectSha256'IS DISTINCT FROM'null'::jsonb OR x->'objectBytes'IS DISTINCT FROM'null'::jsonb OR x->'keyId'IS DISTINCT FROM'null'::jsonb THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: nonready summary pointer';END IF;
 END IF;
 INSERT INTO sessions.ingest_history_summaries AS q VALUES(v.org_id,v.workspace_id,v.harness,v.session_id,x->>'kind',v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256,x->>'status',x->>'objectKey',x->>'objectSha256',(x->>'objectBytes')::bigint,x->>'keyId',x->>'model',v_at)
 ON CONFLICT(org_id,workspace_id,harness,session_id,kind)DO UPDATE SET authority_id=excluded.authority_id,source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,
  mask_sha256=excluded.mask_sha256,status=excluded.status,object_key=excluded.object_key,object_sha256=excluded.object_sha256,object_bytes=excluded.object_bytes,key_id=excluded.key_id,model=excluded.model,built_at=excluded.built_at
 WHERE q.authority_id<>excluded.authority_id OR q.built_at<=excluded.built_at;
 GET DIAGNOSTICS v_n=ROW_COUNT;RETURN jsonb_build_object('applied',v_n=1);
END $summary$;
CREATE FUNCTION sessions.session_history_state_publish(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $state$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;v_at timestamptz;v_n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','state','classifiedAt']);v:=sessions.ingest_history_require_model_permit(p);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('applied',false);END IF;
 IF p->>'state'NOT IN('active','done','abandoned','unknown')OR jsonb_typeof(p->'classifiedAt')IS DISTINCT FROM'string'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: state metadata';END IF;
 v_at:=(p->>'classifiedAt')::timestamptz;
 INSERT INTO sessions.ingest_history_states AS q VALUES(v.org_id,v.workspace_id,v.harness,v.session_id,v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256,p->>'state',v_at)
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET authority_id=excluded.authority_id,source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,
  mask_sha256=excluded.mask_sha256,state=excluded.state,classified_at=excluded.classified_at WHERE q.authority_id<>excluded.authority_id OR q.classified_at<=excluded.classified_at;
 GET DIAGNOSTICS v_n=ROW_COUNT;
 IF v_n=1 THEN UPDATE sessions.session_directory SET state=p->>'state'WHERE org_id=v.org_id AND workspace_id=v.workspace_id AND harness=v.harness AND session_id=v.session_id AND deleted_at IS NULL;END IF;
 RETURN jsonb_build_object('applied',v_n=1);
END $state$;
REVOKE ALL ON FUNCTION sessions.ingest_history_mask_register(jsonb),sessions.ingest_history_mask_rows(text,bigint,integer),sessions.ingest_history_source_current(jsonb,boolean),sessions.ingest_history_require_model_permit(jsonb),
 sessions.session_history_source_lock(jsonb),sessions.session_history_mask_page(jsonb),sessions.session_history_register_object(jsonb),sessions.session_history_summary_publish(jsonb),sessions.session_history_state_publish(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_mask_register(jsonb)TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.session_history_source_lock(jsonb),sessions.session_history_mask_page(jsonb),sessions.session_history_register_object(jsonb),sessions.session_history_summary_publish(jsonb),sessions.session_history_state_publish(jsonb)TO bulls96_session_ingest;


CREATE FUNCTION sessions.session_history_capture(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capture$
DECLARE c jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity']);PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM sessions.ingest_exact_keys(p->'identity',ARRAY['orgId','workspaceId','source','sessionId']);
 c:=sessions.capture_session_replay_pages(p#>>'{identity,orgId}',p#>>'{identity,workspaceId}',p#>>'{identity,source}',p#>>'{identity,sessionId}');
 IF c#>>'{snapshot,history,required}'='true'AND c#>>'{snapshot,history,ready}'IS DISTINCT FROM'true'THEN c:=jsonb_set(c,'{blocked}','true'::jsonb);END IF;
 RETURN c;
END $capture$;
CREATE FUNCTION sessions.session_history_summary_get(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $get$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;q sessions.ingest_history_summaries%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','kind']);v:=sessions.ingest_history_source_current(p,false);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('current',false,'pointer',NULL);END IF;
 IF p->>'kind'NOT IN('short','long')THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: summary kind';END IF;
 SELECT *INTO q FROM sessions.ingest_history_summaries WHERE org_id=v.org_id AND workspace_id=v.workspace_id AND harness=v.harness AND session_id=v.session_id
  AND kind=p->>'kind'AND authority_id=v.authority_id AND source_epoch=v.source_epoch AND source_fingerprint=v.source_fingerprint AND mask_sha256=v.mask_sha256;
 RETURN jsonb_build_object('current',true,'pointer',CASE WHEN FOUND THEN jsonb_build_object('kind',q.kind,'status',q.status,'objectKey',q.object_key,'objectSha256',q.object_sha256,'objectBytes',q.object_bytes,'keyId',q.key_id,'model',q.model,'builtAt',q.built_at)ELSE NULL END);
END $get$;
REVOKE ALL ON FUNCTION sessions.session_history_capture(jsonb),sessions.session_history_summary_get(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_capture(jsonb),sessions.session_history_summary_get(jsonb)TO bulls96_session_ingest;

CREATE OR REPLACE FUNCTION sessions.publish_session_replay_pages(
 p_org text,p_workspace text,p_harness text,p_session text,p_epoch bigint,p_fingerprint text,p_snapshot jsonb,
 p_generation text,p_source_generation text,p_prefix text,p_index_key text,p_index_sha256 text,p_index_bytes integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $publish$
DECLARE live jsonb; fp text; anchor jsonb; expected_prefix text; history_anchor text;
BEGIN
 IF EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session)THEN
  IF nullif(current_setting('relay_history.source_permit',true),'')IS NULL THEN RETURN false;END IF;
  DECLARE e jsonb;v sessions.ingest_history_scope_versions%ROWTYPE;
  BEGIN
   e:=current_setting('relay_history.source_permit',true)::jsonb;
   IF e#>>'{identity,orgId}'IS DISTINCT FROM p_org OR e#>>'{identity,workspaceId}'IS DISTINCT FROM p_workspace OR e#>>'{identity,source}'IS DISTINCT FROM p_harness OR e#>>'{identity,sessionId}'IS DISTINCT FROM p_session
    OR e#>>'{source,epoch}'IS DISTINCT FROM p_epoch::text OR e#>>'{source,fingerprint}'IS DISTINCT FROM p_fingerprint
    OR e->>'authorityId'IS DISTINCT FROM p_snapshot#>>'{history,authorityId}'OR e->>'maskSha256'IS DISTINCT FROM p_snapshot#>>'{history,maskSha256}'THEN RETURN false;END IF;
   v:=sessions.ingest_history_source_current(e,true);IF v.authority_id IS NULL THEN RETURN false;END IF;
   IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects o WHERE o.authority_id=v.authority_id
    AND o.object_key=p_index_key AND o.sha256=p_index_sha256 AND o.bytes=p_index_bytes AND o.cleanup_state='live')THEN RETURN false;END IF;
   history_anchor:=v.derived_anchor_key;
  END;
 END IF;
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
 expected_prefix:=CASE WHEN history_anchor IS NOT NULL THEN left(history_anchor,length(history_anchor)-length('manifest.json'))||'derived/replay/'||p_generation||'/'
 ELSE regexp_replace(anchor->>'manifest_key','manifest\.json$','')||'normalized/read-projection-v1/'||(anchor->>'content_sha256')||'/'||p_generation||'/'END;
 IF p_prefix IS DISTINCT FROM expected_prefix OR p_index_key IS DISTINCT FROM p_prefix||p_index_sha256||'.json' THEN RETURN false; END IF;
 UPDATE sessions.session_replay_publications SET published_epoch=p_epoch,source_fingerprint=fp,source_snapshot=live,
  generation=p_generation,source_generation=p_source_generation,artifact_prefix=p_prefix,index_key=p_index_key,index_sha256=p_index_sha256,index_bytes=p_index_bytes,published_at=now()
  WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=p_session AND epoch=p_epoch;
 RETURN FOUND;
END $publish$;

CREATE FUNCTION sessions.session_history_replay_publish(p jsonb)RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $replay$
DECLARE e jsonb;v sessions.ingest_history_scope_versions%ROWTYPE;a jsonb;snapshot jsonb;ok boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','parameters','largeLease']);
 e:=p-ARRAY['parameters','largeLease'];v:=sessions.ingest_history_source_current(e,true);IF v.authority_id IS NULL THEN RETURN false;END IF;
 a:=p->'parameters';IF jsonb_typeof(a)IS DISTINCT FROM'array'OR jsonb_array_length(a)<>13 OR octet_length(a::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: replay publication parameters';END IF;
 snapshot:=CASE WHEN jsonb_typeof(a->6)='string'THEN(a->>6)::jsonb ELSE a->6 END;
 IF a->>0 IS DISTINCT FROM v.org_id OR a->>1 IS DISTINCT FROM v.workspace_id OR a->>2 IS DISTINCT FROM v.harness OR a->>3 IS DISTINCT FROM v.session_id
  OR a->>4 IS DISTINCT FROM v.source_epoch::text OR a->>5 IS DISTINCT FROM v.source_fingerprint OR snapshot#>>'{history,authorityId}'IS DISTINCT FROM v.authority_id::text THEN RETURN false;END IF;
 PERFORM set_config('relay_history.source_permit',e::text,true);
 IF p->'largeLease'='null'::jsonb THEN
  ok:=sessions.publish_session_replay_pages(a->>0,a->>1,a->>2,a->>3,(a->>4)::bigint,a->>5,snapshot,a->>7,a->>8,a->>9,a->>10,a->>11,(a->>12)::integer);
 ELSE
  ok:=sessions.publish_session_large_read(a->>0,a->>1,a->>2,a->>3,(a->>4)::bigint,a->>5,snapshot,a->>7,a->>8,a->>9,a->>10,a->>11,(a->>12)::integer,(p->>'largeLease')::uuid);
 END IF;
 RETURN ok;
END $replay$;
REVOKE ALL ON FUNCTION sessions.session_history_replay_publish(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_replay_publish(jsonb)TO bulls96_session_ingest;


-- Model workers consume exactly the current masked graph through backend-only
-- metadata, without manufacturing JWT claims or reading publication tables.
CREATE FUNCTION sessions.session_history_replay_descriptor(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $descriptor$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;a sessions.session_replay_publications%ROWTYPE;live jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256']);
 v:=sessions.ingest_history_source_current(p,false);
 IF v.authority_id IS NULL THEN RETURN jsonb_build_object('current',false,'publication',NULL);END IF;
 SELECT *INTO a FROM sessions.session_replay_publications WHERE org_id=v.org_id AND workspace_id=v.workspace_id AND harness=v.harness AND session_id=v.session_id;
 IF NOT FOUND OR a.index_key IS NULL OR a.epoch IS DISTINCT FROM v.source_epoch OR a.published_epoch IS DISTINCT FROM v.source_epoch
  OR a.source_fingerprint IS DISTINCT FROM v.source_fingerprint THEN RETURN jsonb_build_object('current',true,'publication',NULL);END IF;
 live:=sessions.session_replay_source_snapshot(v.org_id,v.workspace_id,v.harness,v.session_id);
 IF a.source_snapshot IS DISTINCT FROM live OR (sessions.ingest_history_source_current(p,false)).authority_id IS NULL THEN RETURN jsonb_build_object('current',false,'publication',NULL);END IF;
 RETURN jsonb_build_object('current',true,'publication',jsonb_build_object('version',1,'identity',p->'identity','generation',a.generation,
  'source',p->'source','prefix',a.artifact_prefix,'index',jsonb_build_object('key',a.index_key,'sha256',a.index_sha256,'bytes',a.index_bytes)));
END $descriptor$;
REVOKE ALL ON FUNCTION sessions.session_history_replay_descriptor(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_replay_descriptor(jsonb)TO bulls96_session_ingest;

-- Internal COW traversal exposes only one bounded immutable metadata page from
-- the explicit workspace. No installed scope/root visibility is implied.
CREATE FUNCTION sessions.ingest_history_mask_get(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $mask_get$
DECLARE d jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','orgId','workspaceId','sha256']);
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'sha256')IS DISTINCT FROM'string'OR p->>'sha256'!~'^[a-f0-9]{64}$'THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: mask digest';END IF;
 SELECT document INTO d FROM sessions.ingest_history_mask_pages WHERE sha256=p->>'sha256'AND org_id=p->>'orgId'AND workspace_id=p->>'workspaceId';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: unknown mask page';END IF;
 RETURN d;
END $mask_get$;
REVOKE ALL ON FUNCTION sessions.ingest_history_mask_get(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_history_mask_get(jsonb)TO bulls96_session_ingest;


CREATE FUNCTION sessions.session_history_register_artifact(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $artifact$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;prefix text;tail text;k text;digest text;n bigint;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','identity','source','authorityId','maskSha256','key','sha256','bytes']);
 v:=sessions.ingest_history_source_current(p,true);
 IF v.authority_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: artifact source changed';END IF;
 prefix:=left(v.derived_anchor_key,length(v.derived_anchor_key)-length('manifest.json'))||'derived/replay/';
 k:=p->>'key';tail:=substr(k,length(prefix)+1);digest:=p->>'sha256';
 IF jsonb_typeof(p->'key')IS DISTINCT FROM'string'OR left(k,length(prefix))<>prefix
  OR tail!~'^[a-f0-9]{64}/[a-f0-9]{64}[.](json|bin)$'OR split_part(split_part(tail,'/',2),'.',1)IS DISTINCT FROM digest
  OR jsonb_typeof(p->'sha256')IS DISTINCT FROM'string'OR digest!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p->'bytes')IS DISTINCT FROM'number'OR(p->>'bytes')::numeric<>trunc((p->>'bytes')::numeric)
  OR(p->>'bytes')::numeric NOT BETWEEN 1 AND 16777216 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='history: artifact object proof';END IF;
 n:=(p->>'bytes')::bigint;
 INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes)VALUES(k,v.authority_id,digest,n)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects WHERE object_key=k AND authority_id=v.authority_id AND sha256=digest AND bytes=n AND cleanup_state='live')THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: artifact intent changed';END IF;
 RETURN jsonb_build_object('registered',true);
END $artifact$;
REVOKE ALL ON FUNCTION sessions.session_history_register_artifact(jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.session_history_register_artifact(jsonb)TO bulls96_session_ingest;


-- Captured source owners are explicit, including prior owners whose last row
-- disappeared. Controls invalidate legacy-only scopes even when no lazy native
-- slot or buffered payload has ever existed for the affected record.
CREATE TABLE sessions.ingest_history_scope_owners(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 control_epoch text NOT NULL CHECK(control_epoch~'^[a-f0-9]{64}$'),control_version bigint NOT NULL CHECK(control_version>=0),
 PRIMARY KEY(org_id,workspace_id,user_id,harness,session_id)
);
CREATE TABLE sessions.ingest_history_privacy_outbox(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,control_epoch text NOT NULL,
 control_version bigint NOT NULL,request_version bigint NOT NULL DEFAULT 1,state text NOT NULL DEFAULT'pending'CHECK(state IN('pending','leased','complete')),
 lease_token uuid,lease_until timestamptz,lease_generation text,requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
 PRIMARY KEY(org_id,workspace_id,user_id),CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
ALTER TABLE sessions.ingest_history_scope_owners ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_history_privacy_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_history_scope_owners,sessions.ingest_history_privacy_outbox FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_history_invalidate_scope(p_org text,p_ws text,p_harness text,p_session text)RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $invalidate$
BEGIN
 IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',p_org,p_ws,p_harness,p_session),0))THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: logical scope busy';END IF;
 UPDATE sessions.ingest_history_scope_current SET ready=false WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_harness AND session_id=p_session;
 UPDATE sessions.session_replay_publications SET epoch=nextval('sessions.session_replay_epoch_seq')WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_harness AND session_id=p_session;
 UPDATE sessions.ingest_history_derived_objects o SET cleanup_state='retired',row_version=row_version+1 FROM sessions.ingest_history_scope_versions v
  WHERE o.authority_id=v.authority_id AND v.org_id=p_org AND v.workspace_id=p_ws AND v.harness=p_harness AND v.session_id=p_session AND o.cleanup_state='live';
END $invalidate$;
CREATE FUNCTION sessions.ingest_history_control_changed()RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $privacy$
DECLARE r record;
BEGIN
 FOR r IN SELECT z.*FROM(
  SELECT DISTINCT o.org_id,o.workspace_id,o.harness,o.session_id FROM history_controls n JOIN sessions.ingest_history_scope_owners o
   ON o.org_id=n.org_id AND o.workspace_id=n.workspace_id AND o.user_id=n.user_id
   WHERE n.epoch<>o.control_epoch OR n.version>o.control_version
 )z ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
 LOOP PERFORM sessions.ingest_history_invalidate_scope(r.org_id,r.workspace_id,r.harness,r.session_id);END LOOP;
 INSERT INTO sessions.ingest_history_privacy_outbox(org_id,workspace_id,user_id,control_epoch,control_version)
  SELECT DISTINCT ON(org_id,workspace_id,user_id)org_id,workspace_id,user_id,epoch,version FROM history_controls ORDER BY org_id,workspace_id,user_id,version DESC
 ON CONFLICT(org_id,workspace_id,user_id)DO UPDATE SET control_epoch=excluded.control_epoch,control_version=excluded.control_version,
  request_version=sessions.ingest_history_privacy_outbox.request_version+1,state='pending',lease_token=NULL,lease_until=NULL,lease_generation=NULL,requested_at=clock_timestamp(),completed_at=NULL;
 RETURN NULL;
END $privacy$;
CREATE TRIGGER ingest_history_control_changed AFTER INSERT ON sessions.ingest_control_receipts
 REFERENCING NEW TABLE AS history_controls FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_history_control_changed();
REVOKE ALL ON FUNCTION sessions.ingest_history_invalidate_scope(text,text,text,text),sessions.ingest_history_control_changed()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
