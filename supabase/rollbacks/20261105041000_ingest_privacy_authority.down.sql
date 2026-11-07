-- Generated: reverse 041000. Only while Neon is the privacy authority (no sessions.ingest_privacy_authority row):
-- once Supabase was active its heads/frozen privacy may be ahead of Neon, so dropping the authority would hand
-- privacy back to a stale Neon. Release first (sessions.ingest_privacy_authority_release).
-- Explicit transaction: guards, marker fence, restore and ledger removal are atomic.
-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: migration ledger/marker required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105041000_ingest_privacy_authority.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105041000_ingest_privacy_authority.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'a02de29a8a1ecc263f8d8868044ee9e9da07368e4fb6715ec3e2f125de9a3b75'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: own applied checksum required';END IF;
 PERFORM pg_advisory_xact_lock(1919249529,96); -- the authority lock: no begin/transition in flight
 IF EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: release to neon first (ingest_privacy_authority_release)';END IF;
 UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: validation marker required';END IF;
END $rollback_guard$;
DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_source_control_heads;
DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_deletions;
DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_resets;
DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_records;
DROP TRIGGER ingest_privacy_authority_guard ON sessions.ingest_frozen_source_owned_sessions;
DROP FUNCTION sessions.ingest_privacy_eraser_still_fenced(jsonb);
DROP FUNCTION sessions.ingest_privacy_eraser_page(jsonb);
DROP FUNCTION sessions.ingest_privacy_eraser_groups();
DROP FUNCTION sessions.ingest_privacy_eraser_require_active();
DROP FUNCTION sessions.ingest_privacy_eraser_state();
REVOKE USAGE ON SCHEMA sessions FROM bulls96_privacy_eraser;
DROP FUNCTION sessions.ingest_privacy_authority_begin(jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_complete(jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_apply(text,text,text,text,jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_purge_derived(text,text);
DROP FUNCTION sessions.ingest_privacy_authority_release(jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_activate(jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_fence(jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_state();
DROP FUNCTION sessions.ingest_privacy_authority_owned_digest();
DROP FUNCTION sessions.ingest_privacy_authority_packet(jsonb,jsonb,jsonb);
DROP FUNCTION sessions.ingest_privacy_authority_guard();
-- The legacy mirror begin/complete exactly as 20261105010000 created them (they call the lock function: restore first).
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
CREATE OR REPLACE FUNCTION sessions.ingest_source_control_complete(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $control_complete$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;h sessions.ingest_source_control_heads%ROWTYPE;
 v_org text;v_ws text;v_user text;v_version bigint;v_outcome text;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['epoch','tenant','version','digest','kind','outcome']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);v_outcome:=p->>'outcome';
 IF p->>'version'!~'^[1-9][0-9]{0,18}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='frozen source: invalid control';END IF;
 v_version:=(p->>'version')::bigint;
 pol:=sessions.ingest_frozen_source_policy();
 IF p->>'epoch' IS DISTINCT FROM pol.control_epoch OR v_outcome IS NULL OR v_outcome NOT IN('applied','replay','stale') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: control held';END IF;
 SELECT * INTO STRICT h FROM sessions.ingest_source_control_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user FOR UPDATE;
 IF h.epoch IS DISTINCT FROM p->>'epoch' OR v_version>h.version THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control';END IF;
 IF v_version<h.version THEN RETURN jsonb_build_object('completed',true,'superseded',true);END IF;
 IF v_outcome NOT IN('applied','replay') OR h.digest IS DISTINCT FROM p->>'digest' OR h.kind IS DISTINCT FROM p->>'kind' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='frozen source: invalid control';END IF;
 UPDATE sessions.ingest_source_control_heads SET pending=false,updated_at=clock_timestamp()
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 RETURN jsonb_build_object('completed',true,'superseded',false);
END $control_complete$;
-- The lazy-current adoption checks exactly as 20261105040000 left them (prefix exclusion).
CREATE OR REPLACE FUNCTION sessions.ingest_lazy_user(p_w jsonb,p_kind text,p_control jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_user$
DECLARE v_org text;v_ws text;v_user text;v_epoch text;v_source text;v_first boolean;v_last bigint;v_version bigint;v_privacy_version bigint;
 s sessions.ingest_lazy_user_seals%ROWTYPE;b sessions.ingest_seed_fence_baselines%ROWTYPE;v_privacy jsonb;v_d jsonb;v_r jsonb;v_fp text;k record;v_exists boolean;
BEGIN
 v_org:=sessions.ingest_identifier(p_w#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p_w#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p_w#>'{tenant,userId}',false);v_epoch:=sessions.ingest_digest(p_w->'controlEpoch');
 v_source:=sessions.ingest_digest(p_w->'sourceIdentitySha256');
 IF jsonb_typeof(p_w->'sourceControlVersion') IS DISTINCT FROM 'string' OR p_w->>'sourceControlVersion'!~'^(0|[1-9][0-9]{0,18})$'
  OR jsonb_typeof(p_w->'privacyVersion') IS DISTINCT FROM 'string' OR p_w->>'privacyVersion'!~'^(0|[1-9][0-9]{0,18})$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: witness version shape';END IF;
 v_version:=(p_w->>'sourceControlVersion')::bigint;v_privacy_version:=(p_w->>'privacyVersion')::bigint;
 INSERT INTO sessions.ingest_fence_owners VALUES(v_org,v_ws,v_user) ON CONFLICT DO NOTHING RETURNING true INTO v_first;
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 SELECT coalesce(max(version),0) INTO v_last FROM sessions.ingest_control_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND epoch=v_epoch;
 IF p_kind='batch' THEN
  IF p_w->'pending' IS DISTINCT FROM 'false'::jsonb OR p_w->'pendingControl' IS DISTINCT FROM 'null'::jsonb
   OR v_version<>v_last OR v_privacy_version<>v_version THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control version changed';END IF;
 ELSE
  IF p_w->'pending' IS DISTINCT FROM 'true'::jsonb OR p_w->'pendingControl' IS DISTINCT FROM p_control
   OR p_control->>'epoch' IS DISTINCT FROM v_epoch OR p_control->>'kind' IS DISTINCT FROM p_kind
   OR p_control->>'version' IS DISTINCT FROM v_version::text OR v_version=0 OR v_privacy_version<>v_version-1 THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: pending source control mismatch';END IF;
  IF v_last<v_version THEN
   IF v_last=9223372036854775807 OR v_last+1<>v_version THEN
    RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control gap';END IF;
  ELSE
   PERFORM 1 FROM sessions.ingest_control_receipts WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
    AND epoch=v_epoch AND version=v_version AND kind=p_kind AND digest=p_control->>'digest';
   IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control replay mismatch';END IF;
  END IF;
 END IF;
 SELECT * INTO s FROM sessions.ingest_lazy_user_seals WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
 IF FOUND THEN
  SELECT * INTO b FROM sessions.ingest_seed_fence_baselines WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user;
  IF NOT FOUND OR s.source_identity_sha256<>v_source OR s.control_epoch<>v_epoch OR s.privacy_fingerprint<>b.fingerprint THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: existing user privacy authority changed';END IF;
  RETURN jsonb_build_object('privacySealed',true,'created',false,'targetVersion',v_last::text);
 END IF;
 -- An already-completed source control implies a previously sealed target.
 -- Lost authority cannot silently become a fresh user merely because rows vanished.
 IF v_first IS DISTINCT FROM true OR v_last<>0 OR v_privacy_version<>0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unsealed user authority cannot be adopted';END IF;
 FOR k IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='sessions' AND c.relkind='r' AND (c.relname LIKE 'ingest_%' OR c.relname LIKE 'upload_%')
   AND c.relname<>'ingest_fence_owners'
   -- Source-side state (the frozen Neon copy and the Supabase-first control heads) is not prior TARGET authority.
   AND c.relname NOT LIKE 'ingest\_frozen\_source\_%' AND c.relname<>'ingest_source_control_heads'
   AND NOT EXISTS(SELECT 1 FROM unnest(ARRAY['org_id','workspace_id','user_id']) col
    WHERE NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname=col AND a.attnum>0 AND NOT a.attisdropped))
 LOOP
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM sessions.%I WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3)',k.relname)
   INTO v_exists USING v_org,v_ws,v_user;
  IF v_exists THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: prior user authority requires reviewed recovery';END IF;
 END LOOP;
 v_privacy:=p_w->'privacy';
 PERFORM sessions.ingest_exact_keys(v_privacy,ARRAY['complete','deletions','resets']);
 IF v_privacy->'complete' IS DISTINCT FROM 'true'::jsonb OR octet_length(v_privacy::text)>524288
  OR jsonb_typeof(v_privacy->'deletions') IS DISTINCT FROM 'array' OR jsonb_typeof(v_privacy->'resets') IS DISTINCT FROM 'array'
  OR jsonb_array_length(v_privacy->'deletions')+jsonb_array_length(v_privacy->'resets')>5000 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: complete bounded cold privacy required';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_privacy->'deletions') q(e) WHERE CASE WHEN jsonb_typeof(e)<>'object' THEN true ELSE
  ARRAY(SELECT x FROM jsonb_object_keys(e)x ORDER BY x COLLATE "C") IS DISTINCT FROM ARRAY['origin_id','session_id','source']::text[] END)
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_privacy->'resets')q(e) WHERE CASE WHEN jsonb_typeof(e)<>'object' THEN true ELSE
  ARRAY(SELECT x FROM jsonb_object_keys(e)x ORDER BY x COLLATE "C") IS DISTINCT FROM ARRAY['deleted','request_id']::text[]
  OR jsonb_typeof(e->'deleted') IS DISTINCT FROM 'string' OR e->>'deleted'!~'^(0|[1-9][0-9]{0,18})$' END)
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid cold privacy metadata';END IF;
 SELECT coalesce(jsonb_agg(x ORDER BY x::text COLLATE "C"),'[]') INTO v_d FROM(
  SELECT DISTINCT jsonb_build_object('origin_id',sessions.ingest_identifier(e->'origin_id',false),
   'source',sessions.ingest_identifier(e->'source',false),'session_id',sessions.ingest_identifier(e->'session_id',false)) x
  FROM jsonb_array_elements(v_privacy->'deletions')q(e))q;
 SELECT coalesce(jsonb_agg(x ORDER BY x::text COLLATE "C"),'[]') INTO v_r FROM(
  SELECT DISTINCT jsonb_build_object('request_id',sessions.ingest_identifier(e->'request_id',false),'deleted',(e->>'deleted')::bigint) x
  FROM jsonb_array_elements(v_privacy->'resets')q(e))q;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_r)x GROUP BY x->>'request_id' HAVING count(*)>1) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: conflicting reset metadata';END IF;
 v_fp:=encode(sha256(convert_to(jsonb_build_array('neon-fence-baseline-v1',v_org,v_ws,v_user,v_d,v_r)::text,'UTF8')),'hex');
 INSERT INTO sessions.ingest_fence_versions(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
  SELECT v_org,v_ws,v_user,x.origin_id,x.source,x.session_id,1 FROM jsonb_to_recordset(v_d)x(origin_id text,source text,session_id text);
 INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT v_org,v_ws,v_user,x.origin_id,x.source,x.session_id FROM jsonb_to_recordset(v_d)x(origin_id text,source text,session_id text);
 INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id,deleted)
  SELECT v_org,v_ws,v_user,x.request_id,x.deleted FROM jsonb_to_recordset(v_r)x(request_id text,deleted bigint);
 INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint,imported_at)
  VALUES(v_org,v_ws,v_user,v_fp,clock_timestamp());
 INSERT INTO sessions.ingest_lazy_user_seals(org_id,workspace_id,user_id,source_identity_sha256,control_epoch,privacy_fingerprint,initial_privacy_version)
  VALUES(v_org,v_ws,v_user,v_source,v_epoch,v_fp,v_privacy_version);
 RETURN jsonb_build_object('privacySealed',true,'created',true,'targetVersion',v_last::text);
END $lazy_user$;
CREATE OR REPLACE FUNCTION sessions.ingest_lazy_begin(p jsonb,p_kind text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_begin$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;
 v_gen text;v_packet jsonb;w jsonb;v_org text;v_ws text;v_user text;v_new boolean;v_exists boolean;k record;
 v_records jsonb;v_count integer;v_seen integer;v_ack jsonb;
BEGIN
 IF p_kind NOT IN('batch','selection','reset') OR p_kind IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid witness operation';END IF;
 v_gen:=sessions.ingest_digest(p->'generation');PERFORM sessions.ingest_require_generation(v_gen);
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF pol.generation<>v_gen OR pol.valid_until<=clock_timestamp() OR pol.source_identity_sha256<>c.source_identity_sha256
  OR pol.control_epoch<>c.control_epoch OR pol.source_proof_sha256 IS DISTINCT FROM c.baseline->>'sourceProofSha256' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody policy unavailable';END IF;
 v_packet:=p->'custody';PERFORM sessions.ingest_exact_keys(v_packet,ARRAY['witness','sourceDigest']);w:=v_packet->'witness';
 PERFORM sessions.ingest_exact_keys(w,ARRAY['version','issuer','sourceIdentitySha256','controlEpoch','privacyBaselineSha256',
  'tenant','originId','batchId','batchDigest','sourceControlVersion','pending','pendingControl','privacyVersion',
  'originalRecordCount','candidateCount','records','privacy']);
 IF encode(sha256(convert_to(w::text,'UTF8')),'hex')<>sessions.ingest_digest(v_packet->'sourceDigest')
  OR w->'version' IS DISTINCT FROM '1'::jsonb OR w->>'issuer' IS DISTINCT FROM pol.issuer
  OR w->>'sourceIdentitySha256' IS DISTINCT FROM pol.source_identity_sha256 OR w->>'controlEpoch' IS DISTINCT FROM pol.control_epoch
  OR w->>'privacyBaselineSha256' IS DISTINCT FROM pol.privacy_baseline_sha256 OR w->'tenant' IS DISTINCT FROM p->'tenant' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source witness identity mismatch';END IF;
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 IF jsonb_typeof(w->'records') IS DISTINCT FROM 'array' OR octet_length((w->'records')::text)>4194304
  OR jsonb_typeof(w->'candidateCount') IS DISTINCT FROM 'number' OR w->>'candidateCount'!~'^(0|[1-9][0-9]{0,2})$'
  OR jsonb_typeof(w->'originalRecordCount') IS DISTINCT FROM 'number' OR w->>'originalRecordCount'!~'^(0|[1-9][0-9]{0,2})$'
  OR (w->>'candidateCount')::integer>500 OR (w->>'originalRecordCount')::integer>500
  OR (w->>'candidateCount')::integer>(w->>'originalRecordCount')::integer
  OR jsonb_array_length(w->'records')<>(w->>'candidateCount')::integer THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: source witness record bound';END IF;
 IF p_kind='batch' THEN
  v_records:=sessions.ingest_records(p->'records');v_count:=jsonb_array_length(v_records);
  IF w->>'originId' IS DISTINCT FROM p->>'originId' OR w->>'batchId' IS DISTINCT FROM p->>'batchId'
   OR w->>'batchDigest' IS DISTINCT FROM p->>'batchDigest' OR v_count<>(w->>'candidateCount')::integer THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source witness batch mismatch';END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(w->'records')q(e) WHERE CASE WHEN jsonb_typeof(e)<>'object' THEN true ELSE
   ARRAY(SELECT x FROM jsonb_object_keys(e)x ORDER BY x COLLATE "C") IS DISTINCT FROM
    ARRAY['candidateDigest','candidateFenced','current','currentFenced','preparedOrd','processingRank','recordId','revision','revisionId']::text[]
   OR jsonb_typeof(e->'preparedOrd') IS DISTINCT FROM 'number' OR e->>'preparedOrd'!~'^(0|[1-9][0-9]{0,2})$'
   OR jsonb_typeof(e->'processingRank') IS DISTINCT FROM 'number' OR e->>'processingRank'!~'^(0|[1-9][0-9]{0,2})$'
   OR jsonb_typeof(e->'candidateFenced') IS DISTINCT FROM 'boolean' OR jsonb_typeof(e->'currentFenced') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(e->'current') NOT IN('object','null') END)
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: source witness record shape';END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(w->'records')q(e)
   WHERE (e->>'preparedOrd')::integer>=v_count OR (e->>'processingRank')::integer>=v_count)
   OR (SELECT count(DISTINCT e->>'preparedOrd') FROM jsonb_array_elements(w->'records')q(e))<>v_count
   OR (SELECT count(DISTINCT e->>'processingRank') FROM jsonb_array_elements(w->'records')q(e))<>v_count THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source witness rank coverage';END IF;
  SELECT count(*)::integer INTO v_seen FROM jsonb_array_elements(w->'records')q(e)
   JOIN jsonb_to_recordset(v_records)r(ord integer,record_id text,revision bigint,revision_id text,digest text,session_id text)
   ON r.ord=(e->>'preparedOrd')::integer AND r.record_id=e->>'recordId' AND r.revision::text=e->>'revision'
    AND r.revision_id=e->>'revisionId' AND r.digest=e->>'candidateDigest'
   WHERE NOT((e->>'candidateFenced')::boolean AND r.session_id IS NULL);
  IF v_seen<>v_count THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source candidate metadata mismatch';END IF;
 ELSE
  IF w->'batchId' IS DISTINCT FROM 'null'::jsonb OR w->'batchDigest' IS DISTINCT FROM 'null'::jsonb
   OR w->'originalRecordCount' IS DISTINCT FROM '0'::jsonb OR w->'candidateCount' IS DISTINCT FROM '0'::jsonb
   OR w->'records' IS DISTINCT FROM '[]'::jsonb
   OR (p_kind='reset' AND w->'originId' IS DISTINCT FROM 'null'::jsonb)
   OR (p_kind='selection' AND w->>'originId' IS DISTINCT FROM p->>'originId') THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: source control witness shape';END IF;
 END IF;
 -- The trusted source context permits provisioning, not a claim of empty org
 -- history. Retained closed admissions are never reopened by a runtime request.
 INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)
 VALUES(v_org,pol.privacy_baseline_sha256,pol.retention_contract_sha256,v_gen,pol.valid_until,pol.control_epoch)
 ON CONFLICT DO NOTHING RETURNING true INTO v_new;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org AND privacy_baseline_sha256=pol.privacy_baseline_sha256
  AND retention_contract_sha256=pol.retention_contract_sha256 AND schema_generation=v_gen AND control_epoch=pol.control_epoch
  AND valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: retained org admission closed or conflicting';END IF;
 IF v_new IS TRUE THEN
  FOR k IN SELECT t.relname FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
   WHERE n.nspname='sessions' AND t.relkind='r' AND(t.relname LIKE 'ingest_%' OR t.relname LIKE 'upload_%')
    AND t.relname<>'ingest_buffer_admission'
    -- Source-side state (the frozen Neon copy and the Supabase-first control heads) is not prior TARGET authority.
    AND t.relname NOT LIKE 'ingest\_frozen\_source\_%' AND t.relname<>'ingest_source_control_heads'
    AND EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=t.oid AND a.attname='org_id' AND a.attnum>0 AND NOT a.attisdropped)
  LOOP
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM sessions.%I WHERE org_id=$1)',k.relname) INTO v_exists USING v_org;
   IF v_exists THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unbound prior org authority requires recovery';END IF;
  END LOOP;
 END IF;
 v_ack:=sessions.ingest_lazy_user(w,p_kind,p->'control');
 PERFORM sessions.ingest_require_generation(v_gen);
 IF pol.valid_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: policy expired during operation';END IF;
 RETURN v_ack;
END $lazy_begin$;
DROP FUNCTION sessions.ingest_privacy_authority_lock(boolean);
DROP TABLE sessions.ingest_privacy_authority;
DROP TABLE sessions.ingest_frozen_source_owned_sessions;
ALTER TABLE sessions.ingest_source_control_heads DROP COLUMN deleted;
-- The post-apply index job's index, if it was built (scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql).
DROP INDEX IF EXISTS sessions.ingest_frozen_source_records_user_session;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105041000_ingest_privacy_authority.sql' AND sha256='a02de29a8a1ecc263f8d8868044ee9e9da07368e4fb6715ec3e2f125de9a3b75';
COMMIT;
