-- Reverse of ingest_privacy_authority (MIGRATION SLOT TBD; see the up file). Only valid while Neon is the
-- privacy authority (no sessions.ingest_privacy_authority row): once Supabase was active, heads and frozen
-- privacy may be ahead of Neon and dropping the authority would silently hand privacy back to a stale Neon.
-- When the slot is assigned, add the usual later-migration guard and the __96bulls_migrations ledger cleanup.
DO $rollback_guard$
BEGIN
 IF to_regclass('sessions.ingest_privacy_authority') IS NOT NULL
  AND EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='privacy authority: release to neon first (ingest_privacy_authority_release)';END IF;
END $rollback_guard$;
DROP TRIGGER IF EXISTS ingest_privacy_authority_guard ON sessions.ingest_source_control_heads;
DROP TRIGGER IF EXISTS ingest_privacy_authority_guard ON sessions.ingest_frozen_source_deletions;
DROP TRIGGER IF EXISTS ingest_privacy_authority_guard ON sessions.ingest_frozen_source_resets;
DROP TRIGGER IF EXISTS ingest_privacy_authority_guard ON sessions.ingest_frozen_source_records;
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_begin(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_complete(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_apply(text,text,text,text,jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_release(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_activate(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_fence(jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_state();
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_packet(jsonb,jsonb,jsonb);
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_guard();
-- The legacy begin/complete as created by 20261105010000 (verbatim below); they reference the lock function, so restore them first.
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
DROP INDEX IF EXISTS sessions.ingest_frozen_source_records_user_session;
DROP FUNCTION IF EXISTS sessions.ingest_privacy_authority_lock(boolean);
DROP TABLE IF EXISTS sessions.ingest_privacy_authority;
ALTER TABLE sessions.ingest_source_control_heads DROP COLUMN IF EXISTS deleted;
