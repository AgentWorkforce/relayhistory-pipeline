-- Durable owner/machine authority for the one-shot Neon legacy export.
-- The exporter registers an immutable scope before any S3 PUT through one
-- narrow SECURITY DEFINER function. The hosted eraser reads only through its
-- least-privilege RPCs; neither runtime receives table write privileges.

CREATE TABLE sessions.legacy_export_scope_authority(
 org_id text NOT NULL,
 workspace_id text NOT NULL,
 source text NOT NULL,
 session_id text NOT NULL,
 legacy_user_id text NOT NULL,
 legacy_machine_id text NOT NULL CHECK(legacy_machine_id~'^srv_[0-9a-f]{32}$'),
 PRIMARY KEY(org_id,workspace_id,source,session_id),
 CHECK(org_id<>'' AND workspace_id<>'' AND source<>'' AND session_id<>'' AND legacy_user_id<>'')
);
ALTER TABLE sessions.legacy_export_scope_authority ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.legacy_export_scope_authority
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,
 bulls96_privacy_eraser,bulls96_legacy_export_privacy,bulls96_babysitter_retention;

-- The exporter's privacy credential is a dedicated table-SELECT role plus
-- this migration's one register RPC. It is never an owner or BYPASSRLS role.
DO $legacy_export_role$
DECLARE r pg_roles%ROWTYPE;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_legacy_export_privacy';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='42704',MESSAGE='role bulls96_legacy_export_privacy does not exist; bootstrap first';END IF;
 IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='role bulls96_legacy_export_privacy exists elevated';END IF;
 IF EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='role bulls96_legacy_export_privacy is a member of another role';END IF;
END $legacy_export_role$;

CREATE FUNCTION sessions.ingest_legacy_export_scope_authority_register(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $register$
DECLARE v_org text;v_ws text;v_source text;v_session text;v_user text;v_machine text;r sessions.legacy_export_scope_authority%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_privacy_eraser_require_active();
 PERFORM sessions.ingest_exact_keys(p,ARRAY['org_id','workspace_id','source','session_id','legacy_user_id','legacy_machine_id']);
 IF EXISTS(SELECT 1 FROM jsonb_each(p) e WHERE jsonb_typeof(e.value)<>'string') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='legacy export authority: string fields required';END IF;
 v_org:=p->>'org_id';v_ws:=CASE WHEN p->>'workspace_id'='' THEN 'default' ELSE p->>'workspace_id' END;
 v_source:=p->>'source';v_session:=p->>'session_id';v_user:=p->>'legacy_user_id';v_machine:=p->>'legacy_machine_id';
 IF v_org='' OR v_org!~'^[A-Za-z0-9._-]{1,128}$' OR left(v_org,1)='.'
   OR v_ws='' OR length(v_ws)>1024 OR v_source='' OR length(v_source)>1024
   OR v_session='' OR length(v_session)>1024 OR v_user='' OR length(v_user)>1024
   OR v_machine!~'^srv_[0-9a-f]{32}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='legacy export authority: invalid scope';END IF;
 INSERT INTO sessions.legacy_export_scope_authority
  VALUES(v_org,v_ws,v_source,v_session,v_user,v_machine) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT r FROM sessions.legacy_export_scope_authority
  WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
 IF r.legacy_user_id<>v_user OR r.legacy_machine_id<>v_machine THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export authority: immutable owner mismatch';END IF;
 RETURN jsonb_build_object('org_id',r.org_id,'workspace_id',r.workspace_id,'source',r.source,'session_id',r.session_id,
  'legacy_user_id',r.legacy_user_id,'legacy_machine_id',r.legacy_machine_id);
END $register$;

-- The exporter must never create a legacy copy while Neon is still the
-- erasure authority: Neon has no access to the immutable owner row and is
-- forbidden to re-derive one. This narrow probe exposes only readiness.
CREATE FUNCTION sessions.legacy_export_privacy_authority_ready() RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $ready$
BEGIN
 PERFORM sessions.ingest_privacy_eraser_require_active();
 RETURN true;
END $ready$;

CREATE FUNCTION sessions.legacy_export_privacy_eraser_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $legacy_page$
DECLARE v_limit int;a jsonb;v jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['after','limit']);
 IF jsonb_typeof(p->'limit') IS DISTINCT FROM 'number' OR p->>'limit'!~'^[1-9][0-9]{0,3}$' OR (p->>'limit')::int>5000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='legacy export authority: invalid eraser page limit';END IF;
 v_limit:=(p->>'limit')::int;a:=p->'after';
 IF a<>'null'::jsonb AND (jsonb_typeof(a)<>'array' OR jsonb_array_length(a)<>6
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(a) e WHERE jsonb_typeof(e)<>'string')) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='legacy export authority: invalid eraser page cursor';END IF;
 PERFORM sessions.ingest_privacy_eraser_require_active();
 SELECT coalesce(jsonb_agg(jsonb_build_object(
   'org_id',org_id,'workspace_id',workspace_id,'source',source,'session_id',session_id,
   'legacy_user_id',legacy_user_id,'legacy_machine_id',legacy_machine_id)
   ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",source COLLATE "C",session_id COLLATE "C",
    legacy_user_id COLLATE "C",legacy_machine_id COLLATE "C"),'[]'::jsonb) INTO v
 FROM(
  SELECT q.* FROM sessions.legacy_export_scope_authority q
  WHERE (a='null'::jsonb OR
    (q.org_id COLLATE "C",q.workspace_id COLLATE "C",q.source COLLATE "C",q.session_id COLLATE "C",
     q.legacy_user_id COLLATE "C",q.legacy_machine_id COLLATE "C")>
    (a->>0,a->>1,a->>2,a->>3,a->>4,a->>5))
   AND (
    EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=q.org_id AND d.source=q.source AND d.session_id=q.session_id
      AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=q.workspace_id)
    OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_deletions d WHERE d.org_id=q.org_id AND d.source=q.source AND d.session_id=q.session_id
      AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=q.workspace_id)
    OR EXISTS(SELECT 1 FROM sessions.session_erasure_receipts d WHERE d.org_id=q.org_id AND d.source=q.source AND d.session_id=q.session_id
      AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=q.workspace_id)
    OR EXISTS(SELECT 1 FROM sessions.turn_session_fences d WHERE d.org_id=q.org_id AND d.source=q.source AND d.session_id=q.session_id
      AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=q.workspace_id)
    OR EXISTS(SELECT 1 FROM sessions.upload_resets d WHERE d.org_id=q.org_id AND d.user_id=q.legacy_user_id
      AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=q.workspace_id)
    OR EXISTS(SELECT 1 FROM sessions.operator_user_erasures d WHERE d.org_id=q.org_id AND d.user_id=q.legacy_user_id
      AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=q.workspace_id))
  ORDER BY q.org_id COLLATE "C",q.workspace_id COLLATE "C",q.source COLLATE "C",q.session_id COLLATE "C",
   q.legacy_user_id COLLATE "C",q.legacy_machine_id COLLATE "C" LIMIT v_limit)q;
 RETURN v;
END $legacy_page$;

CREATE FUNCTION sessions.legacy_export_privacy_eraser_still_fenced(p jsonb) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,sessions,pg_temp AS $legacy_still$
DECLARE r sessions.legacy_export_scope_authority%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['org_id','workspace_id','source','session_id','legacy_user_id','legacy_machine_id']);
 IF EXISTS(SELECT 1 FROM jsonb_each(p) e WHERE jsonb_typeof(e.value)<>'string') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='legacy export authority: invalid eraser scope';END IF;
 PERFORM sessions.ingest_privacy_eraser_require_active();
 SELECT * INTO r FROM sessions.legacy_export_scope_authority q
  WHERE q.org_id=p->>'org_id' AND q.workspace_id=p->>'workspace_id' AND q.source=p->>'source'
   AND q.session_id=p->>'session_id' AND q.legacy_user_id=p->>'legacy_user_id' AND q.legacy_machine_id=p->>'legacy_machine_id';
 IF NOT FOUND THEN RETURN false;END IF;
 RETURN
  EXISTS(SELECT 1 FROM sessions.upload_deletions d WHERE d.org_id=r.org_id AND d.source=r.source AND d.session_id=r.session_id
    AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=r.workspace_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_deletions d WHERE d.org_id=r.org_id AND d.source=r.source AND d.session_id=r.session_id
    AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=r.workspace_id)
  OR EXISTS(SELECT 1 FROM sessions.session_erasure_receipts d WHERE d.org_id=r.org_id AND d.source=r.source AND d.session_id=r.session_id
    AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=r.workspace_id)
  OR EXISTS(SELECT 1 FROM sessions.turn_session_fences d WHERE d.org_id=r.org_id AND d.source=r.source AND d.session_id=r.session_id
    AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=r.workspace_id)
  OR EXISTS(SELECT 1 FROM sessions.upload_resets d WHERE d.org_id=r.org_id AND d.user_id=r.legacy_user_id
    AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=r.workspace_id)
  OR EXISTS(SELECT 1 FROM sessions.operator_user_erasures d WHERE d.org_id=r.org_id AND d.user_id=r.legacy_user_id
    AND CASE WHEN d.workspace_id='' THEN 'default' ELSE d.workspace_id END=r.workspace_id);
END $legacy_still$;

REVOKE ALL ON FUNCTION sessions.ingest_legacy_export_scope_authority_register(jsonb),
 sessions.legacy_export_privacy_authority_ready(),
 sessions.legacy_export_privacy_eraser_page(jsonb),sessions.legacy_export_privacy_eraser_still_fenced(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,
 bulls96_privacy_eraser,bulls96_legacy_export_privacy,bulls96_babysitter_retention;
GRANT USAGE ON SCHEMA sessions TO bulls96_legacy_export_privacy;
GRANT SELECT ON sessions.session_erasure_receipts,sessions.upload_deletions,sessions.ingest_frozen_source_deletions,
 sessions.upload_resets,sessions.operator_user_erasures TO bulls96_legacy_export_privacy;
CREATE POLICY legacy_export_privacy_read_all ON sessions.session_erasure_receipts
 FOR SELECT TO bulls96_legacy_export_privacy USING(true);
CREATE POLICY legacy_export_privacy_read_all ON sessions.upload_deletions
 FOR SELECT TO bulls96_legacy_export_privacy USING(true);
CREATE POLICY legacy_export_privacy_read_all ON sessions.ingest_frozen_source_deletions
 FOR SELECT TO bulls96_legacy_export_privacy USING(true);
CREATE POLICY legacy_export_privacy_read_all ON sessions.upload_resets
 FOR SELECT TO bulls96_legacy_export_privacy USING(true);
CREATE POLICY legacy_export_privacy_read_all ON sessions.operator_user_erasures
 FOR SELECT TO bulls96_legacy_export_privacy USING(true);
-- 062010 is a separately owned turns-forward migration and sorts before this
-- file in the combined release. The current standalone branch can still be
-- rehearsed without it; the exporter then fails closed on the missing table.
DO $turn_fence_grant$ BEGIN
 IF to_regclass('sessions.turn_session_fences') IS NOT NULL THEN
  GRANT SELECT ON sessions.turn_session_fences TO bulls96_legacy_export_privacy;
  CREATE POLICY legacy_export_privacy_read_all ON sessions.turn_session_fences
   FOR SELECT TO bulls96_legacy_export_privacy USING(true);
 END IF;
END $turn_fence_grant$;
GRANT EXECUTE ON FUNCTION sessions.ingest_legacy_export_scope_authority_register(jsonb),
 sessions.legacy_export_privacy_authority_ready() TO bulls96_legacy_export_privacy;
GRANT EXECUTE ON FUNCTION sessions.legacy_export_privacy_eraser_page(jsonb),
 sessions.legacy_export_privacy_eraser_still_fenced(jsonb) TO bulls96_privacy_eraser;
