-- Slot 20261105062390 (reserved by layered-sessions-pipeline). Seed capture lease issuance is admission-class under
-- the per-org admission hold (20261105062320): gap C of the hold-install precondition (sessions-wave-tooling).
-- Code only: applying it to any real database needs Khaliq's explicit OK.
--
-- ingest_seed_scope_capture in mode 'lease' issues the lease (a sessions.ingest_seed_captures row) that
-- ingest_seed_proven_records imports against. 062320 classified it unaffected, so it took no org key, and an install
-- holding the org key EXCLUSIVE could not exclude a racing capture. This slot replaces it with its latest prior body
-- (20261104040000) verbatim plus one marked line: in lease mode, ingest_org_admission_hold_check(org,'admission')
-- right after the mode is validated and before any lock or read. It takes the org key SHARED (so an install waits for
-- an in-flight capture, and a capture waits for an in-flight install), then refuses RH042 while the org has any hold
-- that is not released, and 40001 outside READ COMMITTED (the check's own rules). Mode 'preview' is metadata-only and
-- issues nothing: it is unchanged and never refused. Owner, SECURITY DEFINER, search_path and ACL are unchanged
-- (CREATE OR REPLACE keeps them). Leases issued before this slot are not fenced by it: the installer's post-key
-- active-lease check (sessions-wave-tooling) still refuses an install while an unexpired capture exists.
-- The generated down restores the exact prior body; it refuses while any hold is not released.

CREATE OR REPLACE FUNCTION sessions.ingest_seed_scope_capture(p jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capture$
DECLARE v_org text;v_ws text;v_source text;v_session text;v_generation text;v_mode text;
 v_state jsonb;v_fingerprint text;v_id uuid;v_expiry timestamptz;v_user text;v_before bigint;v_locked bigint;v_rows jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest_seeder');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['org_id','workspace_id','source','session_id','generation','mode']);
 v_org:=sessions.ingest_identifier(p->'org_id',false);v_ws:=sessions.ingest_identifier(p->'workspace_id',true);
 v_source:=sessions.ingest_identifier(p->'source',false);v_session:=sessions.ingest_identifier(p->'session_id',false);
 v_generation:=sessions.ingest_digest(p->'generation');v_mode:=p->>'mode';
 IF v_mode NOT IN('preview','lease') OR v_mode IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ingest seed: mode'; END IF;
 IF v_mode='lease' THEN PERFORM sessions.ingest_org_admission_hold_check(v_org,'admission'); END IF; -- 062390 org admission hold
 IF NOT EXISTS(SELECT 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker' AND status='pass' AND generation=v_generation)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: generation not ready'; END IF;
 IF v_mode='lease' THEN
  IF EXISTS(SELECT 1 FROM sessions.session_index s WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_source AND s.session_id=v_session AND s.deleted_at IS NULL
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_seed_fence_baselines b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id AND b.user_id=s.user_id AND b.closed_at IS NULL))
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ingest seed: fence baseline required';END IF;
  SELECT epoch INTO v_before FROM sessions.ingest_seed_source_epochs WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
  FOR v_user IN SELECT DISTINCT user_id FROM sessions.session_index WHERE org_id=v_org AND workspace_id=v_ws AND harness=v_source AND session_id=v_session AND deleted_at IS NULL ORDER BY user_id LOOP
   PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
  END LOOP;
  SELECT epoch INTO v_locked FROM sessions.ingest_seed_source_epochs WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session FOR UPDATE;
  IF v_before IS DISTINCT FROM v_locked THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='ingest seed: source changed'; END IF;
 END IF;
 v_state:=sessions.ingest_seed_scope_state(v_org,v_ws,v_source,v_session);
 v_fingerprint:=encode(sha256(convert_to(v_state::text,'UTF8')),'hex');
 IF v_mode='lease' THEN
  INSERT INTO sessions.ingest_seed_captures(org_id,workspace_id,source,session_id,generation,source_epoch,fingerprint,state)
  VALUES(v_org,v_ws,v_source,v_session,v_generation,(v_state->>'source_epoch')::bigint,v_fingerprint,v_state)
  RETURNING id,expires_at INTO v_id,v_expiry;
 END IF;
 SELECT coalesce(jsonb_agg(
  (x-'reset_epoch'-'fence_versions'-'deleted_origins')||jsonb_build_object('capture',jsonb_build_object(
   'pointer_id',encode(sha256(convert_to(jsonb_build_array(v_fingerprint,x)::text,'UTF8')),'hex'),
   'scope_sha256',v_fingerprint,'reset_epoch',x->'reset_epoch','fence_versions',x->'fence_versions','deleted_origins',x->'deleted_origins'))
  ORDER BY x->>'machine_id',x->>'manifest_key'),'[]') INTO v_rows FROM jsonb_array_elements(v_state->'pointers')x;
 RETURN jsonb_build_object('protocol',1,'capture_id',v_id,'expires_at',v_expiry,'fingerprint',v_fingerprint,
   'source_epoch',v_state->'source_epoch','pointers',v_rows);
END $capture$;
