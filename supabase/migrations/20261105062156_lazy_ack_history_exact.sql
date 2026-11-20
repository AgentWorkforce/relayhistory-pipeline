-- Slot 20261105062156 (assigned by the lander). Gated, default OFF: the staged ACK makes its canonical-history scope
-- invalidations itself, once per scope, with the SAME counters, epoch draws and stored values as main's per-accept
-- trigger calls. Code only: applying it to any real database, and flipping the gate anywhere, need Khaliq's explicit OK.
-- Built from MAIN's bodies (043800 ACK loop, 026000 trigger). Independent of 062155 (review/receipt-clock-fix-set-based,
-- held): never apply the two together; each replaces sessions.ingest_lazy_ack_batch(jsonb) wholesale.
--
-- Why. With canonical history active, every accepted record's slot UPDATE fires ingest_history_native_changed, which
-- calls ingest_history_invalidate_scope once per managed scope it touches. A 500-record batch over a few sessions makes
-- ~500 invalidations of the same few scopes, all under the global seat clock (measured locally: ~52% of the ACK).
--
-- Gate. bulls96.lazy_ack_h_exact in the ACK's proconfig, installed 'off', read once at the top; exactly 'on' enables
-- the path below, anything else (or absent) is main byte-for-byte in behaviour. Only the owner can ALTER FUNCTION; a
-- runtime-role SET/set_config is overridden by proconfig inside the ACK and, outside it, finds no mark (below).
--   on:  ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_h_exact='on';
--   off: ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_h_exact='off';
--   read: SELECT proconfig FROM pg_proc WHERE oid='sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure;
-- A later CREATE OR REPLACE of the ACK resets proconfig to that file's SET list. A call in flight keeps its body.
--
-- Gate ON, exactly:
--  * The ACK inserts an owner-only mark (sessions.lazy_ack_history_marks, key = top-level xid) immediately before
--    its per-record loop and deletes it immediately after (asserting it is gone), before its own invalidations and
--    before it returns; an error in between aborts the (sub)transaction with the mark. Only this ACK writes it.
--  * ingest_history_native_changed (replaced; body otherwise 026000's) still runs per accept and still does its build
--    fence and outbox upsert exactly as before; under that mark, and only when its caller's setting is 'on', it skips
--    its two invalidation loops. Without the mark it is 026000's behaviour; for every other writer (lazy_clear's
--    multi-row UPDATE, claims, gate-off ACKs) the added cost is one current_setting compare and no table read.
--  * Per accept, right after its slot UPDATE, the ACK records the trigger's invalidation targets from the slot as it
--    was before that UPDATE and as written by it (lazy_ack_history_intents): the new-scope set (scope_keys hits for
--    the record plus the new current session), filtered by ingest_history_scope_managed and sorted as the trigger sorts,
--    then the old scope when the tracked tuple changed. One native key updated twice in a batch (A->B->A) contributes
--    per accept, as the trigger does. ingest_history_scope_managed implies ingest_history_active(), so no extra
--    active() read is needed: with no managed scope neither body invalidates anything.
--  * Right after each accepted slot UPDATE, for that accept's intents in the trigger's order, the ACK takes the scope's
--    directory-events advisory lock (main's key, main's order) and draws the sequence values main's invalidate_scope
--    call draws at that point (inline, in the ACK): its session_replay_publications INSERT ... ON CONFLICT DO UPDATE
--    SET epoch=nextval draws the column default for the proposed row AND, on conflict, the update's value, i.e. 1 draw
--    while the scope has no publications row (its first invalidation, absence read under the lock), else 2; the last
--    draw is the value main stores. Only the per-scope count k and that last value are kept (plpgsql variables).
--  * After the loop (mark removed), lazy_ack_history_replay does the deferred row work once per scope, in
--    first-invalidation order: required_scopes request_version = k (new row: default 1 then +1 per later call) or +k,
--    scope_current.ready=false, the session_directory reset, the publications row = the scope's last drawn value
--    (written explicitly: the replay draws nothing), and the derived-object retire.
-- Serial parity (same input, same state; the differential PGlite suite): every stored row equal, including per-scope
-- request_version and epoch, and the sequence position, after success AND after an abort at any accept boundary or at
-- a scope-lock wait (55P03), because draws and scope locks happen where main makes them.
-- Managed-ness is stable inside the ACK: invalidate_scope only sets ready=false, never inserts or deletes a scope_current
-- row, and nothing in the ACK transaction writes scope_keys or scope_current rows.
-- Remaining differences:
--   - requested_at / updated_at timestamps;
--   - a failure INSIDE the deferred row work (a row-lock or statement timeout on required_scopes, scope_current, the
--     directory or the publications row during the replay): main would have failed at an earlier accept with fewer
--     draws, so the sequence position after such an abort is ahead of main's by the draws of the intents after the
--     failing one (bounded by this ACK's intents). Stated to the history owner; nothing else differs;
--   - a statement error inside the slot UPDATE or the trigger body raised between an accept's UPDATE and its draws:
--     main would have drawn nothing for that accept either (its trigger raised before its invalidations), so equal;
--   - build capture (026000 scope_capture) inserts a publications row ON CONFLICT DO NOTHING, drawing the default,
--     after try-locking the same scope key: while this ACK holds that key (from its first draw to commit) the capture
--     cannot take it, exactly as against main (which holds it from its first invalidate_scope call); race-only before
--     the ACK's first draw for the scope, identical for both bodies;
--   - race-only: a scope install or erase committing in the middle of a READ COMMITTED ACK is observed per statement by
--     main and per accept here (the same number of reads, at slightly different instants).
-- Mark-table churn: one INSERT and one DELETE per gated ACK; rows exist only inside a call; autovacuum keeps the table
-- tiny (as 062010's marks).
-- Lock order: fence -> clock (NO KEY UPDATE) -> origin -> stage row -> slot rows, with each accept's scope advisory locks
-- taken right after its slot UPDATE, as main's trigger takes them: main's order exactly.
-- No grant changes: the new table and helpers are owner-only; the ACK stays SECURITY INVOKER, the trigger SECURITY DEFINER.
-- Rollback (generated by scripts/sessions-function-boundary/lanes/lazy-ack-history-exact.ts):
-- supabase/rollbacks/20261105062156_lazy_ack_history_exact.down.sql restores both bodies, drops the helpers and the table.
DO $lazy_ack_history_exact_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid IN('sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure,'sessions.ingest_history_native_changed()'::regprocedure)
  AND proowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: history-exact ACK requires original creator';END IF;
END $lazy_ack_history_exact_creator$;

CREATE TABLE sessions.lazy_ack_history_marks(
 xact xid8 PRIMARY KEY
);
ALTER TABLE sessions.lazy_ack_history_marks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.lazy_ack_history_marks FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- The trigger's invalidation targets for ONE accepted slot UPDATE (old tuple -> new tuple), in the trigger's order:
-- the sorted new-scope set (026000 ingest_history_native_changed, first loop), then the changed old scope (second loop).
CREATE FUNCTION sessions.lazy_ack_history_intents(p_org text,p_ws text,p_record text,
 o_owner text,o_source text,o_session text,o_revision bigint,o_digest text,o_lineage bigint,
 n_owner text,n_source text,n_session text,n_revision bigint,n_digest text,n_lineage bigint)RETURNS text[]
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ack_intents$
DECLARE v_out text[];
BEGIN
 SELECT coalesce(array_agg(jsonb_build_array(z.org_id,z.workspace_id,z.harness,z.session_id)::text
   ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"),'{}')INTO v_out FROM(
  SELECT k.org_id,k.workspace_id,k.harness,k.session_id FROM sessions.ingest_history_scope_keys k
   WHERE k.org_id=p_org AND k.workspace_id=p_ws AND k.record_id=p_record
  UNION SELECT p_org,p_ws,n_source,n_session WHERE n_owner IS NOT NULL AND n_session IS NOT NULL
 )z WHERE sessions.ingest_history_scope_managed(z.org_id,z.workspace_id,z.harness,z.session_id);
 IF o_owner IS NOT NULL AND o_session IS NOT NULL
  AND ROW(o_owner,o_source,o_session,o_revision,o_digest,o_lineage)IS DISTINCT FROM ROW(n_owner,n_source,n_session,n_revision,n_digest,n_lineage)
  AND sessions.ingest_history_scope_managed(p_org,p_ws,o_source,o_session) THEN
  v_out:=v_out||jsonb_build_array(p_org,p_ws,o_source,o_session)::text;
 END IF;
 RETURN v_out;
END $ack_intents$;

-- The scope's directory-events advisory xact lock, exactly the key ingest_history_invalidate_scope takes (026000).
CREATE FUNCTION sessions.lazy_ack_history_scope_lock(p_scope jsonb)RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ack_scope_lock$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',p_scope->>0,p_scope->>1,p_scope->>2,p_scope->>3),0));
END $ack_scope_lock$;

-- The deferred row work of main's ingest_history_invalidate_scope calls, once per scope in first-invalidation order:
-- required_scopes ends at k (new row: default 1, then +1 per later call) or at +k; ready=false, the directory reset and
-- the derived-object retire are idempotent after the first call; the publications row gets the scope's last drawn value,
-- written explicitly (the INSERT names the column and the DO UPDATE uses EXCLUDED.epoch): this function draws nothing.
CREATE FUNCTION sessions.lazy_ack_history_replay(p_scopes text[],p_k integer[],p_last bigint[])RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $ack_replay$
DECLARE j jsonb;x integer;
BEGIN
 FOR x IN 1..coalesce(cardinality(p_scopes),0) LOOP
  j:=p_scopes[x]::jsonb;
  INSERT INTO sessions.ingest_history_required_scopes AS r(org_id,workspace_id,harness,session_id,request_version)
   VALUES(j->>0,j->>1,j->>2,j->>3,p_k[x])
   ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET request_version=r.request_version+p_k[x],requested_at=clock_timestamp();
  UPDATE sessions.ingest_history_scope_current SET ready=false WHERE org_id=j->>0 AND workspace_id=j->>1 AND harness=j->>2 AND session_id=j->>3;
  UPDATE sessions.session_directory SET state='unknown',short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
   short_summary_source=NULL,short_summary_generated_at=NULL,event_projection_ready=false,work_status=NULL,work_status_at=NULL
   WHERE org_id=j->>0 AND workspace_id=j->>1 AND harness=j->>2 AND session_id=j->>3
    AND(state<>'unknown'OR short_summary IS NOT NULL OR event_projection_ready OR work_status IS NOT NULL);
  INSERT INTO sessions.session_replay_publications AS a(org_id,workspace_id,harness,session_id,epoch)VALUES(j->>0,j->>1,j->>2,j->>3,p_last[x])
   ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET epoch=EXCLUDED.epoch;
  UPDATE sessions.ingest_history_derived_objects o SET cleanup_state='retired',row_version=row_version+1 FROM sessions.ingest_history_scope_versions v
   WHERE o.authority_id=v.authority_id AND v.org_id=j->>0 AND v.workspace_id=j->>1 AND v.harness=j->>2 AND v.session_id=j->>3 AND o.cleanup_state='live';
 END LOOP;
END $ack_replay$;
REVOKE ALL ON FUNCTION sessions.lazy_ack_history_intents(text,text,text,text,text,text,bigint,text,bigint,text,text,text,bigint,text,bigint),
 sessions.lazy_ack_history_scope_lock(jsonb),sessions.lazy_ack_history_replay(text[],integer[],bigint[])
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.ingest_history_native_changed()RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE v_scope record;v_builds uuid[];v_old uuid[];
BEGIN
 -- Dormant: the production ACK (ingest_lazy_ack_batch) is untouched.
 IF NOT sessions.ingest_history_active()THEN RETURN NULL;END IF;
 -- FIRST fence every in-flight build this statement touches: a native it registered as a key, or a
 -- native seated (now, or before this UPDATE) in its scope. Fencing is the ONLY effect a build in
 -- flight ever has on a live ACK: no invalidation, required scope, directory clear or epoch advance.
 SELECT array_agg(DISTINCT b.build_id)INTO v_builds FROM sessions.ingest_history_scope_builds b WHERE b.state IN('keys','captured')AND b.lease_until>clock_timestamp()AND(
  EXISTS(SELECT 1 FROM history_new n JOIN sessions.ingest_history_scope_build_records br ON br.record_id=n.record_id
   WHERE br.build_id=b.build_id AND n.org_id=b.org_id AND n.workspace_id=b.workspace_id)
  OR EXISTS(SELECT 1 FROM history_new n WHERE n.current_session IS NOT NULL AND n.org_id=b.org_id AND n.workspace_id=b.workspace_id
   AND n.current_source=b.harness AND n.current_session=b.session_id));
 IF TG_OP='UPDATE'THEN
  SELECT array_agg(DISTINCT b.build_id)INTO v_old FROM sessions.ingest_history_scope_builds b WHERE b.state IN('keys','captured')AND b.lease_until>clock_timestamp()
   AND EXISTS(SELECT 1 FROM history_old o WHERE o.current_session IS NOT NULL AND o.org_id=b.org_id AND o.workspace_id=b.workspace_id
    AND o.current_source=b.harness AND o.current_session=b.session_id);
  v_builds:=coalesce(v_builds,'{}')||coalesce(v_old,'{}');
 END IF;
 PERFORM sessions.ingest_history_fence_builds(v_builds);
 -- Reconcile requests only for natives that touch a managed scope (its key set or its current seat).
 -- A changed source identity re-requests the native instead of aborting the ACK.
 INSERT INTO sessions.ingest_history_reconcile_outbox(native_key_sha256,requested_lineage,source_identity_sha256,control_epoch,current_owner)
 SELECT n.key_sha256,n.lineage,n.source_identity_sha256,n.control_epoch,n.current_owner FROM history_new n
  WHERE(n.current_session IS NOT NULL AND sessions.ingest_history_scope_managed(n.org_id,n.workspace_id,n.current_source,n.current_session))
   OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_keys k WHERE k.org_id=n.org_id AND k.workspace_id=n.workspace_id AND k.record_id=n.record_id
    AND sessions.ingest_history_scope_managed(k.org_id,k.workspace_id,k.harness,k.session_id))
 ON CONFLICT(native_key_sha256)DO UPDATE SET requested_lineage=excluded.requested_lineage,source_identity_sha256=excluded.source_identity_sha256,
  control_epoch=excluded.control_epoch,request_version=sessions.ingest_history_reconcile_outbox.request_version+1,
  prior_owner=CASE WHEN sessions.ingest_history_reconcile_outbox.current_owner IS DISTINCT FROM excluded.current_owner
   THEN sessions.ingest_history_reconcile_outbox.current_owner ELSE sessions.ingest_history_reconcile_outbox.prior_owner END,
  current_owner=excluded.current_owner,state='pending',lease_token=NULL,lease_until=NULL,lease_generation=NULL,requested_at=clock_timestamp(),completed_at=NULL;
 -- 062156: inside a gated staged ACK (its function-local setting is 'on' AND it holds the owner-only mark of this
 -- transaction) the invalidations below are made by the ACK itself, in order, after its per-record loop. Every
 -- other writer (lazy_clear, claims, a gate-off ACK) pays one string compare and reads no table here.
 IF current_setting('bulls96.lazy_ack_h_exact',true) IS NOT DISTINCT FROM 'on' THEN
  IF EXISTS(SELECT 1 FROM sessions.lazy_ack_history_marks WHERE xact=pg_current_xact_id()) THEN RETURN NULL;END IF;
 END IF;
 -- A native winner can change without any physical pointer changing: invalidate every MANAGED
 -- consumer in this same ACK/control transaction, in sorted order (waits, never 40001).
 FOR v_scope IN
  SELECT z.*FROM(
   SELECT k.org_id,k.workspace_id,k.harness,k.session_id FROM history_new n JOIN sessions.ingest_history_scope_keys k
    ON k.org_id=n.org_id AND k.workspace_id=n.workspace_id AND k.record_id=n.record_id
   UNION SELECT n.org_id,n.workspace_id,n.current_source,n.current_session FROM history_new n
    WHERE n.current_owner IS NOT NULL AND n.current_session IS NOT NULL
  )z WHERE sessions.ingest_history_scope_managed(z.org_id,z.workspace_id,z.harness,z.session_id)
  ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
 LOOP
  PERFORM sessions.ingest_history_invalidate_scope(v_scope.org_id,v_scope.workspace_id,v_scope.harness,v_scope.session_id);
 END LOOP;
 IF TG_OP='UPDATE'THEN
  FOR v_scope IN SELECT z.*FROM(
   SELECT DISTINCT o.org_id,o.workspace_id,o.current_source AS harness,o.current_session AS session_id
    FROM history_old o JOIN history_new n USING(key_sha256)WHERE o.current_owner IS NOT NULL AND o.current_session IS NOT NULL
     AND ROW(o.current_owner,o.current_source,o.current_session,o.current_revision,o.current_digest,o.lineage)
      IS DISTINCT FROM ROW(n.current_owner,n.current_source,n.current_session,n.current_revision,n.current_digest,n.lineage)
  )z WHERE sessions.ingest_history_scope_managed(z.org_id,z.workspace_id,z.harness,z.session_id)
  ORDER BY z.org_id COLLATE"C",z.workspace_id COLLATE"C",z.harness COLLATE"C",z.session_id COLLATE"C"
  LOOP PERFORM sessions.ingest_history_invalidate_scope(v_scope.org_id,v_scope.workspace_id,v_scope.harness,v_scope.session_id);END LOOP;
 END IF;
 RETURN NULL;
END $changed$;

CREATE OR REPLACE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET bulls96.lazy_ack_h_exact='off' AS $lazy_ack_batch$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 t sessions.ingest_stage_intents%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 d sessions.ingest_lazy_ack_decisions%ROWTYPE;r record;v_last bigint;v_key text;v_material jsonb;v_accept boolean;v_first text;v_new_lineage bigint;
 v_hx boolean;v_scopes text[]:='{}';v_k integer[]:='{}';v_lastv bigint[]:='{}';v_marks integer;v_iv text;v_ij jsonb;v_ix integer;v_iseq bigint;
BEGIN
 -- 062156 gate, read once: exactly 'on' defers this ACK's scope invalidations (see header); anything else is main.
 v_hx:=current_setting('bulls96.lazy_ack_h_exact',true) IS NOT DISTINCT FROM 'on';
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_lazy_user_seals u JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE u.org_id=p->>'org_id' AND u.workspace_id=p->>'workspace_id' AND u.user_id=p->>'user_id' AND u.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: real user seal required before any receipt';END IF;
 -- 043500: a suppressed receipt allocates no seat and writes nothing here. It returns BELOW the guard/fence/seal
 -- checks above and ABOVE the exclusive clock lock, holding only KEY SHARE: still excluded by set_live,
 -- control_changed and lazy_clear (FOR UPDATE), and no KEY SHARE -> FOR UPDATE upgrade inside a claim (the
 -- claim's ingest_lazy_records already holds KEY SHARE; two claims upgrading would deadlock).
 IF p->>'buffer_receipt_kind'='suppressed' THEN
  PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR KEY SHARE;RETURN;END IF;
 -- 043800: NO KEY UPDATE, not FOR UPDATE. This ACK writes last_seat only (a non-key column), so NO KEY UPDATE
 -- is exactly what its own UPDATE below takes. It still serializes staged ACKs (NO KEY UPDATE vs NO KEY UPDATE)
 -- and is still excluded by set_live/control_changed/lazy_clear (FOR UPDATE) and the FOR SHARE holders, but it
 -- no longer blocks a claim's KEY SHARE: claims read only the write-once identity/epoch columns. Taken before
 -- the loop, as before: the seats are numbered from it, and clock-before-slot keeps ACK slot locks ordered.
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;
 IF p->>'buffer_receipt_kind' IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unexpected receipt authority';END IF;
 -- 043800: the same bounded origin advisory xact lock as ingest_lazy_records (same version string, key and seed),
 -- taken after the clock and BEFORE the stage-row lock and every native-slot lock below: fence -> clock ->
 -- origin -> slot, as in the claim. A same-origin claim of another user now queues on this lock instead of
 -- interleaving its slot locks with ours (reversed slot order would be a 40P01 cycle). Single origin per call:
 -- p is the one ingest_batches row of the ingest_receipt_seat_ack trigger, and the slot keys below use
 -- a.(org,workspace,origin), which the commit path (ingest_attempt_epochs) and the a/t checks below pin to it.
 -- So exactly one advisory lock, no multi-origin ordering needed.
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:lazy-native-origin-v1',p->>'org_id',p->>'workspace_id',p->>'origin_id')::text,0));
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
 -- 062156: the mark lives exactly across the per-record loop (deleted, and asserted gone, right after it).
 IF v_hx THEN INSERT INTO sessions.lazy_ack_history_marks(xact) VALUES(pg_current_xact_id());END IF;
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
   -- 062156: the invalidations main's trigger makes for THIS accept, from the slot as it was before this UPDATE
   -- (slot) and as written by it, in the trigger's own order: new-scope set sorted, then the changed old scope.
   -- Its scope locks and sequence draws happen here, at main's point and in main's order; the row work is deferred.
   -- Per intent: the scope's directory-events advisory lock (re-entrant; main's key and order), then the draws main's
   -- ingest_history_invalidate_scope makes there. Its publications INSERT ... ON CONFLICT DO UPDATE SET epoch=nextval
   -- draws the column default for the proposed row AND, on conflict, the update's value: 1 draw while the scope has no
   -- publications row (its first invalidation; absence read under the lock), else 2. The last draw is what main stores.
   IF v_hx THEN
    FOREACH v_iv IN ARRAY sessions.lazy_ack_history_intents(slot.org_id,slot.workspace_id,slot.record_id,
     slot.current_owner,slot.current_source,slot.current_session,slot.current_revision,slot.current_digest,slot.lineage,
     a.user_id,r.key_material->>10,r.key_material->>11,(r.key_material->>8)::bigint,r.candidate_digest,v_new_lineage) LOOP
     v_ij:=v_iv::jsonb;
     PERFORM sessions.lazy_ack_history_scope_lock(v_ij);
     v_ix:=array_position(v_scopes,v_iv);
     IF v_ix IS NULL THEN
      v_scopes:=v_scopes||v_iv;v_k:=v_k||0;v_lastv:=v_lastv||NULL::bigint;v_ix:=cardinality(v_scopes);
      IF EXISTS(SELECT 1 FROM sessions.session_replay_publications pp WHERE pp.org_id=v_ij->>0 AND pp.workspace_id=v_ij->>1
       AND pp.harness=v_ij->>2 AND pp.session_id=v_ij->>3) THEN v_iseq:=nextval('sessions.session_replay_epoch_seq');END IF;
     ELSE
      v_iseq:=nextval('sessions.session_replay_epoch_seq');
     END IF;
     v_iseq:=nextval('sessions.session_replay_epoch_seq');
     v_k[v_ix]:=v_k[v_ix]+1;v_lastv[v_ix]:=v_iseq;
    END LOOP;
   END IF;
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
 IF v_hx THEN
  DELETE FROM sessions.lazy_ack_history_marks WHERE xact=pg_current_xact_id();GET DIAGNOSTICS v_marks=ROW_COUNT;
  IF v_marks<>1 OR EXISTS(SELECT 1 FROM sessions.lazy_ack_history_marks WHERE xact=pg_current_xact_id()) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: history ACK mark lost';END IF;
  PERFORM sessions.lazy_ack_history_replay(v_scopes,v_k,v_lastv);
 END IF;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF t.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: stage expired while assigning seats';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
END $lazy_ack_batch$;
