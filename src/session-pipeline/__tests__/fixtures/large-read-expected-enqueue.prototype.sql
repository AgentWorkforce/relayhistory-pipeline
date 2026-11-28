-- OFFLINE PROTOTYPE, NOT A MIGRATION (layered-sessions-pipeline d99235d6fca0: slot 062360 is reserved elsewhere; this
-- SQL lives in a test fixture until a separate slot is assigned). Installed only by
-- src/session-pipeline/__tests__/large-read-expected-enqueue.postgres.test.ts on a disposable database.
-- Large-read enqueue bound to an expected, validated source incarnation. For SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED holds (src/session-pipeline/
-- search-deadline-isolation.ts enqueueLargeReadExpected): a hold waiting for incarnation A must never insert,
-- rewrite or unlease the job of a newer incarnation B.
--
-- sessions.enqueue_session_large_read_expected(org, workspace, harness, session, p_expected_epoch,
-- p_expected_fingerprint):
--  1. c := capture_session_replay_pages(...). Unavailable (no live pointer) or blocked: {enqueued:false,
--     reason:'source_unavailable'}. (epoch, fingerprint) other than the expected: {enqueued:false, obsolete:true}.
--     Neither mutates anything.
--  2. Acts only on an EXISTING job for exactly that incarnation (Pipeline ae37e77b3809; initial creation stays on the
--     originating route): one UPDATE re-queues it when it is 'superseded'. Never an INSERT.
--  3. Otherwise the stored row decides: none -> {enqueued:false, reason:'no_job'}; same incarnation ->
--     {enqueued:true, changed:false, state} (untouched; 'failed' is not recovery); newer epoch, or same epoch with
--     another fingerprint -> {obsolete, newer_job}; an older incarnation's job -> {enqueued:false, 'no_exact_job'}.
-- So this function never creates a job, never writes another incarnation's job and never clears another
-- incarnation's lease. Epochs are nextval(session_replay_epoch_seq). Source currency is NOT asserted: if the source
-- moves after step 1, step 2 can at most re-queue the expected incarnation's own superseded job, which claim marks
-- superseded again. Only the exact row's UPDATE row lock; no lock across I/O.
-- Grants as 20261105021000's enqueue: REVOKE ALL from PUBLIC, anon, authenticated, service_role; EXECUTE to
-- service_role. Metadata only. Dormant: nothing calls it unless SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true.
-- Code only: applying it to any hosted database needs Khaliq's separate explicit approval.
-- TEST GATE (prototype only, remove before any migration): when the session setting rh.large_read_test_gate is set,
-- the function waits on that advisory key after its capture and validation, so a test can commit a source change
-- in that exact window from another backend.

CREATE FUNCTION sessions.enqueue_session_large_read_expected(p_org text,p_workspace text,p_harness text,p_session text,
 p_expected_epoch bigint,p_expected_fingerprint text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $expected$
DECLARE c jsonb;canonical_id text;e bigint;f text;j sessions.session_large_read_jobs%ROWTYPE;
BEGIN
 IF p_expected_epoch IS NULL OR p_expected_epoch<1 OR p_expected_fingerprint IS NULL OR p_expected_fingerprint !~ '^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='large read: invalid expected incarnation';
 END IF;
 -- capture validates the scope (22023) and raises its own refusals (54000, 55000).
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('enqueued',false,'reason','source_unavailable'); END IF;
 IF c->'source'->>'epoch' IS DISTINCT FROM p_expected_epoch::text OR c->'source'->>'fingerprint' IS DISTINCT FROM p_expected_fingerprint THEN
  RETURN jsonb_build_object('enqueued',false,'obsolete',true,'reason','incarnation_moved');
 END IF;
 IF coalesce(current_setting('rh.large_read_test_gate',true),'')<>'' THEN
  PERFORM pg_advisory_xact_lock_shared(current_setting('rh.large_read_test_gate')::bigint);
 END IF;
 canonical_id:=c->'identity'->>'sessionId';e:=p_expected_epoch;f:=p_expected_fingerprint;
 -- Pipeline ae37e77b3809: a delayed hold may act only on an EXISTING job for exactly the expected incarnation; initial
 -- creation stays on the originating route (enqueue_session_large_read). An empty slot or another incarnation's job
 -- gives this call no authority: no insert, no rewrite.
 UPDATE sessions.session_large_read_jobs SET state='queued',available_at=clock_timestamp(),lease_nonce=NULL,lease_expires_at=NULL,
   attempts=0,last_error=NULL,updated_at=now()
  WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=canonical_id
   AND source_epoch=e AND source_fingerprint=f AND state='superseded';
 IF FOUND THEN
  RETURN jsonb_build_object('enqueued',true,'changed',true,'state','queued','identity',c->'identity','source',c->'source');
 END IF;
 SELECT * INTO j FROM sessions.session_large_read_jobs WHERE org_id=p_org AND workspace_id=p_workspace AND harness=p_harness AND session_id=canonical_id;
 IF NOT FOUND THEN RETURN jsonb_build_object('enqueued',false,'reason','no_job'); END IF;
 IF j.source_epoch=e AND j.source_fingerprint=f THEN
  -- Same incarnation, not rewritten: the stored state is returned; 'failed' is not durable recovery.
  RETURN jsonb_build_object('enqueued',true,'changed',false,'state',j.state,'identity',c->'identity','source',c->'source');
 END IF;
 IF j.source_epoch>e OR (j.source_epoch=e AND j.source_fingerprint<>f) THEN
  RETURN jsonb_build_object('enqueued',false,'obsolete',true,'reason','newer_job');
 END IF;
 RETURN jsonb_build_object('enqueued',false,'reason','no_exact_job');
END $expected$;

REVOKE ALL ON FUNCTION sessions.enqueue_session_large_read_expected(text,text,text,text,bigint,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.enqueue_session_large_read_expected(text,text,text,text,bigint,text) TO service_role;

-- TEST ONLY: enqueue_session_large_read (20261105021000) verbatim, plus the same gate after its capture, to show how
-- the ordinary enqueue behaves when the source moves between its capture and its upsert.
CREATE FUNCTION sessions.enqueue_session_large_read_gated(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue$
DECLARE c jsonb; e bigint; f text; canonical_id text;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('enqueued',false,'reason','source_unavailable'); END IF;
 IF coalesce(current_setting('rh.large_read_test_gate',true),'')<>'' THEN
  PERFORM pg_advisory_xact_lock_shared(current_setting('rh.large_read_test_gate')::bigint);
 END IF;
 canonical_id:=c->'identity'->>'sessionId';e:=(c->'source'->>'epoch')::bigint;f:=c->'source'->>'fingerprint';
 INSERT INTO sessions.session_large_read_jobs AS j(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint)
 VALUES(p_org,p_workspace,p_harness,canonical_id,e,f)
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET
 source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,state='queued',
 available_at=clock_timestamp(),lease_nonce=NULL,lease_expires_at=NULL,attempts=0,last_error=NULL,updated_at=now()
 WHERE j.source_epoch IS DISTINCT FROM excluded.source_epoch OR j.source_fingerprint IS DISTINCT FROM excluded.source_fingerprint
    OR j.state='superseded';
 RETURN jsonb_build_object('enqueued',true,'changed',FOUND,'identity',c->'identity','source',c->'source');
END $enqueue$;
