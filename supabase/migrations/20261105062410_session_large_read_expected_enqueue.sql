-- Large-read enqueue for a delayed SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED hold (slot 062410, assigned by
-- layered-sessions-pipeline 220fe2274d20; code only). Contract (Pipeline ae37e77b3809): a delayed hold may act ONLY on
-- an existing job for exactly the incarnation it waits for; an empty slot or another incarnation's job gives it no
-- authority. Initial creation stays on the originating route (enqueue_session_large_read, unchanged).
--
-- sessions.enqueue_session_large_read_expected(org, workspace, harness, session, p_expected_epoch,
-- p_expected_fingerprint):
--  1. c := capture_session_replay_pages(...): unavailable (no live pointer) or blocked -> {enqueued:false,
--     reason:'source_unavailable'}; another (epoch, fingerprint) -> {enqueued:false, obsolete:true,
--     reason:'incarnation_moved'}. Neither mutates anything.
--  2. One UPDATE re-queues the expected incarnation's own job, only when it is 'superseded'. Never an INSERT.
--  3. Otherwise the stored row decides, with no mutation: none -> 'no_job'; same incarnation -> {enqueued:true,
--     changed:false, state} ('failed' is not recovery); newer epoch, or the same epoch with another fingerprint ->
--     {obsolete, 'newer_job'}; an older incarnation's job -> 'no_exact_job'.
-- It never creates a job, never writes another incarnation's job and never clears another incarnation's lease.
-- Source currency is NOT asserted (no lock is shared by every source-set writer: pointer, pending and history writers
-- differ): if the source moves after step 1, step 2 can at most re-queue the expected incarnation's own superseded
-- job, which claim_session_large_read marks superseded again. Only the exact row's UPDATE row lock; no lock across
-- I/O. Epochs are nextval(session_replay_epoch_seq).
-- Also replaces sessions.enqueue_session_large_read (20261105021000; signature, owner, SECURITY DEFINER, search_path
-- and ACL unchanged by CREATE OR REPLACE): its upsert no longer replaces a job whose source epoch is NEWER than the one
-- it captured (Pipeline b0e93ec730c1: a delayed ordinary enqueue of B overwrote C's newer, leased job). Within one epoch
-- the 021000 rule is kept. Its body is otherwise byte for byte 021000's.
-- Grants as 20261105021000's enqueue: REVOKE ALL from PUBLIC, anon, authenticated, service_role; EXECUTE to
-- service_role. Metadata only. Dormant: called only with SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED=true.
-- Code only: applying it to any hosted database needs Khaliq's separate explicit approval.
SET LOCAL lock_timeout='5s';

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

-- The ordinary enqueue, 20261105021000's body with the newer-epoch guard (see header).
CREATE OR REPLACE FUNCTION sessions.enqueue_session_large_read(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue$
DECLARE c jsonb; e bigint; f text; canonical_id text;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('enqueued',false,'reason','source_unavailable'); END IF;
 canonical_id:=c->'identity'->>'sessionId';e:=(c->'source'->>'epoch')::bigint;f:=c->'source'->>'fingerprint';
 INSERT INTO sessions.session_large_read_jobs AS j(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint)
 VALUES(p_org,p_workspace,p_harness,canonical_id,e,f)
 ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET
 source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,state='queued',
 available_at=clock_timestamp(),lease_nonce=NULL,lease_expires_at=NULL,attempts=0,last_error=NULL,updated_at=now()
 -- 062410: never replace a newer incarnation's job (a delayed enqueue of B must not overwrite C, nor clear its lease).
 -- Epochs are nextval(session_replay_epoch_seq). Within one epoch the 021000 rule is kept (another fingerprint, or
 -- 'superseded', re-queues).
 WHERE j.source_epoch < excluded.source_epoch
    OR (j.source_epoch = excluded.source_epoch AND (j.source_fingerprint IS DISTINCT FROM excluded.source_fingerprint OR j.state='superseded'));
 RETURN jsonb_build_object('enqueued',true,'changed',FOUND,'identity',c->'identity','source',c->'source');
END $enqueue$;
