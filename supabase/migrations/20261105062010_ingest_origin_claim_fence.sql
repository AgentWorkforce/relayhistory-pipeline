-- Slot 20261105062010 (assigned by the lander; the first slot after 062000). Per-origin serialization for the
-- buffered upload hot path. Default off: nothing calls the three new entry points until a Worker runs with
-- INGEST_ORIGIN_CLAIM_FENCE=on (src/session-ingest/ledger-sql.ts originClaimFence). Code only: applying this to
-- any real database needs Khaliq's explicit OK. Design: docs/design/per-origin-claim-fence.md.
--
-- Problem. Every buffered claim, stage prepare and commit takes the user's sessions.ingest_fence_owners row
-- FOR UPDATE (sessions.ingest_lock_fences, last replaced by 20261105058010). A user with several machines (one
-- origin each) therefore serializes against themself: one machine's 2-9 s delivery makes the next machine's
-- claim wait, and under lock_timeout/statement_timeout it fails 55P03/57014. Ordering ACROSS one user's machines
-- is not required (product decision, 2026-11-16); ordering WITHIN one origin stream is.
--
-- What the per-user row protects, and what this keeps (docs/design/per-origin-claim-fence.md section 2):
--   * exclusion of every per-user control (selection, reset, ordered controls, privacy/consent authority writes
--     that go through ingest_lock_fences, the buffered GC claim, publication and history claims, seat/seal
--     adoption by owner paths): they keep FOR UPDATE, which conflicts with FOR SHARE, so a control still waits
--     for every in-flight claim of the user and every claim still waits for an in-flight control;
--   * the operator user erase: unchanged. It takes (961996,50000) EXCLUSIVE and the fence row FOR UPDATE; every
--     claim still INSERTs the fence row first (20261105050010's statement trigger takes 50000 SHARED) and still
--     runs the RH041 check after its row lock;
--   * first-seen user adoption (ingest_lazy_user): decided by its own INSERT ... ON CONFLICT DO NOTHING
--     RETURNING on the fence row, which waits for an uncommitted first insert of another origin; unchanged;
--   * per-origin state (batches, attempts, ownership, fence versions, deletions, custody slots, stages, outbox):
--     serialized per (org, workspace, user, origin) by the new origin transaction advisory lock below;
--   * global seat numbering: still the seat clock (043500/043800); native slots still the (org, ws, origin)
--     advisory lock and the slot rows.
-- Only claim -> claim of DIFFERENT origins of one user stop excluding each other.
--
-- What it adds:
--   sessions.ingest_origin_claim_marks        owner-only; one row per in-flight origin-scoped call, keyed by the
--                                             top-level transaction id, deleted before the call returns (an error
--                                             rolls it back). No org_id/workspace_id/user_id columns on purpose (the
--                                             tenant is one text value): ingest_lazy_user / ingest_lazy_begin treat a
--                                             row of any ingest_ table WITH those columns as prior authority for a new
--                                             user/org, and the operator user erase inventories such tables.
--   sessions.ingest_origin_claim_enter(p)     owner-only: role check, tenant/origin from ingest_batch_key, the
--                                             origin advisory lock, then the mark.
--   sessions.ingest_origin_claim_leave(x)     owner-only: deletes the mark.
--   sessions.ingest_origin_claim_buffered_batch(p), sessions.ingest_origin_prepare_buffer_stage(p),
--   sessions.ingest_origin_commit_buffered_batch(p)
--                                             SECURITY DEFINER, EXECUTE to bulls96_session_ingest only: enter, the
--                                             UNCHANGED existing function with the same input, leave. Same result.
-- What it replaces (owner-only, signature/owner/ACL kept; the rollback restores 058010's exact body):
--   sessions.ingest_lock_fences(text,text,text)  FOR SHARE instead of FOR UPDATE only while the current
--                                             transaction holds a mark for exactly this (org, workspace, user);
--                                             otherwise byte-for-byte the 058010 behaviour.
-- The mark cannot be forged: the table is owner-only and only enter writes it, after taking the origin lock. A
-- runtime role setting a GUC, taking advisory locks or crafting a call stack gains nothing.
-- ingest_lock_fences stays SECURITY INVOKER (as in 058010) and the mark table is owner-only with RLS on: the lock
-- reads the mark with its caller's rights. Every caller today is owner-owned (SECURITY DEFINER wrappers or owner
-- paths). A FUTURE SECURITY DEFINER function owned by another role that calls it gets 42501 on the mark read (fail
-- closed), never a silent FOR SHARE.
--
-- Lock order (claims): origin advisory (new namespace, only ever taken first, by enter) -> (961996,50000) S ->
-- fence row S -> seat clock -> (org,ws,origin) native advisory -> slot rows. Controls: 50000 S -> fence row X.
-- Erase: 50000 X -> fence row X. Nobody takes the new origin key after any other lock, so it adds no cycle; a
-- Worker without the flag (or a mixed fleet during rollout) never takes it.
--
-- Interaction with 20261105044030 (generation overlap): with admitDuringApply (off by default) a pending-marker
-- call is admitted only when the outermost sessions frame is one of the five listed entry points; the three new
-- entry points are not listed, so during such an apply they fail closed (55000), exactly as any other caller.
-- With the marker at PASS (every normal window) nothing changes. Keep the Worker flag OFF during any schema apply.
--
-- Operational notes: every origin-scoped call inserts and deletes one mark row (vacuum churn; the table stays tiny),
-- and concurrent FOR SHARE holders of one fence row create MultiXacts (autovacuum freezes them; watch
-- multixact age if a user is extremely hot). A steady stream of FOR SHARE claims can delay a per-user FOR UPDATE
-- control (selection/reset) of that user; the operator erase is not starved: it takes (961996,50000) EXCLUSIVE
-- first, and every claim needs that lock SHARED (the fence-row INSERT trigger) before its row lock.

CREATE TABLE sessions.ingest_origin_claim_marks (
 xact xid8 PRIMARY KEY,
 tenant text NOT NULL
);
ALTER TABLE sessions.ingest_origin_claim_marks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_origin_claim_marks
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_origin_claim_enter(p jsonb) RETURNS xid8
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $origin_enter$
DECLARE k record;v xid8;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 IF jsonb_typeof(p) IS DISTINCT FROM 'object' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid origin-scoped input';END IF;
 k:=sessions.ingest_batch_key(p);
 -- Same-origin calls of one user serialize here, before any other lock: the per-origin stream order the per-user
 -- fence row used to give them. Different origins (and different users) never share this key.
 PERFORM pg_advisory_xact_lock(hashtextextended(
  jsonb_build_array('96bulls:ingest-origin-claim-v1',k.org_id,k.workspace_id,k.user_id,k.origin_id)::text,0));
 -- The mark's key is the TOP-LEVEL xid (also inside a subtransaction). One call at a time per transaction: leave
 -- deletes the mark before the wrapper returns, so sequential calls are fine; a wrapper nested inside another
 -- wrapper in one transaction would raise 23505 here (no such caller exists).
 v:=pg_current_xact_id();
 INSERT INTO sessions.ingest_origin_claim_marks(xact,tenant)
  VALUES(v,jsonb_build_array(k.org_id,k.workspace_id,k.user_id)::text);
 RETURN v;
END $origin_enter$;

CREATE FUNCTION sessions.ingest_origin_claim_leave(p_xact xid8) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $origin_leave$
BEGIN
 DELETE FROM sessions.ingest_origin_claim_marks WHERE xact=p_xact;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: origin claim mark lost';END IF;
END $origin_leave$;

CREATE FUNCTION sessions.ingest_origin_claim_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $origin_claim$
DECLARE v_mark xid8;v_result jsonb;
BEGIN
 v_mark:=sessions.ingest_origin_claim_enter(p);
 v_result:=sessions.ingest_claim_buffered_batch(p);
 PERFORM sessions.ingest_origin_claim_leave(v_mark);
 RETURN v_result;
END $origin_claim$;

CREATE FUNCTION sessions.ingest_origin_prepare_buffer_stage(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $origin_prepare$
DECLARE v_mark xid8;v_result jsonb;
BEGIN
 v_mark:=sessions.ingest_origin_claim_enter(p);
 v_result:=sessions.ingest_prepare_buffer_stage(p);
 PERFORM sessions.ingest_origin_claim_leave(v_mark);
 RETURN v_result;
END $origin_prepare$;

CREATE FUNCTION sessions.ingest_origin_commit_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $origin_commit$
DECLARE v_mark xid8;v_result jsonb;
BEGIN
 v_mark:=sessions.ingest_origin_claim_enter(p);
 v_result:=sessions.ingest_commit_buffered_batch(p);
 PERFORM sessions.ingest_origin_claim_leave(v_mark);
 RETURN v_result;
END $origin_commit$;

-- 20261105058010's body; the only change is the row-lock strength under a mark of this exact tenant. The mark is
-- read with pg_current_xact_id_if_assigned(): a transaction without an xid holds no mark (enter assigns one).
CREATE OR REPLACE FUNCTION sessions.ingest_lock_fences(p_org text,p_ws text,p_user text)
RETURNS void LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lock$
BEGIN
 INSERT INTO sessions.ingest_fence_owners VALUES(p_org,p_ws,p_user) ON CONFLICT DO NOTHING;
 IF EXISTS(SELECT 1 FROM sessions.ingest_origin_claim_marks AS m
   WHERE m.xact=pg_current_xact_id_if_assigned() AND m.tenant=jsonb_build_array(p_org,p_ws,p_user)::text) THEN
  PERFORM 1 FROM sessions.ingest_fence_owners AS f
  WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.user_id=p_user FOR SHARE;
 ELSE
  PERFORM 1 FROM sessions.ingest_fence_owners AS f
  WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.user_id=p_user FOR UPDATE;
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures AS e
   WHERE e.org_id=p_org AND e.workspace_id=p_ws AND e.user_id=p_user AND e.state<>'bytes_verified') THEN
  RAISE EXCEPTION USING ERRCODE='RH041',
   MESSAGE='operator user erase: uploads for this user are refused until the erase is verified';
 END IF;
END
$lock$;

REVOKE ALL ON FUNCTION sessions.ingest_lock_fences(text,text,text),
 sessions.ingest_origin_claim_enter(jsonb),sessions.ingest_origin_claim_leave(xid8)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
REVOKE ALL ON FUNCTION sessions.ingest_origin_claim_buffered_batch(jsonb),
 sessions.ingest_origin_prepare_buffer_stage(jsonb),sessions.ingest_origin_commit_buffered_batch(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_origin_claim_buffered_batch(jsonb),
 sessions.ingest_origin_prepare_buffer_stage(jsonb),sessions.ingest_origin_commit_buffered_batch(jsonb)
 TO bulls96_session_ingest;
