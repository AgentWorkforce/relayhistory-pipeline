-- Buffered search gate: retired-claim witness (slot 043890, follows 20261105043880).
--
-- Why. 20261105043880 (ingest bookkeeping cleanup, Phase A) retires a terminal claim by inserting
-- a tombstone into sessions.ingest_bookkeeping_retired_claims and, in the same transaction,
-- deleting the claim's sessions.ingest_lazy_ack_decisions rows. 20261105043860's gate judges
-- admission_version_mismatch with
--   NOT EXISTS (decision of the claim WITH a different source_control_version)
-- so once the decisions are gone that witness is vacuously true: a hit the gate hid as
-- admission_version_mismatch before the cleanup could pass that branch after it, without anything
-- about its admission changing. Deleting evidence must never make a hit MORE visible. This
-- migration replaces sessions.buffered_search_gate_for so a tombstoned claim is hidden explicitly.
--
-- What. CREATE OR REPLACE of sessions.buffered_search_gate_for with the SAME signature, return
-- type, language, volatility (STABLE), SECURITY DEFINER and search_path. The body is 043860's,
-- byte for byte, plus two additions:
--   * a judged-CTE column
--       retired = EXISTS (SELECT 1 FROM sessions.ingest_bookkeeping_retired_claims r
--                          WHERE r.claim_token = a.claim_token)
--   * one CASE branch, after admission_epoch_mismatch and before admission_version_mismatch:
--       WHEN j.retired IS NOT FALSE THEN 'retired'
-- Reason table: 043860's header, first match, with one new row:
--     admission_epoch_mismatch   unchanged (043860);
--     retired                    NEW: the claim has a sessions.ingest_bookkeeping_retired_claims
--                                tombstone (20261105043880 Phase A retired it and deleted its lazy
--                                ACK decisions, so the decision-version witness of
--                                admission_version_mismatch is gone);
--     admission_version_mismatch, admission_session_mismatch, gate_closed, parity_missing,
--     parity_stale, deleted, batch_not_buffered, reset, superseded: unchanged (043860).
-- The 043700 PARITY table and THREAT MODEL of 043860 still apply; their line numbers refer to
-- 043860's file. Retirement only adds a drop reason: the new column only reads the tombstone
-- table by claim token, and 'retired' can only replace a NULL (visible) or a later reason, never
-- reveal more. The only new disclosure is "this claim of the owner was retired", which is no more
-- than the 043860 reasons already reveal about that claim's bookkeeping.
--
-- Additive. No table, column, index, role, grant or row is created, altered or deleted. Callers
-- see one more drop_reason value; src/session-read-api/buffered-search-gate.ts lists it in
-- GATE_SQL_REASONS (an unknown reason was already mapped to admission_unknown_reason and dropped,
-- so an older Worker stays fail closed against this function).
--
-- Locks. As 043860: STABLE, read-only, AccessShareLock only (now also on
-- ingest_bookkeeping_retired_claims, read by its primary key). CREATE OR REPLACE takes an
-- AccessExclusiveLock on the function's pg_proc row for the migration's transaction; lock_timeout
-- bounds the wait.
--
-- Roles. Never creates or alters a role. CREATE OR REPLACE keeps 043860's owner and ACL; this file
-- re-issues 043860's REVOKE and its revoke sweep but no GRANT (the bulls96_read_api EXECUTE grant
-- belongs to 043860's lane, scripts/sessions-function-boundary/lanes/buffered-search-gate.ts; the
-- replacement lane is lanes/buffered-search-gate-retired-witness.ts), and a post-condition refuses
-- (55000) unless the function is still SECURITY DEFINER, STABLE, owned by the applier and
-- executable by bulls96_read_api. src/hoopdb/sessions-function-acl.json is unchanged.
--
-- Schema version. Applying this migration changes the migration-set generation()
-- (src/hoopdb/migrate.ts), as 043860 did.
--
-- Rollout. Code only; nothing here is applied, deployed, enabled or granted by this repository
-- (applying it to any real database needs Khaliq's explicit OK). Safe in either
-- READ_API_BUFFERED_SEARCH_GATE state: off, the gate is not called; on, retired claims are hidden.
-- Apply it before (or together with) enabling the 043880 decisions phase. Down path (generated):
-- supabase/rollbacks/20261105043890_buffered_search_gate_retired_witness.down.sql, refused while
-- any claim is retired.
SET LOCAL lock_timeout = '5s';
DO $buffered_search_gate_retired_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF NOT EXISTS(SELECT 1 FROM pg_proc p
   WHERE p.oid=to_regprocedure('sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)')
     AND p.proowner=me AND p.prosecdef)
  OR NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relname='ingest_bookkeeping_retired_claims' AND c.relkind='r' AND c.relowner=me)
  OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_read_api') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search gate retired witness: requires the 20261105043860 gate and the 20261105043880 tombstone table, both owned by the applier, and bulls96_read_api';END IF;
END $buffered_search_gate_retired_creator$;

CREATE OR REPLACE FUNCTION sessions.buffered_search_gate_for(
  p_org_id text, p_workspace_id text, p_owner_user_id text,
  p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text, p_head_kind text,
  p_head_pending boolean, p_privacy_baseline_sha256 text, p_hits jsonb)
RETURNS TABLE(claim_token text, source text, session_id text, drop_reason text)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp
AS $gate$
#variable_conflict use_column
DECLARE
  v_org text := nullif(current_setting('bulls96_read.org_id', true), '');
  v_ws text := nullif(current_setting('bulls96_read.workspace_id', true), '');
  v_has_head boolean := p_head_epoch IS NOT NULL OR p_head_version IS NOT NULL OR p_head_digest IS NOT NULL
                        OR p_head_kind IS NOT NULL;
  v_well_formed boolean;
  v_version bigint;
  v_adm_version bigint;
  v_gate text;
  v_resets bigint;
  v_hit jsonb;
  pol sessions.ingest_lazy_policy%ROWTYPE;
  par sessions.owner_buffered_parity%ROWTYPE;
  x record;
BEGIN
  IF session_user::text IS DISTINCT FROM 'bulls96_read_api' OR coalesce(current_setting('role', true), 'none') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'buffered search gate: caller is not the read api role';
  END IF;
  IF v_org IS NULL OR v_ws IS NULL OR p_org_id IS DISTINCT FROM v_org OR p_workspace_id IS DISTINCT FROM v_ws THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'buffered search gate: tenant is not the bound tenant';
  END IF;
  -- Shapes, exactly owner_buffered_read_scope's (CASE keeps the bigint range check behind the
  -- regex). Malformed input: no rows (the read API drops every hit of this owner).
  v_well_formed := coalesce(p_cutover_epoch ~ '^[0-9a-f]{64}$', false)
    AND coalesce(p_privacy_baseline_sha256 ~ '^[0-9a-f]{64}$', false) AND p_head_pending IS NOT NULL
    AND CASE WHEN NOT v_has_head THEN NOT p_head_pending
             ELSE coalesce(p_head_epoch ~ '^[0-9a-f]{64}$', false)
                  AND coalesce(p_head_digest ~ '^[0-9a-f]{64}$', false)
                  AND coalesce(p_head_kind IN ('selection', 'reset'), false)
                  AND CASE WHEN p_head_version ~ '^[1-9][0-9]{0,18}$'
                           THEN p_head_version::numeric <= 9223372036854775807 ELSE false END
        END
    AND coalesce(p_owner_user_id <> '', false)
    AND coalesce(jsonb_typeof(p_hits) = 'array', false);
  IF NOT v_well_formed THEN
    RETURN;
  END IF;
  IF jsonb_array_length(p_hits) NOT BETWEEN 1 AND 500 THEN
    RETURN;
  END IF;
  FOR v_hit IN SELECT t.e FROM jsonb_array_elements(p_hits) AS t(e) LOOP
    IF jsonb_typeof(v_hit) IS DISTINCT FROM 'object' THEN
      RETURN;
    END IF;
    IF ARRAY(SELECT k.k FROM jsonb_object_keys(v_hit) AS k(k) ORDER BY k.k COLLATE "C")
         IS DISTINCT FROM ARRAY['claim_token', 'session_id', 'source']::text[] THEN
      RETURN;
    END IF;
    IF jsonb_typeof(v_hit -> 'claim_token') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_hit -> 'source') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_hit -> 'session_id') IS DISTINCT FROM 'string' THEN
      RETURN;
    END IF;
    IF (v_hit ->> 'claim_token') !~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
       OR (v_hit ->> 'source') = '' OR (v_hit ->> 'session_id') = '' THEN
      RETURN;
    END IF;
  END LOOP;
  IF v_has_head THEN
    v_version := p_head_version::bigint;
  END IF;
  -- Required admission (owner_buffered_read_scope): version = head version, 0 without a head.
  v_adm_version := coalesce(v_version, 0::bigint);

  -- The gate and the G2 parity proof, exactly owner_buffered_read_scope's conditions for
  -- (org, ws, owner). Plain snapshot reads, no lock.
  IF p_head_pending OR (v_has_head AND p_head_epoch IS DISTINCT FROM p_cutover_epoch)
     OR EXISTS (SELECT 1 FROM sessions.ingest_privacy_authority AS pa WHERE pa.singleton AND pa.state = 'fenced') THEN
    v_gate := 'gate_closed';
  ELSE
    SELECT lp.* INTO pol FROM sessions.ingest_lazy_policy AS lp
     WHERE lp.singleton AND lp.valid_until > clock_timestamp();
    IF NOT FOUND OR pol.control_epoch IS DISTINCT FROM p_cutover_epoch
       OR pol.privacy_baseline_sha256 IS DISTINCT FROM p_privacy_baseline_sha256 THEN
      v_gate := 'gate_closed';
    ELSE
      SELECT bp.* INTO par FROM sessions.owner_buffered_parity AS bp
       WHERE bp.org_id = v_org AND bp.workspace_id = v_ws AND bp.user_id = p_owner_user_id;
      IF NOT FOUND THEN
        v_gate := 'parity_missing';
      ELSE
        SELECT e.* INTO x
          FROM sessions.owner_buffered_parity_expected(v_org, v_ws, p_owner_user_id, p_cutover_epoch, v_version,
                                                       p_head_digest, p_head_kind) AS e;
        IF NOT coalesce(x.receipts_ok AND x.target_generation IS NOT NULL
             AND par.cutover_epoch = p_cutover_epoch
             AND par.privacy_baseline_sha256 = p_privacy_baseline_sha256
             AND par.no_head = NOT v_has_head
             AND par.head_epoch IS NOT DISTINCT FROM p_head_epoch
             AND par.head_version IS NOT DISTINCT FROM v_version
             AND par.head_digest IS NOT DISTINCT FROM p_head_digest
             AND par.head_kind IS NOT DISTINCT FROM p_head_kind
             AND par.target_generation = x.target_generation
             AND par.deletion_set_digest = x.deletion_set_digest
             AND par.reset_set_digest = x.reset_set_digest
             AND par.target_lineage_digest = x.target_lineage_digest, false) THEN
          v_gate := 'parity_stale';
        END IF;
      END IF;
    END IF;
  END IF;

  -- 043700 ctl.resets: the owner's upload_resets count, read once per call.
  SELECT count(*) INTO v_resets FROM sessions.upload_resets AS ur
   WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = p_owner_user_id;

  RETURN QUERY
  WITH hits AS (
    SELECT DISTINCT (t.e ->> 'claim_token')::uuid AS h_token, t.e ->> 'source' AS h_source, t.e ->> 'session_id' AS h_session
      FROM jsonb_array_elements(p_hits) AS t(e)
  ), lp AS (
    SELECT p.control_epoch AS lp_epoch, p.source_identity_sha256 AS lp_identity
      FROM sessions.ingest_lazy_policy AS p WHERE p.singleton
  ), judged AS (
    SELECT h.h_token, h.h_source, h.h_session,
           (SELECT lp.lp_epoch FROM lp
             WHERE a.source_identity_sha256 IS NOT NULL AND lp.lp_identity = a.source_identity_sha256) AS adm_epoch,
           a.source_control_version AS adm_version,
           NOT EXISTS (SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS d
                        WHERE d.claim_token = a.claim_token
                          AND d.source_control_version IS DISTINCT FROM a.source_control_version) AS version_consistent,
           -- 20261105043890: the 043880 cleanup (Phase A) tombstoned this claim and deleted its lazy ACK
           -- decisions, so version_consistent above no longer witnesses anything for it.
           EXISTS (SELECT 1 FROM sessions.ingest_bookkeeping_retired_claims AS r
                    WHERE r.claim_token = a.claim_token) AS retired,
           -- The admission witness is bound to the hit: the claim's immutable original records
           -- (one element per claimed record: ord, source, session_id, ...) hold this session.
           (a.records_retired_at IS NULL AND jsonb_typeof(a.records) = 'array'
            AND a.records @> jsonb_build_array(jsonb_build_object('source', h.h_source, 'session_id', h.h_session))) AS covers,
           -- RULE 5(a)/(b): ANY origin of the owner.
           (EXISTS (SELECT 1 FROM sessions.upload_deletions AS ud
                     WHERE ud.org_id = v_org AND ud.workspace_id = v_ws AND ud.user_id = p_owner_user_id
                       AND ud.source = h.h_source AND ud.session_id = h.h_session)
            OR EXISTS (SELECT 1 FROM sessions.ingest_frozen_source_deletions AS fd
                        WHERE fd.org_id = v_org AND fd.workspace_id = v_ws AND fd.user_id = p_owner_user_id
                          AND fd.source = h.h_source AND fd.session_id = h.h_session)) AS is_deleted,
           -- 043700 stg batch predicate: the claim's batch row exists, is the owner's, carries the
           -- claim's digest and is committed, buffered and accepted. A missing row fails closed.
           coalesce(b.user_id = p_owner_user_id AND b.digest = a.digest AND b.state = 'committed'
                    AND b.publication_state = 'buffered' AND b.committed_at IS NOT NULL, false) AS batch_ok,
           -- RULE 5(c): the claim's admission reset epoch and the per-session epochs entry's reset
           -- epoch are the owner's current upload_resets count, and no reset of the owner is at or
           -- after the batch's acceptance (committed_at). A NULL anywhere hides.
           (a.reset_epoch IS NULL OR a.reset_epoch IS DISTINCT FROM v_resets
            OR b.committed_at IS NULL
            OR EXISTS (SELECT 1 FROM sessions.upload_resets AS ur
                        WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = p_owner_user_id
                          AND (ur.reset_at IS NULL OR ur.reset_at >= b.committed_at))
            OR (ev.n > 0 AND ev.reset_epoch IS DISTINCT FROM v_resets)) AS reset_behind,
           -- 043700 judged.live (ingest_attempt_fenced): exactly one unambiguous epochs entry for
           -- (source, session) whose fence epoch is still the session's current fence version
           -- (no fence row = 0) on the claim's origin. A later deletion, disable or reset bumps the
           -- fence and supersedes this admission; a missing or ambiguous entry fails closed.
           coalesce(ev.n > 0 AND ev.epoch = ev.epoch_max AND ev.reset_epoch = ev.reset_max
                    AND coalesce(fv.epoch, 0) = ev.epoch, false) AS is_latest
      FROM hits AS h
      JOIN sessions.ingest_claim_attempts AS a
        ON a.claim_token = h.h_token AND a.org_id = v_org AND a.workspace_id = v_ws AND a.user_id = p_owner_user_id
      LEFT JOIN sessions.ingest_batches AS b
        ON b.org_id = a.org_id AND b.workspace_id = a.workspace_id AND b.origin_id = a.origin_id
       AND b.batch_id = a.batch_id
      LEFT JOIN LATERAL (
        SELECT min(x.epoch) AS epoch, max(x.epoch) AS epoch_max, min(x.reset_epoch) AS reset_epoch,
               max(x.reset_epoch) AS reset_max, count(*) AS n
          FROM jsonb_to_recordset(CASE WHEN jsonb_typeof(a.epochs) = 'array' THEN a.epochs ELSE '[]'::jsonb END)
               AS x(source text, session_id text, epoch bigint, reset_epoch bigint)
         WHERE x.source = h.h_source AND x.session_id = h.h_session) AS ev ON true
      LEFT JOIN sessions.ingest_fence_versions AS fv
        ON fv.org_id = v_org AND fv.workspace_id = v_ws AND fv.user_id = p_owner_user_id
       AND fv.origin_id = a.origin_id AND fv.source = h.h_source AND fv.session_id = h.h_session
  )
  SELECT j.h_token::text, j.h_source, j.h_session,
         CASE
           WHEN j.adm_epoch IS NULL OR j.adm_epoch IS DISTINCT FROM p_cutover_epoch THEN 'admission_epoch_mismatch'
           WHEN j.retired IS NOT FALSE THEN 'retired'
           WHEN j.version_consistent IS NOT TRUE OR j.adm_version IS NULL
                OR j.adm_version IS DISTINCT FROM v_adm_version THEN 'admission_version_mismatch'
           WHEN j.covers IS NOT TRUE THEN 'admission_session_mismatch'
           WHEN v_gate IS NOT NULL THEN v_gate
           WHEN j.is_deleted IS NOT FALSE THEN 'deleted'
           WHEN j.batch_ok IS NOT TRUE THEN 'batch_not_buffered'
           WHEN j.reset_behind IS NOT FALSE THEN 'reset'
           WHEN j.is_latest IS NOT TRUE THEN 'superseded'
         END::text
    FROM judged AS j;
END
$gate$;

REVOKE ALL ON FUNCTION sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;
-- No GRANT: CREATE OR REPLACE keeps 043860's owner and ACL, and that grant belongs to 043860's lane
-- (scripts/sessions-function-boundary/lanes/buffered-search-gate.ts). The post-condition below
-- proves it survived.

-- As in 20261105043860 (and 043700): strip EXECUTE from every other grantee present now (default
-- privileges, a pre-existing runtime role). A grant added later is still refused at call time
-- by the role guard.
DO $revoke$
DECLARE
  v_fn regprocedure := 'sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)'::regprocedure;
  v_grantee text;
BEGIN
  FOR v_grantee IN
    SELECT r.rolname
      FROM pg_proc AS p
      CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
      JOIN pg_roles AS r ON r.oid = a.grantee
     WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
       AND r.rolname <> 'bulls96_read_api'
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
  END LOOP;
END
$revoke$;

DO $buffered_search_gate_retired_post$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_proc p
   WHERE p.oid=to_regprocedure('sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)')
     AND p.prosecdef AND p.provolatile='s' AND p.proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
     AND p.prosrc LIKE '%ingest_bookkeeping_retired_claims%')
  OR NOT has_function_privilege('bulls96_read_api',
   'sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)','EXECUTE') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search gate retired witness: replacement lost SECURITY DEFINER, STABLE, its owner or the read API grant';END IF;
END $buffered_search_gate_retired_post$;
