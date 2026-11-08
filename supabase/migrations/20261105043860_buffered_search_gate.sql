-- Buffered search gate reader (slot 043860, assigned by the registry keeper).
--
-- The session read Worker (role bulls96_read_api) gates buffered-origin hits in
-- transcript_events (READ_API_BUFFERED_SEARCH_GATE, src/session-read-api/buffered-search-gate.ts).
-- The gate reads sessions.ingest_claim_attempts, ingest_batches, ingest_fence_versions,
-- ingest_lazy_policy, ingest_lazy_ack_decisions, owner_buffered_parity (and, through
-- owner_buffered_parity_expected, ingest_control_receipts), ingest_privacy_authority,
-- upload_resets, upload_deletions and ingest_frozen_source_deletions.
-- bulls96_read_api holds no grant on any of them (20261105043700 REVOKEs owner_buffered_parity from
-- it explicitly), so a direct read fails 42501. This migration adds ONE SECURITY DEFINER reader
-- that evaluates the gate for one owner's hits. The Worker role never reads the gate tables
-- directly; no table privilege is granted.
--
--   sessions.buffered_search_gate_for(p_org_id, p_workspace_id, p_owner_user_id, <head tuple>, p_hits)
--     <head tuple> = p_cutover_epoch, p_head_epoch, p_head_version, p_head_digest, p_head_kind,
--                    p_head_pending, p_privacy_baseline_sha256 (exactly 20261105043700's encoding)
--     RETURNS TABLE(claim_token, source, session_id, drop_reason)
--   Role guard: refuses 42501 unless the caller is bulls96_read_api (SET ROLE or session user;
--   inside a definer function current_setting('role') still names the caller's role).
--   Tenant pins: refuses 42501 unless bulls96_read.org_id and bulls96_read.workspace_id are set
--   (transaction-local, bound by the read API from the verified identity) and equal p_org_id and
--   p_workspace_id. The owner is an argument, not a pin: in a shared workspace the hit's owner is
--   usually not the reader. The head tuple is the OWNER's R2 control head, fetched by the read API
--   for that owner (controlEpoch, privacyBaselineSha256 and the source-control head; no head =
--   epoch/version/digest/kind NULL and pending false).
--   Input: the head tuple in 20261105043700's shapes (epochs/digest/baseline ^[0-9a-f]{64}$,
--   version ^[1-9][0-9]{0,18}$ within bigint, kind selection|reset, all-or-none head fields,
--   pending false without a head); p_owner_user_id non-empty; p_hits a jsonb array of 1..500
--   objects with exactly the keys claim_token (lowercase uuid text), source and session_id
--   (non-empty strings). Anything else returns NO rows (never an error): the read API treats a
--   missing row as admission_missing, so malformed input drops every hit.
--
--   One row per distinct input hit whose claim (sessions.ingest_claim_attempts by claim token)
--   belongs to exactly (pinned org, pinned workspace, p_owner_user_id); a hit borrowing another
--   user's or tenant's claim gets no row. drop_reason NULL = visible; otherwise, first match:
--     admission_epoch_mismatch   ADMISSION WITNESS (20261105043700 rule 3): the ingest_lazy_policy
--                                singleton's control_epoch, taken only when its
--                                source_identity_sha256 equals the claim's (non-null) one, is
--                                missing or is not p_cutover_epoch;
--     admission_version_mismatch the claim's source_control_version is NULL or is not the head
--                                version (0 without a head), or a lazy ACK decision of the claim
--                                carries a different source_control_version;
--     admission_session_mismatch the claim does not cover the hit: its immutable original
--                                records (ingest_claim_attempts.records) are retired or not an
--                                array, or hold no record of exactly (source, session_id);
--     gate_closed                20261105043700 gate: the head is pending or its epoch is not the
--                                cutover epoch; the privacy authority is fenced; or the
--                                ingest_lazy_policy singleton is expired or its control_epoch /
--                                privacy_baseline_sha256 is not the passed cutover epoch / baseline;
--     parity_missing             no sessions.owner_buffered_parity row for (org, ws, owner);
--     parity_stale               G2 PARITY (rule 4), exactly as owner_buffered_read_scope: the
--                                row's cutover epoch, baseline, head tuple (or no_head),
--                                target_generation, deletion/reset digests and target lineage must
--                                equal the passed head and sessions.owner_buffered_parity_expected
--                                recomputed now for the owner, with receipts_ok and a PASS
--                                generation;
--     deleted                    RULE 5(a)/(b): (org, ws, owner, source, session) is in
--                                upload_deletions or ingest_frozen_source_deletions under ANY
--                                origin;
--     batch_not_buffered         20261105043700 stg batch predicate: the claim's ingest_batches row
--                                (org, ws, origin, batch) is missing, or is not the owner's, or
--                                carries another digest, or is not state committed with
--                                publication_state buffered and a non-NULL committed_at;
--     reset                      RULE 5(c): the claim's reset_epoch is NULL or is not the owner's
--                                upload_resets count; the batch's committed_at is NULL; a reset of
--                                the owner has reset_at NULL or >= committed_at; or the hit's epochs
--                                entry carries a reset_epoch other than that count;
--     superseded                 LATEST ADMISSION (043700 judged.live, ingest_attempt_fenced): the
--                                claim's epochs (jsonb array) hold no entry for (source,
--                                session_id), or ambiguous entries (min <> max epoch or reset
--                                epoch), or the entry's epoch is not the session's current
--                                ingest_fence_versions epoch on the claim's origin (0 without a
--                                row). A deletion, disable or reset bumps that fence after
--                                admission, so the claim is no longer the record's latest
--                                admission. Not covered by the parity digests.
--
-- 043700 PARITY: every admission condition 20261105043700 applies at read time (owner_buffered_read_scope,
-- then the keep predicate shared by buffered_sessions_for and buffered_session_stages_for), and
-- where this gate checks it (line numbers in this file) or why it does not.
--   Scope
--     caller is bulls96_read_api ................................ L204 (42501)
--     org/workspace = transaction pins .......................... L207 (42501)
--     user = the pinned reader (043700) ......................... DIFFERENT BY DESIGN: owner is a
--         parameter (L342: claim must be (pins, owner)); see THREAT MODEL below
--     head/baseline/epoch shapes ................................ L212-L222; hit shapes L242
--     privacy authority not fenced (043700: 55006) .............. L256 (gate_closed)
--     head not pending, head epoch = cutover ................... L255 (gate_closed)
--     policy unexpired, control_epoch/baseline = passed ......... L260-L262 (gate_closed)
--     G2 parity row exists ...................................... L268 (parity_missing)
--     parity tuple, receipts_ok, target generation, digests ..... L273-L284 (parity_stale)
--     required admission epoch (policy epoch via identity) ..... L305, CASE L358
--     required admission version (head version, 0 no head) ..... L306, CASE L360
--   Stage (stg)
--     claim identity (org, ws, user) ............................ L342
--     batch row: owner, digest, committed, buffered, accepted ... L343, L323 (batch_not_buffered)
--     records is an array, holds (source, session), not retired  L312 (admission_session_mismatch)
--     epochs is an array ........................................ L349 (non-array = no entry: superseded)
--     claim reset_epoch NOT NULL and = upload_resets count ...... L292, L328 (reset)
--     RULE 5(c): no reset at or after committed_at ............. L332 (reset)
--     claim version/identity set, identity = policy identity .... L305, L306
--     lazy ACK decisions carry the claim's version .............. L309 (admission_version_mismatch)
--     outbox buffered with this active stage; stage acknowledged,
--       not redacted; outbox/stage/claim identity rows equal;
--       group_ordinals an array ................................. NOT MIRRORED: per-stage. The hit
--         names a claim, not a stage; these normally move together with a deletion, reset or fence
--         bump the gate does check, but that is not proven here (residual, see below)
--     GC ancestor chain carries the same claim (bound 64) ....... NOT MIRRORED: per-stage (residual)
--   Record (bad_stage, recn, ev, judged.live, kept)
--     epochs entry present and unambiguous ..................... L338 (superseded)
--     entry epoch = current fence version (claim origin) ........ L352, L338 (superseded)
--     entry reset_epoch = upload_resets count ................... L333 (reset)
--     upload_deletions same origin / ANY origin; frozen ANY ..... L315 (deleted; ANY covers same)
--     record source/session non-NULL ............................ L242 (non-empty hit strings)
--     group ordinals well formed, no duplicate ordinals;
--       group is one session; >= 1 live (lazy) or all live ...... NOT MIRRORED: per-group/ordinal; the
--         hit carries no ordinal (residual)
--     lazy ACK branch: native slot current owner/seat/digest ... NOT MIRRORED: per-ordinal (residual)
--   List only
--     published (ingest_publication_heads) exclusion ........... INAPPLICABLE: list dedupe so the
--         canonical copy wins; buffered_session_stages_for (detail) does not apply it, so it is not
--         an admission condition
-- Residual divergence: the NOT MIRRORED rows are 043700 conditions on the stage/ordinal serving
-- the record. A hit whose claim passes every row above but whose stage is not (or no longer) the
-- active, acknowledged one would pass here and be hidden by 043700. Closing it needs the hit's
-- stage (ingest_stage_intents by claim_token); it is out of this follow-up's scope.
--
-- THREAT MODEL. p_owner_user_id is a parameter, not a pin: the reader of a shared workspace sees
-- hits owned by other users, and the gate judges each against its owner. Only bulls96_read_api may
-- call (L204), only inside the verified (org, workspace) pins (L207); a different tenant is
-- 42501. Within that tenant a caller may name ANY user. What it can learn per call: for each
-- supplied (claim_token, source, session_id), whether that claim token is a claim of (org, ws,
-- owner) (a row or no row), and, only for such a claim, one drop reason, which can reveal the
-- owner's gate/parity state, whether the session is deleted or reset, the batch's state and
-- whether a later fence superseded it. It returns no content, no timestamps and no other claim.
-- The claim token bounds this: rows exist only for a token that IS a claim of that owner in the
-- pinned tenant (L342); tokens are random v4 UUIDs, stored only in the ledger and in the
-- owner's transcript_events rows, so a caller without a token learns nothing (no row, no reason)
-- and cannot enumerate users, sessions or deletions. The Worker only passes tokens and owners
-- taken from transcript_events rows the reader's own tenant-scoped search returned, and reports
-- only aggregate drop counts to logs, never per-hit reasons to the client.
--
-- Locks. STABLE, read-only, no row lock, no advisory lock, no write: AccessShareLock only, inside
-- the read API's READ ONLY transactions under the read role's 2 s statement_timeout. The parity
-- digests are recomputed once per call (one owner); the input is capped at 500 hits per call.
--
-- Schema version. Applying this migration changes the migration-set generation()
-- (src/hoopdb/migrate.ts); see 20261105043700's note on the buffered Worker's captured generation.
--
-- Roles. Never creates or alters a role. Grants no table privilege and no write. EXECUTE to
-- bulls96_read_api only (src/hoopdb/sessions-function-acl.json +1 entry, lane
-- scripts/sessions-function-boundary/lanes/buffered-search-gate.ts). The function runs as its
-- owner, which must also own owner_buffered_parity_expected (owner-only EXECUTE).
--
-- Rollout. Must be applied (with Khaliq's explicit apply OK) BEFORE READ_API_BUFFERED_SEARCH_GATE
-- is set to on; without it every gate call fails and every buffered hit is dropped (fail closed).
-- Code only; nothing here is applied, deployed, enabled or granted by this repository. Down path:
-- supabase/rollbacks/20261105043860_buffered_search_gate.down.sql.
DO $buffered_search_gate_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relname IN('ingest_claim_attempts','ingest_lazy_policy','ingest_lazy_ack_decisions',
    'owner_buffered_parity','upload_resets','upload_deletions','ingest_frozen_source_deletions',
    'ingest_privacy_authority','ingest_control_receipts','ingest_batches','ingest_fence_versions')
    AND c.relowner<>me)
  OR (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relkind='r' AND c.relname IN('ingest_claim_attempts','ingest_lazy_policy',
    'ingest_lazy_ack_decisions','owner_buffered_parity','upload_resets','upload_deletions',
    'ingest_frozen_source_deletions','ingest_privacy_authority','ingest_control_receipts','ingest_batches',
    'ingest_fence_versions'))<>11
  OR NOT EXISTS(SELECT 1 FROM pg_proc p
   WHERE p.oid=to_regprocedure('sessions.owner_buffered_parity_expected(text, text, text, text, bigint, text, text)')
     AND p.proowner=me)
  OR to_regprocedure('sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)') IS NOT NULL
  OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_read_api') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search gate: requires original creator, the buffered ledger, 20261105043700 and bulls96_read_api';END IF;
END $buffered_search_gate_creator$;

CREATE FUNCTION sessions.buffered_search_gate_for(
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
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text) IS DISTINCT FROM 'bulls96_read_api' THEN
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
GRANT EXECUTE ON FUNCTION sessions.buffered_search_gate_for(text, text, text, text, text, text, text, text, boolean, text, jsonb)
  TO bulls96_read_api;

-- As in 20261105043700: strip EXECUTE from every other grantee present at creation (default
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
