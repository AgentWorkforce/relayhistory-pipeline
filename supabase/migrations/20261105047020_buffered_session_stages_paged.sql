-- SLOT 047020: paged exact-session buffered detail, default off in the Worker.
-- A NEW RPC; 044090, 047010 and every landed migration remain unchanged.
-- sessions.buffered_session_stages_for_target_paged returns ONE envelope row per call:
--   stages      jsonb array of the stage rows buffered_session_stages_for_target returns for the
--               claims of this page (same column names as keys, same row order),
--   cursor      the keyset position of the last INSPECTED candidate (NULL when exhausted),
--   has_more    true when a further candidate existed after this page,
--   snapshot_at clock_timestamp() of the first page, carried in the cursor to later pages.
-- Work bound: the FIRST CTE after ctl, `page`, is MATERIALIZED and selects at most
-- p_page_claims+1 candidate claims (p_page_claims hard-clamped to 1..4) from the session's side
-- rows (ingest_claim_sessions_session_idx), keyset ordered by (batches.committed_at,
-- batch_id COLLATE "C", stage_key COLLATE "C", stage_token::text COLLATE "C") strictly after the
-- cursor; no OFFSET. Only the first p_page_claims candidates are expanded (records/epochs jsonb);
-- the extra one only decides has_more. The cursor advances past every inspected candidate, even
-- when the privacy predicates hide all of its records, so filtered pages cannot loop.
-- Later pages admit only claims whose batch committed_at <= snapshot_at.
-- Scope, 22023, control-head gate and the v2 claim-sessions reader gate are 047010's. With the
-- v2 gate off the first page returns 047010's unchanged fallback result in one envelope.
-- READ_API_OWNER_BUFFERED_STAGES_PAGED=on (with READ_API_OWNER_BUFFERED_EXACT_SESSION=on) selects
-- this RPC. Code only; migration apply and flag activation are separate production operations.
SET LOCAL lock_timeout='5s';
DO $stages_paged_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF to_regprocedure('sessions.buffered_session_stages_for_target_paged(text,text,text,text,text,text,text,text,boolean,text,jsonb,integer)') IS NOT NULL
    OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
               WHERE n.nspname='sessions' AND p.proname='buffered_session_stages_for_target_paged')
    OR NOT EXISTS(SELECT 1 FROM pg_proc p
      WHERE p.oid=to_regprocedure('sessions.buffered_session_stages_for_target(text,text,text,text,text,text,text,text,boolean,text)')
        AND p.proowner=me AND p.prosecdef AND p.provolatile='s'
        AND p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql')
        AND p.proconfig=ARRAY['search_path=pg_catalog, sessions, pg_temp']
        AND encode(sha256(convert_to(p.prosrc,'UTF8')),'hex')='0e81869a672e252fadb4bda4c7aa80ff74e1965978a9782810f801ef4490389a')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='paged session stages: exact owned buffered_session_stages_for_target reader and no prior paged RPC required';END IF;
END $stages_paged_creator$;

CREATE FUNCTION sessions.buffered_session_stages_for_target_paged(
  p_workspace text, p_source text, p_session_id text,
  p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text, p_head_kind text,
  p_head_pending boolean, p_privacy_baseline_sha256 text,
  p_cursor jsonb DEFAULT NULL, p_page_claims integer DEFAULT 2
)
RETURNS TABLE(stages jsonb, cursor jsonb, has_more boolean, snapshot_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $buffered_stages$
#variable_conflict use_column
DECLARE
  v_org text;
  v_ws text;
  v_user text;
  v_open boolean;
  v_adm_epoch text;
  v_adm_version bigint;
  v_source text := p_source;
  v_session text := p_session_id;
  v_v2 boolean;
  -- Hard clamp, never an error: NULL is the default 2, anything outside 1..4 is clamped.
  v_page integer := least(greatest(coalesce(p_page_claims, 2), 1), 4);
  v_snapshot timestamptz;
  v_c_at timestamptz;
  v_c_batch text;
  v_c_key text;
  v_c_token text;
BEGIN
  SELECT sc.org_id, sc.workspace_id, sc.user_id, sc.gate_open, sc.admission_epoch, sc.admission_version
    INTO STRICT v_org, v_ws, v_user, v_open, v_adm_epoch, v_adm_version
    FROM sessions.owner_buffered_read_scope(p_workspace, p_cutover_epoch, p_head_epoch, p_head_version,
                                              p_head_digest, p_head_kind, p_head_pending,
                                              p_privacy_baseline_sha256) AS sc;
  IF v_source IS NULL OR char_length(v_source) NOT BETWEEN 1 AND 64
     OR v_session IS NULL OR char_length(v_session) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'owner buffered read: source and session are required';
  END IF;
  -- The cursor is exactly this function's own output: {v:1, snapshot_at, accepted_at, batch_id,
  -- stage_key, stage_token}, all strings except v. Anything else, or a snapshot in the future, is 22023.
  IF p_cursor IS NULL THEN
    v_snapshot := clock_timestamp();
  ELSE
    IF jsonb_typeof(p_cursor) IS DISTINCT FROM 'object'
       OR p_cursor -> 'v' IS DISTINCT FROM '1'::jsonb
       OR (SELECT count(*) FROM jsonb_object_keys(p_cursor)) <> 6
       OR jsonb_typeof(p_cursor -> 'snapshot_at') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_cursor -> 'accepted_at') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_cursor -> 'batch_id') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_cursor -> 'stage_key') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_cursor -> 'stage_token') IS DISTINCT FROM 'string'
       OR p_cursor ->> 'stage_token' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'paged session stages: malformed cursor';
    END IF;
    BEGIN
      v_snapshot := (p_cursor ->> 'snapshot_at')::timestamptz;
      v_c_at := (p_cursor ->> 'accepted_at')::timestamptz;
    EXCEPTION WHEN data_exception THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'paged session stages: malformed cursor';
    END;
    v_c_batch := p_cursor ->> 'batch_id';
    v_c_key := p_cursor ->> 'stage_key';
    v_c_token := p_cursor ->> 'stage_token';
    IF v_snapshot IS NULL OR v_c_at IS NULL OR v_snapshot > clock_timestamp() OR v_c_at > v_snapshot THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'paged session stages: malformed cursor';
    END IF;
  END IF;
  IF NOT v_open THEN
    -- 047010 returns no row when the gate is closed. A later page cannot be served from a closed
    -- gate: refuse, so the caller never mistakes a truncated traversal for a complete one.
    IF p_cursor IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'paged session stages: gate changed during traversal';
    END IF;
    RETURN QUERY SELECT '[]'::jsonb, NULL::jsonb, false, v_snapshot;
    RETURN;
  END IF;
  -- 044090: the claim-sessions reader gate, a plain read of the policy row (no lock).
  SELECT coalesce(bool_or(cp.read_enabled AND cp.write_enabled AND cp.backfill_complete_at IS NOT NULL), false)
    INTO v_v2
    FROM sessions.ingest_claim_sessions_policy AS cp
   WHERE cp.singleton;
  IF NOT v_v2 THEN
    -- Gate off: 047010's unchanged fallback (044040's query), in one envelope on the first page.
    -- The function runs in the caller's snapshot (STABLE), so it sees this gate and scope.
    IF p_cursor IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'paged session stages: gate changed during traversal';
    END IF;
    RETURN QUERY
    SELECT coalesce(jsonb_agg(to_jsonb(f) - 'ordinality' ORDER BY f.ordinality), '[]'::jsonb), NULL::jsonb, false, v_snapshot
      FROM ROWS FROM (sessions.buffered_session_stages_for_target(p_workspace, p_source, p_session_id,
                        p_cutover_epoch, p_head_epoch, p_head_version, p_head_digest, p_head_kind,
                        p_head_pending, p_privacy_baseline_sha256)) WITH ORDINALITY AS f;
    RETURN;
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (047010's v2 query; stg is driven from `page` instead of the side rows,
  -- every other CTE is byte-identical to 047010's)
  WITH ctl AS MATERIALIZED (
    SELECT (SELECT count(*) FROM sessions.upload_resets AS ur
             WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = v_user) AS resets,
           EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS lazy,
           (SELECT ck.control_epoch FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_epoch,
           (SELECT ck.source_identity_sha256 FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_identity,
           (SELECT lp.source_identity_sha256 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_identity,
           (SELECT lp.control_epoch FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_epoch
  ), page AS MATERIALIZED (
    -- 047020 WORK BOUND. The candidate claims of (v_source, v_session): the session's side rows
    -- (ingest_claim_sessions_session_idx equality on org/workspace/user/source/session), each
    -- joined to its claim's active stage of a buffered outbox row and that batch's commit. No
    -- jsonb is read here. Keyset = (committed_at, batch_id "C", stage_key "C", stage_token "C"),
    -- the output order of 047010 plus the unique stage token; stage_key is UNIQUE, so the
    -- concatenated pages are in exactly the unpaged order. side-row accepted_at is a nullable
    -- hint; the batch's committed_at is the value 047010 returns as accepted_at, so it is the key.
    -- At most v_page + 1 rows; only n <= v_page are expanded below, row v_page + 1 only proves
    -- has_more. A candidate the privacy predicates later hide still counts and still moves the cursor.
    SELECT c.claim_token, c.stage_token, c.accepted_at, c.batch_id, c.stage_key,
           row_number() OVER (ORDER BY c.accepted_at, c.batch_id COLLATE "C", c.stage_key COLLATE "C",
                                       c.stage_token::text COLLATE "C") AS n
      FROM (SELECT cs.claim_token, s.stage_token, b.committed_at AS accepted_at, s.batch_id, s.stage_key
              FROM sessions.ingest_claim_sessions AS cs
              JOIN sessions.ingest_stage_intents AS s ON s.claim_token = cs.claim_token
              JOIN sessions.ingest_buffer_outbox AS o ON o.active_stage_token = s.stage_token
              JOIN sessions.ingest_batches AS b
                ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
             WHERE cs.org_id = v_org AND cs.workspace_id = v_ws AND cs.user_id = v_user
               AND cs.source = v_source AND cs.session_id = v_session
               AND o.org_id = v_org AND o.workspace_id = v_ws AND o.user_id = v_user AND o.state = 'buffered'
               AND b.committed_at IS NOT NULL AND b.committed_at <= v_snapshot
               AND (v_c_at IS NULL
                    OR ROW(b.committed_at, s.batch_id COLLATE "C", s.stage_key COLLATE "C", s.stage_token::text COLLATE "C")
                       > ROW(v_c_at, v_c_batch COLLATE "C", v_c_key COLLATE "C", v_c_token COLLATE "C"))
             ORDER BY b.committed_at, s.batch_id COLLATE "C", s.stage_key COLLATE "C", s.stage_token::text COLLATE "C"
             LIMIT v_page + 1) AS c
  ), stg AS MATERIALIZED (
    -- 047010's stg, driven from the first v_page candidates of `page` (each is one claim and its
    -- active stage) instead of every side row of the session; every 047010 predicate is kept,
    -- including the containment recheck and the side-row identity. ADMISSION WITNESS, GC ORIGINAL
    -- CLAIM and RULE 5(c) as documented in 044090/047010.
    SELECT s.stage_token, s.claim_token, s.origin_id, s.batch_id, s.stage_key, s.stage_sha256, s.stage_bytes,
           s.group_ordinals, s.reset_epoch, a.records, a.epochs, a.reset_epoch AS a_reset,
           b.committed_at AS accepted_at, a.source_control_version AS adm_version, ctl.pol_epoch AS adm_epoch
      FROM page AS pg
      JOIN sessions.ingest_claim_sessions AS cs
        ON cs.claim_token = pg.claim_token AND cs.source = v_source AND cs.session_id = v_session
      JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = cs.claim_token
      JOIN sessions.ingest_stage_intents AS s ON a.claim_token = s.claim_token AND s.stage_token = pg.stage_token
      JOIN sessions.ingest_buffer_outbox AS o ON s.stage_token = o.active_stage_token
      JOIN sessions.ingest_batches AS b
        ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
     CROSS JOIN ctl
     WHERE pg.n <= v_page
       AND cs.org_id = v_org AND cs.workspace_id = v_ws AND cs.user_id = v_user
       AND cs.source = v_source AND cs.session_id = v_session
       AND jsonb_typeof(a.records) = 'array'
       AND coalesce(a.records @> jsonb_build_array(jsonb_build_object('source', v_source, 'session_id', v_session)), false)
       AND o.org_id = v_org AND o.workspace_id = v_ws AND o.user_id = v_user AND o.state = 'buffered'
       AND s.receipt_state = 'acknowledged' AND s.cleanup_state <> 'redacted'
       AND ROW(s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest)
           = ROW(o.org_id, o.workspace_id, o.user_id, o.origin_id, o.batch_id, o.digest)
       AND ROW(a.org_id, a.workspace_id, a.user_id, a.origin_id, a.batch_id, a.digest, a.reset_epoch)
           IS NOT DISTINCT FROM ROW(s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest, s.reset_epoch)
       AND b.user_id = v_user AND b.digest = o.digest AND b.state = 'committed' AND b.publication_state = 'buffered'
       AND b.committed_at IS NOT NULL
       AND jsonb_typeof(a.records) = 'array' AND jsonb_typeof(a.epochs) = 'array'
       AND jsonb_typeof(s.group_ordinals) = 'array'
       AND ctl.resets = s.reset_epoch
       AND a.reset_epoch IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM sessions.upload_resets AS ur
                        WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = v_user
                          AND (ur.reset_at IS NULL OR ur.reset_at >= b.committed_at))
       AND a.source_control_version IS NOT NULL AND a.source_identity_sha256 IS NOT NULL
       AND ctl.pol_identity IS NOT NULL AND ctl.pol_epoch IS NOT NULL
       AND a.source_identity_sha256 = ctl.pol_identity
       AND ctl.pol_epoch = v_adm_epoch AND a.source_control_version = v_adm_version
       AND NOT EXISTS (
         WITH RECURSIVE anc(claim_token, parent_stage_token, depth) AS (
           SELECT p.claim_token, p.parent_stage_token, 1
             FROM sessions.ingest_stage_intents AS p WHERE p.stage_token = s.parent_stage_token
           UNION ALL
           SELECT p.claim_token, p.parent_stage_token, an.depth + 1
             FROM anc AS an JOIN sessions.ingest_stage_intents AS p ON p.stage_token = an.parent_stage_token
            WHERE an.depth < 64)
         SELECT 1 FROM anc
          WHERE anc.claim_token IS DISTINCT FROM s.claim_token
             OR (anc.depth >= 64 AND anc.parent_stage_token IS NOT NULL))
  ), grp AS (
    -- One row per (stage, object group index, original claim ordinal).
    SELECT st.stage_token, (g.idx - 1)::integer AS group_index, (e.value::text)::integer AS ord
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_array_elements(st.group_ordinals) WITH ORDINALITY AS g(value, idx)
     CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(g.value) = 'array' THEN g.value ELSE '[null]'::jsonb END) AS e(value)
     WHERE jsonb_typeof(e.value) = 'number' AND e.value::text ~ '^(0|[1-9][0-9]{0,2})$'
  ), bad_stage AS MATERIALIZED (
    -- gc_kept refuses malformed groups and duplicate ordinals: such a stage is never shown.
    -- 044040: MATERIALIZED, so it is computed once. Inlined (043700), judged's NOT EXISTS re-ran it for every
    -- (stage, ordinal) row: O(stages^2 x groups), the list's dominant cost on a large backlog.
    SELECT st.stage_token
      FROM stg AS st
     WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(st.group_ordinals) AS g(value)
                    WHERE jsonb_typeof(g.value) IS DISTINCT FROM 'array' OR jsonb_array_length(g.value) = 0
                       OR EXISTS (SELECT 1 FROM jsonb_array_elements(g.value) AS e(value)
                                   WHERE jsonb_typeof(e.value) IS DISTINCT FROM 'number'
                                      OR e.value::text !~ '^(0|[1-9][0-9]{0,2})$'))
        -- 044040: one hashed pass over grp instead of a correlated group-by per stage (was O(stages x groups)).
        OR st.stage_token IN (SELECT g2.stage_token FROM grp AS g2 GROUP BY g2.stage_token, g2.ord HAVING count(*) > 1)
  ), rec AS (
    SELECT st.stage_token, r.ord, r.source, r.session_id, r.record_id, r.revision, r.digest
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_to_recordset(st.records)
           AS r(ord integer, source text, session_id text, record_id text, revision bigint, digest text)
  ), recn AS (
    -- A duplicate original ordinal makes every record of that ordinal unusable (gc_kept 55000).
    -- 044040: a window count over (stage, ord) instead of a correlated count per record (was O(records^2)).
    -- A NULL ord equals nothing, so the old count was 0 for it: excluded here too.
    SELECT w.stage_token, w.ord, w.source, w.session_id, w.record_id, w.revision, w.digest
      FROM (SELECT rc.*, count(*) OVER (PARTITION BY rc.stage_token, rc.ord) AS n FROM rec AS rc) AS w
     WHERE w.ord IS NOT NULL AND w.n = 1
  ), ev AS (
    SELECT st.stage_token, x.source, x.session_id, min(x.epoch) AS epoch, max(x.epoch) AS epoch_max,
           min(x.reset_epoch) AS reset_epoch, max(x.reset_epoch) AS reset_max, count(*) AS n
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_to_recordset(st.epochs) AS x(source text, session_id text, epoch bigint, reset_epoch bigint)
     GROUP BY st.stage_token, x.source, x.session_id
  ), target_groups AS MATERIALIZED (
    -- Resolve WHOLE groups before any per-record privacy work. The frozen bad_stage CTE still
    -- validates every group/ordinal of the stage, including unrelated groups. Missing or ambiguous
    -- records and mixed source/session groups cannot be targets (IS DISTINCT FROM rejects NULL).
    SELECT gr.stage_token, gr.group_index
      FROM grp AS gr
      LEFT JOIN recn AS rc ON rc.stage_token = gr.stage_token AND rc.ord = gr.ord
     GROUP BY gr.stage_token, gr.group_index
    HAVING count(*) FILTER (WHERE rc.source IS DISTINCT FROM v_source OR rc.session_id IS DISTINCT FROM v_session) = 0
  ), judged AS (
    SELECT gr.stage_token, gr.group_index, gr.ord, rc.source, rc.session_id,
           -- ingest_attempt_fenced: an epoch entry exists, the fence version, the per-session
           -- reset epoch and upload_deletions (a missing or ambiguous entry is not live).
           coalesce(rc.session_id IS NOT NULL AND ev.n IS NOT NULL
            AND ev.epoch = ev.epoch_max AND ev.reset_epoch = ev.reset_max
            AND coalesce(fv.epoch, 0) = ev.epoch AND ctl.resets = ev.reset_epoch
            AND NOT EXISTS (SELECT 1 FROM sessions.upload_deletions AS d
                             WHERE d.org_id = v_org AND d.workspace_id = v_ws AND d.user_id = v_user
                               AND d.origin_id = st.origin_id AND d.source = rc.source AND d.session_id = rc.session_id)
            -- RULE 5(a)/(b): a frozen source deletion or an upload_deletions entry of this
            -- (user, source, session) under ANY origin hides the record (stricter than the GC's
            -- exact-origin check above).
            AND rc.source IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM sessions.ingest_frozen_source_deletions AS fd
                             WHERE fd.org_id = v_org AND fd.workspace_id = v_ws AND fd.user_id = v_user
                               AND fd.source = rc.source AND fd.session_id = rc.session_id)
            AND NOT EXISTS (SELECT 1 FROM sessions.upload_deletions AS d2
                             WHERE d2.org_id = v_org AND d2.workspace_id = v_ws AND d2.user_id = v_user
                               AND d2.source = rc.source AND d2.session_id = rc.session_id)
            -- ingest_buffer_gc_ordinals ACK branch (lazy-current policy present): the native
            -- slot's current owner is the caller, with the same seat/digest/revision/session.
            AND (NOT ctl.lazy OR EXISTS (
              SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS ad
                JOIN sessions.ingest_lazy_native_slots AS ns ON ns.key_sha256 = ad.native_key_sha256
               WHERE ad.claim_token = st.claim_token AND ad.ord = gr.ord AND ad.body_accepted
                 AND ad.source_control_version = st.adm_version
                 AND rc.record_id IS NOT NULL AND rc.source IS NOT NULL AND rc.revision > 0
                 AND rc.digest ~ '^[0-9a-f]{64}$' AND ev.n = 1 AND ev.epoch >= 0 AND ev.reset_epoch = st.a_reset
                 AND ad.candidate_key_sha256 = encode(sha256(convert_to(jsonb_build_array('buffer-first-ack-seat-v1',
                       ctl.clock_epoch, ctl.clock_identity, v_org, v_ws, v_user, st.origin_id, rc.record_id,
                       rc.revision::text, st.a_reset::text, rc.source, rc.session_id, ev.epoch::text)::text, 'UTF8')), 'hex')
                 AND ad.candidate_digest = rc.digest AND ad.reset_epoch = st.a_reset AND ad.fence_epoch = ev.epoch
                 AND ns.org_id = v_org AND ns.workspace_id = v_ws AND ns.origin_id = st.origin_id
                 AND ns.record_id = rc.record_id AND ns.current_owner = v_user
                 AND ns.current_seat_key = ad.seat_key_sha256 AND ns.current_digest = ad.candidate_digest
                 AND ns.current_revision = rc.revision
                 AND ns.current_source IS NOT DISTINCT FROM rc.source
                 AND ns.current_session IS NOT DISTINCT FROM rc.session_id)), false) AS live
      FROM grp AS gr
      JOIN target_groups AS tg ON tg.stage_token = gr.stage_token AND tg.group_index = gr.group_index
      JOIN stg AS st ON st.stage_token = gr.stage_token
     CROSS JOIN ctl
      LEFT JOIN recn AS rc ON rc.stage_token = gr.stage_token AND rc.ord = gr.ord
      LEFT JOIN ev ON ev.stage_token = gr.stage_token AND ev.source = rc.source AND ev.session_id = rc.session_id
      LEFT JOIN sessions.ingest_fence_versions AS fv
        ON fv.org_id = v_org AND fv.workspace_id = v_ws AND fv.user_id = v_user AND fv.origin_id = st.origin_id
       AND fv.source = rc.source AND fv.session_id = rc.session_id
     WHERE NOT EXISTS (SELECT 1 FROM bad_stage AS bs WHERE bs.stage_token = gr.stage_token)
  ), kept AS (
    -- A group is shown only when every ordinal resolves to a record of ONE non-null session.
    -- Outside lazy mode the whole group must be live (gc_kept keeps or drops whole groups); in
    -- lazy mode each live ordinal counts (gc_ordinals keeps per ordinal).
    SELECT j.stage_token, j.group_index, min(j.source) AS source, min(j.session_id) AS session_id,
           array_agg(j.ord ORDER BY j.ord) FILTER (WHERE j.live) AS ords
      FROM judged AS j CROSS JOIN ctl
     GROUP BY j.stage_token, j.group_index, ctl.lazy
    HAVING count(j.session_id) = count(*) AND count(j.source) = count(*)
       AND count(DISTINCT j.source) = 1 AND count(DISTINCT j.session_id) = 1
       AND count(*) FILTER (WHERE j.live) > 0
       AND (ctl.lazy OR count(*) FILTER (WHERE j.live) = count(*))
  ), out AS (
    SELECT st.batch_id, st.stage_key, st.stage_sha256, st.stage_bytes,
           array_agg(k.group_index ORDER BY k.group_index) AS group_indexes, st.group_ordinals,
           (SELECT array_agg(o ORDER BY o) FROM kept AS k2 CROSS JOIN LATERAL unnest(k2.ords) AS o
             WHERE k2.stage_token = st.stage_token AND k2.source = v_source AND k2.session_id = v_session) AS kept_ordinals,
           st.reset_epoch, st.accepted_at, v_adm_epoch AS admission_epoch, v_adm_version::text AS admission_version
      FROM kept AS k
      JOIN stg AS st ON st.stage_token = k.stage_token
     WHERE k.source = v_source AND k.session_id = v_session
     GROUP BY st.stage_token, st.batch_id, st.stage_key, st.stage_sha256, st.stage_bytes, st.group_ordinals,
              st.reset_epoch, st.accepted_at
  ), more AS (
    SELECT count(*) > v_page AS has_more FROM page
  )
  -- One envelope: 047010's rows in 047010's order; the cursor is the last INSPECTED candidate
  -- (n = v_page), independent of what the privacy predicates kept; NULL when nothing follows.
  SELECT coalesce((SELECT jsonb_agg(to_jsonb(ot) ORDER BY ot.accepted_at, ot.batch_id COLLATE "C", ot.stage_key COLLATE "C")
                     FROM out AS ot), '[]'::jsonb),
         CASE WHEN mr.has_more THEN
           (SELECT jsonb_build_object('v', 1, 'snapshot_at', v_snapshot, 'accepted_at', pg.accepted_at,
                                      'batch_id', pg.batch_id, 'stage_key', pg.stage_key, 'stage_token', pg.stage_token::text)
              FROM page AS pg WHERE pg.n = v_page)
         END,
         mr.has_more, v_snapshot
    FROM more AS mr;
END
$buffered_stages$;

REVOKE ALL ON FUNCTION sessions.buffered_session_stages_for_target_paged(text,text,text,text,text,text,text,text,boolean,text,jsonb,integer) FROM PUBLIC;
DO $stages_paged_acl$
DECLARE grantee text;
BEGIN
 FOR grantee IN SELECT r.rolname FROM pg_proc p
   CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
   JOIN pg_roles r ON r.oid=a.grantee
   WHERE p.oid='sessions.buffered_session_stages_for_target_paged(text,text,text,text,text,text,text,text,boolean,text,jsonb,integer)'::regprocedure AND a.grantee<>p.proowner
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION sessions.buffered_session_stages_for_target_paged(text,text,text,text,text,text,text,text,boolean,text,jsonb,integer) FROM %I',grantee);END LOOP;
END $stages_paged_acl$;
GRANT EXECUTE ON FUNCTION sessions.buffered_session_stages_for_target_paged(text,text,text,text,text,text,text,text,boolean,text,jsonb,integer) TO bulls96_read_api;
