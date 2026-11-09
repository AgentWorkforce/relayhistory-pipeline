-- Slot 20261105044040 assigned by the registry owner; 044040 follows 044030 (#386, ingest generation overlap),
-- which touches neither reader nor ingest_claim_attempts. Code only: applying this to any real database needs
-- Khaliq's explicit OK. Operator packet: docs/handoffs/buffered-stages-session-first-044040/operator-packet.md.
--
-- Why. sessions.buffered_session_stages_for (043700) reads ONE session, but its stg CTE is driven from the
-- user's whole outbox (every buffered batch, each joined to its stage, claim and batch, with the per-stage
-- ancestor, deletion and lazy-ACK probes) and only then filters on the claim's records. For a user with a
-- large backlog (167k buffered batches) one session detail costs the whole backlog. Two CTEs are also
-- quadratic: recn counts duplicates with a correlated count per record (O(records^2)) and bad_stage runs a
-- correlated GROUP BY per stage (O(stages x groups)).
--
-- Production evidence (read-only operator measurement, 2026-11-09): for the 167k-backlog owner, a minimal
-- owner-scoped candidate COUNT that walks outbox -> stage -> claim and applies the (source, session_id) JSON
-- filter to the claim's records was cancelled at the 2000 ms statement timeout, while the plain owner outbox
-- COUNT (167,684 rows) finished in 376 ms. The cost is the per-candidate stage/claim lookup plus the records
-- filter, not the outbox count and not a missing session predicate. So the detail's stg starts from the GIN
-- jsonb_path_ops containment on ingest_claim_attempts.records, never from the outbox, and keeps every
-- privacy, reset, owner and admission predicate verbatim.
--
-- What changes (both readers are replaced with CREATE OR REPLACE: same signature, return type, STABLE,
-- SECURITY DEFINER, search_path, owner and ACL; no GRANT/REVOKE here, CREATE OR REPLACE keeps the ACL):
--  1. ingest_claim_attempts_records_gin_idx, a jsonb_path_ops GIN on ingest_claim_attempts(records), under
--     the 044020 prebuild rule (below).
--  2. buffered_session_stages_for: stg is driven from the claims whose records contain (source, session_id)
--     (`jsonb_typeof(a.records) = 'array' AND a.records @> ...`, the GIN), then the claim's stages, the
--     outbox row that has one of them active, and the batch. Every other 043700 predicate is kept verbatim;
--     043700's `v_source IS NULL OR` branch was dead (v_source is never NULL past its 22023 check) and is
--     what kept a generic plan off any index. Same rows, same order (parity test against the 043700 body).
--  3. Both readers: recn uses count(*) OVER (PARTITION BY stage_token, ord) = 1 and bad_stage one hashed
--     `stage_token IN (... GROUP BY stage_token, ord HAVING count(*) > 1)`, MATERIALIZED (computed once;
--     inlined, judged's NOT EXISTS re-ran it per row). Same rows (a NULL ord counted 0 before and is
--     excluded explicitly now).
--  4. buffered_sessions_for (the list): (3), plus per_session reads each kept stage's bytes and batch time
--     back by primary key (043700 joined the unindexed stg CTE: a nested loop, O(stages^2)), and drops its
--     always-true session predicate and two unused variables. It stays driven from the user's outbox: it must
--     see every buffered session of the user, so its cost remains linear in the backlog (packet: numbers).
-- Helpers, outputs, error codes, ordering and the fence behaviour are unchanged.
--
-- The index, adopt or create, read from the catalog only (pg_class, pg_index, pg_get_indexdef), exactly as
-- 20261105044020:
--  * an index of that name in sessions, on ingest_claim_attempts, whose pg_get_indexdef is exactly the
--    expected definition and that is valid, ready and live: adopted, nothing is executed for it (production
--    after the operator's CREATE INDEX CONCURRENTLY);
--  * absent and the table EMPTY (fresh, dev, kind, CI): plain CREATE INDEX with the expected definition;
--  * absent and the table POPULATED: REFUSE (55000), unless the run opts into a blocking build
--    (HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true binds hoopdb.allow_blocking_index_build=on; default off). The
--    migrator's preflight refuses the same case first, read-only, with index_prebuild_required;
--  * anything else (INVALID or not ready, another definition or table, a non-index relation): RAISE, never
--    adopted, nothing is dropped.
-- Emptiness is an existence probe with row_security off, first without a lock (a refusal never queues behind
-- writers), then again under SHARE right before the build. The name is declared adoptable for this step only
-- (ADOPTABLE_INDEXES in src/hoopdb/migration-sentinels.ts), byte-identical to the definition below (a test
-- checks); the two replaced readers are declared there too, so an operator-built index is adopted while the
-- step's sentinel (the replaced bodies) still detects an unledgered apply.
DO $buffered_stages_records_gin$
DECLARE
 v_spec text[];
 v_tbl regclass;
 v_rel record;
 v_idx record;
 v_def text;
 v_rows boolean;
 v_allow boolean:=coalesce(current_setting('hoopdb.allow_blocking_index_build',true),'')='on';
 v_rls text:=current_setting('row_security');
BEGIN
 FOREACH v_spec SLICE 1 IN ARRAY ARRAY[
  ['ingest_claim_attempts_records_gin_idx','ingest_claim_attempts',
   'CREATE INDEX ingest_claim_attempts_records_gin_idx ON sessions.ingest_claim_attempts USING gin (records jsonb_path_ops)']
 ] LOOP
  v_tbl:=to_regclass(format('sessions.%I',v_spec[2]));
  IF v_tbl IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='42P01',
    MESSAGE=format('buffered stages session-first: table sessions.%s does not exist',v_spec[2]);
  END IF;
  SELECT c.oid,c.relkind INTO v_rel FROM pg_class c
   WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  IF NOT FOUND THEN
   -- Absent. Only an empty table, or an explicit opt-in, may take the blocking build.
   PERFORM set_config('row_security','off',true);
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
   IF v_rows AND NOT v_allow THEN
    RAISE EXCEPTION USING ERRCODE='55000',
     MESSAGE=format('buffered stages session-first: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
     HINT=format('prebuild it out of band, CONCURRENTLY, with the operator packet (docs/handoffs/buffered-stages-session-first-044040/operator-packet.md) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
   END IF;
   EXECUTE format('LOCK TABLE %s IN SHARE MODE',v_tbl);
   IF NOT v_allow THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
    IF v_rows THEN
     RAISE EXCEPTION USING ERRCODE='55000',
      MESSAGE=format('buffered stages session-first: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
      HINT=format('prebuild it out of band, CONCURRENTLY, with the operator packet (docs/handoffs/buffered-stages-session-first-044040/operator-packet.md) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
    END IF;
   END IF;
   PERFORM set_config('row_security',v_rls,true);
   EXECUTE v_spec[3];
   SELECT c.oid,c.relkind INTO STRICT v_rel FROM pg_class c
    WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  END IF;
  IF v_rel.relkind<>'i' THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('buffered stages session-first: sessions.%s exists but is not a plain index (relkind %s); reconcile it by hand, nothing was changed',v_spec[1],v_rel.relkind);
  END IF;
  SELECT i.indrelid,i.indisvalid,i.indisready,i.indislive INTO STRICT v_idx FROM pg_index i WHERE i.indexrelid=v_rel.oid;
  v_def:=pg_get_indexdef(v_rel.oid);
  IF v_idx.indrelid<>v_tbl OR v_def IS DISTINCT FROM v_spec[3] THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('buffered stages session-first: index sessions.%s exists with a different definition; reconcile it (expected: %s)',v_spec[1],v_spec[3]),
    DETAIL=format('found: %s',v_def);
  END IF;
  IF NOT (v_idx.indisvalid AND v_idx.indisready AND v_idx.indislive) THEN
   RAISE EXCEPTION USING ERRCODE='55000',
    MESSAGE=format('buffered stages session-first: index sessions.%s is INVALID or not ready (a failed CONCURRENTLY build); drop it with DROP INDEX CONCURRENTLY sessions.%s, rebuild it out of band or let this migration create it, and rerun',v_spec[1],v_spec[1]);
  END IF;
 END LOOP;
END
$buffered_stages_records_gin$;

CREATE OR REPLACE FUNCTION sessions.buffered_sessions_for(
  p_workspace text, p_cursor_ts timestamptz, p_cursor_session text, p_cursor_source text, p_limit integer,
  p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text, p_head_kind text,
  p_head_pending boolean, p_privacy_baseline_sha256 text
)
RETURNS TABLE(source text, session_id text, first_ts timestamptz, last_ts timestamptz,
              stage_count integer, bytes bigint, user_id text, admission_epoch text, admission_version text)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $buffered_sessions$
#variable_conflict use_column
DECLARE
  v_org text;
  v_ws text;
  v_user text;
  v_open boolean;
  v_adm_epoch text;
  v_adm_version bigint;
  v_limit integer := least(greatest(coalesce(p_limit, 50), 1), 100);
BEGIN
  SELECT sc.org_id, sc.workspace_id, sc.user_id, sc.gate_open, sc.admission_epoch, sc.admission_version
    INTO STRICT v_org, v_ws, v_user, v_open, v_adm_epoch, v_adm_version
    FROM sessions.owner_buffered_read_scope(p_workspace, p_cutover_epoch, p_head_epoch, p_head_version,
                                              p_head_digest, p_head_kind, p_head_pending,
                                              p_privacy_baseline_sha256) AS sc;
  IF (p_cursor_ts IS NULL) <> (p_cursor_session IS NULL) OR (p_cursor_ts IS NULL) <> (p_cursor_source IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'owner buffered read: cursor needs ts, session and source together';
  END IF;
  IF NOT v_open THEN
    RETURN;
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (identical in buffered_sessions_for and buffered_session_stages_for except
  -- stg's driving FROM/WHERE head and its comment, which 044040 makes session-first in the detail;
  -- src/session-ingest/__tests__/owner-buffered-read.pglite.test.ts asserts it)
  WITH ctl AS MATERIALIZED (
    SELECT (SELECT count(*) FROM sessions.upload_resets AS ur
             WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = v_user) AS resets,
           EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS lazy,
           (SELECT ck.control_epoch FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_epoch,
           (SELECT ck.source_identity_sha256 FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_identity,
           (SELECT lp.source_identity_sha256 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_identity,
           (SELECT lp.control_epoch FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_epoch
  ), stg AS MATERIALIZED (
    -- The active, acknowledged, unredacted stage of each buffered batch of this user. Outbox,
    -- stage, attempt and batch identities are equal; the stage reset_epoch equals the user's
    -- upload_resets count (gc_kept). The list reads every buffered batch of the user, driven from the outbox.
    -- ADMISSION WITNESS: the version is source_control_version on the stage's ORIGINAL claim
    -- attempt; the epoch is not stored on the claim and is derived from the immutable
    -- ingest_lazy_policy.control_epoch, valid only when the claim's source_identity_sha256 is
    -- the policy's. A NULL version, NULL identity, a mismatch or no policy hides the stage.
    -- The pair must equal the gate's required (v_adm_epoch, v_adm_version): no control of
    -- any kind since admission. GC ORIGINAL CLAIM: every ancestor of a GC replacement stage
    -- (parent_stage_token chain, bounded at 64) carries this same claim_token, so a
    -- replacement can never lend a newer claim's admission. RULE 5(c): the claim's admission
    -- reset epoch is set, and no upload_resets row of the user is at or after the batch's
    -- acceptance (committed_at); a reset at or after acceptance hides the stage even when the
    -- count check above still matches.
    SELECT s.stage_token, s.claim_token, s.origin_id, s.batch_id, s.stage_key, s.stage_sha256, s.stage_bytes,
           s.group_ordinals, s.reset_epoch, a.records, a.epochs, a.reset_epoch AS a_reset,
           b.committed_at AS accepted_at, a.source_control_version AS adm_version, ctl.pol_epoch AS adm_epoch
      FROM sessions.ingest_buffer_outbox AS o
      JOIN sessions.ingest_stage_intents AS s ON s.stage_token = o.active_stage_token
      JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = s.claim_token
      JOIN sessions.ingest_batches AS b
        ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
     CROSS JOIN ctl
     WHERE o.org_id = v_org AND o.workspace_id = v_ws AND o.user_id = v_user AND o.state = 'buffered'
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
  )
  -- keep-predicate:end
  , per_session AS (
    -- 044040: a kept stage is a stg row, so its bytes and acceptance are read back by primary key from the
    -- same stage and batch rows stg selected (identical values, one snapshot). 043700 joined the stg CTE,
    -- which has no index: the planner's small estimate for kept made that a nested loop, O(stages^2).
    SELECT k.source, k.session_id, min(bb.committed_at) AS first_ts, max(bb.committed_at) AS last_ts,
           count(*)::integer AS stage_count, sum(si.stage_bytes)::bigint AS bytes
      FROM (SELECT DISTINCT k0.stage_token, k0.source, k0.session_id FROM kept AS k0) AS k
      JOIN sessions.ingest_stage_intents AS si ON si.stage_token = k.stage_token
      JOIN sessions.ingest_batches AS bb
        ON bb.org_id = si.org_id AND bb.workspace_id = si.workspace_id AND bb.origin_id = si.origin_id
       AND bb.batch_id = si.batch_id
     WHERE NOT EXISTS (
       SELECT 1 FROM sessions.ingest_publication_heads AS ph
        WHERE ph.org_id = v_org AND ph.workspace_id = v_ws AND ph.user_id = v_user
          AND ph.source = k.source AND ph.session_id = k.session_id
          AND (ph.state = 'live' OR ph.current_key IS NOT NULL))
     GROUP BY k.source, k.session_id
  )
  SELECT ps.source, ps.session_id, ps.first_ts, ps.last_ts, ps.stage_count, ps.bytes, v_user,
         v_adm_epoch, v_adm_version::text
    FROM per_session AS ps
   WHERE p_cursor_ts IS NULL
      OR ps.last_ts < p_cursor_ts
      OR (ps.last_ts = p_cursor_ts AND (ps.session_id COLLATE "C", ps.source COLLATE "C")
                                      > (p_cursor_session COLLATE "C", p_cursor_source COLLATE "C"))
   ORDER BY ps.last_ts DESC, ps.session_id COLLATE "C", ps.source COLLATE "C"
   LIMIT v_limit;
END
$buffered_sessions$;

CREATE OR REPLACE FUNCTION sessions.buffered_session_stages_for(
  p_workspace text, p_source text, p_session_id text,
  p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text, p_head_kind text,
  p_head_pending boolean, p_privacy_baseline_sha256 text
)
RETURNS TABLE(batch_id text, stage_key text, stage_sha256 text, stage_bytes bigint,
              group_indexes integer[], group_ordinals jsonb, kept_ordinals integer[], reset_epoch bigint,
              accepted_at timestamptz,
              admission_epoch text, admission_version text)
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
  IF NOT v_open THEN
    RETURN;
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (identical in buffered_sessions_for and buffered_session_stages_for except
  -- stg's driving FROM/WHERE head and its comment, which 044040 makes session-first in the detail;
  -- src/session-ingest/__tests__/owner-buffered-read.pglite.test.ts asserts it)
  WITH ctl AS MATERIALIZED (
    SELECT (SELECT count(*) FROM sessions.upload_resets AS ur
             WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = v_user) AS resets,
           EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS lazy,
           (SELECT ck.control_epoch FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_epoch,
           (SELECT ck.source_identity_sha256 FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_identity,
           (SELECT lp.source_identity_sha256 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_identity,
           (SELECT lp.control_epoch FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_epoch
  ), stg AS MATERIALIZED (
    -- The active, acknowledged, unredacted stage of each buffered batch of this user. Outbox,
    -- stage, attempt and batch identities are equal; the stage reset_epoch equals the user's
    -- upload_resets count (gc_kept). 044040: driven from the claims holding (v_source, v_session)
    -- (ingest_claim_attempts_records_gin_idx), then the claim's stages, the outbox row that has one
    -- of them active, and the batch; every 043700 predicate below is kept verbatim. v_source is
    -- never NULL here (22023 above), so 043700's 'v_source IS NULL OR' branch was dead.
    -- ADMISSION WITNESS: the version is source_control_version on the stage's ORIGINAL claim
    -- attempt; the epoch is not stored on the claim and is derived from the immutable
    -- ingest_lazy_policy.control_epoch, valid only when the claim's source_identity_sha256 is
    -- the policy's. A NULL version, NULL identity, a mismatch or no policy hides the stage.
    -- The pair must equal the gate's required (v_adm_epoch, v_adm_version): no control of
    -- any kind since admission. GC ORIGINAL CLAIM: every ancestor of a GC replacement stage
    -- (parent_stage_token chain, bounded at 64) carries this same claim_token, so a
    -- replacement can never lend a newer claim's admission. RULE 5(c): the claim's admission
    -- reset epoch is set, and no upload_resets row of the user is at or after the batch's
    -- acceptance (committed_at); a reset at or after acceptance hides the stage even when the
    -- count check above still matches.
    SELECT s.stage_token, s.claim_token, s.origin_id, s.batch_id, s.stage_key, s.stage_sha256, s.stage_bytes,
           s.group_ordinals, s.reset_epoch, a.records, a.epochs, a.reset_epoch AS a_reset,
           b.committed_at AS accepted_at, a.source_control_version AS adm_version, ctl.pol_epoch AS adm_epoch
      FROM sessions.ingest_claim_attempts AS a
      JOIN sessions.ingest_stage_intents AS s ON a.claim_token = s.claim_token
      JOIN sessions.ingest_buffer_outbox AS o ON s.stage_token = o.active_stage_token
      JOIN sessions.ingest_batches AS b
        ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
     CROSS JOIN ctl
     WHERE jsonb_typeof(a.records) = 'array'
       AND a.records @> jsonb_build_array(jsonb_build_object('source', v_source, 'session_id', v_session))
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
  )
  -- keep-predicate:end
  SELECT st.batch_id, st.stage_key, st.stage_sha256, st.stage_bytes,
         array_agg(k.group_index ORDER BY k.group_index), st.group_ordinals,
         (SELECT array_agg(o ORDER BY o) FROM kept AS k2 CROSS JOIN LATERAL unnest(k2.ords) AS o
           WHERE k2.stage_token = st.stage_token AND k2.source = v_source AND k2.session_id = v_session),
         st.reset_epoch, st.accepted_at, v_adm_epoch, v_adm_version::text
    FROM kept AS k
    JOIN stg AS st ON st.stage_token = k.stage_token
   WHERE k.source = v_source AND k.session_id = v_session
   GROUP BY st.stage_token, st.batch_id, st.stage_key, st.stage_sha256, st.stage_bytes, st.group_ordinals,
            st.reset_epoch, st.accepted_at
   ORDER BY st.accepted_at, st.batch_id COLLATE "C", st.stage_key COLLATE "C";
END
$buffered_stages$;
