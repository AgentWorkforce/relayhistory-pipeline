-- Option C, PR A: owner-only read of buffered data (slot 043700, assigned by the registry keeper).
--
-- Adds three read-only functions over the buffered acceptance ledger so the session read API
-- (bulls96-session-read-api, role bulls96_read_api) can show the UPLOADING user their own
-- buffered sessions before canonical publication (049000 -> 050000 -> 051000) exists:
--
--   1. sessions.owner_buffered_read_scope(p_workspace_id): the role guard and the user pin.
--      A copy of sessions.session_directory_read_api_scope (20261105034000) plus the NEW
--      transaction-local setting bulls96_read.user_id, which the trusted read API binds from
--      the verified identity exactly as it binds bulls96_read.org_id / workspace_id. Refuses
--      42501 unless the caller is bulls96_read_api, all three settings are non-empty and
--      p_workspace_id equals the bound workspace. Refuses 55006 'buffered read fenced' while
--      sessions.ingest_privacy_authority.state = 'fenced' (the read API answers 503
--      buffered_fenced on detail and omits buffered rows on the list). Not SECURITY DEFINER;
--      owner-only EXECUTE (called by the two definer functions below as their owner).
--      MIRROR GATE (same 55006 refusal). 20261105041000 is dormant, so Neon is still the
--      privacy authority and deletions/resets reach Supabase only through the relayhistory
--      control mirror (sessions.ingest_source_control_heads, 20261105010000:59-77, written by
--      ingest_source_control_begin/complete). The scope also refuses when the
--      sessions.ingest_lazy_policy singleton is missing or its valid_until <= clock_timestamp(),
--      or when the caller's (org, workspace, user) control head is pending or carries an epoch
--      other than the policy's control_epoch. No head row is NOT a refusal (the user may never
--      have issued a control). This proves only "no in-flight or stale control for the
--      caller"; it does NOT prove the mirror is running. That operational precondition (the
--      mirror live on 100% of isolates) is a release-owner check outside SQL, required before
--      the read API flag is enabled.
--   2. sessions.buffered_sessions_for(p_workspace, p_cursor_ts, p_cursor_session,
--      p_cursor_source, p_limit): the caller's own KEPT buffered sessions, newest first.
--      PUBLISHED EXCLUSION: a (source, session_id) with a canonical pointer for the caller
--      (sessions.ingest_publication_heads, 20261104060000, state 'live' or a current_key) is
--      not listed, so a partly published session is never double-counted; canonical wins.
--      Scoped to (org, workspace, user) to stay on the heads primary-key prefix; a cross-user
--      collision on the same (source, session_id) is left to the read API's dedup (canonical
--      wins there too). The detail function is not filtered: the read API serves canonical
--      first and only falls back to buffered detail when canonical has nothing.
--   3. sessions.buffered_session_stages_for(p_workspace, p_source, p_session_id): for one
--      owned, kept session, the S3 stage rows the read API fetches and projects.
--
-- Keep predicate. Both readers apply, at read time, a lock-free copy of the predicates the
-- buffered GC uses to decide which bytes survive. They never call the GC helpers:
--   - sessions.ingest_buffer_gc_kept (20261104051000:22-74): stage/attempt identity equal; the
--     user's upload_resets count equals the stage reset_epoch; the claim attempt is not fenced
--     (the sessions.ingest_attempt_fenced predicate, 20261104030000:85-111: fence version
--     epoch, per-session reset epoch, upload_deletions); a group never crosses sessions.
--   - sessions.ingest_buffer_gc_ordinals (20261104055000:1479-1517), ACK branch, when the
--     lazy-current policy row exists: per ordinal, an accepted immutable ACK decision whose
--     native slot's current_owner is the caller, with the same seat/digest/revision/source/
--     session equality and the same upload_deletions check. ingest_buffer_gc_ordinals takes
--     ingest_receipt_seat_clock FOR SHARE (the singleton behind the claim-503 contention);
--     this copy reads the clock row with a plain snapshot read and takes NO row lock.
--   Where the GC raises on malformed authority (unknown ordinal, mixed group, missing epoch),
--   these readers fail closed: the affected group or session is simply not returned.
--   The conditions are a conjunction: in lazy mode the attempt-fence check applies too, so
--   the reader can hide bytes the GC still keeps, never the reverse.
--
-- Locks. No FOR UPDATE / FOR SHARE / FOR KEY SHARE / FOR NO KEY UPDATE, no advisory lock, no
-- write: both readers are STABLE and run inside the read API's READ ONLY transactions, taking
-- only AccessShareLock on the relations they read. The read API sets statement_timeout per
-- transaction (a STABLE function cannot SET LOCAL); the list is clamped to 1..100 rows.
--
-- Index. None is created here: the migrator is transactional, and a plain CREATE INDEX on the
-- hot outbox would take a SHARE lock that blocks claim/commit writes. The user-keyed index is
-- proposed out of band, CONCURRENTLY (no slot under the registry rule; a separate Khaliq apply,
-- required before the read API flag is enabled):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_buffer_outbox_user_buffered_idx
--     ON sessions.ingest_buffer_outbox (org_id, workspace_id, user_id, created_at)
--     WHERE state = 'buffered';
--
-- Schema version. Applying this migration changes the migration-set generation()
-- (src/hoopdb/migrate.ts). The buffered Worker's captured generation must be updated only
-- after the new PASS (docs/private-ingest-buffered-queues.md:17); until then buffered
-- acceptance answers a transient 503, and no pending generation is acknowledged.
--
-- Roles. Never creates or alters a role, grants no table and no write. EXECUTE on 2 and 3 to
-- bulls96_read_api only (src/hoopdb/sessions-function-acl.json +3 entries, lane
-- scripts/sessions-function-boundary/lanes/owner-buffered-read.ts).
--
-- Code only; nothing here is applied, deployed, enabled or granted by this repository. It
-- ships only in a reviewed generation window with Khaliq's explicit apply OK. Down path:
-- supabase/rollbacks/20261105043700_owner_buffered_read.down.sql.
DO $owner_buffered_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relname IN('ingest_buffer_outbox','ingest_stage_intents','ingest_claim_attempts',
    'ingest_batches','upload_deletions','upload_resets','ingest_fence_versions','ingest_lazy_policy',
    'ingest_lazy_ack_decisions','ingest_lazy_native_slots','ingest_receipt_seat_clock','ingest_privacy_authority',
    'ingest_source_control_heads','ingest_publication_heads')
    AND c.relowner<>me)
  OR (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relkind='r' AND c.relname IN('ingest_buffer_outbox','ingest_stage_intents','ingest_claim_attempts',
    'ingest_batches','upload_deletions','upload_resets','ingest_fence_versions','ingest_lazy_policy',
    'ingest_lazy_ack_decisions','ingest_lazy_native_slots','ingest_receipt_seat_clock','ingest_privacy_authority',
    'ingest_source_control_heads','ingest_publication_heads'))<>14
  OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_read_api') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='owner buffered read: requires original creator, the buffered ledger and bulls96_read_api';END IF;
END $owner_buffered_creator$;

-- The user pin. Inside SECURITY DEFINER the `role` setting still names the caller's SET ROLE,
-- and session_user names the login (session_directory_read_api_scope). A transaction-local
-- set_config reads '' on a pooled connection after its transaction ends, and NULL where it
-- was never set: both refuse.
CREATE FUNCTION sessions.owner_buffered_read_scope(p_workspace_id text)
RETURNS TABLE(org_id text, workspace_id text, user_id text)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_org text := nullif(current_setting('bulls96_read.org_id', true), '');
  v_workspace text := nullif(current_setting('bulls96_read.workspace_id', true), '');
  v_user text := nullif(current_setting('bulls96_read.user_id', true), '');
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'bulls96_read_api' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'owner buffered read: caller is not the read api role';
  END IF;
  IF v_org IS NULL OR v_workspace IS NULL OR v_user IS NULL
     OR p_workspace_id IS DISTINCT FROM v_workspace THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'owner buffered read: user binding missing or not this workspace';
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.ingest_privacy_authority AS pa WHERE pa.singleton AND pa.state = 'fenced') THEN
    RAISE EXCEPTION USING ERRCODE = '55006', MESSAGE = 'buffered read fenced',
      HINT = 'The privacy authority is fenced; the read API answers 503 buffered_fenced (retryable).';
  END IF;
  -- Mirror gate: plain snapshot reads, no lock (see header).
  IF NOT EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp
                  WHERE lp.singleton AND lp.valid_until > clock_timestamp())
     OR EXISTS (SELECT 1 FROM sessions.ingest_source_control_heads AS h
                 WHERE h.org_id = v_org AND h.workspace_id = v_workspace AND h.user_id = v_user
                   AND (h.pending OR h.epoch IS DISTINCT FROM
                        (SELECT lp.control_epoch FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton))) THEN
    RAISE EXCEPTION USING ERRCODE = '55006', MESSAGE = 'buffered read fenced',
      HINT = 'The control mirror policy is missing/expired or the caller has an in-flight or stale control; the read API answers 503 buffered_fenced (retryable).';
  END IF;
  RETURN QUERY SELECT v_org, v_workspace, v_user;
END
$scope$;

-- The caller's own kept buffered sessions. One row per (source, session_id): first_ts/last_ts
-- are the acceptance (ingest_batches.committed_at) times of the oldest/newest buffered batch
-- holding a kept group of that session (the ledger has no transcript timestamps); stage_count/
-- bytes count the active stage objects holding one (bytes are whole objects: the ledger records
-- no per-group size). Ordered by last_ts DESC, session_id, source (COLLATE "C"); the cursor is
-- the last row's triple and a page holds the rows strictly after it. All three cursor values or
-- none. p_limit NULL is 50, otherwise clamped to 1..100. Errors: 22023 (partial cursor) and the
-- scope's 42501 / 55006. No row lock, no advisory lock, no write.
CREATE FUNCTION sessions.buffered_sessions_for(
  p_workspace text, p_cursor_ts timestamptz, p_cursor_session text, p_cursor_source text, p_limit integer
)
RETURNS TABLE(source text, session_id text, first_ts timestamptz, last_ts timestamptz,
              stage_count integer, bytes bigint, user_id text)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $buffered_sessions$
#variable_conflict use_column
DECLARE
  v_org text;
  v_ws text;
  v_user text;
  v_source text := NULL;
  v_session text := NULL;
  v_limit integer := least(greatest(coalesce(p_limit, 50), 1), 100);
BEGIN
  SELECT sc.org_id, sc.workspace_id, sc.user_id INTO STRICT v_org, v_ws, v_user
    FROM sessions.owner_buffered_read_scope(p_workspace) AS sc;
  IF (p_cursor_ts IS NULL) <> (p_cursor_session IS NULL) OR (p_cursor_ts IS NULL) <> (p_cursor_source IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'owner buffered read: cursor needs ts, session and source together';
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (identical in buffered_sessions_for and buffered_session_stages_for;
  -- src/session-ingest/__tests__/owner-buffered-read.pglite.test.ts asserts it)
  WITH ctl AS MATERIALIZED (
    SELECT (SELECT count(*) FROM sessions.upload_resets AS ur
             WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = v_user) AS resets,
           EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS lazy,
           (SELECT ck.control_epoch FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_epoch,
           (SELECT ck.source_identity_sha256 FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_identity
  ), stg AS MATERIALIZED (
    -- The active, acknowledged, unredacted stage of each buffered batch of this user. Outbox,
    -- stage, attempt and batch identities are equal; the stage reset_epoch equals the user's
    -- upload_resets count (gc_kept). v_source/v_session narrow to one session (detail only).
    SELECT s.stage_token, s.claim_token, s.origin_id, s.batch_id, s.stage_key, s.stage_sha256, s.stage_bytes,
           s.group_ordinals, s.reset_epoch, a.records, a.epochs, a.reset_epoch AS a_reset,
           b.committed_at AS accepted_at
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
       AND (v_source IS NULL OR a.records @> jsonb_build_array(jsonb_build_object('source', v_source, 'session_id', v_session)))
  ), grp AS (
    -- One row per (stage, object group index, original claim ordinal).
    SELECT st.stage_token, (g.idx - 1)::integer AS group_index, (e.value::text)::integer AS ord
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_array_elements(st.group_ordinals) WITH ORDINALITY AS g(value, idx)
     CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(g.value) = 'array' THEN g.value ELSE '[null]'::jsonb END) AS e(value)
     WHERE jsonb_typeof(e.value) = 'number' AND e.value::text ~ '^(0|[1-9][0-9]{0,2})$'
  ), bad_stage AS (
    -- gc_kept refuses malformed groups and duplicate ordinals: such a stage is never shown.
    SELECT st.stage_token
      FROM stg AS st
     WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(st.group_ordinals) AS g(value)
                    WHERE jsonb_typeof(g.value) IS DISTINCT FROM 'array' OR jsonb_array_length(g.value) = 0
                       OR EXISTS (SELECT 1 FROM jsonb_array_elements(g.value) AS e(value)
                                   WHERE jsonb_typeof(e.value) IS DISTINCT FROM 'number'
                                      OR e.value::text !~ '^(0|[1-9][0-9]{0,2})$'))
        OR EXISTS (SELECT 1 FROM grp AS g2 WHERE g2.stage_token = st.stage_token GROUP BY g2.ord HAVING count(*) > 1)
  ), rec AS (
    SELECT st.stage_token, r.ord, r.source, r.session_id, r.record_id, r.revision, r.digest
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_to_recordset(st.records)
           AS r(ord integer, source text, session_id text, record_id text, revision bigint, digest text)
  ), recn AS (
    -- A duplicate original ordinal makes every record of that ordinal unusable (gc_kept 55000).
    SELECT rc.* FROM rec AS rc
     WHERE (SELECT count(*) FROM rec AS r2 WHERE r2.stage_token = rc.stage_token AND r2.ord = rc.ord) = 1
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
            -- ingest_buffer_gc_ordinals ACK branch (lazy-current policy present): the native
            -- slot's current owner is the caller, with the same seat/digest/revision/session.
            AND (NOT ctl.lazy OR EXISTS (
              SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS ad
                JOIN sessions.ingest_lazy_native_slots AS ns ON ns.key_sha256 = ad.native_key_sha256
               WHERE ad.claim_token = st.claim_token AND ad.ord = gr.ord AND ad.body_accepted
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
    SELECT k.source, k.session_id, min(st.accepted_at) AS first_ts, max(st.accepted_at) AS last_ts,
           count(*)::integer AS stage_count, sum(st.stage_bytes)::bigint AS bytes
      FROM (SELECT DISTINCT k0.stage_token, k0.source, k0.session_id FROM kept AS k0) AS k
      JOIN stg AS st ON st.stage_token = k.stage_token
     WHERE NOT EXISTS (
       SELECT 1 FROM sessions.ingest_publication_heads AS ph
        WHERE ph.org_id = v_org AND ph.workspace_id = v_ws AND ph.user_id = v_user
          AND ph.source = k.source AND ph.session_id = k.session_id
          AND (ph.state = 'live' OR ph.current_key IS NOT NULL))
     GROUP BY k.source, k.session_id
  )
  SELECT ps.source, ps.session_id, ps.first_ts, ps.last_ts, ps.stage_count, ps.bytes, v_user
    FROM per_session AS ps
   WHERE p_cursor_ts IS NULL
      OR ps.last_ts < p_cursor_ts
      OR (ps.last_ts = p_cursor_ts AND (ps.session_id COLLATE "C", ps.source COLLATE "C")
                                      > (p_cursor_session COLLATE "C", p_cursor_source COLLATE "C"))
   ORDER BY ps.last_ts DESC, ps.session_id COLLATE "C", ps.source COLLATE "C"
   LIMIT v_limit;
END
$buffered_sessions$;

-- The stage rows of ONE owned, kept buffered session, oldest batch first: what the read API
-- needs to GET the object (stage_key, stage_sha256, stage_bytes; the object is a zstd frame
-- whose JSON header lists groups as [source, sessionId], src/session-ingest/staging.ts) and
-- to project only this session's kept lines: group_indexes are indexes into the object's
-- header.groups (= stage group_ordinals positions) and kept_ordinals are the original claim
-- ordinals (group_ordinals[g][j] names line j of group g) still kept. Same keep predicate and
-- scope as buffered_sessions_for. Not owned, absent, deleted, reset, fenced or redacted all
-- answer the same empty result. Errors: 22023 (missing source/session) and the scope's.
CREATE FUNCTION sessions.buffered_session_stages_for(
  p_workspace text, p_source text, p_session_id text
)
RETURNS TABLE(batch_id text, stage_key text, stage_sha256 text, stage_bytes bigint,
              group_indexes integer[], kept_ordinals integer[], reset_epoch bigint, accepted_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $buffered_stages$
#variable_conflict use_column
DECLARE
  v_org text;
  v_ws text;
  v_user text;
  v_source text := p_source;
  v_session text := p_session_id;
BEGIN
  SELECT sc.org_id, sc.workspace_id, sc.user_id INTO STRICT v_org, v_ws, v_user
    FROM sessions.owner_buffered_read_scope(p_workspace) AS sc;
  IF v_source IS NULL OR char_length(v_source) NOT BETWEEN 1 AND 64
     OR v_session IS NULL OR char_length(v_session) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'owner buffered read: source and session are required';
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (identical in buffered_sessions_for and buffered_session_stages_for;
  -- src/session-ingest/__tests__/owner-buffered-read.pglite.test.ts asserts it)
  WITH ctl AS MATERIALIZED (
    SELECT (SELECT count(*) FROM sessions.upload_resets AS ur
             WHERE ur.org_id = v_org AND ur.workspace_id = v_ws AND ur.user_id = v_user) AS resets,
           EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS lazy,
           (SELECT ck.control_epoch FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_epoch,
           (SELECT ck.source_identity_sha256 FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_identity
  ), stg AS MATERIALIZED (
    -- The active, acknowledged, unredacted stage of each buffered batch of this user. Outbox,
    -- stage, attempt and batch identities are equal; the stage reset_epoch equals the user's
    -- upload_resets count (gc_kept). v_source/v_session narrow to one session (detail only).
    SELECT s.stage_token, s.claim_token, s.origin_id, s.batch_id, s.stage_key, s.stage_sha256, s.stage_bytes,
           s.group_ordinals, s.reset_epoch, a.records, a.epochs, a.reset_epoch AS a_reset,
           b.committed_at AS accepted_at
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
       AND (v_source IS NULL OR a.records @> jsonb_build_array(jsonb_build_object('source', v_source, 'session_id', v_session)))
  ), grp AS (
    -- One row per (stage, object group index, original claim ordinal).
    SELECT st.stage_token, (g.idx - 1)::integer AS group_index, (e.value::text)::integer AS ord
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_array_elements(st.group_ordinals) WITH ORDINALITY AS g(value, idx)
     CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(g.value) = 'array' THEN g.value ELSE '[null]'::jsonb END) AS e(value)
     WHERE jsonb_typeof(e.value) = 'number' AND e.value::text ~ '^(0|[1-9][0-9]{0,2})$'
  ), bad_stage AS (
    -- gc_kept refuses malformed groups and duplicate ordinals: such a stage is never shown.
    SELECT st.stage_token
      FROM stg AS st
     WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(st.group_ordinals) AS g(value)
                    WHERE jsonb_typeof(g.value) IS DISTINCT FROM 'array' OR jsonb_array_length(g.value) = 0
                       OR EXISTS (SELECT 1 FROM jsonb_array_elements(g.value) AS e(value)
                                   WHERE jsonb_typeof(e.value) IS DISTINCT FROM 'number'
                                      OR e.value::text !~ '^(0|[1-9][0-9]{0,2})$'))
        OR EXISTS (SELECT 1 FROM grp AS g2 WHERE g2.stage_token = st.stage_token GROUP BY g2.ord HAVING count(*) > 1)
  ), rec AS (
    SELECT st.stage_token, r.ord, r.source, r.session_id, r.record_id, r.revision, r.digest
      FROM stg AS st
     CROSS JOIN LATERAL jsonb_to_recordset(st.records)
           AS r(ord integer, source text, session_id text, record_id text, revision bigint, digest text)
  ), recn AS (
    -- A duplicate original ordinal makes every record of that ordinal unusable (gc_kept 55000).
    SELECT rc.* FROM rec AS rc
     WHERE (SELECT count(*) FROM rec AS r2 WHERE r2.stage_token = rc.stage_token AND r2.ord = rc.ord) = 1
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
            -- ingest_buffer_gc_ordinals ACK branch (lazy-current policy present): the native
            -- slot's current owner is the caller, with the same seat/digest/revision/session.
            AND (NOT ctl.lazy OR EXISTS (
              SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS ad
                JOIN sessions.ingest_lazy_native_slots AS ns ON ns.key_sha256 = ad.native_key_sha256
               WHERE ad.claim_token = st.claim_token AND ad.ord = gr.ord AND ad.body_accepted
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
         array_agg(k.group_index ORDER BY k.group_index),
         (SELECT array_agg(o ORDER BY o) FROM kept AS k2 CROSS JOIN LATERAL unnest(k2.ords) AS o
           WHERE k2.stage_token = st.stage_token AND k2.source = v_source AND k2.session_id = v_session),
         st.reset_epoch, st.accepted_at
    FROM kept AS k
    JOIN stg AS st ON st.stage_token = k.stage_token
   WHERE k.source = v_source AND k.session_id = v_session
   GROUP BY st.stage_token, st.batch_id, st.stage_key, st.stage_sha256, st.stage_bytes, st.reset_epoch, st.accepted_at
   ORDER BY st.accepted_at, st.batch_id COLLATE "C", st.stage_key COLLATE "C";
END
$buffered_stages$;

REVOKE ALL ON FUNCTION sessions.owner_buffered_read_scope(text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.buffered_sessions_for(text, timestamptz, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.buffered_session_stages_for(text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.buffered_sessions_for(text, timestamptz, text, text, integer)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.buffered_session_stages_for(text, text, text)
  TO bulls96_read_api;

-- As in 20261105034000: strip EXECUTE from every other grantee present at creation (default
-- privileges, a pre-existing runtime role). A grant added later is still refused at call time
-- by the role guard in the scope helper.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.owner_buffered_read_scope(text)'::regprocedure,
    'sessions.buffered_sessions_for(text, timestamptz, text, text, integer)'::regprocedure,
    'sessions.buffered_session_stages_for(text, text, text)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api'
                  AND v_fn IN ('sessions.buffered_sessions_for(text, timestamptz, text, text, integer)'::regprocedure,
                               'sessions.buffered_session_stages_for(text, text, text)'::regprocedure))
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
