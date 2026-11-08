-- Option C, PR A: owner-only read of buffered data (slot 043700, assigned by the registry keeper).
--
-- Adds a read-only gate and two read-only readers over the buffered acceptance ledger so the
-- session read API (bulls96-session-read-api, role bulls96_read_api) can show the UPLOADING user
-- their own buffered sessions (list, turns, metadata; there is no events surface) before
-- canonical publication (049000 -> 050000 -> 051000) exists. Built to the R2 control-head
-- contract v4 consumer rule (docs/handoffs/neon-census-role-20261107/r2-control-head-contract-v1.md):
--
--   1. sessions.owner_buffered_read_scope(p_workspace_id, p_cutover_epoch, p_head_epoch,
--      p_head_version, p_head_digest, p_head_pending): role guard, user pin and the gate.
--      A copy of sessions.session_directory_read_api_scope (20261105034000) plus the NEW
--      transaction-local setting bulls96_read.user_id, which the trusted read API binds from
--      the verified identity exactly as it binds bulls96_read.org_id / workspace_id. Refuses
--      42501 unless the caller is bulls96_read_api, all three settings are non-empty and
--      p_workspace_id equals the bound workspace. Refuses 55006 'buffered read fenced' while
--      sessions.ingest_privacy_authority.state = 'fenced', and on a MALFORMED head tuple.
--      Not SECURITY DEFINER; owner-only EXECUTE (called by the two definer readers as owner).
--      HEAD TUPLE. The read API passes the R2 control-head answer it fetched AFTER the S3 fetch
--      and before emitting any byte: the active cutover epoch, and the head (epoch, version,
--      digest, pending) or a NULL head. Shapes, checked here: epochs/digest ^[0-9a-f]{64}$;
--      version a decimal string ^[1-9][0-9]{0,18}$ within bigint (never a JS number: 2^53+1
--      must not round); the three head fields all NULL or all set; pending NOT NULL and false
--      when there is no head. Anything else is malformed: 55006.
--      GATE (a closed gate returns gate_open = false: the readers return nothing; no error, so a
--      hidden reason is indistinguishable from "no buffered data"). Open only when ALL hold:
--        - NOT p_head_pending, and p_head_epoch IS NULL or p_head_epoch = p_cutover_epoch;
--        - the ingest_lazy_policy singleton exists, valid_until > clock_timestamp(), and its
--          (immutable, 20261104055000:113-131) control_epoch = p_cutover_epoch;
--        - G2 PARITY: a sessions.owner_buffered_parity row for exactly (org, workspace, user)
--          whose cutover_epoch = p_cutover_epoch; whose head tuple equals the passed head, or
--          no_head with a NULL head; whose target_generation = the current schema generation
--          (sessions.__96bulls_role_validation 'session_directory_search_worker' PASS, plain
--          read); and whose deletion_set_digest, reset_set_digest and target_lineage_digest equal
--          sessions.owner_buffered_parity_expected(...) recomputed now;
--        - RECEIPTS: the user's sessions.ingest_control_receipts are exactly versions 1..V at the
--          cutover epoch with receipt V's digest = p_head_digest (V = head version), or none at
--          all with a NULL head; any receipt on another epoch, a gap, a missing tip or a
--          different tip digest closes the gate.
--      Required admission when open: epoch = p_cutover_epoch, version = head version (0 with a
--      NULL head). Head epoch <> cutover epoch: nothing is visible.
--   2. sessions.owner_buffered_parity_expected(p_org, p_workspace_id, p_user_id, p_cutover_epoch,
--      p_head_version, p_head_digest): the one canonical definition of the parity digests (owner-
--      only, not SECURITY DEFINER, STABLE, no write). A future owner-run proof writer and the
--      tests use the same function; it is not a writer. Encodings (sha256 hex over the UTF-8
--      jsonb text of the array):
--        deletion_set_digest   ['owner-buffered-deletion-set-v1', org, ws, user,
--                               [[origin_id, source, session_id], ...] ordered COLLATE "C"]
--                              over sessions.upload_deletions of the user;
--        reset_set_digest      ['owner-buffered-reset-set-v1', org, ws, user,
--                               [request_id, ...] ordered COLLATE "C"] over sessions.upload_resets;
--        target_lineage_digest ['owner-buffered-target-lineage-v1', org, ws, user, cutover_epoch,
--                               head_version text|null, head_digest|null,
--                               [[version text, digest, kind], ...] ordered by version] over the
--                              user's ingest_control_receipts at the cutover epoch. A digest over
--                              the contiguous Supabase receipts 1..V bound to head V; it is NOT a
--                              Neon history chain (none exists).
--   3. sessions.buffered_sessions_for(p_workspace, p_cursor_ts, p_cursor_session, p_cursor_source,
--      p_limit, <head tuple>): the caller's own KEPT, ADMITTED buffered sessions, newest first.
--      PUBLISHED EXCLUSION: a (source, session_id) with a canonical pointer for the caller
--      (sessions.ingest_publication_heads, 20261104060000, state 'live' or a current_key) is
--      not listed, so a partly published session is never double-counted; canonical wins.
--      The detail reader is not filtered: the read API serves canonical first and only falls
--      back to buffered detail when canonical has nothing.
--   4. sessions.buffered_session_stages_for(p_workspace, p_source, p_session_id, <head tuple>):
--      for one owned, kept, admitted session, the S3 stage rows the read API fetches and
--      projects (turns and metadata).
--   5. sessions.owner_buffered_parity: the per-user G2 parity proof table. Owner-written only:
--      RLS on, every privilege revoked from PUBLIC and all app roles, no writer function in this
--      migration, NO ROWS by default (nobody is visible until an owner-run proof write, a
--      separate production operation with Khaliq's explicit OK). A proof is never relabeled:
--      when the head or the generation moves, the old row no longer matches and hides.
--
-- Per-row ADMISSION WITNESS (inside the keep predicate). Version = source_control_version on
-- the ORIGINAL ingest_claim_attempts row reached through ingest_stage_intents.claim_token (and,
-- in lazy mode, the same value on every ingest_lazy_ack_decisions row used). Epoch is DERIVED
-- from the immutable policy, not persisted: ingest_lazy_policy.control_epoch, valid only when
-- claim.source_identity_sha256 = policy.source_identity_sha256. NULL version, NULL identity, a
-- mismatch or no policy: hidden. A GC replacement stage inherits the parent's claim_token
-- (20261104051000:249, 20261104055000:1691); the readers additionally require every ancestor on
-- the parent_stage_token chain to carry the same claim_token, so a replacement can never bind
-- a newer claim's admission. Consequence (contract, accepted): any selection/reset after an
-- upload hides that upload until it is canonically published.
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
--   The conditions are a conjunction, so the reader can hide bytes the GC still keeps, never
--   the reverse.
--
-- Locks. No FOR UPDATE / FOR SHARE / FOR KEY SHARE / FOR NO KEY UPDATE, no advisory lock, no
-- write: every function here is STABLE and runs inside the read API's READ ONLY transactions,
-- taking only AccessShareLock on the relations it reads. The read API sets statement_timeout
-- per transaction (a STABLE function cannot SET LOCAL); the list is clamped to 1..100 rows.
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
-- acceptance answers a transient 503, and no pending generation is acknowledged. Ordering:
-- schema-independent of 20261105043500, but recorded after it, in its own generation window.
--
-- Roles. Never creates or alters a role. Grants no table privilege and no write. EXECUTE on
-- 3 and 4 to bulls96_read_api only (src/hoopdb/sessions-function-acl.json +4 entries, lane
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
    'ingest_control_receipts','ingest_publication_heads')
    AND c.relowner<>me)
  OR (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relkind='r' AND c.relname IN('ingest_buffer_outbox','ingest_stage_intents','ingest_claim_attempts',
    'ingest_batches','upload_deletions','upload_resets','ingest_fence_versions','ingest_lazy_policy',
    'ingest_lazy_ack_decisions','ingest_lazy_native_slots','ingest_receipt_seat_clock','ingest_privacy_authority',
    'ingest_control_receipts','ingest_publication_heads'))<>14
  OR to_regclass('sessions.owner_buffered_parity') IS NOT NULL
  OR NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_read_api') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='owner buffered read: requires original creator, the buffered ledger and bulls96_read_api';END IF;
END $owner_buffered_creator$;

-- The per-user G2 parity proof. Owner-written only; no rows by default (see header, item 5).
CREATE TABLE sessions.owner_buffered_parity (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  cutover_epoch text NOT NULL CHECK (cutover_epoch ~ '^[0-9a-f]{64}$'),
  head_epoch text CHECK (head_epoch ~ '^[0-9a-f]{64}$'),
  head_version bigint CHECK (head_version > 0),
  head_digest text CHECK (head_digest ~ '^[0-9a-f]{64}$'),
  no_head boolean NOT NULL,
  deletion_set_digest text NOT NULL CHECK (deletion_set_digest ~ '^[0-9a-f]{64}$'),
  reset_set_digest text NOT NULL CHECK (reset_set_digest ~ '^[0-9a-f]{64}$'),
  target_lineage_digest text NOT NULL CHECK (target_lineage_digest ~ '^[0-9a-f]{64}$'),
  target_generation text NOT NULL CHECK (target_generation ~ '^[0-9a-f]{64}$'),
  written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id, workspace_id, user_id),
  CONSTRAINT owner_buffered_parity_head_all_or_none CHECK (
    (head_epoch IS NULL AND head_version IS NULL AND head_digest IS NULL AND no_head)
    OR (head_epoch IS NOT NULL AND head_version IS NOT NULL AND head_digest IS NOT NULL AND NOT no_head))
);
ALTER TABLE sessions.owner_buffered_parity ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.owner_buffered_parity
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder, bulls96_read_api;
-- Default privileges may have granted the new table to another role at creation: strip every
-- non-owner grantee.
DO $parity_acl$
DECLARE v_grantee text;
BEGIN
  FOR v_grantee IN
    SELECT DISTINCT r.rolname
      FROM pg_class AS c
      CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) AS a
      JOIN pg_roles AS r ON r.oid = a.grantee
     WHERE c.oid = 'sessions.owner_buffered_parity'::regclass AND a.grantee <> c.relowner
  LOOP
    EXECUTE format('REVOKE ALL ON sessions.owner_buffered_parity FROM %I', v_grantee);
  END LOOP;
END
$parity_acl$;

-- The canonical parity digests and the receipt-lineage check (header, item 2). Plain snapshot
-- reads only. receipts_ok: with a head, exactly versions 1..V at the cutover epoch and receipt
-- V's digest = p_head_digest; without a head, no receipt at all; never a receipt on another
-- epoch. target_generation is NULL when no PASS generation is recorded.
CREATE FUNCTION sessions.owner_buffered_parity_expected(
  p_org text, p_workspace_id text, p_user_id text, p_cutover_epoch text, p_head_version bigint, p_head_digest text
)
RETURNS TABLE(deletion_set_digest text, reset_set_digest text, target_lineage_digest text,
              target_generation text, receipts_ok boolean)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $parity_expected$
DECLARE
  v_gen text;
  v_n bigint;
  v_min bigint;
  v_max bigint;
  v_other bigint;
  v_tip text;
  v_deletions jsonb;
  v_resets jsonb;
  v_lineage jsonb;
BEGIN
  -- The role-validation record is created by the migrator outside the migration set.
  IF to_regclass('sessions.__96bulls_role_validation') IS NOT NULL THEN
    EXECUTE 'SELECT rv.generation FROM sessions.__96bulls_role_validation AS rv
              WHERE rv.name = ''session_directory_search_worker'' AND rv.status = ''pass'''
      INTO v_gen;
  END IF;
  IF v_gen IS NOT NULL AND v_gen !~ '^[0-9a-f]{64}$' THEN
    v_gen := NULL;
  END IF;
  SELECT coalesce(jsonb_agg(jsonb_build_array(d.origin_id, d.source, d.session_id)
                            ORDER BY d.origin_id COLLATE "C", d.source COLLATE "C", d.session_id COLLATE "C"), '[]'::jsonb)
    INTO v_deletions
    FROM sessions.upload_deletions AS d
   WHERE d.org_id = p_org AND d.workspace_id = p_workspace_id AND d.user_id = p_user_id;
  SELECT coalesce(jsonb_agg(to_jsonb(u.request_id) ORDER BY u.request_id COLLATE "C"), '[]'::jsonb)
    INTO v_resets
    FROM sessions.upload_resets AS u
   WHERE u.org_id = p_org AND u.workspace_id = p_workspace_id AND u.user_id = p_user_id;
  SELECT count(*) FILTER (WHERE r.epoch = p_cutover_epoch),
         min(r.version) FILTER (WHERE r.epoch = p_cutover_epoch),
         max(r.version) FILTER (WHERE r.epoch = p_cutover_epoch),
         count(*) FILTER (WHERE r.epoch IS DISTINCT FROM p_cutover_epoch),
         coalesce(jsonb_agg(jsonb_build_array(r.version::text, r.digest, r.kind) ORDER BY r.version)
                  FILTER (WHERE r.epoch = p_cutover_epoch), '[]'::jsonb)
    INTO v_n, v_min, v_max, v_other, v_lineage
    FROM sessions.ingest_control_receipts AS r
   WHERE r.org_id = p_org AND r.workspace_id = p_workspace_id AND r.user_id = p_user_id;
  SELECT r.digest INTO v_tip
    FROM sessions.ingest_control_receipts AS r
   WHERE r.org_id = p_org AND r.workspace_id = p_workspace_id AND r.user_id = p_user_id
     AND r.epoch = p_cutover_epoch AND r.version = p_head_version;
  RETURN QUERY SELECT
    encode(sha256(convert_to(jsonb_build_array('owner-buffered-deletion-set-v1', p_org, p_workspace_id, p_user_id,
                                               v_deletions)::text, 'UTF8')), 'hex'),
    encode(sha256(convert_to(jsonb_build_array('owner-buffered-reset-set-v1', p_org, p_workspace_id, p_user_id,
                                               v_resets)::text, 'UTF8')), 'hex'),
    encode(sha256(convert_to(jsonb_build_array('owner-buffered-target-lineage-v1', p_org, p_workspace_id, p_user_id,
                                               p_cutover_epoch, p_head_version::text, p_head_digest,
                                               v_lineage)::text, 'UTF8')), 'hex'),
    v_gen,
    coalesce(v_other = 0 AND CASE
      WHEN p_head_version IS NULL THEN p_head_digest IS NULL AND v_n = 0
      ELSE v_n = p_head_version AND v_min = 1 AND v_max = p_head_version AND v_tip = p_head_digest
    END, false);
END
$parity_expected$;

-- The user pin and the gate. Inside SECURITY DEFINER the `role` setting still names the caller's
-- SET ROLE, and session_user names the login (session_directory_read_api_scope). A
-- transaction-local set_config reads '' on a pooled connection after its transaction ends, and
-- NULL where it was never set: both refuse.
CREATE FUNCTION sessions.owner_buffered_read_scope(
  p_workspace_id text, p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text,
  p_head_pending boolean
)
RETURNS TABLE(org_id text, workspace_id text, user_id text, gate_open boolean,
              admission_epoch text, admission_version bigint)
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_org text := nullif(current_setting('bulls96_read.org_id', true), '');
  v_workspace text := nullif(current_setting('bulls96_read.workspace_id', true), '');
  v_user text := nullif(current_setting('bulls96_read.user_id', true), '');
  v_has_head boolean := p_head_epoch IS NOT NULL OR p_head_version IS NOT NULL OR p_head_digest IS NOT NULL;
  v_well_formed boolean;
  v_version bigint;
  v_open boolean;
  pol sessions.ingest_lazy_policy%ROWTYPE;
  par sessions.owner_buffered_parity%ROWTYPE;
  x record;
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
  -- Shapes. CASE keeps the bigint range check behind the regex (no 22P02/22003 leak).
  v_well_formed := coalesce(p_cutover_epoch ~ '^[0-9a-f]{64}$', false) AND p_head_pending IS NOT NULL
    AND CASE WHEN NOT v_has_head THEN NOT p_head_pending
             ELSE coalesce(p_head_epoch ~ '^[0-9a-f]{64}$', false)
                  AND coalesce(p_head_digest ~ '^[0-9a-f]{64}$', false)
                  AND CASE WHEN p_head_version ~ '^[1-9][0-9]{0,18}$'
                           THEN p_head_version::numeric <= 9223372036854775807 ELSE false END
        END;
  IF NOT v_well_formed THEN
    RAISE EXCEPTION USING ERRCODE = '55006', MESSAGE = 'buffered read fenced',
      HINT = 'Malformed control head; the read API answers 503 buffered_fenced.';
  END IF;
  IF v_has_head THEN
    v_version := p_head_version::bigint;
  END IF;
  -- The gate (header, item 1). Plain snapshot reads, no lock.
  v_open := NOT p_head_pending AND (NOT v_has_head OR p_head_epoch = p_cutover_epoch);
  IF v_open THEN
    SELECT lp.* INTO pol FROM sessions.ingest_lazy_policy AS lp
     WHERE lp.singleton AND lp.valid_until > clock_timestamp();
    v_open := FOUND AND pol.control_epoch = p_cutover_epoch;
  END IF;
  IF v_open THEN
    SELECT bp.* INTO par FROM sessions.owner_buffered_parity AS bp
     WHERE bp.org_id = v_org AND bp.workspace_id = v_workspace AND bp.user_id = v_user;
    v_open := FOUND;
  END IF;
  IF v_open THEN
    SELECT e.* INTO x
      FROM sessions.owner_buffered_parity_expected(v_org, v_workspace, v_user, p_cutover_epoch, v_version,
                                                   p_head_digest) AS e;
    v_open := coalesce(x.receipts_ok AND x.target_generation IS NOT NULL
      AND par.cutover_epoch = p_cutover_epoch
      AND par.no_head = NOT v_has_head
      AND par.head_epoch IS NOT DISTINCT FROM p_head_epoch
      AND par.head_version IS NOT DISTINCT FROM v_version
      AND par.head_digest IS NOT DISTINCT FROM p_head_digest
      AND par.target_generation = x.target_generation
      AND par.deletion_set_digest = x.deletion_set_digest
      AND par.reset_set_digest = x.reset_set_digest
      AND par.target_lineage_digest = x.target_lineage_digest, false);
  END IF;
  RETURN QUERY SELECT v_org, v_workspace, v_user, coalesce(v_open, false), p_cutover_epoch, coalesce(v_version, 0::bigint);
END
$scope$;

-- The caller's own kept buffered sessions. One row per (source, session_id): first_ts/last_ts
-- are the acceptance (ingest_batches.committed_at) times of the oldest/newest buffered batch
-- holding a kept group of that session (the ledger has no transcript timestamps); stage_count/
-- bytes count the active stage objects holding one (bytes are whole objects: the ledger records
-- no per-group size). Ordered by last_ts DESC, session_id, source (COLLATE "C"); the cursor is
-- the last row's triple and a page holds the rows strictly after it. All three cursor values or
-- none. p_limit NULL is 50, otherwise clamped to 1..100. A closed gate (scope gate_open false)
-- returns no row. admission_epoch/admission_version (decimal text) echo the required admission
-- every returned row matched. Errors: 22023 (partial cursor) and the scope's 42501 / 55006. No row lock, no advisory lock, no write.
CREATE FUNCTION sessions.buffered_sessions_for(
  p_workspace text, p_cursor_ts timestamptz, p_cursor_session text, p_cursor_source text, p_limit integer,
  p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text, p_head_pending boolean
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
  v_source text := NULL;
  v_session text := NULL;
  v_limit integer := least(greatest(coalesce(p_limit, 50), 1), 100);
BEGIN
  SELECT sc.org_id, sc.workspace_id, sc.user_id, sc.gate_open, sc.admission_epoch, sc.admission_version
    INTO STRICT v_org, v_ws, v_user, v_open, v_adm_epoch, v_adm_version
    FROM sessions.owner_buffered_read_scope(p_workspace, p_cutover_epoch, p_head_epoch, p_head_version,
                                              p_head_digest, p_head_pending) AS sc;
  IF (p_cursor_ts IS NULL) <> (p_cursor_session IS NULL) OR (p_cursor_ts IS NULL) <> (p_cursor_source IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'owner buffered read: cursor needs ts, session and source together';
  END IF;
  IF NOT v_open THEN
    RETURN;
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (identical in buffered_sessions_for and buffered_session_stages_for;
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
    -- upload_resets count (gc_kept). v_source/v_session narrow to one session (detail only).
    -- ADMISSION WITNESS: the version is source_control_version on the stage's ORIGINAL claim
    -- attempt; the epoch is not stored on the claim and is derived from the immutable
    -- ingest_lazy_policy.control_epoch, valid only when the claim's source_identity_sha256 is
    -- the policy's. A NULL version, NULL identity, a mismatch or no policy hides the stage.
    -- The pair must equal the gate's required (v_adm_epoch, v_adm_version): no control of
    -- any kind since admission. GC ORIGINAL CLAIM: every ancestor of a GC replacement stage
    -- (parent_stage_token chain, bounded at 64) carries this same claim_token, so a
    -- replacement can never lend a newer claim's admission.
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

-- The stage rows of ONE owned, kept buffered session, oldest batch first: what the read API
-- needs to GET the object (stage_key, stage_sha256, stage_bytes; the object is a zstd frame
-- whose JSON header lists groups as [source, sessionId], src/session-ingest/staging.ts) and
-- to project only this session's kept lines: group_indexes are indexes into the object's
-- header.groups (= stage group_ordinals positions) and kept_ordinals are the original claim
-- ordinals (group_ordinals[g][j] names line j of group g) still kept. Same keep predicate and
-- scope as buffered_sessions_for. Not owned, absent, deleted, reset, redacted, not admitted at
-- the required (epoch, version), or a closed gate all answer the same empty result. Errors: 22023 (missing source/session) and the scope's.
CREATE FUNCTION sessions.buffered_session_stages_for(
  p_workspace text, p_source text, p_session_id text,
  p_cutover_epoch text, p_head_epoch text, p_head_version text, p_head_digest text, p_head_pending boolean
)
RETURNS TABLE(batch_id text, stage_key text, stage_sha256 text, stage_bytes bigint,
              group_indexes integer[], kept_ordinals integer[], reset_epoch bigint, accepted_at timestamptz,
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
                                              p_head_digest, p_head_pending) AS sc;
  IF v_source IS NULL OR char_length(v_source) NOT BETWEEN 1 AND 64
     OR v_session IS NULL OR char_length(v_session) NOT BETWEEN 1 AND 255 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'owner buffered read: source and session are required';
  END IF;
  IF NOT v_open THEN
    RETURN;
  END IF;
  RETURN QUERY
  -- keep-predicate:begin (identical in buffered_sessions_for and buffered_session_stages_for;
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
    -- upload_resets count (gc_kept). v_source/v_session narrow to one session (detail only).
    -- ADMISSION WITNESS: the version is source_control_version on the stage's ORIGINAL claim
    -- attempt; the epoch is not stored on the claim and is derived from the immutable
    -- ingest_lazy_policy.control_epoch, valid only when the claim's source_identity_sha256 is
    -- the policy's. A NULL version, NULL identity, a mismatch or no policy hides the stage.
    -- The pair must equal the gate's required (v_adm_epoch, v_adm_version): no control of
    -- any kind since admission. GC ORIGINAL CLAIM: every ancestor of a GC replacement stage
    -- (parent_stage_token chain, bounded at 64) carries this same claim_token, so a
    -- replacement can never lend a newer claim's admission.
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
         array_agg(k.group_index ORDER BY k.group_index),
         (SELECT array_agg(o ORDER BY o) FROM kept AS k2 CROSS JOIN LATERAL unnest(k2.ords) AS o
           WHERE k2.stage_token = st.stage_token AND k2.source = v_source AND k2.session_id = v_session),
         st.reset_epoch, st.accepted_at, v_adm_epoch, v_adm_version::text
    FROM kept AS k
    JOIN stg AS st ON st.stage_token = k.stage_token
   WHERE k.source = v_source AND k.session_id = v_session
   GROUP BY st.stage_token, st.batch_id, st.stage_key, st.stage_sha256, st.stage_bytes, st.reset_epoch, st.accepted_at
   ORDER BY st.accepted_at, st.batch_id COLLATE "C", st.stage_key COLLATE "C";
END
$buffered_stages$;

REVOKE ALL ON FUNCTION sessions.owner_buffered_read_scope(text, text, text, text, text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.owner_buffered_parity_expected(text, text, text, text, bigint, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.buffered_sessions_for(text, timestamptz, text, text, integer, text, text, text, text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.buffered_session_stages_for(text, text, text, text, text, text, text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.buffered_sessions_for(text, timestamptz, text, text, integer, text, text, text, text, boolean)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.buffered_session_stages_for(text, text, text, text, text, text, text, boolean)
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
    'sessions.owner_buffered_read_scope(text, text, text, text, text, boolean)'::regprocedure,
    'sessions.owner_buffered_parity_expected(text, text, text, text, bigint, text)'::regprocedure,
    'sessions.buffered_sessions_for(text, timestamptz, text, text, integer, text, text, text, text, boolean)'::regprocedure,
    'sessions.buffered_session_stages_for(text, text, text, text, text, text, text, boolean)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api'
                  AND v_fn IN ('sessions.buffered_sessions_for(text, timestamptz, text, text, integer, text, text, text, text, boolean)'::regprocedure,
                               'sessions.buffered_session_stages_for(text, text, text, text, text, text, text, boolean)'::regprocedure))
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
