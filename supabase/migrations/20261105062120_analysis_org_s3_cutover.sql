-- Slot 20261105062120. A scoped, per-org completeness proof for the session analysis (src/session-analysis):
-- condition (d) of loadSessionSource, beside (a) the legacy export's coverage marker, (b) the global
-- SESSION_ANALYSIS_ASSUME_NO_LEGACY_TURNS switch and (c) the census-sealed never-in-Neon proof
-- (046000 session_analysis_neon_free). Docs: docs/session-analysis-completeness.md.
--
-- Inert until an operator records a cutover: with no row in sessions.org_s3_cutover the proof is false for
-- every session, and the worker consults it only when SESSION_ANALYSIS_CUTOVER_PROOF=on. Code only:
-- applying this to any real database, and recording any cutover row, need Khaliq's separate, explicit OK.
--
-- sessions.org_s3_cutover: one explicit, audited attestation per org that BOTH Neon write paths are closed
-- for it, each with its own time:
--   uploads_cutover_at  session uploads moved to S3: the org's fence-authority marker is written and the
--                       Worker HTTP admission for the org stopped, both at or before this time;
--   turns_cutover_at    conversation turns are no longer written to Neon for the org (the cloud turns route
--                       forwards the org: TURNS_WRITE_PATH 'forward'), at or before this time.
-- cutover_at is the later of the two (generated), and is the only time the proof compares against. An org
-- without turns forwarding cannot be attested (the recorder and runbook refuse it; there is no partial row).
-- Shape: one row per org (PRIMARY KEY org_id). A trigger refuses DELETE and TRUNCATE for every role, the
-- owner included, and refuses every UPDATE except a correction made through the owner-only
-- sessions.org_s3_cutover_correct_v1, which only moves cutover_at LATER (stricter) unless its caller
-- passes p_khaliq_approved_lowering. Every correction first copies the prior row into the append-only
-- sessions.org_s3_cutover_history (same guard: no UPDATE, DELETE or TRUNCATE). The row and its history are
-- the audit record: who (recorded_by), when (recorded_at, the database clock), what was attested
-- (attestation, a fixed literal naming both conditions) and why (note). Lowering cutover_at or deleting a
-- row needs Khaliq's explicit approval (docs/session-analysis-completeness.md); deletion has no function
-- at all (runbook-only owner action). A correction retracts nothing already stored; with 20261105062130
-- applied it moves the source revision of the org's sessions, so earlier analyses are no longer reused or
-- published. org_id is text, as sessions.session_index.org_id is (the proof joins on it).
-- Owner-only: no runtime, read or admission role holds any privilege on it; only SECURITY DEFINER code
-- (the proof below, and session_analysis_source_revision from 062130) reads it, and the operator recorder (scripts/record-org-s3-cutover.ts) writes it on the owner
-- connection, as the Neon census and its seal are loaded.
--
-- sessions.session_analysis_cutover_free(org, workspace, harness, session, earliest_evidence_at): true iff
--   1. the org has a cutover row;
--   2. the scope (org, workspace, harness, session as the read API matches it: canonical id, else raw id) has
--      a live pointer;
--   3. the session's FIRST-SEEN time (sessions.session_analysis_first_seen_v1 below) is strictly after
--      cutover_at + 1 hour (the fixed clock-skew margin, a constant in the function body; operators add
--      their own margin on top). First-seen is the least session_index.created_at over EVERY pointer
--      anywhere in the org (any workspace, harness or machine; live and tombstoned) whose physical OR
--      canonical id is one of the session's ids (the scope's physical and canonical ids, and the requested
--      id). created_at is the session's first activity from its first manifest; every upsert keeps it with
--      least() (20261025000300), and a pointer move carries least(old, new) across its delete and re-insert
--      (src/session-pipeline/pointer.ts). It can still FALL (a new older pointer, possibly in another
--      workspace) or rise if a row is hard-deleted by an erase; 20261105062130 puts first-seen, cutover_at
--      and this verdict into session_analysis_source_revision so any such change invalidates reuse and
--      publication;
--   4. p_earliest_evidence_at (the earliest turn/event time in the canonical S3 snapshot the worker is about
--      to analyze) is non-null and strictly after cutover_at + 1 hour, so evidence older than that refuses
--      even when the pointer's own clock disagrees.
-- Both clocks are the client's: the proof rests on client clocks being within the 1 hour margin.
-- Why first activity, not the pointer's insert time: Neon receives turns when a client uploads them, which
-- is at or after their activity. A session whose first activity follows the later of the two cutovers
-- therefore had every upload, turn write included, after both Neon write paths were closed for its org: no
-- Neon-only turn or event can exist. A pointer's insert time cannot show that (an old Neon session whose
-- first S3 pointer lands after the cutover would pass). Any doubt is false. The proof never reads Neon.

CREATE TABLE sessions.org_s3_cutover (
  org_id text PRIMARY KEY CHECK (char_length(org_id) BETWEEN 1 AND 512),
  uploads_cutover_at timestamptz NOT NULL,
  turns_cutover_at timestamptz NOT NULL,
  cutover_at timestamptz GENERATED ALWAYS AS (greatest(uploads_cutover_at, turns_cutover_at)) STORED,
  attestation text NOT NULL
    CHECK (attestation = 'uploads_fenced_worker_admission_stopped+neon_turn_writes_forwarded'),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  recorded_by text NOT NULL CHECK (char_length(btrim(recorded_by)) BETWEEN 1 AND 128),
  note text CHECK (note IS NULL OR char_length(note) <= 1000),
  -- Operator-attested: when the pointer-move first-seen fix (one-transaction move keeping least
  -- created_at, src/hoopdb/index.ts rekeyFrom) was deployed in the org's session pipeline. Moves before it
  -- may have raised first-seen, so the effective cutover may not precede it.
  pointer_fix_deployed_at timestamptz NOT NULL,
  CONSTRAINT org_s3_cutover_after_pointer_fix
    CHECK (greatest(uploads_cutover_at, turns_cutover_at) >= pointer_fix_deployed_at),
  -- An attestation of the past: neither cutover may be later than the time it was recorded.
  CONSTRAINT org_s3_cutover_not_future CHECK (uploads_cutover_at <= recorded_at AND turns_cutover_at <= recorded_at)
);
ALTER TABLE sessions.org_s3_cutover ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.org_s3_cutover FROM PUBLIC, anon, authenticated, service_role;

-- recorded_at (and a history row's superseded_at) is the database clock, never the writer's: an
-- INSERT's value is overwritten.
CREATE FUNCTION sessions.org_s3_cutover_stamp_v1()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $cutover_stamp$
BEGIN
  IF TG_TABLE_NAME = 'org_s3_cutover_history' THEN
    NEW.superseded_at := clock_timestamp();
  ELSE
    NEW.recorded_at := clock_timestamp();
  END IF;
  RETURN NEW;
END
$cutover_stamp$;
CREATE TRIGGER org_s3_cutover_stamp
  BEFORE INSERT ON sessions.org_s3_cutover
  FOR EACH ROW EXECUTE FUNCTION sessions.org_s3_cutover_stamp_v1();

-- The prior version of every corrected row, append-only. Owner-only.
CREATE TABLE sessions.org_s3_cutover_history (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL,
  uploads_cutover_at timestamptz NOT NULL,
  turns_cutover_at timestamptz NOT NULL,
  cutover_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL,
  recorded_by text NOT NULL,
  note text,
  pointer_fix_deployed_at timestamptz NOT NULL,
  superseded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  superseded_by text NOT NULL,
  lowering_approved boolean NOT NULL
);
ALTER TABLE sessions.org_s3_cutover_history ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.org_s3_cutover_history FROM PUBLIC, anon, authenticated, service_role;

-- DELETE and TRUNCATE are always refused; UPDATE is refused unless sessions.org_s3_cutover_correct_v1 set
-- its transaction-local marker, and even then a LOWER cutover_at is refused unless the marker records
-- Khaliq's approval. Only the owner can write either table at all (no grants). The marker is a guard
-- against an accidental owner UPDATE, not against the owner (who can also disable the trigger): an owner
-- that sets 'approved_lowering' by hand is taking the Khaliq-gated action the runbook describes.
CREATE FUNCTION sessions.org_s3_cutover_immutable_v1()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $cutover_immutable$
DECLARE
  v_mode text := coalesce(current_setting('sessions.org_s3_cutover_correction', true), '');
BEGIN
  IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'org_s3_cutover' AND v_mode IN ('later', 'approved_lowering')
     AND NEW.org_id = OLD.org_id THEN
    -- NEW.cutover_at (a stored generated column) is not yet computed in a BEFORE trigger: compare the inputs.
    IF greatest(NEW.uploads_cutover_at, NEW.turns_cutover_at) IS NULL
       OR (greatest(NEW.uploads_cutover_at, NEW.turns_cutover_at) < OLD.cutover_at AND v_mode <> 'approved_lowering') THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = 'org s3 cutover: a correction may only move cutover_at later without Khaliq''s approval';
    END IF;
    RETURN NEW;
  END IF;
  RAISE EXCEPTION USING ERRCODE = '55000',
    MESSAGE = 'org s3 cutover: a recorded cutover is immutable (' || TG_OP || ' refused)';
END
$cutover_immutable$;
CREATE TRIGGER org_s3_cutover_immutable
  BEFORE UPDATE OR DELETE ON sessions.org_s3_cutover
  FOR EACH ROW EXECUTE FUNCTION sessions.org_s3_cutover_immutable_v1();
CREATE TRIGGER org_s3_cutover_no_truncate
  BEFORE TRUNCATE ON sessions.org_s3_cutover
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.org_s3_cutover_immutable_v1();
CREATE TRIGGER org_s3_cutover_history_immutable
  BEFORE UPDATE OR DELETE ON sessions.org_s3_cutover_history
  FOR EACH ROW EXECUTE FUNCTION sessions.org_s3_cutover_immutable_v1();
CREATE TRIGGER org_s3_cutover_history_stamp
  BEFORE INSERT ON sessions.org_s3_cutover_history
  FOR EACH ROW EXECUTE FUNCTION sessions.org_s3_cutover_stamp_v1();
CREATE TRIGGER org_s3_cutover_history_no_truncate
  BEFORE TRUNCATE ON sessions.org_s3_cutover_history
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.org_s3_cutover_immutable_v1();

-- The only correction path (scripts/record-org-s3-cutover.ts --correct). Owner-only (no grants; not
-- SECURITY DEFINER: it runs as its caller, and only the owner can write these tables). Copies the prior row
-- into the history, then sets the new times, recorded_by, recorded_at and note. The new cutover_at must be
-- at least the old one unless p_khaliq_approved_lowering is true (the trigger enforces the same). Returns
-- {"direction": "later" | "unchanged" | "lowered"}. Refuses (P0002) when the org has no row.
CREATE FUNCTION sessions.org_s3_cutover_correct_v1(
  p_org_id text, p_uploads_cutover_at timestamptz, p_turns_cutover_at timestamptz,
  p_recorded_by text, p_note text, p_khaliq_approved_lowering boolean)
RETURNS jsonb LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $cutover_correct$
DECLARE
  v_old sessions.org_s3_cutover%ROWTYPE;
  v_new timestamptz := greatest(p_uploads_cutover_at, p_turns_cutover_at);
  v_lower boolean;
BEGIN
  SELECT * INTO v_old FROM sessions.org_s3_cutover WHERE org_id = p_org_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'org s3 cutover: no recorded cutover to correct';
  END IF;
  v_lower := v_new IS NULL OR v_new < v_old.cutover_at;
  IF v_lower AND p_khaliq_approved_lowering IS NOT TRUE THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'org s3 cutover: a correction may only move cutover_at later without Khaliq''s approval';
  END IF;
  INSERT INTO sessions.org_s3_cutover_history (org_id, uploads_cutover_at, turns_cutover_at, cutover_at, recorded_at,
    recorded_by, note, pointer_fix_deployed_at, superseded_by, lowering_approved)
  VALUES (v_old.org_id, v_old.uploads_cutover_at, v_old.turns_cutover_at, v_old.cutover_at, v_old.recorded_at,
    v_old.recorded_by, v_old.note, v_old.pointer_fix_deployed_at, p_recorded_by, v_lower);
  PERFORM set_config('sessions.org_s3_cutover_correction', CASE WHEN v_lower THEN 'approved_lowering' ELSE 'later' END, true);
  UPDATE sessions.org_s3_cutover
     SET uploads_cutover_at = p_uploads_cutover_at, turns_cutover_at = p_turns_cutover_at,
         recorded_by = p_recorded_by, recorded_at = clock_timestamp(), note = p_note
   WHERE org_id = p_org_id;
  PERFORM set_config('sessions.org_s3_cutover_correction', '', true);
  RETURN jsonb_build_object('direction',
    CASE WHEN v_lower THEN 'lowered' WHEN v_new = v_old.cutover_at THEN 'unchanged' ELSE 'later' END);
END
$cutover_correct$;

-- First-seen of a session, as described above. Owner-only, not SECURITY DEFINER: called only by the
-- SECURITY DEFINER proof below and by session_analysis_source_revision (20261105062130). Two lookups, each
-- over live AND tombstoned rows: by physical id (si_session (org_id, session_id) serves it) and by
-- canonical id (served by the operator index in supabase/pending/20261105062120_session_index_org_canonical.sql;
-- without it that branch reads the org's pointers: slower, never different). A row whose canonical id is
-- null is matched by its physical id, so the union equals coalesce(canonical_session_id, session_id) or
-- session_id in the ids.
CREATE FUNCTION sessions.session_analysis_first_seen_v1(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS timestamptz LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $first_seen$
  WITH scope AS (
    SELECT p.session_id, p.canonical_session_id FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
  ),
  ids AS (
    SELECT scope.session_id AS id FROM scope
    UNION SELECT scope.canonical_session_id FROM scope WHERE scope.canonical_session_id IS NOT NULL
    UNION SELECT p_session_id
  )
  SELECT min(f.created_at) FROM (
    SELECT p.created_at FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.session_id IN (SELECT ids.id FROM ids)
    UNION ALL
    SELECT p.created_at FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.canonical_session_id IS NOT NULL AND p.canonical_session_id IN (SELECT ids.id FROM ids)
  ) AS f
$first_seen$;

-- Callers: the analysis worker only (service_role or the configured runtime role, pinned by
-- session_analysis_require_caller). The read API and the admission principal never call it.
-- The verdict is NOT monotone: it can flip back to false (a stricter correction, or a new older pointer of
-- the session in any workspace). 20261105062130 therefore adds cutover_at, first-seen and the verdict's
-- database half to session_analysis_source_revision, so a flip moves the revision and refuses reuse and
-- publication of an analysis made before it. Until 062130 is applied the switch must stay off. The
-- immutability trigger keeps a correction on the reviewed, audited path. The margin is the fixed
-- constant interval '1 hour'.
CREATE FUNCTION sessions.session_analysis_cutover_free(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text, p_earliest_evidence_at timestamptz)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $cutover_free$
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>']);
  RETURN coalesce((
  WITH cut AS (SELECT c.cutover_at FROM sessions.org_s3_cutover AS c WHERE c.org_id = p_org_id),
  scope AS (
    SELECT p.deleted_at FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id AND p.harness = p_harness
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
  ),
  seen AS (SELECT sessions.session_analysis_first_seen_v1(p_org_id, p_workspace_id, p_session_id) AS first_seen)
  SELECT p_earliest_evidence_at IS NOT NULL
     AND EXISTS (SELECT 1 FROM scope WHERE scope.deleted_at IS NULL)
     AND seen.first_seen IS NOT NULL
     AND seen.first_seen > cut.cutover_at + interval '1 hour'
     AND p_earliest_evidence_at > cut.cutover_at + interval '1 hour'
    FROM cut, seen), false);
END
$cutover_free$;

REVOKE ALL ON FUNCTION sessions.org_s3_cutover_immutable_v1() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.org_s3_cutover_stamp_v1() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_first_seen_v1(text, text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.org_s3_cutover_correct_v1(text, timestamptz, timestamptz, text, text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_cutover_free(text, text, text, text, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_analysis_cutover_free(text, text, text, text, timestamptz) TO service_role;

-- As 046000: strip EXECUTE from every grantee present at creation (default privileges), keeping only
-- service_role on the proof.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.org_s3_cutover_immutable_v1()'::regprocedure,
    'sessions.org_s3_cutover_stamp_v1()'::regprocedure,
    'sessions.session_analysis_first_seen_v1(text, text, text)'::regprocedure,
    'sessions.org_s3_cutover_correct_v1(text, timestamptz, timestamptz, text, text, boolean)'::regprocedure,
    'sessions.session_analysis_cutover_free(text, text, text, text, timestamptz)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'service_role'
                  AND v_fn = 'sessions.session_analysis_cutover_free(text, text, text, text, timestamptz)'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
