-- Slot 20261105062320 (reserved, registry receipt 251934773687459840). Per-org admission hold: an enforced writer fence
-- for the fence-authority marker. Design: docs/design/org-admission-hold.md (revision 4, joint APPROVE at 892f089e).
-- Code only: applying it to any real database, installing a hold, or provisioning the mirror role need Khaliq's
-- explicit OK. Nothing here is reachable by a runtime role except through the entry checks below.
--
-- Objects:
--   sessions.ingest_org_admission_holds        one row per hold epoch; at most one non-released row per org.
--   sessions.ingest_org_admission_hold_events  insert-only audit, exactly one event per (epoch, kind).
--   Both tables avoid `org_id`/`workspace_id`/`user_id` columns on purpose: ingest_lazy_begin's bootstrap scan
--   (041000) and ingest_lazy_user treat any ingest_%/upload_% table with those columns as prior authority.
-- State machine (design section 6.1): held -> sealed -> marker_attempted -> released('bridge_complete');
--   held|sealed -> released('verified_rollback') only before any attempt. No other transition (guard trigger).
-- Key: pg_advisory_xact_lock[_shared](hashtextextended('96bulls:ingest-org-hold-v1:'||org, 0)), stable per org.
--   _check takes it SHARED, then reads the hold in a later statement (READ COMMITTED: a fresh snapshot after the
--   grant). Lifecycle functions take it EXCLUSIVE after the migrator's (961996,1) SHARED and a fresh slot check.
-- Refusals: RH042 'session ingest: org admission held' (retryable); 40001 outside READ COMMITTED.

CREATE TABLE sessions.ingest_org_admission_holds (
  epoch uuid PRIMARY KEY,
  held_org text NOT NULL CHECK (held_org ~ '^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$'),
  request_id uuid NOT NULL UNIQUE,
  state text NOT NULL CHECK (state IN ('held', 'sealed', 'marker_attempted', 'released')),
  installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  sealed_at timestamptz,
  marker_attempted_at timestamptz,
  released_at timestamptz,
  release_reason text CHECK (release_reason IN ('bridge_complete', 'verified_rollback')),
  release_evidence_sha256 text CHECK (release_evidence_sha256 ~ '^[0-9a-f]{64}$'),
  CHECK ((state = 'released') = (released_at IS NOT NULL)),
  CHECK ((state = 'released') = (release_reason IS NOT NULL AND release_evidence_sha256 IS NOT NULL)),
  CHECK (state NOT IN ('sealed', 'marker_attempted') OR sealed_at IS NOT NULL),
  CHECK (state <> 'marker_attempted' OR marker_attempted_at IS NOT NULL),
  CHECK (release_reason IS DISTINCT FROM 'bridge_complete' OR marker_attempted_at IS NOT NULL),
  CHECK (release_reason IS DISTINCT FROM 'verified_rollback' OR marker_attempted_at IS NULL)
);
CREATE UNIQUE INDEX ingest_org_admission_holds_live ON sessions.ingest_org_admission_holds (held_org)
  WHERE state <> 'released';

CREATE TABLE sessions.ingest_org_admission_hold_events (
  epoch uuid NOT NULL REFERENCES sessions.ingest_org_admission_holds (epoch),
  kind text NOT NULL CHECK (kind IN ('installed', 'sealed', 'marker_attempted', 'released')),
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  detail text CHECK (detail IS NULL OR detail ~ '^[a-z][a-z0-9_]{0,63}$'),
  evidence_sha256 text CHECK (evidence_sha256 IS NULL OR evidence_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (epoch, kind)
);

ALTER TABLE sessions.ingest_org_admission_holds ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_org_admission_hold_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_org_admission_holds, sessions.ingest_org_admission_hold_events
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;

-- Immutability: no DELETE or TRUNCATE of either table, no UPDATE of an event, and a hold row changes only along the
-- state machine with its identity (epoch, org, request, installed_at) and every earlier timestamp unchanged.
CREATE FUNCTION sessions.ingest_org_admission_hold_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_guard$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') OR TG_TABLE_NAME = 'ingest_org_admission_hold_events' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold history is immutable';
  END IF;
  IF NEW.epoch IS DISTINCT FROM OLD.epoch OR NEW.held_org IS DISTINCT FROM OLD.held_org
     OR NEW.request_id IS DISTINCT FROM OLD.request_id OR NEW.installed_at IS DISTINCT FROM OLD.installed_at
     OR (OLD.sealed_at IS NOT NULL AND NEW.sealed_at IS DISTINCT FROM OLD.sealed_at)
     OR (OLD.marker_attempted_at IS NOT NULL AND NEW.marker_attempted_at IS DISTINCT FROM OLD.marker_attempted_at)
     OR NOT ((OLD.state = 'held' AND NEW.state IN ('sealed', 'released'))
          OR (OLD.state = 'sealed' AND NEW.state IN ('marker_attempted', 'released'))
          OR (OLD.state = 'marker_attempted' AND NEW.state = 'released')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold transition refused';
  END IF;
  RETURN NEW;
END
$hold_guard$;
CREATE TRIGGER ingest_org_admission_holds_guard BEFORE UPDATE OR DELETE ON sessions.ingest_org_admission_holds
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_admission_hold_guard();
CREATE TRIGGER ingest_org_admission_holds_truncate BEFORE TRUNCATE ON sessions.ingest_org_admission_holds
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_admission_hold_guard();
CREATE TRIGGER ingest_org_admission_hold_events_guard BEFORE UPDATE OR DELETE ON sessions.ingest_org_admission_hold_events
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_admission_hold_guard();
CREATE TRIGGER ingest_org_admission_hold_events_truncate BEFORE TRUNCATE ON sessions.ingest_org_admission_hold_events
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_org_admission_hold_guard();

-- The stable per-org key. Its namespace literal is used by nothing else (design section 4).
CREATE FUNCTION sessions.ingest_org_admission_hold_key(p_org text) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog, pg_temp AS $hold_key$
  SELECT hashtextextended('96bulls:ingest-org-hold-v1:' || p_org, 0)
$hold_key$;

-- The entry check. p_class:
--   'admission'   refused while the org has any non-released hold;
--   'publication' refused while it is sealed or marker_attempted;
--   'control'     refused while sealed or marker_attempted unless session_user is a member of the NOLOGIN role
--                 bulls96_session_ingest_mirror (the Worker's mirrored control login). If that role does not exist,
--                 every control is refused in those states (fail closed); this slot creates no role;
--   'erasure'     never refused: takes the key SHARED only, so a release (EXCLUSIVE) serializes with it.
CREATE FUNCTION sessions.ingest_org_admission_hold_check(p_org text, p_class text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_check$
DECLARE v_state text;
BEGIN
  IF p_class IS NULL OR p_class NOT IN ('admission', 'publication', 'control', 'erasure') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org admission hold class invalid';
  END IF;
  IF p_org IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org admission hold org missing';
  END IF;
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'session ingest: org admission hold requires read committed';
  END IF;
  PERFORM pg_advisory_xact_lock_shared(sessions.ingest_org_admission_hold_key(p_org));
  IF p_class = 'erasure' THEN RETURN; END IF;
  -- A later statement: under READ COMMITTED its snapshot is taken after the shared key was granted.
  SELECT h.state INTO v_state FROM sessions.ingest_org_admission_holds AS h
   WHERE h.held_org = p_org AND h.state <> 'released';
  IF NOT FOUND THEN RETURN; END IF;
  IF p_class = 'admission'
     OR (p_class = 'publication' AND v_state IN ('sealed', 'marker_attempted'))
     OR (p_class = 'control' AND v_state IN ('sealed', 'marker_attempted')
         AND NOT (EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bulls96_session_ingest_mirror')
                  AND pg_has_role(session_user, 'bulls96_session_ingest_mirror', 'MEMBER'))) THEN
    RAISE EXCEPTION USING ERRCODE = 'RH042', MESSAGE = 'session ingest: org admission held';
  END IF;
END
$hold_check$;

-- Lifecycle serialization with this slot's down (design section 6, "Schema down"): the generated down holds
-- (961996,1) EXCLUSIVE; every lifecycle function takes it SHARED first, then re-checks in a fresh statement that the
-- slot is still applied, so a call paused before its first write refuses after a down instead of writing unenforced.
CREATE FUNCTION sessions.ingest_org_admission_hold_lifecycle(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_lifecycle$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'session ingest: org admission hold requires read committed';
  END IF;
  IF p_org IS NULL OR p_org !~ '^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org admission hold org invalid';
  END IF;
  PERFORM pg_advisory_xact_lock_shared(961996, 1);
  IF to_regclass('sessions.__96bulls_migrations') IS NULL OR NOT EXISTS (
       SELECT 1 FROM sessions.__96bulls_migrations
        WHERE name = 'upstream:20261105062320_ingest_org_admission_fences.sql') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold slot not applied';
  END IF;
  PERFORM pg_advisory_xact_lock(sessions.ingest_org_admission_hold_key(p_org));
END
$hold_lifecycle$;

-- Install: waits for every in-flight transaction holding the org key SHARED, then refuses on any pending batch.
-- Idempotent by request_id (a lost response returns the same epoch, no second event).
CREATE FUNCTION sessions.ingest_org_admission_hold_install(p_org text, p_request uuid) RETURNS uuid
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_install$
DECLARE h sessions.ingest_org_admission_holds%ROWTYPE; v_epoch uuid;
BEGIN
  IF p_request IS NULL THEN RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org admission hold request missing'; END IF;
  PERFORM sessions.ingest_org_admission_hold_lifecycle(p_org);
  SELECT * INTO h FROM sessions.ingest_org_admission_holds WHERE request_id = p_request;
  IF FOUND THEN
    IF h.held_org <> p_org OR h.state = 'released' THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold request reused';
    END IF;
    RETURN h.epoch;
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.ingest_org_admission_holds WHERE held_org = p_org AND state <> 'released') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold already live';
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.ingest_batches WHERE org_id = p_org AND state = 'pending') THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold needs no pending batch';
  END IF;
  v_epoch := gen_random_uuid();
  INSERT INTO sessions.ingest_org_admission_holds (epoch, held_org, request_id, state) VALUES (v_epoch, p_org, p_request, 'held');
  INSERT INTO sessions.ingest_org_admission_hold_events (epoch, kind) VALUES (v_epoch, 'installed');
  RETURN v_epoch;
END
$hold_install$;

-- Seal: waits for every in-flight publication (they hold the key SHARED), then publication is refused.
-- Idempotent for the same epoch. The caller's gate binds the FULL run that must precede it.
CREATE FUNCTION sessions.ingest_org_admission_hold_seal(p_org text, p_epoch uuid, p_evidence text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_seal$
DECLARE h sessions.ingest_org_admission_holds%ROWTYPE;
BEGIN
  PERFORM sessions.ingest_org_admission_hold_lifecycle(p_org);
  SELECT * INTO h FROM sessions.ingest_org_admission_holds WHERE held_org = p_org AND state <> 'released';
  IF NOT FOUND OR h.epoch IS DISTINCT FROM p_epoch THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold epoch stale';
  END IF;
  IF h.state = 'sealed' THEN RETURN; END IF;
  IF h.state <> 'held' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold transition refused';
  END IF;
  UPDATE sessions.ingest_org_admission_holds SET state = 'sealed', sealed_at = clock_timestamp() WHERE epoch = p_epoch;
  INSERT INTO sessions.ingest_org_admission_hold_events (epoch, kind, evidence_sha256) VALUES (p_epoch, 'sealed', p_evidence);
END
$hold_seal$;

-- Marker attempt: committed before EVERY marker PUT attempt; from sealed (first) or marker_attempted (same epoch, no
-- event). After it, only bridge_complete releases. The caller's gate binds the final catch-up that must precede it.
CREATE FUNCTION sessions.ingest_org_admission_hold_marker_attempt(p_org text, p_epoch uuid, p_evidence text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_marker_attempt$
DECLARE h sessions.ingest_org_admission_holds%ROWTYPE;
BEGIN
  PERFORM sessions.ingest_org_admission_hold_lifecycle(p_org);
  SELECT * INTO h FROM sessions.ingest_org_admission_holds WHERE held_org = p_org AND state <> 'released';
  IF NOT FOUND OR h.epoch IS DISTINCT FROM p_epoch THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold epoch stale';
  END IF;
  IF h.state = 'marker_attempted' THEN RETURN; END IF;
  IF h.state <> 'sealed' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold transition refused';
  END IF;
  UPDATE sessions.ingest_org_admission_holds SET state = 'marker_attempted', marker_attempted_at = clock_timestamp()
   WHERE epoch = p_epoch;
  INSERT INTO sessions.ingest_org_admission_hold_events (epoch, kind, evidence_sha256)
    VALUES (p_epoch, 'marker_attempted', p_evidence);
END
$hold_marker_attempt$;

-- Release: 'verified_rollback' only from held or sealed with no attempt; 'bridge_complete' only from marker_attempted.
-- Idempotent for the same (epoch, reason, evidence); any other repeat or a stale epoch refuses.
CREATE FUNCTION sessions.ingest_org_admission_hold_release(p_org text, p_epoch uuid, p_reason text, p_evidence text,
  p_detail text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_release$
DECLARE h sessions.ingest_org_admission_holds%ROWTYPE;
BEGIN
  IF p_reason IS NULL OR p_reason NOT IN ('bridge_complete', 'verified_rollback') OR p_evidence IS NULL
     OR p_evidence !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: org admission hold release invalid';
  END IF;
  PERFORM sessions.ingest_org_admission_hold_lifecycle(p_org);
  SELECT * INTO h FROM sessions.ingest_org_admission_holds WHERE epoch = p_epoch AND held_org = p_org;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold epoch stale';
  END IF;
  IF h.state = 'released' THEN
    IF h.release_reason = p_reason AND h.release_evidence_sha256 = p_evidence THEN RETURN; END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold already released';
  END IF;
  IF (p_reason = 'bridge_complete' AND h.state <> 'marker_attempted')
     OR (p_reason = 'verified_rollback' AND (h.state NOT IN ('held', 'sealed') OR EXISTS (
          SELECT 1 FROM sessions.ingest_org_admission_hold_events WHERE epoch = p_epoch AND kind = 'marker_attempted'))) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'session ingest: org admission hold transition refused';
  END IF;
  UPDATE sessions.ingest_org_admission_holds
     SET state = 'released', released_at = clock_timestamp(), release_reason = p_reason, release_evidence_sha256 = p_evidence
   WHERE epoch = p_epoch;
  INSERT INTO sessions.ingest_org_admission_hold_events (epoch, kind, detail, evidence_sha256)
    VALUES (p_epoch, 'released', p_detail, p_evidence);
END
$hold_release$;

-- Backstop: a future inserter of batch rows that bypasses the entry checks is still refused. Not the primary
-- enforcement (design section 2).
CREATE FUNCTION sessions.ingest_org_admission_hold_batch_backstop() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $hold_batch_backstop$
BEGIN
  PERFORM sessions.ingest_org_admission_hold_check(NEW.org_id, 'admission');
  RETURN NEW;
END
$hold_batch_backstop$;
CREATE TRIGGER ingest_org_admission_hold_batch_backstop BEFORE INSERT ON sessions.ingest_batches
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_org_admission_hold_batch_backstop();

REVOKE ALL ON FUNCTION sessions.ingest_org_admission_hold_guard(), sessions.ingest_org_admission_hold_key(text),
  sessions.ingest_org_admission_hold_check(text, text), sessions.ingest_org_admission_hold_lifecycle(text),
  sessions.ingest_org_admission_hold_install(text, uuid), sessions.ingest_org_admission_hold_seal(text, uuid, text),
  sessions.ingest_org_admission_hold_marker_attempt(text, uuid, text),
  sessions.ingest_org_admission_hold_release(text, uuid, text, text, text),
  sessions.ingest_org_admission_hold_batch_backstop()
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;

-- ENTRY CHECKS (class 1, publication, control, erasure): appended below once the classification of every
-- runtime-granted function is complete (design section 2).
