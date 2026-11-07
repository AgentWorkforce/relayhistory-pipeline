-- Rollback for registry slot 20261105048000 (portable search reader contract).
-- Removes exactly what the up granted and created; the read API's portable completeness
-- check then fails closed (permission denied -> unavailable). Disable the portable reader
-- composition first. Explicit transaction: guard, marker fence, revokes and ledger removal
-- are atomic.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NULL OR to_regclass('sessions.__96bulls_role_validation') IS NULL THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search reader contract: migration ledger/marker required';
  END IF;
  LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
  IF EXISTS (SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%'
              AND name COLLATE "C" > 'upstream:20261105048000_portable_search_reader_contract.sql') THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search reader contract: a later migration is applied; roll it back first';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.__96bulls_migrations WHERE name='upstream:20261105048000_portable_search_reader_contract.sql') THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search reader contract: not applied';
  END IF;
  UPDATE sessions.__96bulls_role_validation SET status='pending', updated_at=clock_timestamp()
   WHERE name='session_directory_search_worker';
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search reader contract: validation marker required';
  END IF;
END
$rollback_guard$;
DROP FUNCTION sessions.portable_reader_readiness(text[], text);
DROP POLICY session_directory_portable_read_api ON sessions.session_directory;
DROP POLICY search_projection_state_read_api ON sessions.search_projection_state;
REVOKE SELECT (org_id, workspace_id, harness, session_id, event_projection_epoch, event_projection_ready)
  ON sessions.session_directory FROM bulls96_read_api;
REVOKE SELECT (backend_id, org_id, workspace_id, source, session_id, canonical_session_id,
               source_epoch, coverage_from_ms, complete)
  ON sessions.search_projection_state FROM bulls96_read_api;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105048000_portable_search_reader_contract.sql';
COMMIT;
