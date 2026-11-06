-- Rollback for registry slot 20261105047000 (portable search storage).
-- Disable every portable Postgres search composition first: this drops the derived
-- index and completeness state (rebuildable from canonical projection). The runtime
-- grants and trusted-backend policies on these tables go with the tables; the image
-- that still lists them in RUNTIME_APPLICATION_TABLE_PRIVILEGES must not migrate again.
-- Explicit transaction: guard, marker fence, drops and ledger removal are atomic.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NULL OR to_regclass('sessions.__96bulls_role_validation') IS NULL THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search storage: migration ledger/marker required';
  END IF;
  LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
  IF EXISTS (SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%'
              AND name COLLATE "C" > 'upstream:20261105047000_portable_search_storage.sql') THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search storage: a later migration is applied; roll it back first';
  END IF;
  UPDATE sessions.__96bulls_role_validation SET status='pending', updated_at=clock_timestamp()
   WHERE name='session_directory_search_worker';
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='portable search storage: validation marker required';
  END IF;
END
$rollback_guard$;
DROP POLICY session_directory_portable_read_api ON sessions.session_directory;
REVOKE SELECT (org_id, workspace_id, harness, session_id, event_projection_epoch, event_projection_ready)
  ON sessions.session_directory FROM bulls96_read_api;
DROP TABLE sessions.search_transcript_events;
DROP TABLE sessions.search_projection_state;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105047000_portable_search_storage.sql';
COMMIT;
