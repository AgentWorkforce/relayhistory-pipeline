-- Rolls back 20261105062170 (session intelligence workspace view readers): drops
-- sessions.session_intel_workspace_alerts_v1 and sessions.session_intel_workspace_claims_v1 and the ledger row.
-- The two functions only read: no table, row, index or other function is touched, so nothing is lost. Turn
-- READ_API_SESSION_INTEL_ENABLED off first: without 062170 GET /v1/session-intel/workspace answers 501
-- session_intel_not_migrated.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session intelligence workspace: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062170_session_intel_workspace.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session intelligence workspace: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062170_session_intel_workspace.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'0004f27029b3bb6e776e92433a9ee531ae00277e2587ccd5a356de7cf53065a5'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session intelligence workspace: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.session_intel_workspace_alerts_v1(text,text,integer),
 sessions.session_intel_workspace_claims_v1(text,text,text[],integer);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062170_session_intel_workspace.sql';
COMMIT;
