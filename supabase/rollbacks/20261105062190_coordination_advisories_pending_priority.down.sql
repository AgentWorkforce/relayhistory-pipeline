-- Rolls back 20261105062190 (coordination advisories, priority-first pull reader): drops
-- sessions.coordination_advisories_pending_for_v2 and the ledger row. The function only reads: no table, row, index
-- or other function is touched, and 062000's pending_for_v1 stays as it is. The read API falls back to v1 on 42883,
-- so the pull path keeps working (oldest first) without a redeploy.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories priority reader: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062190_coordination_advisories_pending_priority.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories priority reader: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062190_coordination_advisories_pending_priority.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'a716ba376766022ad3503ffe76609ff469ed165a79a6bbd7d2b4a54631ea6845'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='coordination advisories priority reader: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.coordination_advisories_pending_for_v2(text,text,text,text,integer);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062190_coordination_advisories_pending_priority.sql';
COMMIT;
