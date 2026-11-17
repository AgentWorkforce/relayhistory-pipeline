-- Drops the 20261105062090 memory documents erase: the fence guard trigger on sessions.decision_evidence_v1,
-- its function, sessions.decision_evidence_erase_session_v1 and the ledger row. It adds no table, so nothing
-- else is removed. Evidence already erased is not restored; after this, late evidence writes for an erased
-- session are no longer refused.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='memory documents erase: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062090_memory_documents_erase.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='memory documents erase: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062090_memory_documents_erase.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'709513ab227b8a5388405fcada246e21cbc9f63389dfca5e631bd0501b136ac5'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='memory documents erase: own applied checksum required';END IF;
END $rollback_guard$;
DROP TRIGGER decision_evidence_fence_guard ON sessions.decision_evidence_v1;
DROP FUNCTION sessions.decision_evidence_fence_guard_v1(),
 sessions.decision_evidence_erase_session_v1(text,text,text,text,uuid);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062090_memory_documents_erase.sql';
COMMIT;
