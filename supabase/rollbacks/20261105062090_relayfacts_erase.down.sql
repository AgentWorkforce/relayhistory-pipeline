-- Drops the 20261105062090 relayfacts erase: the fence guard trigger on sessions.relayfacts_evidence_v1,
-- its function, sessions.relayfacts_evidence_erase_session_v1 and the ledger row. It adds no table, so nothing
-- else is removed. Evidence already erased is not restored; after this, late evidence writes for an erased
-- session are no longer refused.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts erase: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062090_relayfacts_erase.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts erase: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062090_relayfacts_erase.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'8225a8e85863d42e504d05fafd367528cf1fee9769fdf883169de3e59d598f7a'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='relayfacts erase: own applied checksum required';END IF;
END $rollback_guard$;
DROP TRIGGER relayfacts_evidence_fence_guard ON sessions.relayfacts_evidence_v1;
DROP FUNCTION sessions.relayfacts_evidence_fence_guard_v1(),
 sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062090_relayfacts_erase.sql';
COMMIT;
