-- Drops the 20261105062080 memory documents writers and readers, its two tables
-- (sessions.decision_evidence_v1 and sessions.decision_records_v1, with their indexes) and its ledger row.
-- Guard, object removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='memory documents: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062080_memory_documents.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='memory documents: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062080_memory_documents.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'667efc3fe334e826f97317e00ebe0887ae46f165e136c99e5fb4d162d7357d05'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='memory documents: own applied checksum required';END IF;
END $rollback_guard$;
DROP FUNCTION sessions.decision_records_insert_v1(text,text,jsonb),
 sessions.decision_records_supersede_v1(text,text,uuid,uuid),
 sessions.decision_records_topics_v1(text,text),
 sessions.decision_records_list_v1(text,text,text,timestamptz,uuid,integer);
DROP TABLE sessions.decision_evidence_v1;
DROP TABLE sessions.decision_records_v1;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062080_memory_documents.sql';
COMMIT;
