-- Drops the 20261105062020 turns forward gates functions, triggers, tables and
-- ledger row. Guard, object removal and ledger removal are one transaction
-- under the migrator lock. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062020_turns_forward_gates.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062020_turns_forward_gates.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha IS DISTINCT FROM '5d7ddb071a21eb7eb3be5c48b512260bf9fda61b5d14a4a073afea3422d57776'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: own applied checksum required';END IF;
 -- live rows are a derived cache of upload_deletions, including the bounded historical backfill;
 -- dropping them restores the pre-migration state. Imported frozen-Neon evidence is not derivable
 -- and registered writers may not be discarded, so either one makes rollback refuse.
 IF EXISTS(SELECT 1 FROM sessions.turn_session_fences WHERE fence_basis='legacy_import')
    OR EXISTS(SELECT 1 FROM sessions.turn_writers)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: privacy state must be preserved';END IF;
END $rollback_guard$;
DROP TRIGGER aa_session_directory_hide_turn_only ON sessions.session_directory;
DROP FUNCTION sessions.aa_session_directory_hide_turn_only();
DROP FUNCTION sessions.buffered_turn_sources_for(text,text,text,text,text,text,boolean,text,text);
DROP FUNCTION sessions.ingest_turn_fences_import(jsonb);
DROP FUNCTION sessions.ingest_turn_register(jsonb);
DROP FUNCTION sessions.ingest_turn_capability(jsonb);
DROP TRIGGER ingest_turn_capture_erasure ON sessions.upload_deletions;
DROP FUNCTION sessions.ingest_turn_capture_erasure();
DROP TRIGGER ingest_turn_capture_user_erasure ON sessions.operator_user_erasures;
DROP FUNCTION sessions.ingest_turn_capture_user_erasure();
DROP FUNCTION sessions.ingest_turn_materialize_fence(text,text,text,text);
DROP TABLE sessions.turn_writers;
DROP TABLE sessions.turn_session_fences;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062020_turns_forward_gates.sql';
COMMIT;
