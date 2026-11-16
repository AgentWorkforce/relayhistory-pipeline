-- Drops the 20261105062020 turns forward gates functions, triggers, tables and
-- ledger row. Guard, object removal and ledger removal are one transaction
-- under the migrator lock. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_identity_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062020_turns_forward_gates.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062020_turns_forward_gates.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha IS DISTINCT FROM '63ca3c75bd1b09ec32369e761446da6a592e57fd90aca6640e9fb6a5dbd8ff5a'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: own applied checksum required';END IF;
END $rollback_identity_guard$;

-- Freeze the privacy authority before inspecting its witnesses. The global erase lock serializes
-- operator_user_erase_step; the table locks also exclude ordinary selection/re-inclusion writes
-- that do not take that global lock. They are acquired before the witness query, so READ COMMITTED
-- cannot approve rollback and then observe a witness disappear while waiting for the DROP locks.
SELECT pg_advisory_xact_lock(961996,50000);
LOCK TABLE sessions.turn_session_fences,sessions.turn_writers,
 sessions.upload_deletions,sessions.operator_user_erasures IN SHARE ROW EXCLUSIVE MODE;

DO $rollback_privacy_guard$ BEGIN
 -- A live row is derivable only while an ordinary-origin upload_deletions witness remains. Whole-user
 -- erase can intentionally remove that witness while retaining the permanent logical fence, so an
 -- orphaned live row, imported Neon evidence, a registered writer, or its durable registration
 -- witness makes rollback refuse.
 IF EXISTS(SELECT 1 FROM sessions.turn_session_fences WHERE fence_basis='legacy_import')
    OR EXISTS(SELECT 1 FROM sessions.turn_session_fences WHERE registered_writer_seen)
    OR EXISTS(SELECT 1 FROM sessions.turn_session_fences f WHERE f.fence_basis='live'
      AND NOT EXISTS(SELECT 1 FROM sessions.upload_deletions d
       WHERE d.org_id=f.org_id AND d.workspace_id=f.workspace_id
        AND d.source=f.source AND d.session_id=f.session_id
        AND d.origin_id<>'relayhistory:conversation_turns:v1'))
    OR EXISTS(SELECT 1 FROM sessions.turn_writers)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns forward gates: privacy state must be preserved';END IF;
END $rollback_privacy_guard$;
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
