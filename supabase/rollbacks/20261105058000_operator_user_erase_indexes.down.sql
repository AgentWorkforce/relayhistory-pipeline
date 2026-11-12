-- Drops the fifteen operator-user-erase support indexes of 20261105058000 and its ledger row.
-- LOCAL/TEST ONLY. There are no down-migrations on real databases. On production the four large-table indexes
-- (and any other the packet prebuilt) are operator-built ones (CREATE INDEX CONCURRENTLY, adopted by the
-- migration); dropping them puts every slot/seat/stage delete back on per-row sequential scans. Never run against
-- a real database without the operator's explicit word.
-- DROP INDEX here is not CONCURRENTLY (it cannot be inside this transaction) and takes ACCESS EXCLUSIVE locks.
-- Explicit transaction: guard, drops and ledger removal are atomic. On an error the caller must ROLLBACK.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase indexes: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105058000_operator_user_erase_indexes.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase indexes: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105058000_operator_user_erase_indexes.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'fbbfe84622b64921a39a207a83d46bf11e1fce57847f7d2d455e8e01b21f50f3'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase indexes: own applied checksum required';END IF;
END $rollback_guard$;
-- Reverse build order; only the fifteen declared names.
DROP INDEX IF EXISTS sessions.ingest_receipt_record_heads_key_material_origin_idx;
DROP INDEX IF EXISTS sessions.ingest_receipt_record_heads_key_material_user_idx;
DROP INDEX IF EXISTS sessions.ingest_receipt_seats_key_material_origin_idx;
DROP INDEX IF EXISTS sessions.ingest_receipt_seats_key_material_user_idx;
DROP INDEX IF EXISTS sessions.ingest_lazy_native_slots_unowned_origin_idx;
DROP INDEX IF EXISTS sessions.ingest_history_plans_stage_token_idx;
DROP INDEX IF EXISTS sessions.ingest_buffer_outbox_active_stage_token_idx;
DROP INDEX IF EXISTS sessions.ingest_stage_intents_batch_idx;
DROP INDEX IF EXISTS sessions.ingest_stage_intents_gc_replacement_stage_token_idx;
DROP INDEX IF EXISTS sessions.ingest_stage_intents_parent_stage_token_idx;
DROP INDEX IF EXISTS sessions.ingest_lazy_ack_decisions_seat_key_sha256_idx;
DROP INDEX IF EXISTS sessions.ingest_lazy_ack_decisions_native_key_sha256_idx;
DROP INDEX IF EXISTS sessions.ingest_lazy_native_slots_current_seat_key_idx;
DROP INDEX IF EXISTS sessions.ingest_receipt_record_heads_winner_key_sha256_idx;
DROP INDEX IF EXISTS sessions.ingest_receipt_ord_seats_key_sha256_idx;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105058000_operator_user_erase_indexes.sql';
COMMIT;
