-- DRAFT operator file, NOT a migration (CREATE INDEX CONCURRENTLY cannot run inside the migrator's
-- transaction; same pattern as scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql).
-- Run statement by statement, outside a transaction, ONLY under Khaliq's explicit apply OK.
-- Design: docs/design/ingest-bookkeeping-cleanup.md §5.3.
--
-- Phase A (decisions) needs only the first index: a small, off-ACK-path table.
-- Phase B (seats, custody) needs the remaining seven. They are the FK-side indexes the RI checks of a
-- seat/custody DELETE probe; none exists today. COST: the decisions index adds one btree insert per ACKed
-- record INSIDE the ACK transaction while the global seat-clock row is held FOR UPDATE, and the slot
-- current_seat_key index removes HOT for the slot update of every accepted ACK. pipeline-lead measures
-- ACK p99 with and without them (after 043500) before Phase B. A failed CONCURRENTLY build leaves an
-- INVALID index: DROP INDEX CONCURRENTLY it and retry; the cleanup functions refuse while any is invalid.
SET lock_timeout='5s';

-- Phase A
CREATE INDEX CONCURRENTLY IF NOT EXISTS babysitter_evidence_index_claim_idx
 ON sessions.babysitter_evidence_index(claim_token);

-- Phase B
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_lazy_ack_decisions_seat_key_idx
 ON sessions.ingest_lazy_ack_decisions(seat_key_sha256);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_lazy_native_slots_current_seat_idx
 ON sessions.ingest_lazy_native_slots(current_seat_key) WHERE current_seat_key IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_lazy_native_slots_current_legacy_idx
 ON sessions.ingest_lazy_native_slots(current_legacy_key) WHERE current_legacy_key IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_receipt_record_heads_winner_idx
 ON sessions.ingest_receipt_record_heads(winner_key_sha256) WHERE winner_key_sha256 IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_receipt_record_heads_legacy_idx
 ON sessions.ingest_receipt_record_heads(legacy_key_sha256) WHERE legacy_key_sha256 IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_receipt_ord_seats_seat_idx
 ON sessions.ingest_receipt_ord_seats(key_sha256);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_receipt_ord_seats_legacy_idx
 ON sessions.ingest_receipt_ord_seats(legacy_custody_key_sha256) WHERE legacy_custody_key_sha256 IS NOT NULL;
