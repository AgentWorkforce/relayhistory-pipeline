-- DRAFT ClickHouse DDL for the buffered-stage search feed. Not applied anywhere; an operator applies it
-- (with Khaliq's OK) BEFORE INGEST_BUFFERED_SEARCH_FEED=on. Additive and idempotent: existing rows and
-- every writer that does not name the columns (the canonical indexer, the Neon tail) read as canonical.
-- The column names and types are the ones the read-side gate (#338, src/session-read-api/buffered-search-gate.ts)
-- reads. The vendored schema.ts is owned by relayhistory-cloud and is not edited here; the feeder checks
-- system.columns for exactly these definitions and refuses to write without them.
ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin LowCardinality(String) DEFAULT 'canonical';
ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin_claim_token String DEFAULT '';
