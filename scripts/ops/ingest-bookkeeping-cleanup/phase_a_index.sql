-- Post-apply operator job for 20261105043600 (NOT a migration: CREATE INDEX CONCURRENTLY cannot run in the
-- migrator's transaction; same pattern as scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql).
-- Run outside a transaction, ONLY under Khaliq's explicit apply OK. sessions.ingest_bookkeeping_cleanup_*
-- refuses (55000) until this index exists, is valid and ready, AND its pg_get_indexdef is exactly
--   CREATE INDEX babysitter_evidence_index_claim_idx ON sessions.babysitter_evidence_index USING btree (claim_token)
-- No IF NOT EXISTS: a same-name index with another definition must make this FAIL (42P07), never be kept.
-- A failed CONCURRENTLY build leaves an INVALID index: DROP INDEX CONCURRENTLY it and retry.
SET lock_timeout='5s';
CREATE INDEX CONCURRENTLY babysitter_evidence_index_claim_idx ON sessions.babysitter_evidence_index(claim_token);
SELECT pg_get_indexdef(i.indexrelid)='CREATE INDEX babysitter_evidence_index_claim_idx ON sessions.babysitter_evidence_index USING btree (claim_token)'
  AND i.indisvalid AND i.indisready AS exact_and_ready
 FROM pg_index i WHERE i.indexrelid='sessions.babysitter_evidence_index_claim_idx'::regclass;
