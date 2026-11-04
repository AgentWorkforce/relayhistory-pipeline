-- 040610 proposal ONLY — DRAFT, not reserved. Coordinate Root/Native before
-- moving to supabase/migrations/. Durable canonical feature journal: the only
-- input the producer trusts for publication + retirement ordering.
-- Metadata-only; no payload storage, no transcript bytes, no role grants.
CREATE TABLE sessions.canonical_feature_journal (
  revision bigint PRIMARY KEY,             -- managed counter, seed > legacy
  kind text NOT NULL CHECK (kind IN ('seed','publish','retire')),
  schema_generation text NOT NULL,         -- same-generation CAS fence
  image_key text NOT NULL,                 -- publication image identity
  source_hash char(64) NOT NULL,           -- image content CAS
  prior_keyset_hash char(64),              -- retire: exact prior keyset
  recorded_at timestamptz NOT NULL DEFAULT now()
);
-- The managed counter is the table's own max(revision): a single-writer
-- allocation (caller in the same transaction) writes last+1 under a row lock.
-- seed() writes the legacy high-water once so the counter starts ABOVE the
-- last legacy feature clock — never overlapping a legacy-issued revision.
CREATE INDEX canonical_feature_journal_kind_rev
  ON sessions.canonical_feature_journal (kind, revision);
