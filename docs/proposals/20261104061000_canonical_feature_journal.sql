-- 040610 proposal ONLY — DRAFT, not reserved. Coordinate Root/Native before
-- moving to supabase/migrations/. Durable canonical feature journal: the only
-- input the producer trusts for publication + retirement ordering.
-- Metadata-only; no payload storage, no transcript bytes, no role grants.
CREATE TABLE sessions.canonical_feature_journal (
  revision bigint PRIMARY KEY,             -- managed counter, seed > legacy
  kind text NOT NULL CHECK (kind IN ('seed','publish','retire')),
  org_id text NOT NULL,                    -- per-org erasure sweep key
  workspace_id text NOT NULL,              -- tenant-level sweep granularity
  scope jsonb NOT NULL,                    -- {orgId,workspaceId,harness,sessionId}
  schema_generation text NOT NULL,         -- same-generation CAS fence
  image_key text NOT NULL,                 -- publication image identity
  source_hash char(64) NOT NULL,           -- image content CAS
  prior_keyset_hash char(64),              -- retire: exact prior keyset
  source_authority jsonb NOT NULL,         -- publish: {epoch,fingerprint}
  prior_source_authority jsonb,            -- retire: same shape; null = new scope
  pointer_set jsonb NOT NULL,              -- {hash,count} inventory identity
  recorded_at timestamptz NOT NULL DEFAULT now()
);
-- The managed counter is the table's own max(revision): a single-writer
-- allocation (caller in the same transaction) writes last+1 under a row lock.
-- seed() writes the legacy high-water once so the counter starts ABOVE the
-- last legacy feature clock — never overlapping a legacy-issued revision.
CREATE INDEX canonical_feature_journal_kind_rev
  ON sessions.canonical_feature_journal (kind, revision);
-- Per-org retention sweep: one retention job reads (org_id, recorded_at),
-- expires rows through the reviewed erasure path, and retires the S3 pointer
-- set — no dangling image/pointer references.
CREATE INDEX canonical_feature_journal_org_time
  ON sessions.canonical_feature_journal (org_id, recorded_at);

-- Companion producer-side state (the consumer's own ledger — NOT the journal).
CREATE TABLE sessions.canonical_feature_page_ack(
  image_key text NOT NULL, pool text NOT NULL, cursor text NOT NULL,
  page_sha256 text NOT NULL,
  acked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (image_key, pool, cursor));
CREATE TABLE sessions.canonical_feature_copy_state(
  image_key text PRIMARY KEY, entry_revision text NOT NULL,
  terminal_event text, terminal_brief text,
  source_authority jsonb, profile_binding text,
  finished boolean NOT NULL DEFAULT false);
REVOKE ALL ON sessions.canonical_feature_page_ack,
  sessions.canonical_feature_copy_state
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
