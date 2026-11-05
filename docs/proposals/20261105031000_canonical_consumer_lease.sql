-- UNALLOCATED SLOT pending layered-sessions-pipeline registry assignment.
-- Stem: canonical_consumer_lease. The supervised consumer's own tables,
-- separate from the 030000 ack/finish contract (stays signed-off).
SET LOCAL lock_timeout='5s';
CREATE TABLE sessions.canonical_feature_consumer_lease(
  partition text PRIMARY KEY
    CHECK (partition ~ '^[a-z0-9-]{4,64}$'),
  owner text NOT NULL CHECK (length(owner) BETWEEN 1 AND 128),
  heartbeat_at timestamptz NOT NULL);
CREATE TABLE sessions.canonical_feature_consumer_checkpoint(
  partition text PRIMARY KEY
    CHECK (partition ~ '^[a-z0-9-]{4,64}$'),
  rev bigint NOT NULL CHECK (rev >= 0));
CREATE TABLE sessions.canonical_feature_consumer_dead_letter(
  entry_revision bigint NOT NULL CHECK (entry_revision > 0),
  partition text NOT NULL CHECK (partition ~ '^[a-z0-9-]{4,64}$'),
  image_key text NOT NULL
    CHECK (image_key ~ '^hs1:[a-f0-9]{64}:[a-f0-9]{64}:.+$'),
  -- stable bounded poison code, never a raw exception message
  reason text NOT NULL CHECK (reason IN (
    'reference_invalid','tombstone_invalid','payload_invalid',
    'vector_invalid','payload_bound','batch_invalid','duplicate',
    'row_invalid','row_bound','image_page_changed',
    'image_descriptor_changed','corrupt_chain',
    'page_bytes_bound','page_rows_bound')),
  attempts int NOT NULL DEFAULT 1 CHECK (attempts > 0 AND attempts <= 100),
  dead_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (entry_revision, partition));
ALTER TABLE sessions.canonical_feature_consumer_lease
  ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.canonical_feature_consumer_dead_letter
  ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.canonical_feature_consumer_checkpoint
  ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.canonical_feature_consumer_lease,
  sessions.canonical_feature_consumer_checkpoint,
  sessions.canonical_feature_consumer_dead_letter
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Evidence is immutable: producer INSERTs+SELECTs dead letters but can never
-- DELETE or UPDATE them; lease gets the mutable set it needs.
GRANT INSERT,SELECT ON sessions.canonical_feature_consumer_dead_letter
  TO bulls96_canonical_feature_producer;
GRANT INSERT,SELECT,UPDATE ON sessions.canonical_feature_consumer_lease
  TO bulls96_canonical_feature_producer;
GRANT INSERT,SELECT,UPDATE ON sessions.canonical_feature_consumer_checkpoint
  TO bulls96_canonical_feature_producer;
CREATE POLICY consumer_lease_producer
  ON sessions.canonical_feature_consumer_lease
  TO bulls96_canonical_feature_producer USING (true) WITH CHECK (true);
CREATE POLICY consumer_dead_letter_producer
  ON sessions.canonical_feature_consumer_dead_letter
  TO bulls96_canonical_feature_producer USING (true) WITH CHECK (true);
CREATE POLICY consumer_checkpoint_producer
  ON sessions.canonical_feature_consumer_checkpoint
  TO bulls96_canonical_feature_producer USING (true) WITH CHECK (true);
