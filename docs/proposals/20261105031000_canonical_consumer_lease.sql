-- UNALLOCATED SLOT pending layered-sessions-pipeline registry assignment.
-- Stem: canonical_consumer_lease. The supervised consumer's own tables,
-- separate from the 030000 ack/finish contract (stays signed-off).
SET LOCAL lock_timeout='5s';
CREATE TABLE sessions.canonical_feature_consumer_lease(
  partition text PRIMARY KEY,
  owner text NOT NULL,
  heartbeat_at timestamptz NOT NULL);
CREATE TABLE sessions.canonical_feature_consumer_dead_letter(
  entry_revision bigint PRIMARY KEY CHECK (entry_revision > 0),
  image_key text NOT NULL
    CHECK (image_key ~ '^hs1:[a-f0-9]{64}:[a-f0-9]{64}:.+$'),
  -- stable bounded poison code, never a raw exception message
  reason text NOT NULL
    CHECK (reason IN ('row_invalid','image_page_changed',
                      'image_descriptor_changed','row_bound')),
  attempts int NOT NULL DEFAULT 1 CHECK (attempts > 0 AND attempts <= 100),
  dead_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE sessions.canonical_feature_consumer_lease
  ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.canonical_feature_consumer_dead_letter
  ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.canonical_feature_consumer_lease,
  sessions.canonical_feature_consumer_dead_letter
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Evidence is immutable: producer INSERTs+SELECTs dead letters but can never
-- DELETE or UPDATE them; lease gets the mutable set it needs.
GRANT INSERT,SELECT ON sessions.canonical_feature_consumer_dead_letter
  TO bulls96_canonical_feature_producer;
GRANT INSERT,SELECT,UPDATE ON sessions.canonical_feature_consumer_lease
  TO bulls96_canonical_feature_producer;
CREATE POLICY consumer_lease_producer
  ON sessions.canonical_feature_consumer_lease
  TO bulls96_canonical_feature_producer USING (true) WITH CHECK (true);
CREATE POLICY consumer_dead_letter_producer
  ON sessions.canonical_feature_consumer_dead_letter
  TO bulls96_canonical_feature_producer USING (true) WITH CHECK (true);
