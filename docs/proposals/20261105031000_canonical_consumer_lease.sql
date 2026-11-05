-- UNALLOCATED SLOT pending layered-sessions-pipeline registry assignment.
-- Stem: canonical_consumer_lease. The supervised consumer's own tables,
-- separate from the 030000 ack/finish contract (stays signed-off).
SET LOCAL lock_timeout='5s';
CREATE TABLE sessions.canonical_feature_consumer_lease(
  partition text PRIMARY KEY,
  owner text NOT NULL,
  heartbeat_at timestamptz NOT NULL);
CREATE TABLE sessions.canonical_feature_consumer_dead_letter(
  entry_revision bigint PRIMARY KEY,
  image_key text NOT NULL,
  reason text NOT NULL,
  attempts int NOT NULL DEFAULT 1,
  dead_at timestamptz NOT NULL DEFAULT now());
REVOKE ALL ON sessions.canonical_feature_consumer_lease,
  sessions.canonical_feature_consumer_dead_letter
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT INSERT,SELECT,UPDATE,DELETE ON
  sessions.canonical_feature_consumer_lease,
  sessions.canonical_feature_consumer_dead_letter
  TO bulls96_canonical_feature_producer;
