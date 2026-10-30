-- PROPOSED, NOT CONTRACT. No ClickHouse or ObsessionDB schema, version or deployment
-- exists. Nothing in this harness runs this file and no result in this packet comes
-- from it. Syntax follows the ClickHouse text index docs for 26.2+, unverified on a
-- server. ngrams(3) is the candidate for ILIKE-equivalent substring search; a token
-- tokenizer would be cheaper but would not match `%depl%`.
--
-- Open: tenant isolation has no RLS equivalent here, so every read needs a row policy
-- or a mandatory (org_id, workspace_id) predicate, and the isolation checks must run
-- against it. Duplicate replicas need revision-based deduplication at read time.
CREATE TABLE session_events (
  org_id LowCardinality(String),
  workspace_id LowCardinality(String),
  harness LowCardinality(String),
  session_id String,
  event_id String,
  ts DateTime64(3, 'UTC'),
  revision UInt64,
  deleted UInt8 DEFAULT 0,
  content String,
  INDEX content_text content TYPE text(tokenizer = ngrams(3), preprocessor = lower(content))
) ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(ts)
ORDER BY (org_id, workspace_id, ts, session_id, event_id);
