-- Synthetic schema for the unwired adapter. No production migration or grants.
CREATE SCHEMA IF NOT EXISTS sessions;
CREATE TABLE sessions.publication_retry_checkpoints (
  scope text PRIMARY KEY CHECK (scope ~ '^[a-f0-9]{64}$'),
  version bigint NOT NULL CHECK (version BETWEEN 1 AND 9007199254740991),
  scan jsonb NOT NULL CHECK (jsonb_typeof(scan)='object'),
  retry_turn boolean NOT NULL
);
CREATE TABLE sessions.publication_receipt_retries (
  scope text NOT NULL REFERENCES sessions.publication_retry_checkpoints(scope),
  stage_token uuid NOT NULL,
  attempts integer NOT NULL CHECK (attempts BETWEEN 1 AND 6),
  state text NOT NULL CHECK (state='audit' OR (state='exhausted' AND attempts=6) OR (state='retry' AND attempts<6)),
  due_ms bigint NOT NULL CHECK (due_ms BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY (scope,stage_token)
);
CREATE INDEX publication_receipt_retries_due ON sessions.publication_receipt_retries(scope,due_ms,stage_token)
  WHERE state='retry';
