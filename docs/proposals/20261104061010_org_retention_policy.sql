-- Retention policy table — SHARED contract, coordinate with migration-completeness.
-- Proposal only, no reservation. One per-org policy row; NULL = keep forever.
CREATE TABLE sessions.org_retention_policy (
  org_id text PRIMARY KEY,
  retention_days integer,            -- NULL = unlimited retention
  updated_at timestamptz NOT NULL DEFAULT now()
);
-- One retention job reads the policy and drives expiry through the reviewed
-- erasure path: Supabase rows via (org_id, recorded_at) sweeps, CH pool rows
-- via retention_days stamped at insert (TTL toDateTime(ts)+toIntervalDay),
-- S3 image objects via the durable pointer set.
