-- The writer gate for backfill and adopted manifest epochs. Applying this to a
-- real database needs Khaliq's explicit OK; see runbooks/manifest-activation.md.
create table if not exists sessions.manifest_readiness (
  reader text not null,
  capability text not null,
  release_sha text not null,
  attested_at timestamptz not null,
  hmac text not null,
  primary key (reader, capability)
);

create table if not exists sessions.backfill_activation_ready (
  manifest_key text not null,
  grant_id text not null,
  plan_digest text not null,
  ready_at timestamptz not null default now(),
  primary key (manifest_key, grant_id)
);

-- The new queued stage. The span stages stay allowed where they were.
ALTER TABLE sessions.pipeline_events DROP CONSTRAINT pipeline_events_stage_check;
ALTER TABLE sessions.pipeline_events ADD CONSTRAINT pipeline_events_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'fetch'));
ALTER TABLE sessions.pipeline_dead_letters DROP CONSTRAINT pipeline_dead_letters_stage_check;
ALTER TABLE sessions.pipeline_dead_letters ADD CONSTRAINT pipeline_dead_letters_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'fetch'));
ALTER TABLE sessions.pipeline_jobs DROP CONSTRAINT pipeline_jobs_stage_check;
ALTER TABLE sessions.pipeline_jobs ADD CONSTRAINT pipeline_jobs_stage_check
  CHECK (stage IN ('object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation'));
