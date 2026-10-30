-- The columns and indexes the current Neon search reads, copied from
-- relayhistory-cloud 7343404: packages/relayhistory/src/db/schema.ts,
-- db/migrations/0048_session_brief_search.sql and CONCURRENT_INDEXES in
-- scripts/apply-neon-migration.mjs. A subset for a local stand-in, never run on Neon.
-- The embedding columns and HNSW indexes are left out: lexical only.
CREATE SCHEMA neon_baseline;

CREATE TABLE neon_baseline.convergence_events (
  org_id text NOT NULL, workspace_id text NOT NULL, machine_id text NOT NULL, source text NOT NULL,
  session_id text NOT NULL, kind text NOT NULL, event_id text NOT NULL, ts timestamptz NOT NULL,
  content text, task_title text, project_id text,
  PRIMARY KEY (org_id, machine_id, source, session_id, kind, event_id)
);
CREATE INDEX convergence_events_org_ts_idx ON neon_baseline.convergence_events (org_id, ts);
CREATE INDEX convergence_events_org_session_ts_idx ON neon_baseline.convergence_events (org_id, session_id, ts, event_id);
CREATE INDEX convergence_events_org_session_workspace_idx ON neon_baseline.convergence_events (org_id, session_id, workspace_id);
CREATE INDEX convergence_events_task_title_direct_trgm_idx ON neon_baseline.convergence_events
  USING gin (task_title public.gin_trgm_ops) WITH (fastupdate = off) WHERE task_title IS NOT NULL;
CREATE INDEX convergence_events_content_prefix_trgm_idx ON neon_baseline.convergence_events
  USING gin (left(content, 4096) public.gin_trgm_ops) WITH (fastupdate = off);

CREATE TABLE neon_baseline.conversation_turns (
  org_id text NOT NULL, workspace_id text, session_id text NOT NULL
);
CREATE INDEX conversation_turns_session ON neon_baseline.conversation_turns (org_id, session_id);

CREATE TABLE neon_baseline.session_brief_search (
  org_id text NOT NULL, session_id text NOT NULL, workspace_id text, source text NOT NULL,
  evidence_workspace_ids jsonb NOT NULL DEFAULT '[]',
  title text NOT NULL, description text NOT NULL, brief_completed_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, session_id)
);
CREATE INDEX session_brief_search_recent_idx ON neon_baseline.session_brief_search (org_id, brief_completed_at DESC);
CREATE INDEX session_brief_search_title_trgm_idx ON neon_baseline.session_brief_search
  USING gin (title public.gin_trgm_ops) WITH (fastupdate = off);
CREATE INDEX session_brief_search_description_trgm_idx ON neon_baseline.session_brief_search
  USING gin (description public.gin_trgm_ops) WITH (fastupdate = off);
