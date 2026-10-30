-- G6: a bounded, scrubbed short summary on the logical-session directory.
--
-- Khaliq approved (2026-10-30, docs/session-search-projection.md) a short summary
-- of at most 1,000 characters on sessions.session_directory, so Cloud and Relay
-- Desktop can search it remotely. Long summaries and transcripts stay in object
-- storage only. No embeddings, no pgvector, no tsvector over transcript content.
--
-- Invariants this migration enforces in the database (the TypeScript writer in
-- src/hoopdb/session-directory-summary.ts enforces the same ones first):
--   - hard length bound: char_length(short_summary) <= 1000;
--   - a stored summary always carries its scrub provenance (ruleset and version)
--     and its source and generation time; none of them exist without the text;
--   - a tombstoned row (deleted_at set) never holds a summary: a trigger clears it
--     and the search document, and a CHECK fails closed if anything bypasses it.
--
-- The tenant-scoped trigram index leads with (org_id, workspace_id); btree_gin is
-- what lets those scalar text keys sit in the same GIN index as the trigrams.
--
-- Code only. Nothing here is applied to a hosted database by this repository;
-- applying it needs Khaliq's go. The down path is
-- supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql.

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;

ALTER TABLE sessions.session_directory
  ADD COLUMN short_summary text
    CONSTRAINT session_directory_short_summary_length
    CHECK (char_length(short_summary) BETWEEN 1 AND 1000),
  -- Which lib/scrub.ts rules the text went through (raw-chunks SCRUB_RULESET and
  -- SCRUB_RULESET_VERSION; the same pair the manifest records as raw.scrub).
  ADD COLUMN short_summary_scrub_ruleset text
    CONSTRAINT session_directory_short_summary_scrub_ruleset
    CHECK (short_summary_scrub_ruleset = 'relayhistory-scrub'),
  ADD COLUMN short_summary_scrub_version smallint
    CONSTRAINT session_directory_short_summary_scrub_version
    CHECK (short_summary_scrub_version >= 1),
  -- The producer and its version, e.g. 'bob-costas/extractive/v1' (the same shape as
  -- session_summaries.model_id and session_verdicts.model_id).
  ADD COLUMN short_summary_source text
    CONSTRAINT session_directory_short_summary_source
    CHECK (short_summary_source ~ '^[a-z0-9][a-z0-9._/-]{0,63}$'),
  ADD COLUMN short_summary_generated_at timestamptz;

ALTER TABLE sessions.session_directory
  ADD CONSTRAINT session_directory_short_summary_provenance CHECK (
    (short_summary IS NULL) = (short_summary_scrub_ruleset IS NULL) AND
    (short_summary IS NULL) = (short_summary_scrub_version IS NULL) AND
    (short_summary IS NULL) = (short_summary_source IS NULL) AND
    (short_summary IS NULL) = (short_summary_generated_at IS NULL)
  ),
  ADD CONSTRAINT session_directory_short_summary_not_on_tombstone CHECK (
    deleted_at IS NULL OR short_summary IS NULL
  );

COMMENT ON COLUMN sessions.session_directory.short_summary IS
  'Scrubbed short summary, at most 1000 characters. Long summaries and transcripts stay in object storage. Cleared on tombstone; removed with the row on erasure.';
COMMENT ON COLUMN sessions.session_directory.search_text IS
  'Search document built by sessions.session_directory_search_text: metadata plus the scrubbed short_summary. MUST NOT contain transcript or event content.';

-- The one definition of the search document. The summary writer uses it, and the
-- future projector must too, so short_summary is always part of search_text.
-- Fields are trimmed, empty ones skipped, and the result held to the 8192 bound.
CREATE FUNCTION sessions.session_directory_search_text(
  p_title text, p_task_title text, p_agent_name text, p_project_id text,
  p_git_repo text, p_git_branch text, p_model text, p_short_summary text
) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $search_text$
  SELECT left(concat_ws(' ',
    nullif(btrim(p_title), ''), nullif(btrim(p_task_title), ''),
    nullif(btrim(p_agent_name), ''), nullif(btrim(p_project_id), ''),
    nullif(btrim(p_git_repo), ''), nullif(btrim(p_git_branch), ''),
    nullif(btrim(p_model), ''), nullif(btrim(p_short_summary), '')), 8192)
$search_text$;

-- Deletion propagation: a tombstone keeps the row addressable but holds no summary
-- text, in the column or in the search document that embedded it.
CREATE FUNCTION sessions.session_directory_tombstone_clears_summary()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $tombstone$
BEGIN
  IF NEW.deleted_at IS NOT NULL THEN
    NEW.short_summary := NULL;
    NEW.short_summary_scrub_ruleset := NULL;
    NEW.short_summary_scrub_version := NULL;
    NEW.short_summary_source := NULL;
    NEW.short_summary_generated_at := NULL;
    NEW.search_text := '';
  END IF;
  RETURN NEW;
END
$tombstone$;

CREATE TRIGGER session_directory_tombstone_clears_summary
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_tombstone_clears_summary();

-- Tenant-scoped summary substring search. Same partial predicate shape as the other
-- directory indexes: tombstones and rows without a summary are not indexed.
CREATE INDEX session_directory_summary_trgm_idx
  ON sessions.session_directory
  USING gin (org_id, workspace_id, short_summary public.gin_trgm_ops)
  WHERE short_summary IS NOT NULL AND deleted_at IS NULL;

REVOKE ALL ON FUNCTION sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_directory_tombstone_clears_summary() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text) TO service_role;
