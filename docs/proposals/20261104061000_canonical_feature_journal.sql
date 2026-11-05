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

-- Companion producer-side state — REVISION-keyed page-chain ledger.
-- A retire's image_key equals its prior publish's, so progress identity is
-- the unique journal revision (kind+image bound), never image_key alone.
CREATE TABLE sessions.canonical_feature_page_ack(
  entry_revision bigint NOT NULL
    REFERENCES sessions.canonical_feature_journal(revision),
  kind text NOT NULL CHECK (kind IN ('publish','retire')),
  pool text NOT NULL CHECK (pool IN ('event','brief')),
  cursor text NOT NULL,
  next_cursor text,                     -- NULL = terminal page
  page_sha256 text NOT NULL CHECK (page_sha256 ~ '^[0-9a-f]{64}$'),
  acked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (entry_revision, pool, cursor));
-- Page state: one dirty row per journal entry; finish CAS binds the captured
-- authority/profile and refuses on drift or a second finish.
CREATE TABLE sessions.canonical_feature_copy_state(
  entry_revision bigint PRIMARY KEY
    REFERENCES sessions.canonical_feature_journal(revision),
  kind text NOT NULL CHECK (kind IN ('publish','retire')),
  image_key text NOT NULL,
  source_authority jsonb NOT NULL, pointer_set jsonb NOT NULL,
  profile_binding text NOT NULL,
  dirty boolean NOT NULL DEFAULT true);
REVOKE ALL ON sessions.canonical_feature_page_ack,
  sessions.canonical_feature_copy_state
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Producer progress role: SELECT on journal, write ONLY the progress tables
-- (never the journal or creator authority). OWNER-ONLY RPC boundary pending.
GRANT SELECT ON sessions.canonical_feature_journal
  TO bulls96_canonical_feature_producer;
GRANT INSERT,SELECT,UPDATE(dirty) ON sessions.canonical_feature_page_ack,
  sessions.canonical_feature_copy_state
  TO bulls96_canonical_feature_producer;

-- Server-side finish: verifies BOTH contiguous page chains (initial cursor ->
-- terminal), the exact journal entry, and unchanged authority/profile, then
-- CAS dirty->complete in one transaction. A skipped or misordered page fails
-- the chain check — a partial image is never marked done.
CREATE OR REPLACE FUNCTION sessions.finish_canonical_copy(
  p_revision bigint,
  p_source_authority jsonb,
  p_profile_binding text
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path='' AS $finish$
DECLARE
  v_st sessions.canonical_feature_copy_state%ROWTYPE;
  v_pool text;
  v_prev_next text;
  v_last text;
  v_terminal_ok boolean;
BEGIN
  SELECT * INTO v_st FROM sessions.canonical_feature_copy_state
    WHERE entry_revision=p_revision FOR UPDATE;
  IF NOT FOUND THEN RETURN false; END IF;
  IF NOT v_st.dirty THEN RETURN false; END IF;
  IF v_st.source_authority IS DISTINCT FROM p_source_authority
     OR v_st.profile_binding IS DISTINCT FROM p_profile_binding
  THEN RETURN false; END IF;

  -- Each pool's chain must be contiguous: page[i].next_cursor = page[i+1].cursor
  -- and the LAST page is the terminal (next_cursor IS NULL).
  FOR v_pool IN SELECT pool FROM (VALUES('event'),('brief'))p(pool)
  LOOP
    v_prev_next := NULL;
    v_terminal_ok := false;
    FOR v_last IN
      SELECT cursor,next_cursor FROM sessions.canonical_feature_page_ack
        WHERE entry_revision=p_revision AND pool=v_pool ORDER BY cursor
    LOOP
      IF v_prev_next IS DISTINCT FROM NULL
         AND v_last IS DISTINCT FROM v_prev_next
      THEN RETURN false; END IF;          -- a gap/skipped page
      v_prev_next := v_last;
    END LOOP;
    -- Terminal: the last page has next_cursor IS NULL.
    SELECT count(*)=0 INTO v_terminal_ok
      FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND pool=v_pool AND next_cursor IS NOT NULL
        AND next_cursor NOT IN (SELECT cursor FROM sessions.canonical_feature_page_ack
          WHERE entry_revision=p_revision AND pool=v_pool)
        AND next_cursor IS NOT NULL;
    IF NOT v_terminal_ok THEN RETURN false; END IF;
  END LOOP;
  UPDATE sessions.canonical_feature_copy_state SET dirty=false
    WHERE entry_revision=p_revision;
  RETURN true;
END;
$finish$;
