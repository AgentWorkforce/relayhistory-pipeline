-- SLOT 060000. Scrubbed, bounded long summary beside the short summary on sessions.session_directory,
-- and its read-role-only reader. Bob Costas already writes the long summary as an encrypted object
-- (summary/long.json.enc); this adds an optional, default-off HoopDb projection of the scrubbed text so
-- the read API can serve GET /v1/sessions/:id/summary?kind=long without the Locker.
--
-- Shape: six nullable columns on sessions.session_directory, not a separate table:
--   * the writer is the existing runtime role, which already holds table-level DML on session_directory
--     through the runtime application ACL (src/hoopdb/migrate-guards.ts). A new table would need a new
--     runtime ACL catalog entry, and the migrator's ACL install casts every catalog relation to regclass,
--     so a release-target run of this binary with this step held would fail on the absent table;
--   * every erase, tombstone and history invalidation path already rewrites this row and clears
--     short_summary in the same statement. The trigger below clears the long summary in that same row
--     write, so no erase function is replaced;
--   * tenancy and RLS are the row's own. `authenticated` reads session_directory only through the
--     column-level SELECT grant of 20261030020000, which does not name these columns. The column-level
--     REVOKE below is explicit and defensive only: a later table-level GRANT SELECT on session_directory
--     would still expose every column, these included. They are not part of search_text and not part
--     of the brief document (sessions.session_brief_document_json).
--
-- Invariants (checked by the database, repeated by src/hoopdb/session-long-summary.ts):
--   - hard length bound: char_length(long_summary) BETWEEN 1 AND 8000 code points;
--   - provenance all-or-none: scrub ruleset ('relayhistory-scrub'), scrub version (>= 1), producer source,
--     generated_at and updated_at (write time) are set exactly when the text is;
--   - never on a tombstone, and never without a short summary: the long summary is derived from the same
--     transcript, so any path that clears the short summary (erase, pointer tombstone, history
--     invalidation, a refused or non-ready short summary) clears the long one too. The trigger name sorts
--     after every other BEFORE trigger on the table (PostgreSQL fires them in name order), so it sees the
--     final short_summary of the row.
--
-- Reader: sessions.session_long_summary_for(workspace, harness, session_id), SECURITY DEFINER, EXECUTE for
-- bulls96_read_api only, tenant-bound by sessions.session_directory_read_api_scope exactly like
-- sessions.session_brief_summary_for (20261105044100). Invalid key: 22023. Managed history: 0A000.
--
-- Rollback: supabase/rollbacks/20261105060000_session_long_summary.down.sql (drops the reader, the trigger,
-- its function and the columns; the encrypted summary objects are untouched). Writing the long summary
-- does not touch projected_at or search_text, so the search index never re-reads a row for it.
--
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
ALTER TABLE sessions.session_directory
  ADD COLUMN long_summary text
    CONSTRAINT session_directory_long_summary_length
    CHECK (char_length(long_summary) BETWEEN 1 AND 8000),
  ADD COLUMN long_summary_scrub_ruleset text
    CONSTRAINT session_directory_long_summary_scrub_ruleset
    CHECK (long_summary_scrub_ruleset = 'relayhistory-scrub'),
  ADD COLUMN long_summary_scrub_version smallint
    CONSTRAINT session_directory_long_summary_scrub_version
    CHECK (long_summary_scrub_version >= 1),
  ADD COLUMN long_summary_source text
    CONSTRAINT session_directory_long_summary_source
    CHECK (long_summary_source ~ '^[a-z0-9][a-z0-9._/-]{0,63}$'),
  ADD COLUMN long_summary_generated_at timestamptz,
  ADD COLUMN long_summary_updated_at timestamptz,
  ADD CONSTRAINT session_directory_long_summary_provenance CHECK (
    (long_summary IS NULL) = (long_summary_scrub_ruleset IS NULL) AND
    (long_summary IS NULL) = (long_summary_scrub_version IS NULL) AND
    (long_summary IS NULL) = (long_summary_source IS NULL) AND
    (long_summary IS NULL) = (long_summary_generated_at IS NULL) AND
    (long_summary IS NULL) = (long_summary_updated_at IS NULL)
  ),
  ADD CONSTRAINT session_directory_long_summary_follows_short CHECK (
    long_summary IS NULL OR (deleted_at IS NULL AND short_summary IS NOT NULL)
  );

COMMENT ON COLUMN sessions.session_directory.long_summary IS
  'Scrubbed (relayhistory-scrub), bounded (8000 code points) long session summary. Written only by src/hoopdb/session-long-summary.ts behind SESSION_LONG_SUMMARY_PROJECTION_ENABLED; read only through sessions.session_long_summary_for. Never part of search_text.';

CREATE FUNCTION sessions.session_directory_long_summary_follows_short()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $long_summary$
BEGIN
  IF NEW.long_summary IS NOT NULL AND (NEW.deleted_at IS NOT NULL OR NEW.short_summary IS NULL) THEN
    NEW.long_summary := NULL;
    NEW.long_summary_scrub_ruleset := NULL;
    NEW.long_summary_scrub_version := NULL;
    NEW.long_summary_source := NULL;
    NEW.long_summary_generated_at := NULL;
    NEW.long_summary_updated_at := NULL;
  END IF;
  RETURN NEW;
END
$long_summary$;
-- "zz": fires after every other BEFORE INSERT/UPDATE row trigger on session_directory.
CREATE TRIGGER session_directory_zz_long_summary_follows_short
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_long_summary_follows_short();
REVOKE ALL ON FUNCTION sessions.session_directory_long_summary_follows_short() FROM PUBLIC;

-- Direct column reads stay closed to end-user and anonymous roles: the column-level grant of
-- 20261030020000 never names these columns. This REVOKE is explicit and defensive only; it does not
-- stop a later table-level GRANT SELECT on session_directory from exposing every column.
DO $long_summary_columns$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['anon','authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=role_name) THEN
      EXECUTE format('REVOKE ALL (long_summary, long_summary_scrub_ruleset, long_summary_scrub_version, long_summary_source, long_summary_generated_at, long_summary_updated_at) ON sessions.session_directory FROM %I', role_name);
    END IF;
  END LOOP;
END
$long_summary_columns$;

-- The trigger function remains owner-only, whatever the default ACLs grant.
DO $long_summary_trigger_acl$
DECLARE v_grantee text; v_fn regprocedure := 'sessions.session_directory_long_summary_follows_short()'::regprocedure;
BEGIN
 FOR v_grantee IN SELECT r.rolname FROM pg_catalog.pg_proc p
  CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
  JOIN pg_catalog.pg_roles r ON r.oid=a.grantee
  WHERE p.oid=v_fn AND a.privilege_type='EXECUTE' AND a.grantee<>p.proowner
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',v_fn,v_grantee); END LOOP;
END
$long_summary_trigger_acl$;

CREATE FUNCTION sessions.session_long_summary_for(
 p_workspace_id text, p_harness text, p_session_id text
) RETURNS TABLE(org_id text, workspace_id text, harness text, session_id text,
 long_summary text, source text, generated_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $long_summary_read$
DECLARE v_org text;
BEGIN
 v_org := sessions.session_directory_read_api_scope(p_workspace_id);
 IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
    OR p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='long summary: invalid session key';
 END IF;
 -- Same alias posture as the brief summary reader: one physical source per logical session.
 IF (SELECT count(DISTINCT p.session_id) FROM sessions.session_index p
     WHERE p.org_id=v_org AND p.workspace_id=p_workspace_id AND p.harness=p_harness
       AND coalesce(p.canonical_session_id,p.session_id)=p_session_id AND p.deleted_at IS NULL)>1 THEN
  RAISE EXCEPTION USING ERRCODE='0A000', MESSAGE='long summary: multiple physical aliases unsupported';
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.session_index a JOIN sessions.session_index p
   ON p.org_id=a.org_id AND p.workspace_id=a.workspace_id AND p.harness=a.harness AND p.session_id=a.session_id
   WHERE a.org_id=v_org AND a.workspace_id=p_workspace_id AND a.harness=p_harness
    AND coalesce(a.canonical_session_id,a.session_id)=p_session_id AND a.deleted_at IS NULL
    AND p.deleted_at IS NULL AND coalesce(p.canonical_session_id,p.session_id)<>p_session_id) THEN
  RAISE EXCEPTION USING ERRCODE='0A000', MESSAGE='long summary: inconsistent canonical mapping';
 END IF;
 -- Managed history needs qualified metadata, exactly as the brief summary reader.
 IF sessions.ingest_history_live() AND EXISTS(
  SELECT 1 FROM sessions.ingest_history_scope_current h
  WHERE h.org_id=v_org AND h.workspace_id=p_workspace_id AND h.harness=p_harness
   AND (h.session_id=p_session_id OR h.session_id IN (
    SELECT p.session_id FROM sessions.session_index p WHERE p.org_id=v_org
     AND p.workspace_id=p_workspace_id AND p.harness=p_harness
     AND coalesce(p.canonical_session_id,p.session_id)=p_session_id))) THEN
  RAISE EXCEPTION USING ERRCODE='0A000', MESSAGE='history: qualified long summary not ready';
 END IF;
 RETURN QUERY
 SELECT v_org, p_workspace_id, p_harness, p_session_id, d.long_summary, d.long_summary_source, d.long_summary_generated_at
 FROM sessions.session_directory d
 WHERE d.org_id=v_org AND d.workspace_id=p_workspace_id AND d.harness=p_harness AND d.session_id=p_session_id
  AND d.deleted_at IS NULL AND d.long_summary IS NOT NULL
  AND EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=v_org
    AND p.workspace_id=p_workspace_id AND p.harness=p_harness
    AND coalesce(p.canonical_session_id,p.session_id)=p_session_id AND p.deleted_at IS NULL);
END
$long_summary_read$;
REVOKE ALL ON FUNCTION sessions.session_long_summary_for(text,text,text)
 FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.session_long_summary_for(text,text,text) TO bulls96_read_api;
DO $revoke$
DECLARE v_grantee text; v_fn regprocedure := 'sessions.session_long_summary_for(text,text,text)'::regprocedure;
BEGIN
 FOR v_grantee IN SELECT r.rolname FROM pg_proc p
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
  JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=v_fn AND a.privilege_type='EXECUTE'
   AND a.grantee<>p.proowner AND r.rolname<>'bulls96_read_api'
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',v_fn,v_grantee); END LOOP;
END
$revoke$;
