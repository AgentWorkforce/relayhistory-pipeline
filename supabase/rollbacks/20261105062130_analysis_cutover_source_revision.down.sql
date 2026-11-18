-- Restores 20261105046000's sessions.session_analysis_source_revision body exactly (CREATE OR REPLACE keeps
-- its owner and grants) and removes the 20261105062130 ledger row. Turn SESSION_ANALYSIS_CUTOVER_PROOF off
-- first: without 062130 a flip of the cutover verdict no longer moves the revision.
-- For orgs without a cutover row the revision is unchanged by this; for orgs with one it moves back.
-- Guard, restore and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='cutover source revision: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062130_analysis_cutover_source_revision.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='cutover source revision: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062130_analysis_cutover_source_revision.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'6aa9ef4917e6a45960b9611b2243aba330dbd662860b7a146157ef8528be736d'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='cutover source revision: own applied checksum required';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.session_analysis_source_revision(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS text LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $revision$
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>', 'bulls96_analysis_admit']);
  RETURN (
  WITH ids AS (
    SELECT DISTINCT p.session_id FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
    UNION SELECT p_session_id)
  SELECT CASE WHEN count(*) FILTER (WHERE p.deleted_at IS NULL) = 0 THEN NULL ELSE encode(sha256(convert_to(jsonb_build_array(
           p_org_id, p_workspace_id, p_session_id,
           sessions.session_analysis_neon_free(p_org_id, p_workspace_id, p_session_id),
           (SELECT jsonb_build_array(c.sealed_at, c.manifest_sha256) FROM sessions.ingest_frozen_source_completeness AS c WHERE c.singleton),
           EXISTS (SELECT 1 FROM sessions.ingest_frozen_source_records AS r WHERE r.org_id = p_org_id AND r.session_id IN (SELECT session_id FROM ids)),
           (SELECT jsonb_build_array(c.sealed_at, c.census_sha256) FROM sessions.session_neon_census_seal AS c WHERE c.singleton),
           EXISTS (SELECT 1 FROM sessions.session_neon_census AS c WHERE c.org_id = p_org_id AND c.session_id IN (SELECT session_id FROM ids)),
           (SELECT f.purged_at FROM sessions.session_analysis_purge_fences AS f
             WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id AND f.session_id = p_session_id),
           jsonb_agg(jsonb_build_array(
             p.harness, coalesce(p.canonical_session_id, p.session_id),
             coalesce(p.canonical_machine_id, p.machine_id), p.machine_id, p.user_id,
             p.storage_provider, p.bucket, p.manifest_key, p.manifest_etag, p.revision,
             encode(p.content_sha256, 'hex'), p.raw_format, p.created_at, p.deleted_at)
             ORDER BY p.manifest_key, p.harness, p.machine_id, p.created_at))::text, 'UTF8')), 'hex') END
    FROM sessions.session_index AS p
   WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
     AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id));
END
$revision$;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062130_analysis_cutover_source_revision.sql';
COMMIT;
