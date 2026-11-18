-- Slot 20261105062130. The source revision sees the analysis org S3 cutover proof (20261105062120, condition
-- (d), docs/session-analysis-completeness.md). Code only: applying it to any real database needs Khaliq's
-- separate, explicit OK. It must be applied before SESSION_ANALYSIS_CUTOVER_PROOF is ever turned on.
--
-- sessions.session_analysis_source_revision (046000) binds every input of the never-in-Neon proof, so a change
-- that could flip that proof refuses reuse and publication of an analysis made before it. The cutover proof's
-- database inputs were outside it: an older pointer of the same session in ANOTHER workspace (first-seen is
-- org-wide) or a corrected cutover row could flip the verdict to false while the earlier analysis kept being
-- reused. This CREATE OR REPLACE appends, only for an org with a recorded cutover, one element
-- ["org_s3_cutover_v1", cutover_at, first-seen (session_analysis_first_seen_v1), first-seen > cutover_at + 1h]
-- to the revision tuple. (The evidence half, the snapshot's earliest turn/event time, lives in the manifests
-- whose etags the tuple already binds.)
--
-- For an org WITHOUT a cutover row the tuple and the hash are byte-identical to 046000's, so applying this
-- invalidates no existing analysis. Recording an org's cutover (or correcting it) moves the revision of every
-- session of that org once: their earlier analyses are re-made on the next request, not reused.
-- CREATE OR REPLACE keeps the owner, SECURITY DEFINER and the existing EXECUTE grants (service_role and the
-- runtime role); nothing is granted or revoked here.

CREATE OR REPLACE FUNCTION sessions.session_analysis_source_revision(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS text LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $revision$
DECLARE
  v_live bigint;
  v_tuple jsonb;
  v_cutover timestamptz;
  v_first_seen timestamptz;
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>', 'bulls96_analysis_admit']);
  WITH ids AS (
    SELECT DISTINCT p.session_id FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
    UNION SELECT p_session_id)
  SELECT count(*) FILTER (WHERE p.deleted_at IS NULL), jsonb_build_array(
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
             ORDER BY p.manifest_key, p.harness, p.machine_id, p.created_at))
    INTO v_live, v_tuple
    FROM sessions.session_index AS p
   WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
     AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id);
  IF v_live = 0 THEN
    RETURN NULL;
  END IF;
  -- 062130: only for an org with a recorded S3 cutover. Without a row the tuple, and so the hash, is
  -- byte-identical to 046000's.
  SELECT c.cutover_at INTO v_cutover FROM sessions.org_s3_cutover AS c WHERE c.org_id = p_org_id;
  IF FOUND THEN
    v_first_seen := sessions.session_analysis_first_seen_v1(p_org_id, p_workspace_id, p_session_id);
    v_tuple := v_tuple || jsonb_build_array(jsonb_build_array('org_s3_cutover_v1', v_cutover, v_first_seen,
      coalesce(v_first_seen > v_cutover + interval '1 hour', false)));
  END IF;
  RETURN encode(sha256(convert_to(v_tuple::text, 'UTF8')), 'hex');
END
$revision$;
