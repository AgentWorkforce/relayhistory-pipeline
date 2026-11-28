-- T130 consumed-record membership (slot 062510). For each published native brief, the scoped
-- identities of the records its generator actually consumed (the final summary input, after
-- filtering and truncation), stored as SHA-256 digests, plus the exact pointer incarnation binding
-- the brief was published over. Written in the same transaction as the guarded brief (recordBrief);
-- removed with the brief: ON DELETE CASCADE from session_briefs, and a trigger drops it whenever
-- the brief's own fingerprint changes or its text is cleared (quarantine, scrub).
-- A read-role function answers one record at a time (member / not_member / unknown, or changed when
-- the caller's brief_binding is no longer the served brief) against the
-- served brief and its binding, never the list. Additive only: no change to existing objects'
-- bodies or grants, no backfill (legacy briefs have no evidence and answer unknown).

CREATE TABLE sessions.session_brief_evidence (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  brief_fingerprint text NOT NULL CHECK (brief_fingerprint ~ '^[0-9a-f]{64}$'),
  -- Per machine: manifest key, revision, raw digest, manifest ETag, user, bucket, provider and
  -- format, exactly as session_index held them under the publication guard.
  source_binding jsonb NOT NULL CHECK (jsonb_typeof(source_binding) = 'object'),
  member_count integer NOT NULL CHECK (member_count BETWEEN 0 AND 100000),
  complete boolean NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT session_brief_evidence_pkey PRIMARY KEY (org_id, workspace_id, harness, session_id),
  CONSTRAINT session_brief_evidence_brief FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.session_briefs (org_id, workspace_id, harness, session_id) ON DELETE CASCADE
);

CREATE TABLE sessions.session_brief_members (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  member_sha256 bytea NOT NULL CHECK (octet_length(member_sha256) = 32),
  CONSTRAINT session_brief_members_pkey PRIMARY KEY (org_id, workspace_id, harness, session_id, member_sha256),
  CONSTRAINT session_brief_members_evidence FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.session_brief_evidence (org_id, workspace_id, harness, session_id) ON DELETE CASCADE
);

ALTER TABLE sessions.session_brief_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_brief_members ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_brief_evidence FROM PUBLIC;
REVOKE ALL ON sessions.session_brief_members FROM PUBLIC;

-- Evidence never outlives the brief text it describes.
CREATE FUNCTION sessions.session_brief_evidence_retire()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $retire$
BEGIN
  IF NEW.brief_fingerprint IS DISTINCT FROM OLD.brief_fingerprint OR NEW.description IS NULL THEN
    DELETE FROM sessions.session_brief_evidence e
     WHERE e.org_id = OLD.org_id AND e.workspace_id = OLD.workspace_id
       AND e.harness = OLD.harness AND e.session_id = OLD.session_id;
  END IF;
  RETURN NEW;
END
$retire$;
CREATE TRIGGER session_brief_evidence_retire
  AFTER UPDATE ON sessions.session_briefs
  FOR EACH ROW EXECUTE FUNCTION sessions.session_brief_evidence_retire();
REVOKE ALL ON FUNCTION sessions.session_brief_evidence_retire() FROM PUBLIC;

-- One record's membership in the served brief. Authority, refusals and the served row are exactly
-- session_brief_summary_v2_for's (called here, so a refusal raises the same code). The evidence
-- counts only when it belongs to the served brief (same fingerprint) AND its binding equals the
-- live pointers now; otherwise unknown. not_member needs complete evidence.
CREATE FUNCTION sessions.session_brief_membership_for(
 p_workspace_id text, p_harness text, p_session_id text, p_record_id text, p_binding text
) RETURNS TABLE(org_id text, workspace_id text, harness text, session_id text,
 membership text, brief_revision bigint, source_revisions jsonb, brief_binding text)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $membership$
DECLARE v_served record; v_physical text; v_fp text; v_ev record; v_live jsonb; v_member boolean;
BEGIN
 IF p_record_id IS NULL OR char_length(p_record_id) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='brief membership: invalid record id';
 END IF;
 IF p_binding IS NOT NULL AND p_binding !~ '^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='brief membership: invalid binding';
 END IF;
 SELECT * INTO v_served FROM sessions.session_brief_summary_v2_for(p_workspace_id, p_harness, p_session_id);
 IF NOT FOUND THEN
  RETURN QUERY SELECT current_setting('bulls96_read.org_id', true), p_workspace_id, p_harness, p_session_id,
   (CASE WHEN p_binding IS NULL THEN 'unknown' ELSE 'changed' END)::text, NULL::bigint, NULL::jsonb, NULL::text;
  RETURN;
 END IF;
 -- The caller holds a different brief than the one served now: no definitive answer for it.
 IF p_binding IS NOT NULL AND p_binding <> v_served.brief_binding THEN
  RETURN QUERY SELECT v_served.org_id, p_workspace_id, p_harness, p_session_id, 'changed'::text,
   v_served.brief_revision, v_served.source_revisions, v_served.brief_binding;
  RETURN;
 END IF;
 -- v2 refused multiple physical aliases, so exactly one physical source serves this brief.
 SELECT DISTINCT p.session_id INTO v_physical FROM sessions.session_index p
  WHERE p.org_id=v_served.org_id AND p.workspace_id=p_workspace_id AND p.harness=p_harness
    AND coalesce(p.canonical_session_id,p.session_id)=p_session_id AND p.deleted_at IS NULL;
 SELECT b.brief_fingerprint INTO v_fp FROM sessions.session_briefs b
  WHERE b.org_id=v_served.org_id AND b.workspace_id=p_workspace_id AND b.harness=p_harness AND b.session_id=v_physical;
 SELECT * INTO v_ev FROM sessions.session_brief_evidence e
  WHERE e.org_id=v_served.org_id AND e.workspace_id=p_workspace_id AND e.harness=p_harness AND e.session_id=v_physical;
 SELECT jsonb_object_agg(p.machine_id, jsonb_build_object(
   'manifest_key', p.manifest_key, 'revision', p.revision, 'content_sha256', encode(p.content_sha256,'hex'),
   'manifest_etag', p.manifest_etag, 'user_id', p.user_id, 'bucket', p.bucket,
   'storage_provider', p.storage_provider, 'raw_format', p.raw_format))
  INTO v_live FROM sessions.session_index p
  WHERE p.org_id=v_served.org_id AND p.workspace_id=p_workspace_id AND p.harness=p_harness
    AND p.session_id=v_physical AND p.deleted_at IS NULL AND p.manifest_key IS NOT NULL;
 IF v_ev IS NULL OR v_fp IS NULL OR v_ev.brief_fingerprint <> v_fp OR v_ev.source_binding IS DISTINCT FROM v_live THEN
  RETURN QUERY SELECT v_served.org_id, p_workspace_id, p_harness, p_session_id, 'unknown'::text,
   v_served.brief_revision, v_served.source_revisions, v_served.brief_binding;
  RETURN;
 END IF;
 SELECT EXISTS(SELECT 1 FROM sessions.session_brief_members m
   WHERE m.org_id=v_served.org_id AND m.workspace_id=p_workspace_id AND m.harness=p_harness
     AND m.session_id=v_physical AND m.member_sha256=sha256(convert_to(p_record_id,'UTF8'))) INTO v_member;
 RETURN QUERY SELECT v_served.org_id, p_workspace_id, p_harness, p_session_id,
  CASE WHEN v_member THEN 'member' WHEN v_ev.complete THEN 'not_member' ELSE 'unknown' END,
  v_served.brief_revision, v_served.source_revisions, v_served.brief_binding;
END
$membership$;
REVOKE ALL ON FUNCTION sessions.session_brief_membership_for(text,text,text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.session_brief_membership_for(text,text,text,text,text) TO bulls96_read_api;
DO $revoke$
DECLARE v_grantee text; v_fn regprocedure;
BEGIN
 FOREACH v_fn IN ARRAY ARRAY['sessions.session_brief_membership_for(text,text,text,text,text)'::regprocedure,
                             'sessions.session_brief_evidence_retire()'::regprocedure] LOOP
  FOR v_grantee IN SELECT r.rolname FROM pg_proc p
   CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
   JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=v_fn AND a.privilege_type='EXECUTE'
    AND a.grantee<>p.proowner AND NOT (r.rolname='bulls96_read_api' AND v_fn='sessions.session_brief_membership_for(text,text,text,text,text)'::regprocedure)
  LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',v_fn,v_grantee); END LOOP;
 END LOOP;
END
$revoke$;
