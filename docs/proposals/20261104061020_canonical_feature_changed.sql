-- 040610 companion RPC — DRAFT proposal, coordinate before reserving.
-- OWNER-ONLY invoker run INSIDE the 620 final-CAS transaction (scope_install,
-- same-owner SECURITY DEFINER — no data-plane grant). Every input is
-- server-constructed. Rollback of the enclosing tx emits nothing.

CREATE OR REPLACE FUNCTION sessions.canonical_feature_changed(
  p_scope jsonb,
  p_source jsonb,
  p_prior_source jsonb,
  p_pointer_set jsonb,
  p_prior_pointer_set jsonb
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_revision bigint;
  v_uid text := '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$';
  v_hex text := '^[0-9a-f]{64}$';
BEGIN
  -- Exact shapes: scope carries the 64-hex ingest generation; source carries
  -- a mandatory UUID authorityId; pointer sets are bounded {hash,count}.
  IF jsonb_typeof(p_scope) IS DISTINCT FROM 'object'
     OR p_scope->>'generation' !~ v_hex
     OR p_scope->>'orgId' IS NULL OR p_scope->>'workspaceId' IS NULL
     OR p_scope->>'harness' IS NULL OR p_scope->>'sessionId' IS NULL
  THEN RAISE EXCEPTION 'scope_invalid' USING ERRCODE='22023'; END IF;
  IF jsonb_typeof(p_source) IS DISTINCT FROM 'object'
     OR p_source->>'authorityId' !~ v_uid
     OR p_source->>'epoch' IS NULL
     OR p_source->>'fingerprint' !~ v_hex
  THEN RAISE EXCEPTION 'source_invalid' USING ERRCODE='22023'; END IF;
  IF jsonb_typeof(p_pointer_set) IS DISTINCT FROM 'object'
     OR p_pointer_set->>'hash' !~ v_hex
     OR (p_pointer_set->>'count')::bigint < 0
     OR (p_pointer_set->>'count')::bigint > 100000
  THEN RAISE EXCEPTION 'pointer_set_invalid' USING ERRCODE='22023'; END IF;
  -- Prior args: both NULL or both valid; prior authority must be a full
  -- identity with a non-null 64-hex fingerprint (no vague retirement).
  IF (p_prior_source IS NULL) IS DISTINCT FROM (p_prior_pointer_set IS NULL)
  THEN RAISE EXCEPTION 'prior_incomplete' USING ERRCODE='22023'; END IF;
  IF p_prior_source IS NOT NULL THEN
    IF jsonb_typeof(p_prior_source) IS DISTINCT FROM 'object'
       OR p_prior_source->>'authorityId' !~ v_uid
       OR p_prior_source->>'epoch' IS NULL
       OR p_prior_source->>'fingerprint' !~ v_hex
    THEN RAISE EXCEPTION 'prior_source_invalid' USING ERRCODE='22023'; END IF;
    IF jsonb_typeof(p_prior_pointer_set) IS DISTINCT FROM 'object'
       OR p_prior_pointer_set->>'hash' !~ v_hex
       OR (p_prior_pointer_set->>'count')::bigint < 0
       OR (p_prior_pointer_set->>'count')::bigint > 100000
    THEN RAISE EXCEPTION 'prior_pointer_set_invalid' USING ERRCODE='22023'; END IF;
    -- Self-retire refused: a replacement's prior authority differs from new.
    IF p_prior_source->>'authorityId' = p_source->>'authorityId'
       OR p_prior_source->>'fingerprint' = p_source->>'fingerprint'
    THEN RAISE EXCEPTION 'self_retire' USING ERRCODE='22023'; END IF;
  END IF;

  -- Fail closed: no operator seed above the drained legacy clock means the
  -- counter was never initialized — refuse, never allocate revision 1.
  PERFORM pg_advisory_xact_lock(hashtext('canonical_feature_changed'));
  IF NOT EXISTS (SELECT 1 FROM sessions.canonical_feature_journal
                  WHERE kind = 'seed') THEN
    RAISE EXCEPTION 'unseeded' USING ERRCODE='23505';
  END IF;

  -- Immutable authority versions: imageKey binds org+session+authorityId so a
  -- replacement is a distinct authority version, never mutable org:session.
  -- schema_generation = the scope's ingest generation (64-hex), not epoch.
  IF p_prior_source IS NOT NULL THEN
    SELECT COALESCE(MAX(revision),0)+1 INTO v_revision
      FROM sessions.canonical_feature_journal;
    INSERT INTO sessions.canonical_feature_journal(
      revision,kind,org_id,workspace_id,scope,schema_generation,
      image_key,source_hash,prior_keyset_hash,source_authority,
      prior_source_authority,pointer_set)
    VALUES(
      v_revision,'retire',
      p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
      p_scope->>'generation',
      (p_scope->>'orgId'||':'||p_scope->>'sessionId'||':'
        ||p_prior_source->>'authorityId'),
      p_prior_source->>'fingerprint',
      p_prior_pointer_set->>'hash',
      p_prior_source, NULL, p_prior_pointer_set);
    SELECT v_revision+1 INTO v_revision;
    INSERT INTO sessions.canonical_feature_journal(
      revision,kind,org_id,workspace_id,scope,schema_generation,
      image_key,source_hash,prior_keyset_hash,source_authority,
      prior_source_authority,pointer_set)
    VALUES(
      v_revision,'publish',
      p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
      p_scope->>'generation',
      (p_scope->>'orgId'||':'||p_scope->>'sessionId'||':'
        ||p_source->>'authorityId'),
      p_source->>'fingerprint',
      p_prior_pointer_set->>'hash',
      p_source, p_prior_source, p_pointer_set);
    RETURN v_revision;
  END IF;

  SELECT COALESCE(MAX(revision),0)+1 INTO v_revision
    FROM sessions.canonical_feature_journal;
  INSERT INTO sessions.canonical_feature_journal(
    revision,kind,org_id,workspace_id,scope,schema_generation,
    image_key,source_hash,prior_keyset_hash,source_authority,
    prior_source_authority,pointer_set)
  VALUES(
    v_revision,'publish',
    p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
    p_scope->>'generation',
    (p_scope->>'orgId'||':'||p_scope->>'sessionId'||':'
      ||p_source->>'authorityId'),
    p_source->>'fingerprint',
    NULL, p_source, NULL, p_pointer_set);
  RETURN v_revision;
END;
$$;

-- Exact 5-arg identity; owner-only (no data-plane grant).
REVOKE ALL ON FUNCTION sessions.canonical_feature_changed(
  jsonb,jsonb,jsonb,jsonb,jsonb) FROM PUBLIC;
