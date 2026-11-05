-- 040610 companion RPC — DRAFT proposal, not reserved. Coordinate with the
-- 620 final-CAS owner before moving to supabase/migrations/.
-- OWNER-ONLY invoker: callable INSIDE the 620 pointer-CAS transaction by the
-- privileged caller only; inputs are server-constructed, never app-provided.
-- Rollback of the enclosing transaction emits nothing — the journal append is
-- durable only with the pointer CAS.

CREATE OR REPLACE FUNCTION sessions.canonical_feature_changed(
  p_scope jsonb,
  p_source jsonb,
  p_prior_source jsonb,
  p_pointer_set jsonb,
  p_prior_pointer_set jsonb DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_revision bigint;
BEGIN
  -- Server-constructed inputs only: every field is required and well-shaped.
  IF p_scope IS NULL OR p_source IS NULL OR p_pointer_set IS NULL
     OR p_scope->>'orgId' IS NULL OR p_scope->>'sessionId' IS NULL
     OR p_source->>'epoch' IS NULL OR p_source->>'fingerprint' IS NULL
     OR p_pointer_set->>'hash' IS NULL OR p_pointer_set->>'count' IS NULL THEN
    RAISE EXCEPTION 'canonical_feature_changed: invalid input' USING ERRCODE = '22023';
  END IF;

  -- The managed counter: strictly increasing in the shared clock space.
  -- Seeded ABOVE the last legacy feature clock at drain time — never overlaps
  -- a legacy-issued revision and never moves backwards.
  -- Replacement emits TWO ordered rows: retire(PRIOR authority + PRIOR
  -- pointer inventory), then publish(NEW authority + NEW pointer inventory).
  IF p_prior_source IS NOT NULL THEN
    IF p_prior_pointer_set IS NULL OR p_prior_pointer_set->>'hash' IS NULL
      THEN RAISE EXCEPTION 'prior pointer set required' USING ERRCODE = '22023';
    END IF;
    SELECT COALESCE(MAX(revision), 0) + 1 INTO v_revision
      FROM sessions.canonical_feature_journal FOR UPDATE;
    INSERT INTO sessions.canonical_feature_journal(
      revision,kind,org_id,workspace_id,scope,schema_generation,
      image_key,source_hash,prior_keyset_hash,source_authority,
      prior_source_authority,pointer_set)
    VALUES(
      v_revision,'retire',
      p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
      p_prior_source->>'epoch',
      p_scope->>'orgId'||':'||p_scope->>'sessionId',
      p_prior_source->>'fingerprint',
      p_prior_pointer_set->>'hash',
      '',p_prior_source,p_prior_pointer_set);
    SELECT v_revision + 1 INTO v_revision;
    INSERT INTO sessions.canonical_feature_journal(
      revision,kind,org_id,workspace_id,scope,schema_generation,
      image_key,source_hash,source_authority,pointer_set)
    VALUES(
      v_revision,'publish',
      p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
      p_source->>'epoch',
      p_scope->>'orgId'||':'||p_scope->>'sessionId',
      p_source->>'fingerprint',p_source,p_pointer_set);
    RETURN v_revision;
  END IF;

  SELECT COALESCE(MAX(revision), 0) + 1 INTO v_revision
    FROM sessions.canonical_feature_journal
    FOR UPDATE;
  INSERT INTO sessions.canonical_feature_journal(
    revision,kind,org_id,workspace_id,scope,schema_generation,
    image_key,source_hash,source_authority,pointer_set)
  VALUES(
    v_revision,'publish',
    p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
    p_source->>'epoch',
    p_scope->>'orgId'||':'||p_scope->>'sessionId',
    p_source->>'fingerprint',p_source,p_pointer_set);
  RETURN v_revision;
END;
$$;

-- OWNER-ONLY: revoke all, grant only to the privileged 620 caller.
REVOKE ALL ON FUNCTION sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb) FROM PUBLIC;
