-- TEST-ONLY proposed canary authority. This file is never in the migration
-- directory and must not be applied to a real database. It proves the minimum
-- managed-role contract required before a production migration is assigned.

CREATE TABLE sessions.proof_publication_frontier_attestations (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  origin_id text NOT NULL,
  capture_id text NOT NULL CHECK (capture_id ~ '^[a-f0-9]{64}$'),
  frontier_plan_sha256 text NOT NULL CHECK (frontier_plan_sha256 ~ '^[a-f0-9]{64}$'),
  frontier_evidence_sha256 text NOT NULL CHECK (frontier_evidence_sha256 ~ '^[a-f0-9]{64}$'),
  source_frontier_sha256 text NOT NULL CHECK (source_frontier_sha256 ~ '^[a-f0-9]{64}$'),
  record_maxima_sha256 text NOT NULL CHECK (record_maxima_sha256 ~ '^[a-f0-9]{64}$'),
  valid_until timestamptz NOT NULL,
  target_bucket text NOT NULL,
  target_region text NOT NULL,
  key_root text NOT NULL,
  schema_generation text NOT NULL CHECK (schema_generation ~ '^[a-f0-9]{64}$'),
  PRIMARY KEY (org_id, workspace_id, user_id, origin_id),
  FOREIGN KEY (org_id, workspace_id, user_id, origin_id)
    REFERENCES sessions.ingest_publication_frontiers
      (org_id, workspace_id, user_id, origin_id) ON DELETE RESTRICT
);

CREATE TABLE sessions.proof_publication_canary_bindings (
  stage_token uuid PRIMARY KEY REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
  plan_id uuid NOT NULL,
  owner_id text NOT NULL CHECK (octet_length(owner_id) BETWEEN 1 AND 255),
  plan_json text NOT NULL CHECK (octet_length(plan_json) BETWEEN 1 AND 65536),
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[a-f0-9]{64}$'),
  inspection_sha256 text NOT NULL CHECK (inspection_sha256 ~ '^[a-f0-9]{64}$'),
  frontier_plan_sha256 text NOT NULL CHECK (frontier_plan_sha256 ~ '^[a-f0-9]{64}$'),
  frontier_evidence_sha256 text NOT NULL CHECK (frontier_evidence_sha256 ~ '^[a-f0-9]{64}$'),
  expected jsonb NOT NULL CHECK (jsonb_typeof(expected) = 'object'),
  attempt_token uuid UNIQUE,
  ticket jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((attempt_token IS NULL) = (ticket IS NULL))
);

ALTER TABLE sessions.proof_publication_frontier_attestations ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.proof_publication_canary_bindings ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.proof_publication_frontier_attestations,
  sessions.proof_publication_canary_bindings
  FROM PUBLIC, anon, authenticated, service_role,
    bulls96_session_ingest, bulls96_session_ingest_seeder;

-- Exact metadata evidence available to the managed caller. Transcript bytes,
-- record bodies and arbitrary table reads never cross this function boundary.
CREATE FUNCTION sessions.proof_publication_canary_expected(p_stage uuid)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $expected$
  SELECT jsonb_build_object(
    'stageToken', s.stage_token,
    'tenant', jsonb_build_object(
      'orgId', s.org_id,
      'workspaceId', s.workspace_id,
      'userId', s.user_id
    ),
    'originId', s.origin_id,
    'batchId', s.batch_id,
    'batchDigest', s.digest,
    'stage', jsonb_build_object(
      'key', s.stage_key,
      'sha256', s.stage_sha256,
      'bytes', s.stage_bytes,
      'groupOrdinalsSha256', encode(sha256(convert_to(s.group_ordinals::text, 'UTF8')), 'hex'),
      'resetEpoch', s.reset_epoch::text,
      'rowVersion', s.row_version::text
    ),
    'outboxVersion', o.row_version::text,
    'privacy', jsonb_build_object(
      'privacyBaselineSha256', d.privacy_baseline_sha256,
      'retentionContractSha256', d.retention_contract_sha256,
      'controlEpoch', d.control_epoch,
      'validUntilEpoch', extract(epoch FROM d.valid_until)::text,
      'schemaGeneration', d.schema_generation
    ),
    'frontier', jsonb_build_object(
      'captureId', a.capture_id,
      'planSha256', a.frontier_plan_sha256,
      'evidenceSha256', a.frontier_evidence_sha256,
      'sourceFrontierSha256', f.source_frontier_sha256,
      'recordMaximaSha256', f.record_maxima_sha256,
      'validUntilEpoch', extract(epoch FROM f.valid_until)::text,
      'bucket', f.target_bucket,
      'region', f.target_region,
      'keyRoot', f.key_root,
      'schemaGeneration', f.schema_generation
    )
  )
  FROM sessions.ingest_stage_intents s
  JOIN sessions.ingest_buffer_outbox o
    ON o.org_id = s.org_id AND o.workspace_id = s.workspace_id
   AND o.origin_id = s.origin_id AND o.batch_id = s.batch_id
   AND o.user_id = s.user_id AND o.digest = s.digest
   AND o.active_stage_token = s.stage_token
  JOIN sessions.ingest_buffer_admission d ON d.org_id = s.org_id
  JOIN sessions.ingest_publication_frontiers f
    ON f.org_id = s.org_id AND f.workspace_id = s.workspace_id
   AND f.user_id = s.user_id AND f.origin_id = s.origin_id
  JOIN sessions.proof_publication_frontier_attestations a
    ON a.org_id = f.org_id AND a.workspace_id = f.workspace_id
   AND a.user_id = f.user_id AND a.origin_id = f.origin_id
   AND ROW(a.source_frontier_sha256, a.record_maxima_sha256, a.valid_until,
           a.target_bucket, a.target_region, a.key_root, a.schema_generation)
       IS NOT DISTINCT FROM
       ROW(f.source_frontier_sha256, f.record_maxima_sha256, f.valid_until,
           f.target_bucket, f.target_region, f.key_root, f.schema_generation)
  WHERE s.stage_token = p_stage;
$expected$;

REVOKE ALL ON FUNCTION sessions.proof_publication_canary_expected(uuid)
  FROM PUBLIC, anon, authenticated, service_role,
    bulls96_session_ingest, bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.proof_publication_canary_inspect(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $inspect$
DECLARE
  s sessions.ingest_stage_intents%ROWTYPE;
  expected jsonb;
  evidence text;
  eligible boolean;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  PERFORM sessions.ingest_exact_keys(p, ARRAY['stageToken', 'bucket', 'region', 'keyRoot', 'generation']);
  PERFORM sessions.ingest_require_generation(p->>'generation');
  SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token = (p->>'stageToken')::uuid;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('decision', 'hold', 'holdReason', 'stage_absent', 'expected', NULL);
  END IF;
  expected := sessions.proof_publication_canary_expected(s.stage_token);
  IF expected IS NULL THEN
    RETURN jsonb_build_object('decision', 'hold', 'holdReason', 'authority_incomplete',
      'stage', jsonb_build_object('stageToken', s.stage_token, 'orgId', s.org_id,
        'workspaceId', s.workspace_id, 'userId', s.user_id, 'originId', s.origin_id),
      'expected', NULL);
  END IF;
  evidence := encode(sha256(convert_to(expected::text, 'UTF8')), 'hex');
  eligible := s.receipt_state = 'acknowledged' AND s.cleanup_state = 'live'
    AND s.gc_replacement_stage_token IS NULL
    AND (s.gc_lease_until IS NULL OR s.gc_lease_until <= clock_timestamp())
    AND jsonb_array_length(sessions.ingest_buffer_gc_kept(s.stage_token)) = jsonb_array_length(s.group_ordinals)
    AND (expected#>>'{privacy,validUntilEpoch}')::numeric > extract(epoch FROM clock_timestamp())
    AND (expected#>>'{frontier,validUntilEpoch}')::numeric > extract(epoch FROM clock_timestamp())
    AND expected#>>'{frontier,bucket}' = p->>'bucket'
    AND expected#>>'{frontier,region}' = p->>'region'
    AND expected#>>'{frontier,keyRoot}' = p->>'keyRoot'
    AND expected#>>'{frontier,schemaGeneration}' = p->>'generation';
  RETURN jsonb_build_object(
    'decision', CASE WHEN eligible THEN 'eligible' ELSE 'hold' END,
    'holdReason', CASE WHEN eligible THEN NULL ELSE 'stage_not_claimable' END,
    'evidenceDigest', evidence,
    'expected', expected
  );
END
$inspect$;

-- The binding is inserted before the ordinary claim while holding the same
-- USER -> BATCH -> INTENT -> OUTBOX -> FRONTIER locks. Any later failure rolls
-- back both the binding and every nested-claim side effect.
CREATE FUNCTION sessions.proof_publication_canary_claim(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $guarded$
DECLARE
  s sessions.ingest_stage_intents%ROWTYPE;
  o sessions.ingest_buffer_outbox%ROWTYPE;
  f sessions.ingest_publication_frontiers%ROWTYPE;
  att sessions.proof_publication_frontier_attestations%ROWTYPE;
  prior sessions.proof_publication_canary_bindings%ROWTYPE;
  expected jsonb;
  plan_doc jsonb;
  evidence text;
  v_ticket jsonb;
  expires_at timestamptz;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  PERFORM sessions.ingest_exact_keys(p, ARRAY[
    'planId', 'ownerId', 'planJson', 'planDigest', 'expiresAt', 'inspectionSha256',
    'expected', 'bucket', 'region', 'keyRoot', 'generation'
  ]);
  PERFORM sessions.ingest_require_generation(p->>'generation');
  IF jsonb_typeof(p->'ownerId') IS DISTINCT FROM 'string'
     OR octet_length(p->>'ownerId') NOT BETWEEN 1 AND 255
     OR p->>'ownerId' ~ '[\x00-\x1f\x7f/\\]'
     OR p->>'planId' !~ '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
     OR jsonb_typeof(p->'planJson') IS DISTINCT FROM 'string'
     OR octet_length(p->>'planJson') NOT BETWEEN 1 AND 65536
     OR p->>'planDigest' !~ '^[a-f0-9]{64}$'
     OR encode(sha256(convert_to(p->>'planJson', 'UTF8')), 'hex') IS DISTINCT FROM p->>'planDigest'
     OR p->>'inspectionSha256' !~ '^[a-f0-9]{64}$'
     OR jsonb_typeof(p->'expected') IS DISTINCT FROM 'object'
     OR octet_length((p->'expected')::text) > 65536
     OR jsonb_typeof(p->'expiresAt') IS DISTINCT FROM 'string'
     OR p->>'expiresAt' !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'publication canary: invalid plan binding';
  END IF;
  BEGIN
    expires_at := (p->>'expiresAt')::timestamptz;
    plan_doc := (p->>'planJson')::jsonb;
  EXCEPTION WHEN others THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'publication canary: invalid plan expiry';
  END;
  IF expires_at <= clock_timestamp() OR expires_at > clock_timestamp() + interval '24 hours' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'publication canary: invalid plan expiry';
  END IF;

  SELECT * INTO s FROM sessions.ingest_stage_intents
   WHERE stage_token = (p#>>'{expected,stageToken}')::uuid;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'publication canary: stage absent'; END IF;
  IF plan_doc->>'version' IS DISTINCT FROM '1'
     OR plan_doc->>'planId' IS DISTINCT FROM p->>'planId'
     OR plan_doc->>'ownerId' IS DISTINCT FROM p->>'ownerId'
     OR plan_doc->>'expiresAt' IS DISTINCT FROM p->>'expiresAt'
     OR plan_doc#>>'{tenant,orgId}' IS DISTINCT FROM p#>>'{expected,tenant,orgId}'
     OR plan_doc#>>'{tenant,workspaceId}' IS DISTINCT FROM p#>>'{expected,tenant,workspaceId}'
     OR plan_doc#>>'{target,bucket}' IS DISTINCT FROM p->>'bucket'
     OR plan_doc#>>'{target,region}' IS DISTINCT FROM p->>'region'
     OR plan_doc#>>'{target,keyRoot}' IS DISTINCT FROM p->>'keyRoot'
     OR plan_doc#>>'{target,generation}' IS DISTINCT FROM p->>'generation'
     OR jsonb_typeof(plan_doc->'stages') IS DISTINCT FROM 'array'
     OR jsonb_array_length(plan_doc->'stages') <> 1
     OR (SELECT count(*) FROM jsonb_array_elements(plan_doc->'stages') x
          WHERE x->>'stageToken' = p#>>'{expected,stageToken}'
            AND x#>>'{tenant,orgId}' = p#>>'{expected,tenant,orgId}'
            AND x#>>'{tenant,workspaceId}' = p#>>'{expected,tenant,workspaceId}'
            AND x->>'userId' = p#>>'{expected,tenant,userId}'
            AND x->>'originId' = p#>>'{expected,originId}'
            AND x->>'sha256' = p#>>'{expected,stage,sha256}'
            AND x->>'bytes' = p#>>'{expected,stage,bytes}') <> 1 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'publication canary: plan does not bind expected stage';
  END IF;
  PERFORM sessions.ingest_require_buffer_admission(s.org_id);
  PERFORM sessions.ingest_lock_fences(s.org_id, s.workspace_id, s.user_id);
  SELECT * INTO prior FROM sessions.proof_publication_canary_bindings
   WHERE stage_token = s.stage_token FOR UPDATE;
  IF FOUND THEN
    IF ROW(prior.plan_id::text, prior.owner_id, prior.plan_json, prior.plan_digest, prior.inspection_sha256,
           prior.frontier_plan_sha256, prior.frontier_evidence_sha256, prior.expected)
       IS DISTINCT FROM
       ROW(p->>'planId', p->>'ownerId', p->>'planJson', p->>'planDigest', p->>'inspectionSha256',
           p#>>'{expected,frontier,planSha256}', p#>>'{expected,frontier,evidenceSha256}', p->'expected')
       OR prior.ticket IS NULL OR (prior.ticket->>'leaseUntil')::timestamptz <= clock_timestamp() THEN
      RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'publication canary: owner plan conflict';
    END IF;
    RETURN jsonb_build_object('binding', jsonb_build_object(
      'planId', prior.plan_id, 'ownerId', prior.owner_id, 'planDigest', prior.plan_digest,
      'stageToken', prior.stage_token, 'evidenceDigest', prior.inspection_sha256), 'ticket', prior.ticket);
  END IF;
  PERFORM 1 FROM sessions.ingest_batches b
   WHERE b.org_id = s.org_id AND b.workspace_id = s.workspace_id
     AND b.origin_id = s.origin_id AND b.batch_id = s.batch_id
     AND b.user_id = s.user_id AND b.digest = s.digest
     AND b.state = 'committed' AND b.buffer_receipt_kind = 'staged' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'publication canary: receipt changed'; END IF;
  SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token = s.stage_token FOR UPDATE;
  SELECT * INTO o FROM sessions.ingest_buffer_outbox b
   WHERE b.org_id = s.org_id AND b.workspace_id = s.workspace_id
     AND b.origin_id = s.origin_id AND b.batch_id = s.batch_id FOR UPDATE;
  SELECT * INTO f FROM sessions.ingest_publication_frontiers
   WHERE org_id = s.org_id AND workspace_id = s.workspace_id
     AND user_id = s.user_id AND origin_id = s.origin_id FOR SHARE;
  SELECT * INTO att FROM sessions.proof_publication_frontier_attestations
   WHERE org_id = s.org_id AND workspace_id = s.workspace_id
     AND user_id = s.user_id AND origin_id = s.origin_id FOR SHARE;

  expected := sessions.proof_publication_canary_expected(s.stage_token);
  evidence := encode(sha256(convert_to(expected::text, 'UTF8')), 'hex');
  IF expected IS NULL OR expected IS DISTINCT FROM p->'expected'
     OR evidence IS DISTINCT FROM p->>'inspectionSha256'
     OR o.state <> 'buffered' OR o.active_stage_token IS DISTINCT FROM s.stage_token
     OR s.receipt_state <> 'acknowledged' OR s.cleanup_state <> 'live'
     OR s.gc_replacement_stage_token IS NOT NULL
     OR (s.gc_lease_until IS NOT NULL AND s.gc_lease_until > clock_timestamp())
     OR jsonb_array_length(sessions.ingest_buffer_gc_kept(s.stage_token)) <> jsonb_array_length(s.group_ordinals)
     OR f.valid_until <= clock_timestamp()
     OR expected#>>'{frontier,bucket}' IS DISTINCT FROM p->>'bucket'
     OR expected#>>'{frontier,region}' IS DISTINCT FROM p->>'region'
     OR expected#>>'{frontier,keyRoot}' IS DISTINCT FROM p->>'keyRoot'
     OR expected#>>'{frontier,schemaGeneration}' IS DISTINCT FROM p->>'generation' THEN
    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'publication canary: inspected authority changed';
  END IF;

  INSERT INTO sessions.proof_publication_canary_bindings(
    stage_token, plan_id, owner_id, plan_json, plan_digest, inspection_sha256,
    frontier_plan_sha256, frontier_evidence_sha256, expected
  ) VALUES (
    s.stage_token, (p->>'planId')::uuid, p->>'ownerId', p->>'planJson', p->>'planDigest', evidence,
    expected#>>'{frontier,planSha256}', expected#>>'{frontier,evidenceSha256}', expected
  );

  v_ticket := sessions.ingest_publication_claim(jsonb_build_object(
    'stageToken', s.stage_token, 'keyRoot', p->>'keyRoot',
    'bucket', p->>'bucket', 'region', p->>'region', 'generation', p->>'generation'
  ));
  IF v_ticket#>>'{tenant,orgId}' IS DISTINCT FROM expected#>>'{tenant,orgId}'
     OR v_ticket#>>'{tenant,workspaceId}' IS DISTINCT FROM expected#>>'{tenant,workspaceId}'
     OR v_ticket#>>'{tenant,userId}' IS DISTINCT FROM expected#>>'{tenant,userId}'
     OR v_ticket->>'originId' IS DISTINCT FROM expected->>'originId'
     OR v_ticket->>'batchId' IS DISTINCT FROM expected->>'batchId'
     OR v_ticket->>'batchDigest' IS DISTINCT FROM expected->>'batchDigest'
     OR v_ticket#>>'{stage,sha256}' IS DISTINCT FROM expected#>>'{stage,sha256}'
     OR v_ticket#>>'{stage,bytes}' IS DISTINCT FROM expected#>>'{stage,bytes}' THEN
    RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'publication canary: ticket evidence changed';
  END IF;
  UPDATE sessions.proof_publication_canary_bindings
   SET attempt_token = (v_ticket->>'attemptToken')::uuid, ticket = v_ticket
   WHERE stage_token = s.stage_token;
  RETURN jsonb_build_object('binding', jsonb_build_object(
    'planId', p->>'planId', 'ownerId', p->>'ownerId', 'planDigest', p->>'planDigest',
    'stageToken', s.stage_token, 'evidenceDigest', evidence), 'ticket', v_ticket);
END
$guarded$;

REVOKE ALL ON FUNCTION sessions.proof_publication_canary_inspect(jsonb),
  sessions.proof_publication_canary_claim(jsonb)
  FROM PUBLIC, anon, authenticated, service_role,
    bulls96_session_ingest, bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.proof_publication_canary_inspect(jsonb),
  sessions.proof_publication_canary_claim(jsonb) TO bulls96_session_ingest;

-- A canary stage cannot bypass the owner-plan guard through the ordinary port.
-- A future migration must coordinate this permission change with the shared
-- runtime factory; this fixture proves the intended least-privilege boundary.
REVOKE EXECUTE ON FUNCTION sessions.ingest_publication_claim(jsonb)
  FROM bulls96_session_ingest;
