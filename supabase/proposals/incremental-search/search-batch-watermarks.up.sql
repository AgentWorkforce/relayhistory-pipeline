-- M3 proposal only: no migration slot and never applied by runMigrations.
-- Scope watermarks are derived search state. Every write goes through the
-- owner SECURITY DEFINER CAS so the runtime has no table DML.

CREATE TABLE sessions.search_scope_watermark_versions (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  version bigint NOT NULL DEFAULT 0 CHECK (version BETWEEN 0 AND 9007199254740991),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id, workspace_id, harness, session_id)
);
ALTER TABLE sessions.search_scope_watermark_versions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_scope_watermark_versions FROM PUBLIC, anon, authenticated, service_role;

CREATE TABLE sessions.search_batch_watermarks (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  harness text NOT NULL,
  session_id text NOT NULL,
  manifest_key text NOT NULL CHECK (manifest_key <> ''),
  last_seq bigint NOT NULL CHECK (last_seq BETWEEN 0 AND 9007199254740991),
  chain_digest text NOT NULL CHECK (chain_digest ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (org_id, workspace_id, harness, session_id, manifest_key),
  FOREIGN KEY (org_id, workspace_id, harness, session_id)
    REFERENCES sessions.search_scope_watermark_versions(org_id, workspace_id, harness, session_id)
    ON DELETE CASCADE
);
ALTER TABLE sessions.search_batch_watermarks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_batch_watermarks FROM PUBLIC, anon, authenticated, service_role;

-- Validate and canonicalize the RPC's JSON boundary. Exact keys prevent a
-- caller from believing evidence was bound when the server silently ignored it.
CREATE FUNCTION sessions.canonical_search_watermarks_v1(p_rows jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path = pg_catalog, pg_temp AS $canonical$
DECLARE out_rows jsonb;
BEGIN
  IF jsonb_typeof(p_rows) <> 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: expected an array';
  END IF;
  IF jsonb_array_length(p_rows) > 256 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: scope exceeds 256 manifests';
  END IF;
  IF EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_rows) AS e(value)
    WHERE jsonb_typeof(e.value) <> 'object'
       OR (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(e.value) AS k)
          IS DISTINCT FROM ARRAY['chainDigest','lastSeq','manifestKey']::text[]
       OR jsonb_typeof(e.value->'manifestKey') <> 'string'
       OR coalesce(e.value->>'manifestKey', '') = ''
       OR octet_length(e.value->>'manifestKey') > 4096
       OR jsonb_typeof(e.value->'lastSeq') <> 'number'
       OR (e.value->>'lastSeq') !~ '^(0|[1-9][0-9]{0,15})$'
       OR (e.value->>'lastSeq')::numeric > 9007199254740991
       OR jsonb_typeof(e.value->'chainDigest') <> 'string'
       OR (e.value->>'chainDigest') !~ '^[0-9a-f]{64}$'
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: invalid row';
  END IF;
  IF (SELECT count(*) FROM jsonb_array_elements(p_rows)) IS DISTINCT FROM
     (SELECT count(DISTINCT e.value->>'manifestKey') FROM jsonb_array_elements(p_rows) AS e(value)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: duplicate manifest key';
  END IF;
  SELECT coalesce(jsonb_agg(jsonb_build_object(
    'manifestKey', e.value->>'manifestKey',
    'lastSeq', (e.value->>'lastSeq')::bigint,
    'chainDigest', e.value->>'chainDigest') ORDER BY e.value->>'manifestKey'), '[]'::jsonb)
    INTO out_rows FROM jsonb_array_elements(p_rows) AS e(value);
  RETURN out_rows;
END $canonical$;
REVOKE ALL ON FUNCTION sessions.canonical_search_watermarks_v1(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE FUNCTION sessions.read_search_batch_watermarks_v1(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $read$
DECLARE current_version bigint; current_rows jsonb;
BEGIN
  IF coalesce(p_org_id,'') = '' OR coalesce(p_workspace_id,'') = '' OR
     coalesce(p_harness,'') = '' OR coalesce(p_session_id,'') = '' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: incomplete scope';
  END IF;
  SELECT s.version INTO current_version
    FROM sessions.search_scope_watermark_versions s
   WHERE (s.org_id,s.workspace_id,s.harness,s.session_id) =
         (p_org_id,p_workspace_id,p_harness,p_session_id);
  IF NOT FOUND THEN
    RETURN jsonb_build_object('version', 0, 'watermarks', '[]'::jsonb);
  END IF;
  SELECT coalesce(jsonb_agg(jsonb_build_object(
      'manifestKey', w.manifest_key, 'lastSeq', w.last_seq, 'chainDigest', w.chain_digest)
      ORDER BY w.manifest_key), '[]'::jsonb)
    INTO current_rows
    FROM sessions.search_batch_watermarks w
   WHERE (w.org_id,w.workspace_id,w.harness,w.session_id) =
         (p_org_id,p_workspace_id,p_harness,p_session_id);
  RETURN jsonb_build_object('version', current_version, 'watermarks', current_rows);
END $read$;
REVOKE ALL ON FUNCTION sessions.read_search_batch_watermarks_v1(text,text,text,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.read_search_batch_watermarks_v1(text,text,text,text) TO service_role;

CREATE FUNCTION sessions.cas_search_batch_watermarks_v1(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text,
  p_expected_version bigint, p_expected jsonb, p_next jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $cas$
DECLARE expected_rows jsonb; next_rows jsonb; current_rows jsonb; current_version bigint; next_version bigint;
BEGIN
  IF coalesce(p_org_id,'') = '' OR coalesce(p_workspace_id,'') = '' OR
     coalesce(p_harness,'') = '' OR coalesce(p_session_id,'') = '' OR
     p_expected_version IS NULL OR p_expected_version NOT BETWEEN 0 AND 9007199254740991 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: invalid CAS scope or version';
  END IF;
  expected_rows := sessions.canonical_search_watermarks_v1(p_expected);
  next_rows := sessions.canonical_search_watermarks_v1(p_next);

  -- The runtime may update derived state only for a current canonical scope.
  -- Erasure cleanup is an owner operation and is deliberately not exposed by
  -- this runtime boundary.
  IF NOT EXISTS (
    SELECT 1 FROM sessions.session_directory d
     WHERE (d.org_id,d.workspace_id,d.harness,d.session_id) =
           (p_org_id,p_workspace_id,p_harness,p_session_id)
       AND d.deleted_at IS NULL
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search watermarks: scope is not live';
  END IF;

  INSERT INTO sessions.search_scope_watermark_versions(org_id,workspace_id,harness,session_id)
  VALUES(p_org_id,p_workspace_id,p_harness,p_session_id)
  ON CONFLICT DO NOTHING;
  SELECT s.version INTO STRICT current_version
    FROM sessions.search_scope_watermark_versions s
   WHERE (s.org_id,s.workspace_id,s.harness,s.session_id) =
         (p_org_id,p_workspace_id,p_harness,p_session_id)
   FOR UPDATE;
  SELECT coalesce(jsonb_agg(jsonb_build_object(
      'manifestKey', w.manifest_key, 'lastSeq', w.last_seq, 'chainDigest', w.chain_digest)
      ORDER BY w.manifest_key), '[]'::jsonb)
    INTO current_rows
    FROM sessions.search_batch_watermarks w
   WHERE (w.org_id,w.workspace_id,w.harness,w.session_id) =
         (p_org_id,p_workspace_id,p_harness,p_session_id);

  IF current_version <> p_expected_version OR current_rows IS DISTINCT FROM expected_rows THEN
    RETURN jsonb_build_object('committed', false, 'version', current_version, 'watermarks', current_rows);
  END IF;
  IF current_version = 9007199254740991 THEN
    RAISE EXCEPTION USING ERRCODE = '22003', MESSAGE = 'search watermarks: version exhausted';
  END IF;
  DELETE FROM sessions.search_batch_watermarks w
   WHERE (w.org_id,w.workspace_id,w.harness,w.session_id) =
         (p_org_id,p_workspace_id,p_harness,p_session_id);
  INSERT INTO sessions.search_batch_watermarks(org_id,workspace_id,harness,session_id,manifest_key,last_seq,chain_digest)
  SELECT p_org_id,p_workspace_id,p_harness,p_session_id,
         e.value->>'manifestKey',(e.value->>'lastSeq')::bigint,e.value->>'chainDigest'
    FROM jsonb_array_elements(next_rows) AS e(value);
  next_version := current_version + 1;
  UPDATE sessions.search_scope_watermark_versions s
     SET version = next_version, updated_at = clock_timestamp()
   WHERE (s.org_id,s.workspace_id,s.harness,s.session_id) =
         (p_org_id,p_workspace_id,p_harness,p_session_id);
  RETURN jsonb_build_object('committed', true, 'version', next_version, 'watermarks', next_rows);
END $cas$;
REVOKE ALL ON FUNCTION sessions.cas_search_batch_watermarks_v1(text,text,text,text,bigint,jsonb,jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.cas_search_batch_watermarks_v1(text,text,text,text,bigint,jsonb,jsonb)
  TO service_role;

