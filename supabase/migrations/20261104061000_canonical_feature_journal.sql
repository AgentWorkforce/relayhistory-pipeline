-- 610 canonical feature journal + owner-only invoker (supporter-cloud proposal
-- docs/proposals/20261104061000/061020 on feat/canonical-feature-producer @ 92e5a8f),
-- composed with the 620 final install CAS. Metadata only: no payload, no transcript
-- bytes. The ONLY caller is sessions.ingest_history_scope_install (SECURITY DEFINER,
-- same owner), inside its transaction: a rollback of the install emits nothing.
-- Changes from the draft, agreed with the 610 owner:
--  * revision allocation takes a transaction advisory lock (MAX ... FOR UPDATE is not
--    valid with an aggregate);
--  * image_key concatenation is parenthesized (->> and || share precedence);
--  * schema_generation is the ingest generation from p_scope, not the source epoch.
-- Retention-ready: org_id + recorded_at.
CREATE TABLE sessions.canonical_feature_journal(
 revision bigint PRIMARY KEY CHECK(revision>0),
 kind text NOT NULL CHECK(kind IN('seed','publish','retire')),
 org_id text NOT NULL,
 workspace_id text NOT NULL,
 scope jsonb NOT NULL,
 schema_generation text NOT NULL CHECK(schema_generation~'^[a-f0-9]{64}$'),
 image_key text NOT NULL,
 source_hash text NOT NULL CHECK(source_hash~'^[a-f0-9]{64}$'),
 prior_keyset_hash text CHECK(prior_keyset_hash~'^[a-f0-9]{64}$'),
 source_authority jsonb NOT NULL,
 prior_source_authority jsonb,
 pointer_set jsonb NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX canonical_feature_journal_kind_rev ON sessions.canonical_feature_journal(kind,revision);
CREATE INDEX canonical_feature_journal_org_time ON sessions.canonical_feature_journal(org_id,recorded_at);
ALTER TABLE sessions.canonical_feature_journal ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.canonical_feature_journal FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.canonical_feature_changed(p_scope jsonb,p_source jsonb,p_prior_source jsonb,p_pointer_set jsonb)
RETURNS bigint LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $feature$
DECLARE v_revision bigint;
BEGIN
 IF p_scope IS NULL OR p_source IS NULL OR p_pointer_set IS NULL
  OR jsonb_typeof(p_scope->'orgId')IS DISTINCT FROM'string'OR jsonb_typeof(p_scope->'workspaceId')IS DISTINCT FROM'string'
  OR jsonb_typeof(p_scope->'harness')IS DISTINCT FROM'string'OR jsonb_typeof(p_scope->'sessionId')IS DISTINCT FROM'string'
  OR jsonb_typeof(p_scope->'generation')IS DISTINCT FROM'string'OR p_scope->>'generation'!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p_source->'epoch')IS DISTINCT FROM'string'OR p_source->>'epoch'!~'^[1-9][0-9]{0,18}$'
  OR jsonb_typeof(p_source->'fingerprint')IS DISTINCT FROM'string'OR p_source->>'fingerprint'!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p_pointer_set->'hash')IS DISTINCT FROM'string'OR p_pointer_set->>'hash'!~'^[a-f0-9]{64}$'
  OR jsonb_typeof(p_pointer_set->'count')IS DISTINCT FROM'string'OR p_pointer_set->>'count'!~'^(0|[1-9][0-9]{0,9})$'
  OR(p_prior_source IS NOT NULL AND(jsonb_typeof(p_prior_source->'epoch')IS DISTINCT FROM'string'OR p_prior_source->>'epoch'!~'^[1-9][0-9]{0,18}$'
   OR(p_prior_source->'fingerprint'IS DISTINCT FROM'null'::jsonb AND(jsonb_typeof(p_prior_source->'fingerprint')IS DISTINCT FROM'string'OR p_prior_source->>'fingerprint'!~'^[a-f0-9]{64}$'))))
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='canonical_feature_changed: invalid input';END IF;
 -- Single global allocator: strictly increasing, never reused; the seed row (written once at
 -- drain time) puts the managed counter above the last legacy feature clock.
 PERFORM pg_advisory_xact_lock(hashtextextended('96bulls:canonical-feature-journal',0));
 SELECT coalesce(max(revision),0)+1 INTO v_revision FROM sessions.canonical_feature_journal;
 INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,
  prior_keyset_hash,source_authority,prior_source_authority,pointer_set)
 VALUES(v_revision,CASE WHEN p_prior_source IS NULL THEN'publish'ELSE'retire'END,p_scope->>'orgId',p_scope->>'workspaceId',p_scope,
  p_scope->>'generation',(p_scope->>'orgId')||':'||(p_scope->>'sessionId'),p_source->>'fingerprint',
  CASE WHEN p_prior_source IS NULL THEN NULL ELSE p_prior_source->>'fingerprint'END,p_source,p_prior_source,p_pointer_set);
 RETURN v_revision;
END $feature$;
-- OWNER-ONLY: no role receives EXECUTE; the 620 install (same owner, SECURITY DEFINER) calls it.
REVOKE ALL ON FUNCTION sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb)FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
