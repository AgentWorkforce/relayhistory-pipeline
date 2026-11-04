-- Dormant metadata-only global source+kind checkpoints. Feature payloads stay in CH.
CREATE TABLE sessions.search_feature_sources(
 source_id text PRIMARY KEY CHECK(source_id~'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
 target_hash text UNIQUE NOT NULL CHECK(target_hash~'^[0-9a-f]{64}$'),
 authority_hash text NOT NULL CHECK(authority_hash~'^[0-9a-f]{64}$'),
 profile_hash text NOT NULL CHECK(profile_hash~'^[0-9a-f]{64}$'),profile_json text NOT NULL CHECK(octet_length(profile_json)<=262144),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions.search_feature_copy_state(
 source_id text NOT NULL REFERENCES sessions.search_feature_sources(source_id),kind text NOT NULL CHECK(kind IN('event','brief')),
 generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0),committed_generation bigint NOT NULL DEFAULT 0 CHECK(committed_generation>=0),
 state jsonb NOT NULL DEFAULT '{"phase":"baseline","baselineH":null,"baselineCursor":null,"checkpoint":"0","observedRevision":"0","polled":false}',
 lease_nonce uuid,lease_expires_at timestamptz,next_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 checked_at timestamptz,fresh_until timestamptz,last_error text,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_id,kind),CHECK((lease_nonce IS NULL)=(lease_expires_at IS NULL))
);
ALTER TABLE sessions.search_feature_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.search_feature_copy_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_feature_sources,sessions.search_feature_copy_state FROM PUBLIC,anon,authenticated,service_role;

CREATE FUNCTION sessions.search_feature_copy_state_valid(p_state jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $valid$
DECLARE k text;n numeric;
BEGIN
 IF p_state IS NULL OR jsonb_typeof(p_state)<>'object' OR NOT(p_state ?& ARRAY['phase','baselineH','baselineCursor','checkpoint','observedRevision','polled'])
  OR p_state-ARRAY['phase','baselineH','baselineCursor','checkpoint','observedRevision','polled']<>'{}'::jsonb
  OR jsonb_typeof(p_state->'phase') IS DISTINCT FROM 'string' OR p_state->>'phase' NOT IN('baseline','journal') OR jsonb_typeof(p_state->'polled')<>'boolean'
  OR jsonb_typeof(p_state->'baselineCursor') NOT IN('string','null') OR octet_length(coalesce(p_state->>'baselineCursor',''))>16384 THEN RETURN false;END IF;
 FOREACH k IN ARRAY ARRAY['checkpoint','observedRevision','baselineH'] LOOP
  IF k='baselineH' AND p_state->k='null'::jsonb THEN CONTINUE;END IF;
  IF jsonb_typeof(p_state->k)<>'string' OR p_state->>k!~'^(0|[1-9][0-9]{0,18})$' THEN RETURN false;END IF;
  n:=(p_state->>k)::numeric;IF n>9223372036854775807 THEN RETURN false;END IF;
 END LOOP;
 IF (p_state->>'checkpoint')::bigint>(p_state->>'observedRevision')::bigint THEN RETURN false;END IF;
 IF p_state->>'phase'='baseline' THEN
  IF (p_state->>'polled')::boolean THEN RETURN false;END IF;
  IF p_state->'baselineH'='null'::jsonb THEN RETURN p_state->>'checkpoint'='0' AND p_state->>'observedRevision'='0' AND p_state->'baselineCursor'='null'::jsonb;END IF;
  RETURN p_state->>'checkpoint'=p_state->>'baselineH' AND p_state->>'observedRevision'=p_state->>'baselineH';
 END IF;
 RETURN p_state->'baselineH'<>'null'::jsonb AND p_state->'baselineCursor'='null'::jsonb AND (p_state->>'checkpoint')::bigint>=(p_state->>'baselineH')::bigint;
END $valid$;

CREATE FUNCTION sessions.search_feature_copy_body(p_source text,p_kind text) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $body$
 WITH x AS(SELECT s.*,c.kind,c.generation,c.committed_generation,c.state,c.lease_nonce,c.lease_expires_at,c.checked_at,c.fresh_until,
 encode(sha256(convert_to(c.state::text,'UTF8')),'hex') state_hash,
 c.committed_generation=c.generation AND c.state->>'phase'='journal' AND (c.state->>'polled')::boolean AND c.state->>'checkpoint'=c.state->>'observedRevision' complete
 FROM sessions.search_feature_sources s JOIN sessions.search_feature_copy_state c USING(source_id) WHERE s.source_id=p_source AND c.kind=p_kind),
 h AS(SELECT x.*,encode(sha256(convert_to(concat_ws(chr(31),'semantic-copy-data-v1',source_id,kind,authority_hash,target_hash,profile_hash,state_hash),'UTF8')),'hex') data_hash FROM x)
 SELECT jsonb_build_object('version',1,'sourceId',source_id,'kind',kind,'sourceAuthorityHash',authority_hash,'targetHash',target_hash,
 'profileHash',profile_hash,'profileJson',profile_json,'generation',generation::text,'leaseNonce',lease_nonce::text,
 'leaseExpiresAt',to_char(lease_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'state',state,'stateJson',state::text,'stateHash',state_hash,'dataHash',data_hash,'complete',complete,
 'authorityHash',encode(sha256(convert_to(concat_ws(chr(31),'semantic-copy-authority-v1',data_hash,generation::text,complete::text),'UTF8')),'hex'),
 'checkedAt',to_char(checked_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'freshUntil',to_char(fresh_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))
 FROM h
$body$;

CREATE FUNCTION sessions.register_search_feature_source(p_source text,p_target text,p_authority text,p_profile text,p_json text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $register$
DECLARE v jsonb;
BEGIN
 IF p_source IS NULL OR p_source!~'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' OR p_target IS NULL OR p_target!~'^[0-9a-f]{64}$'
 OR p_authority IS NULL OR p_authority!~'^[0-9a-f]{64}$' OR p_profile IS NULL OR p_profile!~'^[0-9a-f]{64}$'
 OR p_json IS NULL OR octet_length(p_json)>262144 OR encode(sha256(convert_to(p_json,'UTF8')),'hex')<>p_profile THEN RETURN false;END IF;
 v:=p_json::jsonb;IF jsonb_typeof(v->'source') IS DISTINCT FROM 'object' OR jsonb_typeof(v->'entries') IS DISTINCT FROM 'array' THEN RETURN false;END IF;IF jsonb_array_length(v->'entries')>8192 THEN RETURN false;END IF;
 INSERT INTO sessions.search_feature_sources(source_id,target_hash,authority_hash,profile_hash,profile_json) VALUES(p_source,p_target,p_authority,p_profile,p_json)
 ON CONFLICT DO NOTHING;
 RETURN EXISTS(SELECT 1 FROM sessions.search_feature_sources WHERE source_id=p_source AND target_hash=p_target AND authority_hash=p_authority AND profile_hash=p_profile AND profile_json=p_json);
EXCEPTION WHEN invalid_text_representation THEN RETURN false;
END $register$;

CREATE FUNCTION sessions.claim_search_feature_copy(p_source text,p_kind text,p_authority text,p_nonce uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE c sessions.search_feature_copy_state%ROWTYPE;
BEGIN
 IF p_kind IS NULL OR p_kind NOT IN('event','brief') OR p_nonce IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='feature copy: invalid claim';END IF;
 IF NOT EXISTS(SELECT 1 FROM sessions.search_feature_sources WHERE source_id=p_source AND authority_hash=p_authority) THEN RETURN NULL;END IF;
 INSERT INTO sessions.search_feature_copy_state(source_id,kind) VALUES(p_source,p_kind) ON CONFLICT DO NOTHING;
 SELECT * INTO c FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind FOR UPDATE SKIP LOCKED;
 IF NOT FOUND OR c.next_at>clock_timestamp() OR c.lease_expires_at>clock_timestamp() OR c.generation>=9223372036854775806 THEN RETURN NULL;END IF;
 UPDATE sessions.search_feature_copy_state SET generation=generation+1,lease_nonce=p_nonce,lease_expires_at=clock_timestamp()+interval '120 seconds',last_error=NULL,updated_at=clock_timestamp() WHERE source_id=p_source AND kind=p_kind;
 IF NOT EXISTS(SELECT 1 FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind AND lease_nonce=p_nonce AND lease_expires_at>clock_timestamp()) THEN RAISE EXCEPTION USING ERRCODE='RH003',MESSAGE='feature copy: claim authority expired';END IF;
 RETURN sessions.search_feature_copy_body(p_source,p_kind);
EXCEPTION WHEN SQLSTATE 'RH003' THEN RETURN NULL;
END $claim$;

CREATE FUNCTION sessions.initialize_search_feature_baseline(p_source text,p_kind text,p_nonce uuid,p_generation bigint,p_state_hash text,p_h bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $initialize$
DECLARE c sessions.search_feature_copy_state%ROWTYPE;
BEGIN
 SELECT * INTO c FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind FOR UPDATE;
 IF NOT FOUND OR p_nonce IS NULL OR p_generation IS NULL OR c.lease_nonce IS NULL OR c.lease_expires_at IS NULL OR c.lease_nonce IS DISTINCT FROM p_nonce OR c.generation<>p_generation OR c.lease_expires_at<=clock_timestamp()
 OR encode(sha256(convert_to(c.state::text,'UTF8')),'hex') IS DISTINCT FROM p_state_hash OR p_h IS NULL OR p_h<0
 OR c.state->>'phase'<>'baseline' OR c.state->'baselineH'<>'null'::jsonb THEN RETURN NULL;END IF;
 UPDATE sessions.search_feature_copy_state SET state=jsonb_build_object('phase','baseline','baselineH',p_h::text,'baselineCursor',NULL,'checkpoint',p_h::text,'observedRevision',p_h::text,'polled',false),updated_at=clock_timestamp() WHERE source_id=p_source AND kind=p_kind;
 IF NOT EXISTS(SELECT 1 FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind AND lease_nonce=p_nonce AND generation=p_generation AND lease_expires_at>clock_timestamp()) THEN RAISE EXCEPTION USING ERRCODE='RH003',MESSAGE='feature copy: initialization authority expired';END IF;
 RETURN sessions.search_feature_copy_body(p_source,p_kind);
EXCEPTION WHEN SQLSTATE 'RH003' THEN RETURN NULL;
END $initialize$;

CREATE FUNCTION sessions.heartbeat_search_feature_copy(p_source text,p_kind text,p_nonce uuid,p_generation bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $heartbeat$
DECLARE c sessions.search_feature_copy_state%ROWTYPE;
BEGIN
 SELECT * INTO c FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind FOR UPDATE;
 IF NOT FOUND OR p_nonce IS NULL OR p_generation IS NULL OR c.lease_nonce IS NULL OR c.lease_expires_at IS NULL OR c.lease_nonce IS DISTINCT FROM p_nonce OR c.generation<>p_generation OR c.lease_expires_at<=clock_timestamp() THEN RETURN NULL;END IF;
 UPDATE sessions.search_feature_copy_state SET lease_expires_at=clock_timestamp()+interval '120 seconds' WHERE source_id=p_source AND kind=p_kind;
 IF c.lease_expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='RH003',MESSAGE='feature copy: heartbeat authority expired';END IF;
 RETURN sessions.search_feature_copy_body(p_source,p_kind);
EXCEPTION WHEN SQLSTATE 'RH003' THEN RETURN NULL;
END $heartbeat$;

CREATE FUNCTION sessions.commit_search_feature_copy(p_source text,p_kind text,p_nonce uuid,p_generation bigint,p_state_hash text,p_next jsonb,p_checked_at timestamptz) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $commit$
DECLARE c sessions.search_feature_copy_state%ROWTYPE;
BEGIN
 IF NOT coalesce(sessions.search_feature_copy_state_valid(p_next),false) THEN RETURN NULL;END IF;
 SELECT * INTO c FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind FOR UPDATE;
 IF NOT FOUND OR p_nonce IS NULL OR p_generation IS NULL OR c.lease_nonce IS NULL OR c.lease_expires_at IS NULL OR c.lease_nonce IS DISTINCT FROM p_nonce OR c.generation<>p_generation OR c.committed_generation=p_generation OR c.lease_expires_at<=clock_timestamp()
 OR encode(sha256(convert_to(c.state::text,'UTF8')),'hex') IS DISTINCT FROM p_state_hash
 OR c.state->'baselineH'='null'::jsonb OR p_next->'baselineH' IS DISTINCT FROM c.state->'baselineH'
 OR (c.state->>'phase'='journal' AND p_next->>'phase'<>'journal')
 OR ((c.state->>'polled')::boolean AND NOT(p_next->>'polled')::boolean)
 OR (p_next->>'checkpoint')::bigint<(c.state->>'checkpoint')::bigint
 OR (p_next->>'observedRevision')::bigint<(c.state->>'observedRevision')::bigint
 OR (p_next->>'phase'='baseline' AND p_next->'baselineCursor' IS NOT DISTINCT FROM c.state->'baselineCursor')
 OR p_checked_at IS NULL OR p_checked_at<clock_timestamp()-interval '120 seconds' OR p_checked_at>clock_timestamp()+interval '30 seconds' THEN RETURN NULL;END IF;
 UPDATE sessions.search_feature_copy_state SET state=p_next,committed_generation=p_generation,checked_at=p_checked_at,
 fresh_until=least(lease_expires_at,p_checked_at+interval '120 seconds'),updated_at=clock_timestamp() WHERE source_id=p_source AND kind=p_kind;
 IF NOT EXISTS(SELECT 1 FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind AND lease_nonce=p_nonce AND generation=p_generation AND lease_expires_at>clock_timestamp() AND fresh_until>clock_timestamp()) THEN
 RAISE EXCEPTION USING ERRCODE='RH003',MESSAGE='feature copy: checkpoint authority expired';END IF;
 RETURN sessions.search_feature_copy_body(p_source,p_kind);
EXCEPTION WHEN SQLSTATE 'RH003' THEN RETURN NULL;
END $commit$;

CREATE FUNCTION sessions.finish_search_feature_copy(p_source text,p_kind text,p_nonce uuid,p_generation bigint,p_authority_hash text,p_error text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $finish$
DECLARE c sessions.search_feature_copy_state%ROWTYPE;b jsonb;
BEGIN
 IF p_error IS NOT NULL AND p_error!~'^[a-z][a-z0-9_]{0,63}$' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='feature copy: invalid error';END IF;
 SELECT * INTO c FROM sessions.search_feature_copy_state WHERE source_id=p_source AND kind=p_kind FOR UPDATE;
 IF NOT FOUND OR p_nonce IS NULL OR p_generation IS NULL OR c.lease_nonce IS NULL OR c.lease_expires_at IS NULL OR c.lease_nonce IS DISTINCT FROM p_nonce OR c.generation<>p_generation OR c.lease_expires_at<=clock_timestamp() THEN RETURN false;END IF;
 b:=sessions.search_feature_copy_body(p_source,p_kind);
 IF p_error IS NULL AND (c.committed_generation<>p_generation OR b->>'authorityHash' IS DISTINCT FROM p_authority_hash OR c.fresh_until<=clock_timestamp()) THEN RETURN false;END IF;
 UPDATE sessions.search_feature_copy_state SET lease_nonce=NULL,lease_expires_at=NULL,last_error=p_error,
 next_at=clock_timestamp()+make_interval(secs=>CASE WHEN p_error IS NOT NULL OR (b->>'complete')::boolean THEN 30 ELSE 0 END),updated_at=clock_timestamp()
 WHERE source_id=p_source AND kind=p_kind AND lease_nonce=p_nonce AND generation=p_generation AND lease_expires_at>clock_timestamp();
 IF c.lease_expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='RH003',MESSAGE='feature copy: finish authority expired';END IF;
 RETURN FOUND;
EXCEPTION WHEN SQLSTATE 'RH003' THEN RETURN false;
END $finish$;

-- Strip future/default grantee drift as well as PUBLIC. Only the backend executes.
DO $acl$ DECLARE f text;r text;BEGIN
 FOREACH f IN ARRAY ARRAY[
 'sessions.search_feature_copy_state_valid(jsonb)','sessions.search_feature_copy_body(text,text)',
 'sessions.register_search_feature_source(text,text,text,text,text)','sessions.claim_search_feature_copy(text,text,text,uuid)',
 'sessions.initialize_search_feature_baseline(text,text,uuid,bigint,text,bigint)','sessions.heartbeat_search_feature_copy(text,text,uuid,bigint)',
 'sessions.commit_search_feature_copy(text,text,uuid,bigint,text,jsonb,timestamp with time zone)','sessions.finish_search_feature_copy(text,text,uuid,bigint,text,text)'] LOOP
 EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated,service_role',f);
 FOR r IN SELECT roles.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles roles ON roles.oid=a.grantee WHERE p.oid=f::regprocedure AND a.grantee<>p.proowner LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',f,r);END LOOP;
 END LOOP;
END $acl$;
GRANT EXECUTE ON FUNCTION sessions.register_search_feature_source(text,text,text,text,text),sessions.claim_search_feature_copy(text,text,text,uuid),
sessions.initialize_search_feature_baseline(text,text,uuid,bigint,text,bigint),sessions.heartbeat_search_feature_copy(text,text,uuid,bigint),
sessions.commit_search_feature_copy(text,text,uuid,bigint,text,jsonb,timestamp with time zone),sessions.finish_search_feature_copy(text,text,uuid,bigint,text,text) TO service_role;
