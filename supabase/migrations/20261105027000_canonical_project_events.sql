-- F10 standalone ingest-track outbox, registry v2 slot (initial proposal 040640).
-- Depends on canonical_feature_journal + canonical_history_lineage (original
-- 20261104061000/20261104062000, re-slotted 20261105025000/20261105026000).
-- No 610/620 function replacement. No external I/O, transcript text, implicit
-- activation, brief completion or fabricated analysis IDs. A disabled/absent
-- scope row has no effect on publication. Operator enables ONE reviewed scope.
CREATE TABLE sessions.canonical_project_event_scopes(
 org_id text NOT NULL CHECK(org_id~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'),
 workspace_id text NOT NULL CHECK(workspace_id~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'),
 schema_generation text NOT NULL CHECK(schema_generation~'^[0-9a-f]{64}$'),
 enabled boolean NOT NULL DEFAULT false,
 PRIMARY KEY(org_id,workspace_id)
);
CREATE TABLE sessions.canonical_project_events(
 event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 generation bigint NOT NULL CHECK(generation>0),schema_generation text NOT NULL CHECK(schema_generation~'^[0-9a-f]{64}$'),
 authority_id uuid NOT NULL,source_epoch bigint NOT NULL CHECK(source_epoch>0),
 source_fingerprint text NOT NULL CHECK(source_fingerprint~'^[0-9a-f]{64}$'),
 mask_sha256 text NOT NULL CHECK(mask_sha256~'^[0-9a-f]{64}$'),
 publication_kind text NOT NULL CHECK(publication_kind IN('publish','retire')),
 user_id text,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 sent_at timestamptz,acknowledged_at timestamptz,recovery_at timestamptz,
 lease_token uuid,lease_until timestamptz,lease_version bigint NOT NULL DEFAULT 0 CHECK(lease_version>=0),
 UNIQUE(org_id,workspace_id,harness,session_id),
 CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
CREATE INDEX canonical_project_events_pending ON sessions.canonical_project_events(org_id,workspace_id,recorded_at,event_id)WHERE acknowledged_at IS NULL;
ALTER TABLE sessions.canonical_project_event_scopes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.canonical_project_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.canonical_project_event_scopes,sessions.canonical_project_events FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_project_events;

CREATE FUNCTION sessions.canonical_project_event_json(q sessions.canonical_project_events)RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $json$
 SELECT jsonb_build_object('eventId',q.event_id::text,'kind','session.revised','generation',q.generation::text,'schemaGeneration',q.schema_generation,
  'identity',jsonb_build_object('orgId',q.org_id,'workspaceId',q.workspace_id,'source',q.harness,'sessionId',q.session_id,'userId',q.user_id),
  'authorityId',q.authority_id::text,'sourceEpoch',q.source_epoch::text,'sourceFingerprint',q.source_fingerprint,'publicationKind',q.publication_kind)
$json$;
CREATE FUNCTION sessions.canonical_project_events_journal_changed()RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;u text;users integer;
BEGIN
 IF NEW.kind NOT IN('publish','retire')OR NOT EXISTS(SELECT 1 FROM sessions.canonical_project_event_scopes s
  WHERE s.org_id=NEW.org_id AND s.workspace_id=NEW.workspace_id AND s.enabled)THEN RETURN NEW;END IF;
 IF NOT EXISTS(SELECT 1 FROM sessions.canonical_project_event_scopes s WHERE s.org_id=NEW.org_id AND s.workspace_id=NEW.workspace_id
  AND s.enabled AND s.schema_generation=NEW.schema_generation)THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='project events: configured generation changed';END IF;
 SELECT *INTO v FROM sessions.ingest_history_scope_versions WHERE authority_id=(NEW.source_authority->>'authorityId')::uuid
  AND org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND harness=NEW.scope->>'harness'AND session_id=NEW.scope->>'sessionId'
  AND source_epoch::text=NEW.source_authority->>'epoch'AND source_fingerprint=NEW.source_hash;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='project events: frozen authority missing';END IF;
 -- Informational attribution only. Mixed or unknown users stay NULL; never pick
 -- a convenient billable member. Cloud re-authorizes any model spending itself.
 SELECT count(DISTINCT user_id),min(user_id)INTO users,u FROM sessions.session_index
  WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND harness=v.harness AND coalesce(canonical_session_id,session_id)=v.session_id AND deleted_at IS NULL;
 IF users<>1 OR u!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'THEN u:=NULL;END IF;
 INSERT INTO sessions.canonical_project_events AS e(org_id,workspace_id,harness,session_id,generation,schema_generation,authority_id,
  source_epoch,source_fingerprint,mask_sha256,publication_kind,user_id)
 VALUES(NEW.org_id,NEW.workspace_id,v.harness,v.session_id,NEW.revision,NEW.schema_generation,v.authority_id,v.source_epoch,v.source_fingerprint,v.mask_sha256,NEW.kind,u)
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET generation=excluded.generation,schema_generation=excluded.schema_generation,
  authority_id=excluded.authority_id,source_epoch=excluded.source_epoch,source_fingerprint=excluded.source_fingerprint,mask_sha256=excluded.mask_sha256,
  publication_kind=excluded.publication_kind,user_id=excluded.user_id,recorded_at=clock_timestamp(),sent_at=NULL,acknowledged_at=NULL,
  lease_token=NULL,lease_until=NULL,lease_version=e.lease_version+1
 WHERE e.generation<excluded.generation;
 RETURN NEW;
END $changed$;
CREATE TRIGGER canonical_project_events_after_journal AFTER INSERT ON sessions.canonical_feature_journal
 FOR EACH ROW EXECUTE FUNCTION sessions.canonical_project_events_journal_changed();

CREATE FUNCTION sessions.canonical_project_events_guard(p jsonb,p_sender boolean)RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $guard$
BEGIN
 IF p_sender AND coalesce(nullif(current_setting('role',true),'none'),session_user::text)='bulls96_session_ingest'
 THEN PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 ELSE PERFORM sessions.ingest_require_role('bulls96_project_events');END IF;
 IF jsonb_typeof(p->'schemaGeneration')IS DISTINCT FROM'string'OR p->>'schemaGeneration'!~'^[a-f0-9]{64}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='project events: invalid schema generation';END IF;
 PERFORM sessions.ingest_require_generation(p->>'schemaGeneration');
 PERFORM sessions.ingest_exact_keys(p->'scope',ARRAY['orgId','workspaceId']);
 IF jsonb_typeof(p#>'{scope,orgId}')IS DISTINCT FROM'string'OR jsonb_typeof(p#>'{scope,workspaceId}')IS DISTINCT FROM'string'
  OR NOT EXISTS(SELECT 1 FROM sessions.canonical_project_event_scopes s WHERE s.org_id=p#>>'{scope,orgId}'AND s.workspace_id=p#>>'{scope,workspaceId}'
   AND s.enabled AND s.schema_generation=p->>'schemaGeneration')THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='project events: scope not enabled';END IF;
END $guard$;
CREATE FUNCTION sessions.canonical_project_event_is_current(q sessions.canonical_project_events,p_generation text,p_lock boolean)RETURNS boolean
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $current$
DECLARE v sessions.ingest_history_scope_versions%ROWTYPE;
BEGIN
 IF q.publication_kind='retire'THEN
  RETURN EXISTS(SELECT 1 FROM sessions.ingest_history_scope_versions v0 WHERE v0.authority_id=q.authority_id AND v0.state='retired'
   AND v0.org_id=q.org_id AND v0.workspace_id=q.workspace_id AND v0.harness=q.harness AND v0.session_id=q.session_id
   AND v0.source_epoch=q.source_epoch AND v0.source_fingerprint=q.source_fingerprint AND v0.mask_sha256=q.mask_sha256);
 END IF;
 v:=sessions.ingest_history_source_current(jsonb_build_object('generation',p_generation,
  'identity',jsonb_build_object('orgId',q.org_id,'workspaceId',q.workspace_id,'source',q.harness,'sessionId',q.session_id),
  'source',jsonb_build_object('epoch',q.source_epoch::text,'fingerprint',q.source_fingerprint),'authorityId',q.authority_id::text,'maskSha256',q.mask_sha256),p_lock);
 RETURN v.authority_id IS NOT NULL AND v.feature_revision=q.generation;
END $current$;

CREATE FUNCTION sessions.canonical_project_events_claim_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE q sessions.canonical_project_events%ROWTYPE;r jsonb:='[]';n integer;t uuid;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,true);PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','limit']);
 IF jsonb_typeof(p->'limit')IS DISTINCT FROM'number'OR(p->>'limit')::numeric<>trunc((p->>'limit')::numeric)OR(p->>'limit')::numeric NOT BETWEEN 1 AND 50
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='project events: invalid batch bound';END IF;n:=(p->>'limit')::integer;
 FOR q IN SELECT *FROM sessions.canonical_project_events WHERE org_id=p#>>'{scope,orgId}'AND workspace_id=p#>>'{scope,workspaceId}'
  AND schema_generation=p->>'schemaGeneration'AND acknowledged_at IS NULL AND(lease_until IS NULL OR lease_until<=clock_timestamp())
  AND(sent_at IS NULL OR sent_at<=clock_timestamp()-interval'5 minutes')ORDER BY(sent_at IS NOT NULL),recorded_at,event_id LIMIT n FOR UPDATE SKIP LOCKED LOOP
  t:=gen_random_uuid();UPDATE sessions.canonical_project_events SET lease_token=t,lease_until=clock_timestamp()+interval'60 seconds',lease_version=lease_version+1 WHERE event_id=q.event_id RETURNING*INTO q;
  r:=r||jsonb_build_array(jsonb_build_object('event',sessions.canonical_project_event_json(q),'leaseToken',t::text,'leaseVersion',q.lease_version::text));
 END LOOP;RETURN r;
END $claim$;
CREATE FUNCTION sessions.canonical_project_events_sent_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $sent$
DECLARE ok boolean;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,true);PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','eventId','generation','leaseToken','leaseVersion']);
 UPDATE sessions.canonical_project_events SET sent_at=clock_timestamp(),lease_token=NULL,lease_until=NULL
 WHERE event_id=(p->>'eventId')::uuid AND org_id=p#>>'{scope,orgId}'AND workspace_id=p#>>'{scope,workspaceId}'AND schema_generation=p->>'schemaGeneration'
  AND generation=(p->>'generation')::bigint AND lease_token=(p->>'leaseToken')::uuid AND lease_version=(p->>'leaseVersion')::bigint AND lease_until>clock_timestamp()AND acknowledged_at IS NULL;
 ok:=FOUND;RETURN jsonb_build_object('ok',ok);
END $sent$;
CREATE FUNCTION sessions.canonical_project_events_release_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $release$
DECLARE ok boolean;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,true);PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','eventId','generation','leaseToken','leaseVersion']);
 UPDATE sessions.canonical_project_events SET lease_token=NULL,lease_until=NULL WHERE event_id=(p->>'eventId')::uuid
  AND org_id=p#>>'{scope,orgId}'AND workspace_id=p#>>'{scope,workspaceId}'AND schema_generation=p->>'schemaGeneration'AND generation=(p->>'generation')::bigint
  AND lease_token=(p->>'leaseToken')::uuid AND lease_version=(p->>'leaseVersion')::bigint AND lease_until>clock_timestamp()AND acknowledged_at IS NULL;
 ok:=FOUND;RETURN jsonb_build_object('ok',ok);
END $release$;
CREATE FUNCTION sessions.canonical_project_event_read_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $read$
DECLARE q sessions.canonical_project_events%ROWTYPE;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,false);PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','eventId']);
 SELECT*INTO q FROM sessions.canonical_project_events WHERE event_id=(p->>'eventId')::uuid AND org_id=p#>>'{scope,orgId}'AND workspace_id=p#>>'{scope,workspaceId}'
  AND schema_generation=p->>'schemaGeneration'AND acknowledged_at IS NULL;
 IF NOT FOUND THEN RETURN jsonb_build_object('status','missing');END IF;
 IF NOT sessions.canonical_project_event_is_current(q,p->>'schemaGeneration',false)THEN RETURN jsonb_build_object('status','deferred');END IF;
 RETURN jsonb_build_object('status','ready','event',sessions.canonical_project_event_json(q));
END $read$;
CREATE FUNCTION sessions.canonical_project_events_pending_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $pending$
DECLARE r jsonb;n integer;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,false);PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','limit']);
 IF jsonb_typeof(p->'limit')IS DISTINCT FROM'number'OR(p->>'limit')::numeric<>trunc((p->>'limit')::numeric)OR(p->>'limit')::numeric NOT BETWEEN 1 AND 50
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='project events: invalid batch bound';END IF;n:=(p->>'limit')::integer;
 WITH picked AS(SELECT event_id FROM sessions.canonical_project_events WHERE org_id=p#>>'{scope,orgId}'AND workspace_id=p#>>'{scope,workspaceId}'
  AND schema_generation=p->>'schemaGeneration'AND acknowledged_at IS NULL ORDER BY recovery_at NULLS FIRST,recorded_at,event_id LIMIT n FOR UPDATE SKIP LOCKED),
 rotated AS(UPDATE sessions.canonical_project_events e SET recovery_at=clock_timestamp()FROM picked x WHERE e.event_id=x.event_id RETURNING e.event_id,e.generation,e.recorded_at)
 SELECT coalesce(jsonb_agg(jsonb_build_object('eventId',event_id::text,'generation',generation::text)ORDER BY recorded_at,event_id),'[]')INTO r FROM rotated;RETURN r;
END $pending$;
CREATE FUNCTION sessions.canonical_project_event_ack_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $ack$
DECLARE q sessions.canonical_project_events%ROWTYPE;ok boolean;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,false);PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','eventId','generation']);
 SELECT*INTO q FROM sessions.canonical_project_events WHERE event_id=(p->>'eventId')::uuid AND org_id=p#>>'{scope,orgId}'AND workspace_id=p#>>'{scope,workspaceId}'
  AND schema_generation=p->>'schemaGeneration'AND generation=(p->>'generation')::bigint AND acknowledged_at IS NULL;
 IF NOT FOUND OR NOT sessions.canonical_project_event_is_current(q,p->>'schemaGeneration',true)THEN RETURN jsonb_build_object('ok',false);END IF;
 UPDATE sessions.canonical_project_events SET acknowledged_at=clock_timestamp(),lease_token=NULL,lease_until=NULL WHERE event_id=q.event_id AND generation=q.generation
  AND schema_generation=p->>'schemaGeneration'AND acknowledged_at IS NULL;
 ok:=FOUND;RETURN jsonb_build_object('ok',ok);
END $ack$;

REVOKE ALL ON FUNCTION sessions.canonical_project_event_json(sessions.canonical_project_events),sessions.canonical_project_events_journal_changed(),
 sessions.canonical_project_events_guard(jsonb,boolean),sessions.canonical_project_event_is_current(sessions.canonical_project_events,text,boolean),
 sessions.canonical_project_events_claim_v1(jsonb),sessions.canonical_project_events_sent_v1(jsonb),sessions.canonical_project_events_release_v1(jsonb),
 sessions.canonical_project_event_read_v1(jsonb),sessions.canonical_project_events_pending_v1(jsonb),sessions.canonical_project_event_ack_v1(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_project_events;
GRANT USAGE ON SCHEMA sessions TO bulls96_project_events;
GRANT EXECUTE ON FUNCTION sessions.canonical_project_events_claim_v1(jsonb),sessions.canonical_project_events_sent_v1(jsonb),sessions.canonical_project_events_release_v1(jsonb)TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.canonical_project_event_read_v1(jsonb),sessions.canonical_project_events_pending_v1(jsonb),sessions.canonical_project_event_ack_v1(jsonb)TO bulls96_project_events;
