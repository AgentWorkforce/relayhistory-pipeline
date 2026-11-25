-- Review component for post-H installation. Apply in one owner transaction only.
-- Dedicated NOLOGIN roles are provisioned by the deployment owner. The source
-- attester alone may authorize a stage. No runtime role can mint admission.
BEGIN;
DO $roles$
DECLARE r record;
BEGIN
 FOR r IN SELECT * FROM pg_catalog.pg_roles WHERE rolname IN ('bulls96_publication_scoped','bulls96_publication_recovery') LOOP
   IF r.rolcanlogin OR r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR
      EXISTS(SELECT FROM pg_catalog.pg_auth_members WHERE member=r.oid) OR
      has_schema_privilege(r.oid,'sessions','CREATE') THEN
     RAISE EXCEPTION 'scoped publication: dedicated role must have no elevated privileges or memberships';
   END IF;
 END LOOP;
END $roles$;
CREATE SCHEMA publication_private;
REVOKE ALL ON SCHEMA publication_private FROM PUBLIC;
CREATE TABLE publication_private.admission (
  id uuid PRIMARY KEY, binding jsonb NOT NULL, stage uuid UNIQUE NOT NULL,
  frontier_image jsonb NOT NULL, admission_image jsonb NOT NULL,
  stage_sha text NOT NULL CHECK (stage_sha ~ '^[a-f0-9]{64}$'),
  valid_until timestamptz NOT NULL,
  enabled boolean NOT NULL DEFAULT false,
  attempt uuid UNIQUE, lease uuid, attempts_recovered int NOT NULL DEFAULT 0 CHECK (attempts_recovered BETWEEN 0 AND 3),
  consumed boolean NOT NULL DEFAULT false,
  CHECK ((attempt IS NULL) = (lease IS NULL)),
  CHECK (binding->>'admissionId' IS NOT DISTINCT FROM id::text),
  CHECK (binding#>>'{target,provider}' IS NOT DISTINCT FROM 's3'),
  CHECK (coalesce(binding->>'planSha256','') ~ '^[a-f0-9]{64}$'),
  CHECK (coalesce(binding->>'frontierSha256','') ~ '^[a-f0-9]{64}$')
);
REVOKE ALL ON publication_private.admission FROM PUBLIC, bulls96_session_ingest;

CREATE FUNCTION publication_private.gate(p jsonb) RETURNS publication_private.admission
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; b jsonb := p->'binding'; s sessions.ingest_stage_intents; f sessions.ingest_publication_frontiers; d sessions.ingest_buffer_admission;
BEGIN
 SELECT * INTO STRICT a FROM publication_private.admission WHERE id=(b->>'admissionId')::uuid;
 IF b IS DISTINCT FROM a.binding OR p->>'generation' IS DISTINCT FROM b#>>'{target,generation}'
    OR (p ? 'stageToken' AND p->>'stageToken' IS DISTINCT FROM a.stage::text)
    OR (p ? 'bucket' AND p->>'bucket' IS DISTINCT FROM b#>>'{target,bucket}')
    OR (p ? 'region' AND p->>'region' IS DISTINCT FROM b#>>'{target,region}')
    OR (p ? 'keyRoot' AND p->>'keyRoot' IS DISTINCT FROM b#>>'{target,keyRoot}')
 THEN RAISE EXCEPTION 'scoped publication: binding mismatch'; END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM sessions.ingest_lock_fences(b#>>'{tenant,orgId}',b#>>'{tenant,workspaceId}',b#>>'{tenant,userId}');
 SELECT * INTO STRICT a FROM publication_private.admission WHERE id=(b->>'admissionId')::uuid FOR UPDATE;
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=a.stage FOR SHARE;
 SELECT * INTO STRICT f FROM sessions.ingest_publication_frontiers WHERE (org_id,workspace_id,user_id,origin_id)=(s.org_id,s.workspace_id,s.user_id,s.origin_id) FOR SHARE;
 SELECT * INTO STRICT d FROM sessions.ingest_buffer_admission WHERE org_id=s.org_id FOR SHARE;
 IF b IS DISTINCT FROM a.binding OR NOT a.enabled OR a.stage::text IS DISTINCT FROM COALESCE(p->>'stageToken',a.stage::text)
    OR (s.org_id,s.workspace_id,s.user_id,s.origin_id) IS DISTINCT FROM
       (b#>>'{tenant,orgId}',b#>>'{tenant,workspaceId}',b#>>'{tenant,userId}',b#>>'{tenant,originId}')
    OR (f.target_bucket,f.target_region,f.key_root,f.schema_generation) IS DISTINCT FROM
       (b#>>'{target,bucket}',b#>>'{target,region}',b#>>'{target,keyRoot}',b#>>'{target,generation}')
    OR s.stage_sha256 IS DISTINCT FROM a.stage_sha OR to_jsonb(f) IS DISTINCT FROM a.frontier_image
    OR to_jsonb(d) IS DISTINCT FROM a.admission_image OR least(f.valid_until,d.valid_until,a.valid_until)<=clock_timestamp()
 THEN RAISE EXCEPTION 'scoped publication: authority stale'; END IF;
 IF jsonb_array_length(sessions.ingest_buffer_gc_kept(a.stage))<>jsonb_array_length(s.group_ordinals) THEN
   RAISE EXCEPTION 'scoped publication: privacy eligibility changed';
 END IF;
 -- These authority rows remain locked until commit. Time can still advance.
 a.valid_until:=least(f.valid_until,d.valid_until,a.valid_until);
 RETURN a;
END $$;
CREATE FUNCTION publication_private.check_window(deadline timestamptz) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF deadline IS NULL OR deadline<=clock_timestamp() THEN
   RAISE EXCEPTION 'scoped publication: authorization expired during operation';
 END IF;
END $$;

-- Specialize the exact reviewed canonical bodies. This retains all generation,
-- privacy, stage, object and head checks. Refuse drift instead of silently
-- copying new upstream semantics. Ordinary functions and ACLs remain intact.
DO $install$
DECLARE op text; pin text; body text;
 guard CONSTANT text := 'PERFORM sessions.ingest_require_role(''bulls96_session_ingest'');';
BEGIN
 FOR op,pin IN SELECT * FROM (VALUES
    ('claim','2fa9e456f0f48149e77f66df914433f4d5735294c0e72900d594cd7f94bf879f'),
    ('heartbeat','bbff58095269eeb0e6bce6e05e5b7245639d2bde53b5181c0a9fa0dab2bb7fb0'),
    ('prepare','985f61eaff8af19c9521b0c37c179885fa352406fe806efd094093af0d55a7ed'),
    ('register_objects','753db1c2f88f2220d8c5f038114bfd8ba29d22a034b9f695c83af33bf9fa8091'),
    ('publish','7ca919ff4e8f0162fcd4a104b621fa581c627bc77b1c42bf0e0fbc2771b19f82'),
    ('abandon','9a74c72e5e80a903a45e3fc1876a43119033663c1ac3ee21b14801e639405dd3')
 ) pins(operation,body_sha) LOOP
   SELECT prosrc INTO STRICT body FROM pg_catalog.pg_proc
    WHERE oid=format('sessions.ingest_publication_%I(jsonb)',op)::regprocedure;
   IF encode(sha256(convert_to(body,'UTF8')),'hex') <> pin THEN
     RAISE EXCEPTION 'scoped publication: canonical body differs for %',op;
   END IF;
   EXECUTE format('CREATE FUNCTION publication_private.%I(p jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS %L',
      op,replace(body,guard,'-- Restricted kernel; caller has validated the scoped admission.'));
 END LOOP;
END $install$;
CREATE FUNCTION sessions.ingest_publication_scoped_candidates_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','limit','generation','bucket','region','keyRoot']);
 IF p->'limit' IS DISTINCT FROM '1'::jsonb THEN RAISE EXCEPTION 'scoped publication: limit'; END IF;
 a:=publication_private.gate(p);
 RETURN jsonb_build_object('stageTokens',CASE WHEN NOT a.consumed AND a.attempt IS NULL AND EXISTS(
  SELECT 1 FROM sessions.ingest_stage_intents s JOIN sessions.ingest_buffer_outbox o ON o.active_stage_token=s.stage_token
  LEFT JOIN sessions.ingest_publication_deferrals d ON d.stage_token=s.stage_token
  WHERE s.stage_token=a.stage AND s.receipt_state='acknowledged' AND s.cleanup_state='live' AND o.state='buffered'
  AND (d.retry_after IS NULL OR d.retry_after<=clock_timestamp()) AND s.gc_replacement_stage_token IS NULL
  AND (s.gc_lease_until IS NULL OR s.gc_lease_until<=clock_timestamp())
  )
  THEN jsonb_build_array(a.stage) ELSE '[]'::jsonb END);
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_claim_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; ticket jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','stageToken','generation','bucket','region','keyRoot']);
 a:=publication_private.gate(p);
 IF a.consumed OR a.attempt IS NOT NULL THEN RAISE EXCEPTION 'scoped publication: admission busy'; END IF;
 IF EXISTS(SELECT FROM sessions.ingest_publication_deferrals WHERE stage_token=a.stage AND retry_after>clock_timestamp()) THEN
   RAISE EXCEPTION 'scoped publication: deferred stage';
 END IF;
 ticket:=publication_private.claim(p-'binding');
 UPDATE publication_private.admission SET attempt=(ticket->>'attemptToken')::uuid,lease=(ticket->>'leaseToken')::uuid WHERE id=a.id;
 PERFORM publication_private.check_window(a.valid_until);
 RETURN ticket;
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_publish_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; result jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','stageToken','generation','attemptToken','leaseToken','messages']);
 a:=publication_private.gate(p);
 IF a.attempt IS NULL OR a.lease IS NULL OR (a.attempt::text,a.lease::text) IS DISTINCT FROM
     (p->>'attemptToken',p->>'leaseToken') THEN RAISE EXCEPTION 'scoped publication: attempt mismatch'; END IF;
 result:=publication_private.publish(p-'binding'-'stageToken');
 IF result->'published' IS DISTINCT FROM 'true'::jsonb THEN RAISE EXCEPTION 'scoped publication: incomplete'; END IF;
 UPDATE publication_private.admission SET consumed=true WHERE id=a.id;
 PERFORM publication_private.check_window(a.valid_until);
 RETURN result;
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_heartbeat_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; result jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 a:=publication_private.gate(p);
 IF a.consumed OR a.attempt IS NULL OR a.lease IS NULL OR
    (a.stage::text,a.attempt::text,a.lease::text) IS DISTINCT FROM
    (p->>'stageToken',p->>'attemptToken',p->>'leaseToken')
 THEN RAISE EXCEPTION 'scoped publication: attempt mismatch'; END IF;
 result:=publication_private.heartbeat(p-'binding'-'stageToken');
 PERFORM publication_private.check_window(a.valid_until);
 RETURN result;
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_prepare_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; result jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 a:=publication_private.gate(p);
 IF a.consumed OR a.attempt IS NULL OR a.lease IS NULL OR
    (a.stage::text,a.attempt::text,a.lease::text) IS DISTINCT FROM
    (p->>'stageToken',p->>'attemptToken',p->>'leaseToken')
 THEN RAISE EXCEPTION 'scoped publication: attempt mismatch'; END IF;
 result:=publication_private.prepare(p-'binding'-'stageToken');
 PERFORM publication_private.check_window(a.valid_until);
 RETURN result;
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_register_objects_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; result jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 a:=publication_private.gate(p);
 IF a.consumed OR a.attempt IS NULL OR a.lease IS NULL OR
    (a.stage::text,a.attempt::text,a.lease::text) IS DISTINCT FROM
    (p->>'stageToken',p->>'attemptToken',p->>'leaseToken')
 THEN RAISE EXCEPTION 'scoped publication: attempt mismatch'; END IF;
 result:=publication_private.register_objects(p-'binding'-'stageToken');
 PERFORM publication_private.check_window(a.valid_until);
 RETURN result;
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_abandon_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; result jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_scoped');
 a:=publication_private.gate(p);
 IF a.consumed OR a.attempt IS NULL OR a.lease IS NULL OR
    (a.stage::text,a.attempt::text,a.lease::text) IS DISTINCT FROM
    (p->>'stageToken',p->>'attemptToken',p->>'leaseToken')
 THEN RAISE EXCEPTION 'scoped publication: attempt mismatch'; END IF;
 result:=publication_private.abandon(p-'binding'-'stageToken');
 -- Abandon terminates this admission; reauthorization requires a new owner decision.
 IF result->'abandoned'='true'::jsonb THEN
   UPDATE publication_private.admission SET enabled=false WHERE id=a.id;
 END IF;
 PERFORM publication_private.check_window(a.valid_until);
 RETURN result;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA publication_private FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_candidates_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_candidates_v1(jsonb) TO bulls96_publication_scoped;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_claim_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_claim_v1(jsonb) TO bulls96_publication_scoped;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_heartbeat_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_heartbeat_v1(jsonb) TO bulls96_publication_scoped;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_prepare_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_prepare_v1(jsonb) TO bulls96_publication_scoped;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_register_objects_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_register_objects_v1(jsonb) TO bulls96_publication_scoped;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_publish_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_publish_v1(jsonb) TO bulls96_publication_scoped;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_abandon_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_abandon_v1(jsonb) TO bulls96_publication_scoped;
-- Refuse environment-specific defaults that would grant an alternative path.
DO $acl$
DECLARE role_name text; op text;
BEGIN
 FOREACH role_name IN ARRAY ARRAY['bulls96_publication_scoped','bulls96_publication_recovery'] LOOP
   IF has_schema_privilege(role_name,'publication_private','USAGE') OR
      has_schema_privilege(role_name,'publication_private','CREATE') THEN
     RAISE EXCEPTION 'scoped publication: private schema is reachable';
   END IF;
   FOREACH op IN ARRAY ARRAY['candidates','claim','heartbeat','prepare','register_objects','publish','abandon','defer'] LOOP
     IF has_function_privilege(role_name,format('sessions.ingest_publication_%I(jsonb)',op),'EXECUTE') THEN
       RAISE EXCEPTION 'scoped publication: ordinary function is reachable';
     END IF;
   END LOOP;
 END LOOP;
END $acl$;
COMMIT;
