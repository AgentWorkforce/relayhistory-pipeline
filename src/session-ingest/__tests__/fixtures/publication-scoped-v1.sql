-- Disposable single-stage contract rehearsal. Never a production migration.
-- Synthetic, preverified admission binds an exact authority image and one stage.
CREATE TABLE sessions.fixture_scoped_admission (
  id uuid PRIMARY KEY, binding jsonb NOT NULL, stage uuid UNIQUE NOT NULL,
  frontier_image jsonb NOT NULL, admission_image jsonb NOT NULL,
  stage_sha text NOT NULL, enabled boolean NOT NULL DEFAULT true,
  attempt uuid UNIQUE, lease uuid, consumed boolean NOT NULL DEFAULT false
);
REVOKE ALL ON sessions.fixture_scoped_admission FROM PUBLIC, bulls96_session_ingest;

CREATE FUNCTION sessions.fixture_scoped_gate(p jsonb) RETURNS sessions.fixture_scoped_admission
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a sessions.fixture_scoped_admission; b jsonb := p->'binding'; s sessions.ingest_stage_intents; f sessions.ingest_publication_frontiers; d sessions.ingest_buffer_admission;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 SELECT * INTO STRICT a FROM sessions.fixture_scoped_admission WHERE id=(b->>'admissionId')::uuid;
 IF b IS DISTINCT FROM a.binding OR p->>'generation' IS DISTINCT FROM b#>>'{target,generation}'
    OR (p ? 'stageToken' AND p->>'stageToken' IS DISTINCT FROM a.stage::text)
    OR (p ? 'bucket' AND p->>'bucket' IS DISTINCT FROM b#>>'{target,bucket}')
    OR (p ? 'region' AND p->>'region' IS DISTINCT FROM b#>>'{target,region}')
    OR (p ? 'keyRoot' AND p->>'keyRoot' IS DISTINCT FROM b#>>'{target,keyRoot}')
 THEN RAISE EXCEPTION 'scoped fixture: binding mismatch'; END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 PERFORM sessions.ingest_lock_fences(b#>>'{tenant,orgId}',b#>>'{tenant,workspaceId}',b#>>'{tenant,userId}');
 SELECT * INTO STRICT a FROM sessions.fixture_scoped_admission WHERE id=(b->>'admissionId')::uuid FOR UPDATE;
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=a.stage;
 SELECT * INTO STRICT f FROM sessions.ingest_publication_frontiers WHERE (org_id,workspace_id,user_id,origin_id)=(s.org_id,s.workspace_id,s.user_id,s.origin_id);
 SELECT * INTO STRICT d FROM sessions.ingest_buffer_admission WHERE org_id=s.org_id;
 IF b IS DISTINCT FROM a.binding OR NOT a.enabled OR a.stage::text IS DISTINCT FROM COALESCE(p->>'stageToken',a.stage::text)
    OR (s.org_id,s.workspace_id,s.user_id,s.origin_id) IS DISTINCT FROM
       (b#>>'{tenant,orgId}',b#>>'{tenant,workspaceId}',b#>>'{tenant,userId}',b#>>'{tenant,originId}')
    OR (f.target_bucket,f.target_region,f.key_root,f.schema_generation) IS DISTINCT FROM
       (b#>>'{target,bucket}',b#>>'{target,region}',b#>>'{target,keyRoot}',b#>>'{target,generation}')
    OR s.stage_sha256 IS DISTINCT FROM a.stage_sha OR to_jsonb(f) IS DISTINCT FROM a.frontier_image
    OR to_jsonb(d) IS DISTINCT FROM a.admission_image OR least(f.valid_until,d.valid_until)<=clock_timestamp()
 THEN RAISE EXCEPTION 'scoped fixture: authority stale'; END IF;
 RETURN a;
END $$;
REVOKE ALL ON FUNCTION sessions.fixture_scoped_gate(jsonb) FROM PUBLIC, bulls96_session_ingest;

CREATE FUNCTION sessions.ingest_publication_scoped_candidates_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a sessions.fixture_scoped_admission;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','limit','generation','bucket','region','keyRoot']);
 IF p->'limit' IS DISTINCT FROM '1'::jsonb THEN RAISE EXCEPTION 'scoped fixture: limit'; END IF;
 a:=sessions.fixture_scoped_gate(p);
 RETURN jsonb_build_object('stageTokens',CASE WHEN NOT a.consumed AND a.attempt IS NULL AND EXISTS(
  SELECT 1 FROM sessions.ingest_stage_intents s JOIN sessions.ingest_buffer_outbox o ON o.active_stage_token=s.stage_token
  LEFT JOIN sessions.ingest_publication_deferrals d ON d.stage_token=s.stage_token
  WHERE s.stage_token=a.stage AND s.receipt_state='acknowledged' AND s.cleanup_state='live' AND o.state='buffered'
  AND (d.retry_after IS NULL OR d.retry_after<=clock_timestamp()) AND s.gc_replacement_stage_token IS NULL
  AND (s.gc_lease_until IS NULL OR s.gc_lease_until<=clock_timestamp())
  AND jsonb_array_length(sessions.ingest_buffer_gc_kept(s.stage_token))=jsonb_array_length(s.group_ordinals))
  THEN jsonb_build_array(a.stage) ELSE '[]'::jsonb END);
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_claim_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a sessions.fixture_scoped_admission; ticket jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','stageToken','generation','bucket','region','keyRoot']);
 a:=sessions.fixture_scoped_gate(p);
 IF a.consumed OR a.attempt IS NOT NULL THEN RAISE EXCEPTION 'scoped fixture: admission busy'; END IF;
 ticket:=sessions.ingest_publication_claim(p-'binding');
 UPDATE sessions.fixture_scoped_admission SET attempt=(ticket->>'attemptToken')::uuid,lease=(ticket->>'leaseToken')::uuid WHERE id=a.id;
 RETURN ticket;
END $$;

CREATE FUNCTION sessions.ingest_publication_scoped_publish_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a sessions.fixture_scoped_admission; result jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','stageToken','generation','attemptToken','leaseToken','messages']);
 a:=sessions.fixture_scoped_gate(p);
 IF a.attempt IS NULL OR a.lease IS NULL OR (a.attempt::text,a.lease::text) IS DISTINCT FROM
     (p->>'attemptToken',p->>'leaseToken') THEN RAISE EXCEPTION 'scoped fixture: attempt mismatch'; END IF;
 result:=sessions.ingest_publication_publish(p-'binding'-'stageToken');
 IF result->'published' IS DISTINCT FROM 'true'::jsonb THEN RAISE EXCEPTION 'scoped fixture: incomplete'; END IF;
 UPDATE sessions.fixture_scoped_admission SET consumed=true WHERE id=a.id;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_candidates_v1(jsonb),sessions.ingest_publication_scoped_claim_v1(jsonb),sessions.ingest_publication_scoped_publish_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_candidates_v1(jsonb),sessions.ingest_publication_scoped_claim_v1(jsonb),sessions.ingest_publication_scoped_publish_v1(jsonb) TO bulls96_session_ingest;
REVOKE EXECUTE ON FUNCTION sessions.ingest_publication_candidates(jsonb),sessions.ingest_publication_claim(jsonb),sessions.ingest_publication_publish(jsonb) FROM bulls96_session_ingest;
