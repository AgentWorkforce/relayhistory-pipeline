-- SYNTHETIC SOURCE AND TARGET. Never add this file to a release or use it to enroll a user.
-- Separate owners populate the enrollment/expected set and the observed terminal
-- receipts. The scoped reader has no table privileges or write interface.
CREATE SCHEMA fixture_receipt_inventory;
REVOKE ALL ON SCHEMA fixture_receipt_inventory FROM PUBLIC;
CREATE TABLE fixture_receipt_inventory.enrollment (
 profile_id uuid PRIMARY KEY,
 tenant jsonb NOT NULL, generation text NOT NULL, target jsonb NOT NULL, authority jsonb NOT NULL,
 terminal_counts jsonb NOT NULL,
 domain_sha text NOT NULL CHECK (domain_sha ~ '^[a-f0-9]{64}$')
);
CREATE TABLE fixture_receipt_inventory.expected (
 profile_id uuid NOT NULL REFERENCES fixture_receipt_inventory.enrollment,
 source text NOT NULL CHECK (source IN ('native','equal_revision_loser','legacy','queue','s3_first')),
 ack_id text NOT NULL, decision jsonb NOT NULL CHECK (octet_length(decision::text)<=8192), PRIMARY KEY(profile_id,source,ack_id)
);
CREATE TABLE fixture_receipt_inventory.observed (
 profile_id uuid NOT NULL, source text NOT NULL, ack_id text NOT NULL,
 decision jsonb NOT NULL CHECK (octet_length(decision::text)<=8192), terminal boolean NOT NULL, PRIMARY KEY(profile_id,source,ack_id)
);

CREATE FUNCTION sessions.ingest_scoped_publication_receipt_capture_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog, pg_temp AS $$
DECLARE profile frontier_receipt_private.read_profiles; enrollment fixture_receipt_inventory.enrollment;
 actual_counts jsonb; n int; domain_hash text; page jsonb; last_row jsonb; response jsonb;
 after_key text[]; wanted int; observed_count int;
BEGIN
 profile:=frontier_receipt_private.check_request(p);
 SELECT * INTO enrollment FROM fixture_receipt_inventory.enrollment e WHERE e.profile_id=profile.profile_id;
 IF NOT FOUND OR (profile.tenant,profile.generation,profile.target,profile.expected_authority) IS DISTINCT FROM
    (enrollment.tenant,enrollment.generation,enrollment.target,enrollment.authority) THEN
   RAISE EXCEPTION 'receipt inventory: enrollment held';
 END IF;
 -- A small fixed fixture limit prevents an accidental high-volume proof.
 wanted:=least(profile.max_decisions,64);
 SELECT count(*) INTO n FROM (SELECT 1 FROM fixture_receipt_inventory.expected e
   WHERE e.profile_id=profile.profile_id LIMIT wanted+1) bounded;
 SELECT count(*) INTO observed_count FROM (SELECT 1 FROM fixture_receipt_inventory.observed o
   WHERE o.profile_id=profile.profile_id LIMIT wanted+1) bounded;
 IF n>wanted OR observed_count>wanted THEN RAISE EXCEPTION 'receipt inventory: work bound'; END IF;
 SELECT jsonb_object_agg(s,counted) INTO actual_counts FROM (
   SELECT s,count(e.ack_id) counted FROM unnest(ARRAY['native','equal_revision_loser','legacy','queue','s3_first']) s
   LEFT JOIN fixture_receipt_inventory.expected e ON e.profile_id=profile.profile_id AND e.source=s GROUP BY s
 ) counts;
 IF actual_counts<>enrollment.terminal_counts OR EXISTS(
   SELECT 1 FROM (SELECT * FROM fixture_receipt_inventory.expected WHERE profile_id=profile.profile_id) e
   FULL JOIN (SELECT * FROM fixture_receipt_inventory.observed WHERE profile_id=profile.profile_id) o USING (source,ack_id)
   WHERE e.ack_id IS NULL OR o.ack_id IS NULL OR NOT o.terminal OR o.decision IS DISTINCT FROM e.decision
     OR e.decision->'tenant' IS DISTINCT FROM profile.tenant
 ) THEN RAISE EXCEPTION 'receipt inventory: terminal comparison held'; END IF;
 -- One canonical decision per cursor. Ambiguous provenance stays held here.
 IF EXISTS (SELECT FROM fixture_receipt_inventory.observed o WHERE o.profile_id=profile.profile_id
   GROUP BY o.decision->>'recordId',o.decision->>'revision',o.decision->>'revisionId',o.decision->>'candidateDigest' HAVING count(*)>1)
 THEN RAISE EXCEPTION 'receipt inventory: ambiguous cursor'; END IF;
 SELECT encode(sha256(convert_to(E'first-ack-seat-domain-v1\n'||coalesce(string_agg(encoded,'' ORDER BY cursor_key COLLATE "C"),''),'UTF8')),'hex')
 INTO domain_hash FROM (
   SELECT ARRAY[d->>'recordId',d->>'revision',d->>'revisionId',d->>'candidateDigest'] AS cursor_key,
     'row:'||(SELECT string_agg(CASE WHEN field IS NULL THEN '-1:' ELSE octet_length(convert_to(field,'UTF8'))::text||':'||field END,'' ORDER BY ord)
       FROM unnest(ARRAY[d#>>'{tenant,orgId}',d#>>'{tenant,workspaceId}',d#>>'{tenant,userId}',d#>>'{tenant,originId}',
         d->>'recordId',d->>'revision',d->>'revisionId',d->>'source',d->>'sessionId',d->>'seat',
         d->>'bodyAccepted',d->>'candidateDigest',d->>'firstDigest']) WITH ORDINALITY f(field,ord)) AS encoded
   FROM (SELECT o.decision AS d FROM fixture_receipt_inventory.observed o WHERE o.profile_id=profile.profile_id) observed
 ) framed;
 IF domain_hash<>enrollment.domain_sha THEN RAISE EXCEPTION 'receipt inventory: independent root mismatch'; END IF;
 IF p->'after'<>'null'::jsonb THEN
   IF jsonb_typeof(p->'after')<>'array' OR jsonb_array_length(p->'after')<>4 THEN RAISE EXCEPTION 'receipt inventory: cursor'; END IF;
   IF EXISTS(SELECT FROM jsonb_array_elements(p->'after') v WHERE jsonb_typeof(v)<>'string')
   THEN RAISE EXCEPTION 'receipt inventory: cursor'; END IF;
   SELECT array_agg(v ORDER BY i) INTO after_key FROM jsonb_array_elements_text(p->'after') WITH ORDINALITY k(v,i);
 END IF;
 SELECT coalesce(jsonb_agg(d ORDER BY k COLLATE "C"),'[]'::jsonb) INTO page FROM (
   SELECT d,k FROM (SELECT o.decision d,ARRAY[o.decision->>'recordId',o.decision->>'revision',o.decision->>'revisionId',o.decision->>'candidateDigest'] k
     FROM fixture_receipt_inventory.observed o WHERE o.profile_id=profile.profile_id) candidates
   WHERE after_key IS NULL OR k COLLATE "C">after_key COLLATE "C" ORDER BY k COLLATE "C" LIMIT (p->>'limit')::int
 ) selected;
 last_row:=page->-1;
 response:=jsonb_build_object('protocol',p->>'protocol','profileId',profile.profile_id,'profileSha256',profile.profile_sha256,
   'tenant',profile.tenant,'generation',profile.generation,'authority',enrollment.authority,'snapshot',pg_current_snapshot()::text,
   'decisionCount',n,'decisionsSha256',domain_hash,'rows',page,'next',CASE WHEN last_row IS NULL THEN NULL ELSE
     jsonb_build_array(last_row->>'recordId',last_row->>'revision',last_row->>'revisionId',last_row->>'candidateDigest') END);
 IF octet_length(response::text)>profile.max_response_bytes THEN RAISE EXCEPTION 'receipt inventory: response bound'; END IF;
 RETURN response;
END $$;
REVOKE ALL ON FUNCTION sessions.ingest_scoped_publication_receipt_capture_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_scoped_publication_receipt_capture_v1(jsonb) TO bulls96_frontier_receipt_reader;
