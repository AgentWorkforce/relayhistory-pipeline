-- Review component for post-H durable retry. One admission can retire at most
-- three expired tickets. Each replacement needs a new ordinary scoped claim.
BEGIN;
CREATE TABLE publication_private.expired_tickets (
 admission uuid NOT NULL REFERENCES publication_private.admission,
 attempt uuid PRIMARY KEY REFERENCES sessions.ingest_publication_attempts,
 lease uuid NOT NULL,
 recovered_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON publication_private.expired_tickets FROM PUBLIC;
CREATE FUNCTION sessions.ingest_publication_scoped_recover_v1(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE a publication_private.admission; old sessions.ingest_publication_attempts;
 s sessions.ingest_stage_intents;
 attempt_id uuid := (p->>'attemptToken')::uuid; lease_id uuid := (p->>'leaseToken')::uuid;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_publication_recovery');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['binding','stageToken','attemptToken','leaseToken','generation']);
 a:=publication_private.gate(p);
 IF EXISTS(SELECT FROM publication_private.expired_tickets WHERE admission=a.id AND attempt=attempt_id AND lease=lease_id) THEN
   RETURN jsonb_build_object('recovered',true,'replay',true);
 END IF;
 IF a.consumed OR a.attempts_recovered>=3 OR a.attempt IS NULL OR
    (a.attempt,a.lease) IS DISTINCT FROM (attempt_id,lease_id) THEN
   RAISE EXCEPTION 'scoped publication: recovery not admitted';
 END IF;
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=a.stage FOR UPDATE;
 SELECT * INTO STRICT old FROM sessions.ingest_publication_attempts WHERE attempt_token=attempt_id FOR UPDATE;
 IF old.state NOT IN ('admitted','prepared') OR old.lease_until>clock_timestamp()
    OR (old.stage_token,old.lease_token,old.schema_generation) IS DISTINCT FROM (a.stage,lease_id,p->>'generation')
    OR (s.gc_lease_token,s.row_version,s.gc_generation) IS DISTINCT FROM (lease_id,old.stage_version,old.schema_generation)
    OR s.gc_lease_until IS NULL OR s.gc_lease_until>clock_timestamp()
    OR s.gc_replacement_stage_token IS NOT NULL OR s.cleanup_state<>'live' OR s.receipt_state<>'acknowledged'
    OR NOT EXISTS(SELECT FROM sessions.ingest_buffer_outbox WHERE active_stage_token=a.stage AND state='buffered' AND row_version=old.outbox_version FOR SHARE)
 THEN RAISE EXCEPTION 'scoped publication: recovery lease or stage changed'; END IF;
 -- Preserve object registrations and head/GC ownership. The existing claim
 -- kernel alone may acquire expired leases and build a new attempt namespace.
 UPDATE sessions.ingest_publication_attempts SET state='rejected' WHERE attempt_token=attempt_id;
 INSERT INTO publication_private.expired_tickets(admission,attempt,lease) VALUES(a.id,attempt_id,lease_id);
 UPDATE publication_private.admission SET attempt=NULL,lease=NULL,attempts_recovered=attempts_recovered+1 WHERE id=a.id;
 PERFORM publication_private.check_window(a.valid_until);
 RETURN jsonb_build_object('recovered',true,'replay',false);
END $$;
REVOKE ALL ON FUNCTION sessions.ingest_publication_scoped_recover_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_recover_v1(jsonb) TO bulls96_publication_recovery;
COMMIT;
