BEGIN;
DROP FUNCTION sessions.ingest_publication_scoped_candidates_v1(jsonb);
DROP FUNCTION sessions.ingest_publication_scoped_claim_v1(jsonb);
DROP FUNCTION sessions.ingest_publication_scoped_heartbeat_v1(jsonb);
DROP FUNCTION sessions.ingest_publication_scoped_prepare_v1(jsonb);
DROP FUNCTION sessions.ingest_publication_scoped_register_objects_v1(jsonb);
DROP FUNCTION sessions.ingest_publication_scoped_publish_v1(jsonb);
DROP FUNCTION sessions.ingest_publication_scoped_abandon_v1(jsonb);
DROP FUNCTION publication_private.claim(jsonb);
DROP FUNCTION publication_private.heartbeat(jsonb);
DROP FUNCTION publication_private.prepare(jsonb);
DROP FUNCTION publication_private.register_objects(jsonb);
DROP FUNCTION publication_private.publish(jsonb);
DROP FUNCTION publication_private.abandon(jsonb);
DROP FUNCTION publication_private.check_window(timestamptz);
DROP FUNCTION publication_private.gate(jsonb);
DO $$ BEGIN
 IF EXISTS(SELECT FROM publication_private.admission) THEN
   RAISE EXCEPTION 'scoped publication: retain admission history; disable grants and reconcile before rollback';
 END IF;
END $$;
DROP TABLE publication_private.admission;
DROP SCHEMA publication_private;
COMMIT;
