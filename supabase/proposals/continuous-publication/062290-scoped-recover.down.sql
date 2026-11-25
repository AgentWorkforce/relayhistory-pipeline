BEGIN;
DO $$ BEGIN
 IF EXISTS(SELECT FROM publication_private.expired_tickets) THEN
   RAISE EXCEPTION 'scoped publication: retain recovery receipts; reconcile before rollback';
 END IF;
END $$;
DROP FUNCTION sessions.ingest_publication_scoped_recover_v1(jsonb);
DROP TABLE publication_private.expired_tickets;
COMMIT;
