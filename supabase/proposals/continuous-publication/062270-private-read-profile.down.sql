BEGIN;
DO $$ BEGIN
 IF EXISTS(SELECT FROM frontier_receipt_private.read_profiles) THEN
   RAISE EXCEPTION 'receipt capture: retain enrolled profiles and revocations before removal';
 END IF;
END $$;
DROP FUNCTION frontier_receipt_private.check_request(jsonb);
DROP TABLE frontier_receipt_private.revoked_profiles;
DROP TABLE frontier_receipt_private.read_profiles;
DROP FUNCTION frontier_receipt_private.immutable_record();
DROP SCHEMA frontier_receipt_private;
COMMIT;
