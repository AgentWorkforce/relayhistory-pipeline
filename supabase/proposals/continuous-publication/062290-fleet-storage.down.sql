-- Proposed 062290 component. The final down composes this inside its outer
-- transaction and latest-step/checksum guard. Do not drop work to allow rollback.
SET LOCAL row_security = off;
LOCK TABLE sessions.publication_scheduler_fleets,sessions.publication_scheduler_scopes,
  sessions.publication_fleet_cursors,sessions.publication_retry_checkpoints,sessions.publication_receipt_retries
  IN ACCESS EXCLUSIVE MODE;
DO $preserve$
BEGIN
  IF EXISTS (SELECT FROM sessions.publication_scheduler_fleets)
     OR EXISTS (SELECT FROM sessions.publication_scheduler_scopes)
     OR EXISTS (SELECT FROM sessions.publication_fleet_cursors)
     OR EXISTS (SELECT FROM sessions.publication_retry_checkpoints)
     OR EXISTS (SELECT FROM sessions.publication_receipt_retries)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication scheduler: durable state or access bindings remain';
  END IF;
END $preserve$;
DROP TABLE sessions.publication_receipt_retries,sessions.publication_retry_checkpoints,
  sessions.publication_fleet_cursors,sessions.publication_scheduler_scopes,sessions.publication_scheduler_fleets;
