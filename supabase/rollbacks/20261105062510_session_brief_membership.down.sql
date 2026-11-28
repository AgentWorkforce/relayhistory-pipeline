-- Remove T130 consumed-record membership (062510): the read function, the retirement trigger and
-- function, and the two evidence tables. Membership evidence is derived data and is dropped with
-- them; briefs, pointers and every read surface before 062510 are untouched.
-- Guard, removal and ledger removal are one transaction under the migrator lock.
-- On refusal the caller must ROLLBACK or disconnect.
-- Code only: hosted execution needs Khaliq's separate explicit approval.
BEGIN;
SELECT pg_advisory_xact_lock(961996, 1);
DO $rollback_guard$
DECLARE v_sha text;
BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief membership: migration ledger required';
 END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations
   WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062510_session_brief_membership.sql') THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief membership: a later migration is applied; roll it back first';
 END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations
  WHERE name='upstream:20261105062510_session_brief_membership.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'9b48a3e82a2caf6f07d82b53822dc0e517c35c562b2c8bee5b57e81f196f4660' THEN
  RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='brief membership: own applied checksum required';
 END IF;
END
$rollback_guard$;
DROP FUNCTION sessions.session_brief_membership_for(text,text,text,text);
DROP TRIGGER session_brief_evidence_retire ON sessions.session_briefs;
DROP FUNCTION sessions.session_brief_evidence_retire();
DROP TABLE sessions.session_brief_members;
DROP TABLE sessions.session_brief_evidence;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062510_session_brief_membership.sql';
COMMIT;
