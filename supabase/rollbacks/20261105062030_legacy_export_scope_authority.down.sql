-- Drops the durable legacy-export authority, its narrow exporter/eraser RPCs,
-- the privacy reader's ledger grants/policies and this migration's ledger row.
-- Guard, object removal and ledger removal are one transaction under the
-- migrator lock. On refusal the caller must ROLLBACK or disconnect.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export authority: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062030_legacy_export_scope_authority.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export authority: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062030_legacy_export_scope_authority.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'fcfe6d8915e4f8bb43b2c0eb7726f39d25a23106ee06093b65230c54d01db899'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export authority: own applied checksum required';END IF;
 LOCK TABLE sessions.legacy_export_scope_authority IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.legacy_export_scope_authority)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='legacy export authority: owner mappings present; erase or export-verify before rollback';END IF;
END $rollback_guard$;
DROP POLICY legacy_export_privacy_read_all ON sessions.session_erasure_receipts;
DROP POLICY legacy_export_privacy_read_all ON sessions.upload_deletions;
DROP POLICY legacy_export_privacy_read_all ON sessions.ingest_frozen_source_deletions;
DROP POLICY legacy_export_privacy_read_all ON sessions.upload_resets;
DROP POLICY legacy_export_privacy_read_all ON sessions.operator_user_erasures;
DO $turn_fence_policy$ BEGIN
 IF to_regclass('sessions.turn_session_fences') IS NOT NULL THEN
  DROP POLICY IF EXISTS legacy_export_privacy_read_all ON sessions.turn_session_fences;
  REVOKE SELECT ON sessions.turn_session_fences FROM bulls96_legacy_export_privacy;
 END IF;
END $turn_fence_policy$;
REVOKE SELECT ON sessions.session_erasure_receipts,sessions.upload_deletions,
 sessions.ingest_frozen_source_deletions,sessions.upload_resets,sessions.operator_user_erasures
 FROM bulls96_legacy_export_privacy;
DROP FUNCTION sessions.legacy_export_privacy_eraser_still_fenced(jsonb),
 sessions.legacy_export_privacy_eraser_page(jsonb),
 sessions.legacy_export_privacy_authority_ready(),
 sessions.ingest_legacy_export_scope_authority_register(jsonb);
DROP TABLE sessions.legacy_export_scope_authority;
REVOKE USAGE ON SCHEMA sessions FROM bulls96_legacy_export_privacy;
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062030_legacy_export_scope_authority.sql';
COMMIT;
