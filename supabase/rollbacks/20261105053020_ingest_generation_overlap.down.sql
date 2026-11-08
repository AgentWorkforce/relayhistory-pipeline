-- Generated: restore the exact 050000 ingest_require_generation and 055000 ingest_lazy_guard (strict PASS
-- equality, no overlap window), then drop the overlap window table and its four owner-only functions.
-- Owner/ACL of the two restored helpers stay unchanged. Close any open window first (it stops admitting
-- the moment the restored bodies commit either way). Code only: running it against a hosted database
-- needs the user's OK. Explicit transaction: guard, marker fence, replacement and ledger removal are atomic.
-- On an error the caller must ROLLBACK (or disconnect); nothing is committed.
BEGIN;
SELECT pg_advisory_xact_lock(961996,1); -- same namespace as the real migrator
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations')IS NULL OR to_regclass('sessions.__96bulls_role_validation')IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: migration ledger/marker required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105053020_ingest_generation_overlap.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105053020_ingest_generation_overlap.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha<>'da2fcd5011ea2c7ac99d05191ec0113714b50f21645489562188657a464a4a07'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: own applied checksum required';END IF;
 UPDATE sessions.__96bulls_role_validation SET status='pending',updated_at=clock_timestamp() WHERE name='session_directory_search_worker';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='generation overlap: validation marker required';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.ingest_require_generation(p_expected text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $generation$
BEGIN
 IF p_expected IS NULL OR p_expected!~'^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: generation required';END IF;
 PERFORM 1 FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'
  AND generation=p_expected AND status='pass' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: generation not ready';END IF;
END $generation$;
CREATE OR REPLACE FUNCTION sessions.ingest_lazy_guard(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_guard$
DECLARE pol sessions.ingest_lazy_policy%ROWTYPE;c sessions.ingest_receipt_seat_clock%ROWTYPE;v_gen text;
BEGIN
 SELECT generation INTO v_gen FROM sessions.__96bulls_role_validation
  WHERE name='session_directory_search_worker' AND status='pass' FOR SHARE;
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF v_gen IS NULL OR pol.generation<>v_gen OR pol.valid_until<=clock_timestamp()
  OR c.source_identity_sha256<>pol.source_identity_sha256 OR c.control_epoch<>pol.control_epoch
  OR c.baseline->>'sourceProofSha256' IS DISTINCT FROM pol.source_proof_sha256 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody policy unavailable';END IF;
 PERFORM 1 FROM sessions.ingest_buffer_admission WHERE org_id=p_org AND schema_generation=v_gen
  AND control_epoch=pol.control_epoch AND privacy_baseline_sha256=pol.privacy_baseline_sha256
  AND retention_contract_sha256=pol.retention_contract_sha256 AND valid_until>clock_timestamp() FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered admission closed';END IF;
END $lazy_guard$;
DROP TABLE sessions.ingest_generation_overlap_windows;
DROP FUNCTION sessions.ingest_generation_overlap_immutable();
DROP FUNCTION sessions.ingest_generation_overlap_admits(text,text,text);
DROP FUNCTION sessions.ingest_generation_overlap_open(jsonb);
DROP FUNCTION sessions.ingest_generation_overlap_close(jsonb);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105053020_ingest_generation_overlap.sql' AND sha256='da2fcd5011ea2c7ac99d05191ec0113714b50f21645489562188657a464a4a07';
COMMIT;
