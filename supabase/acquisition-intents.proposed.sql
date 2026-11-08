-- PROPOSED DDL for the native issuer's durable state (src/acquisition/intent-store.ts,
-- PgAcquisitionStore). NOT a migration: deliberately outside supabase/migrations so no
-- migrator picks it up. The SQL lane (feat/ingest-acquisition-owner-20261107) folds it
-- into its migration, assigns the registry slot, and owns the role creation and the
-- boundary/pg-proof tests. Never apply this file directly to any database.
--
-- Trust model: only bulls96_acquisition_installer (the role in ACQUISITION_DATABASE_URL)
-- may read or write these tables. Every other runtime role is explicitly revoked. G is
-- stored only as AES-256-GCM ciphertext (ACQUISITION_INTENT_KEY, held by the service);
-- the installation proof is stored only as its domain-separated SHA-256.

CREATE TABLE IF NOT EXISTS sessions.ingest_acquisition_native_installations (
  installation_id uuid PRIMARY KEY,
  request_uuid    uuid NOT NULL UNIQUE,                 -- client idempotency key (P3a)
  org_id          text NOT NULL,
  workspace_id    text NOT NULL,
  user_id         text NOT NULL,
  origin_id       text NOT NULL,
  label           text,
  proof_sha256    text NOT NULL CHECK (proof_sha256 ~ '^[0-9a-f]{64}$'),
  retired_at      timestamptz,
  created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS sessions.ingest_acquisition_native_intents (
  operation_uuid        uuid PRIMARY KEY,               -- install: also the grant id
  kind                  text NOT NULL CHECK (kind IN ('install', 'revoke', 'rebind')),
  installation_id       uuid NOT NULL REFERENCES sessions.ingest_acquisition_native_installations(installation_id),
  org_id                text NOT NULL,
  workspace_id          text NOT NULL,
  source                text NOT NULL,
  session_id            text NOT NULL,
  target_grant_id       uuid,                           -- revoke/rebind: the grant targeted
  origin_id             text,                           -- install: ownerOriginId; rebind: newOriginId
  assertion             bytea NOT NULL,                 -- A, exactly as signed (retries never re-sign)
  signature             bytea NOT NULL CHECK (octet_length(signature) = 64),
  evidence_sha256       text NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
  request_sha256        text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
  grant_ciphertext      bytea,                          -- install only: AES-256-GCM(G) || tag
  grant_nonce           bytea CHECK (grant_nonce IS NULL OR octet_length(grant_nonce) = 12),
  status                text NOT NULL CHECK (status IN ('reserved', 'pending', 'applied', 'refused', 'void')),
  refusal_code          text,
  result                text,                           -- SQL result as text (bigints exact)
  client_operation_uuid uuid UNIQUE,                    -- revoke: the Desktop operationUuid that activated it
  activation_reason     text CHECK (activation_reason IN ('explicit_exclude', 'installation_retired', 'session_removed')),
  created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((kind = 'install') = (grant_ciphertext IS NOT NULL AND grant_nonce IS NOT NULL)),
  CHECK (kind = 'install' OR target_grant_id IS NOT NULL),
  CHECK (status <> 'reserved' OR kind = 'revoke')
);

-- Exactly one reserved (pre-signed) revoke per installed grant.
CREATE UNIQUE INDEX IF NOT EXISTS ingest_acquisition_native_intents_one_revoke
  ON sessions.ingest_acquisition_native_intents (target_grant_id) WHERE kind = 'revoke';
CREATE INDEX IF NOT EXISTS ingest_acquisition_native_intents_pending_revokes
  ON sessions.ingest_acquisition_native_intents (created_at) WHERE kind = 'revoke' AND status = 'pending';
CREATE INDEX IF NOT EXISTS ingest_acquisition_native_intents_by_installation
  ON sessions.ingest_acquisition_native_intents (installation_id, kind);

ALTER TABLE sessions.ingest_acquisition_native_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_acquisition_native_installations FORCE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_acquisition_native_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_acquisition_native_intents FORCE ROW LEVEL SECURITY;

-- Revoke from every runtime role (skipping roles absent on this deployment), then grant
-- the installer exactly the DML PgAcquisitionStore issues. No DELETE, no TRUNCATE.
DO $$
DECLARE
  v_role text;
BEGIN
  FOREACH v_role IN ARRAY ARRAY['public', 'anon', 'authenticated', 'service_role', 'bulls96_session_ingest', 'bulls96_session_ingest_seeder'] LOOP
    IF v_role = 'public' THEN
      REVOKE ALL ON sessions.ingest_acquisition_native_installations, sessions.ingest_acquisition_native_intents FROM PUBLIC;
    ELSIF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
      EXECUTE format('REVOKE ALL ON sessions.ingest_acquisition_native_installations, sessions.ingest_acquisition_native_intents FROM %I', v_role);
    END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_acquisition_installer') THEN
    GRANT SELECT, INSERT, UPDATE (retired_at) ON sessions.ingest_acquisition_native_installations TO bulls96_acquisition_installer;
    GRANT SELECT, INSERT, UPDATE (status, refusal_code, result, client_operation_uuid, activation_reason, updated_at)
      ON sessions.ingest_acquisition_native_intents TO bulls96_acquisition_installer;
  ELSE
    RAISE EXCEPTION 'bulls96_acquisition_installer role must exist (created by the SQL lane migration)';
  END IF;
END $$;

CREATE POLICY ingest_acquisition_native_installations_installer ON sessions.ingest_acquisition_native_installations
  FOR ALL TO bulls96_acquisition_installer USING (true) WITH CHECK (true);
CREATE POLICY ingest_acquisition_native_intents_installer ON sessions.ingest_acquisition_native_intents
  FOR ALL TO bulls96_acquisition_installer USING (true) WITH CHECK (true);
