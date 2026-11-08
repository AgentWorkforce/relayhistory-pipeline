-- PROPOSED DDL for the native issuer's durable state (src/acquisition/intent-store.ts,
-- PgAcquisitionStore). NOT a migration: deliberately outside supabase/migrations so no
-- migrator picks it up. The SQL lane (feat/ingest-acquisition-owner-20261107) folds it
-- into its migration, assigns the registry slot, and owns the role creation and the
-- boundary/pg-proof tests. NEVER APPLY THIS FILE DIRECTLY TO ANY DATABASE.
--
-- Conventions (supabase/migrations/20261105042000_babysitter_evidence_retention.sql):
-- plain transactional DDL (no IF NOT EXISTS, no CONCURRENTLY); the role is checked, never
-- created or altered here: absent -> 42704, can log in / elevated / a member of any role
-- -> 55000; every runtime role is revoked explicitly; USAGE on schema sessions is granted;
-- no allow-all row policies.
--
-- ============================================================================================
-- DEVIATION from design §6 (PR #314 docs/factory-acquisition-owner-design.md), SQL lane to rule
-- ============================================================================================
-- §6 has the installer role reach state only through SECURITY DEFINER functions on tables
-- with row level security enabled and no policies. PgAcquisitionStore instead issues plain
-- DML, so this proposal grants the role column-scoped table privileges and does NOT enable
-- row level security: enabling it would require allow-all policies for the role, which the
-- convention forbids. Isolation therefore rests on the grants alone (every other role is
-- revoked; no DELETE, no TRUNCATE, UPDATE only on the columns the store writes). Preferred end
-- state, if the SQL lane rules for §6: wrap each PgAcquisitionStore statement in a SECURITY
-- DEFINER function (SET search_path=pg_catalog,pg_temp; GRANT EXECUTE to the role only),
-- ENABLE ROW LEVEL SECURITY with no policies, and drop the table grants below. The service
-- code changes only inside PgAcquisitionStore. Mirrored in docs/acquisition-exchange.md.
-- ============================================================================================
--
-- Trust model: only bulls96_acquisition_installer (the role in ACQUISITION_DATABASE_URL,
-- NOLOGIN until the operator step) may read or write these tables. G is stored only as
-- AES-256-GCM ciphertext (ACQUISITION_INTENT_KEY, held by the service); the installation
-- proof is stored only as its domain-separated SHA-256.

CREATE TABLE sessions.ingest_acquisition_native_installations (
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

CREATE TABLE sessions.ingest_acquisition_native_intents (
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
  attempts              integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),  -- failed sends (stalled, never terminal, at MAX_SEND_ATTEMPTS)
  next_attempt_at       timestamptz NOT NULL DEFAULT clock_timestamp(),    -- reconciler backoff
  reservation           text CHECK (reservation IN ('signed', 'no_epoch', 'sign_failed')),
  cancel_reason         text CHECK (cancel_reason IN ('explicit_exclude', 'installation_retired', 'session_removed')),  -- install: revoke on outcome
  lease_until           timestamptz,                    -- per-intent send lease (claimSend compare-and-set)
  lease_token           uuid,                           -- minted per claimSend; every completion CASes on it (stale = no-op)
  created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((kind = 'install') = (grant_ciphertext IS NOT NULL AND grant_nonce IS NOT NULL)),
  CHECK (kind = 'install' OR target_grant_id IS NOT NULL),
  CHECK (status <> 'reserved' OR kind = 'revoke'),
  CHECK (reservation IS NULL OR kind = 'install'),
  CHECK (cancel_reason IS NULL OR kind = 'install')
);

-- At most one revoke (reserved, or signed at revocation time) per installed grant.
CREATE UNIQUE INDEX ingest_acquisition_native_intents_one_revoke
  ON sessions.ingest_acquisition_native_intents (target_grant_id) WHERE kind = 'revoke';
-- The reconciler's due queue (every kind): next_attempt_at, then creation order.
CREATE INDEX ingest_acquisition_native_intents_due
  ON sessions.ingest_acquisition_native_intents (next_attempt_at, created_at) WHERE status = 'pending';
CREATE INDEX ingest_acquisition_native_intents_by_installation
  ON sessions.ingest_acquisition_native_intents (installation_id, kind);

DO $acquisition_role$
DECLARE r pg_roles%ROWTYPE;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_acquisition_installer';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='42704',MESSAGE='acquisition: role bulls96_acquisition_installer absent; run the role bootstrap first';END IF;
 -- NOLOGIN here; LOGIN only by the separate operator step. A pre-existing role that can log in,
 -- is elevated or inherits another role's privileges is refused rather than silently granted.
 IF r.rolcanlogin OR r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
  OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='acquisition: role bulls96_acquisition_installer can log in, is elevated or is a member';END IF;
END $acquisition_role$;

REVOKE ALL ON sessions.ingest_acquisition_native_installations,sessions.ingest_acquisition_native_intents
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_acquisition_installer;
GRANT USAGE ON SCHEMA sessions TO bulls96_acquisition_installer;
-- Exactly the DML PgAcquisitionStore issues (DEVIATION above). No DELETE, no TRUNCATE.
-- UPDATE (retired_at) also lets createInstallIntent take SELECT ... FOR SHARE on the installation.
GRANT SELECT, INSERT, UPDATE (retired_at) ON sessions.ingest_acquisition_native_installations TO bulls96_acquisition_installer;
GRANT SELECT, INSERT,
 UPDATE (status, refusal_code, result, client_operation_uuid, activation_reason, attempts, next_attempt_at, reservation,
         cancel_reason, lease_until, lease_token, updated_at)
 ON sessions.ingest_acquisition_native_intents TO bulls96_acquisition_installer;
