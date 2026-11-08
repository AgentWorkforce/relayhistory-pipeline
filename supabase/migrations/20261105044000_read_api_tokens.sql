-- Self-issued tokens for the session read API (READ_API_AUTH_MODE=self_issued, default off).
-- Code only: applying this needs Khaliq's go, and nothing uses it until the mode is selected.
--
-- Tokens are opaque, generated with 256 bits of CSPRNG entropy, and stored only as the lowercase
-- hex SHA-256 of the full token string. The row is the only thing that can make a token valid, so
-- a lookup answers every request, deleting the row is immediate revocation, and there is no
-- signing key to rotate. Issuer and audience live on the row: `issuer` is fixed, and the read API
-- refuses a row whose audience is not its own origin. Other services never look in this table, so
-- they cannot accept these tokens, and the read API looks nowhere else.
--
-- Tenancy. Org, workspace and user come only from the row. There is no credential broader than one
-- user in one org and workspace, and the scope is fixed to rth:read by a table constraint.
--
-- Access. RLS is on with no policy and no role holds a table privilege. bulls96_read_api gets
-- EXECUTE on the functions below only. Each one is SECURITY DEFINER and refuses any caller but the
-- genuine login (session_user, role 'none'; SET ROLE into it is 42501). The role stays read-only
-- by default: the write functions run only in an explicit BEGIN READ WRITE, and it gains no table
-- privilege. A login that can already set any tenant setting gains no read of session data here.
--
-- Sweep. Issue and refresh remove up to 100 fully expired rows per call (a row is fully expired
-- once its refresh expiry passes; a service token's refresh expiry equals its access expiry).
-- Expired rows are inert before that: verify and refresh filter on expiry.
--
-- Revocation. read_api_token_delete_by_hash ends one session; read_api_token_delete_for ends a
-- token by id, a user's tokens (offboarding, erasure) or a whole org's. Both delete the row, which
-- ends the access token and the refresh family together.

DO $read_api_tokens_role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    RAISE EXCEPTION USING ERRCODE = '42704', MESSAGE = 'read api tokens: role bulls96_read_api does not exist';
  END IF;
END
$read_api_tokens_role$;

CREATE TABLE sessions.read_api_tokens (
  id text PRIMARY KEY,
  token_family_id text NOT NULL,
  subject_type text NOT NULL CHECK (subject_type IN ('cli', 'service')),
  issuer text NOT NULL DEFAULT 'session-read-api' CHECK (issuer = 'session-read-api'),
  audience text NOT NULL CHECK (audience <> ''),
  user_id text NOT NULL CHECK (user_id <> ''),
  org_id text NOT NULL CHECK (org_id <> ''),
  workspace_id text NOT NULL CHECK (workspace_id <> ''),
  scopes jsonb NOT NULL DEFAULT '["rth:read"]'::jsonb CHECK (scopes = '["rth:read"]'::jsonb),
  access_token_hash text NOT NULL UNIQUE CHECK (access_token_hash ~ '^[0-9a-f]{64}$'),
  access_token_expires_at timestamptz NOT NULL,
  refresh_token_hash text NOT NULL UNIQUE CHECK (refresh_token_hash ~ '^[0-9a-f]{64}$'),
  refresh_token_expires_at timestamptz NOT NULL,
  label text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX read_api_tokens_expiry_idx ON sessions.read_api_tokens (refresh_token_expires_at);
CREATE INDEX read_api_tokens_org_user_idx ON sessions.read_api_tokens (org_id, user_id);
ALTER TABLE sessions.read_api_tokens ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.read_api_tokens FROM PUBLIC, anon, authenticated, service_role;

CREATE FUNCTION sessions.read_api_token_verify(p_access_token_hash text, p_now timestamptz)
RETURNS TABLE (id text, subject_type text, audience text, user_id text, org_id text, workspace_id text, scopes jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
  IF session_user <> 'bulls96_read_api' OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'read api tokens: caller is not the read api login';
  END IF;
  -- The later of the caller's clock and the database's: a caller cannot revive an expired token.
  RETURN QUERY
    SELECT t.id, t.subject_type, t.audience, t.user_id, t.org_id, t.workspace_id, t.scopes
      FROM sessions.read_api_tokens AS t
     WHERE t.access_token_hash = p_access_token_hash
       AND t.access_token_expires_at > GREATEST(p_now, now());
END
$fn$;

CREATE FUNCTION sessions.read_api_token_sweep_expired(p_now timestamptz)
RETURNS void
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
  DELETE FROM sessions.read_api_tokens AS t
   WHERE t.id IN (SELECT e.id FROM sessions.read_api_tokens AS e
                   WHERE e.refresh_token_expires_at <= GREATEST(p_now, now())
                   ORDER BY e.refresh_token_expires_at
                   LIMIT 100 FOR UPDATE SKIP LOCKED);
$fn$;

CREATE FUNCTION sessions.read_api_token_issue(
  p_id text, p_family text, p_subject text, p_audience text, p_user text, p_org text, p_workspace text,
  p_access_hash text, p_access_expires timestamptz, p_refresh_hash text, p_refresh_expires timestamptz,
  p_label text, p_now timestamptz)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
  IF session_user <> 'bulls96_read_api' OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'read api tokens: caller is not the read api login';
  END IF;
  PERFORM sessions.read_api_token_sweep_expired(p_now);
  INSERT INTO sessions.read_api_tokens (id, token_family_id, subject_type, audience, user_id, org_id, workspace_id,
    access_token_hash, access_token_expires_at, refresh_token_hash, refresh_token_expires_at, label, created_at, updated_at)
  VALUES (p_id, p_family, p_subject, p_audience, p_user, p_org, p_workspace,
    p_access_hash, p_access_expires, p_refresh_hash, p_refresh_expires, p_label, p_now, p_now);
END
$fn$;

CREATE FUNCTION sessions.read_api_token_refresh(
  p_refresh_hash text, p_user text, p_org text, p_workspace text, p_audience text,
  p_next_access_hash text, p_access_expires timestamptz, p_next_refresh_hash text, p_refresh_expires timestamptz,
  p_now timestamptz)
RETURNS TABLE (id text, subject_type text, audience text, user_id text, org_id text, workspace_id text, scopes jsonb)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
  IF session_user <> 'bulls96_read_api' OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'read api tokens: caller is not the read api login';
  END IF;
  PERFORM sessions.read_api_token_sweep_expired(p_now);
  -- One compare-and-set: of two concurrent refreshes of one token exactly one matches, and a
  -- service token (subject_type) or another identity's token never does.
  RETURN QUERY
    UPDATE sessions.read_api_tokens AS t
       SET access_token_hash = p_next_access_hash, access_token_expires_at = p_access_expires,
           refresh_token_hash = p_next_refresh_hash, refresh_token_expires_at = p_refresh_expires, updated_at = p_now
     WHERE t.refresh_token_hash = p_refresh_hash AND t.subject_type = 'cli'
       AND t.user_id = p_user AND t.org_id = p_org AND t.workspace_id = p_workspace AND t.audience = p_audience
       AND t.refresh_token_expires_at > GREATEST(p_now, now())
    RETURNING t.id, t.subject_type, t.audience, t.user_id, t.org_id, t.workspace_id, t.scopes;
END
$fn$;

CREATE FUNCTION sessions.read_api_token_delete_by_hash(p_kind text, p_hash text)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE v_count integer;
BEGIN
  IF session_user <> 'bulls96_read_api' OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'read api tokens: caller is not the read api login';
  END IF;
  DELETE FROM sessions.read_api_tokens AS t
   WHERE (p_kind = 'access' AND t.access_token_hash = p_hash)
      OR (p_kind = 'refresh' AND t.refresh_token_hash = p_hash);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count > 0;
END
$fn$;

CREATE FUNCTION sessions.read_api_token_delete_for(p_org text, p_id text, p_user text, p_workspace text)
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE v_count integer;
BEGIN
  IF session_user <> 'bulls96_read_api' OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'read api tokens: caller is not the read api login';
  END IF;
  -- The org predicate is in the WHERE clause: an id from another org matches nothing.
  DELETE FROM sessions.read_api_tokens AS t
   WHERE t.org_id = p_org
     AND (p_id IS NULL OR t.id = p_id)
     AND (p_user IS NULL OR t.user_id = p_user)
     AND (p_workspace IS NULL OR t.workspace_id = p_workspace);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END
$fn$;

REVOKE ALL ON FUNCTION sessions.read_api_token_verify(text, timestamptz) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.read_api_token_sweep_expired(timestamptz) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.read_api_token_issue(text, text, text, text, text, text, text, text, timestamptz, text, timestamptz, text, timestamptz) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.read_api_token_refresh(text, text, text, text, text, text, timestamptz, text, timestamptz, timestamptz) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.read_api_token_delete_by_hash(text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.read_api_token_delete_for(text, text, text, text) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION sessions.read_api_token_verify(text, timestamptz) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.read_api_token_issue(text, text, text, text, text, text, text, text, timestamptz, text, timestamptz, text, timestamptz) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.read_api_token_refresh(text, text, text, text, text, text, timestamptz, text, timestamptz, timestamptz) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.read_api_token_delete_by_hash(text, text) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.read_api_token_delete_for(text, text, text, text) TO bulls96_read_api;
