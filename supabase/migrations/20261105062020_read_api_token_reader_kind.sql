-- Slot 20261105062020. Self-issued read API tokens: record the reader kind at issue
-- (READ_API_MINT_READER_KIND, default off). Code only: applying this to any real database needs
-- Khaliq's separate, explicit OK.
--
-- Adds one nullable column, sessions.read_api_tokens.reader_kind, limited to 'browser', 'device' or
-- 'mcp'. Existing rows, and rows issued with the flag off, keep NULL. Nothing reads the column yet.
--
-- The 044000 issue function is not replaced: read_api_token_issue_v2 is a new function with the same
-- 13 parameters plus p_reader_kind. The read API calls it only when a request carries reader_kind, so
-- with the flag off it runs exactly the 044000 SQL, and code shipped before this migration is applied
-- keeps working. Refresh is unchanged: its UPDATE never touches reader_kind, so a rotated row keeps the
-- value recorded at issue.
--
-- Access is the 044000 model: RLS stays on with no policy, no role gains a table privilege, and the
-- new function is SECURITY DEFINER, pinned to pg_catalog, pg_temp, refuses any caller but the genuine
-- bulls96_read_api login, and is executable by bulls96_read_api only.

DO $read_api_token_reader_kind_role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    RAISE EXCEPTION USING ERRCODE = '42704', MESSAGE = 'read api token reader kind: role bulls96_read_api does not exist';
  END IF;
END
$read_api_token_reader_kind_role$;

ALTER TABLE sessions.read_api_tokens
  ADD COLUMN reader_kind text CONSTRAINT read_api_tokens_reader_kind_check CHECK (reader_kind IN ('browser', 'device', 'mcp'));

CREATE FUNCTION sessions.read_api_token_issue_v2(
  p_id text, p_family text, p_subject text, p_audience text, p_user text, p_org text, p_workspace text,
  p_access_hash text, p_access_expires timestamptz, p_refresh_hash text, p_refresh_expires timestamptz,
  p_label text, p_now timestamptz, p_reader_kind text)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
  IF session_user <> 'bulls96_read_api' OR current_setting('role') <> 'none' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'read api tokens: caller is not the read api login';
  END IF;
  PERFORM sessions.read_api_token_sweep_expired(p_now);
  INSERT INTO sessions.read_api_tokens (id, token_family_id, subject_type, audience, user_id, org_id, workspace_id,
    access_token_hash, access_token_expires_at, refresh_token_hash, refresh_token_expires_at, label, reader_kind,
    created_at, updated_at)
  VALUES (p_id, p_family, p_subject, p_audience, p_user, p_org, p_workspace,
    p_access_hash, p_access_expires, p_refresh_hash, p_refresh_expires, p_label, p_reader_kind, p_now, p_now);
END
$fn$;

REVOKE ALL ON FUNCTION sessions.read_api_token_issue_v2(text, text, text, text, text, text, text, text, timestamptz, text, timestamptz, text, timestamptz, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.read_api_token_issue_v2(text, text, text, text, text, text, text, text, timestamptz, text, timestamptz, text, timestamptz, text) TO bulls96_read_api;
