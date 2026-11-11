-- Server-owned upload consent (src/session-ingest/upload-consent.ts, docs/upload-consent.md).
--
-- The batch wire carries no sharing mode, so the receiver keeps its own record of what each
-- authenticated principal (org, workspace, user) chose. Keyed by principal, never by the
-- client-generated origin_id, so a rotated origin cannot pick up a fresh or empty policy.
-- No transcript content is stored: session identities, decisions, and two timestamps.
--
-- Writes: sessions.upload_consent_apply_v1 (the ingest login). Narrowing needs no trust;
-- widening needs p_trusted, which the app sets only for a signed identity carrying the
-- rth:consent scope. Containment holds (sessions.upload_consent_holds) have NO function:
-- only the owner (an operator, runbooks/upload-consent-containment.md) inserts them.
-- A person's confirmed write can release a hold on their own principal, never an org-wide one.

CREATE TABLE sessions.upload_consent_policies (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
  mode text NOT NULL CHECK (mode IN ('selected','new','all')),
  revision bigint NOT NULL CHECK (revision > 0),
  new_session_boundary_ms bigint CHECK ((mode = 'new') = (new_session_boundary_ms IS NOT NULL)),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, user_id)
);
CREATE TABLE sessions.upload_consent_sessions (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
  source text NOT NULL, session_id text NOT NULL,
  decision text CHECK (decision IN ('allow','deny')),
  decided_revision bigint,
  first_seen_ms bigint NOT NULL,
  first_activity_ms bigint,
  PRIMARY KEY (org_id, workspace_id, user_id, source, session_id)
);
CREATE TABLE sessions.upload_consent_origins (
  org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL, origin_id text NOT NULL,
  confirmed_revision bigint NOT NULL,
  confirmed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, user_id, origin_id)
);
-- '' in workspace_id / user_id widens the hold to every workspace / user of the org.
CREATE TABLE sessions.upload_consent_holds (
  hold_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL, workspace_id text NOT NULL DEFAULT '', user_id text NOT NULL DEFAULT '',
  reason_code text NOT NULL CHECK (reason_code ~ '^[a-z0-9_]{1,64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  released_by text CHECK (released_by IN ('operator','person'))
);
CREATE INDEX upload_consent_holds_active ON sessions.upload_consent_holds (org_id) WHERE released_at IS NULL;

ALTER TABLE sessions.upload_consent_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.upload_consent_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.upload_consent_origins ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.upload_consent_holds ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.upload_consent_policies, sessions.upload_consent_sessions, sessions.upload_consent_origins, sessions.upload_consent_holds
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;

-- One snapshot for one batch. Records first-seen (server clock) and the earliest claimed first
-- activity for each named session, then answers what is known. p_sessions:
-- [{"source":..,"session_id":..,"first_activity_ms":<int|absent>}], at most 4096.
CREATE FUNCTION sessions.upload_consent_snapshot_v1(
  p_org text, p_workspace text, p_user text, p_origin text, p_sessions jsonb, p_now_ms bigint
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $consent_snapshot$
DECLARE v_policy sessions.upload_consent_policies%ROWTYPE; v_sessions jsonb;
BEGIN
  IF p_org IS NULL OR p_workspace IS NULL OR p_user IS NULL OR p_origin IS NULL OR p_now_ms IS NULL
     OR jsonb_typeof(p_sessions) IS DISTINCT FROM 'array' OR jsonb_array_length(p_sessions) > 4096 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'upload consent snapshot: invalid input';
  END IF;
  INSERT INTO sessions.upload_consent_sessions AS c (org_id, workspace_id, user_id, source, session_id, first_seen_ms, first_activity_ms)
  SELECT DISTINCT ON (s->>'source', s->>'session_id') p_org, p_workspace, p_user, s->>'source', s->>'session_id', p_now_ms,
    CASE WHEN jsonb_typeof(s->'first_activity_ms') = 'number' THEN (s->>'first_activity_ms')::numeric::bigint END
  FROM jsonb_array_elements(p_sessions) s
  WHERE s->>'source' IS NOT NULL AND s->>'session_id' IS NOT NULL
  ORDER BY s->>'source', s->>'session_id',
    CASE WHEN jsonb_typeof(s->'first_activity_ms') = 'number' THEN (s->>'first_activity_ms')::numeric END ASC NULLS LAST
  ON CONFLICT (org_id, workspace_id, user_id, source, session_id) DO UPDATE
    SET first_activity_ms = CASE
      WHEN EXCLUDED.first_activity_ms IS NULL THEN c.first_activity_ms
      WHEN c.first_activity_ms IS NULL THEN EXCLUDED.first_activity_ms
      ELSE least(c.first_activity_ms, EXCLUDED.first_activity_ms) END;
  SELECT * INTO v_policy FROM sessions.upload_consent_policies
    WHERE org_id = p_org AND workspace_id = p_workspace AND user_id = p_user;
  SELECT coalesce(jsonb_agg(jsonb_build_object('source', c.source, 'session_id', c.session_id, 'decision', c.decision,
           'first_seen_ms', c.first_seen_ms, 'first_activity_ms', c.first_activity_ms)), '[]'::jsonb)
    INTO v_sessions
    FROM sessions.upload_consent_sessions c
    JOIN (SELECT DISTINCT s->>'source' AS source, s->>'session_id' AS session_id FROM jsonb_array_elements(p_sessions) s) n
      ON n.source = c.source AND n.session_id = c.session_id
    WHERE c.org_id = p_org AND c.workspace_id = p_workspace AND c.user_id = p_user;
  RETURN jsonb_build_object(
    'held', EXISTS (SELECT 1 FROM sessions.upload_consent_holds h WHERE h.released_at IS NULL AND h.org_id = p_org
                      AND h.workspace_id IN ('', p_workspace) AND h.user_id IN ('', p_user)),
    'policy', CASE WHEN v_policy.org_id IS NULL THEN NULL ELSE jsonb_build_object('mode', v_policy.mode, 'revision', v_policy.revision,
                'new_session_boundary_ms', v_policy.new_session_boundary_ms) END,
    'origin_confirmed', EXISTS (SELECT 1 FROM sessions.upload_consent_origins o WHERE o.org_id = p_org AND o.workspace_id = p_workspace
                                  AND o.user_id = p_user AND o.origin_id = p_origin),
    'sessions', v_sessions);
END;
$consent_snapshot$;

-- One write, one transaction, one revision. Returns {"ok":true,"revision":n} or
-- {"ok":false,"code":"untrusted"|"revision"|"invalid"}; never partially applied.
CREATE FUNCTION sessions.upload_consent_apply_v1(
  p_org text, p_workspace text, p_user text, p_write jsonb, p_trusted boolean, p_now_ms bigint
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $consent_apply$
DECLARE
  v_cur sessions.upload_consent_policies%ROWTYPE; v_mode text; v_rev bigint; v_boundary bigint;
  v_rank_old int; v_rank_new int; v_broadens boolean;
BEGIN
  IF p_org IS NULL OR p_workspace IS NULL OR p_user IS NULL OR p_now_ms IS NULL OR p_trusted IS NULL
     OR jsonb_typeof(p_write) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'upload consent apply: invalid input';
  END IF;
  IF (p_write ? 'mode') AND p_write->>'mode' NOT IN ('selected','new','all') THEN RETURN jsonb_build_object('ok', false, 'code', 'invalid'); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(coalesce(p_write->'allow','[]')) a JOIN jsonb_array_elements(coalesce(p_write->'deny','[]')) d
             ON a->>'source' = d->>'source' AND a->>'session_id' = d->>'session_id') THEN
    RETURN jsonb_build_object('ok', false, 'code', 'invalid');
  END IF;
  -- Serialize every write for the principal, even its first.
  PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array('upload_consent', p_org, p_workspace, p_user)::text, 0));
  SELECT * INTO v_cur FROM sessions.upload_consent_policies
    WHERE org_id = p_org AND workspace_id = p_workspace AND user_id = p_user FOR UPDATE;
  IF (p_write ? 'expected_revision') AND (p_write->>'expected_revision')::bigint <> coalesce(v_cur.revision, 0) THEN
    RETURN jsonb_build_object('ok', false, 'code', 'revision');
  END IF;
  v_mode := coalesce(p_write->>'mode', v_cur.mode, 'selected');
  v_rank_old := CASE v_cur.mode WHEN 'selected' THEN 0 WHEN 'new' THEN 1 WHEN 'all' THEN 2 END;
  v_rank_new := CASE v_mode WHEN 'selected' THEN 0 WHEN 'new' THEN 1 WHEN 'all' THEN 2 END;
  v_broadens := jsonb_array_length(coalesce(p_write->'allow','[]')) > 0
    OR jsonb_array_length(coalesce(p_write->'confirm_origins','[]')) > 0
    OR coalesce((p_write->>'release_hold')::boolean, false)
    OR ((p_write ? 'mode') AND (CASE WHEN v_cur.org_id IS NULL THEN v_rank_new > 0 ELSE v_rank_new > v_rank_old END));
  IF v_broadens AND NOT p_trusted THEN RETURN jsonb_build_object('ok', false, 'code', 'untrusted'); END IF;
  v_rev := coalesce(v_cur.revision, 0) + 1;
  v_boundary := CASE WHEN v_mode <> 'new' THEN NULL WHEN v_cur.mode = 'new' THEN v_cur.new_session_boundary_ms ELSE p_now_ms END;
  INSERT INTO sessions.upload_consent_policies (org_id, workspace_id, user_id, mode, revision, new_session_boundary_ms, updated_at)
    VALUES (p_org, p_workspace, p_user, v_mode, v_rev, v_boundary, now())
  ON CONFLICT (org_id, workspace_id, user_id) DO UPDATE
    SET mode = EXCLUDED.mode, revision = EXCLUDED.revision, new_session_boundary_ms = EXCLUDED.new_session_boundary_ms, updated_at = now();
  INSERT INTO sessions.upload_consent_sessions AS c (org_id, workspace_id, user_id, source, session_id, decision, decided_revision, first_seen_ms)
  SELECT p_org, p_workspace, p_user, x.source, x.session_id, x.decision, v_rev, p_now_ms
  FROM (SELECT DISTINCT a->>'source' AS source, a->>'session_id' AS session_id, 'allow' AS decision FROM jsonb_array_elements(coalesce(p_write->'allow','[]')) a
        UNION ALL
        SELECT DISTINCT d->>'source', d->>'session_id', 'deny' FROM jsonb_array_elements(coalesce(p_write->'deny','[]')) d) x
  WHERE x.source IS NOT NULL AND x.session_id IS NOT NULL
  ON CONFLICT (org_id, workspace_id, user_id, source, session_id) DO UPDATE
    SET decision = EXCLUDED.decision, decided_revision = EXCLUDED.decided_revision;
  INSERT INTO sessions.upload_consent_origins (org_id, workspace_id, user_id, origin_id, confirmed_revision)
  SELECT DISTINCT p_org, p_workspace, p_user, o, v_rev FROM jsonb_array_elements_text(coalesce(p_write->'confirm_origins','[]')) o
  ON CONFLICT DO NOTHING;
  IF coalesce((p_write->>'release_hold')::boolean, false) THEN
    UPDATE sessions.upload_consent_holds SET released_at = now(), released_by = 'person'
      WHERE released_at IS NULL AND org_id = p_org AND workspace_id = p_workspace AND user_id = p_user;
  END IF;
  RETURN jsonb_build_object('ok', true, 'revision', v_rev);
END;
$consent_apply$;

REVOKE ALL ON FUNCTION sessions.upload_consent_snapshot_v1(text,text,text,text,jsonb,bigint),
  sessions.upload_consent_apply_v1(text,text,text,jsonb,boolean,bigint)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.upload_consent_snapshot_v1(text,text,text,text,jsonb,bigint),
  sessions.upload_consent_apply_v1(text,text,text,jsonb,boolean,bigint)
  TO bulls96_session_ingest;
