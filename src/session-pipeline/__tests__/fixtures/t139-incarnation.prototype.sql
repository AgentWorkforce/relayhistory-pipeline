-- T139 OFFLINE PROTOTYPE (design f959bd92 + revision 3 fb6c7fd4), NOT a migration. Loaded by
-- t139-incarnation.postgres.test.ts on a scratch database migrated with every migration.
--
-- R1  k: a transaction-scoped advisory lock on the LOGICAL session (org, workspace, source, session); it exists with no
--     row. EXCLUSIVE: ordinary deletion, reissue, operator erase. SHARED: publication.
-- R2  Active authority, checked under k SHARED right before commit (t139_publish_check).
-- R3  Single-use deletion receipts, consumed by exactly one reissue.
-- Owner scope (Pipeline review of f959bd92): incarnation and receipts are per (logical session, user, origin). One owner's
-- deletion never revokes or reissues another owner's authority; k is shared only for serialization.

CREATE TABLE sessions.t139_session_incarnations (
  org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
  user_id text NOT NULL, origin_id text NOT NULL,
  incarnation bigint NOT NULL CHECK (incarnation >= 0),
  PRIMARY KEY (org_id, workspace_id, source, session_id, user_id, origin_id)
);

CREATE TABLE sessions.t139_deletion_receipts (
  org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
  user_id text NOT NULL, origin_id text NOT NULL,
  incarnation bigint NOT NULL,
  receipt_id uuid NOT NULL DEFAULT gen_random_uuid(),
  deleted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  consumed_by bigint,
  PRIMARY KEY (org_id, workspace_id, source, session_id, user_id, origin_id, incarnation)
);

-- Owners seen publishing under the logical session (Pipeline acceptance of fb6c7fd4: positive reissue only for a
-- DEMONSTRABLY single-owner logical session). Recorded by t139_publish_check when it returns ok; never removed.
CREATE TABLE sessions.t139_session_owners (
  org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
  user_id text NOT NULL, origin_id text NOT NULL,
  PRIMARY KEY (org_id, workspace_id, source, session_id, user_id, origin_id)
);

CREATE FUNCTION sessions.t139_k(p_org text, p_ws text, p_source text, p_session text) RETURNS bigint
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $k$
  SELECT hashtextextended('96bulls:incarnation' || chr(31) || p_org || chr(31) || p_ws || chr(31) || p_source || chr(31) || p_session, 0)
$k$;

CREATE FUNCTION sessions.t139_current(p_org text, p_ws text, p_source text, p_session text, p_user text, p_origin text) RETURNS bigint
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $cur$
  SELECT coalesce((SELECT i.incarnation FROM sessions.t139_session_incarnations AS i
     WHERE i.org_id = p_org AND i.workspace_id = p_ws AND i.source = p_source AND i.session_id = p_session
       AND i.user_id = p_user AND i.origin_id = p_origin), 0)
$cur$;

-- Ordinary deletion of the owner's CURRENT incarnation: k EXCLUSIVE, one receipt (idempotent), then the real
-- ingest_apply_selection (upload_deletions, fence versions, ledger). Returns the deleted incarnation.
CREATE FUNCTION sessions.t139_delete(p_org text, p_ws text, p_user text, p_origin text, p_source text, p_session text) RETURNS bigint
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $del$
DECLARE v_n bigint;
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.t139_k(p_org, p_ws, p_source, p_session));
  v_n := sessions.t139_current(p_org, p_ws, p_source, p_session, p_user, p_origin);
  INSERT INTO sessions.t139_deletion_receipts (org_id, workspace_id, source, session_id, user_id, origin_id, incarnation)
  VALUES (p_org, p_ws, p_source, p_session, p_user, p_origin, v_n) ON CONFLICT DO NOTHING;
  PERFORM sessions.ingest_apply_selection(p_org, p_ws, p_user, p_origin, jsonb_build_array(jsonb_build_array(p_source, p_session)), false);
  RETURN v_n;
END
$del$;

-- Reissue (the claim admitting the first re-created batch): k EXCLUSIVE; refused under an erasure fence; consumes the
-- owner's unconsumed receipt for its current incarnation exactly once. Returns N+1, or NULL when nothing was issued.
CREATE FUNCTION sessions.t139_reissue(p_org text, p_ws text, p_user text, p_origin text, p_source text, p_session text) RETURNS bigint
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $re$
DECLARE v_n bigint; v_c integer;
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.t139_k(p_org, p_ws, p_source, p_session));
  IF EXISTS (SELECT 1 FROM sessions.session_erasure_fences AS f
              WHERE f.org_id = p_org AND f.workspace_id = p_ws AND f.source = p_source AND f.session_id = p_session) THEN
    RETURN NULL;
  END IF;
  -- Ambiguous ownership: any OTHER owner known for this logical session (it published, holds a receipt or an
  -- incarnation) refuses the reissue WITHOUT mutation. Positive reissue is for single-owner sessions only.
  IF EXISTS (SELECT 1 FROM (
        SELECT o.user_id, o.origin_id FROM sessions.t139_session_owners AS o
         WHERE o.org_id = p_org AND o.workspace_id = p_ws AND o.source = p_source AND o.session_id = p_session
        UNION SELECT r.user_id, r.origin_id FROM sessions.t139_deletion_receipts AS r
         WHERE r.org_id = p_org AND r.workspace_id = p_ws AND r.source = p_source AND r.session_id = p_session
        UNION SELECT i.user_id, i.origin_id FROM sessions.t139_session_incarnations AS i
         WHERE i.org_id = p_org AND i.workspace_id = p_ws AND i.source = p_source AND i.session_id = p_session) AS owners
      WHERE (owners.user_id, owners.origin_id) IS DISTINCT FROM (p_user, p_origin)) THEN
    RETURN NULL;
  END IF;
  v_n := sessions.t139_current(p_org, p_ws, p_source, p_session, p_user, p_origin);
  UPDATE sessions.t139_deletion_receipts AS r SET consumed_by = v_n + 1
   WHERE r.org_id = p_org AND r.workspace_id = p_ws AND r.source = p_source AND r.session_id = p_session
     AND r.user_id = p_user AND r.origin_id = p_origin AND r.incarnation = v_n AND r.consumed_by IS NULL;
  GET DIAGNOSTICS v_c = ROW_COUNT;
  IF v_c <> 1 THEN RETURN NULL; END IF;
  INSERT INTO sessions.t139_session_incarnations (org_id, workspace_id, source, session_id, user_id, origin_id, incarnation)
  VALUES (p_org, p_ws, p_source, p_session, p_user, p_origin, v_n + 1)
  ON CONFLICT (org_id, workspace_id, source, session_id, user_id, origin_id) DO UPDATE SET incarnation = excluded.incarnation;
  RETURN v_n + 1;
END
$re$;

-- Operator permanent erase (revision 3): k EXCLUSIVE, then the fence.
CREATE FUNCTION sessions.t139_erase(p_org text, p_ws text, p_source text, p_session text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $er$
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.t139_k(p_org, p_ws, p_source, p_session));
  INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id)
  VALUES (p_org, p_ws, p_source, p_session) ON CONFLICT DO NOTHING;
END
$er$;

-- Publication's active-authority check: k SHARED (held to commit), then: no erasure fence; the manifest's incarnation is
-- the owner's current one; that incarnation has no deletion receipt. 'ok' or the refusal reason.
CREATE FUNCTION sessions.t139_publish_check(p_org text, p_ws text, p_source text, p_session text, p_user text, p_origin text,
                                            p_manifest_incarnation bigint) RETURNS text
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $pc$
DECLARE v_cur bigint;
BEGIN
  PERFORM pg_advisory_xact_lock_shared(sessions.t139_k(p_org, p_ws, p_source, p_session));
  IF EXISTS (SELECT 1 FROM sessions.session_erasure_fences AS f
              WHERE f.org_id = p_org AND f.workspace_id = p_ws AND f.source = p_source AND f.session_id = p_session) THEN
    RETURN 'erased';
  END IF;
  v_cur := sessions.t139_current(p_org, p_ws, p_source, p_session, p_user, p_origin);
  IF p_manifest_incarnation <> v_cur THEN RETURN 'incarnation_moved'; END IF;
  IF EXISTS (SELECT 1 FROM sessions.t139_deletion_receipts AS r
              WHERE r.org_id = p_org AND r.workspace_id = p_ws AND r.source = p_source AND r.session_id = p_session
                AND r.user_id = p_user AND r.origin_id = p_origin AND r.incarnation = v_cur) THEN
    RETURN 'incarnation_deleted';
  END IF;
  INSERT INTO sessions.t139_session_owners (org_id, workspace_id, source, session_id, user_id, origin_id)
  VALUES (p_org, p_ws, p_source, p_session, p_user, p_origin) ON CONFLICT DO NOTHING;
  RETURN 'ok';
END
$pc$;
