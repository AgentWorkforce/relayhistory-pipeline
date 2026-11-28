/**
 * TEST-ONLY stand-in for the W2 visibility decision (docs/design/org-bridge-gate.md), shared by the PGlite proof and
 * the real-PostgreSQL orderings. Never a migration: it lives in schema `cutover_proof` of a test database only.
 */
/** The stand-in: one visibility row per (effect, session), published under the org hold key EXCLUSIVE. */
export const STAND_IN = `
CREATE SCHEMA cutover_proof;
CREATE TABLE cutover_proof.effects (
  effect_org text NOT NULL, kind text NOT NULL, digest text NOT NULL, session_key text NOT NULL,
  published_snapshot pg_snapshot NOT NULL, PRIMARY KEY (effect_org, kind, digest, session_key));
CREATE FUNCTION cutover_proof.publish(p_org text, p_kind text, p_digest text, p_session text, p_bound pg_snapshot) RETURNS text
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN RAISE EXCEPTION USING ERRCODE = '40001'; END IF;
  -- EXCLUSIVE: waits for every in-flight write to any of the 15 sources (their capture triggers hold it SHARED).
  PERFORM pg_advisory_xact_lock(sessions.ingest_org_admission_hold_key(p_org));
  -- Later statements: fresh READ COMMITTED snapshots taken after the grant.
  IF NOT EXISTS (SELECT 1 FROM sessions.ingest_org_admission_holds h WHERE h.held_org = p_org AND h.state <> 'released') THEN
    RETURN 'refused_released';
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.ingest_org_erasure_history h WHERE h.erased_org = p_org AND NOT pg_visible_in_snapshot(h.xid, p_bound)) THEN
    RETURN 'refused_erased';
  END IF;
  INSERT INTO cutover_proof.effects VALUES (p_org, p_kind, p_digest, p_session, pg_current_snapshot()) ON CONFLICT DO NOTHING;
  RETURN 'visible';
END $$;
-- The reader's verdict (batched in the design; one effect here). Revoked: a history row of the org committed after
-- the publish snapshot (org-scoped: 062340 records no session).
CREATE FUNCTION cutover_proof.verdict(p_org text, p_kind text, p_digest text, p_session text) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
  SELECT CASE
    WHEN e.digest IS NULL THEN 'absent'
    WHEN EXISTS (SELECT 1 FROM sessions.ingest_org_erasure_history h WHERE h.erased_org = p_org AND NOT pg_visible_in_snapshot(h.xid, e.published_snapshot)) THEN 'revoked'
    ELSE 'visible' END
  FROM (SELECT 1) AS one LEFT JOIN cutover_proof.effects AS e
    ON e.effect_org = p_org AND e.kind = p_kind AND e.digest = p_digest AND e.session_key = p_session
$$;`;

