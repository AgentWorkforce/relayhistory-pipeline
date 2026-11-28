-- Slot 20261105062380 (allocated by layered-sessions-pipeline; 062360 and 062370 are reserved for other lanes).
-- The session binding: a server-issued, immutable id for one INCARNATION of a logical session.
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- Why. A logical session is keyed (org_id, workspace_id, harness, session_id), and that key alone is not an
-- incarnation: once a session is tombstoned and later comes back as a genuinely new upload, or its directory row is
-- erased and re-created, the same key names different content. A consumer that attaches anything to the key (a
-- Cloud project assignment, docs/session-read-api-design.md "Session bindings") would then inherit it silently.
-- The binding id names exactly one incarnation, so such a consumer keys on (workspace_id, session_binding_id) and
-- revalidates it against the live binding on every read, failing closed.
--
-- What decides an incarnation is the existing directory lifecycle, not anything new or client-supplied:
--  * a new directory row (first projection, or re-creation after a hard delete) is a new incarnation;
--  * a tombstone -> live transition is a new incarnation. The projector allows that only for a genuinely new pointer
--    revision above the machine's resurrection fence (decision 0002, src/hoopdb/session-directory-projector.ts); an
--    operator-erased session is fenced for good (20261105050000), so it never comes back at all;
--  * an UPDATE that changes the row's key (org, workspace, harness or session id) is a new incarnation under the new
--    key: a binding never follows a row to another key;
--  * every other write (a retry, a re-sent or new revision, a reprojection, a catalog or summary write, a tombstone)
--    keeps the binding. A tombstoned row keeps its retired id; the reader never serves it.
--
-- Identity and order. session_binding_id (uuid) identifies a binding; session_binding_revision orders them: it is
-- drawn from one sequence (sessions.session_binding_revision) at every mint, so a later binding of the same key
-- always carries a strictly greater revision, even across a hard delete and re-creation of the row (the sequence is
-- not stored on the row). A consumer compares the exact (id, revision) pair with the live one; created_at is
-- informational and never an order.
--
-- Hard delete. No migration hard-deletes directory rows: the lifecycle ends in a tombstone that keeps the retired
-- binding. Only operator tooling (eraseSessionDirectory, the legacy controlled re-projection) deletes rows; a row
-- re-inserted after that gets a new id and a greater revision, so a held binding fails closed.
--
-- What this does NOT decide. Whether a first-seen record that arrives after a tombstone is old work or a new upload is
-- the existing projector rule (a new pointer revision above the machine's resurrection fence is new); this migration
-- adds no ingest evidence. It guarantees only that nothing keyed on a binding crosses an incarnation boundary as the
-- directory draws it: such a record lands under a NEW binding, never the retired one.
--
-- The columns: nullable, NO default and NO backfill, so adding them rewrites nothing; lock_timeout (5s) bounds the
-- brief ACCESS EXCLUSIVE lock, and a timeout fails the migration (55P03) without a partial change. A live row written
-- before this migration has no binding until its next directory write mints one (lazily, in that write's
-- transaction). Until then the reader returns nothing for it, which a consumer reads as "no binding" (fail closed).
--
-- The trigger is the only writer: BEFORE INSERT OR UPDATE on every column, named zz_ so it runs after every other
-- BEFORE trigger (aa_session_directory_hide_turn_only can set deleted_at) and sees the final deleted_at. Any value a
-- caller writes into the three columns is overwritten. Concurrent first inserts serialize on the primary key: the
-- loser takes ON CONFLICT DO UPDATE, whose BEFORE UPDATE sees the winner's row and keeps its id, so exactly one id
-- is ever issued per incarnation.
--
-- The reader: sessions.session_directory_bindings_for, the directory list's read-role tenant scope (42501), SECURITY
-- DEFINER, EXECUTE to bulls96_read_api alone. It returns the live binding of each requested key that has one (at
-- most 200 keys, primary-key lookups); a tombstoned, unknown, out-of-tenant or not yet minted key is simply absent.
-- Errors: 42501 scope; 22023 invalid arguments.

SET LOCAL lock_timeout='5s';

ALTER TABLE sessions.session_directory
  ADD COLUMN session_binding_id uuid NULL,
  ADD COLUMN session_binding_revision bigint NULL,
  ADD COLUMN session_binding_created_at timestamptz NULL;

-- All three or none. NOT VALID: every row is all-NULL now, so no scan is needed; new writes are checked.
ALTER TABLE sessions.session_directory ADD CONSTRAINT session_directory_binding_pair
  CHECK ((session_binding_id IS NULL) = (session_binding_created_at IS NULL)
     AND (session_binding_id IS NULL) = (session_binding_revision IS NULL)) NOT VALID;

-- The binding order. Only the trigger function (its owner) draws from it.
CREATE SEQUENCE sessions.session_binding_revision AS bigint;
REVOKE ALL ON SEQUENCE sessions.session_binding_revision FROM PUBLIC, anon, authenticated, service_role;

COMMENT ON COLUMN sessions.session_directory.session_binding_id IS
  'Server-issued id of this logical session''s current incarnation; written only by zz_session_directory_binding.';

-- SECURITY DEFINER only to draw from the owner-only sequence: every directory writer role fires it.
CREATE FUNCTION sessions.session_directory_binding()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $binding$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.deleted_at IS NULL THEN
   NEW.session_binding_id:=gen_random_uuid();NEW.session_binding_created_at:=clock_timestamp();
   NEW.session_binding_revision:=nextval('sessions.session_binding_revision');
  ELSE
   NEW.session_binding_id:=NULL;NEW.session_binding_created_at:=NULL;NEW.session_binding_revision:=NULL;
  END IF;
 ELSIF NEW.deleted_at IS NULL AND (OLD.deleted_at IS NOT NULL OR OLD.session_binding_id IS NULL
   OR (OLD.org_id, OLD.workspace_id, OLD.harness, OLD.session_id) IS DISTINCT FROM (NEW.org_id, NEW.workspace_id, NEW.harness, NEW.session_id)) THEN
  -- A tombstone -> live transition, or a key change, is a new incarnation; a live row without a binding (written
  -- before 062380) gets its first one.
  NEW.session_binding_id:=gen_random_uuid();NEW.session_binding_created_at:=clock_timestamp();
  NEW.session_binding_revision:=nextval('sessions.session_binding_revision');
 ELSE
  -- Every other write keeps the binding, including a tombstone (the retired id stays on the row, never served).
  NEW.session_binding_id:=OLD.session_binding_id;NEW.session_binding_created_at:=OLD.session_binding_created_at;
  NEW.session_binding_revision:=OLD.session_binding_revision;
 END IF;
 RETURN NEW;
END $binding$;

REVOKE ALL ON FUNCTION sessions.session_directory_binding() FROM PUBLIC, anon, authenticated, service_role;

CREATE TRIGGER zz_session_directory_binding BEFORE INSERT OR UPDATE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_binding();

CREATE FUNCTION sessions.session_directory_bindings_for(
 p_workspace_id text, p_harnesses text[], p_session_ids text[]
) RETURNS TABLE(harness text, session_id text, session_binding_id uuid, session_binding_revision bigint,
                session_binding_created_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $bindings$
DECLARE v_org text;
BEGIN
 IF p_workspace_id IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='session bindings: workspace is required';
 END IF;
 -- Role guard and tenant pin; refuses (42501) before any value is read.
 v_org := sessions.session_directory_read_api_scope(p_workspace_id);
 IF p_harnesses IS NULL OR p_session_ids IS NULL
    OR cardinality(p_harnesses) <> cardinality(p_session_ids)
    OR cardinality(p_harnesses) NOT BETWEEN 1 AND 200
    OR array_position(p_harnesses, NULL) IS NOT NULL OR array_position(p_session_ids, NULL) IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='session bindings: 1 to 200 paired, non-null keys';
 END IF;
 RETURN QUERY
 SELECT d.harness, d.session_id, d.session_binding_id, d.session_binding_revision, d.session_binding_created_at
   FROM (SELECT DISTINCT k.h, k.s FROM unnest(p_harnesses, p_session_ids) AS k(h, s)) AS k
   JOIN sessions.session_directory AS d
     ON d.org_id = v_org AND d.workspace_id = p_workspace_id AND d.harness = k.h AND d.session_id = k.s
  WHERE d.deleted_at IS NULL AND d.session_binding_id IS NOT NULL
  ORDER BY d.harness COLLATE "C", d.session_id COLLATE "C";
END
$bindings$;

REVOKE ALL ON FUNCTION sessions.session_directory_bindings_for(text, text[], text[])
 FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_bindings_for(text, text[], text[])
 TO bulls96_read_api;

-- As 034000: strip EXECUTE from every other grantee present at creation (default privileges), on both functions.
DO $revoke$
DECLARE v_grantee text; v_fn regprocedure;
BEGIN
 FOREACH v_fn IN ARRAY ARRAY['sessions.session_directory_bindings_for(text, text[], text[])'::regprocedure,
                             'sessions.session_directory_binding()'::regprocedure] LOOP
  FOR v_grantee IN SELECT r.rolname FROM pg_proc p
   CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
   JOIN pg_roles r ON r.oid = a.grantee WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE'
    AND a.grantee <> p.proowner
    AND NOT (v_fn = 'sessions.session_directory_bindings_for(text, text[], text[])'::regprocedure AND r.rolname = 'bulls96_read_api')
  LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee); END LOOP;
 END LOOP;
END
$revoke$;
