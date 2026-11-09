-- TEST-ONLY proposal to fold into the reserved, unapplied summary migration.
-- This is not a migration slot, a release claim, or authority to apply a database.
ALTER TABLE sessions.session_directory
  ADD COLUMN recorded_catalog_title text
  CHECK (recorded_catalog_title IS NULL OR
    (char_length(recorded_catalog_title) BETWEEN 1 AND 90
      AND recorded_catalog_title = btrim(recorded_catalog_title)
      AND recorded_catalog_title !~ '[[:cntrl:]]')),
  ADD CONSTRAINT session_directory_recorded_title_live CHECK
    (recorded_catalog_title IS NULL OR (deleted_at IS NULL AND event_projection_ready));

COMMENT ON COLUMN sessions.session_directory.recorded_catalog_title IS
  'Bounded already-scrubbed recorded session catalog title, written only after a complete canonical projection; below native brief and pointer title in display precedence.';

CREATE FUNCTION sessions.session_directory_recorded_title_invalidate()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $recorded$
DECLARE invalidated boolean;
BEGIN
  invalidated := NEW.deleted_at IS NOT NULL OR NEW.event_projection_ready IS NOT TRUE;
  IF TG_OP = 'UPDATE' THEN
    invalidated := invalidated OR NEW.event_projection_epoch IS DISTINCT FROM OLD.event_projection_epoch;
  END IF;
  IF invalidated THEN
    NEW.recorded_catalog_title := NULL;
    -- A derived title may not retain invalidated recorded content. Never retain an
    -- old model title here as a fallback; normal projectors recompute priority.
    IF TG_OP = 'UPDATE' AND OLD.recorded_catalog_title IS NOT NULL
      AND NEW.title IS NOT DISTINCT FROM OLD.recorded_catalog_title THEN
      NEW.title := NULL;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.recorded_catalog_title IS NOT NULL THEN
      NEW.search_text := '';
    END IF;
  END IF;
  RETURN NEW;
END
$recorded$;
CREATE TRIGGER session_directory_recorded_title_invalidate
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_recorded_title_invalidate();
REVOKE ALL ON FUNCTION sessions.session_directory_recorded_title_invalidate() FROM PUBLIC;
DO $optional_roles$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['anon','authenticated','service_role','search_consumer_runtime'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=role_name) THEN
      EXECUTE format('REVOKE ALL ON FUNCTION sessions.session_directory_recorded_title_invalidate() FROM %I', role_name);
    END IF;
  END LOOP;
END
$optional_roles$;
