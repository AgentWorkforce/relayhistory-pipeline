-- Reverse of 20261105025000. Only valid after 20261105026000 is rolled back (its install calls the invoker).
DROP FUNCTION IF EXISTS sessions.canonical_feature_journal_seed(bigint,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb);
DROP FUNCTION IF EXISTS sessions.canonical_feature_image_key(jsonb,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_source_valid(jsonb);
DROP FUNCTION IF EXISTS sessions.canonical_feature_pointer_set_valid(jsonb);
DROP TABLE IF EXISTS sessions.canonical_feature_journal;
DO $r$BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_canonical_feature_producer')THEN
 REVOKE USAGE ON SCHEMA sessions FROM bulls96_canonical_feature_producer;END IF;END$r$;
