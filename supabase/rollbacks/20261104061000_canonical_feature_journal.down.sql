-- Reverse of 20261104061000. Only valid after 20261104062000 is rolled back (its install calls the invoker).
DROP FUNCTION IF EXISTS sessions.canonical_feature_journal_seed(bigint,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb);
DROP FUNCTION IF EXISTS sessions.canonical_feature_image_key(jsonb,text);
DROP FUNCTION IF EXISTS sessions.canonical_feature_source_valid(jsonb);
DROP FUNCTION IF EXISTS sessions.canonical_feature_pointer_set_valid(jsonb);
DROP TABLE IF EXISTS sessions.canonical_feature_journal;
