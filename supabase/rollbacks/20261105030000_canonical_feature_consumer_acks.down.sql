-- Rollback 20261105030000: drop the consumer ack/state objects.
DROP FUNCTION IF EXISTS sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text);
DROP TABLE IF EXISTS sessions.canonical_feature_copy_state;
DROP TABLE IF EXISTS sessions.canonical_feature_page_ack;
DROP FUNCTION IF EXISTS sessions.canonical_feature_page_ack_fence();
