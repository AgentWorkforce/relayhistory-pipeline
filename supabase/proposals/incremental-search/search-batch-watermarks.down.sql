-- M3 proposal rollback. The state is derived; flags must be off before this
-- proposal is ever promoted and rolled back.
DROP FUNCTION IF EXISTS sessions.cas_search_batch_watermarks_v1(text,text,text,text,bigint,jsonb,jsonb);
DROP FUNCTION IF EXISTS sessions.read_search_batch_watermarks_v1(text,text,text,text);
DROP FUNCTION IF EXISTS sessions.canonical_search_watermarks_v1(jsonb);
DROP TABLE IF EXISTS sessions.search_batch_watermarks;
DROP TABLE IF EXISTS sessions.search_scope_watermark_versions;

