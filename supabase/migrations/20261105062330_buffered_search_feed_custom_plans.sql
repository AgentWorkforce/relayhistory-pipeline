-- Buffered search feed: plan the stage RPC's statements for each call's own values (slot 062330).
--
-- Why. sessions.buffered_search_feed_stage(jsonb) (20261105045010) and the retention decision it calls,
-- sessions.ingest_buffer_gc_ordinals(uuid) (20261104055000), are PL/pgSQL. PL/pgSQL caches each statement's
-- plan per session; from about the sixth call PostgreSQL may switch to a cached GENERIC plan, planned without
-- the call's values. Under that plan (a plan-only EXPLAIN under force_generic_plan on a heavy tenant):
--   - gc_ordinals' native-slot join hashes every native slot of the owner via
--     ingest_lazy_native_current_owner (org_id, workspace_id, current_owner) on a 1-row estimate;
--   - the stage RPC's same-second sibling count takes ingest_stage_intents_batch_idx
--     (org_id, workspace_id, origin_id) and filters created_at/user_id, walking the origin's whole intent
--     history instead of the one-second ingest_stage_intents_cleanup_idx range.
-- With custom plans (the call's literal values) both are short index scans. The long-lived feeder sessions
-- reach the generic plan and the stage RPC hits the runtime login's statement_timeout (57014).
--
-- What changes. ALTER FUNCTION ... SET plan_cache_mode = force_custom_plan on exactly those two functions,
-- appended to their existing proconfig (search_path, and the stage RPC's lock_timeout, are unchanged). A
-- function-level SET applies for the call and every function it calls that does not set it itself, so on the
-- feed path ingest_buffer_gc_kept and ingest_receipt_seat_records (called by gc_ordinals) are custom-planned
-- too. Setting it on gc_ordinals as well covers its other callers (buffer GC claim/prepare/swap, canonical
-- history lineage, the operator session and user erase), which plan the same native-slot join. No body,
-- signature, volatility, SECURITY DEFINER, owner or ACL change (ALTER FUNCTION keeps the ACL; no GRANT/REVOKE;
-- src/hoopdb/sessions-function-acl.json unchanged). Never creates or alters a role, table or index.
--
-- Cost. Every statement of these two functions is planned on every execution (no plan reuse). They are short,
-- single-row-keyed statements; planning is microseconds against the generic plans' seconds.
--
-- Rollout. Code only: applying this to any real database needs Khaliq's explicit OK. The ALTERs only update the
-- two pg_proc rows (no table lock); sessions pick the setting up on their next call, when the pg_proc change
-- invalidates their cached function and its plans. Applying it changes the migration-set
-- generation (src/hoopdb/migrate.ts). Down path: supabase/rollbacks/20261105062330_buffered_search_feed_custom_plans.down.sql
-- (RESET plan_cache_mode on both; checksum-pinned, refused while a later migration is applied).
SET LOCAL lock_timeout='5s';

DO $buffered_search_feed_custom_plans$ BEGIN
 IF to_regprocedure('sessions.buffered_search_feed_stage(jsonb)') IS NULL
  OR NOT (SELECT prosecdef FROM pg_proc WHERE oid=to_regprocedure('sessions.buffered_search_feed_stage(jsonb)'))
  OR NOT EXISTS(SELECT 1 FROM pg_proc WHERE oid=to_regprocedure('sessions.buffered_search_feed_stage(jsonb)')
   AND prosrc LIKE '%''sameSecond''%')
  OR to_regprocedure('sessions.ingest_buffer_gc_ordinals(uuid)') IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed custom plans: requires 20261105045010 and 20261104055000';END IF;
END $buffered_search_feed_custom_plans$;

ALTER FUNCTION sessions.buffered_search_feed_stage(jsonb) SET plan_cache_mode = force_custom_plan;
ALTER FUNCTION sessions.ingest_buffer_gc_ordinals(uuid) SET plan_cache_mode = force_custom_plan;
