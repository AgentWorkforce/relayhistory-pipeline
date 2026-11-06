-- Rollback for registry slot 20261105035000 (upstream #191 semantic_copy_state).
-- Disable the semantic copier (SESSION_SEMANTIC_COPY_ENABLED=0, schedule DISABLED) first.
-- Checkpoints are restartable metadata; feature payloads stay in ClickHouse.
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105035000_semantic_copy_state.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='semantic copy state: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;
DROP FUNCTION IF EXISTS sessions.finish_search_feature_copy(text,text,uuid,bigint,text,text);
DROP FUNCTION IF EXISTS sessions.commit_search_feature_copy(text,text,uuid,bigint,text,jsonb,timestamp with time zone);
DROP FUNCTION IF EXISTS sessions.heartbeat_search_feature_copy(text,text,uuid,bigint);
DROP FUNCTION IF EXISTS sessions.initialize_search_feature_baseline(text,text,uuid,bigint,text,bigint);
DROP FUNCTION IF EXISTS sessions.claim_search_feature_copy(text,text,text,uuid);
DROP FUNCTION IF EXISTS sessions.register_search_feature_source(text,text,text,text,text);
DROP FUNCTION IF EXISTS sessions.search_feature_copy_body(text,text);
DROP FUNCTION IF EXISTS sessions.search_feature_copy_state_valid(jsonb);
DROP TABLE IF EXISTS sessions.search_feature_copy_state;
DROP TABLE IF EXISTS sessions.search_feature_sources;
DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105035000_semantic_copy_state.sql';
  END IF;
END
$ledger$;
