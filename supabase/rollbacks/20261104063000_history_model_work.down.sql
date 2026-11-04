-- Operational rollback: stop the model schedule/consumers first. Never discard
-- durable continuation or known cleanup obligations to make rollback look green.
DO $guard$ BEGIN
 IF EXISTS(SELECT 1 FROM sessions.history_model_work) OR EXISTS(SELECT 1 FROM sessions.history_model_wakeups)THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='retain model work and source-bound continuation ledgers; stop consumers for operational rollback';
 END IF;
END $guard$;
DROP TRIGGER IF EXISTS history_model_graph_wake ON sessions.session_replay_publications;
DROP FUNCTION IF EXISTS
 sessions.session_history_model_discover(jsonb),
 sessions.session_history_model_enqueue(jsonb),
 sessions.session_history_model_claim(jsonb),
 sessions.session_history_model_heartbeat(jsonb),
 sessions.session_history_model_checkpoint(jsonb),
 sessions.ingest_history_model_retry(jsonb),
 sessions.ingest_history_model_graph_wake(),
 sessions.ingest_history_model_checkpoint_valid(jsonb,jsonb,jsonb),
 sessions.ingest_history_model_ticket(sessions.history_model_work);
REVOKE EXECUTE ON FUNCTION sessions.session_history_capture(jsonb),sessions.session_history_replay_descriptor(jsonb),sessions.session_history_register_object(jsonb) FROM bulls96_session_model;
DROP TABLE sessions.history_model_wakeups,sessions.history_model_work;
-- Role/password/secret deletion is an independent operator decision.620's
-- registered encrypted objects and retirement ledger remain intact.
