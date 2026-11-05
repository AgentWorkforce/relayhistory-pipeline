-- Schema removal is only for an unused, disabled installation. Normal rollout
-- rollback disables the scoped source/queue and RETAINS pending event metadata.
DO $guard$ BEGIN
 IF EXISTS(SELECT 1 FROM sessions.canonical_project_events)OR EXISTS(SELECT 1 FROM sessions.canonical_project_event_scopes WHERE enabled)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='project events: retain durable events; disable the scoped runtime instead';END IF;
END $guard$;
DROP TRIGGER canonical_project_events_after_journal ON sessions.canonical_feature_journal;
DROP FUNCTION sessions.canonical_project_events_claim_v1(jsonb),sessions.canonical_project_events_sent_v1(jsonb),sessions.canonical_project_events_release_v1(jsonb),
 sessions.canonical_project_event_read_v1(jsonb),sessions.canonical_project_events_pending_v1(jsonb),sessions.canonical_project_event_ack_v1(jsonb),
 sessions.canonical_project_events_guard(jsonb,boolean),sessions.canonical_project_event_is_current(sessions.canonical_project_events,text,boolean),
 sessions.canonical_project_events_journal_changed(),sessions.canonical_project_event_json(sessions.canonical_project_events);
DROP TABLE sessions.canonical_project_events,sessions.canonical_project_event_scopes;
-- The cluster-level actor remains; never drop a shared role in a schema rollback.
