-- Destructive: removes persisted reports and jobs. Disable coordination reports and
-- obtain explicit database-migration approval before applying this rollback.
-- Unwind these triggers before rolling back their event-time source tables.
BEGIN;

DROP TRIGGER coordination_report_pointer_changed ON sessions.session_index;
DROP TRIGGER coordination_report_directory_changed ON sessions.session_directory;
DROP TRIGGER coordination_report_pending_changed ON sessions.session_directory_event_pending;
DROP TRIGGER coordination_report_times_changed ON sessions.session_directory_event_times;
DROP FUNCTION sessions.coordination_report_source_changed();
DROP TABLE sessions.coordination_report_jobs;
DROP TABLE sessions.coordination_report_epochs;

COMMIT;
