BEGIN;
LOCK TABLE sessions.pipeline_jobs,sessions.pipeline_events,sessions.pipeline_dead_letters IN ACCESS EXCLUSIVE MODE;
DROP TRIGGER guard_search_wake_delete ON sessions.pipeline_jobs;
DROP TRIGGER guard_search_wake_truncate ON sessions.pipeline_jobs;
DROP FUNCTION sessions.guard_managed_search_truncate_v1();
DROP TRIGGER guard_search_wake_update ON sessions.pipeline_jobs;
DROP TRIGGER guard_search_wake_insert ON sessions.pipeline_jobs;
DROP FUNCTION sessions.guard_managed_search_wake_v1();
DROP FUNCTION sessions.enqueue_managed_search_wake_v1(jsonb,uuid);
DROP INDEX sessions.pipeline_jobs_search_index_scope;
-- Restoring the checks refuses rollback atomically if any search-stage history remains.
DO $checks$
DECLARE entry record; cname text; current_def text;
BEGIN
 FOR entry IN SELECT * FROM publication_private.search_stage_checks LOOP
   SELECT relname||'_stage_check' INTO STRICT cname FROM pg_class WHERE oid=entry.relation;
   SELECT pg_get_constraintdef(oid) INTO STRICT current_def FROM pg_constraint WHERE conrelid=entry.relation AND conname=cname;
   IF current_def<>entry.installed_definition THEN RAISE EXCEPTION 'search wake: stage check drift'; END IF;
   EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, ADD CONSTRAINT %I %s',entry.relation,cname,cname,entry.original_definition);
 END LOOP;
END $checks$;
DROP TABLE publication_private.search_stage_checks;
COMMIT;
