-- Disable CH metadata search and publisher before this operator-only rollback.
DROP TRIGGER session_brief_document_changed ON sessions.session_directory;
DROP FUNCTION sessions.session_brief_document_authority(text,text[],text[]);
DROP FUNCTION sessions.retry_session_brief_documents(uuid,text);
DROP FUNCTION sessions.ack_session_brief_documents(uuid,jsonb);
DROP FUNCTION sessions.claim_session_brief_documents(uuid,integer);
DROP FUNCTION sessions.backfill_session_brief_documents(integer);
DROP FUNCTION sessions.session_brief_document_changed();
DROP FUNCTION sessions.queue_session_brief_document(jsonb);
DROP FUNCTION sessions.session_brief_document_json(sessions.session_directory,boolean);
DROP TABLE sessions.session_brief_document_outbox;
DROP TABLE sessions.session_brief_document_scopes;
DROP SEQUENCE sessions.session_brief_document_epoch_seq;
-- Retain the CH derived table until separately authorized physical cleanup.
