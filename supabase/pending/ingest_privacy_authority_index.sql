-- C3 post-apply job (NON-TRANSACTIONAL; run after ingest_privacy_authority.sql, before activation).
-- The Supabase-authority apply deletes one user's frozen records per selected session, or all of them on a
-- reset (Neon semantics). The primary key (org, workspace, origin, record) cannot serve "this user's session",
-- so without this index each control would scan a whole origin/org of the ~66M-row frozen copy.
-- ingest_privacy_authority_activate refuses until this index exists and is valid. Build time on 66M rows is
-- unmeasured: rehearse it, and per the registry rule (>30 s) it stays outside the migration transaction.
-- If the build fails it leaves an INVALID index: DROP INDEX CONCURRENTLY it and rerun.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_frozen_source_records_user_session
 ON sessions.ingest_frozen_source_records(org_id,workspace_id,user_id,source,session_id);
