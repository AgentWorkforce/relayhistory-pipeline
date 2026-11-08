/**
 * The buffered search feed's runtime role: the one TypeScript source of its name. Read by the feeder's
 * login check (buffered-feed-login.ts), the function-boundary lane and its generated phase 2 verifier
 * (scripts/sessions-function-boundary/lanes/buffered-search-feed.ts, verifiers.ts). The SQL files name it
 * literally: supabase/migrations/20261105043870_buffered_search_feed_cursor.sql and its rollback,
 * supabase/bootstrap/{session_ingest_roles,cloudsql_migration_roles,rds_migration_roles,
 * hoopdb_runtime_role_rds,buffered_search_feed_runtime_role}.sql. A rename changes all of them together.
 */
export const BUFFERED_SEARCH_FEED_RUNTIME_ROLE = "buffered_search_feed_runtime";
