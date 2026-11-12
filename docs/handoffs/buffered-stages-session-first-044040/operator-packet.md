# Buffered stages session-first index (migration 20261105044040)

Migration `supabase/migrations/20261105044040_buffered_stages_session_first.sql`
adds a GIN index on claim records so the session-first stage lookup is an index
probe. On a populated table it refuses a blocking build and its error hint
points here.

Prebuild the index with `CREATE INDEX CONCURRENTLY`, following
[Prebuilding indexes out of band](../../out-of-band-index-prebuild.md), then
rerun the migration:

```sql
CREATE INDEX CONCURRENTLY ingest_claim_attempts_records_gin_idx
  ON sessions.ingest_claim_attempts USING gin (records jsonb_path_ops);
```

The exact definition is the `BUFFERED_STAGES_SESSION_FIRST_STEP` entry of
`ADOPTABLE_INDEXES` in `src/hoopdb/migration-sentinels.ts`. A jsonb_path_ops
GIN reads the heap once; a budget of about one minute per GB of heap (minimum
five minutes) is a reasonable start.

The migration also replaces `sessions.buffered_sessions_for` and
`sessions.buffered_session_stages_for`. Let the migration apply those; do not
replace them out of band unless the bodies are byte-identical to the migration
file, or the next run refuses with `unledgered_schema`.
