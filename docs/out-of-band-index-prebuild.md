# Prebuilding indexes out of band (`CREATE INDEX CONCURRENTLY`)

Some session-schema migrations add an index to a table that can already be
large. The migrator (`src/hoopdb/migrate.ts`) runs every migration inside a
transaction, and PostgreSQL cannot run `CREATE INDEX CONCURRENTLY` (CIC) inside
a transaction block (SQLSTATE 25001). Those migrations therefore refuse to take
a blocking build on a populated table: the migrator's preflight reports
`index_prebuild_required`, and the migration itself raises SQLSTATE 55000 with a
hint that names the expected definition.

To clear the refusal, build the index yourself with CIC and rerun the
migration. The migration adopts an existing index only when it is the only
index of that name, sits in `sessions` on exactly the expected table, is valid,
ready and live, and its `pg_get_indexdef` is byte-identical to the expected
definition. Anything else is still refused. The adoptable definitions are listed
in `ADOPTABLE_INDEXES` in `src/hoopdb/migration-sentinels.ts`, which a test
keeps byte-identical to the migrations.

On an empty or small database you can skip all of this: the migration builds
the index itself. On a non-production database, a deliberate blocking build is
also available with `HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true`, at the cost of
blocking writes to the table for the duration of the build. The migrator refuses
that flag for production.

## Session rules

- Use `psql` with autocommit on (the default), one statement per command. Never
  wrap the build in `BEGIN`, and do not use `psql -1` / `--single-transaction`.
- Use a direct connection, not a transaction-mode pooler. A pooler does not keep
  session `SET`s and may recycle the connection mid-build.
- Connect as the role that owns the tables (the migration owner).
- Build one table at a time. CIC takes `SHARE UPDATE EXCLUSIVE`, which does not
  block normal reads and writes but conflicts with autovacuum, `ANALYZE`, other
  DDL and another CIC on the same table.
- Give every build a finite budget, never `statement_timeout = 0`:

  ```sql
  SET statement_timeout = '<budget>';
  SET lock_timeout = '5s';
  ```

  `lock_timeout` also applies to CIC's waits on older transactions between
  phases, so a long-running transaction can cancel the build. Raising it for the
  session (for example `'60s'`) is safe; it only waits longer.

## Build

Run the exact `CREATE INDEX` statement from `ADOPTABLE_INDEXES`, with
`CONCURRENTLY` inserted, for example:

```sql
CREATE INDEX CONCURRENTLY ingest_claim_attempts_records_gin_idx
  ON sessions.ingest_claim_attempts USING gin (records jsonb_path_ops);
```

From a second session, watch progress and find the backend if you need to
cancel it:

```sql
SELECT pid, relid::regclass AS tbl, phase, blocks_done, blocks_total
FROM pg_stat_progress_create_index;

SELECT pg_cancel_backend(<pid>);
```

## Verify

Each index must be valid, ready and live, with exactly the expected definition:

```sql
SELECT i.indexrelid::regclass AS idx, i.indisvalid, i.indisready, i.indislive,
       pg_get_indexdef(i.indexrelid) AS def
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname = '<index name>';
```

Then rerun the migration. It adopts the index without locking the table.

## Failed or cancelled builds

A cancelled, timed-out or failed CIC leaves an INVALID index. The planner never
uses it and the migration refuses to adopt it. `IF NOT EXISTS` only checks the
name, so re-running the build would silently keep the INVALID index. Find it and
drop it before retrying:

```sql
SELECT indexrelid::regclass FROM pg_index
WHERE NOT indisvalid AND indrelid = 'sessions.<table>'::regclass;

DROP INDEX CONCURRENTLY sessions.<index name>;
```

Retry once. If the retry also fails, find the cause (usually a long
transaction, the lock timeout, the budget or IO pressure) before trying again.

## Rollback

`DROP INDEX CONCURRENTLY IF EXISTS sessions.<index name>;`, one statement at a
time with the same session rules, in reverse build order.
