# Operator user erase: CONCURRENTLY index prebuild packet (20261105058000)

Status: **not run.** Running any statement below against a real database needs Khaliq's explicit OK.

The migrator runs each migration in one transaction, so it cannot build indexes CONCURRENTLY. On production three
of 058000's five indexes sit on large tables (sizes from 2026-11-11):

| Index | Table | Size |
|---|---|---|
| `ingest_lazy_native_slots_current_seat_key_idx` | `ingest_lazy_native_slots` (~28M rows) | 35 GB |
| `ingest_lazy_ack_decisions_native_key_sha256_idx` | `ingest_lazy_ack_decisions` (~74M rows) | 45 GB |
| `ingest_lazy_ack_decisions_seat_key_sha256_idx` | `ingest_lazy_ack_decisions` | 45 GB |

`ingest_receipt_ord_seats` and `ingest_receipt_record_heads` are empty; 058000 creates their indexes itself.

## Build (owner login, one statement at a time, outside any transaction)

```sql
SET statement_timeout = 0;
SET lock_timeout = '5s';
CREATE INDEX CONCURRENTLY ingest_lazy_native_slots_current_seat_key_idx ON sessions.ingest_lazy_native_slots USING btree (current_seat_key);
CREATE INDEX CONCURRENTLY ingest_lazy_ack_decisions_native_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (native_key_sha256);
CREATE INDEX CONCURRENTLY ingest_lazy_ack_decisions_seat_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (seat_key_sha256);
```

Watch progress with `pg_stat_progress_create_index`. A CONCURRENTLY build takes a SHARE UPDATE EXCLUSIVE lock (writes
continue) and two table scans; expect tens of minutes per index on the ack table. Keep a TCP keepalive on the
session (`keepalives_idle=30`), as a quiet multi-minute statement can lose its connection.

## Verify, then apply 058000

```sql
SELECT c.relname, i.indisvalid, i.indisready, i.indislive, pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname IN (
 'ingest_lazy_native_slots_current_seat_key_idx','ingest_lazy_ack_decisions_native_key_sha256_idx',
 'ingest_lazy_ack_decisions_seat_key_sha256_idx');
```

Each must be valid, ready and live, with exactly the definition above (ADOPTABLE_INDEXES in
`src/hoopdb/migration-sentinels.ts`). 058000 then adopts them without locking their tables.

A failed build leaves an INVALID index: `DROP INDEX CONCURRENTLY sessions.<name>;` and rebuild. 058000 refuses an
INVALID or differently defined index and never drops anything.
