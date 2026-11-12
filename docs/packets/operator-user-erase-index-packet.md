# Operator user erase: CONCURRENTLY index prebuild packet (20261105058000)

Status: **not run.** Running any statement below against a real database needs Khaliq's explicit OK.

The migrator runs each migration in one transaction, so it cannot build indexes CONCURRENTLY. 058000 creates eleven
indexes. It creates an absent index only on an empty table; on a populated table it refuses with `55000` and names
this packet. So every index on a populated production table must be prebuilt here first.

Four of them sit on large production tables (sizes from 2026-11-11):

| Index | Table | Size |
|---|---|---|
| `ingest_lazy_native_slots_current_seat_key_idx` | `ingest_lazy_native_slots` (~28M rows) | 35 GB |
| `ingest_lazy_native_slots_unowned_origin_idx` | `ingest_lazy_native_slots` | 35 GB |
| `ingest_lazy_ack_decisions_native_key_sha256_idx` | `ingest_lazy_ack_decisions` (~74M rows) | 45 GB |
| `ingest_lazy_ack_decisions_seat_key_sha256_idx` | `ingest_lazy_ack_decisions` | 45 GB |

`ingest_receipt_ord_seats` and `ingest_receipt_record_heads` are empty; 058000 creates their indexes itself.

The production row counts of `ingest_stage_intents`, `ingest_buffer_outbox` and `ingest_history_plans` are **not
known** to this packet. Check them first (section below); prebuild their five indexes too if any is populated.

## Check row counts (read-only)

```sql
SELECT 'ingest_stage_intents' AS t, EXISTS (SELECT 1 FROM sessions.ingest_stage_intents) AS populated
UNION ALL SELECT 'ingest_buffer_outbox', EXISTS (SELECT 1 FROM sessions.ingest_buffer_outbox)
UNION ALL SELECT 'ingest_history_plans', EXISTS (SELECT 1 FROM sessions.ingest_history_plans);
SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_stat_user_tables WHERE schemaname = 'sessions'
  AND relname IN ('ingest_stage_intents','ingest_buffer_outbox','ingest_history_plans');
```

## Build (owner login, one statement at a time, outside any transaction)

```sql
SET statement_timeout = 0;
SET lock_timeout = '5s';
CREATE INDEX CONCURRENTLY ingest_lazy_native_slots_current_seat_key_idx ON sessions.ingest_lazy_native_slots USING btree (current_seat_key);
CREATE INDEX CONCURRENTLY ingest_lazy_native_slots_unowned_origin_idx ON sessions.ingest_lazy_native_slots USING btree (org_id, workspace_id, origin_id) WHERE (current_owner IS NULL);
CREATE INDEX CONCURRENTLY ingest_lazy_ack_decisions_native_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (native_key_sha256);
CREATE INDEX CONCURRENTLY ingest_lazy_ack_decisions_seat_key_sha256_idx ON sessions.ingest_lazy_ack_decisions USING btree (seat_key_sha256);
```

Only if the count check shows the table populated:

```sql
-- ingest_stage_intents
CREATE INDEX CONCURRENTLY ingest_stage_intents_parent_stage_token_idx ON sessions.ingest_stage_intents USING btree (parent_stage_token) WHERE (parent_stage_token IS NOT NULL);
CREATE INDEX CONCURRENTLY ingest_stage_intents_gc_replacement_stage_token_idx ON sessions.ingest_stage_intents USING btree (gc_replacement_stage_token) WHERE (gc_replacement_stage_token IS NOT NULL);
CREATE INDEX CONCURRENTLY ingest_stage_intents_batch_idx ON sessions.ingest_stage_intents USING btree (org_id, workspace_id, origin_id, batch_id);
-- ingest_buffer_outbox
CREATE INDEX CONCURRENTLY ingest_buffer_outbox_active_stage_token_idx ON sessions.ingest_buffer_outbox USING btree (active_stage_token) WHERE (active_stage_token IS NOT NULL);
-- ingest_history_plans
CREATE INDEX CONCURRENTLY ingest_history_plans_stage_token_idx ON sessions.ingest_history_plans USING btree (stage_token);
```

Watch progress with `pg_stat_progress_create_index`. A CONCURRENTLY build takes a SHARE UPDATE EXCLUSIVE lock (writes
continue) and two table scans; expect tens of minutes per index on the ack table. Keep a TCP keepalive on the
session (`keepalives_idle=30`), as a quiet multi-minute statement can lose its connection.

## Verify, then apply 058000

```sql
SELECT c.relname, i.indisvalid, i.indisready, i.indislive, pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relnamespace = 'sessions'::regnamespace AND c.relname IN (
 'ingest_lazy_native_slots_current_seat_key_idx','ingest_lazy_native_slots_unowned_origin_idx',
 'ingest_lazy_ack_decisions_native_key_sha256_idx','ingest_lazy_ack_decisions_seat_key_sha256_idx',
 'ingest_stage_intents_parent_stage_token_idx','ingest_stage_intents_gc_replacement_stage_token_idx',
 'ingest_stage_intents_batch_idx','ingest_buffer_outbox_active_stage_token_idx',
 'ingest_history_plans_stage_token_idx');
```

Each prebuilt index must be valid, ready and live, with exactly the definition above (ADOPTABLE_INDEXES in
`src/hoopdb/migration-sentinels.ts`, minus `CONCURRENTLY`). 058000 then adopts them without locking their tables. An index on an empty table may be left absent; 058000 builds it.

A failed build leaves an INVALID index: `DROP INDEX CONCURRENTLY sessions.<name>;` and rebuild. 058000 refuses an
INVALID or differently defined index and never drops anything.
