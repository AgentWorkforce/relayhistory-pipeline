# Operator user erase indexes (migration 20261105058000)

Migration `supabase/migrations/20261105058000_operator_user_erase_indexes.sql`
adds ten foreign-key support indexes and one index for the un-owned-slot lookup,
so the operator per-user erase (20261105058010) probes the referencing tables by
index instead of by sequential scan. On a populated table it refuses a blocking
build and its error hint points here.

Prebuild the indexes with `CREATE INDEX CONCURRENTLY`, following
[Prebuilding indexes out of band](../out-of-band-index-prebuild.md), then rerun
the migration. The exact definitions are the
`OPERATOR_USER_ERASE_INDEXES_STEP` entries of `ADOPTABLE_INDEXES` in
`src/hoopdb/migration-sentinels.ts`. Build them in this order:

| index | columns | predicate |
| --- | --- | --- |
| `sessions.ingest_receipt_ord_seats_key_sha256_idx` | `key_sha256` | |
| `sessions.ingest_receipt_record_heads_winner_key_sha256_idx` | `winner_key_sha256` | |
| `sessions.ingest_lazy_native_slots_current_seat_key_idx` | `current_seat_key` | |
| `sessions.ingest_lazy_ack_decisions_native_key_sha256_idx` | `native_key_sha256` | |
| `sessions.ingest_lazy_ack_decisions_seat_key_sha256_idx` | `seat_key_sha256` | |
| `sessions.ingest_stage_intents_parent_stage_token_idx` | `parent_stage_token` | `WHERE (parent_stage_token IS NOT NULL)` |
| `sessions.ingest_stage_intents_gc_replacement_stage_token_idx` | `gc_replacement_stage_token` | `WHERE (gc_replacement_stage_token IS NOT NULL)` |
| `sessions.ingest_stage_intents_batch_idx` | `org_id, workspace_id, origin_id, batch_id` | |
| `sessions.ingest_buffer_outbox_active_stage_token_idx` | `active_stage_token` | `WHERE (active_stage_token IS NOT NULL)` |
| `sessions.ingest_history_plans_stage_token_idx` | `stage_token` | |
| `sessions.ingest_lazy_native_slots_unowned_origin_idx` | `org_id, workspace_id, origin_id` | `WHERE (current_owner IS NULL)` |

All are `USING btree`. An index whose table is empty may be left absent; the
migration builds it itself. Every index on a populated table must be prebuilt;
`SELECT EXISTS (SELECT 1 FROM sessions.<table>)` tells you which tables are
populated. After the run, the verification query in the guide must return each
prebuilt index valid, ready and live, with exactly these definitions.
