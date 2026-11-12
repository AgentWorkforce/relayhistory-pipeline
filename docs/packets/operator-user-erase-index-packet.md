# Operator user erase indexes (migration 20261105058000)

Migration `supabase/migrations/20261105058000_operator_user_erase_indexes.sql`
adds sixteen indexes: ten foreign-key support indexes, one for the un-owned-slot
lookup, four `key_material` expression indexes for the receipt seat and record-head
lookups (org, workspace, user and org, workspace, origin) and one for the ord-seat
lookup by candidate key, so the operator per-user erase (20261105058010) probes the referencing tables by
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
| `sessions.ingest_receipt_seats_key_material_user_idx` | `(key_material ->> 3), (key_material ->> 4), (key_material ->> 5)` | |
| `sessions.ingest_receipt_seats_key_material_origin_idx` | `(key_material ->> 3), (key_material ->> 4), (key_material ->> 6)` | |
| `sessions.ingest_receipt_record_heads_key_material_user_idx` | `(key_material ->> 3), (key_material ->> 4), (key_material ->> 5)` | |
| `sessions.ingest_receipt_record_heads_key_material_origin_idx` | `(key_material ->> 3), (key_material ->> 4), (key_material ->> 6)` | |
| `sessions.ingest_receipt_ord_seats_candidate_key_idx` | `candidate_key_sha256` | |

All are `USING btree`; the four `key_material` indexes are expression indexes (each column is a `->>`
expression on the `jsonb` `key_material` array, written `((key_material ->> 3))` in the exact definition). An index whose table is empty may be left absent; the
migration builds it itself. Every index on a populated table must be prebuilt;
`SELECT EXISTS (SELECT 1 FROM sessions.<table>)` tells you which tables are
populated. After the run, the verification query in the guide must return each
prebuilt index valid, ready and live, with exactly these definitions.
