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

## Guarded workflow

`.github/workflows/index-prebuild-058000.yml` runs this packet without a hand-typed
statement. It reads the sixteen names, definitions and order from
`ADOPTABLE_INDEXES`, and the only DDL it can run is each declared definition with
`CONCURRENTLY` inserted. It never runs a blocking build, never sets the blocking-build
opt-in, never drops anything and never applies the migration. It uses the same gates as
the guarded schema release: exact live `main`, first attempt, the pinned dispatcher as
actor and triggering actor, green push CI and pg-proof at the SHA, and the protected
schema environment with its recorded approval. The owner credential reaches only an
allowlisted child process, over stdin. Prebuild is dormant (`INDEX_PREBUILD_ENABLED` in
`src/hoopdb/index-prebuild-058000.ts`). Enabling it takes a separate reviewed change.

1. `census` (read-only). For each index it records: whether the table is empty,
   absent/exact/drifted, `indisvalid`/`indisready`/`indislive`, owner, schema, definition
   digest and any `pg_stat_progress_create_index` row. It then gives an action:
   `present`, `deferred` (absent on an empty table, so the migration builds it),
   `prebuild` (absent on a populated table) or `refused`. The readiness-marker generation
   and the ledger tail and digest must not change during the census. The run publishes
   a census artifact (kept 1 day). A `blocked` census means something needs a human:
   an invalid, in-progress, drifted or wrong-owner object, an index of the same name in
   another schema, or a readiness marker whose status is not `pass` (for example
   `pending`, which can be left by a failed migration). The census still reports these.
2. `prebuild`. It needs a census run from under 24 hours ago with verdict
   `prebuild_required`, and a typed confirmation copied byte for byte from that census:
   `PREBUILD-058000 <sha> GENERATION <g> TAIL <tail> LEDGER <digest> DESCRIPTOR <d> CENSUS <run> INDEXES <names>`.
   It holds a dedicated advisory lock (try-only) and re-checks the census against the
   live database. The readiness marker must be `pass` then and before every build. Then it builds the `prebuild` indexes one at a time, with a finite
   statement and lock budget, and reads each one back. After every step it rewrites a
   per-index receipt (kept 30 days). A rerun with the same census resumes: an index that
   is already exact and valid/ready/live is accepted, and nothing else is. If a
   statement's outcome is unknown (the connection dropped, or the server ended the
   session, for example `57P01`), the run reads it back on a fresh session, never on the
   failed one, and stops. It never retries. If no fresh readback is possible, the receipt
   records `outcome_unknown` for that index and a human checks it.
3. Apply the release. The migration adopts the prebuilt indexes and builds the
   deferred ones on empty tables.
4. `readback` (read-only). It passes only when all sixteen are exact, valid, ready and
   live, and no create-index progress row exists on their tables.

Before the separate change that enables prebuild: prove a long statement through the
session pooler on a disposable or dev database (or use a proven direct owner endpoint),
and run a guarded read-only census or readback to prove the credential hand-off.

If an index is refused as invalid or in progress, follow "Failed or cancelled builds"
in the guide by hand: check `pg_stat_progress_create_index` before dropping anything.
Then run a new census. Dispatch steps, approvals and run records are not kept in this
repository.
