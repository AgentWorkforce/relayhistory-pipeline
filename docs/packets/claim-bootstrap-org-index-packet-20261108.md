# Claim bootstrap `org_id` indexes (migration 20261105044020)

Migration `supabase/migrations/20261105044020_claim_bootstrap_org_indexes.sql`
adds `org_id`-leading indexes so the first claim for a new org probes these
tables by index instead of by sequential scan. On a populated table it refuses
a blocking build and its error hint points here.

Prebuild the indexes with `CREATE INDEX CONCURRENTLY`, following
[Prebuilding indexes out of band](../out-of-band-index-prebuild.md), then rerun
the migration. The exact definitions are the
`CLAIM_BOOTSTRAP_ORG_INDEXES_STEP` entries of `ADOPTABLE_INDEXES` in
`src/hoopdb/migration-sentinels.ts`. Build them smallest table first:

| index | columns |
| --- | --- |
| `sessions.ingest_history_mask_pages_org_id_idx` | `org_id` |
| `sessions.ingest_history_plans_org_id_idx` | `org_id, workspace_id, user_id` |
| `sessions.ingest_seed_captures_org_id_idx` | `org_id` |
| `sessions.ingest_publication_attempts_org_id_idx` | `org_id, workspace_id, user_id` |
| `sessions.ingest_publication_generations_org_id_idx` | `org_id, workspace_id, user_id` |
| `sessions.ingest_stage_intents_org_id_idx` | `org_id, workspace_id, user_id` |
| `sessions.ingest_custody_records_org_id_idx` | `org_id, workspace_id, user_id` |
| `sessions.ingest_lazy_native_slots_org_id_idx` | `org_id` |

All are `USING btree`. After the run, the verification query in the guide must
return eight valid, ready indexes with exactly these definitions.
