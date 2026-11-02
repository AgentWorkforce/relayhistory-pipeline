# Session ingest rollout (run sheet)

This moves `POST /v1/delivery/batches` off Neon. The new path runs on the 96Bulls session ingest Worker (`stacks/cloudflare/session-ingest.ts`, `src/session-ingest/`), reached from relayhistory's Worker through the `DELIVERY_INGEST` service binding. The design is in `docs/session-ingest-design.md`, and the companion PR is relayhistory-cloud #265.

**Approvals**
- Already given by Khaliq (2026-11-02, about 05:30Z, via self-hosted-lead): the Alchemy state-store Worker that the first plan creates, and running relayhistory-cloud #245 in `supabase` mode first.
- Every other step below is for the lead to run. **Do not ramp past the canary org without the lead's go.**

**Credential rules**
- Credentials come only from 1Password, through `op read` or process substitution.
- Never put one in a file, an argument that shell history keeps, or a log.

| # | step | command / action | check | rollback |
|---|---|---|---|---|
| 0 | Merge order | relayhistory-cloud #245, then 96BullSessions (sessions-pipeline) #154, then relayhistory-cloud #265. Merging changes nothing at runtime. | CI green on each | revert the PR |
| 1 | relayhistory auth on Supabase (#245) | `AUTH_BACKEND=dual`, then `backfill` and `parity` until `inParity: true`, then `AUTH_BACKEND=supabase`, then one more `backfill` (#245's runbook). | No `auth shadow mismatch`. A `/v1` request in `supabase` mode makes no Neon auth read. | `AUTH_BACKEND=dual` |
| 2 | Supabase migration and roles | Apply `supabase/migrations/20261102010000_session_ingest_ledger.sql` to `pyamkrhoyszgzirqqffq`. As `postgres`, run `supabase/bootstrap/session_ingest_role.sql`, then `\password bulls96_session_ingest` (store it in 1Password), then `session_ingest_role_verify.sql`. Grant `bulls96_session_ingest_seeder` to the operator login used in step 4. | The verify script exits 0, and the migration sentinel `table sessions.ingest_batches` is present. | The tables are new and unused; drop only with Khaliq's OK. |
| 3 | Cloudflare out-of-band resources (relayhistory's account) | **Hyperdrive:** create one named `96bulls-session-ingest`, origin = the Supabase **transaction pooler** (port 6543) as `bulls96_session_ingest`, caching **off** (writes). **Secrets Store:** create `96bulls-session-ingest-s3-access-key-id` and `96bulls-session-ingest-s3-secret-access-key` from the transcript **srv-writer** IAM user's key in 1Password. | Note the Hyperdrive id and the Secrets Store id. | delete them (unused) |
| 4 | Inputs and plan | Write `.96bulls/cf-sessions-prod/cloudflare-inputs.json` (gitignored) with `sessionIngest: { accountId, hyperdriveId, secretsStoreId, kmsKeyArn: <transcripts CMK ARN>, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" }`. Run `bunx alchemy plan --config stacks/cloudflare/alchemy.cloudflare.run.ts --stage cf-sessions-prod` and capture `docs/plans/cf-sessions-prod.*`. | The plan contract reads "session ingest rollout": a Worker, the fan-out queue and its DLQ, and the consumer, all creates. No secret appears in the detailed plan. | n/a |
| 5 | Deploy the ingest (dark) | Bind Khaliq's approval to the captured plan, then run `bunx alchemy deploy … --stage cf-sessions-prod`. | `96bulls-session-ingest` exists with no route, no workers.dev and no domain. Its settings list HYPERDRIVE, INGEST_FANOUT, NEON_REPLICA and the two Secrets Store bindings. | `alchemy` update phase / disable script |
| 6 | Bind it to relayhistory | The next relayhistory production deploy discovers the Worker (`deliveryIngestExists`) and adds `services: DELIVERY_INGEST`. `DELIVERY_FORWARD` stays **unset**. | relayhistory's settings show the `service` binding, and traffic is unchanged (no `delivery forward` log lines). | redeploy; with the secret unset nothing is forwarded |
| 6b | **Gate: Neon→S3 export caught up for the org** | Run `migration:neon-s3-sync` for the canary org until its watermark is past now. The seed (step 7) makes the ingest acknowledge Neon-held revisions as stored without writing them, so S3 must already hold them. Neon-only records are about 30% of today's fallback batches. | The exporter's watermark for the org is ≥ the seed time; repeat this before every org routed in step 9. | n/a |
| 6c | **Gate: erasure covers the ingest** | Before the canary, the erasure job must consume `sessions.upload_erasure_rearm` and erase `live-batches/**.staged.ndjson.zst` for deleted sessions (follow-up). Otherwise canary only an org with no pending deletions, and treat any deletion during the canary as a manual erase. | The erasure follow-up is merged, or the waiver is noted in the canary log. | n/a |
| 7 | Seed the canary org | `NEON_URL_FILE=<(op read …) SUPABASE_SEED_URL_FILE=<(op read …) bun scripts/session-ingest-seed.ts <linuyc org>` | JSON shows `outcome: seeded` and `read` ≈ the org's `delivery_records` count. | idempotent; nothing to undo |
| 8 | Canary on linuyc | `wrangler secret put DELIVERY_FORWARD` on `relayhistory-api` = `{"mode":"on","orgs":["<linuyc org>"],"percent":0,"exclude":[]}` (`onUnreachable` defaults to `"503"`, so the ingest is the only writer for the org). Then re-run step 7 at once to close the seed window. | Next section. | `{"mode":"off"}`, or add the org to `exclude`. Live in seconds; both paths write the same S3 manifests. |
| 9 | Ramp | **Only on the lead's go.** Set `percent` to 10, then 50, then 100. Before each step, run 6b (export caught up) and 7 (seed) for the newly routed orgs (`orgBucket < percent`). | The same gates, per step. | lower `percent` / `exclude` |

## Canary gates (48 h on linuyc, before any ramp)

Any red gate means roll back (`exclude` the org), then investigate.

**From the logs**
- **Forward outcomes:** `delivery forward` lines are all `outcome: forwarded`. Zero `unreachable`, or explained.
- **Ingest outcomes:** `session ingest` lines for linuyc are mostly `replay_fast` while its regenerated history drains. `accepted`, `accepted_staged` and `replay` make up the rest. Zero `delivery_unavailable` that persists across retries.
- **Busy rate:** 429 `delivery_busy` stays under 1% of linuyc's batches, compared with 22 to 40% today. If it rises, raise `INGEST_MAX_PER_USER` (stack input); never add a lock.

**Fan-out and replica**
- **Fan-out:** the `96bulls-session-ingest-fanout-dlq` backlog stays at 0.
- **Replica:** relayhistory's replica consumer logs zero `replica_receipt_mismatch`, and every `replica_conflict` is explained. The `relayhistory-neon-replica` DLQ stays flat.

**Reads**
- **S3:** new manifests under `96bulls/<org>/v1/ws=…/m=srv_…` carry `writer.plugin_version: 96bulls-session-ingest/1`. The session pipeline indexes them, so the read API and MCP see the canary's new sessions.
- **Neon (transition):** relayhistory reads of the canary's sessions return the same records as before the switch. The Neon replica converges them.

**Latency**
- p95 of forwarded requests is below 1.5 s, and p95 of fast-path requests below 200 ms. Measure from the `delivery forward` `duration_ms` field.

## Known limits during the canary

- **Quarantined records** on linuyc's Desktop (124k, from the earlier 409s) stay counted on the client. They come back only when the client regenerates a job; the ingest then acknowledges them on the fast path. No server data is missing.
- **Selection and reset** stay on relayhistory and Neon. A tombstone written there reaches the ingest's fence only after the relayhistory dual-write follow-up. Until then, the Neon→S3 erasure job erases those sessions after the fact. Run the canary before a user deletes anything, or ship the dual-write first.
- **Staged objects** (`live-batches/**.staged.ndjson.zst`) are not yet expired by a lifecycle rule. That rule is a store change and a follow-up.
