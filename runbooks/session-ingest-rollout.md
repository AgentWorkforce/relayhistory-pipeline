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
| 2 | Supabase roles and migration | As `postgres`, **before** migrate: `supabase/bootstrap/session_ingest_roles.sql` (creates `bulls96_session_ingest` and `bulls96_session_ingest_seeder` NOLOGIN; the migration never creates a role and fails closed, 42704, without them). Apply `supabase/migrations/20261102010000_session_ingest_ledger.sql` to `pyamkrhoyszgzirqqffq`. After it: `session_ingest_role.sql`, then `\password bulls96_session_ingest` (store it in 1Password), then `session_ingest_role_verify.sql`. `GRANT bulls96_session_ingest_seeder TO <operator login>` (no ADMIN) for step 7. | The verify script exits 0, and the migration sentinel `table sessions.ingest_batches` is present. | The tables are new and unused; drop only with Khaliq's OK. |
| 3 | Cloudflare out-of-band resources (relayhistory's account) | **Hyperdrive:** create one named `96bulls-session-ingest`, origin = the Supabase **transaction pooler** (port 6543) as `bulls96_session_ingest`, caching **off** (writes). **Secrets Store:** create `96bulls-session-ingest-s3-access-key-id` and `96bulls-session-ingest-s3-secret-access-key` from the transcript **srv-writer** IAM user's key in 1Password, and `96bulls-session-ingest-signing-key` (a new random 32-byte key, stored in 1Password). Set the same signing key on relayhistory: `wrangler secret put DELIVERY_INGEST_SIGNING_KEY`. | Note the Hyperdrive id and the Secrets Store id. | delete them (unused) |
| 4 | Inputs and plan | Write `.96bulls/cf-sessions-prod/cloudflare-inputs.json` (gitignored) with `sessionIngest: { accountId, hyperdriveId, secretsStoreId, kmsKeyArn: <transcripts CMK ARN>, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" }`. Run `bunx alchemy plan --config stacks/cloudflare/alchemy.cloudflare.run.ts --stage cf-sessions-prod` and capture `docs/plans/cf-sessions-prod.*`. | The plan contract reads "session ingest rollout": a Worker, the fan-out queue and its DLQ, and the consumer, all creates. No secret appears in the detailed plan. | n/a |
| 5 | Deploy the ingest (dark) | Bind Khaliq's approval to the captured plan, then run `bunx alchemy deploy … --stage cf-sessions-prod`. | `96bulls-session-ingest` exists with no route, no workers.dev and no domain. Its settings list HYPERDRIVE, INGEST_FANOUT, NEON_REPLICA and the two Secrets Store bindings. | `alchemy` update phase / disable script |
| 6 | Bind it to relayhistory | The next relayhistory production deploy discovers the Worker (`deliveryIngestExists`) and adds `services: DELIVERY_INGEST`. `DELIVERY_FORWARD` stays **unset**. From now on every selection and reset is also written to the ingest's ledger (all orgs). | relayhistory's settings show the `service` binding; no `delivery forward` lines; no `delivery.ingest_fence_failed` lines; `sessions.upload_deletions` grows with new deletions. | redeploy without the binding; nothing is forwarded |
| 6b | **Gate: Neon→S3 export state for the org** | Run `migration:neon-s3-sync` for the canary org. The seed (step 7) reads the exporter's state (`EXPORT_STATE_FILE`) and seeds only sessions it fully exported with no error, so a record the exporter refused, failed or has not reached stays unstored and is written by the ingest when re-sent. | The seed's `skipped` count is explained (no-session records + unexported sessions). | n/a |
| 6c | **Gate: erasure job deployed with this PR** | The erasure job (`migration:neon-s3-erasure`) running this code also erases a fenced machine's staged batches (`live-batches/**.staged.ndjson.zst`) once they are past the 48 h fan-out redrive window. | A test deletion on a staging org erases its session at once and the staged batch after 48 h. | n/a |
| 7 | Seed the canary org | `NEON_URL_FILE=<(op read …) SUPABASE_SEED_URL_FILE=<(op read …) EXPORT_STATE_FILE=<exporter state.sqlite> bun scripts/session-ingest-seed.ts <linuyc org>` (revisions of exported sessions, then Neon's tombstones and resets). | JSON shows `outcome: seeded`, `read` ≈ the org's `delivery_records` count, `fences` = the org's tombstones and resets. | idempotent; nothing to undo |
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
- **Selection and reset** are written to Neon and then to the ingest's ledger (relayhistory-cloud #265): a deletion fences the session and forgets its revisions, so a later re-enable is written again; a reset does the same for every session of the user.
- **Staged objects** have no lifecycle rule (a store change, deliberately not added: it could delete a staged batch whose fan-out is waiting for a redrive). The erasure job erases them for deleted sessions after 48 h.

## Fan-out DLQ redrive (within 24 h)

A staged batch was acknowledged when its object and fan-out messages were queued; its sessions are written by the fan-out. A slice that reaches `96bulls-session-ingest-fanout-dlq` (10 retries) has written nothing it would claim: its records are not stored revisions, so a client re-export writes them again. But the client got a 200 and will not resend on its own, so:

1. Alert on any DLQ message (check its backlog daily during the canary).
2. Read the failure from the Worker log (`session ingest fanout failed`, `error_code`).
3. Fix the cause (for example a rotated secret), then move the DLQ's messages back to `96bulls-session-ingest-fanout` (Cloudflare dashboard → Queues → DLQ → Retry, or the Queues API). The fan-out is idempotent.
4. Do this within 24 h: the erasure job may erase a deleted session's staged batch after 48 h.
