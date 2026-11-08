# F10 canonical session revision events

This is a dormant source/runtime package. The migration-handover lead is the sole production
operator. Do not merge/apply the schema until the consolidated migration-window
signoff; do not deploy or enable from this lane.

Friday scope is `session.revised` only. Brief completion is deferred with L2/#212.
The Cloud ABI remains exactly `{kind:"session.revised",eventId,generation}`;
generation is a decimal bigint string. No transcript text, title, arbitrary
tenant identity, raw.state-derived completion, or fabricated job ID is sent.

## Source installation and activation order

1. Compose on the cleared #230 contract (`6bce87c`, merged on main), through the unified-generator #242 stack. F10 owns
   `20261105033000_canonical_project_events.sql`, after large-read routing `05032000`, journal `05025000`, and
   lineage `05026000`, before read API `05034000`. It does not replace any 610/620 function. Regenerate
   ACL/boundary/verifiers and the F10 rollback with `bun scripts/sync-sessions-function-boundary.ts` and
   check with `--check`; never hand-edit generated ACLs. The canonical-project-events
   lane manifest owns exactly ten functions and six grants, with the actor
   verifier configured in the shared verifiers file. The history script remains
   a wrapper; it must not contain a separate F10 generator. Record the final composed
   `generation()` once; both runtimes must use that reviewed pin.
2. Operator runs the existing phase-one role bootstrap before migrating. It adds
   bare `bulls96_project_events` NOLOGIN/NOINHERIT. The migration grants that actor
   only read/pending/ack metadata RPCs; the existing ingest actor gets only the
   claim/sent/release sender RPCs. No table privileges. The new pure actor verifier
   runs in both migrator planning and the composed generation inventory.
3. After migration, operator verifies the actor, applies
   `supabase/bootstrap/canonical_project_events_role.sql`, sets its password from
   1Password in memory, and verifies again. Use a dedicated Hyperdrive for Cloud
   with this actor. Never reuse an owner or ingest connection for Cloud.
4. Deploy Cloud's canonical Queue and DLQ, attached to its existing project
   Worker. Deploy the separate publisher Worker, default off. The ingest
   HTTP/queue Worker retains its existing private, no-cron contract.
5. **Before the first selected creator publication**, operator inserts/enables
   exactly one reviewed row in `sessions.canonical_project_event_scopes` with
   the final schema-generation pin. The canary is org
   `c4fae7b9-43a9-44e8-94d5-455aa579d67b`, workspace
   `1220c1dc-0ce5-45fe-8b67-e7bb57a2db63`. The table is empty on install. There is
   no migration-time backfill: enabling after a publication does not recreate a
   missed event. Stop and coordinate a bounded journal replay if this order was
   missed; do not synthesize a creator transaction or broaden scope.
6. Operator enables the publisher and Cloud only after both pins/bindings/scope
   are checked. A journal insert and its outbox event commit or roll back in the
   same creator transaction. Replacements coalesce a stable ID with a higher
   generation. Seed journal rows never become Cloud events.

## Publisher deployment contract

`canonicalProjectPublisherConfig` in `src/canonical-project-events/deployment.ts`
renders a Wrangler config; rendering performs no apply. Supply a non-secret
Hyperdrive ID for the existing ingest actor, final schema-generation pin, exact
scope, and `relayhistory-canonical-project-events` (stage suffix when applicable).
The returned config uses `src/canonical-project-events/worker.ts`, Worker name
`96bulls-canonical-project-events`, no public routes/workers.dev/preview URLs,
nodejs_compat, and a once-per-minute cron. Render from the repository root so its
entry path resolves correctly. Keep the generated config on the operator's
review path; it contains IDs/flags only, never URLs/passwords.

| Binding/var | Required value |
| --- | --- |
| `HYPERDRIVE` | Existing `bulls96_session_ingest` Hyperdrive |
| `CANONICAL_PROJECT_EVENTS` | Producer binding to Cloud's separate canonical queue |
| `INGEST_CANONICAL_PROJECT_EVENTS_ENABLED` | `false` until reviewed activation |
| `INGEST_EXPECT_GENERATION` | Final composed generation, never runtime-adopted |
| `INGEST_CANONICAL_PROJECT_EVENTS_ORG_ID` | Exact canary org |
| `INGEST_CANONICAL_PROJECT_EVENTS_WORKSPACE_ID` | Exact canary workspace |

Each tick claims at most 20 events (RPC maximum 50), leases for 60 seconds, sends
metadata-only messages and marks sent with generation/token/version CAS. Send
failure releases failed/unsent claims; ambiguous send confirmation may duplicate
a delivery. Unacknowledged events resend after five minutes. A queue send is not
Cloud admission. Cloud ACK checks the exact generation and current frozen source.
Recovery selection rotates bounded pending pages so backoff cannot starve later
events. Logging contains counts/codes only.

## Acceptance and rollback

Cloud currently resolves identity from the post-cutover authority and admits a
generation-specific durable refresh after workspace/membership checks. Its
canonical PR-evidence port is **not implemented in deployment wiring**. The
default throws `canonical_pr_evidence_unavailable`; it must remain retryable and
must not ACK the source or fabricate empty evidence. F10 is conditional until
that port and operator canary proof are supplied. Summaries remain separately
gated by `CLOUD_PROJECT_SUMMARIES_ENABLED`; enabling F10 does not enable L2.

Local tests cover actual creator → journal → outbox atomicity, restricted actor
RPCs, replacement/stale ACK and lease fencing, queue failure/retry, scope and
readiness refusals, and pure verifier failures on extra grants. Cloud's optional
cross-repository contract uses the actual migrations/publisher. Its explicitly
injected empty-evidence test port proves ACK transport only, not production
evidence readiness.

Operational rollback: disable the scoped publisher and Cloud flags and disable
the exact scope row; retain the outbox and Cloud receipts. Never delete pending
events, reset generation, or revert creator authority to force success. The schema
down migration refuses any event rows or enabled scope and is only for an unused
installation. It refuses a later applied upstream ledger step, so roll later
steps back first. On legal schema removal it deletes only its own final ledger
row, allowing normal migrate to reapply it. It never drops the cluster role. Re-enable only with the same
reviewed generation/scope, or a newly composed and verified rollout.
