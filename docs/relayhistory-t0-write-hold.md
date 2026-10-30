# T0 write hold: design (lane 6)

Status: design, 2026-10-30, `96-no-bull-migration`. Implementation lands in
relayhistory-cloud (`packages/relayhistory`) and Cloud as draft PRs. Nothing
here is deployed or enabled without Khaliq's OK for the exact action.

The writer inventory this is based on comes from relayhistory-cloud
`origin/main` 7343404 and Cloud `origin/main` b407c99ff.

## Requirement

At T0 the Worker's database moves from Neon to Supabase (decision: durable
hold + replay). The switch must:

1. lose no accepted write;
2. never write to Neon after T0 (fail closed);
3. never apply a write to Supabase before every earlier write to Neon for the
   same tenant has finished;
4. survive a Worker isolate or colo failure mid-hold;
5. keep each tenant's order.

## What the inventory changes

- **Most logic is in the database.** `accept_delivery_batch`, the projection
  triggers, rollups, outboxes and revision feeds are plpgsql. Replay
  therefore re-runs **client-originated writes only**, through the same
  handlers, and Supabase's triggers rebuild every derived row. Derived rows are
  never copied or replayed.
- **The write path reads state.** Bearer tokens validate against
  `auth_sessions`. Deliveries check `deleted_upload_sessions`, the
  upload-cleanup guard, origins and receipts. Ingest returns
  `machines.cursors_json`. "Fresh data only" therefore cannot start from an
  empty database: the **T0 copy set** below is copied inside the hold.
- **Writers outside the Worker:** Agent Relay Cloud calls
  `request_session_analysis_v1`, `ack_session_revision_events_v1` and
  `ack_brief_completion_events_v1` on Neon directly. Cloud moves to Supabase
  in the same window (or routes those calls through the Worker's
  `CloudHistoryReads` binding), and must tolerate acks of Neon-era event IDs
  that Supabase doesn't have.
- **GET routes write too:** token `last_used_at`, `GET /v1/delivery/records`
  expiry, `/sessions/:id/brief` admission, and the timing, evaluation and
  digest caches. The hold keys on "touches the database", not on the HTTP
  method.

## T0 copy set (copied in full during the hold, before the gate flips)

Small, read on the write path, or security-critical:

- `auth_sessions`: every live token, CLI and service.
- `deleted_upload_sessions`, `session_resets`, and the upload-cleanup state
  (`upload_cleanup_*`), so deleted data cannot reappear.
- `machines`: upload cursors.
- `delivery_origins` and `delivery_receipts`: delivery dedupe and the fast
  path. They may move to the backfill if they are large; the size is still to
  be measured.
- `probe_connections`.

Everything else is backfilled after T0, newest first, with
`ON CONFLICT DO NOTHING`, and derived tables are recomputed per session.
Sizes are to be measured read-only on Neon before T0. The copy set must
finish within the hold budget, or it moves earlier as a pre-copy with a
catch-up pass inside the hold.

## Route classes during the hold

| Class | Routes | Hold behaviour |
| --- | --- | --- |
| Buffer and replay | `POST /v1/ingest`, `POST /v1/sessions/:id/turns`, `POST /v1/delivery/batches`, `POST /v1/onboarding/heartbeat` | Append to the per-org `HoldLog` and replay in order after the flip. All are natural-key upserts or revision-fenced. |
| Refuse with a retryable error | Token login/refresh/revoke, service tokens, `POST /v1/delivery/sessions/selection`, `POST /v1/delivery/sessions/reset`, `POST /v1/sessions/:id/analysis` | `503` with `Retry-After` (deliveries already have a `delivery_busy` path). Token rotation is synchronous and order-critical, and reset/selection are destructive and depend on state, so these are never buffered. |
| Skip (cache or bookkeeping) | Token `last_used_at`, timing, evaluation and digest caches, `memory_retrieval_log`, revision-claim bookkeeping | Don't write during the hold. Each fills again on Supabase after it. |
| Read-only | `GET` reads | Served from Neon during the hold. Afterwards they read from Supabase, falling back to Neon for pre-T0 history. |
| Paused | The per-minute cron, the session-events queue consumer and the DLQ, the brief admission on `GET /brief`, relaycron `reflex/learn` | The cron skips its tick. Consumers `retry({ delaySeconds })`, and Cloudflare Queues already hold messages durably. The analysis state machine (leases, budgets, LLM calls) is never replayed. |

## Mechanism

- **Gate:** a Durable Object, `CutoverGate`, holds the phase, one of
  `neon -> holding -> supabase`. Only an operator call moves it, and it never
  moves backward. Every database-touching entry point reads the phase first. A
  per-isolate cache is allowed only for `supabase`, the terminal phase.
- **HoldLog:** one Durable Object per org, using SQLite-backed storage, which
  Cloudflare replicates. Each entry holds the route, its resolved auth
  context (never the bearer token), the body, an idempotency key and a
  per-org sequence number. The request waits up to `HOLD_WAIT_MS` for its
  replay result and returns the real response. On timeout it returns `503`
  with `Retry-After`. The entry stays durable, and a retry with the same key
  resolves to the replay result.
- **Neon fail-closed:** at the start of the drain, revoke `INSERT`, `UPDATE`
  and `DELETE` on `sessions` from the Worker and Cloud runtime roles on Neon.
  A stale route that still reaches Neon then fails in the database and is
  mapped to the hold. This changes the production database, so it needs
  Khaliq's OK at cutover. Reads are unaffected, so the pre-T0 read fallback
  keeps working.
- **Sequence fence:** every Supabase sequence starts above Neon's final value,
  including `delivery_catalog_acceptance_seq`,
  `delivery_projection_acceptance_seq` and the `session_links` BIGSERIAL.
  It is re-checked after the drain.

## Sequence at T0

1. **Pre-T0 (no traffic change):** the Supabase schema, rollouts and the T0
   gate tests are green. The sequence fence is applied, and the copy-set
   pre-copy is done. The driver seam is deployed with the phase at `neon`.
   Cloud's change to move its three direct calls is deployed behind a flag.
2. Move the gate to `holding`. Writes are buffered or refused by class,
   consumers retry, and crons skip.
3. **Drain:** wait until no Neon write is in flight. That means the gate's
   in-flight counter reads zero, and Neon's `pg_stat_activity` shows no
   active write from the Worker or Cloud roles, bounded by the longest
   statement timeout. Then apply the Neon fail-closed change.
4. Catch up the copy set from Neon, now frozen, and re-check the sequence
   fence. On any failure, abort: move back to `neon`, undo the fail-closed
   change and replay `HoldLog` against Neon. Nothing has reached Supabase yet.
5. Move the gate to `supabase`, and flip Cloud's flag. Each `HoldLog` replays
   in order, and waiting requests receive their results. Consumers and crons
   resume.
6. **Verify:** canary reads and writes on every route class, hold logs empty,
   error rates normal. The backfill starts later, under its own approval.

## Rollback

- **Before step 5:** see the abort path in step 4.
- **After step 5:** Supabase holds fresh writes. A rollback runs a new hold:
  writes since T0 (keys above the fence) are copied back to Neon, then the
  gate moves to `neon`. Neon is never simply repointed.

## Also required before T0

- `scripts/check-database-target.mjs` and `scripts/dev-local.mjs` reject any
  host that isn't `*.neon.tech`. Allow the Supabase project explicitly, not
  any host.
- Read search uses `db.batch([...])` (recall.ts:731) so that
  transaction-scoped `set_config` applies. The driver seam must run it as a
  real transaction (lane 4).
- Code reads `result.rows`, which node-postgres provides, but postgres-js does
  not. Use node-postgres.
- The `upload_cleanup_ready` fail-closed guard (0038/0044) already exists.
  Check whether it can serve as part of the delivery hold.
