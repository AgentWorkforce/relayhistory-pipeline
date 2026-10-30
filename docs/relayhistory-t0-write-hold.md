# T0 write hold: design (lane 6)

Status: design, 2026-10-30, `96-no-bull-migration`. Implementation lands in
relayhistory-cloud (`packages/relayhistory`) as a draft PR. Nothing here is
deployed or enabled without Khaliq's OK for the exact action.

## Requirement

At T0 the Worker's database moves from Neon to Supabase (decision: durable
hold + replay). The switch must:

1. lose no accepted write;
2. never write to Neon after T0 (fail closed);
3. never apply a write to Supabase before every earlier write to Neon for the
   same tenant has finished;
4. survive a Worker isolate or colo failure mid-hold;
5. keep each tenant's order.

## Mechanism

**Gate:** a Durable Object, `CutoverGate`, holds the phase, one of
`neon -> holding -> supabase`. Only an operator call moves it, and it never
moves backward. A rollback is a separate procedure with its own phases (see
Rollback). The phase change is a single write in DO storage, which is
strongly consistent.

**Write path:** every database-writing entry point asks the gate for the phase
before it writes.

- `neon` or `supabase`: write directly to that database, as today.
- `holding`: append the write intent to the tenant's `HoldLog` Durable Object
  (one per org, in SQLite-backed DO storage, which Cloudflare replicates). The
  entry is the route, its resolved auth context (never the bearer token), the
  body and an idempotency key, plus a per-org sequence number. Then wait up to
  `HOLD_WAIT_MS` for that entry's replay result and return the real response.
  On timeout, return `503` with `Retry-After`. The entry is still durable and
  will be replayed, and a client retry with the same idempotency key resolves
  to the replay result instead of writing twice.

**Queue consumers:** while the phase is not `supabase`, they call
`message.retry({ delaySeconds })`. Cloudflare Queues already hold messages
durably, so they need no extra log.

**Crons:** a tick does nothing while the phase is `holding`. Ticks are
periodic, so the next one after the switch catches up.

**Neon fail-closed:** at the start of the hold, the Worker's Neon role is
made read-only for `sessions`, by revoking `INSERT`/`UPDATE`/`DELETE` or
setting `default_transaction_read_only`. Any write that reached Neon by a
stale or cached route then fails in the database instead of being lost after
T0. The Worker maps that error to the hold path. This needs Khaliq's OK at
cutover (it changes the production database), and it is the reason pre-T0
read fallback still works: reads are unaffected.

## Sequence at T0

1. **Pre-T0 (no traffic change):** schema and the T0 gate tests are green on
   Supabase, and the sequence fence is applied (Supabase sequences sit 1,000,000
   above Neon's `last_value`). The driver seam is deployed with the phase at
   `neon`.
2. Move the gate to `holding`. New writes go to `HoldLog`. Consumers retry,
   and crons skip.
3. **Drain:** wait until no Neon write is in flight. That means the gate's
   per-request in-flight counter reads zero, and Neon's `pg_stat_activity`
   shows no active write from the Worker role, bounded by the longest
   statement timeout. Then apply the Neon read-only change.
4. Re-check that the sequence fence still holds: each Supabase sequence is
   above Neon's final `last_value`. If not, abort, return to `neon` (nothing
   has reached Supabase yet) and investigate.
5. Move the gate to `supabase`. Each `HoldLog` replays in sequence order
   against Supabase, through the same handlers and with the stored auth
   context. Waiting requests receive their results. Consumers and crons
   resume.
6. **Verify:** canary reads and writes, hold logs empty, error rates normal.

The hold should last seconds: steps 3 to 5. The backfill starts later, under
its own approval.

## Rollback

- **Before step 5:** move to `neon`, undo the Neon read-only change, and replay
  `HoldLog` against Neon. Nothing has reached Supabase.
- **After step 5:** Supabase holds fresh writes. Rolling back runs a new hold:
  writes since T0 (keys above the fence) are copied back to Neon, then the
  gate moves to `neon`. Neon is never simply repointed.

## Open items

- Confirm the writer inventory: every write entry point, and any writer
  outside the Worker.
- The idempotency key for each write route: an existing natural key where
  there is one, otherwise a client header.
- `HOLD_WAIT_MS` and the client's retry behavior on `503`.
- The cost of one gate RPC per write in steady state. If it matters, cache the
  phase per isolate only while it is `supabase`, the terminal phase, and never
  while it is `neon`.
