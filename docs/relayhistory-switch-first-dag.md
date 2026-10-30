# Switch-first: dependency graph, T0 protocol and invariants

Khaliq approved switch-first on 2026-10-30, confirmed directly in the
helm-chart-chief thread, for design and code planning. This doc refines
`docs/decisions/0002-relayhistory-onto-the-pipeline.md` and
`docs/relayhistory-pipeline-migration.md`.

It authorizes no live action. Each of these still needs Khaliq's explicit OK
for that exact action: creating the branch, creating keys, merging, applying,
deploying, exporting and cutting over.

## Pinned dependencies (heads as of 2026-10-30)

| Gate | PR / artifact | Head | State |
| --- | --- | --- | --- |
| G1 Cutover gate, durable hold, data-loss fix | relayhistory-cloud #224 | `d697acd9ae` | draft; fix lane active |
| G2 Worker TCP driver | relayhistory-cloud #225 | `5872005b41` | draft; held for independent PASS |
| G3 Worker writes into the pipeline, reads from `session_index` and S3, bounded Neon fallback | relayhistory-cloud (P3) | none yet | **unowned** |
| G4 Cloud consumers (Sessions board, MCP) and post-T0 write gating | cloud #5850 + P4 | `411ef650ab` | ready, held |
| G5 Logical session directory and agent name | 96BullSessions #38 | `71b8ffdd63` | draft |
| G6 V1 search projection columns and indexes | `docs/session-search-projection.md` | `400994d` (doc) | migration not written |
| G7 Per-workspace KMS keys for new writes (P9) | sessionspipelinegrok + Khaliq | none | decision open |
| G8 Erasure path for the search projection (P8a) | sessionspipelinegrok | none | decision open |
| G9 Runner and rollouts against any Postgres | relayhistory-cloud #223 | `b06e973f25` | draft, CI green |
| E1 Exporter (export only, after T0) | 96BullSessions #40 | `a94149a50c` | draft |
| E2 Migration assertion (export only) | relayauth #110 | `a1368a3c10` | draft |
| E3 Single-use assertion check at ingest (export only) | relayhistory-cloud | none | **unowned** |
| E4 Transform reads exported streams (O3, export only) | pipeline | none | **unowned** |

**T0 needs G1 to G8.** E1 to E4 gate only the export after T0. The agent
index (relayhistory-cloud #222, `7264a70`) belongs to the Neon schema; its
role after T0 moves to G5 and G6.

```
G7 (KMS) ─┐
G8 ───────┤
G5 → G6 ──┼─→ G3 (Worker writes + reads + fallback) ─┐
G2 ───────┘                                          ├─→ PRE-FLIP READY → [A7] T0 → export (E1–E4) → [A9] fallback removed → [A10] Neon retired
G1 (gate + hold + fix) ──────────────────────────────┤
G4 (Cloud consumers) ────────────────────────────────┘
```

## Pre-flip readiness (all must hold; checked, not assumed)

1. **G1 to G8 merged and deployed with Khaliq's OK.** The gate is bound and
   in phase `neon`; the Worker runs the new code with routing still on Neon.
2. **Pipeline ready:** pipeline ingest, the S3 bucket with per-workspace KMS
   keys, and the Supabase index and projection (A3) are healthy. A canary
   session written through the pipeline appears in `session_index` with its
   S3 objects.
3. **Fallback rehearsed:** the fallback path is exercised against a staging
   child branch, with telemetry visible.
4. **Rollback rehearsed:** a reverse run on staging.
5. **Neon load relief in place:** compute size, and `pg_stat_statements` for
   monitoring (installed 2026-10-30).

## T0 protocol (atomic routing)

1. **Hold:** move the gate `neon → holding`. Writes are buffered durably in
   the HoldLog or refused with a retryable 503. Reads keep serving from Neon.
2. **Drain and fence:** wait until no Neon write is in flight, then apply the
   scoped Neon write fence (`migration:neon-fence`). Writes to `sessions` are
   now impossible on Neon.
3. **Cutover marker (immutable):** record `pg_current_wal_lsn()`, the
   timestamp and the gate generation. The marker is written once to S3 and to
   Supabase, and is never edited.
4. **Child branch:** create a Neon branch **at the marker LSN** (copy on
   write, its own compute). Validate it before going on:
   - the marker read back from the branch matches;
   - row counts and the sessions ledger equal production at the LSN;
   - the branch's compute answers read-only queries.
5. **Flip:** move the gate `holding → supabase`. Writes now go only to the
   pipeline. HoldLogs replay, in order, into the pipeline. Cloud's flag flips
   at the same moment (G4).

**Abort boundary:** steps 1 to 4 can be aborted. Apply the fence's
`.restore`, move the gate `holding → neon`, and the HoldLogs replay to Neon.
The child branch is discarded. After step 5, no repointing: rollback means a
new hold and a reverse copy of pipeline records written since T0 into Neon in
delivery-record form, then the fence restore.

## Reads after T0 (bounded, read-only fallback)

- **Source order:** a session in `session_index` (exported, or written after
  T0) is read from Supabase and S3. A session that isn't there yet is read
  from the **T0 child branch**, never Neon production, so the production
  compute takes no RelayHistory reads after T0 either.
- **Bounds:** the fallback is read-only (a client with no write methods,
  plus a guard that rejects anything but SELECT). It has per-request row and
  time limits, and a circuit breaker on the branch's compute.
- **Telemetry:** fallback hit rate per route, latency, breaker trips, and the
  export coverage (`migration:copy --mode=check`-style). The fallback is
  removed (A9) when coverage reaches 100% and the hit rate is 0.

## Straddling sessions (O4)

A session active at T0 has records before the marker (on the branch) and
after it (in the pipeline). The session's raw stream is append-only:
- post-T0 records append as live chunks;
- the export appends the pre-T0 records later, in a dedicated epoch;
- the normalizer orders every record by event time and keeps each record's
  latest revision, so the order chunks arrived in doesn't matter.

The dedicated historical epoch reason is `backfill`. The ingest boundary
accepts it only from a grant-bound migration assertion and only as an
append-only change that preserves the live `raw.owner`, writer, session and
all prior chunks. Both migration and live writes compare-and-swap the exact
object version they read, so a straddling session has one manifest order and
the losing writer must reread before it can continue.

## Invariants (data loss and dual write)

- **I1:** every write accepted before the marker is on Neon at the marker LSN,
  so it is on the child branch, and the export reads it from there.
- **I2:** no write reaches Neon `sessions` after the fence (DB-enforced),
  and no RelayHistory write reaches the pipeline before the flip
  (gate-enforced).
- **I3:** every write accepted during the hold is durable in the HoldLog and
  replays to exactly one side: the pipeline after the flip, or Neon on abort.
- **I4:** the export is idempotent (deterministic streams, create-only
  chunks, content-hash comparison) and never overwrites a post-T0 record. On
  the same key the latest revision wins, and a deletion recorded after T0
  wins over the export.
- **I5:** reads are complete in every state: before T0 from Neon, during the
  hold from Neon, after T0 from the pipeline or the child branch.
- **I6:** Neon production takes neither writes nor RelayHistory reads after
  T0. It stays intact, read-only, until A10.
