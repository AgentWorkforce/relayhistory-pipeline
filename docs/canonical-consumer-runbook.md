# Canonical consumer runbook (supervised leased drain)

- **Default off**: `enabled` must be `true` or nothing runs — disabled is a
  constant zero-access result with no lease/journal/CH/DB touch. When enabled
  the scope is hard-fenced to the c4fae7b9/1220c1dc canary pair (out-of-scope
  entries are scope-skipped, never touching CH).
- **Lease**: one active owner per partition via `canonical_feature_consumer_lease`;
  `heartbeat_at` renewed each entry; a stale owner is replaced after `leaseTtlMs`.
- **Order**: revision-ordered journal paging (`journal.entries`); each entry
  `begin->pages->finish` through the real ACK store + finish RPC.
- **Crash-safe**: restart calls `begin`/`resumeCursor` — `already_complete`
  advances the checkpoint; `resume` continues the verified chain; `corrupt`
  refuses.
- **Readiness**: only after `finish` commits BOTH pools complete (dirty->complete)
  for the exact revision — never an in-memory flag.
- **Retries**: bounded `maxRetries` with linear backoff; exhaustion dead-letters
  (never skipped) into `canonical_feature_consumer_dead_letter`.
- **Bindings**: existing PG/CH only; `targetId` (deployment identity) REQUIRED.
- **Metrics**: drain returns `{copied, dead, skipped, checkpoint, status}`;
  the runner aggregates holds/deads/noLease + healthy; the dead-letter table
  is the operator evidence.
