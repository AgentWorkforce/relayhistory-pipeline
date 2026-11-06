# Canonical consumer runbook (supervised leased drain)

- **Default off**: `enabled` must be `true` or nothing runs — disabled is a
  constant zero-access result with no lease/journal/CH/DB touch. When enabled
  an explicit `scope: {orgId, workspaceId}` and `partitionScheme` are required.
  Out-of-scope entries are scope-skipped, never touching images or CH.
  Missing, blank or wildcard scope refuses before any database access.
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

## Tenant configuration and partition continuity

There is no built-in tenant or activation path. The operator supplies the exact
organization/workspace pair through the trusted runtime configuration; the
runner passes the same validated configuration to the consumer. Disabled
consumers require neither tenant nor partition configuration and access nothing.

For new installations select `partitionScheme: "scope-v1"`. The partition hashes
the full organization, workspace and target tuple and fits the existing SQL
partition constraint. Tenants with identical UUID prefixes remain disjoint.

For an existing consumer select `partitionScheme: "legacy-prefix-v1"` and supply
its exact previous UUID tenant pair and unchanged targetId. This reproduces the
old `<org first8>-<workspace first8>-<sha256(target) first32>` key byte for byte;
lease, checkpoint and dead-letter identities stay unchanged. Legacy prefix
partitions are for compatibility with a previously fixed single tenant; do not
use them to multiplex tenants with shared prefixes. The code contains no hosted
tenant fallback. Configure hosted values privately, outside the public source.

Changing scope, target or scheme can select a new checkpoint and replay from
zero. Never switch an existing partition's scheme as an incidental upgrade.
Record the current partition/checkpoint and make any migration a separately
reviewed operator action. This source change edits no migration bytes, applies
no DDL, and enables no consumer. Previously published Git history is unchanged.
