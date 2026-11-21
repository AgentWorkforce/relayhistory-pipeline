# Offline replay of action-scoped outcomes

`action-scoped-outcomes` is an explicit, separate offline experiment over a
manifest-bound `/2` action-provenance capture. It tests T1 duplicate migration
numbers and T2 commits attributed to another session and later reverted. It
does not reproduce the production advisor, claim conflicts, T3 file collisions,
priority ranking, agent delivery or human notifications. Its `pull` alerts are
hypothetical outputs, not delivery receipts. The existing replay default is
unchanged, and the CLI refuses mixing this adapter with session-scoped detectors.

```sh
bun run src/session-intel/replay/cli.ts replay \
  --snapshot /private/operator/capture \
  --detectors action-scoped-outcomes \
  --out /private/operator/replay
```

Produce the capture using the importer's explicit
`--action-repository-paths --repository-map <path>` options. See
[action provenance](../src/session-intel/import/action-provenance.md) for the capture contract.
This consumer accepts only `session-intel-action-provenance/2` with the existing
`m4-import-salted-segments/1` path vocabulary. The `/1` descriptive reader stays
strictly `/1`; the capture's `capture_only` and `consumedByReplay:false` fields
remain its original versioned declaration, not a claim of live integration.
This opt-in adapter is the separately named consumer. No salt, reverse map,
operator map, Git invocation or network read is used during replay.

Each write, commit and outcome joins its own provenance by harness, original
session id, kind, event id and timestamp. Detection runs separately for each
declared action repository. A session active in two repositories keeps its
original identity in both partitions. Session and write branch metadata, titles,
usage and claims are never assigned to those partitions. Original parent links,
including metadata-only ancestors outside a partition, suppress planned fleet
pairs. Parent links are static snapshot metadata; their historical availability
is not established by this experiment.

A fact enters a partition only when both its fact time and provenance
`availableAt` are strictly before the simulated tick. Late repository declarations
therefore delay visibility and the hypothetical alert time. Equality stays hidden.
An alert's `alertAt` includes its latest required evidence availability; `sentAt`
is the replay tick. Evidence keeps its original fact timestamps. The seven-day
lookback uses fact time, so a fact arriving already outside it is counted as
`staleOnArrival`. It was never eligible within that lookback.

Pair cooldown and recipient hourly limits are shared across all repositories.
Deferred candidates remain pending while their supporting finding exists.
An alert id includes the repository and finding identity, and each is emitted
once per replay. This is not the live service's expiration or reincarnation
policy. Repository declarations are operator or trace claims, not independent
proof that an action changed that checkout.

The manifest, retention deadline, exact provenance binding, one-tenant frame and
existing corpus input budgets are checked first. Visible facts are capped at
4,096 overall and 256 per repository; parent traversal at 256 distinct ancestors;
finding identities at 10,000. A cumulative work budget of 20 million units
charges `4 * facts² + lanes` per changed repository, with a bounded detector
result. Work units are a conservative accounting rule, not elapsed CPU time.
Unchanged ticks do not rerun detection. A cap stops further detection and adds a
`rejected` diagnostic. Missing/conflicting action scope, missing semantic write
paths and unkeyed commits or reverted outcomes also add explicit rejected counts.
These gap counts cover all input actions, including actions outside the replay
window. Such runs are incomplete, return CLI exit code 1, and are refused for
held-out scoring. A complete run only means these checks found no known gap;
it does not certify representative workspace coverage or independent labels.

Use the report's diagnostics, detector parameters and source digest when
comparing experiments. Claim extraction and independently adjudicated outcomes
are still needed for a representative real-workspace quality study. Synthetic
tests, migration collisions and a clean replay alone do not establish useful
early-warning precision, recall or lead time. The existing M4 path policy can
also refuse semantic paths, including mapped home roots; this adapter preserves
those refusals rather than guessing a path.
