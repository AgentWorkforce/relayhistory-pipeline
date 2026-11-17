# Ledger-log production pilot: one stream

How to measure, switch and switch back ONE production stream of the S3 ledger into log-commit mode. The design and
the guarantees are in [s3-ledger-log-commit.md](design/s3-ledger-log-commit.md), "Production pilot". The operations
are in `src/session-ingest/ledger-log-stream-ops.ts`, the contract in `src/session-ingest/ledger-log-stream-production.ts`,
the CLI in `scripts/ledger-log-stream.ts` and the dormant workflow in `.github/workflows/ledger-log-pilot.yml`.

Nothing here names a bucket, account, org, user or machine. The real values live in the private ops repository and in
the protected `ledger-log-pilot` environment.

## Before anything (each needs the owner's explicit OK in his own words)

- Every replica that can admit the stream runs `INGEST_LEDGER_LOG_COMMIT=on` and async admission for that stream, and
  the operator has written the deployment-wide proof `<keyRoot>/.ledger/log-commit-on.json`. Today the hosted runtime
  and the stack input both refuse `on`: lifting those is separate work, and until then the preflight refuses
  (`log_commit_not_proven_on`).
- The operator role's grants (`stacks/aws/ledger-log-commit-policy.ts` `ledgerLogOperatorStatements` and
  `ledgerLogOperatorKeyStatement`, including the preflight reads) are applied in code, and the bucket's
  `OnlyTheEraserDeletes` carve-out (`LEDGER_LOG_DELETE_CARVE_OUT`) is in place.
- `LEDGER_LOG_PRODUCTION_ENABLED` is flipped to `true` by a reviewed one-line change. While it is `false`, every
  production apply refuses (`production_dormant`); status, measure and plans still run.

## 1. Measure (read only, always allowed)

Find the hottest stream of the user first. Without `--origin`, every origin of the user is reported, hottest first,
by a 12-hex prefix of its key hash (never the origin id):

```
bun scripts/ledger-log-stream.ts measure --bucket <b> --region <r> --org <o> --workspace <w> --user <u> --window-seconds 900
```

Run it again with `--origin <origin>` for each candidate machine; the `originKey` that matches is the stream. Read
`batchesPerSecond` (the window average) and `peakBinBatchesPerSecond` (the busiest minute). `aboveVisibleRate` is true
at 1.4 batches/s and above, the rate at which the legacy chain's per-user latency becomes visible. `truncated: true`
means the listing budget (`--max-pages`, 1000 objects a page) ran out: the rates are lower bounds.

## 2. Plan

```
bun scripts/ledger-log-stream.ts switch --bucket <b> --region <r> --org <o> --workspace <w> --user <u> --origin <origin> --production
```

Run it from a clean worktree at a commit on `origin/main` (anything else refuses: `production_worktree_dirty`,
`production_code_not_on_main`). It writes nothing. It prints the preflight (counts and verdicts) and, when every check
passes, `production.confirmation`:

```
LEDGER-LOG-SWITCH BUCKET <sha256> STREAM <sha256> SHA <commit> STATE <sha256> EXPIRES <iso> PLAN local
```

The phrase is valid for 30 minutes. Preflight refusals and what to do:

| Code | Meaning | Action |
| --- | --- | --- |
| `stream_not_found` | no committed legacy batch for these ids | check the ids; never switch an unused stream |
| `stream_is_log` / `stream_is_closed` | already switched / already rolled back | a closed stream is never reopened |
| `fence_authority_absent` / `_invalid` / `_stopped` | the org is not on the S3 ledger, or admission is stopped | resolve the migration wave first |
| `user_fence_reset_in_flight` / `_unreadable` | a privacy reset is running for the user | wait for it to finish |
| `log_commit_not_proven_on` | no deployment-wide proof | see "Before anything" |
| `legacy_batches_pending` | a legacy batch is mid-claim | normal on a busy stream: plan again in a few seconds |
| `stream_log_corrupt` | the stream's log fails validation | stop; investigate |

## 3. Switch

```
bun scripts/ledger-log-stream.ts switch ... --production --apply --kms-key-arn <arn> --confirm '<phrase>'
```

The step runs the preflight again, rebuilds the phrase from it and refuses unless it matches byte for byte
(`production_confirmation_mismatch`), is unexpired (`production_confirmation_expired`) and expires no further than one
TTL ahead (`production_confirmation_expiry_too_far`). The same phrase may be retried until it expires (for example
after `legacy_batches_pending`). It then writes entry 0, waits 5 minutes and checks the legacy batch docs again.

Results:

- `applied: true`, no `refused`: the stream is in log mode and registered. Watch `status` until `materializedThrough`
  follows `head`.
- `applied: true, refused: legacy_raced_switch`: entry 0 is written, but a legacy writer committed (or is still
  pending) behind it. The stream IS in log mode; admission registers it on its first take. Let the log drain
  (`status`: `materializedThrough` equals `head`), then roll back (step 4) and investigate the deployment's flag state.
- the process died after entry 0: run `status`. `mode: log` means the switch landed; the registry pointer is written
  by admission's first take anyway. Continue as for `legacy_raced_switch` if in doubt.

## 4. Switch back (rollback)

```
bun scripts/ledger-log-stream.ts close ... --production                     # plan: prints LEDGER-LOG-CLOSE ...
bun scripts/ledger-log-stream.ts close ... --production --apply --kms-key-arn <arn> --confirm '<phrase>'
```

Close refuses until every entry is applied (`entries_not_applied`; on a busy stream, plan and apply again in a few
seconds). It does not need the fence authority: a rollback stays possible while admission is stopped. After the close
marker it waits 5 minutes and reports the stream again as `after`: a writer that had the stream cached may append the
one entry it had in flight (`after.head` above `status.head`). Keep the consumer running until `status` shows
`materializedThrough` equal to `head` again. Close is one way: the stream is never reopened. A crash after the marker
is harmless: `status` reads `closed`, and a repeated close refuses with `stream_is_closed`.

## The workflow (dormant)

`.github/workflows/ledger-log-pilot.yml` runs the same steps from GitHub instead of a laptop: modes `status`,
`measure`, `plan-switch`, `switch`, `plan-close`, `close`, all at the exact `main` head, dispatched by the pinned
account, in the protected `ledger-log-pilot` environment (which does not exist yet: the gate refuses until it is
created with its read and write roles, in code). The stream is the environment secret `LEDGER_LOG_PILOT_STREAM`
(`{"org":..,"workspace":..,"user":..,"origin":..}`), set before the dispatch and removed after; bucket, region, key
root, KMS key and role ARNs are its protected variables (`LEDGER_LOG_PILOT_*`). `switch` and `close` take `plan_run`
(the successful `plan-<action>` run, under 30 minutes old) and the `confirmation` from that run's plan artifact, which
the run checks byte for byte before the operation rebuilds it. The roles are assumed for a 6-hour session
(`scripts/backlog-ops.ts` `assumeRole`), so their maximum session duration must allow that.
