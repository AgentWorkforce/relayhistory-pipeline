# Phase 3 search pre-read handoff

Checkpoint recorded for the 2026-11-22 team reset.

## Location

- Machine: `linuyc`
- Repository checkout: `/home/khaliqgant/work/idx-skip`
- This document: `/home/khaliqgant/work/idx-skip/docs/handoffs/phase3-s2a-b-codex-2026-11-22.md`
- Checkpoint branch: `checkpoint/phase3-s2a-b-codex-20261122`

## Status

The Phase 3 search-index pre-read slice is complete. It moves the legacy
ClickHouse whole-read path's object-store read and index scan outside the
scope-lock transaction behind `SEARCH_INDEX_PREREAD_ENABLED`. The flag is
default off. This lane did not enable it, deploy, or apply a migration.

The slice was re-cut and landed on `main` at
`1c0c435ca88274e37d315061a4ea125cf02d9bcd`. The original frozen review branch
is `review/phase3-s2a-b-codex-search-preread` at
`789cadd7a21942a2f12fc8f6e5ec7d08f4a18a91`. There are no pull requests.

## Interrupt and decision state

No test, build, deploy, migration, or other long-running job belongs to this
lane. It is safe to interrupt or restart the machine. Long-running migration
processes observed in another checkout are outside this lane and were not
touched.

Enabling `SEARCH_INDEX_PREREAD_ENABLED` on any installation still requires
Khaliq's explicit decision. Nothing else is waiting on a decision from him.

## Resume

Fetch current `main`, confirm the landing and push-CI status if that evidence
is needed, and obtain a new assignment from the pipeline owner. Keep the
original review branch frozen unless that owner requests a specific follow-up.
No unfinished implementation needs to be recovered from this checkout.

## Verification at the final review head

- Search pre-read and flag-off golden tests: 12 passed, 0 failed.
- AWS session-pipeline stack tests: 77 passed, 0 failed.
- TypeScript typecheck, engine-identifier guard, unit-test shard check,
  function-boundary check, and `git diff --check`: passed.

