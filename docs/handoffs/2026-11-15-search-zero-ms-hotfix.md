# Handoff: 2026-11-15 search outage, session-pipeline zero-ms hotfix

Owner: agent `search-fix-linuyc` (linuyc). Status: **done, idle**. Nothing mid-run.
Incident record owner: the relayhistory-pipeline coordinating session (`hosted-ops/incidents/2026-11-15-upload-outage` in the private ops repo).

## What happened

Session search returned nothing newer than about 2026-11-15 21:16 UTC. Search was not at fault: Cloud MCP
`search_shared_sessions` → relayhistory-api `/v1/events?q=` reads the same ClickHouse
`transcript_events` table that the session-pipeline Lambda writes.

Root cause: the prod session-pipeline Lambda was still running code from 2026-11-05 (`916e89e3`).
Records admitted through the S3 ledger carry `ts_ms: 0`. The old `activityMs`
(`packages/pipeline/src/lib/pipeline/relayhistory-projection.ts`) took that as a real time, so the event
projected at 1970. `src/session-pipeline/search-index.ts` then dropped it from the 2000-01-01 index window,
and the pass still reported complete with zero rows. Main had already fixed this in `6a44d71f`, but it was
not deployed.

## What was done (with Khaliq's explicit OK)

- Branch `hotfix/search-zero-ms-on-916e89e3` @ `d1650da0` = `916e89e3` + the projection part of `6a44d71f`.
  No PR on purpose: it is based on an old commit and must not enter the auto-merge loop.
- Deploy: code-only `update-function-code` of the session-pipeline Lambda at 2026-11-15T23:35:05Z.
  The code is the previously deployed bundle with the same 2-line change. Env/config/IAM are unchanged.
  New CodeSha256 `85iFexOha+yQI4lO8xbbjIx92XYn5WCmpQVixjXLcrw=`. Previous `kkFjZZ1d3ictwprg80G1UWXu/aVI0sAtilSxGj+VSiQ=`.
- Re-drive: 2025 sessions had zero-row "complete" passes since 11-05. The 1888 that map to a manifest
  modified since 11-05 were re-queued as `{"reconcile":{"key":…}}`: 1596 now have rows and 292 still
  project 0. 137 were not re-driven (no manifest modified since 11-05).
- Proof: after the deploy, live search returned S3-admitted events within seconds of upload.

## Open (not owned here; pipeline coordinator)

- 292 re-driven sessions still at 0 rows; 137 not re-driven. Not investigated.
- Alchemy state differs from the live Lambda code until the next Alchemy deploy of main. Main already contains the fix.
- The desktop sends `ts_ms: 0`, so indexed event time is upload time (`received_at`), not the original event time.
- `scope_too_large` on long sessions; `relayhistory_event_identity` on relay-events sources.

## Resume

Nothing is needed to resume. Rollback is `update-function-code` with the previous zip. A copy is kept
outside git on linuyc at `~/.agent-tmp/search-fix-linuyc/deployed/deployed.zip`; the Lambda can also be rebuilt
from `916e89e3`.
