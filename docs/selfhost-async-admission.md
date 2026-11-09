# Self-host async upload admission (`SELFHOST_INGEST_ADMISSION`)

Status (2026-11-08): code and tests only, default off (`sync`). Nothing is deployed, and no chart,
stack, migration, workflow or `generation()` change comes with it. This is the self-host counterpart
of the hosted design, [`docs/design/s3-first-async-ingest.md`](design/s3-first-async-ingest.md). Read
that first: this page only lists what is different.

Code:
- `src/session-ingest/selfhost-config.ts`: `SELFHOST_INGEST_ADMISSION` and its knobs.
- `src/session-ingest/selfhost-server.ts`: wiring, the startup gate, `/readyz`.
- `src/session-ingest/selfhost-admission.ts`: the consumer (scan, scheduler, dead letters).
- `src/shared/locker.ts` `assertConditionalWrites`: the conditional-write probe.
- `src/session-ingest/app.ts`: the `asyncAdmission.selfhost` hooks (inert unless set; the hosted
  Worker never sets them).

Tests:
- `src/session-ingest/__tests__/selfhost-admission.pglite.test.ts`: over real node:http, every
  migration in PGlite, a counted SQL runner and a scratch object store.
- `src/session-ingest/__tests__/selfhost-admission.test.ts`: settings, the probe against a fake S3
  endpoint that honours or ignores the conditions, the GCS create header, dead-letter keys, and the
  hosted import boundary.

## What it does

| Mode | Request path | Consumer |
| --- | --- | --- |
| `sync` (default; unset or empty is the same) | today's synchronous canonical path, byte for byte. No consumer, no probe, no inbox. | none |
| `async` | relaycontrol introspection, the privacy-journal witness (one store GET), parse, prepare and re-scrub, then ONE create-only inbox PUT. Answers the hosted queued receipt: the same receipt body and `x-relay-admission: {"version":1,"state":"queued"}`. | admits the inbox |
| `drain` | `sync` again | keeps admitting what is already in the inbox |

The mode is deployment config only. No request header or body can select it. The wrapper strips
`x-relay-ingest-identity`, and the opt-in is stamped on the identity relaycontrol verified.

**No SQL on the request path, and no fallback.** In `async`, the upload routes make no database call.
A witness failure, an inbox PUT failure or anything else that keeps a batch off the queued path is a
503 `delivery_unavailable` with nothing acknowledged, never a synchronous retry. Validation answers
(400, 403, 413, 415, 422) keep their synchronous status, code and body (tested side by side).

**The remaining authorization dependency.** The no-SQL claim covers the ingest database only. Every
request still makes one relaycontrol introspection call, with no positive cache, exactly as in `sync`
(`selfhost-auth.ts` is unchanged). A revoked token or a removed `rth:sync` is refused on the next
request. If relaycontrol is down, uploads get a retryable 503 `relaycontrol_unavailable` in every
mode.

**Status read.** In `async` and `drain` only, `GET /v1/delivery/admission?originId=…&receiptId=rhr_…`
(`rth:sync`, tenant from the verified identity, store reads only) answers
`{receiptId, state: admitted | refused | pending, code?}`, as hosted. In `sync` it stays 501.

## The consumer: the inbox is the queue

There is no queue self-host. The consumer runs in the same Node process, and every replica runs one.

- **Discovery.** A ListBucket scan of `<keyRoot>/.admission-inbox/` every `SELFHOST_ADMISSION_SCAN_MS`
  (default 15 s) finds every inbox object that still holds content (`pendingAdmissions`, the hosted
  recovery scan, with no age threshold). The request path also hands its fresh key to its own
  process's consumer, so an upload starts at once. That hint is a latency optimisation only: the scan
  is the authority.
- **Admission.** Each object goes through the hosted `admitMessage` (`admission-consumer.ts`),
  unchanged: decode and key binding, claim, privacy gate, writes, commit, outcome marker, inbox
  tombstone. The ledger is canonical, as on the synchronous self-host path. Nothing is forked.
- **Exactly once, as hosted.** Idempotency is the claim's (batch id + digest): a Desktop retry is a
  second inbox object whose admission replays; two replicas racing on one object converge on one
  admission, as concurrent synchronous retries do today. A crash at any point leaves the content in
  place for the next scan. Before the commit the claim resumes; after the commit but before the
  outcome marker, the claim replays and the outcome is written (both tested across a process restart).
- **Bounded.** `SELFHOST_ADMISSION_CONCURRENCY` (default 4) objects in flight per process, oldest
  upload first. Each object backs off on its own: the hosted 15 s to 5 min by attempt, the slot's
  `Retry-After`, 15 s for a pending privacy control, 5 min for a malformed object. The tenant slots
  (`INGEST_MAX_PER_USER` / `_ORG`) still apply inside the claim.
- **Dead letters.** After `SELFHOST_ADMISSION_MAX_ATTEMPTS` failures (default 20, the hosted queue's
  max retries) in one process, the consumer writes a marker at
  `<keyRoot>/.admission-dlq/<the inbox path>.json` holding `{v, kind, code, attempts, atMs}`. It holds
  no content and no tenant field beyond what the path already names. Every replica skips the object
  while the marker is live.
  - **14-day retention.** A marker is live for 14 days (1 209 600 s, the hosted DLQ's message
    retention), measured from its last write. After that it has expired, and the object is attempted
    again from attempt 1, so it dead-letters again if the cause remains. Hosted, an expired DLQ message
    is gone and only the operator's recovery scan re-sends it. Self-host re-attempts on its own, which
    is never less safe: a re-attempt re-runs every check, so a redrive after a reset still refuses a
    pre-reset upload.
  - **Operator redrive.** Delete the marker. The next scan picks the object up.
  - **The bytes are never deleted.** No code path and no lifecycle rule here expires or deletes an
    acknowledged inbox object. Only a terminal outcome retires its content, with the hosted tombstone.
    A versioned bucket's noncurrent rule then purges the old version; an unversioned MinIO bucket
    overwrites it.

## Privacy and deletion fences

Everything the synchronous claim enforces, the consumer enforces through the same SQL: the deletion
filter, the reset count, the owner, the commit-time invalidation and stale revisions. On top of that,
the hosted privacy-journal gate runs between the claim and any write (`assertNoLaterPrivacyControl`).

| A control lands after the upload, before admission | Result | Test |
| --- | --- | --- |
| disable | the claim filters the session: acknowledged, nothing stored (as a synchronous upload after the disable) | yes |
| reset, journaled | refused `delivery_privacy_superseded`; an upload after the reset is admitted | yes |
| re-enable after a disable, journaled | refused `delivery_privacy_superseded` | yes |
| any journaled control still pending | retried in 15 s, never admitted across it | yes |

**The journal invariant.** The journal only works if every reset and re-enable brackets it
(`beginPrivacyControl`, SQL, `settlePrivacyControl`). Self-host serves no selection or reset route
today (501), so the invariant holds trivially. The app's own control routes, if they were routed, are
refused before SQL in this mode (a non-ordered control is a 503; tested). Two operational rules
follow:

1. **No out-of-band reset or re-enable while the inbox is non-empty.** A reset applied straight in SQL
   (an operator, a script) bypasses the journal. An upload still in the inbox would then be admitted as
   post-reset content. Either bracket it with the journal or first move to `drain` and wait for
   backlog 0 and no dead letters.
2. **Roll back as `async`, then `drain`, then `sync`.** Wait for backlog 0 (`/readyz`) between `drain`
   and `sync`. In `sync` nothing admits the inbox, so objects left there wait, unadmitted but never
   lost, until the mode is `async` or `drain` again.

A future self-host selection/reset route must journal its resets and re-enables, the way the hosted
`journaled()` does. A disable needs no journal. Open PR #392 adds a disable-only self-host selection
(re-enable is a 400). Under it, the consumer's later claim filters the session, and an admission that
claimed before the disable has its commit invalidated, then retries into the filter.

## Conditional writes (startup gate)

The inbox needs a real create-only PUT, and the journal needs a real compare-and-swap. Both adapters
already send them:

| Backend | Create | Compare-and-swap |
| --- | --- | --- |
| S3, MinIO | `If-None-Match: *` (412 or 409 refused) | `If-Match: <etag>` |
| GCS | `x-goog-if-generation-match: 0` | `x-goog-if-generation-match: <generation>` |

A backend that silently ignores them (an old MinIO, an S3 look-alike, a proxy that strips the header)
would turn a create into an overwrite. So in `async` and `drain`, startup runs
`assertConditionalWrites` once, at `<keyRoot>/.admission-probe/v1/ws=probe/m=srv_<nonce>/probe.json`:
create, refused re-create, swap, refused stale swap, read back, then a best-effort delete. Any
failure refuses startup with `ConditionalWriteUnsupported` (`create_only`, `compare_and_swap` or
`probe_failed`), naming the property and an error code only. `sync` never runs it. If the credential
cannot delete, one small probe object per pod start remains.

## Readiness and telemetry

- Probes (internal only, never routed publicly). The exact contract is in
  `docs/selfhost-composition.md` section 5.
  - Both `/healthz` and `/readyz` pin the effective mode, `admission: "sync" | "async" | "drain"`.
    In `async` and `drain` they add
    `consumer: {ok, state, backlog, oldestAgeMs, deadLettered, inFlight, lastScanAgeMs}`, which holds
    numbers and a closed `state` (`starting`, `ok`, `stale`, `stopped`) only.
  - `/readyz` in `async` is 200 only while the consumer's last scan succeeded within
    max(3 × scan interval, 60 s). The database is deliberately not part of it: a database outage shows
    as backlog, and uploads keep succeeding.
  - `/readyz` in `drain` needs the consumer AND the database (`SELECT 1`), since the request path is
    synchronous again.
  - In `sync`, `/readyz` is unchanged apart from the added `admission: "sync"`.
- Logs:
  - one `ingest.admission` line per attempt, with the hosted fields (`outcome`, `error_code`, `stage`,
    `offset` = lag ms, `attempt`, `db_error_*`), plus `outcome: dead_lettered`;
  - `ingest.admission_lock_alert` for 55P03 / 57014;
  - one `ingest.admission_consumer` line per scan: `admission_backlog`, `admission_oldest_age_ms`,
    `admission_dead_letters`, `admission_in_flight` (bounded integers in `shared/log.ts`), or
    `outcome: scan_failed`.
  - Never a key, body, token or header value (tested).

## Object-store permissions

The exact per-prefix contract (operations, the startup probe and its cleanup, S3 and GCS notes) is in
`docs/selfhost-composition.md` section 5.

## Env

| Variable | Values | Default |
| --- | --- | --- |
| `SELFHOST_INGEST_ADMISSION` | `sync`, `async`, `drain`; anything else refuses startup | `sync` |
| `SELFHOST_ADMISSION_CONCURRENCY` | 1..32 | 4 |
| `SELFHOST_ADMISSION_SCAN_MS` | 1000..600000 | 15000 |
| `SELFHOST_ADMISSION_MAX_ATTEMPTS` | 1..100 | 20 |

The knobs are read only in `async` and `drain`. No new secret, role, grant, migration or bucket
setting is needed. The Locker credential must be able to put, get and list under `<keyRoot>/`,
including the dot-prefixed `.admission-inbox`, `.admission-dlq` and `.admission-probe` roots, which
are disjoint from every tenant prefix.

## Known limits (follow-ups)

- **Scan cost grows with history.** Tombstones and outcome markers stay, as hosted, so each scan lists
  every past upload's two small objects. At 15 s that is fine for a kind or small self-host. A large
  one wants a lifecycle rule that expires only small objects under `.admission-inbox/` (never pending
  content, which is always at least 256 bytes), or a consumer that deletes the tombstone after the
  outcome.
- **Attempt counts are per process.** A restart starts an object again from attempt 1, and N replicas
  each spend their own budget before the shared marker stops them all.
- **Desktop.** No change. It treats the 200 as delivered. Polling the status route for refusals is the
  same follow-up as hosted.
