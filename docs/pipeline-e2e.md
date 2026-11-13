# The full-pipeline e2e

One command drives synthetic sessions through every piece of the sessions
pipeline on a fresh, isolated stack and asserts each piece on its own line:

```sh
bun run e2e:pipeline                                  # every step, a fresh compose project on this machine
bun run e2e:pipeline --steps upload.courier,index.hoop,summary.extractive
op run -- bun run e2e:pipeline --target sandbox       # the same assertions in a throwaway Blaxel microVM
flows/node_modules/.bin/flows run --local-agent flows/pipeline-e2e.flow.ts --input '{}'   # the same, as a flow
```

It needs Docker and CI's bun (1.4.2). The CI flow runs it as its `e2e` lane
whenever the classifier says `full` (every step) or `targeted` with steps
(those steps); see [maintainer-workflow.md](maintainer-workflow.md).

## Deterministic

- **Fixed fixtures** (`e2e/pipeline/fixtures.ts`): constant session ids,
  origins, record revisions, timestamps and text. Isolation between runs comes
  from a fresh compose project with fresh volumes, never from random ids.
- **No model calls.** Bob Costas runs the extractive summariser
  (`BOB_COSTAS_MODEL=off`); the session-brief worker is not started.
- **No network beyond the stack** once the images are built: the only stub is
  `control-stub`, Control's token introspection endpoint (e2e/selfhost).
- **Bounded waits**: every poll has a deadline (at most 240 s, for the
  consumer's one-minute reconcile); the lane has a 45-minute ceiling in the CI
  flow, the sandbox run 40 minutes.
- **Per-run credentials** are generated in memory and passed in the compose
  environment; only the Postgres CA and server certificate touch disk, in the
  run's `.96bulls/e2e-*` directory, removed on exit.

## The stack

`e2e/selfhost/compose.yaml` (the self-host acceptance stack: Postgres with TLS,
MinIO, migrations, the operator role step, `session-ingest`, the
`session-pipeline-consumer` and `session-read-api`) with
`e2e/pipeline/compose.yaml` layered over it:

- `session-ingest-s3`: the same ingest with `INGEST_LEDGER=s3` (decision 0005);
- `alleyoop-ingest`, `hoopdb-indexer`, `referee`, `bob-costas` on their own
  bucket: the courier pipeline of the local stack (docs/local-stack.md);
- MinIO and AlleyOop on ephemeral loopback ports so the driver can read
  storage back. Postgres is read with `psql` inside its container.

Only the services the selected steps need are started.

## The steps

| Step | Proves |
| --- | --- |
| `upload.http` | a. upload over HTTP on the SQL ledger: no token 401, read-only token 403, then admitted (ingest log: `accepted`, one manifest written) |
| `upload.s3_ledger` | a. upload over the S3 ledger: refused 503 `ledger_fence_authority_missing` until the test fence-authority marker is written, then admitted; the S3 batch record is `committed` with the receipt; no SQL `ingest_batches` row |
| `canonical.objects` | b. both sessions' canonical `manifest.json` and every chunk it names are in the object store, sealed (the marker is not readable in the stored bytes) |
| `index.directory` | c. the consumer indexes both: one `sessions.session_directory` row each and their `search_index.transcript_events` rows |
| `search` | d. search returns each session to its tenant, a near-miss query returns nothing, and another tenant gets nothing |
| `read.api` | f. the read API: anonymous list 401; the list carries the session; `/v1/sessions/:id/events` serves its events |
| `read.summary` | f. `/v1/sessions/:id/summary`: anonymous 401; 200 `{status: empty}` for a session with no brief; an unsupported kind 501. Brief content is model-only (session-brief), so the extractive summary content is proven by `summary.extractive` |
| `controls.sql` | g. SQL ledger: an equal-revision re-send is stale (nothing written); a disable (HTTP selection), then a re-upload is not admitted and the manifest revision does not move; the session is unlisted. Self-host serves no reset route (501), so reset is proven on the S3 ledger |
| `controls.s3` | g. S3 ledger: equal revision stale; disable, re-upload not admitted; reset (and a replayed reset answers its first count), re-upload of a reset session not admitted, a new session admitted. Self-host under `INGEST_LEDGER=s3` routes no selection or reset, so the controls go through `ledger-s3.ts` against the same bucket and key root; the HTTP upload path proves their effect |
| `rearm.sweeper` | h. the re-arm sweeper (#590): a session admitted over HTTP; a batch claimed before a disable and committed after it, through the real ledger, writes the re-arm marker (`rearmed: 1`); `sweepRearmMarkers` with the S3 eraser (as `rearm-sweeper-main.ts`) erases that session's every canonical object version, leaves the org's other sessions, and a second pass answers `alreadyDone` |
| `backlog.drain` | i. **skipped, pending #578** (the S3-only buffered backlog drain). Same rule for `src/session-ingest/buffered-drain.ts` |
| `upload.courier` | a. the real courier (`runCourierOnce`) over a Concourse `history.db` built from a fixture, with a token minted from the run's key: chunks and a validated manifest through AlleyOop into the Locker |
| `index.hoop` | c. the indexer writes `session_index` and `session_index_ext` (a minted UUIDv7), and the referee classifies it |
| `summary.extractive` | e. Bob Costas writes both summary pointers (`ready`) and the encrypted `summary/{short,long}.json.enc` objects; each decrypts with the run's master key to a ready summary built by `extractive/v1`, and its plain text is not in the stored bytes |

## The report

`report.md` and `report.json` under `$RELAYFLOW_STATE_DIR/pipeline-e2e/`, or
`.relayflow/pipeline-e2e/` in the checkout: a PASS, FAIL or SKIP line per step
with what it observed, the wall clock and the stack start-up time. The run
exits 1 on any FAIL. On a FAIL the tail of every service's log is printed.
`E2E_KEEP=1` leaves a local stack up; `E2E_SKIP_BUILD=1` reuses a built
`96bulls/services:e2e` image.

## The sandbox target

`--target sandbox` (`e2e/pipeline/sandbox.ts`) snapshots the working tree
(tracked and staged edits; nothing untracked), creates a Blaxel sandbox from
`blaxel/docker-in-sandbox` (16 GB, with Docker's data on a 60 GB disk-backed
ephemeral volume), uploads the archive, runs
`e2e/pipeline/sandbox-run.sh` there (CI's bun, the locked install, the same
`run.ts --target local` on the sandbox's own Docker), copies the report back
and deletes the sandbox, whatever happened. `BL_API_KEY` and `BL_WORKSPACE`
come from the environment only.

Status (2026-11-12): **not yet proven end to end.** Probed by hand in this
workspace: the image has Docker 27 and Compose 2.31 on Alpine x86_64, CI's bun
installs, and the e2e starts; two things needed changing and are handled
(build containers there have no DNS, so the image builds with
`--network host`; Docker runs on `vfs` over the in-memory root filesystem, and
the services image build filled a 32 GB sandbox, hence the disk-backed volume
for `/var/lib/docker`). A full run through `sandbox.ts` needs `BL_API_KEY` and
`BL_WORKSPACE`, and the volume's effect on the storage driver is unverified.
Until then the CI flow's default target is `local`.

## Adding a step

Add it to `STEPS` in `e2e/pipeline/steps.ts` with a stable id, a leg (which
half of the stack it needs) and assertions that fail with what was observed.
If only some paths reach it, add the paths to `TARGETED_RULES` in
`flows/lib/classify.mjs`; `flows/lib/classify.test.mjs` checks every step id
the classifier names exists.
