# Moving the S3 upload ledger to its own bucket

`INGEST_LEDGER=s3` keeps the upload ledger (decision 0005) in the Locker bucket by
default. When that bucket is in another region than the ingest edge, every ledger
request crosses regions, and a batch makes about a dozen of them one after
another. `INGEST_LEDGER_BUCKET` (with `INGEST_LEDGER_REGION`) puts the ledger's
own objects in a bucket next to the edge. Unset, nothing changes.

## What moves

`src/session-ingest/ledger-store.ts` decides by key:

| Moves to the ledger bucket | Stays in the Locker bucket |
|---|---|
| `<root>/.admission-inbox/<org>/v1/ws=*/m=*/ledger/**`: batch records, revision shards, ownership stubs, re-arm markers and their completion records, log-commit streams | everything else: the fences (`upload-fences.json`), privacy journals, the admission and upload inboxes, outcomes, dead letters, the fence-authority markers, the sweeper cursor, the canonical layout |
| `<root>/.ledger/fence-authority/.ledger-home.json`: the ledger-home marker | |

The fences stay because the hosted Worker's S3 fence mirror
(`INGEST_S3_FENCE_MIRROR`) writes them in the Locker bucket. A fence moved away
from it would hide a deletion or reset from the ledger. Moving the fences needs
every fence writer moved first; it is not part of this change.

The edge and the re-arm sweeper see one store (`ledgerRoutedStore`): each key
goes to the bucket that owns it, and a listing that spans both is merged in key
order. Each side lists only the keys it owns, so a stale copy of a ledger key
left in the Locker bucket is never seen.

## The fork guard

The ledger-home marker, `{"v":1,"home":"<bucket>","writtenAtMs":<ms>}`, names
the bucket that holds the ledger. A claim checks it next to the fence-authority
marker, and the result is cached with it for 60 s per org:

- An edge with `INGEST_LEDGER_BUCKET` refuses every claim unless the marker in
  that bucket names it (`ledger_home_missing`, `ledger_home_moved`).
- An edge without it reads nothing by default, so flag-off behaviour is
  byte-identical to before. With `INGEST_LEDGER_HOME_GUARD=on` it reads the
  marker in the Locker bucket and refuses if the marker names another bucket
  (`ledger_home_moved`). Once a relocation has written "moved" into the
  Locker bucket, an edge left on the old configuration (an old values file, a
  second release) cannot admit against the stale copy. The runbook therefore
  turns the guard on before step 1.
- The hosted upload-admission Lambda (`upload-admission-hosted.ts`) is not
  covered in this slice:
  - Its code honours `INGEST_LEDGER_HOME_GUARD=on`, but its stack environment
    (`stacks/aws/upload-inbox.ts` `uploadAdmissionFunctionEnv`) is fixed and
    cannot set it. Setting it by hand would be drift.
  - It refuses to start with `INGEST_LEDGER_BUCKET` set.
  - So it is disabled for the window and stays disabled (steps 1 and 6).
- The sweeper with `INGEST_REARM_LEDGER_BUCKET` checks the marker in the ledger
  bucket on every pass, and a missing or wrong marker exits 2. Without it, and
  with `INGEST_LEDGER_HOME_GUARD=on`, it checks the Locker bucket's marker and
  refuses on "moved". Its role then needs GetObject on the marker key; a 403
  exits 2 (`access_denied`).
- The fence backfill (`scripts/upload-fence-backfill.ts`) and the buffered
  drain (`scripts/buffered-drain.ts`) are ledger-bucket aware. They read
  `INGEST_LEDGER_BUCKET`, `INGEST_LEDGER_REGION` and optionally
  `INGEST_LEDGER_ENDPOINT`, parsed and validated exactly as the edge does
  (`src/session-ingest/ledger-tool-store.ts`; a bad value refuses with
  `ledger_bucket_invalid` before any request).
  - Set, they use the edge's `ledgerRoutedStore`. The fences, staged batches,
    drained markers, layout and lease stay in `--bucket` (SSE-KMS on an
    execute). The ownership stubs, revision shards and re-arm markers go to
    the ledger bucket with no encryption header, so its default applies. Both
    stores use the same AWS credentials.
  - Set, they first check both markers, whatever the guard says. The Locker
    bucket's own marker (the "moved" one step 5 writes) must exist and name
    exactly that ledger bucket, else `ledger_home_mismatch`: a setting that
    points at another environment's or a stale ledger refuses before any
    write, even if that bucket's own marker names itself. Then the ledger
    bucket's marker must name it: `ledger_home_missing` or
    `ledger_home_moved`, as the edge does.
  - During a run they re-check, as the edge does about once a minute: before
    any ledger-key write made a minute or more after the last good check, the
    same check runs again. A refusal blocks every later ledger-key write; the
    tool stops at its next boundary (the drain at its next batch group, after
    releasing its lease, a Locker write the check never blocks) and exits 1
    with the code.
  - Unset, they behave as the other single-bucket tools below.
  - The backfill checks the markers unless every section it runs is
    `fences` (an empty section list is checked too). A fences-only run needs
    no ledger bucket and reads no marker; the fences never move.
  - A dry run writes neither bucket: the library refuses every put and delete
    through the routed store.
  - The backlog-ops workflow (`.github/workflows/backlog-ops.yml`) always runs
    the backfill and the drain with `INGEST_LEDGER_HOME_GUARD=on`. The
    credential step adds it to the tool's environment in code, so the
    workflow file cannot drop it (or add it: the step's own environment never
    reaches the tool). It
    passes the ledger bucket to the backfill and the drain from the protected
    environment variables `BACKLOG_OPS_LEDGER_BUCKET` and
    `BACKLOG_OPS_LEDGER_REGION` (docs/backlog-ops-workflow.md). Without them,
    a run after the switch refuses with `ledger_home_moved` instead of writing
    the Locker bucket's stale copy.
  - The accepted-stage export, which the workflow also runs, reads the fences
    and writes stage sidecars and completion records. None of these is a
    ledger key, so it is unaffected. It is given the guard variable too, but
    never reads a marker, so the variable does nothing for it.
- The other single-bucket operator tools (`ledger-reshard-revisions.ts`,
  `ledger-log-stream.ts`) are not ledger-bucket aware, and no workflow runs
  them. They, like the two tools above without a ledger bucket, read their
  bucket's marker at start only with `INGEST_LEDGER_HOME_GUARD=on` in their
  shell. Off, they make no request.
  - The codes are exact:
    - `ledger_home_moved`: a valid marker names another bucket.
    - `ledger_home_missing`: a ledger bucket is configured and its marker is
      absent (the ledger-aware tools only).
    - `ledger_home_mismatch`: a ledger bucket is configured and the Locker
      bucket's marker is absent or names another bucket (the ledger-aware
      tools only).
    - `ledger_home_invalid`: the marker body is bad.
    - `access_denied`: a 403.
    - `ledger_home_unavailable`: a transient failure (5xx, 429, 408, or a
      network, connection or timeout error). Run the tool again.
    - `ledger_home_store_error`: any other store error, such as a
      wrong-region 400, a 301, an invalid key or an unsupported operation.
      Not retryable as-is: fix the bucket, region or endpoint.
    - `ledger_home_guard_invalid`: the flag is neither `on` nor `off`. The
      tool refuses before making any request.

## Encryption

The ledger holds hashes, ids, revisions and claim tokens, never transcript
content. Each S3 GET of an SSE-KMS object is a KMS call, and the benchmark
measured that cost per request (S3 Bucket Keys did not help). Use SSE-S3
(AES256) as the ledger bucket's default encryption. The edge sends no encryption
header, so the bucket default applies.

## Relocation (offline, a whole key root at once)

The tool is `src/session-ingest/ledger-relocate.ts`. It is in the services image,
and reads the edge's own environment. Run it as a Job with the edge's
ServiceAccount, so no credential is ever on disk. It prints counts and codes only.

0. **Before the window.** Turn on `INGEST_LEDGER_HOME_GUARD=on`:
   - Deploy it to the ingest edge (chart `ingestEdge.ledgerHomeGuard: true`).
     That value and `ingestEdge.ledgerBucket` come from the infra chart change
     on branch `feat/ingest-ledger-bucket` (sessions-pipeline chart 0.8.44,
     agent-relay 0.7.41). They are not on any released chart yet; until that
     lands, the edge cannot be configured for steps 0 and 6 through the chart.
   - Optionally deploy it to the sweeper, if its role can read the marker.
   - Export it in every shell that runs the operator tools above.

   Confirm the edge still admits; with no marker present this changes nothing.
   The hosted upload-admission Lambda is not covered (see above), so steps 1
   and 6 keep it disabled.
1. **Stop admission.**
   - Write the deployment-wide stop marker
     `<root>/.ledger/fence-authority/.stop.json`
     (`{"v":1,"stop":"all","writtenAtMs":<now>}`).
   - Scale the edge to 0. The stop alone does not end a commit that was
     already claimed.
   - Disable the hosted upload-admission Lambda if it is deployed (reserved
     concurrency 0).
   - Stop the re-arm sweeper (suspend its CronJob or scale its Deployment to 0).
     The hosted re-arm sweeper Lambda (`rearm-sweeper-lambda.ts`) is not
     supported here; disable it.
     Disable it too, and leave it disabled.
   - Do not run any other single-bucket ledger tool against this key root:
     `scripts/buffered-drain.ts`, `upload-fence-backfill.ts`,
     `ledger-reshard-revisions.ts` or `ledger-log-stream.ts`. After step 5
     they refuse on their own (`ledger_home_moved`), but only in a shell or
     Job with `INGEST_LEDGER_HOME_GUARD=on`. Without the flag they read no
     marker and would write the stale copies. The check stays flag-gated, so
     the flag remains required in every tool shell and Job after the move,
     not just during the window. The backlog-ops workflow always sets it.
     After step 6, run the backfill and the drain with the ledger bucket
     (`INGEST_LEDGER_BUCKET`, or the workflow's `BACKLOG_OPS_LEDGER_*`
     variables).
   - Desktop uploads keep landing in the upload inbox and wait there.
2. **`plan`.** This is read-only. Check that the source counts match what you
   expect, that the destination is empty, and that the stop is present.
3. **`copy --confirm`.** Run it at least `--min-stop-age-ms` (default 5 min) after
   the stop. It copies byte for byte and create-only, and a rerun copies nothing
   new. Any `diverged` fails the run.
4. **`verify`.** The destination must hold exactly the source's keys with
   identical SHA-256. Record the `digest`. This readback is the copy's
   integrity check, because `copy` sends no `contentSha256`. Never switch
   without a passing verify; `switch` runs one itself.
5. **Switch the home.** Write "moved" into the source, then the destination's marker:
   - With one principal that may write both buckets, run `switch --confirm`. It
     verifies again, then writes both markers in that order.
   - Otherwise, the operator first writes
     `<root>/.ledger/fence-authority/.ledger-home.json` in the Locker bucket as
     `{"v":1,"home":"<ledger bucket>","writtenAtMs":<now>}`. The Job then runs
     `switch --confirm --side destination`, which refuses unless the source
     already names the destination.
6. **Reconfigure and resume.**
   - Deploy the edge with `INGEST_LEDGER_BUCKET` / `INGEST_LEDGER_REGION`
     (chart `ingestEdge.ledgerBucket.{bucket,region}`, from the same
     unreleased infra chart change as step 0), and the sweeper with
     `INGEST_REARM_LEDGER_BUCKET` / `_REGION` (the same chart value renders
     them). Startup proves
     conditional writes on the ledger bucket.
   - Leave the hosted upload-admission Lambda disabled: it refuses
     `INGEST_LEDGER_BUCKET` and cannot carry the guard.
   - Leave the hosted re-arm sweeper Lambda disabled too; it is unsupported
     here. Use the EKS sweeper with `INGEST_REARM_LEDGER_BUCKET`.
   - Scale the edge back up, and restart the sweeper with its new setting.
   - Delete the stop marker. Claims resume within 60 s.
7. **Watch.**
   - Inbox backlog falls.
   - No `ledger_home_*` or `ledger_store_*` codes appear.
   - The sweeper's passes complete. A position saved before the switch restarts
     its scope once.

Before step 6, roll back by deleting both home markers. The Locker bucket was
never written, apart from its marker. After resuming on the new bucket, roll back
by running the same steps with `--direction back`. Run `copy` with
`--replace-stale`, because the Locker bucket still holds the pre-move copies.
Then run `switch --direction back`, and deploy without the settings. If the
Job cannot write the Locker bucket, use `switch --direction back --side
source`, then write the Locker bucket's marker by hand, naming the Locker
bucket.

The pre-move copies in the Locker bucket are inert: the routed store never
lists or reads them, and the moved marker refuses any edge that would. Delete
them only after a stable period, as a separate, reviewed step.
