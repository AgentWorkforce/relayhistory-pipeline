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
- The hosted upload-admission Lambda (`upload-admission-hosted.ts`) only ever
  runs on the Locker bucket. With `INGEST_LEDGER_HOME_GUARD=on` it does the
  same check, and it refuses to start with `INGEST_LEDGER_BUCKET` set.
- The sweeper with `INGEST_REARM_LEDGER_BUCKET` checks the marker in the ledger
  bucket on every pass, and a missing or wrong marker exits 2. Without it, and
  with `INGEST_LEDGER_HOME_GUARD=on`, it checks the Locker bucket's marker and
  refuses on "moved". Its role then needs GetObject on the marker key; a 403
  exits 2 (`access_denied`).
- The single-bucket operator tools (`scripts/buffered-drain.ts`,
  `upload-fence-backfill.ts`, `ledger-reshard-revisions.ts`,
  `ledger-log-stream.ts`) read their bucket's marker at start. They refuse
  when it names another bucket or is unreadable.

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

0. **Before the window.** Deploy `INGEST_LEDGER_HOME_GUARD=on` to every writer
   of this key root's ledger that runs on the Locker bucket:
   - the ingest edge;
   - the hosted upload-admission Lambda, if deployed;
   - optionally the sweeper, if its role can read the marker.

   Confirm the writers still admit; with no marker present this changes
   nothing.
1. **Stop admission.**
   - Write the deployment-wide stop marker
     `<root>/.ledger/fence-authority/.stop.json`
     (`{"v":1,"stop":"all","writtenAtMs":<now>}`).
   - Scale the edge to 0, and disable or drain the hosted upload-admission
     Lambda (reserved concurrency 0) if it is deployed. The stop alone does not
     end a commit that was already claimed.
   - Stop the re-arm sweeper (suspend its CronJob or scale its Deployment to 0).
   - Do not run any other single-bucket ledger tool against this key root:
     `scripts/buffered-drain.ts`, `upload-fence-backfill.ts`,
     `ledger-reshard-revisions.ts` or `ledger-log-stream.ts`. After step 5
     they refuse on their own (`ledger_home_moved`).
   - Desktop uploads keep landing in the upload inbox and wait there.
2. **`plan`.** This is read-only. Check that the source counts match what you
   expect, that the destination is empty, and that the stop is present.
3. **`copy --confirm`.** Run it at least `--min-stop-age-ms` (default 5 min) after
   the stop. It copies byte for byte and create-only, and a rerun copies nothing
   new. Any `diverged` fails the run.
4. **`verify`.** The destination must hold exactly the source's keys with
   identical SHA-256. Record the `digest`.
5. **Switch the home.** Write "moved" into the source, then the destination's marker:
   - With one principal that may write both buckets, run `switch --confirm`. It
     verifies again, then writes both markers in that order.
   - Otherwise, the operator first writes
     `<root>/.ledger/fence-authority/.ledger-home.json` in the Locker bucket as
     `{"v":1,"home":"<ledger bucket>","writtenAtMs":<now>}`. The Job then runs
     `switch --confirm --side destination`, which refuses unless the source
     already names the destination.
6. **Reconfigure and resume.**
   - Deploy the edge with `INGEST_LEDGER_BUCKET` / `INGEST_LEDGER_REGION`, and
     the sweeper with `INGEST_REARM_LEDGER_BUCKET` / `_REGION`. Startup proves
     conditional writes on the ledger bucket.
   - Leave the hosted upload-admission Lambda disabled. It has no ledger
     bucket, and with the guard on it refuses (`ledger_home_moved`).
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
Then run `switch --direction back`, and deploy without the settings. If the Job cannot write the Locker bucket, use `switch --direction back --side source`, then write the Locker bucket's marker by hand, naming the Locker bucket.

The pre-move copies in the Locker bucket are inert: the routed store never
lists or reads them, and the moved marker refuses any edge that would. Delete
them only after a stable period, as a separate, reviewed step.
