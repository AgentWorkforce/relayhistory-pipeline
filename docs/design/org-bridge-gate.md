# Org bridge gate: the S3 writer fence through a committed bridge_complete

Status: milestone 1 (code only, nothing wired to a workflow mode). Branch `review/s3-admission-fence-bridge`.
Owner: sessions-s3-admission-fence. Review: layered-sessions-pipeline. Qualification belongs to sessions-hold-test-harness.

## The defect

The synthetic cutover finding `s3_admits_before_bridge_complete` showed the problem. The S3 consumer
(`ledger-s3.ts` `requireFenceAuthority`) admits an org as soon as its per-org fence-authority marker exists. That
happens before the drain, before the erasure replay and before the 062320 hold's `bridge_complete`. The SQL hold does
not bind the S3 consumer, so the direct writer and the fallback writer could both land for one org.

## Writers of one cut-over org O (hold epoch E)

| Class | Writer | Fenced by |
|---|---|---|
| direct | S3 consumer: ledger-s3 `claim`, then `commit` (direct-to-S3 inbox uploads) | this gate: the bridge-complete record |
| fallback | Worker HTTP admission into SQL | the 062320 hold (install to release), then `workerHttpAdmission: "off"` (config, see open item B) |
| fallback | buffered drain (`buffered-drain.ts`, the backlog into the canonical layout) | this gate (refuses once the record exists) plus the drain's run lease |

## Invariant (milestone 1)

- No direct claim for O admits through O's per-org marker unless O's bridge-complete record is present and bound
  to that marker's exact bytes.
- The record exists only after slot 062320's status shows epoch E released with `bridge_complete`, and only once the
  marker has landed.
- Once the record exists, the drain writes no new canonical byte for O and merges no index entry for O.

The record is the create-only object `<keyRoot>/.ledger/fence-authority/.bridge/<org>.json` (`OrgBridgeDoc`). It
holds the org, sha256 of the hold epoch (domain-separated), sha256 of the marker's stored bytes, the release
evidence and `writtenAtMs`. It sits under a dot directory, so no tenant segment can collide with it.

## Pieces

- `ledger-s3.ts`: `orgBridge: "required"`. The hosted Lambda (`upload-admission-hosted.ts`) always sets it, and the
  self-host/ingest-edge server sets it with `INGEST_ORG_BRIDGE=required`.
  - A missing record answers `ledger_fence_authority_bridge_pending`.
  - An invalid record, one for another org or one bound to another marker answers `ledger_fence_authority_bridge_invalid`.
  - Nothing positive is cached until both reads pass. A refusal is never cached.
- `org-bridge-gate.ts` `completeOrgBridge`, in order:
  1. Reads the committed release.
  2. Reads the marker's bytes.
  3. Adopts an identical record, and treats any other record as a conflict.
  4. Takes the drain's own run lease (refusing beside a live drain or export).
  5. Makes a create-only PUT, then reads it back.
  - An ambiguous PUT is decided by the read-back.
  - A crash before the PUT leaves O fenced, and a rerun completes it.
- `buffered-drain.ts` `assertBridgeOpen`: an uncached HEAD of the record.
  - Called before a batch's first canonical byte and again before its index merge.
  - Any object at the key closes the drain: `bridge_complete_drain_refused`, or `bridge_state_unreadable` on a store error.
  - The group-level authority read also reports it.

## Proven (focused suites; the regression tests fail on main)

- `src/session-ingest/__tests__/org-bridge-gate.test.ts`:
  - the marker alone refuses;
  - bound, invalid, other-org and other-marker records;
  - a warm consumer held across cache windows;
  - the deployment-wide marker does not bypass a per-org marker;
  - self-host default unchanged;
  - marker then release;
  - release before the marker lands (late marker PUT);
  - crash between the release and the record;
  - ambiguous PUT;
  - refusals for every non-`bridge_complete` hold;
  - conflict never overwritten;
  - live drain or export lease, and a stalled drain losing its lease to the bridge;
  - store errors.
- `buffered-drain.test.ts` (appended block):
  - a record present before the run;
  - the switch landing after the cached authority read and before the first canonical byte;
  - a drain paused across the switch after its canonical bytes landed (no index merge, not marked, a rerun refuses);
  - control: no record, so the batch drains.
- `upload-admission-s3-ledger.test.ts`: the hosted Lambda retries at the marker and admits after the record.

## Open (milestone 2, from the lander's review of the ACK)

A. **Enrollment before the marker.**
   - An org enrolled in a hosted cutover must refuse even before its marker exists: with a valid deployment-wide
     marker, and with a warm positive cache.
   - The global STOP stays effective, and an unrelated org serves as the control.
   - This needs a trusted enrollment source that is effective by install. A claim acquired before install whose
     commit resumes while the org is held must not complete.
B. **SQL fallback after release.**
   - After `bridge_complete`, slot 062320's `_check` lets admission through again. Only the
     `INGEST_HTTP_ADMISSION_STOPPED_ORGS` configuration keeps the Worker's HTTP path closed.
   - Needed: an inventory of the entry points that must close after `bridge_complete`, retries included, and of
     those that must keep working (seed, recovery), with `bridge_complete` kept distinct from `verified_rollback`.
   - The gap between the SQL release and the S3 projection must be modelled explicitly.
   - No migration or slot is reserved until that review.
C. **Storage-enforced exclusion for paused writers.**
   - A pre-PUT check and an expiring lease still let a paused writer finish after a takeover.
   - The proposal pins every gated PUT's SigV4 `x-amz-date` to the time of the writer's last fresh switch check
     (`packages/object-store/src/sigv4.ts` already takes `now`). S3 and MinIO refuse a request signed more than
     15 minutes from server time, so every admitted effect lands within check + 15 min + skew.
   - The switch writer then waits out that bound before the switch counts as effective.
   - Proof: put-gate (MinIO) holds a PUT past the bound and shows it refused, with lease takeover, a delayed index
     merge, and a crash after the release before projection.
