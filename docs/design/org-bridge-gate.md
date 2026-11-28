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

## Invariant (milestones 1 and 2A)

- An org O is **enrolled** at generation G by a CAS-versioned S3 object, `.ledger/fence-authority/.enrolled/<org>.json`.
  It is bound to sha256 of the hold install's request id and written by `enrollOrg` BEFORE the SQL install. The
  install tool calls `enrollmentFor(org, request)` right before `_install`.
- While O is enrolled, no direct claim admits and no direct commit reaches a re-arm marker, an index merge or
  pending -> committed, unless both of these hold:
  - O's per-org marker is present;
  - the create-only bridge-complete record `.ledger/fence-authority/.bridge/<org>/g<G>.json` binds O, G, the
    enrolled request and the marker's exact bytes.
- The deployment-wide marker and a warm positive cache do not count. Enrollment and the bridge are read uncached on
  every claim and commit. The STOP still refuses new claims first, and an org that is not enrolled is unchanged.
- The record exists only after slot 062320's status shows the hold of G's request released with `bridge_complete`,
  and only once the marker has landed.
- Once the record exists, the drain writes no new canonical byte for O and merges no index entry for O.

Both objects sit under a dot directory of `fence-authority/`, so no tenant segment can collide with them, and the
consumer's read grant already covers them. Records carry hashes only.

**Trusted enrollment source.** Operators write the `fence-authority/` prefix and the consumers only read it, the
same trust as the marker. Enrollment is effective for new claims from its PUT, because every claim reads it
uncached. It does NOT retire a writer that was admitted or in flight before the PUT. A claim that read "not
enrolled" and then paused can still write canonical bytes. Its commit then refuses (see the commit-resume test), but
those bytes are not retracted. That is item C, so **A alone does not qualify install**.

**Recovery** (`withdrawEnrollment`, CAS to `withdrawn`; the markers decide again):
- *Enrollment succeeded, SQL install failed.* Allowed only while the status shows no hold of that request. The next
  attempt enrolls G+1 with a NEW request id. If the old install commits late, it creates a hold no enrollment covers:
  `completeOrgBridge` refuses it (`bridge_request_mismatch`), and its only exit is `verified_rollback`. Marker
  publication must also require `enrollmentFor` (sessions-wave-tooling).
- *Verified rollback.* Allowed only when the latest hold is that request's, released with `verified_rollback`, which
  means no marker was ever attempted. The order is: SQL release first, then withdraw. In between, SQL is open and S3
  is fenced, so there is a single writer.
- *Install before enrollment.* `enrollOrg` refuses (`enrollment_after_install`). Recovery is `verified_rollback` and
  a new request.
- *A later cutover of O.* It takes G+1, allowed from `withdrawn` or once G's record exists. G's bytes sit under G's
  key and bind G's request, so they never satisfy G+1 (tested). An org enrolled at G+1 is fenced again at once.

## Pieces

- `ledger-s3.ts` `orgBridge: "required"`. The hosted Lambda (`upload-admission-hosted.ts`) always sets it, and the
  self-host/ingest-edge server sets it with `INGEST_ORG_BRIDGE=required`. `enrolledGate` runs before the positive
  cache on every claim, and at commit before any effect and again before pending -> committed. While the marker or
  the record is missing the answer is `ledger_fence_authority_bridge_pending`. When something does not bind it is
  `ledger_fence_authority_bridge_invalid` or `ledger_fence_authority_enrollment_invalid`. Under the STOP, a claim
  answers `ledger_fence_authority_stopped`.
- `org-bridge-gate.ts`:
  - `enrollOrg`, `enrollmentFor` and `withdrawEnrollment`.
  - `completeOrgBridge`: checks the enrollment, the committed release of its request, and the marker's bytes. It then
    adopts an identical record or treats any other as a conflict, takes the drain's own run lease, and makes a
    create-only PUT with a read-back.
- `buffered-drain.ts` `assertBridgeOpen`: uncached. It reads the enrollment and HEADs G's record before a batch's
  first canonical byte and again before its index merge. The outcomes are `bridge_complete_drain_refused` and
  `bridge_state_unreadable`.

## Proven (focused suites; regression tests fail with the gate disabled)

- `org-bridge-gate.test.ts` (19 tests; 9 fail with `enrolledGate` disabled):
  - an enrolled org with no marker refuses with a valid global marker and a warm cache, while the unrelated org stays
    open;
  - the STOP keeps precedence;
  - records and enrollments that do not bind;
  - a claim acquired before install, committed while held: stays pending with no index entry, then commits after
    the record;
  - self-host unchanged; withdrawn or unenrolled unchanged;
  - enrollment idempotence, request binding, install-before-enrollment, install-failed withdrawal plus a late old
    install, the verified-rollback withdrawal, a later generation against old bytes;
  - both commit orders, a crash between R and P, an ambiguous PUT, the hold refusals, a conflict, drain and export
    leases, store errors.
- `buffered-drain.test.ts` (appended block):
  - a record before the run;
  - the switch after the cached authority read;
  - a drain paused across the switch after its bytes landed (no merge, not marked);
  - an unreadable enrollment;
  - an old generation's record leaving G+1 open;
  - control.
- `upload-admission-s3-ledger.test.ts`: the hosted Lambda, enrolled, retries at the marker and admits after the record.

**Readable bytes today (input to C).** A direct claim admitted before enrollment, or a drain batch paused across P,
can leave canonical chunk bytes and manifest entries that no index entry covers. The tests pin that the index merge,
the committed state and the drained marker are withheld. The bytes themselves are not retracted, and whether the
reader serves them unindexed is exactly what C must settle.

## B. SQL fallback after bridge_complete: caller inventory (design only, no slot)

Today slot 062320's `_check` returns as soon as O has no live hold, so after `bridge_complete` every class reopens.
The Worker's HTTP path stays closed only through `INGEST_HTTP_ADMISSION_STOPPED_ORGS`, which is configuration, not
enforcement. The table below covers the runtime-granted functions (`src/hoopdb/org-admission-hold-classes.ts`, 85),
for an org whose hold of the enrolled request released with `bridge_complete`:

| 062320 class | Functions | After bridge_complete | Why |
|---|---|---|---|
| admission | claim_batch (via claim_batch_mode), claim_buffered_batch, commit_batch, commit_buffered_batch, commit_revisions, fanout_fence, prepare_buffer_stage, the three origin_* wrappers, seed_proven_records | **close** | SQL is no longer the ledger for O. An admission here is content the S3 index and owner stubs never see, which is the dual-writer case. Retries count too: a Worker request that got RH042 while held, and retries after the release, must keep failing. commit_revisions takes a NULL batch id (no pending batch required), so closing the claims alone is not enough. The seeder imports revisions, and S3 seeding (seed-s3.ts) replaces it for O. |
| publication | publication_claim, publication_publish, history_plan_claim | **close** | They publish O's leftover SQL stages into canonical. The drain has already moved that backlog, so this is a second fallback writer beside the drain. |
| control | set_session_uploads, reset_uploaded_sessions, turn_register, selfhost_set_session_uploads | **keep, mirror-only** | Removal state must stay consistent: erasers still read SQL `upload_deletions`, and the mirror writes S3 first. Keep the sealed-state rule (only members of `bulls96_session_ingest_mirror`) permanently for O, rather than reopening to any caller. |
| erasure | operator_runtime_begin, operator_user_erase_begin | **keep** | Never refused. |
| unaffected | GC (buffer_gc_*), consent, privacy and source control, history, publication bookkeeping, seed_fence_baseline, seed_scope_* | **keep** | They write no admission content. GC redaction and erasure runtime steps must keep working. |

**bridge_complete vs verified_rollback.** After `verified_rollback`, everything reopens as today: the org is back on
SQL, and its enrollment is withdrawn after the release. After `bridge_complete`, admission and publication stay
closed for O for good. There is no rollback after a marker attempt (design 062320 section 6.1). A newer hold of O
does not lift it: the rule keys on "any hold of O ever released with `bridge_complete`". Cutting an org back from S3
is unmodelled and needs its own design.

**The release-to-projection gap, modelled.** Let R be the commit of `_release(bridge_complete)` and P the create-only
PUT of G's record.

| Interval | SQL writers of O | S3 direct writer of O | Drain |
|---|---|---|---|
| install to R | refused (hold) | refused (enrolled, no record) | runs (the bridge) |
| R to P, today | **open** (062320 reopens; only config closes HTTP) | refused | refused only once P exists; still open in this interval |
| R to P, with the proposed rule in R's transaction | refused (permanently) | refused | completeOrgBridge holds the drain lease |
| after P, today | **open**, so dual writer with S3 | open | refused |
| after P, with the rule | refused | open | refused |

With the rule, **the first point where exclusion and safe recovery both hold is R itself**. R closes SQL admission
and publication atomically with the release, and S3 stays fenced until P. A crash anywhere between R and P therefore
leaves zero writers for O (availability only), and the recovery is to rerun `completeOrgBridge`, which is idempotent.
No SQL action is needed. Without the rule there is no such point: from R on, SQL can admit beside the soon-to-open
S3 consumer.

**Placement (open, the lander's call).** Two options:
- a new slot that replaces `_check` with the "ever released `bridge_complete`" refusal, as a new non-retryable code
  so the Worker can answer "use direct upload";
- the same rule folded into 062320's successor body.

Either way the refusal must be read under the org key in `_check`'s second statement, as the hold is today. Then a
release (EXCLUSIVE) serializes with every in-flight admission, and no admission that read "no hold" can commit after R.

## C. Exclusion at the authoritative write (held by the lander)

The SigV4 time-window proposal is withdrawn. A fresh signature bounds only when a request may start, not when its
body or effect lands, and presigned-URL expiry gives no commit-time guarantee for a header-signed PUT.

What C needs is a protocol that does one of two things:
- atomically excludes stale publication at the authoritative write;
- durably registers admitted writers and refuses a takeover or switch while any effect is unresolved, including after
  a crash or a lost ACK, and reconciles ambiguous in-flight effects. It must state which bytes are readable.

Not designed yet. The lander has not approved the 17-minute extension, and it is out of the plan.
