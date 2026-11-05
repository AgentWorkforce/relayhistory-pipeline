# Buffered upload acceptance (dormant)

This child adds durable acceptance before canonical publication. It does not make
new buffered content visible to reads/search. Default acceptance remains canonical.
No schema, role, flag, queue or storage change is performed by this source change.

## Internal mode and acknowledgement

Only an operator-selected Worker binding INGEST_ACCEPTANCE_MODE=buffered selects
the separate ingest_claim_buffered_batch RPC. Caller JSON cannot select the mode.
Buffered settings require INGEST_INLINE_MAX_SESSIONS=0 and replica mode off.
The Worker queue handler refuses publication while buffered even if delivery is
accidentally resumed. Buffered keys, headers and SQL attempts also refuse ordinary
canonical fanout/commit after a later mode change.

Claim records ownership and immutable session epochs plus a separate reset count.
Potential in-batch conflicts with unknown historical new/stale status return
retryable 503 baseline_pending, rather than a premature 409 or a published receipt.
No historical record is suppressed merely because a partial revision index says
it is stale. Current deletion fences still suppress deleted records.

1. Prepare the scrubbed stage and its compressed SHA-256/byte count, with original
   claim ordinal mapping held only in SQL metadata.
2. Register the stage intent BEFORE S3 PUT. Intent binds tenant, owner, batch,
   immutable nonce, group ordinals, original epochs/reset and 120-second expiry.
3. PUT create-only under buffered-batches/v1. An existing object is accepted only
   if its exact registered bytes/hash match; a redaction barrier refuses.
4. Commit receipt and durable SQL outbox pointer atomically after rechecking
   original epochs, reset, intent lifetime and cleanup state under short locks.
   There is no queue send or TTL dependency in this acknowledgement.

Payload stays scrubbed in S3. Canonical manifests/pointers, stored record revisions,
the Neon replica and fanout messages are not produced by this path. A replay
requires a durable acknowledged outbox or an explicitly erased outbox; wholly
suppressed receipts contain no stage. Batch conflicts/idempotency are enforced by
the new ledger. Historical Neon batch digests are not imported by this child.

## Privacy and retention prerequisites

The owner-only ingest_buffer_admission row is an expiring per-org attestation
binding hashes of a verified privacy-fence baseline and a reviewed retention
contract. Empty/default/expired policy refuses a new claim before objects exist.
It is not a historical revision baseline and does not require a global content seed.
Continuous deletion/reset propagation and GC must be verified before attesting it.

The dedicated prefix is outside the old Neon eraser's unsafe whole-live-stage
48-hour cleanup. This is NOT an erasure implementation. Admission requires the
separate 040510 durable GC SQL + GC runtime/operator to be composed and verified.
Every pre-PUT intent is retained, including failed or ambiguous attempts. ACKed
mixed stages must preserve valid groups via tracked replacement intent and CAS,
then install a persistent old-key redaction barrier and permanently erase older
versions. A tombstone must survive late IfNoneMatch PUTs; deleting a key alone is
unsafe. No unacknowledged or acknowledged stage is garbage merely because a
Cloudflare Queue message expired.

UNPAUSE is not authorized by this source. Before canonical drain, a separate
reviewed protocol must refilter original records against complete source-proven
revision maxima, close historical seeding at canonical admission, and implement
storage generation barriers/publication CAS with eraser/projector qualification.
The metadata-only 199 epoch repair does not prevent late canonical S3 publication.

## Synthetic validation

SESSION_INGEST_TEST_PG_URL must name an owned disposable fixture. The opt-in
buffered.postgres.test.ts uses actual PostgreSQL17 and filesystem objects, a signed
HTTP handler, exact event-trigger final ACL, intent-before-PUT observation, atomic
outbox/receipt replay, reset/disable-enable/expiry/retirement races and redaction.
BUFFER_MUTATION=no_receipt_fence removes only the final epoch/reset rejection:
two privacy tests fail. BUFFER_MUTATION=transaction_time substitutes transaction
timestamps and must fail the blocked-lock expiry control. Default Worker settings
and accidental-unpause refusal also have offline controls.
