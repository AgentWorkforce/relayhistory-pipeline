# Buffered first-ACK ordering seats (040530)

This migration preserves buffered record order at the first durable receipt ACK.
It does not authorize admission or canonical publication. Existing staged ACKs
cannot be assigned their lost order retroactively; installation refuses them.

The original ingest creator must own the three producer functions and five
producer tables checked by the migration. Mixed creators are refused before DDL.
The four new functions are private INVOKER capabilities with owner-only EXECUTE.
The four seat tables have RLS and no runtime, seeder, or public table grants.

An owner initializes the transactional clock with exactly:
```json
{
  "protocol": "delivery-acceptance-floor-v1",
  "controlEpoch": "<owner-bound lower-case hex64>",
  "capturedAt": "<UTC timestamp with six fractional digits>",
  "sourceSequences": [
    {"sequenceRegclass":"sessions.delivery_catalog_acceptance_seq","sequenceOid":"<decimal>","allocatedHighWater":"<decimal>","isCalled":true,"incrementBy":"1","cycle":false},
    {"sequenceRegclass":"sessions.delivery_projection_acceptance_seq","sequenceOid":"<decimal>","allocatedHighWater":"<decimal>","isCalled":true,"incrementBy":"1","cycle":false}
  ],
  "allocatedHighWater": "<MAX of both allocated frontiers>",
  "generation": "<compiled current PASS generation>",
  "sourceIdentitySha256": "<verified source identity>",
  "sourceProofSha256": "<hash of complete captured descriptor including sequence floor>"
}
```

The source capture must run after the reviewed allocator cutoff and reread both
actual allocator OIDs and allocated values. Neither a live row maximum nor the
last emitted nextval proves the allocated frontier when sequences cache values.
The initializer does not invent or independently attest that operator proof.
It requires the retained admission epoch and current PASS generation. An
identical baseline is idempotent across schema upgrades; a changed source,
epoch, proof, or floor is refused. No subsequent call resets the counter.

The receipt transaction serializes a short transactional clock row lock and
records the original claim/ordinal maps before COMMIT. A rollback loses the clock
advance and maps together. Lost replies replay without reseating. No S3 I/O
occurs while the clock is held. GC replacements retain original claim/ordinals.

The private `ingest_receipt_seats_for_stage(uuid)` returns:
```text
{
 controlEpoch, sourceIdentitySha256, allocatedHighWater, lastAssignedSeat,
 records: [{ord, seat, candidateDigest, firstDigest, bodyAccepted, resetEpoch, fenceEpoch}]
}
```
All counters are decimal strings; ord is a bounded integer. The first three
fields are immutable baseline identity. `lastAssignedSeat` is the current
committed upper bound, not an immutable generation or a heartbeat equality fence.
Missing maps, incomplete original epochs, unknown stages, and invalid identity
refuse rather than synthesize zero or a timestamp-derived order.

For NEW buffered history, a per-record/reset custody head captures a first-body
decision across source/session aliases at ACK time. Higher revisions replace
custody; a privacy-erased current winner permits a fresh lower revision. A losing
candidate points to the accepted winner's seat and immutable digest. It never
creates a prospective losing seat. A new accepted lifecycle has a unique seat;
old ordinal decisions remain unchanged after later erasure or a new ACK.

**Unresolved old-history authority gate:** this new-only custody head does not
know pre-existing legacy record custody before authoritative warm proof. Its
`bodyAccepted` decision must not be treated as full old+new parity. For example,
legacy rev8/B can make a buffered rev1/A stale; after erasing B, a fresh rev1/C may
be legal even though a new-only head incorrectly retained A. Initial admission
and canonical materialization remain HELD until selected-org current custody is
proven/initialized, or a reviewed ordered candidate/control reconciliation
settles that unknown. All original buffered candidates remain stored; there is
no permission to discard them based on this provisional decision.

Run the real proof only with owned disposable PG17 and versioned MinIO resources.
`scripts/proof-receipt-seats.ts` requires explicit owned fixture paths and
`RECEIPT_SEAT_DISPOSABLE=1`; no production credentials are used. The fixture
captures both real cached allocator frontiers through the immutable source
exporter, runs the full migrator and all four phase2 verifiers, and exercises the
actual producer, codec, SQL ports, and object store. A clean source head is
required unless an explicit work-in-progress proof is requested.
