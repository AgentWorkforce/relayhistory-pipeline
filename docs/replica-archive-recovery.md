# Replica archive recovery into buffered ingest

The old Neon replica archive retains the exact `PreparedDelivery` values that
relayhistory accepted: original batch digest, content-addressed receipt,
prepared record digests and the scrubbed stored bodies. It does not retain the
public upload envelope fields needed to recompute those values. Reconstructing
a new public batch would therefore create different digests and receipts and is
not a valid recovery.

`POST /v1/delivery/recovery` is the private, buffered-only recovery boundary.
It accepts content type
`application/vnd.relay-ingest-recovery-v1+json` with:

```json
{
  "protocol": "relay-ingest-recovery-v1",
  "prepared": { "originId": "...", "batchId": "...", "batchDigest": "...", "receipt": {}, "records": [] },
  "custody": { "witness": {}, "sourceDigest": "..." }
}
```

The normal ingest HMAC covers the exact method, path, signed tenant identity and
body. The receiver additionally requires:

- an exact original receipt whose content-addressed ID recomputes for the
  authenticated tenant, origin, batch and archived batch digest;
- supported, bounded, already-scrubbed prepared records, with every retained
  revision represented by the original receipt;
- a fresh relayhistory current-source custody witness bound to the same tenant,
  origin, batch, digest, prepared ordinals, record identities, revisions and
  original record count; and
- `INGEST_ACCEPTANCE_MODE=buffered`. Canonical/default mode refuses the private
  wire before any claim or object write.

Supabase remains the final custody authority. Its existing lazy-current claim
recomputes the witness digest in PostgreSQL, validates the current source and
privacy/control version, and applies the same deletion, stale revision,
conflict and accepted-seat rules as a live buffered upload. Recovery then uses
the same claim, staged-object and atomic buffered receipt path as the public
batch route. There is no Neon fallback or replica send.

The prepared record list may be empty only when recovery has proven that no
retained archived body can be published (for example, every candidate is behind
a current deletion/reset fence). The original receipt still names the complete
accepted revision set, the witness carries that original count, and the ledger
must return the no-write replay-fast decision; an empty recovery can never enter
the buffered object writer.

The sender must rebuild `PreparedDelivery` directly from the archived replica
message and its retained object pointers, query fresh current custody, sign the
exact recovery body, and require the custody ACK. It must checkpoint success by
the archive object's immutable identity plus receipt, so retries are
idempotent. Listing an archive, sending recoveries, changing runtime bindings,
deploying this endpoint, or replaying a live queue are production actions and
are not authorized by this code component.
