# Background canonical drain protocol (work in progress)

This source lane remains disabled/draft. Buffered ingress remains permanent; it
does not unpause the old canonical fanout or direct writer.

## Authority

A per-(org, workspace, user, origin) owner-only frontier attestation binds the
complete source-proven record maximum set and the Neon-to-S3 export frontier.
Absence of a revision row is meaningful only under this complete attestation.
Privacy admission remains independent. No partial seed is treated as complete.

040600 claims the current ACKed outbox stage under USER -> BATCH -> INTENT locks.
It owns the same gc_lease_token/until/version as040510 with gc_keep_groups NULL:
GC cannot mutate/delete a stage while this lease is valid. It stores an immutable
publisher attempt nonce, original record ordinals/fences/reset count, exact
record-max snapshot and exact accepted set. It changes batch publication_state
to canonical BEFORE any canonical image/chunk I/O, closing historical seeding.
Ordinary fanout/commit still refuse the buffered receipt lineage.

Each physical session has a permanent head and a randomly named upload generation.
Upload generation is independent of raw.epoch, which remains the scrub epoch.
Images are immutable manifest snapshots:

    root/org/upload-generations/g=UUID/v1/ws=.../m=.../h=.../d=.../s=.../publications/UUID/manifest.json

Raw chunks remain at the normal rawChunkKey under the same generation prefix.
The shared parser/prefix rules understand only this narrow qualified shape.
Existing canonical fold and chunk readers resolve the same raw namespace.
The first managed takeover copies verified existing carrier chunks once; later
publications reuse same-generation chunks. It never rebases an old attempt onto
a newer generation. Other legacy carriers remain part of the canonical union.

Every key/hash/byte intent is registered before PUT. An expired or fenced writer
may finish private bytes, but cannot publish them through the database. Only a
short final CAS binds the immutable image into the canonical pointer after
rechecking lease nonce/version/deadline, outbox token/version, original fences,
complete accepted set, all record maxima, previous heads and object registration.
It nonblockingly takes the same canonical directory-events scope locks as the
CH projector BEFORE user/head locks. A busy scope retries, holding no user lock
across remote reads or writes. Thus an old CH pass cannot run after a newer
generation has become canonical.

## Consumers and erasure

The pointer indexer treats S3 events only as hints. A permanent managed head
rejects every noncurrent key, including old legacy keys, after takeover.
Missing current manifests are unavailable, not an invitation to recreate the
namespace or adopt another image. Raw pointer SQL repeats this fence; delayed
readers cannot bypass it with a higher manifest revision.

Derived consumers hydrate/model outside locks, then make a short transaction
with expected current manifest key/revision authority for their final writes.
Unqualified Referee state writes are refused for managed scopes. Managed raw
manifests are immutable: derived output goes to publication-qualified registered
objects/SQL, never a CAS mutation of the source manifest.

Disable/reset retires the generation and canonical pointer authority in the
metadata fence transaction. Retired authority persists after objects disappear.
A fresh upload uses a new namespace. Erasure consumes registered retired keys,
installs a persistent redaction barrier at each key, and removes prior versions;
a late IfNoneMatch PUT cannot recreate it. Cleanup never targets a live/fresh
generation. Orphan chunks within a still-live generation are not individually
deleted, because a later valid snapshot may reuse their content-addressed keys.
All keys are cleaned when that generation is retired.

Required proofs include actual PostgreSQL races/clock stalls, conditional-store
late PUTs, missing-manifest ABA, exact all-record refilter, mixed export+delivery
canonical fold, stale projector/model writes and fresh-generation retention.
No source-level readiness is an activation claim. Operator frontier capture,
consumer configuration, least-privilege validation and complete E2E remain gates.

