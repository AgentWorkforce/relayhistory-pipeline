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

## Checkpoint limits: do not enable this foundation alone

This ordinary publisher checkpoint is a dormant source foundation. It is not a
full-service or activation signoff. In particular, the receipt acceptance-seat
append040530 and its Node materialization integration are required before any
buffer admission or canonical enablement. Existing ordinary fixtures have one
origin; they do not establish cross-origin acceptance parity. The shared fold
requires authoritative accepted_seq for native records competing across origins.
Those seats must come from first durable ACK, above both captured allocated Neon
sequence frontiers, and never from a timestamp or drain order.

History records can legitimately move between sessions (their native identity
excludes session_id). This checkpoint holds cross-scope moves rather than leave
an old-session ghost. General History movement needs an independently reviewed
scope-reconciliation protocol across the canonical fold, artifact fingerprint,
model final authority, search projection and retirement journal. A hold is not
parity and is not permission to activate.

The standard Worker canonical snapshot still cannot fold a1.53GiB union. The
large source must compose PR184 ac4f11489f92e5a8bcc40e357041387338279eac
(canonical-artifact-reader, segmented v1 pages and large Node worker) with PR187
c8cf14c1271c68a9abfbbf888251acf75361db86 (derived publications). Neither the
ordinary publisher proof nor a successful buffer receipt proves large replay,
Bob/Referee, or four-pool search. The full managed consumer proof remains required.

The S3-only canonical feature producer must consume a durable publish AND
retirement/source-CAS journal (the separate040610 lane), not old Neon feature
updates or S3 hints. Plain standalone History q is explicitly deferred; accepted
hybrid callers retain the four legacy pools. No fifth pool is introduced here.

## Node entry and bounds

`publication-main.ts` is a host Node24 entry. `INGEST_PUBLISH_ENABLED` and
`INGEST_PUBLICATION_GC_ENABLED` are separate, exact true/false switches, both
false when absent. Disabled returns before config, credentials, DB or storage.
`node index.mjs --continuous` drains while work exists and polls after5seconds
idle. `node index.mjs --gc --continuous` independently erases retired generations.
Stopping publication must leave erasure armed and monitored.

`bun scripts/build-canonical-publication.ts /home/khaliqgant/relay-briefs/<artifact>`
requires a clean worktree, captures the compiled migration generation and all
four exact verifier SQL assets with the existing derivedSchemaBuildPlugin, and
writes a bundle/zip/hash manifest. It provisions nothing. The package is not a
15-minute Lambda; no Worker limits are changed.

The budget defaults20minutes (maximum1hour), one candidate per tick (maximum20),
30-second heartbeat and5-minute lease. Metadata uses the reviewed bounded PG
executor with dedicated role, owned cancel path and transaction-local deadlines.
A heartbeat cannot renew after any original stage/head/attempt deadline, even
when paused after capture. Refused candidates receive a30-second durable backoff;
their receipt and outbox remain intact. Privacy-invalid stages wait for buffer GC
and cannot starve another healthy upload.

Raw and manifest objects are each bounded16MiB. Image construction keeps one raw
body or historical copy at a time; it verifies historical stored bytes and SHA
before copying, strips the adapter-owned content-sha256 metadata, and registers
every destination object before PUT. Old derived server fields are not inherited
into the new raw snapshot. New derived keys use a fresh artifact UUID selected
BEFORE encryption/AAD; the exact resulting SHA/bytes are registered before PUT.
An encryption retry reuses identical prepared bytes or chooses a fresh key.

Runtime config uses INGEST_PUBLISH_BUCKET, INGEST_PUBLISH_EXPECT_GENERATION,
INGEST_PUBLISH_SECRET_ARN, INGEST_PUBLISH_KMS_KEY_ARN,
INGEST_PUBLISH_INSTANCE_PROFILE_ARN and INGEST_PUBLISH_ROLE_NAME, plus optional
INGEST_PUBLISH_BUDGET_MS, INGEST_PUBLISH_BATCH_LIMIT and
INGEST_PUBLICATION_GC_LIMIT. Values are operator inputs, never invocation payload.
The dedicated Supabase login/host/project and TLS CA checks reuse gcPoolConfig.
No owner login, static access key, SSO token or Neon connection is accepted.

Credentials are temporary IMDSv2 credentials on pinned host i-070f91abb67aa60de,
account941433935742/us-east-1, exact reviewed role/profile. Token PUT and every
GET have deadlines/body caps; default transport connects directly to the fixed
link-local address, bypassing environment/global-fetch proxies, and has no
IMDSv1 fallback. Identity is reverified at least every5minutes and credentials
refresh before expiry. No metadata request is made during build or default-off
boot. The host's hop-limit1 is unchanged; use host Node or a reviewed host-network
container. AWS describes the protocol in
[Retrieve security credentials from instance metadata](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-metadata-security-credentials.html).

## Rollout and proof boundaries

Apply/verify the exact composed schema before deploying consumers: both the
hoopdb indexer and session-pipeline Lambda carry compiled-generation final-write
authority. The Lambda embeds the verifier assets through its existing build
path. Legacy-key events losing first takeover cannot write ext/directory/fanout.
Quiesce/replace older unfenced model binaries before canonical enablement; this
checkpoint does not claim arbitrary direct SQL from an old binary is fenced.

No-session _none/_origin remains an archival/catalog carrier: durable bytes and
revision authority, independent reset epoch and generation erasure, zero public
session rows and zero Bob/Referee jobs. Existing legitimate catalog/presence/
relationship projections remain subject to their original contracts, not a new
synthetic session interpretation.

Before any activation, independently review owner frontier/admission/floor
captures, role/ACL/current-PASS binding, scoped managed compute permissions,
source cutoff, four-pool convergence, actual model/reader callers, capacity,
throughput, cleanup backlog/age and rollback. Existing bucket policy denies
DeleteObjectVersion except its reviewed eraser principal; identity allow alone
will not authorize this new GC. The CMK needs an explicit reviewed role grant as
well. These source jobs do not apply bucket, IAM or KMS changes.

Owned synthetic proofs use isolated loopback PG17 and versioned MinIO, with real
Node24 and the packaged runtime. They cover ordinary publish; delayed raw and
missing-manifest PUT after disable/erase/enable; permanent markers/prior-version
erasure; fresh-generation retention; same-carrier historical copy; separate
export+delivery authoritative revision/tombstone fold; current-generation role
verification; no-session archival behavior and stale indexer/final-write gates.
They do not establish actual AWS ARM64 timing, KMS/IAM access, production
throughput, source-frontier truth, full large-reader/model visibility or an
activation decision.
