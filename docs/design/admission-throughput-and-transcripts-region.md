# Admission throughput, and whether to move the transcripts bucket next to the edge

Status: proposal (2026-11-17). Public design note: no production identifiers. The
private operator detail (bucket names, accounts, key ids, live numbers) goes to the
private ops repository.

## The question

The ingest edge runs on EKS in us-west-2. The upload ledger moved to a us-west-2
bucket this morning ([ledger-relocation.md](../ledger-relocation.md)). Canonical
transcript writes still go to the transcripts bucket in us-east-1, which is in the
sessions account and encrypted with SSE-KMS under that account's key. At about
45–60 admissions a minute per pod, with the "objects" stage at a 2.3–5.4 s median
and about 5.3 s per batch in total, the backlog drains slowly.

The aim is **admission throughput**, i.e. draining the backlog. The region is only one
possible lever. This note asks which lever actually moves throughput before anyone
pays for a storage move.

## What the code does per batch

`deliver.ts` runs the stages in order: claim (ledger) → admission gate → objects →
replica → commit (ledger). The "objects" stage is `writer.ts` `writeBatchObjects` →
`writeGroups`. For each (source, session) group, `writeGroup` runs these steps
**one after another**:

| # | Request | Notes |
|---|---|---|
| 1 | GET the live-session pointer | plus a create-only PUT the first time a session is seen |
| 2 | GET the session manifest | |
| 3 | scrub + zstd + sha256 of the chunk (**CPU, main thread**) | `sealRawChunk`; TS engine or the wasm-bindgen engine, both synchronous |
| 4 | PUT the sealed chunk (create-only, SSE-KMS) | a conflicting chunk costs a GET as well |
| 5 | sha256 over the chunk list (**CPU**) | `computeContentSha256` |
| 6 | conditional PUT of the manifest (`If-Match` / `If-None-Match`) | a precondition failure retries from 2 |

Up to `INGEST_CONCURRENCY = 6` groups run at once. The network critical path of
the stage is therefore about `ceil(groups / 6) × 4` sequential S3 requests, or 5 for
a new session.

The stage timings (`stats.timing_ms`, `mark()`) are **wall clock**. They include
any time a batch spends waiting for the event loop while other batches scrub.

## Network share vs CPU share (estimate)

| Quantity | Cross-region today | Same region |
|---|---|---|
| Round trip, us-west-2 ↔ us-east-1 | ~60–70 ms | ~1 ms |
| One small S3 request, warm connection, SSE-KMS | ~90–150 ms | ~15–40 ms |
| One group round (4 sequential requests) | ~0.4–0.6 s | ~0.1–0.15 s |
| Large chunk bodies (MB) | several extra round trips while TCP ramps up | negligible |

Expected saving from moving the bucket: **~0.3–0.5 s per round of 6 groups**. Most
batches have 1–6 groups, so that is about **6–10 % of a 5.3 s batch**. The stage only
reaches 2.3–5.4 s on network alone if a batch carries about 30 or more sessions.
Nothing measured so far says it does.

CPU, as measured in production (infra-environments): at concurrency 10 the edge
uses about 1.2 cores at about 1 admission a second, roughly **1.2 CPU-seconds per
batch**. Scrub, compression and hashing all run on the Node main thread; nothing in
`src/` uses `worker_threads`. One event loop then caps throughput at
`1 / ~1.2 s ≈ 0.8 batches/s ≈ 50/min`, which is what we observe (45–60/min).
By Little's law, `in-flight = λ × W = 0.8/s × 5.3 s ≈ 4.4`. That is well under the
concurrency limit of 10, so the pod is not starved for concurrency. It is waiting
for its CPU.

**Working conclusion:** the edge is CPU-bound. Moving the bucket would shorten each
batch by a few hundred milliseconds but would barely raise admissions per minute
while the event loop stays saturated. The estimate rests on one CPU reading, so
step M0 below confirms or refutes it before anything is built for the move.

## Plan, in order

| Step | What | Kind | Needs Khaliq's own OK |
|---|---|---|---|
| M0 | **Measure.** Per-request S3 latency by operation and bucket; event-loop utilisation (ELU) sampled every 10 s; per-batch group count, source bytes and CPU-heavy stage time, in the admission log line. Default off (`INGEST_ADMISSION_PROFILE=on`), counts and durations only. | code, then a config deploy | the deploy that turns it on |
| T1 | **Scale out.** 2–3 edge replicas, or a CPU limit that fits the load. Claims are already safe under concurrency (ledger CAS, per-session manifest CAS, slots). Expected near-linear gain while the ledger and S3 are not the limit. | infra values (infra-environments) | yes (deploy) |
| T2 | **Seal off the main thread.** A small `worker_threads` pool, sized to the pod's CPUs, for scrub + zstd + sha256, behind `INGEST_SEAL_WORKERS` (default 0 = today's path). Output bytes must be identical, which a test proves across both engines; the S3 PUTs stay on the main thread. | code (review branch → lander) | the deploy that enables it |
| R0 | Re-measure with M0. Decide the region move from data: go only if, at the new ceiling, S3 wait is more than half of a batch's wall time and ELU is below ~0.6. | analysis | — |
| R1+ | The region move (below), only if R0 says so. | code + infra + data move | every step |

T1 and T2 multiply. A move is worth its risk only once CPU is no longer the
bottleneck.

## If the move is still wanted: inventory

The canonical layout under `<root>/<org>/...` holds live-session pointers,
session manifests, raw chunks, summary objects and the derived views. The same
bucket also holds the upload inbox, admission inbox, fences, fence-authority
markers, privacy journals, outcomes, dead letters, buffered stages and drain state.
Its writers and readers:

| Component | Runtime | Access | Prefixes | Region selected by |
|---|---|---|---|---|
| ingest edge | EKS us-west-2 | read, write, list; delete on dead letters | admission inbox objects, outcomes, privacy journals, fences, fence-authority markers, dead letters, upload inbox (read, tombstone), canonical layout (`v1/ws=…/manifest.json`, `raw/*`, `live-sessions/v1/…`) | `LOCKER_BUCKET` / `LOCKER_REGION` |
| ingest edge, ledger keys | same | read, write, delete | `ledger/**`, ledger-home marker | `INGEST_LEDGER_*`: **already in us-west-2** |
| rearm sweeper | EKS us-west-2 | read, list, delete versions | ledger re-arm and owner keys, fences, manifests; version deletes under `v1/ws=*` and `live-sessions/` | `INGEST_REARM_*` (+ `_LEDGER_*`) |
| summary and status workers | EKS us-west-2 | read canonical, write `summary/*` | canonical layout, `summary/*` | `LOCKER_*` (cross-account worker policy) |
| session-pipeline (indexing) | Lambda us-east-1 | list, read; analysis writes when enabled | canonical layout, `normalized/session-analysis-v2/*` | Lambda region; **triggered by S3 event notifications on `manifest.json`** |
| session read API | Lambda us-east-1 | read (analysis results) | `normalized/session-analysis-v2/*` | `READ_API_ANALYSIS_BUCKET` |
| read-api pod, search indexer, native consumer, operator erase | EKS (chart) | read; erase deletes versions | canonical layout | `LOCKER_*`, `SESSION_ERASE_S3_*` (off in prod today) |
| hosted ingest Worker (+ S3 fence mirror) | Cloudflare | read, write, list | srv-writer canonical layout, live sessions and batches, upload inbox, fences | `INGEST_S3_*` |
| hosted read Worker | Cloudflare | read | raw and canonical objects, `summary/*`; `normalized/session-analysis-v2/*` with `READ_API_ANALYSIS_ENABLED=true` | `PIPELINE_RAW_S3_*` |
| relayhistory-cloud writer, inbox bridge | Cloudflare | read, write | `raw/*`, manifests, `summary/*`; the bridge writes the upload inbox | that repo's config |
| history indexer (reader user) | external | list, read | canonical layout | n/a |
| eraser, buffered GC | purge job; Lambda us-east-1 (dormant) | delete versions | everything; buffered stages | template parameter, fixed region |
| upload-admission consumer | Lambda us-east-1 (**not deployed in prod**) | read, write, delete inbox | upload inbox, srv-writer objects | stack |
| desktop upload role, Cloud presign issuer | desktop / Cloud | create-only PUT; read journals | upload inbox, privacy journals | Cloud `HISTORY_UPLOAD_*` (presigned URLs are region-bound) |
| backlog-ops roles (backfill, export, drain) | GitHub OIDC jobs | read, write (the drain writes the canonical layout) | everything under the key root | job env (allowlisted) |
| publication runtime, canary | EC2 us-east-1 | read, write | `upload-generations/` | fixed to us-east-1 |

Bucket features that matter for a move:

- **Versioning** is on, and noncurrent versions expire after one day.
- **Encryption** is SSE-KMS under the bucket's own regional key, with **S3 Bucket Keys off**: each object has its own data key, bound to its object ARN. Every key-policy statement is conditioned on `kms:ViaService = s3.<region>`.
- The **bucket policy** denies:
  - non-TLS requests;
  - PUTs that name another key;
  - deletes by anyone but the eraser, the GC and the sweeper;
  - access across the upload-inbox wall.
- **S3 event notifications** (`manifest.json` created or removed, and optionally upload-inbox objects) go to SQS queues in us-east-1. A notification can only target its own region. They are applied by a script, not by the stack.
- **Region assumptions** are hard-coded or defaulted in many places: the stack's transcript region and its refusal of any other, `LOCKER_REGION` defaults, the publication runtime, the buffered-GC template, the notification script, and migration-tool defaults. **The key layout itself is region-agnostic.**
- **IAM drift:** the ingest edge's grant on this bucket and on its key is not in this repository's bucket or key policy, and is not in infra code. The live grant must be found and brought into code (the identity side in infra, the resource side here) **before** any move, or the next policy deploy could drop it. This is worth fixing regardless of the move.

An earlier measurement ([decision 0008](../decisions/0008-hosted-ledger-backend.md)) puts one S3 round trip at about 66 ms cross-region and about 5 ms in-region. That agrees with the per-request estimate above.

## Options

### (a) New us-west-2 transcripts bucket with a routed store, background copy, cutover (recommended *if* R0 says move)

The ledger routing (`ledger-store.ts`, `ledgerRoutedStore`) is the precedent, but this
case is much harder:

- **Readers.** The ledger had one reader family; transcripts have about ten, in
  three runtimes (Lambda in us-east-1, Cloudflare Workers, EKS). Every reader needs
  the routed store, or must read the new home, before any canonical key is written
  there.
- **Size.** Listing about 700k ledger keys cross-region took about 38 minutes per
  pass, and that window paused admission for 1 h 47 min. The canonical layout is
  orders of magnitude larger, so an offline copy under an admission stop is not
  viable.
- **Shape.** The move has to be **online and per session**. Keep a "home" in the
  live-session pointer. New sessions are born in the new bucket. An existing session
  moves when its pointer, manifest and chunks are copied under the manifest CAS
  (copy, then CAS the pointer's home; a writer that loses the race re-reads).
  Readers resolve the home through the pointer, falling back to the old bucket. A
  background job walks the rest.
- **Indexing.** The manifest notification must be recreated in us-west-2: a queue
  there that feeds the indexing Lambda across regions, or EventBridge. Otherwise
  indexing silently stops for sessions homed in the new bucket.
- **Keys.** Only the canonical layout moves. Inboxes, fences and markers stay
  until every one of their writers (hosted Worker fence mirror, cloud presigned
  PUTs, the inbox bridge, backlog-ops) has moved. This is the same reasoning that
  kept fences in place for the ledger.
- **Encryption.** A new SSE-KMS key in us-west-2, with S3 Bucket Keys enabled. Its
  key policy grants Encrypt/Decrypt to every reader and writer role across both
  accounts. Erasure must delete in both buckets until the old copy is gone.
- **Rollback.** The per-session home flips back: copy back under CAS. Old copies
  stay inert until a reviewed delete.
- **Pause.** None for the data. A short edge restart at each config change, and a
  backlog-ops freeze during the switch of each writer family.
- **Cost.** Storage moves rather than doubles once the old copy is deleted. Until
  then, double storage. One-off inter-region transfer at about $0.02/GB plus PUT
  requests at $0.005 per 1k for the copy. The ongoing inter-region charge on every
  edge PUT body (about $0.02/GB) goes away. Readers in us-east-1 start paying it on
  their reads instead, until they move too.

### (b) S3 replication

Cross-region replication copies from a source bucket and does not change where
writes go. With us-east-1 as the source, admission gains nothing. Reversing it
(us-west-2 source, us-east-1 replica) is option (c).

### (c) Write in us-west-2, replicate to us-east-1, keep reads in us-east-1

The edge writes chunks and manifests in the new bucket, and replication (with RTC,
15-minute SLA) feeds the old bucket, which the readers keep using. It is rejected
for these reasons:

- The edge's manifest CAS reads its own writes, so it must use the new bucket. But
  the drain, the rearm sweeper (version deletes) and erasure still write or delete
  canonical keys in the old bucket. That is two authorities for one manifest.
- Version-specific deletes do not replicate, so erasure has to be done twice by
  hand.
- Readers would see replication lag on fresh sessions.

## What needs Khaliq's go

1. Turning M0 on (a config deploy).
2. Scaling the edge, or changing its CPU limit (T1).
3. Enabling the seal pool (T2).
4. For the move, separately:
   - the new bucket and KMS key;
   - the cross-account bucket and key policies;
   - each reader's deployment onto the routed store;
   - starting the background copy;
   - each per-family switch;
   - deleting the old copies.

Not while backlog-ops holds `main` or a drain or export is running against the
bucket.
