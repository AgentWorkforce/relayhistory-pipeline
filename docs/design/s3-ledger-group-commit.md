# S3 ledger: grouped admission (group commit per uploader)

Status: design only, not implemented. Behind a default-off flag when built.
Branch: `review/claude-s3-ledger-group-commit`, base `bd058ecd`.

## Problem

Self-host ingest-edge runs `INGEST_LEDGER=s3` with async admission. One org whose backlog comes from one user
on about 29 machines admits about 30–45 batches a minute, while about 140 a minute arrive. The upload inbox
holds about 50k due objects, and the backlog grows.

The batches are not blocked by CAS conflicts. In 10 minutes there were 4 `manifest_contended`, and the fence is
only read on the admission path. The cost is **serial round trips per batch**, multiplied by a per-user cap.

- Each batch walks about 13 dependent S3 round trips (below). Several are SSE-KMS PUTs and GETs, so a batch
  takes about 3–5 s, and a stage 30 s or more under socket pressure.
- Raising concurrency adds in-flight requests (`INGEST_LEDGER_PARALLEL` 32 per claim or commit) rather than
  throughput. The result is status-less `ledger_store_transient` retries from fetch-level failures.

The goal is **group commit per uploader**: admit N queued batches of one uploader in one pass, so that the
per-uploader reads and per-object writes are shared and coalesced, and the serial depth is paid once per group
instead of once per batch.

## 1. What one admission does today

Path: `selfhost-admission.ts` `admitOne` (270) → `admission-consumer.ts` `admitMessage` → `deliver.ts`
`deliverPreparedCore` (363) / `writeAndCommit` (431) → `ledger-s3.ts` `claim` (1115) / `commit` (1189).

Counts below are S3 requests. A batch has R records, K distinct revision shards (about 1 per record, out of
4096 per origin), L live sessions, and S session groups written.

| # | Step | Code | Requests | Serial? |
|---|------|------|----------|---------|
| 1 | read inbox object | async-admission.ts:525 | 1 GET | yes |
| 2 | fence-authority marker | ledger-s3.ts:863 | 0 (60 s cache), else 2–3 GET | yes, on a miss |
| 3 | batch doc + fence | ledger-s3.ts:1126 | 2 GET (parallel) | yes |
| 4 | revision shards of live records | ledger-s3.ts:914 `readShards` | K GET (parallel ≤ `parallel`) | yes |
| 5 | ownership stubs | ledger-s3.ts:1150 `registerOwner` (776) | L × (GET + CAS PUT) | yes |
| 6 | fence again | ledger-s3.ts:1167 | 1 GET | yes |
| 7 | batch doc create/CAS (pending) | ledger-s3.ts:1182 | 1 PUT | yes |
| 8 | privacy-journal gate | async-admission.ts:440 | 1 GET (+1 CAS only to expire stale pending) | yes |
| 9 | consent gate | deliver.ts:404 | 0 S3 (policy) | — |
| 10 | per session: manifest GET, chunk PUT, manifest CAS | writer.ts:360-416 `writeGroup`, 443 `writeGroups` | 3S (≤ 6 sessions at once) | yes, within a session |
| 11 | commit: batch doc + fence | ledger-s3.ts:1201 | 2 GET | yes |
| 12 | re-arm fenced sessions (rare) | ledger-s3.ts:1225 | 0, or 2–3 per fenced session | yes |
| 13 | revision merges | ledger-s3.ts:1245 `mergeIndex` → `mergeIndexBatches` (725) | K × (GET + CAS PUT) | yes |
| 14 | batch doc CAS (committed) | ledger-s3.ts:1251 | 1 PUT | yes |
| 15 | outcome marker, inbox tombstone | admission-consumer.ts `finish`, async-admission.ts:464, 490 | 2 PUT | yes |

A batch costs about **10 + 3K + 2L + 3S** requests over **about 13 dependent round trips**. For a typical
20-record, 1-session batch that is about 75 requests.

Where it serialises:
- **Per session manifest:** step 10. The same session appended by many batches (a long session uploaded in
  many batches) is one CAS chain.
- **Per revision shard:** step 13, but only for the same origin and shard.
- **Per user, by policy:** the tenant limiter (`INGEST_MAX_PER_USER`). It caps in-flight batches, not objects.

The fence and the privacy journal are per-user, but admission only reads them.

## 2. Protocol

### Principle: a group is an execution unit, not a new ledger object

Every batch keeps today's individual state machine, with nothing added:
- its own batch doc (`ledger/batches/o=/b=`, unchanged `BatchDoc`; `validBatch` is strict, so it gets no new
  field);
- its own claim token;
- its own outcome marker, inbox tombstone and §5 status.

A group only shares reads and coalesces writes. So the layout does not change, nothing migrates, and the
status endpoint, replay and redrive work per batch as today.

### Group key and formation

- **Key:** the admission-inbox machine directory `…/<org>/v1/ws=<ws>/m=<liveMachineId(ws, origin, user)>/`.
  That is exactly one (org, workspace, user, origin), so one revision index (per origin), one fence, one
  privacy journal and one writer machine.
- **Formation:** when the pump starts an entry, it takes up to `G_MAX` other **already due** entries of the
  same directory, oldest first. There is no time wait, so it never adds latency.
- **Bounds:**
  - `G_MAX` defaults to 16, range 1..32. It must stay below `REMEMBERED_BATCHES` (64, writer.ts:69), the
    manifest's replay-detection window.
  - Total prepared bytes are bounded (default 8 MiB) and total records are bounded (default 2000).
  - The group holds **one** tenant slot, counted as G toward `INGEST_MAX_PER_USER` / `INGEST_ASYNC_MAX_PER_ORG`
    (a new `weight` on the limiter), so caps keep meaning "batches in flight".
- **Excluded from grouping**, taking today's single path: entries with `attempts > 1` after a group failure
  (split on failure, §3), `custodySource !== "relayhistory"`, and any entry whose inbox read fails.

### Sequence

New optional `Ledger` methods: `claimGroup(inputs[])` and `commitGroup(inputs[])`. Only the S3 ledger
implements them, and the SQL ledger never does, so the caller takes the single path. A group of 1 must be byte
for byte today's `claim` / `commit`.

**A. Read (parallel).** The G inbox objects, the G batch docs, and the fence (once).

**B. Classify each batch on its own.**
- `replay` and `batch_conflict` are decided per batch as today, and leave the group.
- A pending batch with the same digest is a `resume`.
- A pending fence reset fails the whole group as `ledger_control_pending`: retry, as today.

**C. Judge (one shard read).**
- Read the union of the K' shards that live records touch, once.
- Judge the batches in group order, each against stored entries **plus the new records of earlier batches in
  this group**. An earlier group batch's revision counts as stored, under the same #250 rules.
- So the outcome equals admitting the batches sequentially in that order. Today, two concurrent batches with the
  same record and revision but different digests can both be judged new; grouping makes that case
  deterministic.
- `revision_conflict` and `replay_fast` stay per batch. A `replay_fast` batch is committed by its own create/CAS
  in step E, as today.

**D. Register owners (once per session).**
- Register the union L' of live sessions once, all at the same fence generation, because there is one fence
  read.
- Re-read the fence once. If it moved in a way that touches any member (the same predicate as
  ledger-s3.ts:1170, evaluated per batch), only the touched batches re-run from A; untouched ones continue.

**E. Write the batch docs (parallel, distinct keys).** One create/CAS per batch, each carrying its own token,
the shared `fenceVersion`/`fenceInstance`/`resets`, and its own `epochs`. A lost CAS affects that batch only.

**F. Gates.**
- The privacy journal is read once, **after** E, then compared with each batch's own `journalSeq`. A batch that
  fails is refused terminally on its own (`delivery_privacy_superseded`, as today); `pending` means a group
  retry.
- The consent gate runs per batch. A hold is a retry of that batch only.
- No batch writes anything before its own claim, gate and consent pass.

**G. Canonical writes (coalesced per session).**
- For each session in the group: one manifest GET, one chunk PUT **per member batch** touching it (parallel,
  distinct keys, so every batch keeps its own chunk and replay identity), then **one** manifest CAS that appends
  those chunks in group order.
- That CAS also adds each batch's `[digest, seq]` to the remembered batches, so `rememberedBatches` still
  detects each batch's replay (writer.ts:363-372).
- Sessions are written in parallel, bounded by the process-wide in-flight budget (§5).

**H. Commit.**
- One fence read. Compute `fencedBy` (ledger-s3.ts:1209) per batch.
- Re-arm the union of fenced sessions **before** any merge, as today.
- One `mergeIndexBatches` per shard over every member's surviving entries, in group order. That function exists
  and is specified as equal to sequential merges (ledger-s3.ts:720-724).
- Then each batch's pending → committed CAS, in parallel. `ledger_attempt_superseded` applies to that batch
  only.

**I. Finish.** Each batch's outcome marker and inbox tombstone, in parallel.

### Idempotency keys

These are unchanged: batch doc key (origin, batchId), claim token, manifest remembered `[batchDigest, seq]`,
receipt id (content-addressed), outcome marker per receipt dir. There is no group id anywhere durable.

## 3. Failure semantics

- **Partial failure inside a group.** Each step is per batch, or keyed so that a failure names the batches it
  covers.
  - A session's write failure (foreign manifest, `manifest_contended`) fails only the batches touching that
    session. They stay `pending` and retry.
  - A shard merge failure fails the commit of the batches whose entries are in that shard.
  - Everything else proceeds.
- **Split on failure (poison isolation).** A batch that failed inside a group retries **alone**: an attempt
  count above 1 is never grouped. A poison batch can never refuse or wedge its siblings. Terminal answers
  (`batch_conflict`, `revision_conflict`, privacy superseded, 4xx DeliveryError) are already per batch.
- **Crash at each step.**

  | Crash point | State left | Next attempt |
  |---|---|---|
  | after A–D | owner stubs registered (harmless; a reset lists them), no batch doc | claims afresh |
  | after E | pending batch docs | `resume` per batch (new token), as today |
  | between chunk PUTs and the manifest CAS in G | unreferenced chunk objects | appends at the next seq. Same as a single batch crashing today; whether orphan chunks are collected is UNVERIFIED and does not change. |
  | after the manifest CAS, before H | batches written but pending | `remembered` makes each re-write a no-op, then commit |
  | inside H, after re-arm, before or within merges | markers stand; index may lag the manifest, never lead it | merges again (idempotent: max wins, equal keeps) |
  | after some per-batch commits | committed batches answer `replay` | the rest commit |
  | after commits, before I | — | the outcome is re-derived from the claim (replay) and written |

- **Lost responses.** An ambiguous manifest PUT keeps `ambiguousManifestPut` semantics per session. A lost
  batch-doc CAS is re-read per batch (ledger-s3.ts:1252).
- **Lease expiry.**
  - The group's slot has a TTL of 90 s. If a group outlives it, the cap loosens; correctness doesn't change,
    because every decision is a CAS.
  - The group bounds (G, bytes, records) target a p99 group time well under 30 s.
  - A group whose pass exceeds the budget stops forming new phases and finishes or abandons per batch (pending
    batches resume).
- **Concurrency with single-path admissions** (another replica, an old image, a retry) is the same set of CAS
  objects. It is exactly today's concurrent-batch case.

## 4. Privacy invariants

Each invariant holds per batch, with reads that are the same as, or later than, a single batch's.

1. **No write before the gates.** No canonical or index write for a batch happens before its own claim (E), its
   privacy-journal check (F) and its consent check (F). Steps G and H only take batches that passed F.
2. **Reset and selection landing mid-group.**
   - Owners are registered (D) before any canonical write, so a reset that starts after D lists them.
   - The fence is read three times, as today: B, D and H.
   - Each batch's `fencedBy` and `isDeletedForAdmission` run against those reads. A reset that lands between D
     and H fences the batch at H, which re-arms its sessions before the merges, exactly as for a single batch.
3. **Seq gate.** The single journal read happens after every member's claim, so it is never earlier than the
   read each batch would have made alone. Comparing against each batch's own `journalSeq` keeps the refusal per
   batch.
4. **Erasure and `upload_deletions`.**
   - Index entries are stamped with the claim's generation (epoch, resets), per batch, unchanged, so a later
     disable or reset makes them stale.
   - `entryForgotten` runs per entry inside `mergeIndexBatches`.
   - Deleted sessions are filtered per batch in C.
5. **Consent holds.** Per batch: a retry of that batch only, with its bytes kept in the inbox. A hold never
   becomes a group refusal.
6. **Frozen-receipt path.** The S3 ledger refuses custody (ledger-s3.ts:883) and grouping requires
   `custodySource === "relayhistory"`. The frozen-receipt and frozen-witness reads (admission-consumer.ts) never
   run inside a group: such entries take the single path.
7. **Controls mirrored to SQL.** `withSqlControlMirror` (ledger-s3.ts:1442) runs only on the control path.
   Grouping never writes the fence, the journal or SQL. A control interleaves with a group exactly as with N
   concurrent single batches, because every check above is the single-batch check.
8. **Ordering.** Within a group, judging (C), manifest appends (G) and index merges (H) all use the same order,
   the oldest `receivedAtMs` first. A group therefore equals the sequential admission of its members in that
   order. **That equivalence is the proof obligation for the tests in §6.**

## 5. Compatibility

- **Layout and migrations:** none. Objects are unchanged, and the manifest format is unchanged: several chunk
  entries and several remembered batches per CAS are already representable.
- **Mixed versions:** an old image's single-batch admissions and a new image's groups touch the same CAS
  objects. That is today's concurrent case, so a rolling restart and rollback are safe.
- **Flags** (self-host env; a hosted stack input later). `INGEST_ADMISSION_GROUP` is `off|on`, default `off`.
  `off` is byte for byte today's path.

  | Variable | Default | Range |
  |---|---|---|
  | `INGEST_ADMISSION_GROUP_MAX` | 16 | 1..32 |
  | `INGEST_ADMISSION_GROUP_MAX_BYTES` | 8 MiB | — |
  | `INGEST_ADMISSION_GROUP_MAX_RECORDS` | 2000 | — |

  Rollback is the flag off; there is no state to undo.
- **Self-host:** `selfhost-admission.ts` pump forms groups. It needs an admitGroup sibling of `admitMessage`
  that shares `finish`, `classify` and the per-batch outcomes.
- **Hosted Lambda:** SQS batches records from many uploaders. The handler groups records of one SQS batch by
  inbox directory and reports `batchItemFailures` per message, which maps one to one to per-batch outcomes.
  The ingest-edge path ships first; hosted only after.
- **Writer:** `writeGroup` gains a multi-batch variant (G chunks, one manifest CAS). The single-batch call
  keeps its signature.
- **In-flight budget:** a group multiplies parallel requests, so this depends on a **process-wide** S3
  in-flight cap. Today's `parallel` is per call (ledger-s3.ts:566 `mapLimit`), and the transient diagnosis
  points at socket over-subscription. Without that cap, grouping makes the transient problem worse.

## 6. Throughput model and proofs

**Model**, per group of G batches of one session-heavy uploader (K' shards ≤ ΣK, L' and S' distinct sessions):

| | Today, per batch | Grouped, per batch |
|---|---|---|
| Requests | 10 + 3K + 2L + 3S | (5G + 3K' + 2L' + 2S' + chunks) / G |
| Example: G = 16, 20 records, 1 session | about 75 | about 25 (from shard sharing and manifest coalescing) |
| Serial depth | about 13 round trips | about 14 round trips, but per group |

At the same per-user cap c the ceiling rises about G times: c=8 and G=16 is up to 128 batches per about 5 s.
The ceiling then moves to the process in-flight budget and per-session manifest size, so the number to prove
is batches admitted per minute at a fixed in-flight cap.

**Tests and proofs** (all must pass before the flag can be turned on anywhere):
1. **Sequential-equivalence property suite.** A random mix of batches (shared or distinct sessions and records,
   equal and lower revisions, conflicting digests) over an in-memory store with CAS semantics. Admitting
   members as a group must leave the same batch docs, manifests (chunks, remembered batches), index shards,
   owner stubs, re-arm markers and outcomes as admitting them one by one in group order.
2. **Fault injection at every await.** Crash, then resume:
   - no record lost or duplicated;
   - no write for a batch whose gate failed;
   - the index never leads the manifest;
   - every pending batch resumes or replays.
3. **Interleaved controls.** Selections, resets and journal bumps are injected at every step. No deleted
   session gets a canonical write after the fence that deletes it, unless it is re-armed. The seq gate refuses
   exactly the batches it would refuse alone.
4. **Poison isolation.** Cases: one batch with a foreign manifest, one with a revision conflict, one whose inbox
   object is corrupt. Siblings must commit, and the poison batch must retry alone.
5. **Mixed versions.** Old single-batch and new group admissions run concurrently on one store, and the
   suite-1 oracle must still hold.
6. **Lanes.**
   - Object-store conformance (MinIO, fake-gcs, Azurite): real conditional-write semantics.
   - `selfhost-e2e` and `e2e/pipeline/` with the flag on.
   - pglite parity is not applicable: the SQL ledger is unchanged and refuses group calls.
7. **Load proof.** One user with 29 origins and a 50k-object inbox at a fixed process in-flight cap. Report
   admitted per minute against arrivals, plus transients, before and after.

## 7. Risks and open questions (ranked)

1. **Silent loss or duplication from a wrong sequential-equivalence argument.** The two places where grouping
   changes behaviour are in-group judging (C) and coalesced manifest appends (G). An error there is a
   correctness bug with no error code. Mitigation: suite 1 as an oracle against the single path, and the flag
   off until it is green under fault injection.
2. **Process-wide in-flight budget is a prerequisite.** Grouping without a global S3 request cap makes
   tonight's socket starvation worse. Build the cap first, and log the cause of status-less transients.
3. **Coalesced writes widen the blast radius of one bad object.** One corrupt manifest or shard would otherwise
   fail all members touching it. Split-on-failure bounds it to one retry, but the group's wall time is set by
   its slowest session. Group bounds and per-session timeouts are needed.
4. **Hosted path.** SQS visibility timeouts and per-message failure reporting under groups need their own
   design pass before turning anything on there.
5. **Open:** whether orphan chunk objects from a crash between chunk PUT and manifest CAS are ever collected.
   Today's behaviour is unchanged, but groups create them in bursts. UNVERIFIED.
6. **Open:** should one group hold one limiter slot of weight G, or G slots? Weight-G is simpler and keeps the
   cap's meaning, but it needs a limiter API change.
7. **Not addressed here:** relisting the whole upload inbox every promotion pass (about 50 sequential LIST
   pages at 50k objects). It is a separate fix: a cursor or index.

Cheaper steps (separate changes) should be measured first, because they may close most of the gap without
this redesign:
- a process-wide in-flight cap plus a smaller `INGEST_LEDGER_PARALLEL`;
- the per-user pre-claim hint;
- skipping the full relist.
