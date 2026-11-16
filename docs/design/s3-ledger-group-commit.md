# S3 ledger: grouped admission (group commit per uploader)

Status: design only, not implemented. Behind a default-off flag when built.
Branch: `review/claude-s3-ledger-group-commit`, base `bd058ecd`. Revision 2 (review round 1 findings folded in).

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

**Non-goal: ordering or determinism across members.** A group gives each member the outcome that member could
have had as one of N concurrent single admissions. It does not promise the outcome of admitting the members
sequentially (see §2 C and §4.8).

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
| 4 | revision shards of live records | ledger-s3.ts:914 `readShards`, judged at 1151 | K GET (parallel ≤ `parallel`) | yes |
| 5 | ownership stubs | ledger-s3.ts:1159-1162 → `registerOwner` (776) | L GET, plus a CAS PUT only on a newer epoch or a missing machine stamp (779-794) | yes |
| 6 | fence again | ledger-s3.ts:1167 | 1 GET | yes |
| 7 | batch doc create/CAS (pending, or committed for `replay_fast`) | ledger-s3.ts:1181 | 1 PUT | yes |
| 8 | privacy-journal gate | async-admission.ts:440 | 1 GET (+1 CAS only to expire stale pending) | yes |
| 9 | consent gate (admit) | deliver.ts:404 | 0 S3 (policy); refused config under `INGEST_LEDGER=s3`, see §4 | — |
| 10 | per session: pointer GET (+ first PUT), manifest GET, chunk PUT, manifest CAS | writer.ts:204/220 `resolveLocation`, 335 `writeGroup`, 443 `writeGroups` | about 4S (≤ 6 sessions at once) | yes, within a session |
| 11 | commit: batch doc + fence | ledger-s3.ts:1201 | 2 GET | yes |
| 12 | claim-token check | ledger-s3.ts:1204-1206 | 0 (on step 11's read) | — |
| 13 | re-arm fenced sessions (rare) | ledger-s3.ts:1229 (defined), 1241 (called) | 0, or 2–3 per fenced session | yes |
| 14 | revision merges | ledger-s3.ts:1255 `mergeIndex` → `mergeIndexBatches` (725) | K × (GET + CAS PUT) | yes |
| 15 | fenced-but-re-enabled check | ledger-s3.ts:1258 | 0 | — |
| 16 | batch doc CAS (committed); lost CAS re-read | ledger-s3.ts:1260, 1261 | 1 PUT (+1 GET on loss) | yes |
| 17 | outcome marker, inbox tombstone | admission-consumer.ts `finish`, async-admission.ts:464, 490 | 2 PUT | yes |

A batch costs about **10 + 3K + L + 4S** requests (plus owner CAS PUTs only for new generations) over **about 13
dependent round trips**. For a typical 20-record, 1-session batch that is about 75 requests.

Where it serialises:
- **Per session manifest:** step 10. The same session appended by many batches (a long session uploaded in
  many batches) is one CAS chain.
- **Per revision shard:** step 14, but only for the same origin and shard.
- **Per user, by policy:** the tenant limiter (`INGEST_MAX_PER_USER`, default 4, selfhost-config.ts:48 and the
  default at :420). It caps in-flight batches, not objects. The slot is taken **after** the claim
  (deliver.ts:420-422), so a refused slot wastes the claim.

The fence and the privacy journal are per-user, but admission only reads them.

## 2. Protocol

### Principle: a group is an execution unit, not a new ledger object

Every batch keeps today's individual state machine, with nothing added:
- its own batch doc (`ledger/batches/o=/b=`, unchanged `BatchDoc`). `validBatch` (ledger-s3.ts:353-358) checks
  the known fields but does not reject extra ones; the design still adds no field, so the layout is unchanged;
- its own claim token;
- its own outcome marker, inbox tombstone and §5 status.

A group only shares reads and coalesces writes. So the layout does not change, nothing migrates, and the
status endpoint, replay and redrive work per batch as today.

### Group key and formation

- **Key:** the admission-inbox machine directory `…/<org>/v1/ws=<ws>/m=<liveMachineId(ws, origin, user)>/`.
  That is exactly one (org, workspace, user, origin), so one revision index (per origin), one fence, one
  privacy journal and one writer machine.
- **Tenant binding per member (finding P2-9).** Tenant, origin and receipt are bound to each inbox key only by
  `decodeInbox` (async-admission.ts:225-233). Every member is decoded on its own, and its decoded
  (org, workspace, user, origin) must equal the group key's. A member that fails is not grouped (single path),
  and the group never reuses the first member's tenant for another member.
- **Dedupe at formation (finding P1-2).** The inbox holds one object per request attempt
  (async-admission.ts:119-122, `r=<receipt>/a=<ts>-<nonce>`), so several attempts of one receipt, or two batch
  ids with equal digests, can sit in one directory at once.
  - Members are deduplicated by **(origin, batchId)** after decoding. The first by `receivedAtMs` stays in the
    group; the other attempts are left out and take the single path later, where the claim answers them as
    `replay`/`resume`/`batch_conflict` as today.
  - Members are also deduplicated by **batchDigest**: two batch ids with the same digest never share a group
    (the second is left out). That keeps the writer's replay identity (`rememberedBatches`, keyed by digest,
    writer.ts:366-372) one per digest per CAS.
  - Same batch id with a different digest: the second is left out; on its single-path claim it gets
    `batch_conflict`, exactly as sequential.
- **Formation:** when the pump starts an entry, it takes up to `G` other **already due** entries of the same
  directory, oldest first, that pass the checks above. There is no time wait, so it never adds latency.
- **Size (finding P2-1, one meaning for the cap).** The cap keeps its meaning, **batches in flight**. A group
  of G holds G units of the per-user and per-org caps, taken **before the claim (before E)**:
  - `G = min(G_MAX, free per-user units, free per-org units, due entries)`. G is clamped to what is free, so a
    group never waits for a slot it cannot get, and no claim is wasted on a later refusal.
  - The limiter gains a `weight` on acquire/release. A single admission is weight 1, as today.
  - With the default per-user cap of 4, G ≤ 4. Raising the cap is what raises G.
- **Bounds:**
  - `G_MAX` defaults to 16, range 1..16. It stays at or below a quarter of `REMEMBERED_BATCHES` (64,
    writer.ts:69), so that the union of members appended to one manifest in one CAS never pushes a recent
    member's remembered digest out of the window while siblings are still pending.
  - Total prepared bytes are bounded (default 8 MiB) and total records are bounded (default 2000).
- **Excluded from grouping**, taking today's single path:
  - entries that failed inside a group, by the rule in §3 (poison isolation);
  - `custodySource !== "relayhistory"`, and any entry whose inbox read or decode fails;
  - anything that uses a single-path feature (finding P2-8): babysitter retention and inline staged fan-out
    (both unsupported by the S3 ledger, ledger-s3.ts:1110-1113; deliver.ts:450-488 stays single-path), the
    replica send (deliver.ts:533-536), and `withSessionWrites` (deliver.ts:415-417, refused under
    `INGEST_LEDGER=s3` at selfhost-server.ts:228-229). A deployment where any of these is on never forms
    groups.

### Pump bookkeeping (finding P2-2)

The skip-at-cap hint (selfhost-admission.ts:318-348) counts per started entry. A group is G started entries:
- `inflightOrg` and `inflightDir` are bumped by **G** when the group starts, and released per member as each
  member ends.
- **Every member is marked in flight** in the pump's `inflight` set for the life of the group, so the pump
  cannot start a member separately (which would steal its claim token, `ledger_attempt_superseded`, and waste
  the work the hint exists to save).
- A member is eligible only if its own `notBefore` has passed and its directory's `dirBusyUntil` has passed,
  exactly as for a single start. A member deferred inside a group sets its own `notBefore` and the directory's
  `dirBusyUntil`, as `admitOne` does today (selfhost-admission.ts:283-291).

### Sequence

New optional `Ledger` methods: `claimGroup(inputs[])` and `commitGroup(inputs[])`. Only the S3 ledger
implements them, and the SQL ledger never does, so the caller takes the single path. A group of 1 must be byte
for byte today's `claim` / `commit`.

**A. Read (parallel).** The G inbox objects, the G batch docs, and the fence (once).

**B. Classify each batch on its own (finding P2-4).**
- `replay` and `batch_conflict` are decided per batch as today (ledger-s3.ts:1131-1134), and leave the group.
- A pending batch with the same digest is a `resume`.
- The two fence cases stay distinct, exactly as the single claim:
  - **A pending, undecided reset** (`hasPendingReset`, ledger-s3.ts:1141): the whole group fails as
    `ledger_control_pending`, a retry, as today. No member is claimed.
  - **A reset begun but not finished** (`resets > resetsCompleted`, 1146-1150): the group first calls
    `finishResets`, then, if it finished, backs off and re-runs from A with a fresh fence read. A completion
    that fails does not fail the group: the group goes on against the fence as it is, as the single claim does.

**C. Judge (one shard read) (finding P1-1).**
- Read the union of the K' shards that live records touch, once.
- Judge **each member only against the stored index**, exactly as its own `claim` would (ledger-s3.ts:1151,
  `judge`), never against earlier members' new records. Members are judged independently: the outcome for each
  is one it could have had as one of N concurrent single admissions. There is no in-group dependency and
  nothing to re-judge when a member leaves.
- Consequence: two members of a group that carry the same record and revision with different digests are both
  judged new, as two concurrent single batches would be today. Their merges at H resolve it by the existing
  stored-vs-incoming rule. That is today's concurrent behaviour, not a new one.
- `revision_conflict` stays per batch and leaves the group.
- `replay_fast` (nothing to write) is decided **only from the stored index**, never from in-group evidence, so
  it never depends on a sibling's pending write. Such a member is committed by its own create/CAS in E as
  today; that is safe because its evidence is already durable.

**D. Register owners (once per session) (finding P2-7).**
- Register the union L' of live sessions once, with the generation read at A (one fence read).
- Re-read the fence once. If it moved in a way that touches a member (the predicate at ledger-s3.ts:1169-1171,
  evaluated per member), only the touched members re-run from A; untouched ones continue.
- **Bound:** a member re-runs at most `attempts` times in total, the same bound as the single claim loop
  (1120). A member that would exceed it fails with `ledger_batch_contended` (1186), a retry of that member.
- **Fence snapshots stay per member.** Each member carries the fence snapshot of **its own** last claim pass
  into E (its own `fenceVersion`, `fenceInstance`, `resets`, `epochs`). Members that re-ran have a newer
  snapshot than members that did not. Nothing is shared across members after E: H computes `fencedBy`, re-arm
  and owner stamps **per member against that member's own snapshot** (ledger-s3.ts:1215-1235), and only the
  I/O is batched (see H).

**E. Write the batch docs (parallel, distinct keys).** One create/CAS per member, each carrying its own token
and its own snapshot from D. A lost CAS affects that member only (backoff and re-run that member from A, within
the D bound).

**F. Gates.**
- The privacy journal is read once, **after** E, then compared with each member's own `journalSeq`. A member
  that fails is refused terminally on its own (`delivery_privacy_superseded`, as today); `pending` means a
  retry of the members it covers.
- The consent gate (admit phase, deliver.ts:404) runs per member. A hold is a retry of that member only.
- No member writes anything before its own claim and gates pass.

**G. Canonical writes (coalesced per session) (finding P1-2, writer side).**
- For each session in the group: one pointer resolve and one manifest GET, then one chunk PUT **per member
  batch** touching it (parallel, distinct keys and consecutive seqs, so every batch keeps its own chunk and
  replay identity), then **one** manifest CAS that appends those chunks in member order.
- Before appending, each member's digest is checked against the stored manifest's remembered batches **and
  against the digests of earlier members already appended in this CAS**. A match is a no-op for that member
  (its existing chunk is its answer), exactly as a sequential replay. After dedupe at formation this cannot
  happen within one group, but the check stays as defence in depth.
- That CAS also adds each member's `[digest, seq]` to the remembered batches, so `rememberedBatches` still
  detects each member's replay (writer.ts:366-372, 387).
- A session's write failure (foreign manifest, `manifest_contended`, an ambiguous PUT) fails only the members
  touching that session. They stay pending and retry; members that touch only other sessions continue.
- Sessions are written in parallel, bounded by the process-wide in-flight budget (§5).

**H. Commit (findings P2-3, P2-5).** Per member, against its own snapshot; the I/O is shared where it can be:
- One fence read and the G batch docs (parallel).
- **Per member: the claim-token check** before anything else (ledger-s3.ts:1204-1206): `ledger_no_claim` or
  `ledger_attempt_superseded` removes that member only.
- **Per member: the consent gate again** (commit phase, deliver.ts:541). A member now held is left out of
  every write below and retried; it is never merged.
- Per member: `fencedBy` (ledger-s3.ts:1215) against its own snapshot.
- Re-arm the union of fenced (member, session) pairs **before** any merge, as today (1229/1241). Each re-arm
  carries its member's own epoch and resets, so two members' stamps never mix.
- One `mergeIndexBatches` per shard over every surviving member's entries, each member's entries stamped with
  its own claim generation. That function is specified as equal to sequential merges (ledger-s3.ts:716-725).
- **Per member: the post-merge fenced-but-re-enabled check** (1258) → `ledger_attempt_superseded` for that
  member only.
- Then each member's pending → committed CAS (1260), with the lost-CAS re-read (1261), in parallel.

**I. Finish.** Each member's outcome marker and inbox tombstone, in parallel; the member's cap units are
released as it ends.

### Idempotency keys

These are unchanged: batch doc key (origin, batchId), claim token, manifest remembered `[batchDigest, seq]`,
receipt id (content-addressed), outcome marker per receipt dir. There is no group id anywhere durable.

## 3. Failure semantics

- **Partial failure inside a group.** Every decision is per member, or keyed so that a failure names the
  members it covers.
  - A session write failure fails only the members touching that session.
  - A shard merge failure fails the commit of the members whose entries are in that shard.
  - A failure in a step shared by all members (the single fence read, the journal read, the inbox listing) is a
    retry of the group as a whole, not a per-member failure (below).
  - Everything else proceeds.
- **Poison isolation (finding P2-6).** The rule must survive the counters as they are:
  `entry.attempts` increments at the start of every attempt (selfhost-admission.ts:271), a tenant deferral
  subtracts it again (:285), and the counter lives only in memory.
  - Each entry gets a separate in-memory flag, `isolated`, set when the member **itself** failed inside a group:
    its own claim, gate, session write, merge or commit returned an error that names it. Attempt counts are not
    used for the decision.
  - An `isolated` entry is never grouped again; it takes the single path until it ends.
  - A failure of a **shared** step does not set `isolated` on anyone. The group is retried as a group after the
    usual backoff, so one flaky shared read never ungroups the set permanently.
  - After a restart the flag is gone, so a poison batch may join one more group. It then fails on its own again
    and is isolated after that one group. Its siblings' work in that group is unaffected, because failures are
    per member. The existing `maxAttempts` dead-letter (selfhost-admission.ts:300) still applies per entry.
  - Terminal answers (`batch_conflict`, `revision_conflict`, privacy superseded, 4xx DeliveryError) are already
    per member.
- **Crash at each step.** Because members are judged only against the stored index (C) and `replay_fast` is
  decided only from stored evidence, a crash at any point leaves each member in a state its own single
  admission could have left.

  | Crash point | State left | Next attempt |
  |---|---|---|
  | after A–D | owner stubs registered (harmless; a reset lists them), no batch doc | claims afresh |
  | after E | pending batch docs (and `replay_fast` members committed on stored evidence) | `resume` per member (new token), as today; committed ones answer `replay` |
  | between chunk PUTs and the manifest CAS in G | unreferenced chunk objects | appends at the next seq. Same as a single batch crashing today; whether orphan chunks are collected is UNVERIFIED and does not change. |
  | after the manifest CAS, before H | members written but pending | `remembered` makes each re-write a no-op, then commit |
  | inside H, after re-arm, before or within merges | markers stand; the index may lag the manifest, never lead it | merges again (idempotent: max wins, equal keeps) |
  | after some per-member commits | committed members answer `replay` | the rest commit |
  | after commits, before I | — | the outcome is re-derived from the claim (replay) and written |

- **Lost responses.** An ambiguous manifest PUT keeps `ambiguousManifestPut` semantics per session. A lost
  batch-doc CAS is re-read per member (ledger-s3.ts:1261).
- **Lease expiry.**
  - The group's cap units have the limiter's TTL of 90 s. If a group outlives it, the cap loosens; correctness
    doesn't change, because every decision is a CAS.
  - The group bounds (G, bytes, records) target a p99 group time well under 30 s.
  - A group whose pass exceeds the budget stops forming new phases and finishes or abandons per member (pending
    members resume).
- **Concurrency with single-path admissions** (another replica, an old image, a retry) is the same set of CAS
  objects. It is exactly today's concurrent-batch case.

## 4. Privacy invariants

Each invariant holds per member, with reads that are the same as, or later than, a single batch's.

**Scope under `INGEST_LEDGER=s3`.** The self-host server refuses session-selection erase, operator erase,
upload consent and `withSessionWrites` with the S3 ledger (selfhost-server.ts:228-229). So the consent gates
below never run on the path this design changes today; they are kept per member so that the design stays
correct if consent is ever admitted with the S3 ledger. The gates a group can meet today are the privacy
journal (reset, seq) and the fence (selections and deletions written by the control path).

1. **No write before the gates.** No canonical or index write for a member happens before its own claim (E),
   its privacy-journal check (F) and its consent check (F). Steps G and H only take members that passed F, and
   H re-runs consent per member.
2. **Reset and selection landing mid-group.**
   - Owners are registered (D) before any canonical write, so a reset that starts after D lists them.
   - The fence is read three times, as today: A/B, D and H. The two B cases stay distinct (§2 B).
   - Each member's `fencedBy` and `isDeletedForAdmission` run against those reads and its own snapshot. A reset
     that lands between D and H fences the member at H, which re-arms its sessions before the merges, exactly
     as for a single batch. The post-merge check (1258) stays per member.
3. **Seq gate.** The single journal read happens after every member's claim, so it is never earlier than the
   read each member would have made alone. Comparing against each member's own `journalSeq` keeps the refusal
   per member.
4. **Erasure and `upload_deletions`.**
   - Index entries are stamped with each member's own claim generation (epoch, resets), unchanged, so a later
     disable or reset makes them stale.
   - `entryForgotten` runs per entry inside `mergeIndexBatches`.
   - Deleted sessions are filtered per member in C.
5. **Consent holds.** Per member, at admit and at commit: a retry of that member only, with its bytes kept in
   the inbox. A hold never becomes a group refusal.
6. **Frozen-receipt path.** The S3 ledger refuses custody (ledger-s3.ts:883) and grouping requires
   `custodySource === "relayhistory"`. The frozen-receipt and frozen-witness reads (admission-consumer.ts) never
   run inside a group: such entries take the single path.
7. **Controls mirrored to SQL.** `withSqlControlMirror` (ledger-s3.ts:1442) runs only on the control path.
   Grouping never writes the fence, the journal or SQL. A control interleaves with a group exactly as with N
   concurrent single batches, because every check above is the single-batch check.
8. **No cross-member dependency.** Judging (C) uses only the stored index; `replay_fast` uses only stored
   evidence; fence snapshots, gates, re-arms, merges' stamps and commits are per member. Members share I/O, not
   decisions. The order of chunk appends (G) and of entries within one `mergeIndexBatches` call (H) is the
   oldest `receivedAtMs` first, which only fixes how ties resolve where today's concurrent admissions would race.
   **The proof obligation for §6 is: every member's outcome equals an outcome of some interleaving of N
   concurrent single admissions**, not a specific sequential order.

## 5. Compatibility

- **Layout and migrations:** none. Objects are unchanged, and the manifest format is unchanged: several chunk
  entries and several remembered batches per CAS are already representable.
- **Mixed versions:** an old image's single-batch admissions and a new image's groups touch the same CAS
  objects. That is today's concurrent case, so a rolling restart and rollback are safe.
- **Flags** (self-host env; a hosted stack input later). `INGEST_ADMISSION_GROUP` is `off|on`, default `off`.
  `off` is byte for byte today's path.

  | Variable | Default | Range |
  |---|---|---|
  | `INGEST_ADMISSION_GROUP_MAX` | 16 | 1..16 (also clamped to free cap units) |
  | `INGEST_ADMISSION_GROUP_MAX_BYTES` | 8 MiB | — |
  | `INGEST_ADMISSION_GROUP_MAX_RECORDS` | 2000 | — |

  Rollback is the flag off; there is no state to undo.
- **Self-host:** `selfhost-admission.ts` pump forms groups. It needs an `admitGroup` sibling of `admitMessage`
  that shares `finish`, `classify` and the per-member outcomes, and the pump bookkeeping in §2.
- **Tenant limiter:** gains `weight` on acquire/release (weight 1 is today's call).
- **Hosted Lambda:** SQS batches records from many uploaders. The handler groups records of one SQS batch by
  inbox directory and reports `batchItemFailures` per message, which maps one to one to per-member outcomes.
  The ingest-edge path ships first; hosted only after.
- **Writer:** `writeGroup` (writer.ts:335) gains a multi-batch variant (G chunks, one manifest CAS). The
  single-batch call keeps its signature.
- **In-flight budget:** a group multiplies parallel requests, so this depends on a **process-wide** S3
  in-flight cap. Today's `parallel` is per call (ledger-s3.ts:566 `mapLimit`), and the transient diagnosis
  points at socket over-subscription. Without that cap, grouping makes the transient problem worse.

## 6. Throughput model and proofs

**Model**, per group of G batches of one session-heavy uploader (K' shards ≤ ΣK, L' and S' distinct sessions):

| | Today, per batch | Grouped, per batch |
|---|---|---|
| Requests | 10 + 3K + L + 4S | (7G + 3K' + L' + 3S' + chunks) / G |
| Example: G = 4, 20 records, 1 session | about 75 | about 35 (from shard sharing and manifest coalescing) |
| Serial depth | about 13 round trips | about 14 round trips, but per group |

The cap keeps meaning batches in flight, and a group holds G units (§2). So at a per-user cap c the number of
batches in flight is still at most c; what grouping changes is how many round trips each batch costs and how
many serial passes a set of c batches needs. With c = 8 and G = 8, one group of 8 completes in about one
group's depth (about 14 round trips) instead of 8 single admissions sharing 8 slots, each about 13 round trips
deep but contending on the same pointers, manifests and shards. The expected gain is from fewer requests per
batch (about 2x at G = 4, more for session-heavy uploaders) and fewer requests in flight per batch (relieving
the socket pressure), not from more batches in flight. The number to prove is batches admitted per minute at
a fixed per-user cap and a fixed process in-flight cap.

**Tests and proofs** (all must pass before the flag can be turned on anywhere):
1. **Concurrent-equivalence property suite.** A random mix of batches (shared or distinct sessions and records,
   equal and lower revisions, conflicting digests, duplicate attempts of one receipt, equal digests under two
   batch ids) over an in-memory store with CAS semantics. For every member, the grouped admission's final batch
   doc, manifest entries (chunks, remembered batches), index entries, owner stubs, re-arm markers and outcome
   must equal those of **some interleaving** of the same members admitted as concurrent single batches. The
   oracle enumerates interleavings for small G and samples for larger G.
2. **Fault injection at every await.** Crash, then resume:
   - no record lost or duplicated (in particular: no duplicate chunk for one receipt, P1-2);
   - no write for a member whose gate failed;
   - no member committed for data that was never written (P1-1): a sibling refused at F, G or H never changes
     another member's stale/fast judgement;
   - the index never leads the manifest;
   - every pending member resumes or replays.
3. **Interleaved controls.** Selections, resets (both B cases) and journal bumps are injected at every step. No
   deleted session gets a canonical write after the fence that deletes it, unless it is re-armed. The seq gate
   refuses exactly the members it would refuse alone. A member re-run at D carries its own newer snapshot into
   H, and its stamps never mix with a sibling's.
4. **Poison isolation.** Cases: one member with a foreign manifest, one with a revision conflict, one whose
   inbox object is corrupt, and a shared-read failure. Siblings must commit; the member that failed on its own
   is isolated; the shared failure isolates nobody.
5. **Pump bookkeeping.** A group of G holds G cap units and marks G entries in flight; the pump never starts a
   member separately; a member's `notBefore` and the directory's `dirBusyUntil` are honoured.
6. **Mixed versions.** Old single-batch and new group admissions run concurrently on one store, and the
   suite-1 oracle must still hold.
7. **Lanes.**
   - Object-store conformance (MinIO, fake-gcs, Azurite): real conditional-write semantics.
   - `selfhost-e2e` and `e2e/pipeline/` with the flag on.
   - pglite parity is not applicable: the SQL ledger is unchanged and refuses group calls.
8. **Load proof.** One user with 29 origins and a 50k-object inbox at a fixed process in-flight cap. Report
   admitted per minute against arrivals, plus transients, before and after.

## 7. Risks and open questions (ranked)

1. **Process-wide in-flight budget is a prerequisite.** Grouping without a global S3 request cap makes
   tonight's socket starvation worse. Build the cap first, and log the cause of status-less transients.
2. **The gain is bounded by the per-user cap.** Because the cap keeps meaning batches in flight, G ≤ the free
   cap. At the default cap of 4 the gain is modest (about 2x fewer requests per batch). If the cheaper fixes
   remove the socket pressure, raising the cap may give most of the gain without grouping.
3. **Coalesced writes widen the blast radius of one bad object.** One corrupt manifest or shard fails all
   members touching it. Per-member isolation bounds it to one retry each, but the group's wall time is set by
   its slowest session. Group bounds and per-session timeouts are needed.
4. **Hosted path.** SQS visibility timeouts and per-message failure reporting under groups need their own
   design pass before turning anything on there.
5. **Open:** whether orphan chunk objects from a crash between chunk PUT and manifest CAS are ever collected.
   Today's behaviour is unchanged, but groups create them in bursts. UNVERIFIED.
6. **Not addressed here:** relisting the whole upload inbox every promotion pass (about 50 sequential LIST
   pages at 50k objects). It is a separate fix: a cursor or index.

Resolved in revision 2: the in-group judging that could commit a member for data nobody wrote (now judged only
against the stored index); duplicate attempts or digests in one group appending twice (deduplicated at
formation, re-checked at G); the cap's meaning (G units, taken before the claim).

Cheaper steps (separate changes) should be measured first, because they may close most of the gap without
this redesign:
- a process-wide in-flight cap plus a smaller `INGEST_LEDGER_PARALLEL`;
- the per-user pre-claim hint (by user, not by machine directory);
- skipping the full relist.
