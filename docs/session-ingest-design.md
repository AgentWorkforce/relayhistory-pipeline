# 96Bulls session ingest: design (2026-11-01)

**Goal:** take the session **write** path off relayhistory-cloud's Neon.
- S3 (sessions-prod, <account-id>, us-east-1) is the record, and Supabase holds the ledger.
- Neon is an optional, asynchronous replica for the transition only.
- The 96Bulls pipeline (S3 notification → SQS → Lambda, #121) indexes from S3 as today.

**Hard constraints (Khaliq):**
1. **Old Desktops work with no client change.**
2. **No Lambda on the request path.** The new account's Lambda concurrency limit is 10, and cold starts hurt uploads.
3. **No 429 from a database lock.**

Code read for this design:
- relayhistory-cloud main `2b0aea2` (#241/#256/#258/#262), #245 at `844878c` (`auth_sessions`, `auth_session_verify`), and #250 (0071).
- 96BullSessions main (with #135 merged).
- relay-desktop at tags `v2026.10.27`, `desktop-v2026.10.65` and `desktop-v2026.10.66`.

## 0. Shape

```
Desktop (any version) --HTTPS, rth_at_ bearer--> history.agentrelay.com  (relayhistory Worker, unchanged hostname/route)
    requireAuth + rth:sync (#245: Supabase, not Neon)
    DELIVERY_FORWARD routes the org? ──no──> today's path (s3_first / Neon)
        │yes
        └─service binding DELIVERY_INGEST (same account, no public hop)──> 96bulls-session-ingest Worker
              (private: no route, no domain, no workers.dev; identity from the binding)
              prepare  CPU: vendored relayhistory validation, payload scrub, digests, receipt
              claim    Supabase via Hyperdrive, 1 round trip: deletion filter, replay / 409 (#250),
                       stale records; all stale -> committed here: 200   (replay fast path)
              slot     Supabase: per-user / per-org in-flight lease, else 429 + Retry-After
              write    S3 (srv-writer key, SSE-KMS): chunks + manifest CAS + pointers, m=srv_ layout
                       wider than one invocation: ONE staged object + fan-out messages
              replica  optional: relayhistory's NEON_REPLICA Cloudflare Queue (transition only)
              commit   Supabase: committed, written revisions, fence recheck; slot released
              200      the receipt + limits
    INGEST_FANOUT queue -> the same Worker's queue(): staged batches written session by session
S3 manifest.json -> SQS -> session-pipeline Lambda (async, unchanged) -> Supabase pointers + ClickHouse
```

## 1. Why every fallback disappears

`s3_first` writes to S3 only when Neon would accept the same bytes. About 30% of batches fall back to the synchronous Neon write, at 20 to 130 s.

| s3_first fallback / failure | cause | in the ingest |
|---|---|---|
| `scrub_not_identity` | Neon's replica rebuilds rows from stored lines, so a line the pipeline scrubber changes must go to Neon | S3 is the record. The stored lines are the scrubbed lines, as for every chunk the pipeline seals. **Lands.** |
| `chunk_string_too_long` | `prepareDelivery` truncates a long string and appends `[relayhistory: truncated …]`, which pushes it past `MAX_SCRUB_CHARS`. Long `*_json` evidence also exceeds the bound. | Main's `scrubLong` segments it (rule set 2). With no safe cut, `oversize: "redact"` turns that one string into a redaction marker (section 2c). **Lands.** |
| `too_many_sessions` | the Workers subrequest budget | **Staging**: one scrubbed object holding the whole batch is put before the ack, and a queue fans it out (section 2b). **Lands.** |
| `record_without_session` | the per-session layout | The origin's `h=_none/s=_origin` session. **Lands.** |
| `message_too_large` | the 128 KB queue message limit | Fixed by #262. The replica keeps the descriptor path. |
| `fenced` | a fence or replay only Neon could decide | The fence and the receipt live in Supabase (section 3). **Decided here.** |
| 409 `record_revision` on regenerated jobs | old Desktops re-export whole histories at the same revisions with changed bytes (linuyc: 124k quarantined) | #250 rules: an equal revision is a replay. The **replay fast path** costs one round trip (section 2a). |
| 429 `delivery_busy` | Neon's per-user advisory lock (0039), the per-org `memory_claims` lock and the per-origin `FOR UPDATE` (0038) | No database lock is waited on. A 429 means only that the tenant is over its in-flight limit, with `Retry-After` (section 5). With `INGEST_CLAIM_LOCK_BUSY=on` (default off), it can also mean a claim lost its lock wait to a concurrent claim (section 5); with `INGEST_CLAIM_TIMEOUT_BUSY=on` (default off), also a claim statement cancelled by SQLSTATE 57014 by code (statement_timeout or pg_cancel_backend), which rolled back fully. |
| zstd `unexpected end of file` | workerd's compressor limit | relayhistory #258's one-frame encoder, ported into the pipeline's `raw-chunks.ts`. |

**Every batch lands in S3 before the response, and there is no fallback path.** A failure is a retryable 503. The claim stays `pending` until the commit, so the retry resumes the same idempotent write.

## 2. The front door: a private Worker behind relayhistory

- **The edge** (relayhistory-cloud PR, `lib/delivery-forward.ts`):
  - It runs inside the relayhistory Worker that already owns `history.agentrelay.com`, after `requireAuth` and `rth:sync`.
  - **Old clients:** every released Desktop accepts only that hostname (relay-desktop `cloud.rs:671-685`). So nothing about the client changes: not the URL, the bearer, the route or the protocol.
  - **Routing:** `DELIVERY_FORWARD` is a Worker secret, live in seconds.
    - `orgs`: a canary allowlist.
    - `percent`: a stable SHA-256 bucket per org, so a ramp only ever adds orgs.
    - `exclude`: a per-org instant rollback.
    - `mode: "off"`: rollback for everyone.
  - **Forwarding:** a routed batch is handed byte for byte over the `DELIVERY_INGEST` service binding. The verified identity goes in `x-relay-ingest-identity`; **the bearer is never sent.** The ingest's status, body and `Retry-After` come back unchanged.
  - **Ingest unreachable** (the binding throws or times out): by default a retryable 503, so the ingest stays the only writer for a routed org and the retry replays. `onUnreachable: "local"` (an emergency choice) runs today's path on the same bytes; reseed the org afterwards.
- **The ingest Worker** (`src/session-ingest/worker.ts`, stack `stacks/cloudflare/session-ingest.ts`, stage `cf-sessions-prod`):
  - `nodejs_compat` with smart placement, which runs it near S3 and Supabase in us-east-1.
  - **Private:** `workersDev: false`, `routes: []`, `domain: null`. Pure functions, tests and mutations enforce this. The binding is its only way in, so the identity header is trusted.
  - **Same Cloudflare account as relayhistory:** a service binding cannot cross accounts, and the stage refuses any other account.
  - **The identity is signed (review Medium):** relayhistory HMAC-SHA256-signs each call over method, path, the identity header and the body with a Secrets Store key both Workers hold; the ingest refuses an unsigned, altered or older-than-60 s request. A sibling Worker that binds the ingest cannot assert a tenant.
- **Wire compatibility:** relayhistory's `delivery-contracts.ts` and `session-image.ts` are vendored verbatim. The validation, digest, `servicePayload` and `prepareDelivery` code from `delivery.ts` is vendored as exact line ranges, without its Neon half.
  - Location: `vendor/delivery`. `SOURCE.json` records the SHA-256 of every file and range, and `scripts/vendor-relayhistory-delivery.ts --check` fails on drift.
  - Batch digests and the content-addressed `receiptId` therefore match exactly. So do the 400/403/413/415/422 answers, the 409 bodies (`conflict`, `conflicts`, `conflictCount`), the error envelope, `X-Correlation-Id`, `limits.contentEncodings` and both 2 MiB bounds. A gzip bomb stops at the cap.
- **Credentials:**
  - **S3:** the existing transcript **srv-writer** IAM user. Its grants are exactly the ingest's prefixes (`m=srv_*`, `live-sessions/*`, `live-batches/*`), SSE-KMS only, with no delete. Its keys are Secrets Store bindings set from 1Password. **No AWS stack change.**
  - **Supabase:** Hyperdrive to the Supavisor **session** pooler (`aws-0-us-east-1.pooler.supabase.com:5432`), as `bulls96_session_ingest.<project>`, with caching disabled and `verify-full` against the Supabase Root 2021 CA, the same shape as the read API's Hyperdrive. Hyperdrive does its own connection pooling, so the transaction pooler is not needed. The live config is `96bulls-session-ingest` (`0fcc0265e96f43a39976b5aa4263f4a6`, created 2026-11-05). Alchemy looks it up by id and never holds the password.
- **Layout:** relayhistory's live writer exactly. The same `m=srv_<sha256(ws␟origin␟user)>` machine, line shape, `live-sessions/` pointer, and manifest owner `relayhistory-api:delivery` with the `relayhistory_live` replay extension.
  - This is deliberate. During the transition both Workers append to the **same** session manifests; the manifest compare-and-swap makes that safe, and a batch one of them already wrote is a no-op for the other.
  - `writer.plugin_version` (`96bulls-session-ingest/1`) says which one wrote a revision.

### 2a. Replay fast path (old Desktops re-sending whole histories)

- **What old clients do:** in `new`/`all` sharing mode, any include or exclude regenerates the upload job, and the auto-include pass re-runs on every core start because its `requested` set lives in memory only. Each regeneration re-exports the whole history at the **same** revisions, since revisions are local change-feed positions, often with changed bytes (added columns). Today each of those batches takes Neon's per-user lock, and 0026 409s them; that is linuyc's 124k quarantined records.
- **The index:** `sessions.ingest_record_revisions`, keyed on (org, workspace, origin, record_id), holding the **committed** revision, revision id and digest. It never holds content.
  - It is seeded from Neon `delivery_records` by `ingest_seed_records`, which only a separate NOLOGIN seeder role can call. The seed is idempotent and never lowers a revision.
  - Seed per org **before** routing it, and again right after.
  - **Gate:** the Neon→S3 exporter must have caught up for the org before the seed. A seeded revision is acknowledged as stored without an S3 write, so S3 has to hold it already. (Neon-only records come from today's fallbacks.)
- **The rule (#250 / 0071):** a record whose stored revision is at or above the submitted one is **stale** and is acknowledged without a write.
  - Equal revision with any digest: a replay. The stored copy is kept.
  - Lower revision: ignored.
  - Still 409: two identities at one new revision in one batch, and a reused batch id with other content.
- **The cost:** a primary-key lookup of up to 500 ids, read-only, with no row locks.
  - Every record stale or deleted: the claim commits the batch itself, in **one round trip**, with no S3 call, no slot and no lock. That is ~10 to 20 ms against Neon's 20 to 130 s.
  - A mixed batch writes only its new revisions.
- **Correctness:** only committed revisions count. A resumed batch never mistakes its own unfinished write for a stored one, and a record held by another still-pending batch is not stale (tested in SQL and in the write path).

### 2b. Staging (a batch wider than one Worker invocation)

- **The budget:** a session costs about five subrequests, so a batch with more than `INGEST_INLINE_MAX_SESSIONS` sessions to write (default 40) is **staged**.
- **The staged object:** one create-only object, `live-batches/v1/ws=…/m=srv_…/b=<sha32(digest)>.staged.ndjson.zst`, holding a header (digest, received_at, the groups' session ids, which the session keys carry anyway) and every chunk line.
  - It is run through the pipeline scrubber (oversize segmented) **before** the put, so nothing unscrubbed is at rest.
  - It uses one zstd frame that workerd can always write.
- **The request:** stage → `INGEST_FANOUT.sendBatch` (40 sessions per message) → commit → 200. The batch is durable in S3 at acknowledgement.
- **The queue:** the Worker's own `queue()` handler writes each slice. It is idempotent: the manifest replay extension is keyed by digest. Each message gets 10 retries, then the DLQ.
- **Visibility:** readers see the batch seconds later.
- **Retries:** a retried request finds its own staged object and re-queues. Duplicate messages are harmless.
- **Content addressing:** the key names the batch digest **and** a digest of the exact groups and record identities staged. A retry that keeps the same records reuses its own object. A retry whose kept set shrank (records committed meanwhile by another batch) stages a new object. So the fan-out ranges, the replica's placements and the commit always describe the same object (review H1, tested).
- **Revisions follow the fan-out (review Medium):** the request commits a staged batch **without** revisions. Each slice first asks which of its sessions are fenced now (`ingest_fanout_fence`, skipped), writes the rest, then commits exactly what it wrote (`ingest_commit_revisions`, which re-arms erasure for a session fenced in between). A dead-lettered slice therefore leaves its records not stale, and the runbook's DLQ redrive (within 24 h) recovers it.
- **Erasure:** the erasure job erases a fenced machine's staged batches once they are past the 48 h redrive window, and revisits the tombstone until they are gone. No lifecycle rule (it could delete a batch waiting for a redrive).

### 2c. Scrubbing for over-bound text (`raw-chunks.ts`)

- Main's rule set 2 (`scrubLong`) already segments any over-bound text that has a safe whitespace cut, for every writer.
- The ingest adds only `oversize: "redact"`. A text `scrubLong` cannot cut, such as canonical `*_json` evidence or a base64 dump past 256K, becomes one redaction marker. Fail closed: the record and its other fields still land.
- The default stays `refuse`, so every other writer is unchanged.
- The output is deterministic, so a replay seals identical bytes.
- The check covers the decoded value **and** its JSON literal, as the scrubber does: a newline-wrapped dump has no safe cut in its escaped form.
- The fan-out fits staged lines again before sealing them, because scrubbing can lengthen a string (`?state=` becomes `?state=[REDACTED]`).
- **Known limit:** an embedded image over 262,144 characters (about 192 KB) has no whitespace to cut at, so it lands as the redaction marker. Today `s3_first` sends it to Neon instead.

## 3. Ledger (Supabase)

Migration `supabase/migrations/20261104022000_session_ingest_ledger.sql`. It never creates or alters a role (review High 1: a CNPG / Cloud SQL migration owner has no CREATEROLE). `supabase/bootstrap/session_ingest_roles.sql` provisions both roles NOLOGIN before migrate (`src/hoopdb/migrate.ts` runs it as its bootstrap step, the runbook as `postgres` on Supabase); the migration verifies them and fails closed (42704 absent, 55000 elevated or a member). After migrate, `session_ingest_role.sql` makes the ingest role LOGIN with a 5 s statement timeout. Each function refuses any other caller, and no advisory lock is taken anywhere (a test asserts it).

- **`ingest_batches`** holds idempotency and receipts.
  - Same digest and committed: **replay**. The receipt is recomputed from the request, since it is content-addressed. The stored `receipt_id` is cross-checked, and a mismatch is a 503.
  - Same digest and pending: **resume**.
  - Other digest: **409 `batch_id`**.
  - No client revision ids are stored.
- **`ingest_record_revisions`** holds committed revisions only (section 2a), with their user, source and session (Neon's `delivery_records` shape). The commit upserts the written records in sorted order, max revision wins, and an equal revision keeps the stored row.
- **Deletion, re-enable and reset forget revisions (review High 2).** relayhistory writes every selection and reset to the ingest right after Neon (#265, signed, required once the binding is live), as Neon 0038/0039 do:
  - a disable tombstones the session and deletes this user's revision rows of it, so an explicit re-enable backfills the same revisions under fresh batch ids and they are **written**, not acknowledged;
  - an enable lifts the tombstone;
  - a reset tombstones every session of the user and forgets them, idempotent by request id.
  The seed copies Neon's existing tombstones and resets per org.
- **`upload_deletions` / `upload_resets`** are the **deletion fences, from Supabase**.
  - Records of deleted sessions are dropped before the revision checks, and the receipt is unchanged (Neon 0038's filter).
  - The commit rechecks the fences. A tombstone or reset that landed after the claim queues that session in `upload_erasure_rearm` for another erasure sweep. That replaces the per-user lock Neon used to serialize resets against deliveries.
  - `upload_cleanup_ready()` is not ported: it is permanently true.
- **`ingest_tenant_slots`** are per-tenant concurrency leases (section 5).
- **Per-org HTTP admission stop (`INGEST_HTTP_ADMISSION_STOPPED_ORGS`, default none; decision 0005).** A comma-separated list of org ids (Cloudflare stack input `sessionIngest.httpAdmissionStoppedOrgs`). A listed org's batches, turns and recovery POSTs are a 503 `delivery_unavailable` with `Retry-After: 300` before any ledger call; controls, reads and the `INGEST_ADMISSION` consumer are unchanged. A malformed list refuses startup. It is the fence-authority marker's `workerHttpAdmission: "off"` precondition; see `docs/selfhost-composition.md` for the order (stop, queue drained, backfills, marker).
- **S3 fence mirror (`INGEST_S3_FENCE_MIRROR`, default off; decision 0005 "Controls").**
  - Unset, empty or `off`: no S3 write, unchanged. `on`: the selection and reset routes also write the S3 upload fence that the direct-upload consumers (`ledger-s3.ts`) and the backlog drain read. Any other value refuses startup.
  - The Cloudflare stack input is `sessionIngest.s3FenceMirror: "on"`; omitted renders no var.
  - Turning it on needs Khaliq's OK. It needs the Worker's S3 credential to reach the per-user admission directory (Get and conditional Put, plus ListBucket on `…/ledger/owners/`). It is a precondition (`controlMirror: "on"`) of the fence-authority marker.
  - The route's own refusals (control kind, privacy journal) run before any S3 write, and on journaled routes the S3 write runs inside the privacy-journal bracket. SQL's refusals of an ordered control (version gap, foreign epoch, digest) come from SQL itself, after the S3 pending write described below.
  - A failed S3 write fails the control (503 `s3_fence_unavailable`), and the ledger's code is logged (route `delivery.s3_fence_mirror`). An id the fence refuses is a 400.
  - **Unordered controls:** a disable or reset writes S3 first, then SQL (SQL is never called after an S3 failure). A re-enable writes S3 after SQL.
  - **Ordered controls (`identity.control`): SQL is the ordering authority, and S3 follows its verdict** (`ledger-s3.ts` `s3ControlMirror`). S3 never judges order itself: its view of the control head can lag SQL's, and it cannot check SQL's epoch or digest rules.
    - **A disable or reset is first pending in S3** (`beginOrdered`). Admission treats a pending disable's sessions as deleted, and refuses every claim (a retry) while a reset is pending. This keeps S3 fail closed while SQL decides.
    - **Then SQL applies or refuses it:**
      - Applied, replay **or stale**: the effect lands (`finishOrdered`), under per-session order. SQL answers stale only when a same-digest receipt exists, so stale means "applied earlier, since superseded", never "not applied". A stale reset therefore begins the S3 reset if its request id is new.
      - A definite refusal (a SQLSTATE: gap, epoch, digest) releases only **this request's** hold on the pending entry (`abortOrdered`). The entry goes when no hold is left. A duplicate request that got the control applied, then died before its finish, keeps the entry pending, so a refused duplicate can never release an applied control. A refused control never wedges the legitimate one either: its own entry carries its own digest.
      - An ambiguous failure: the hold stays, so the entry stays pending (fail closed) until a retry's verdict.
    - **A re-enable** lands in S3 only after SQL applied or replayed it.
    - **Finalisation keeps per-session order.** An effect lands only if the control is newer than the last one applied to that session, and an enable never lands under a reset at or after it.
      - Across an epoch rotation the order of two controls is unknown, so S3 fails closed: a disable lands and an enable does not.
      - A disable that finishes after a newer reset still tombstones. That is the fail-closed direction, and it is accepted.
    - The result: S3 is never live where SQL is deleted. S3 may be deleted where SQL is live while a control is pending, after a failed S3 write, or across an epoch rotation, until a later control or retry.
    - **A pending entry nobody retries stays pending indefinitely.** There is no resolver yet. A pending reset refuses every claim of that user (`ledger_control_pending`, a retry). A pending disable keeps its sessions deleted, even after a later enable.
      - Operator note: a user whose uploads keep answering `ledger_control_pending` has an ordered control whose verdict never reached S3. Re-send that control (the same epoch, version and digest): SQL replays it, and its verdict lands.
  - **Deploy order before turning the flag on.** Every fence reader must run code that knows `pending`: the Worker, the hosted upload-admission Lambda, self-host and the rearm sweeper. The fence is `v: 2` while anything is pending, and readers from before `pending` accept only `v: 1`, so they refuse (fail closed, a retry) rather than ignore a pending disable or reset. Deploy the readers first, then turn the flag on.
  - **A reset only begins in S3** (one fence CAS, no listing), so it fits the subrequest budget however many sessions the user has. The next S3-ledger claim for the user finishes it. Until then nothing is forgotten (the fence's `resetsCompleted`).
- **Acquisition owner (dormant):** an enrolled `(org, workspace, source)` admits only the grant-holding owner of each session, and the edge answers a refusal with a typed 409. Nothing is enrolled and the switch is off. See `docs/ingest-acquisition-owner.md`.

## 4. Neon replica (transition only)

- **Why:** while Neon serves the read routes the read API (#135) does not, each accepted batch is also sent as relayhistory's own `NeonReplicaMessage` to relayhistory's **existing `relayhistory-neon-replica` Cloudflare Queue**. It is a producer binding in the same account, so there is no SQS and no cross-cloud credential.
- **What the message holds:** no content. It carries parts, per-record placement, revision id and digest, and the reset count from the claim. relayhistory's consumer applies it (`accept_delivery_batch`, idempotent), so Neon's locks are paid by the consumer, never by the request.
- **Large batches** use the create-only descriptor beside the batch marker. The descriptor is written only when the scrubber leaves every id unchanged. Otherwise the replica is skipped and counted (rare).
- **Staged batches** are sent at request time with `[group, line]` placements in the staged object (`staged: {key, groups}`). relayhistory's consumer rebuilds them from that object and fences their sessions first (relayhistory-cloud #265).
- **Scrubbed ids:** messages carry `writer: "96bulls-session-ingest"`. For that writer only, the consumer accepts a stored id that is the pipeline-scrubbed form of the message's id.
- **Modes:** `INGEST_REPLICA_MODE`.
  - `queue` (= `required`): a failed send is a 503, and the retry re-sends.
  - `best_effort`: the failure is logged and the batch answers 200.
  - `off`: no replica.
- **When to drop it:** by the approved retirement criteria, after the 3-day standby.
- **It doubles as the dual-run comparator** (in the consumer, #265). A receipt Neon gives that differs from the one the ingest acknowledged logs `replica_receipt_mismatch`, and a conflict Neon raises logs `replica_conflict`.

## 5. Capacity, 429s and what old clients do with them

- **No lock is waited on.** The serialization points are:
  - Per batch id: its row.
  - Per record id: its committed-revision row, written at commit. The fast path only reads.
  - Per session: the manifest CAS, retried with jitter up to 8 times.
  - Caveat: the claim SQL itself waits on the fence row and the origin lock (runbooks/history-620-operations.md, lock order), bounded by the role's 2 s `lock_timeout`. By default the loser gets a 503. With `INGEST_CLAIM_LOCK_BUSY=on` (default off), a claim that loses its 2 s `lock_timeout` wait to a concurrent claim for the same tenant answers 429 `delivery_busy` with `Retry-After` 5-10 s instead of 503. Nothing was written. With `INGEST_CLAIM_TIMEOUT_BUSY=on` (default off, independent; Cloudflare stack input `sessionIngest.claimTimeoutBusy: "on"`), a claim whose statement hits the 5 s `statement_timeout` or is cancelled (SQLSTATE 57014 by code only; name-only client timeouts stay 503) answers the same 429; other stages stay 503. That several sub-2 s lock waits on the per-user fence row add up to the 5 s `statement_timeout` is a hypothesis pending operator `pg_stat_activity`/lock/plan evidence; a controlled PG15 probe did not reproduce a 57014. See runbooks/history-620-operations.md, "Lock-wait alert".
- **Per-tenant concurrency instead of a lock.** `ingest_tenant_slots` holds one lease per in-flight batch that writes. The limits are `INGEST_MAX_PER_USER` (default 4) and `INGEST_MAX_PER_ORG` (default 32). The lease TTL is 90 s, so a lost release frees itself.
  - Over a limit: 429 `delivery_busy` with relayhistory's jittered `Retry-After` of 2 to 10 s. The claim stays pending, so the retry resumes it.
  - It is a soft limit: two racing acquires can both pass by one.
  - Replays and fast-path batches never take a slot.
- **What old clients do with our answers** (relay-desktop release tags):

  | answer | v2026.10.27 | desktop-v2026.10.65 / .66 (and main) |
  |---|---|---|
  | 429 `delivery_busy` | `RateLimited`: backoff from 1 s, doubling, with jitter. **`Retry-After` is ignored** (`destination.rs:1501`). | Honours `Retry-After` (seconds or HTTP date, capped at 1 h) on 408/425/429 **and 503** (`destination/retry_after.rs`). |
  | 503 | `Transient`: backoff capped at 5 min. | `Retry-After` if present, else the same backoff. |
  | 409 `delivery_conflict` | Quarantines exactly the named records and re-sends the rest. **An untyped 409 blocks the job**, so the ingest never sends a 409 without `code`. | Same; an untyped 409 is `Transient`. |
  | 401 | Refreshes the token under a lock and re-sends. | Same. |

  - Backoff applies to the whole job, and there is no maximum attempt count.
  - Concurrency is 4 batches per drain on .27 and 2 on .65/.66, so the default per-user limit of 4 does not throttle one machine's normal drain.
- **Latency** (estimates; the canary measures them):

  | step | cost |
  |---|---|
  | edge auth (#245 Supabase) + binding hop | ~10 to 40 ms; the binding adds ~0 |
  | prepare | 10 to 50 ms CPU |
  | claim + slot + commit | 3 × ~5 to 15 ms over Hyperdrive (fast path: the claim only) |
  | write | about 4 sequential S3 calls per session, ~60 to 120 ms, 6 sessions concurrent |
  | replica | ~5 ms |

  - **Fast path p95 is ~50 to 100 ms.** Writes are ~0.3 to 1 s p95. Tonight's s3_first wall p95 is ~5 s and its fallbacks 20 to 130 s.
  - There are no cold starts that matter: workerd isolates start in milliseconds.
- **Throughput:** Workers scale without a concurrency reservation, so the binding is not a bottleneck. Supabase sees ≤ 3 short statements per writing batch and 1 per fast-path batch, pooled by Hyperdrive.

## 6. Migration plan

| step | what | rollback |
|---|---|---|
| P0 prereqs | #245 in `supabase` mode, so `requireAuth` reads no Neon. This migration and the roles applied; Hyperdrive and the Secrets Store secrets created by the lead. | not applied |
| P1 dark deploy | `cf-sessions-prod` "session ingest rollout": the Worker, fan-out queue and DLQ. Then relayhistory's next deploy adds the `DELIVERY_INGEST` binding (only when the Worker exists). No traffic: `DELIVERY_FORWARD` is unset. | stack update / no secret |
| P2 canary | Seed the org's revisions, then `DELIVERY_FORWARD` `{"mode":"on","orgs":[canary]}`, then reseed. The heaviest 429 victims (linuyc) go first: they gain the most. Gates over 48 h: 0 receipt mismatches, 5xx < 0.1%, p95 < 1.5 s, an empty fan-out DLQ. | `exclude: [org]` or `mode: "off"`, in seconds. Both write the same manifests, so there is no split. |
| P3 ramp | `percent` 10 → 50 → 100, gated as in P2, with a seed per newly routed org. | the same secret |
| P4 retire | When the read API serves the reads: replica off, 3-day standby, then the approved Neon retirement. The Neon→S3 exporter covers anything that reached Neon before P3. | the replica back on |

**No Desktop release is needed at any step.** Old Desktops stay on the same front door forever. It no longer writes Neon on the request path.

### Quarantined records on old clients

- Records an old client has already quarantined (from a 409 `record_revision` before #250 or the ingest) **are never re-sent by that client on their own.** relay-desktop never reads `delivery_quarantine` back; it only counts those rows, or deletes them on session exclusion, job cancel or reset.
- They come back only when the client re-exports them in a **new job generation**: a regeneration, a revision bump, or a reset and re-upload. The ingest then acknowledges them, via the fast path or as a write.
- For linuyc's 124k the server already holds the same revisions from earlier generations, so no data is missing. What remains is a client-side counter that the server cannot clear. Draining it without a regeneration needs a relay-desktop change, which is out of scope here.

## 7. Follow-ups (in order)

1. 96BullSessions: the erasure job reading `sessions.upload_erasure_rearm` (Supabase) as well as Neon's tombstones. While relayhistory writes every tombstone to Neon too, the Neon sweep already revisits those sessions every pass, so the re-arm table is a second signal, not the only one.
2. relay-desktop: drain an existing quarantine without regenerating, and stop regenerating on auto-include. Neither is needed for the cutover.
3. When relayhistory's front door is retired: Desktop calls the ingest's `selection` / `reset` routes directly (they exist, and are what #265 calls today).

## 8. Self-host SERVICE (`SERVICE=session-ingest`, default off)

Self-host runs without RelayAuth and without relayhistory-cloud, so there is no front door to sign `x-relay-ingest-identity`. `src/session-ingest/selfhost-server.ts` serves the same Hono app (`app.ts`, unchanged handlers) over node:http in the services image. The hosted Worker never imports it, and nothing on the hosted path changes.

| | hosted Worker | self-host SERVICE |
|---|---|---|
| identity | relayhistory's HMAC-signed `x-relay-ingest-identity` | the Desktop's own `sh_at_` bearer, introspected by relaycontrol on **every** request (`selfhost-auth.ts`, no positive cache unless `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` sets one: default off, at most 60 s and never past `exp`, so the revocation lag is that TTL), `aud` = `RELAYHISTORY_INGEST_ORIGIN`, `rth:sync` required |
| ledger | Hyperdrive, role `bulls96_session_ingest` | `pg.Pool` as `bulls96_session_ingest` (discrete fields from `RELAYHISTORY_INGEST_DATABASE_URL`, `SESSION_POSTGRES_TLS_PROFILE` required), admitted at startup only as a real login of that role (see "Startup admission") |
| store | S3 srv-writer key, SSE-KMS | the Locker store (`src/shared/locker.ts`: MinIO keys, EKS Pod Identity, or GCS) under `INGEST_KEY_ROOT` (default `96bulls`) |
| INGEST_FANOUT | Cloudflare Queue for batches over `INGEST_INLINE_MAX_SESSIONS` | **none**: a Node process has no subrequest budget, so every batch is written inline before the receipt. Nothing is staged, so there is no outbox to lose; an interrupted request leaves a pending claim that the Desktop's retry resumes |
| acceptance | canonical or buffered | canonical only. Buffered, staging, the Neon replica, custody sources, privacy authority and `INGEST_SIGNING_KEY` are refused at startup |
| routes | every `app.ts` route | `POST /v1/delivery/batches`, `POST /v1/turns/batches`, `GET /v1/delivery/limits` (plus `sessionDeletion: false`, `sessionReset: false`), `GET /healthz`, `GET /readyz`, and `POST /v1/onboarding/heartbeat` (204) only with `RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true`. Any other `/v1` path is 501 `unsupported_by_backend` before auth |

- **Auth comes before the body.** The wrapper verifies the bearer before it reads a body byte. A caller without a token or without `rth:sync` cannot make the edge buffer 2 MiB.
- **Header-spoofing proof.** `x-relay-ingest-identity` and `x-relay-ingest-signature` are never read; they are dropped before the app sees the request. The verified identity reaches the app only through a WeakMap keyed by the exact `Request` object, so the ingress needs no header stripping (`selfhost-server.pglite.test.ts`, the "causal" test).
- **Startup admission** (`assertIngestRole`, the pattern of the acquisition installer's `assertInstallerLogin`; the rules of `supabase/bootstrap/session_ingest_verify.sql`). The edge refuses to start unless all of these hold:
  - Identity: `session_user` and `current_user` are both `bulls96_session_ingest`, and `current_setting('role')` is `none`. A privileged or member login that runs `SET ROLE bulls96_session_ingest` is refused. A superuser login that runs `SET SESSION AUTHORIZATION bulls96_session_ingest` makes `session_user` and `current_user` the role and leaves `role` at `none`, so this backend's `pg_stat_activity.usename`, which is fixed at authentication, must also be the role. On PG15 that is the only signal. On PostgreSQL 16+ (from `server_version_num`), `system_user` is non-NULL and is `<auth method>:bulls96_session_ingest`.
  - Role shape: not SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, REPLICATION or INHERIT.
  - Membership, both directions: the role is a member of no role, and no role is a member of it. The only exception is PG16+ on a declared Supabase (`SESSION_POSTGRES_TLS_PROFILE=supabase`, the runtime stand-in for the verifier's `hoopdb.db_mode='supabase'`). There, PostgreSQL records Supabase's non-superuser `postgres` as a member of every role it creates: granted by superuser `supabase_admin`, WITH ADMIN, INHERIT FALSE, SET FALSE. That edge cannot assume the role and is admitted. The same edge with SET or INHERIT, from another grantor, or off Supabase is refused. A Supabase deployment on `custom-ca` must revoke the edge as `supabase_admin`.
  - Settings: no per-database role setting, `rolconfig` exactly the profile pinned by `session_ingest_role.sql` (5s statement, 2s lock, 10s idle-in-transaction), and the live session shows the same values. The pool sends that profile as its startup `options`.
  Any failure, including a failed query, refuses with a code that names the property, never an observed value. PGlite has no real login, so the decision (`ingestLoginRefusal`) is tested over rows captured from real scram logins on postgres:15, 16 and 17. The live PGlite test proves the SET ROLE session that the old check admitted is now refused.
- **Fail closed.** If relaycontrol is unreachable, answers 5xx, answers 401 to our secret or returns a malformed answer, the edge returns a retryable 503 `relaycontrol_unavailable` and writes nothing. A revoked token is refused on the next request.
- **Pattern A login.** The self-host read API's `POST /v1/cli/login` advertises `rth:sync` only with `RELAYHISTORY_SELFHOST_LOGIN_ADVERTISES_SYNC=true` (which needs `RELAYHISTORY_SELFHOST_READ_API=true`), and only when introspection granted `rth:sync`. This edge never trusts that reply.
- **Health.** `/healthz` is liveness and always returns 200 `{ ok, enabled }`. `/readyz` returns 200 only when the edge is enabled and the ledger answers `SELECT 1`. A dormant edge is NotReady by design, so no traffic reaches a process that would answer 503. Introspection is not probed, because probing it needs a token.
- **Token refresh.** `POST /v1/auth/token/refresh` always answers 401 `refresh_not_supported` when enabled. A self-host session has no refresh (the read API's login passes the `sh_at_` token through), and the Desktop treats only 400/401/403 as a refused refresh, which sends it to renew through relaycontrol. A 404 or 501 reads as an outage and never renews. The router must send this exact path here.
- **Async admission (`SELFHOST_INGEST_ADMISSION`, default `sync`).** `async` answers uploads after one create-only inbox PUT with no SQL on the request path (one relaycontrol introspection per request remains), and an in-process consumer admits the inbox through the hosted `admitMessage`, discovering work by ListBucket scan. `drain` is the rollback step. Both probes report the effective mode (`admission`) and, in `async` / `drain`, the consumer's state. See [`selfhost-async-admission.md`](selfhost-async-admission.md).
- **Per-session erase (`RELAYHISTORY_SELFHOST_SESSION_SELECTION=true`, default off, needs migration 043910):** `POST /v1/delivery/sessions/selection` with `enabled:false` fences every origin the caller uploaded the session from, and the relay-turns origin, and tombstones those pointers and the directory row in one transaction (`sessions.ingest_selfhost_set_session_uploads`); since migration 057030 the same transaction also removes the erased copy's directory membership (its machine, and the caller when no live pointer of theirs is left; all of it when the row is tombstoned). `enabled:true` answers 400. A read-API 404 afterwards means unreadable, not bytes gone. It covers the session-ingest upload path only; another user's copy is untouched, an origin first used after the erase is not fenced, and a session not yet indexed has no pointer to tombstone. Under a capture policy with `user_retroactive_delete: false` (from relaycontrol introspection v1.5, `capture-policy.ts`), a member's `enabled:false` is refused 403 `policy_forbids_user_delete`; owners and admins are exempt.
- **Capabilities in the limits answer.** `GET /v1/delivery/limits` adds `sessionDeletion: false` and `sessionReset: false` at the top level of the app's answer (the wrapper merges them; the hosted `app.ts` answer is unchanged). The Desktop enables its delete and reset controls only on an explicit `true`.
- **Onboarding heartbeat (`RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true`, default off).** `POST /v1/onboarding/heartbeat` answers 204 with no body after the same introspection and `rth:sync` check, and never reaches the app. The body is discarded: a small declared body is drained, anything over 8 KiB is not read and the connection closes after the answer. Nothing is stored or logged. Off, it is 501 like any other unserved path.
- **Not served self-host yet:** reset, recovery, custody and the rest of onboarding. On hosted they are relayhistory-internal. Desktop selection (unless the flag above is on) and reset against a self-host origin get a 501 until a self-host design for deletion fences lands.

Env (Secret-injected values are marked *): `SERVICE=session-ingest`, `RELAYHISTORY_SELFHOST_INGEST=true`, `RELAYHISTORY_INGEST_PORT` (default 8081), `RELAYHISTORY_INGEST_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL`, `RELAYCONTROL_INTROSPECT_SECRET`*, `RELAYHISTORY_INGEST_DATABASE_URL`*, `SESSION_POSTGRES_TLS_PROFILE` (+ `SESSION_POSTGRES_CA_FILE`/`_SHA256` for custom-ca), `LOCKER_BUCKET` / `LOCKER_ENDPOINT` / `LOCKER_REGION` / `LOCKER_ACCESS_KEY_ID`* / `LOCKER_SECRET_ACCESS_KEY`* (or Pod Identity / `LOCKER_PROVIDER=gcs`), optional `INGEST_KEY_ROOT`, `INGEST_MAX_PER_USER` / `INGEST_MAX_PER_ORG`, `RELAYHISTORY_INGEST_PG_POOL_MAX` (1..32, default 4), `RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT` (default off), `RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS` (default `0` = off; an integer `0`-`60000`, anything else fails startup; see `docs/local-stack.md` "Cache and revocation lag"), and `ALLOW_IN_CLUSTER_HTTP` for plain-http in-cluster relaycontrol.
