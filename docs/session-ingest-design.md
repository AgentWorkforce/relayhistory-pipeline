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
| 429 `delivery_busy` | Neon's per-user advisory lock (0039), the per-org `memory_claims` lock and the per-origin `FOR UPDATE` (0038) | No database lock is waited on. A 429 means only that the tenant is over its in-flight limit, with `Retry-After` (section 5). |
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
