# Babysitter evidence retention (replacement for the Neon sweep)

Status: **dormant by default.** Every switch is off. Nothing changes in production until the production lead applies migration `20261105042000` in a window and turns on the flags in the order below, with Khaliq's OK.

## What is being replaced

relayhistory-cloud enforces the 30-day promise for Babysitter evidence (`session_lineage` and `turn_receipt` delivery records, `retention_class=standard_30d`) in Neon only.

- **Expired:** `payload.expires_at_ms <= now`. The timestamp is the client's, compared with the database clock. Ingest checks that it is a positive number at most 31 days ahead, with no lower bound (`vendor/delivery/prepare.ts`).
- **Purge:** the cron's global `expireBabysitterEvidence` runs every minute, up to 500 rows across all tenants. It sets `operation='delete'` and `payload=NULL`, and keeps the revision and digest.
- **Hide:** every `GET /v1/delivery/records` filters out expired rows, whatever the switches say.

**Finding (2026-11-06):** under Neon's 0078 cutover guard (`phase=active`), that UPDATE has no cutover permit. It most likely raises `delivery_cutover_held` on every tick, which the cron logs as `Babysitter evidence expiry failed`. This is read from code; confirm it with a read-only `wrangler tail`.

## Where the evidence lives on the new store

- **Supabase:** metadata only, in `ingest_record_revisions` and in the frozen copy `ingest_frozen_source_records`. No table holds the payload.
- **S3:** the payload bytes:
  - in session raw chunks, and in `h=_none/s=_origin` for evidence with no session;
  - in staged batch objects.

The pipeline does not project or serve these kinds, and the Neon→S3 exporter never exported them. The transcript eraser key has no GetObject, so it cannot remove a single line from a chunk.

## Design

1. **Write path** (ingest Worker, `INGEST_BABYSITTER_RETENTION_ROUTING=on`; any case, trimmed). Identical in every acceptance mode: canonical inline, canonical staged, and buffered, which is the Neon-exit production path.
   1. **Register** the batch's buckets with `sessions.ingest_babysitter_retention_register`. Each bucket is `h=_babysitter/d=*/s=_bx<yyyymmddhh>-<sha16(source␟session␟reset epoch␟fence epoch)>` under the same machine, keyed by the expiry rounded **up** to the UTC hour.
   2. **Write** each bucket's full Babysitter lines as **one create-only object**, `…/h=_babysitter/d=<erase date>/s=<bucket>/raw/b=<sha32(batch digest)>.ndjson.zst`, with no manifest, no pointer and no CAS. It is scrubbed like every chunk and idempotent by batch digest.
   3. **Carry on** with today's path, with the record in its **own** session as a **payload-free stub**: same identity, revision, operation and digest metadata, payload `{retention_class, expires_at_ms, retention_bucket}`.
   - **Content-free key index** (`sessions.babysitter_evidence_index`, registered in the same call): IMMUTABLE candidate rows, one per (record, revision, registering claim), holding identity, revision, revision id, body digest, operation, kind, the claim's privacy identity (user, source, session, epochs) and, for an upsert, the exact object name, 0-based line and `expires_at_ms`. It never holds a payload, and a row is never re-pointed: a later equal-revision attempt (a changed body, a delete, a retry) adds its own row.
     - **Only the ACCEPTED candidate is served**: the registering claim's batch is committed AND that exact body was accepted (the immutable lazy ACK decision `ingest_lazy_ack_decisions` with the candidate digest and `body_accepted`, or the committed `ingest_record_revisions` row with the same revision, revision id, digest and user). An ACK-refused attempt can neither appear nor displace the accepted winner's original line.
     - **Babysitter-kind deletes** become PERMANENT payload-free tombstone rows with no bucket and no object; they outlive every payload, so `include_deleted` keeps Neon's delete row. Only a privacy fence (disable or reset) removes them: hidden at once, purged by the sweeper in bounded steps (C3).
     - Upsert rows are hidden the moment their bucket is fenced or expired, and are deleted with the bucket's erase.
   - **Write obligations** (`sessions.babysitter_write_obligations` / `_invocations`, the shared `write-obligation.ts` contract): register records, BEFORE the PUT, one DISPATCHED invocation per bucket object (the claim token). Each PUT runs under an enforced wall deadline (`boundedPut`); its definite outcome (acked with its own version id, or refused) is recorded by `ingest_babysitter_retention_written`; an unknown outcome stays dispatched and fails the request. There is NO content attribution: an ETag (content identity, absent as an MD5 under SSE-KMS) never resolves an invocation.
   - **What this guarantees:** a bucket holds only Babysitter lines, and no session chunk, staged object, buffered stage or canonical generation ever holds the payload. No mixed object is ever registered or erased.
   - **What stays unchanged:** the SQL prepare, publication, seats and custody, which compare SQL metadata only, and the revision fence.
   - **Reset epoch:** it is part of the key, so a write after a user reset always opens a new bucket. A reset never erases post-reset evidence.
   - **Request budget: routing is HOLD until the deployed cap is proven.**
     - Cloudflare's documented Workers Paid default became **10,000** subrequests on 2026-02-11, configurable per Worker; Free stays at 50 external and 1,000 internal ([changelog](https://developers.cloudflare.com/changelog/post/2026-02-11-subrequests-limit/), [limits](https://developers.cloudflare.com/workers/platform/limits/)). That is vendor documentation, not proof of this account's entitlement.
     - Root's read-only observation (2026-11-07T02:55Z): the ingest Worker has `usage_model=standard` and no explicit `limits.subrequests`, and the plan entitlement was not readable. The effective deployed cap is **unproven**.
     - So routing is active **only** with an explicit `INGEST_SUBREQUEST_CAP` set to the proven cap. It is never assumed; without it, routing stays inactive and logs a warning.
   - **Worst-case proof** (`worstCaseSubrequests`, counting every subrequest, with retries and redirects at their maxima):

     | Component | Subrequests |
     |---|---|
     | database | 1 (one Hyperdrive connection per request; every ledger call rides it) |
     | replica | 0 (routing requires it off) |
     | bucket PUTs | B, one create-only PUT per bucket, no retry (a failure is a 503) |
     | inline (canonical, S ≤ the inline bound) | S × `GROUP_ATTEMPTS` (3) × (4 pointer calls + `CAS_ATTEMPTS` (8) × 4 CAS calls) = S × 108 |
     | staged (canonical, S > the inline bound) | 1 staged PUT + 1 queue send |
     | buffered | 1 stage PUT + 1 conflict GET |

     Every S3 call is multiplied by (1 + R), where R is the redirects one `fetch` may follow: `INGEST_SUBREQUEST_REDIRECTS`, default **20**, the fetch maximum. R may be lowered only once proven. **Total** = 1 + queue sends + S3 calls × (1 + R).
   - **Refusal is before any write.** A routed batch whose worst case exceeds the cap is refused **before the claim**: no ledger row, no S3 object. The response is a **typed 422** (`delivery_budget_exceeded`). It is not a 503, because the batch is an immutable idempotency unit that can never fit, so replaying it makes no progress.
   - **Supported client reduction and recovery.** The server never splits a batch or changes an identity. Desktop releases from `released-v2026.11.4-beta.1` (relay-desktop `desktop-core/src/history/destination.rs`, `ReceiverRejection::isolatable`: 400/422) isolate a rejected batch **locally**: they split it depth-first into smaller deterministic child batches (at most 32 isolation requests per drain, resuming later) until each child fits, and a single record that is still rejected is set aside.
     - The worst case per record is bounded: 1 bucket plus 1 session (about 110 × (1 + R)). So children fit whenever the cap is at least that; below it nothing can fit and routing must stay off.
     - **Older Desktops** (v2026.10.27, desktop-v2026.10.65/.66): their handling of a 422 is unverified, so they must be on a release with isolation, or routing must stay off for them.
     - Optionally, the destination can publish a lower `limits.maxRecords` in receipts so that *future* batches are prepared smaller. A prepared batch is never resized.
   - **Wide batches are never collapsed.** Every (source, session, reset epoch, hour) keeps its own bucket, so the per-session deletion fence stays immediate.
   - **Replica:** routing requires the Neon replica `off`, because its consumer rebuilds rows from lines. With a replica on, routing is inactive and logs a warning.
   - **Deliberate exception:** this is the only object family the buffered Worker writes outside the stage. The indexer skips it, and it is outside `upload-generations/`.
2. **SQL lane** (`supabase/migrations/20261105042000_babysitter_evidence_retention.sql`).
   - Owner-only settings (`enabled=false, dry_run=true`; `configure(jsonb)` is audited), a metadata-only bucket registry, and an append-only audit (trigger) that holds bucket segments, never S3 keys.
   - **Roles.** `register` belongs to the ingest login. `claim`, `fence` and `complete` belong to a dedicated **least-privilege role, `bulls96_babysitter_retention`**: provisioned NOLOGIN by `supabase/bootstrap/session_ingest_roles.sql`, verified by the migration (42704 if absent, 55000 if elevated), EXECUTE on those three RPCs and **nothing else** (no ingest, register, reset or commit RPC, no table), and made LOGIN only by the operator step `supabase/bootstrap/babysitter_retention_role.sql`. Its verifier is `supabase/bootstrap/babysitter_retention_verify.sql` (generated).
   - RPCs:
     - `register`: the machine is derived in SQL; the ref must name its hour, session and reset epoch; the epoch must still be current (otherwise 40001, retry); it is **refused while the bucket is leased**; a late write re-opens an erased bucket.
     - `claim`: needs `enabled` and the generation, takes 1..200 buckets with SKIP LOCKED and a 5-minute lease, and returns `leaseUntil`.
     - `written` (ingest role): the writer's definite PUT outcome per invocation (acked with its version id, or refused); never retires anything.
    - `obligation` (retention role, leased bucket, current generation): the sweeper's ObligationStore; every mutation is a compare-and-set on the key row version.
    - `complete`: `erased` | `progress` | `released`. `erased` and `progress` are refused while disabled or in dry run. `erased` needs `write_seq` unchanged since the claim AND no write obligation left (55000 otherwise), and it deletes the bucket's upsert key-index rows (tombstones are permanent). `progress` persists the content-free resume checkpoint (or NULL, to restart), which must name this bucket's directory, and the bucket stays pending. `released` keeps the last checkpoint. A registration clears the checkpoint.
   - **Due**, always after a **15-minute hold-off** since the last registration (a race narrower, NOT a proof that no PUT can still commit: the write obligations are the proof), and never while one of its obligations is backing off. The hold-off also delays a deletion or reset fence: a bucket is erased at most about 25 minutes after the fence (15 minutes plus the 10-minute sweep cadence), comparable to E3's own session erasure cadence.
     - the expiry hour has passed; or
     - the session is deleted; or
     - the user reset after the bucket's epoch.
   - **Rollback** refuses while **any** bucket is registered, in any state, and while any erase is recorded. So only a never-used install can be rolled back.
3. **Sweeper: a phase of the existing E3 erasure process** (`src/migration/neon-s3-erasure/babysitter-phase.ts`, hooked into `neon-s3-erasure/main.ts`; `BABYSITTER_RETENTION_SWEEP=off|dry_run|on`, default `off`).
   - **No new Lambda, process, S3 key or IAM binding.** It runs beside the erasure job and shares its eraser, `--execute` gate and stop signal. Without `--execute`, an `on` phase is a dry run.
   - **A new consuming credential binding with write authority: NOT covered by the earlier no-new-binding note.** It is default OFF and needs its own live credential-consumer decision before use. Nothing is provisioned by this change. E3's own read-only session and grants are untouched.
     - **Input:** `retentionDatabaseUrl` for the least-privilege role `bulls96_babysitter_retention`. Its write authority is lease, audit, obligation and permanent-tombstone purge rows only, through the four RPCs (claim, fence, obligation, complete).
     - **Lifetime:** read once, from the first stdin line, at process start, and only when the phase is on **and** the process runs with `--execute`. It is parsed and handed to one `pg.Pool` (max 2), and the field is deleted from the parsed object. It is held only in that pool's memory until shutdown (`pool.end()` in the process's `finally`). It is never logged, written, or put in argv or env.
     - **Role pin:** the URL user must be `bulls96_babysitter_retention`; the ingest login is refused. The host must be the session pooler with the pinned CA. The connected `current_user` is checked before any other call.
     - **Fixed call allowlist** (`ALLOWED_SQL`, enforced on every statement; anything else is refused unsent):
       - `SELECT current_user::text AS c` (no arguments);
       - `SELECT sessions.ingest_babysitter_retention_claim($1::jsonb) AS result` with `{generation, limit 1..200}`;
       - `SELECT sessions.ingest_babysitter_retention_fence($1::jsonb) AS result` with `{generation, leaseToken}` (re-checked before every page's deletes and before the proof);
       - `SELECT sessions.ingest_babysitter_retention_complete($1::jsonb) AS result` with `{generation, leaseToken, outcome: erased|progress|released, versions, checkpoint}`.
       - `SELECT sessions.ingest_babysitter_retention_obligation($1::jsonb) AS result` with `{generation, leaseToken, action, object, rev, invocations, versionId, checkpoint}`.
       The role itself can EXECUTE only those four functions (claim, fence, obligation, complete).
     - **Bounds:** statement and lock timeout 1.8 s, connect 1 s, pool max 2, claim 1..200, tick budget 45 s, lease 5 minutes (an erase starts with at least 2 minutes left and is cut at the lease end less 1 minute), interval at least 60 s.
     - **Error sanitization:** codes only (SQLSTATE class or fixed names). No message, key, URL or id is logged.
     - **Shutdown:** SIGINT and SIGTERM set the process's stop flag. The loop finishes its current tick, the sleep wakes within 1 s, and the pool is ended.
     - **Dry run / no `--execute`:** without `--execute` the phase is **off**: no stdin read, no connection, no database or S3 write. `BABYSITTER_RETENTION_SWEEP=dry_run` with `--execute` claims and releases (lease and audit rows only) and never calls the eraser.
   - A bounded tick every 10 minutes. It erases a bucket only with at least 2 minutes of lease left, and only the bucket's own directory `…/h=_babysitter/d=<date from the ref>/s=<bucket>/` (never a machine-wide listing). The erase is cut off at the lease end less a 1-minute margin, then released and re-claimed later; deletion by version id is idempotent.
   - **Durable, resumable, strict erase** through the ONE shared primitive in `src/migration/neon-s3-erasure/eraser.ts`: `eraseStep` and `verifyErased`, which F1 and the E3 job compose identically.
     - **Each run is bounded:** at most 10,000 versions and 100 pages, cut at the lease end less half the margin, with a hard timeout.
     - **Every page is strict:** complete Key/VersionId/IsLatest, both pagination markers equal to the last entry, every key under the bucket directory, no duplicates, monotonic cursor, cycle detection. Nothing is deleted from an ambiguous page.
     - **The fence RPC runs before every page's deletes.** Deletes are exact key+version only.
     - **A run that ends on its budget** persists a content-free checkpoint through `complete(progress)`, and the bucket stays pending. The checkpoint is bound to the bucket, the generation, both epochs and `write_seq`. Anything else (a new registration, a generation, a stale scope) is refused and the run restarts from the top.
     - **Resume points:** a run resumes at a completed-key boundary, so a deleted marker version cannot strand the cursor. A provider 400 on an in-key marker restarts at that boundary.
     - **Erased needs proof.** A bucket is `erased` only after a full traversal, a fresh strict zero-version inventory under the fence, `write_seq` unchanged, AND every write obligation of the bucket complete (`write-obligation.ts`):
       - an invocation resolves only on its OWN terminal evidence recorded by its writer: refused, or acked with its own version id and that exact version absent from a fresh strict complete inventory (per key, in exact-key mode);
       - before ANY delete, every doomed version of a key with an obligation is RESERVED (`babysitter_write_reservations`, a compare-and-set under the key row's lock, `ObligationStore.reserve`), so a publisher and the erase can never both win; a late commit is erased as cleanup but resolves nothing;
       - a dispatched (unknown) invocation stays **UNRESOLVED indefinitely** (lead ruling, shared-unknown-put-nonterminal): the bucket stays tracked and is re-swept on a bounded backoff (1, 2, 4 ... minutes, capped at a day, so a stuck key costs one listing a day). It is never marked erased by a timeout, a hold-off, `write_seq` or any number of zero inventories, and there is no credential-rotation or bucket-policy shortcut.
       Versions found behind the cursor restart the traversal; nothing is erased-by-assumption.
     - **Crash or lost acknowledgement** at any step reuses the last persisted checkpoint; deletes by version are idempotent.
   - It logs counts and codes only.
   - **Erase/write fence:** a PUT happens only inside its own request, right after its registration (which records its invocation). Registration is refused while a lease holds, a bucket is claimable only 15 minutes after its last registration, and no erase starts near the lease end. These narrow the races; they do NOT prove a dispatched PUT cannot commit later, which is why the obligations above gate `erased`.

## The strict shared eraser and the existing E3 job (lead ruling C3)

The canonical eraser's strict inventory validation and bounded legacy listing ship **unflagged**, as destructive-operation safety hardening. They apply to the existing E3 calls **even with every Babysitter flag off**; see `runbooks/neon-s3-sync.md` ("Strict inventory: fail closed"). A >10k whole-inventory workload is REFUSED, not erased: that is accepted as safety, and it does not extend erasure coverage or the SLA.

## Operator order

Each step is the production lead's, with Khaliq's OK for the window, deploys and flips. No step is authorized by this document.

0. **Read-only checks.**
   - The census of Babysitter upserts already in mixed chunks.
   - A bounded observation of the Neon cron (counts and codes only). The failing-cron finding is a **hypothesis** until observed.
1. **Land in slot order:** #288 (040000), then #289 (041000), then this change (042000), dormant (both flags off, settings disabled). This change rebases onto #289. The E3 hook in `neon-s3-erasure/main.ts` composes with #289's stdin reader by adding `retentionDatabaseUrl` to its optional fields. If this change is ready first, it gets renumbered rather than landed out of order.
2. **Schema-generation ceremony, in a window:**
   1. regenerate the boundary (`bun scripts/sync-sessions-function-boundary.ts --check` clean);
   2. take the new compiled generation from `derivedSchemaAssets()`;
   3. apply 040000, 041000 and 042000 through the migrator (readers stay non-ready while the marker is `pending`);
   4. the role-validation PASS for the new generation;
   5. `ingest-generation-rebind`;
   6. redeploy the ingest Worker, buffered GC and publication with the new `INGEST_EXPECT_GENERATION`;
   7. set the same value for the E3 phase.
   A stale generation is refused by `claim` and `complete` (55000).
3. **HOLD:** first prove the effective deployed subrequest cap and set `INGEST_SUBREQUEST_CAP` (and `INGEST_SUBREQUEST_REDIRECTS` only if R is proven lower), and confirm the deployed Desktops isolate a 422. Then ingest `INGEST_BABYSITTER_RETENTION_ROUTING=on`. **Stop point:** new Babysitter upserts appear as one object per bucket with registry rows, the session and stage copies are stubs, and receipt and replay rates are unchanged.
4. **Separate live credential-consumer decision** for the E3 retention login, then the operator steps `babysitter_retention_role.sql` and `babysitter_retention_verify.sql`. Then E3 phase `dry_run` (with `--execute`), plus `configure({"enabled":true,"dryRun":true})`. **Stop point:** the audit lists exactly the expected due buckets.
5. E3 phase `on` (with `--execute`), plus `configure({"enabled":true,"dryRun":false})`. **Stop point:** buckets are `erased`, the audit reconciles, and an S3 version listing of an erased bucket is empty. Buckets with an UNRESOLVED (unknown) write stay pending by design, re-swept at most daily; report their count and age (counts only), never "fully erased" while any remain.
6. Backlog cleanup, if step 0 finds any. That is a separate owner, review and OK; this change grants none.
7. **Then**, and only once steps 3 to 5 are live and verified, RHC `NEON_JOB_BABYSITTER_EXPIRY=off`. A failing old job does not waive this replacement-before-disable gate.

## Reader: Neon read parity (source only, default off)

**Where:** `GET /v1/delivery/records` for `kind=session_lineage|turn_receipt`, inside the **existing** read API.
- Code: `src/session-read-api/babysitter-evidence.ts`, a route in `app.ts`, deps in `packages/session-read-worker` (`READ_API_BABYSITTER_EVIDENCE=on`).
- It reuses the worker's read-role connection and its `PIPELINE_RAW_S3_*` store: no new service, host or credential.
- Without the flag it answers 501. Other kinds also answer 501: this backend serves only the Babysitter kinds.

**Contract** (Neon's `listDelivery`):
- `X-RelayHistory-Expected-Account` is checked FIRST, before any database or S3 access, against the verified token's account (`relayhistory:` + sha256 of `[orgId, workspaceId]`, Neon's `deliveryAccount`): a missing or mismatched header is Neon's **403 `delivery_account_mismatch`**, "Delivery account does not match authenticated tenant";
- filters `kind`, `source`, `session_id`, `include_deleted`, `cursor`, `limit` (1..100); unknown or repeated filters are 400;
- **Neon's native order:** (origin_id, record_id) in C collation, with the cursor position (origin_id, record_id) bound to the filter scope;
- **bounded page work, no materialization:**
  - one SQL page over the content-free key index (`babysitter_evidence_page_for`, index range scan): limit+1 rows, at most 1,000 records scanned;
  - then exact-key GETs of only the objects the page's live rows point at (at most 32 distinct; no listing at all);
  - any bound that cuts a page returns a cursor at the last record decided, so it resumes there and never claims completeness;
- the 3 s deadline reaches every GET; backend faults (including an indexed line that is not exactly that revision) are 503 `backend_unavailable` with no message;
- envelope `{protocolVersion: 1, listing: "live", records, nextCursor}` and Neon's record shape.

**Visibility:**

| Rule | Behaviour |
|---|---|
| Tenant | Org and workspace come from the verified token. The page RPC (`babysitter_evidence_page_for`) runs as `bulls96_read_api` under the read API's binding (`session_directory_read_api_scope`); a missing or foreign binding is refused (42501). |
| Expiry | `payload.expires_at_ms <= now` is unreadable. `include_deleted` returns a payload-free tombstone, as Neon's read-only predicate does. A bucket past its erase hour is not read at all, so nothing is readable after 30 days. |
| Delete and reset (logical delete) | A bucket whose session fence epoch moved (any disable since its claim) or whose user reset is hidden **at once**, before any physical erase, and **stays hidden after a re-enable**. Erased buckets are never read. |
| Accepted body, supersession and deletes | Per record, its **newest ACCEPTED candidate** decides, as Neon's one row per record does: an ACK-refused equal-revision changed body or delete is never served and never hides the accepted body's original line. A newer committed revision (live or delete, indexed or not) wins. A latest accepted **delete** is Neon's tombstone (operation `delete`, payload null, the delete's revision id and revision), returned **exactly once** under `include_deleted` and hidden otherwise. |
| Tombstone lifetime | Permanent, independent of every payload's lifetime (beyond day 30 and after the physical erase): an offline client resuming with an old cursor still gets the delete. Only a privacy fence (disable/reset) removes it, at once. |

## Re-enable safety (no loss, no resurrection)

- **Bucket identity binds the claim's epochs.** It includes the session **fence epoch** taken from the claim attempt's snapshot (`ingest_claim_attempts.epochs`), plus the reset epoch. The fence epoch increments on every disable and survives re-enable (`ingest_fence_versions`).
- **SQL issues the ref** and refuses (40001) a registration whose claimed session is fenced now. That includes a disable followed by a re-enable since the claim. So a pre-disable write never lands under a post-enable key.
- **A fenced bucket stays fenced** (hidden and erasable) after a re-enable. New writes after the re-enable go to a **new** bucket that stays live.
- **The erase is a paged, strict, resumable directory erase** (`eraser.ts` `eraseStep` / `verifyErased`, the shared 9fef0acd tuple):
  1. each bounded run lists the bucket's own directory in strict pages; the **erase fence** (`ingest_babysitter_retention_fence`: the lease is still held, with 30 s to spare, under the **current schema generation**) is re-checked before EVERY page's deletes;
  2. every doomed version of a key with an obligation is reserved first, then exactly the listed versions are deleted by version id;
  3. a run that ends on its budget persists a checkpoint; `erased` needs a fresh strict zero inventory AND every write obligation complete.
  Registration is refused while the lease holds; a bucket registered again after the erase reopens and is swept again.
- Tests: disable → logical delete → re-enable (still hidden) → sweep → new writes kept and readable, pre-disable erased; a claim taken before a disable refused even after re-enable; a lost PUT response keeps the bucket tracked and a late commit is erased as cleanup.

## Generation fence

Every evidence write (`register`) and every erase (`claim`, the erase `fence` before any delete, `complete`) requires the current compiled schema generation. A stale generation is refused even with a valid lease (55000), and the whole tick is refused at its claim. Routing is inactive without `INGEST_EXPECT_GENERATION`.

**Rollback:**
- **Operational:** set `configure({"enabled":false,"dryRun":true})` and turn both flags off. The registry and audit are kept.
- **Schema rollback** (`supabase/rollbacks/20261105042000_babysitter_evidence_retention.down.sql`): refused while **any** bucket is registered (pending, leased or erased) or any erase is in the audit, and while a later migration is applied. A registered bucket is an S3 object only this registry knows to erase.
- **The NOLOGIN role `bulls96_babysitter_retention` is kept** by the rollback, as every lane keeps its cluster roles (033000). Roles are cluster-wide, they are provisioned by the role bootstrap rather than by this migration, and the next migrate reapplies against the same role. With 042000 gone it can execute nothing. 042000 refuses a pre-existing role that can already log in (55000); LOGIN comes only from the operator step.

## Decisions and open items

- **Final Neon snapshot:** decided. Khaliq said "sure" to capping its retention at **31 days**.
- **Backlog (open):** lines written before routing are in mixed chunks (canonical session chunks and canonical generations). They need a census first and, if any exist, an owner-run chunk rewrite.
