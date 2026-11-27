# Caller matrix for the per-org admission hold

Companion to [org-admission-hold.md](org-admission-hold.md). Latest function bodies by migration filename order
(a later CREATE OR REPLACE supersedes an earlier body); paths under `supabase/migrations/` unless stated. Built
2026-11-26 against main 2bfa8c32; re-verify line numbers against the tree the migration lands on.

Lock shorthand:
- **50000S / 50000X**: `pg_advisory_xact_lock_shared` / `pg_advisory_xact_lock(961996,50000)`. Every
  `ingest_lock_fences` call takes 50000S, because it first INSERTs the `ingest_fence_owners` row and the statement trigger
  `ingest_erase_fence_index_lock` (050010:63-72) takes the lock. That trigger raises 40001 under a non-read-committed
  transaction once any erase has run.
- **LF**: `ingest_lock_fences` (062010:137). It INSERTs the fence_owners row, takes it FOR UPDATE (FOR SHARE only under
  an origin-claim mark), then makes the RH041 erase check. Per (org, ws, user), not per org.
- **LG**: `ingest_lazy_guard` (044030:312). Takes the readiness marker FOR SHARE, then `ingest_lazy_policy` FOR SHARE,
  reads the seat clock, takes the overlap window FOR SHARE (optional), then the `ingest_buffer_admission` row of the org
  FOR SHARE.
- **RBA**: `ingest_require_buffer_admission` (055000:514). Under a lazy policy this is LG. Otherwise it takes the
  marker FOR SHARE and the admission row FOR SHARE (523-524), then runs `ingest_require_custody` (055000:530).

## A. Inserters of `sessions.ingest_batches`

Only two live functions INSERT. The superseded bodies are 022000:399, 030000:116, 050000:85, 055000:1081 and
043850:882. There is no seed inserter: `ingest_seed_records` (040000:264) always raises 42501. The rollbacks
`supabase/rollbacks/20261105043850…down.sql:213` and `…050010…down.sql:196` would reinstate INSERT bodies.

| Entry point (latest) | Security | Runtime EXECUTE | Path to the INSERT, locks in order |
|---|---|---|---|
| `ingest_claim_batch_mode` 050010:117 (INSERT at 303) | invoker | owner only | 1. Refuses the canonical mode when a lazy policy exists (144), or when the org has an admission row or buffered batches (162-166).<br>2. LF (167).<br>3. RBA, buffered only (196).<br>4. `ingest_erase_fence_extend` (200).<br>5. The INSERT ON CONFLICT DO NOTHING (303), which fires the AFTER INSERT triggers `ingest_receipt_seat_ack` and `ingest_seed_close_batch`.<br>6. `ingest_acquisition_lock` (343).<br>7. INSERTs `ingest_session_ownership` (346) and `ingest_claim_attempts` (365). |
| `ingest_claim_buffered_batch` 043850:1130 | definer | yes | Under a lazy policy:<br>1. `ingest_lazy_begin` (1139): marker, policy, admission INSERT and FOR SHARE, then `ingest_lazy_user` (50000S, LF).<br>2. `ingest_lazy_records` (1142/1144): requires read committed (043500:39), then LF, clock FOR KEY SHARE, the native-origin advisory, slots FOR UPDATE.<br>Then, in every mode: `ingest_require_generation` (1151), RBA (1152), `claim_batch_mode` buffered (1153), RBA (1164), `ingest_lazy_ack` (LG). |
| `ingest_origin_claim_buffered_batch` 062010:105 | definer | yes | The origin advisory `'96bulls:ingest-origin-claim-v1'` (org, ws, user, origin) first (062010:87), plus a mark row; then `ingest_claim_buffered_batch`. |
| `ingest_claim_batch` 050000:306 | definer | yes | `claim_batch_mode(p,'canonical')`. No RBA or LG. |

## B. Writers of `sessions.ingest_buffer_admission`

| Writer | Location | Scope | Locks |
|---|---|---|---|
| `ingest_lazy_begin` bootstrap | 041000:893, FOR SHARE at 896-898 | per org | Marker FOR SHARE (834), policy FOR SHARE (835), INSERT ON CONFLICT DO NOTHING, then FOR SHARE. On a new row it scans every `ingest_%`/`upload_%` table that has `org_id` (901-912). Reached from claims (043850:1139) and from ordered controls (055000:955). |
| Renewal `src/hoopdb/ingest-admission-renewal-operator.ts` | :503 | bulk, every org in one statement | (961996,1) X (446), marker FOR SHARE, policy FOR UPDATE, LOCK TABLE SHARE ROW EXCLUSIVE (349), all rows FOR UPDATE. A CAS UPDATE with an asserted row count, then LG for every org (521). |
| Repair `src/hoopdb/ingest-org-admission-repair-operator.ts` | :514 | org list, one transaction | (961996,1) (462), marker, policy, SHARE ROW EXCLUSIVE (345), INSERT, then LG per org (539). Requires the live `lazy_begin` scan text verbatim (347). |
| Rebind `src/hoopdb/ingest-generation-rebind.ts` | :65 | bulk | (961996,1) (31), marker FOR SHARE, overlap window FOR UPDATE, policy FOR UPDATE. Zero rows away from `to` required (67). |
| Cutover ceremony `scripts/ops/cutover-20261105/cutover-ceremony.ts` | :210, :130 | bulk loop | none |

## C. Readers that gate on the admission row

| Guard | Callers |
|---|---|
| LG 044030:312 | 1. RBA and `ingest_require_custody` under a lazy policy.<br>2. `ingest_lazy_ack_batch` 062210:197.<br>3. `ingest_lazy_attempt` 055000:583, `ingest_lazy_ack` 055000:556, `ingest_lazy_stage_seats` 055000:702.<br>4. Renewal and repair postconditions. |
| RBA 055000:514 | 1. Claim: 043850:1152,1164; 050010:196.<br>2. Stage: 043850:1182,1255,1264.<br>3. Commit: 050010:383,416,434.<br>4. ACK trigger: 055000:1326.<br>5. Ordered controls: 055000:981,1033.<br>6. Publication: 043850:1449,1463,1573; 059000:193; 060000:340,122.<br>7. History plans: 026000:429,484,500. |
| `ingest_receipt_seat_initialize` 055000:443 | owner-only; ceremony |
| Direct FOR SHARE | `lazy_begin` 041000:896; ordered control 055000:982-983; seat-ack 055000:1327-1328; custody 054000:117,155,159,256,259,528 |

Canonical `ingest_commit_batch` (050010:441), `ingest_commit_revisions` (060000:907) and `ingest_fanout_fence`
(060000:882) call no admission guard.

## D. Content writers that insert no batch row

| Writer | Table | New admitted content? | Guards |
|---|---|---|---|
| `ingest_lazy_ack_batch` 062210:190 (from the seat-ack trigger on commit) | slots, seats, decisions | yes, for an existing claim | LG, LF, clock, native-origin advisory |
| `ingest_commit_buffered_batch` 050010:373 (+ 062010:125) | outbox, stage, batch state | yes, existing claim | RBA, LF, `ingest_lazy_attempt` |
| `ingest_prepare_buffer_stage` 043850:1172 | stage intents | existing claim | RBA, LF |
| `ingest_lazy_records` 043500:28 | slots | inside a claim (or a resume) | caller's `lazy_begin` |
| `ingest_commit_batch` 050010:441 | revisions | yes, existing canonical claim | LF only |
| `ingest_commit_revisions` 060000:907 | revisions | yes, given a valid canonical claim token | LF only |
| `ingest_publication_publish` 059000:78 | revisions, `session_index`, jobs | publishes accepted content (catch-up) | RBA |
| `ingest_seed_proven_records` 040000:280 | revisions, ownership | imports proven S3 content | LF, capture, marker; no admission check |
| `ingest_buffer_gc_prepare`/`_swap` 055000:1652/1702 | stage, outbox | no (redaction) | 50000S, GC advisory |

Removal-only writers: `ingest_apply_selection` / `ingest_apply_reset` (030000:395/435), `ingest_lazy_clear`
(055000:733), `operator_user_erase_step` / `_purge` (058010:553/421) and `ingest_turn_materialize_fence`
(062020:63).

## E. Controls

None of these inserts batch rows; the user-erase purge deletes them.

| Control | Latest | Writes |
|---|---|---|
| Selection | 062060:36/11, 055000:946, 030000:395 | fence versions, deletions, receipts. Deletes revisions and ownership. May INSERT an admission row via `lazy_begin`. RBA. |
| Reset | 055000:1063, 030000:435 | resets, fence versions, deletions. Same caveat as selection. |
| Self-host erase | 062060:48 | selection, plus directory and index tombstones |
| Consent | 057010:103 | `upload_consent_*`, advisory `'upload_consent'` |
| Privacy authority, source control | 058010:826/893, 041000:510/598 | heads and frozen source tables. 50000S, (1919249529,96) S, read committed |
| Custody | 054000:80/141/244/487 | custody tables (owner-only) |
| Operator session erase | 050000:334 | 50000X; erasure fences, receipts, deletions |
| Operator user erase | 058010:263/482/617 | 50000X, 96 S; purge deletes (batches included) |
| Buffered GC | 058010:668, 055000:1652/1702, 051000:303 | stage intents, outbox |

## F. Drain, backfill, export

- `src/session-ingest/buffered-drain.ts` runs no SQL; it reads and writes S3 only.
- The fence backfill reads SQL in REPEATABLE READ READ ONLY pages (`scripts/upload-fence-backfill.ts:618,647`) and
  writes S3 only.
- `src/session-ingest/accepted-stage-export.ts` reads SQL read-only (587) and writes S3 sidecars only.

## G. Advisory keys in use

**Two-int:**
- (961996,1): migrator, renewal, repair, rebind, overlap
- (961996,2): history migrator
- (961996,50000): erase and writer gate
- (961996,58000): index prebuild
- (1919249529,96): privacy authority
- (1919249529,97): handoff

**Single-bigint `hashtextextended` namespaces:**
- `'96bulls:'` + `ingest-origin-claim-v1`, `lazy-native-origin-v1`, `buffer-gc-user-v1`, `acquisition*`,
  `publication-pointer`, `directory-events`, `operator-write`, `canonical-feature-journal`, `bookkeeping-*`,
  `relayfacts-user-erase-v1`, `search`
- `'upload_consent'`, `'session-analysis-*'`, `'output-facts-bootstrap'`, `'search-consumer-bootstrap'`

The only existing per-org advisory key is `'repo-catalog:'||org` (025000100:235), which is unrelated. The stable
per-org row every buffered admission takes FOR SHARE is the org's `ingest_buffer_admission` row, but publication and
controls take it too.

## Risks the design addresses

1. **Renewal and rebind are all-or-nothing bulk statements.** A hold that touches admission rows, or that raises for
   one org inside them, aborts them for every org. The hold never touches admission rows.
2. **RBA and LG are shared with publication, history plans and ordered controls.** The hold has its own check, run from
   the `ingest_batches` triggers.
3. **Canonical claim and commit paths have no admission guard.** The INSERT trigger covers the claim; the commits are
   open item 1.
4. **A new `ingest_%` table with `org_id` would enter the `lazy_begin` and `lazy_user` scans and the repair scan
   hash.** The hold table uses `held_org` and has no `workspace_id` or `user_id`.
5. **Committing an existing claim admits content without a new batch row.** Install requires zero pending, and the
   pending -> committed transition is refused under a hold.
6. **`ingest_lazy_records` locks before the claim's RBA.** The trigger takes the org key last, and install holds
   nothing a claim needs, so there is no cycle.
7. **Seeding has no admission check.** Open item 2.
8. **Isolation.** `_check` requires read committed (40001 otherwise), as every admission path already runs.
