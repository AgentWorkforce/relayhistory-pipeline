# P1: frozen-source custody (Neon off the upload path)

Design and evidence: `~/relay-briefs/migration-fast-20261103/p1-custody-supabase-design-20261105.md`.

## What it is
Since the 2026-11-05 cutover (relayhistory 0078 phase `active`), Neon's legacy delivery tables are frozen. Migration `20261105010000_frozen_source_custody.sql` adds:
- **A metadata-only copy** of the witness inputs (`ingest_frozen_source_records`, `_deletions`, `_resets`), owner-only. A per-org seal and a completeness seal bind it to the live policy's source identity, epoch and privacy baseline.
- **`ingest_source_control_heads`**, a Supabase-first mirror of Neon's privacy-control heads. It uses the same digest/version/PRE algorithm as `begin/complete_delivery_cutover_control_v1`.
- **`ingest_frozen_source_witness(jsonb)`.** It returns the SAME `{witness, sourceDigest}` packet relayhistory builds against Neon. Packet-shaping orders use `COLLATE "C"`, because Neon is C.UTF-8 and Supabase is ICU en-US. It refuses (55000) while unsealed, while a batch's user has a pending control, or on any invalid row.
- **`ingest_source_control_begin/complete(jsonb)`** for relayhistory: begin here BEFORE Neon begins, complete here AFTER Neon completes.

The ingest Worker reads `INGEST_CUSTODY_SOURCE` (stack input `sessionIngest.custodySource`):
- unset/`relayhistory` (today): only the relayhistory Neon witness;
- `shadow`: also builds the frozen witness and logs `custody.shadow` match/mismatch with the differing top-level fields. The answer never changes;
- `frozen-supabase`: when relayhistory sends no witness, the Worker builds it. relayhistory then stops calling Neon on uploads.

## The single generation window (all through bulls-migrator, one operator)
The new migration changes `generation()`. That generation is pinned in the ingest Worker (`INGEST_EXPECT_GENERATION`), the buffered GC (`INGEST_GC_EXPECT_GENERATION`), `ingest_lazy_policy.generation` and every `ingest_buffer_admission.schema_generation`. Uploads refuse until all of them match. In one window:
1. **Owner-apply** the release (schema-release owner-apply path). Readiness goes PASS for the new generation G2. From here uploads refuse, generation not ready, until step 3.
2. **Redeploy** `96bulls-session-ingest` with `INGEST_EXPECT_GENERATION=G2` (private-ingest operator) AND the buffered GC Lambda with `INGEST_GC_EXPECT_GENERATION=G2`.
3. **Rebind**: `bun scripts/ingest-generation-rebind.ts --from G1 --to G2` (dry run, then `--execute`). That's one transaction under the migrator lock: the policy via `ingest_lazy_policy_set` (identity unchanged), then every admission row. It's idempotent.
4. **Verify** uploads reopen (2xx rate back), and no `generation not ready`.

The whole sequence is rehearsed in `frozen-custody.pglite.test.ts` ("rehearsed generation window").

## Then
1. **Copy and seal**, outside the window. Uploads keep working because frozen mode is not yet on: `--plan`, then `--copy --concurrency 2` (resumable from the owner-only progress table), then `--seal` (see "Copy and seal operator" below).
2. **Shadow**: deploy the ingest Worker with `custodySource: shadow`. Require `custody.shadow` outcome `match` on live batches and controls before going further.
3. **Mirror** (relayhistory PR): call `/v1/custody/control/begin` before Neon begin, require an equal `{version,digest}`, and call `/v1/custody/control/complete` after Neon complete. This must be live before any frozen flip. Shadow mismatches on `sourceControlVersion`/`pending` mean it isn't.
4. **Flip**: deploy the ingest Worker with `custodySource: frozen-supabase`, then relayhistory `DELIVERY_CUSTODY_SOURCE_MODE=frozen-supabase` (skip the Neon witness/pending/status calls on batches).
5. **Prove**: the runtime role's Neon upload-path statements reach zero (pg_stat_statements delta), and the upload 2xx rate is unchanged or better.

**Rollback**: unset the relayhistory flag. It goes back to sending the Neon witness, which the Worker accepts in every mode.

## Copy and seal operator (`scripts/frozen-source-copy.ts`)
No production run of this operator is authorized by this document or by the PR that added it. Running `--copy` or `--seal` against the real target, and applying the progress-table migration below, each need Khaliq's separate, explicit OK.

### Credentials: one stdin document, direct owner only
The script reads ONE JSON document from stdin, which must be a pipe (never a terminal, a file, an environment variable or a CLI flag):
`{"neonOwnerUrl": "<neon owner url>", "supabaseOwnerUrl": "<supabase direct owner url>"}`. `--plan` needs only `neonOwnerUrl`.

- `supabaseOwnerUrl` is the existing direct schema-owner login (`postgres.<project>` through the pinned session pooler). `src/migration/frozen-source-copy-seal.ts` checks it against the fixed schema-release pins before any client exists: host, port 5432, database `postgres`, the owner login, a non-empty password, and nothing beyond `sslmode=require|verify-full` / `channel_binding=require`.
- The client connects with the pinned CA, `rejectUnauthorized` and `servername`, then requires `current_user = session_user =` the owner (inside the seal transaction too).
- There is no `SET ROLE`, no credential minting or refresh, no Supabase CLI temporary login, and no file or environment fallback. The password is never logged or written into the receipt.

The operator produces the document in their own shell; nothing in the repository reads 1Password:

```sh
jq -n --arg neon "$(op read 'op://<vault>/<neon-owner-item>/<field>')" \
      --arg supa "$(op read 'op://<vault>/<supabase-owner-item>/<field>')" \
      '{neonOwnerUrl: $neon, supabaseOwnerUrl: $supa}' \
  | bun scripts/frozen-source-copy.ts --copy --concurrency 2 --receipt /tmp/<new-receipt-file>.json
```

The `op://` references above are placeholders. Do not write the document or either URL to disk.

### Modes
- `--plan`: read-only Neon counts per org.
- `--copy [--org X] [--concurrency 2] [--page 5000] [--max-page-ms 4000] [--budget-ms N] [--receipt PATH]`: resumable, throttled copy of the records and receipts, plus the privacy rows (deletions, resets) and control heads.
- `--seal [--budget-ms N] [--receipt PATH]`: requires a complete checkpoint for every org and dataset under this run's binding, verifies every org on both sides, then seals inside the fence. `--org` is refused with `--seal`, so a subset never becomes the global completeness seal.
- `--repair-head`: unchanged (see `src/hoopdb/frozen-control-head-repair.ts`).

### What the seal proves, and what it does not
A completeness seal (`ingest_frozen_source_completeness`, manifest `frozen-source-manifest-v2`) and its org seals prove that, at one fenced instant:
- every org's **records** and **receipts** in the Supabase copy equal Neon's (count plus an order-independent digest), with no Supabase org that Neon does not name;
- the **deletions**, the **resets** and the **control heads** equal Neon's. Resets are compared row by row on the key `org/workspace/user/request_id` and the value `deleted`; a missing, an extra and a changed reset row each refuse with their own code. The manifest and the org seals' privacy digests include the resets;
- the live Supabase policy's epoch and privacy baseline equal the Neon cutover descriptor, and its epoch, privacy baseline and source identity equal the pre-verification read (no rebind, re-epoch or new source identity mid-seal). The copy itself is bound further: `--seal` requires every checkpoint to be complete under this run's binding (generation, source identity, epoch, privacy baseline, operator SHA).

It does **not** prove:
- that the privacy baseline is correct. The baseline is compared for **equality** with the Neon descriptor; the sealer never recomputes it (the snapshot records `privacyBaseline: "equal to the Neon cutover descriptor; not recomputed"`);
- anything about Neon writes after the fenced snapshot. Neon is frozen by relayhistory phase `active`; the seal relies on that freeze and checks the phase inside the fence;
- the content of record columns outside the canonical row text (`RECORD_COLS`).

### Seal fence (convergence argument)
The fence is one Supabase `SERIALIZABLE` transaction, refusing rather than retrying:
1. The sealer takes the privacy-authority advisory key EXCLUSIVELY at session level. Every writer of the mirror heads and the frozen privacy/record tables takes the same key SHARED per statement (guard trigger of `20261105041000`), so this waits for in-flight writers and blocks new ones; `lock_timeout` refuses (`frozen_copy_fence_busy`).
2. Only then `BEGIN ISOLATION LEVEL SERIALIZABLE`: its snapshot is taken after the lock, so it sees every committed writer and no writer can commit inside it.
3. Inside it: owner identity, guard-trigger presence on every fenced table with exactly this key, authority not `active`.
4. Neon `REPEATABLE READ READ ONLY`: phase `active`, privacy rows and control heads in one snapshot.
5. Supabase privacy rows, control heads and policy in the same transaction. Epoch and privacy baseline are checked here against the Neon descriptor; epoch, privacy baseline and source identity against the pre-read.
6. ANY head difference (Supabase vs Neon, or either side vs the pre-verification read) refuses `frozen_copy_mirror_advanced`; privacy movement refuses `frozen_copy_privacy_advanced`; a parity difference refuses with its own code.
7. A serialization failure (40001/40P01) refuses `frozen_copy_serialization_failure`, with no retry.
8. Every refusal or error rolls back both transactions, releases the key and exits non-zero.
9. So the seal rows exist only if both sides were equal at one instant at which no guarded writer could move either side.
10. A COMMIT with no server answer is reported as `UNKNOWN_COMMIT` and reconciled by reading the completeness row, never retried blindly.

**Rollback = no seal row.** A refused or aborted seal writes nothing: no org seal and no completeness row. Rerun `--seal` after fixing the cause. An existing completeness row is immutable evidence; a rerun with an identical manifest reports `inserted: false`, and a different manifest refuses `frozen_copy_completeness_conflict`.

### Progress table (owner-only, target Supabase; follow-up migration)
The ONLY copy cursor is `sessions.__96bulls_frozen_copy_progress` in the target. There is no file, artifact, log or receipt cursor. This PR does **not** add the migration. The operator refuses with `frozen_copy_progress_table_missing` until the table exists, and with `frozen_copy_progress_table_unsafe` if the table is not owned by the session, RLS is off, anything is granted to another role, or the columns differ. The DDL below is the proposed migration text for a follow-up registry slot (it is executed only in PGlite tests):

```sql
CREATE TABLE sessions.__96bulls_frozen_copy_progress(
 org_id text NOT NULL,
 dataset text NOT NULL CHECK(dataset IN('records','receipts')),
 binding_sha256 text NOT NULL CHECK(binding_sha256~'^[0-9a-f]{64}$'),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256~'^[0-9a-f]{64}$'),
 operator_sha256 text NOT NULL CHECK(operator_sha256~'^[0-9a-f]{64}$'),
 cursor jsonb NOT NULL CHECK(jsonb_typeof(cursor)='array'),
 page_seq bigint NOT NULL CHECK(page_seq>=1),
 rows_copied bigint NOT NULL CHECK(rows_copied>=0),
 status text NOT NULL CHECK(status IN('in_progress','complete')),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,dataset)
);
ALTER TABLE sessions.__96bulls_frozen_copy_progress ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.__96bulls_frozen_copy_progress
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
```

Contract:
- One row per `(org_id, dataset)`, with the binding: generation, source identity, control epoch, privacy baseline, operator SHA (sha256 of the operator's own source files) and `binding_sha256` over all five. It also holds the raw keyset cursor, page sequence, rows copied, status and `updated_at`. Raw org and key values live only in this table.
- Each page's insert and its checkpoint commit in the SAME target transaction: `BEGIN; insert page; upsert checkpoint; COMMIT`. The upsert advances only from exactly `page_seq - 1`, under the same binding, while `in_progress`; anything else refuses `frozen_copy_progress_conflict`. A page without its checkpoint, or a checkpoint without its page, cannot commit.
- The in-memory cursor advances only after COMMIT returns.
- One operator at a time: a session-level `pg_try_advisory_lock` on a dedicated key (distinct from the authority keys) for both `--copy` and `--seal`; a second operator refuses `frozen_copy_operator_busy`.
- Resume re-proves everything before writing: the frozen Neon source (phase, epoch, baseline), the live policy against it, owner/TLS identity, exclusive ownership, and that every existing checkpoint carries this run's exact binding. A different operator build refuses `frozen_copy_resume_operator_mismatch`; any other binding difference refuses `frozen_copy_resume_binding_mismatch`.
- There is no delete, truncate or reset path for the progress rows.

### Resume, budget, abort and unknown commits
- **Crash before COMMIT**: the transaction never committed; the checkpoint still names the previous page, so the resume re-reads exactly that page.
- **Crash after COMMIT**: the checkpoint already names the page, so the resume continues after it. Page inserts are also `ON CONFLICT DO NOTHING`.
- **COMMIT sent, no answer** (`UNKNOWN_COMMIT`): terminal for the run, never retried in-process. The driver reads the checkpoint back (COMMITTED if `page_seq` reached the in-doubt sequence, otherwise NOT_COMMITTED, PENDING if the readback fails). The next `--copy` starts by reading the same row, so it can never skip an unresolved commit or write a page twice. For the seal, the readback is the completeness row: this manifest gives COMMITTED, no row gives NOT_COMMITTED, another manifest gives REFUSED. An in-doubt privacy insert stays PENDING; the next `--copy` re-runs the idempotent insert and `--seal` refuses unless the privacy rows equal Neon's.
- **Budget (`--budget-ms`) or abort (SIGINT/SIGTERM)**: checked before every read, before every write, and again inside the serialized target section before `BEGIN`. New pages stop, the open transaction rolls back, the progress rows are kept, and the receipt says REFUSED (`frozen_copy_budget_exhausted`) or ABORTED (`frozen_copy_aborted`). Rerun `--copy` to resume.
- **SEALED** requires: every parity MATCH (resets included), the exact binding, policy and counts, a committed completeness seal, and no unreconciled unknown commit.

### Job receipt (`frozen-source-copy-job-v2`)
Every `--copy`/`--seal` run ends with one sanitized receipt (stdout, and `--receipt PATH` as a new 0600 file) and a non-zero exit unless the status is COPIED or SEALED. Statuses: `COPIED`, `SEALED`, `REFUSED`, `ABORTED` and `UNKNOWN_COMMIT`, with a `next` action. It carries:
- `progress{schemaVersion, stateTable, bindingSha, totalOrgs, completedOrgs, dataset, pages, rows, resumeCount, lastCheckpointAt, cursorSha256}`;
- `budget{maxWallMs, elapsedWallMs, deadline, pageSizes, concurrency, maxPageMs, statementTimeoutMs, lockTimeoutMs, exceeded}`;
- `abort{requested, observed, boundedReason, requestedAt, completedAt, inFlightResolved, sourceClosed, targetClosed, writesStopped}`;
- `unknownCommit{detected, phase, dataset, checkpointSeq, detectedAt, reconciliation, readbackAt, resolvedAt, noBlindRetry}`, where reconciliation is one of `NOT_REQUIRED|PENDING|COMMITTED|NOT_COMMITTED|REFUSED`.

There is no credential, org id or raw key in the receipt; the cursor appears only as `cursorSha256`.

## Ingest shadow phase (PR #558)
The dormant `step-0-shadow` private-ingest phase is delivered by PR #558 (branch `fix/ingest-ci-shadow-20261111`), not by this operator. The copy/seal operator does not select or depend on it.
