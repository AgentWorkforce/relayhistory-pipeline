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
1. **Copy and seal**, outside the window, each step a dispatch of the protected custody workflow (see "Supervisor (protected workflow)" below). Uploads keep working because frozen mode is not yet on: `plan`, then `copy` (resumable from the owner-only progress table), then `readback`, then `seal`.
2. **Shadow**: deploy the ingest Worker with `custodySource: shadow`. Require `custody.shadow` outcome `match` on live batches and controls before going further.
   Use the CI-only `step-0-shadow` profile in [private ingest production](../runbooks/private-ingest-production.md): a reviewed tracked-profile change, a fresh exact-SHA plan, deployment, and active-version readback. Keep async admission `drain` and the synchronous frozen-receipt bridge absent. A successful mode readback is not the shadow parity proof, and no copy/seal prerequisite is waived.
3. **Mirror** (relayhistory PR): call `/v1/custody/control/begin` before Neon begin, require an equal `{version,digest}`, and call `/v1/custody/control/complete` after Neon complete. This must be live before any frozen flip. Shadow mismatches on `sourceControlVersion`/`pending` mean it isn't.
4. **Flip**: deploy the ingest Worker with `custodySource: frozen-supabase`, then relayhistory `DELIVERY_CUSTODY_SOURCE_MODE=frozen-supabase` (skip the Neon witness/pending/status calls on batches).
5. **Prove**: the runtime role's Neon upload-path statements reach zero (pg_stat_statements delta), and the upload 2xx rate is unchanged or better.

**Rollback**: first return relayhistory to its Neon witness mode and verify the active-version
readback through its CI path. Only then may the Worker return from frozen to shadow through the
private-ingest CI gate. The Worker accepts the Neon witness in every mode. Keep the RHC writer
lane held through the Worker rollback; a frozen, opaque or Supabase-authority RHC mode refuses.
Respect the separate one-way privacy/auth boundaries: this path cannot undo Supabase authority.

## Copy and seal operator (`scripts/frozen-source-copy.ts`)
No production run of this operator is authorized by this document or by the PR that added it. Running `copy`, `readback` or `seal` against the real target, and applying the progress-table migration below, each need Khaliq's separate, explicit OK. `--copy`, `--readback` and `--seal` refuse unless they run as the protected workflow's job ("Supervisor (protected workflow)").

### Credentials: one stdin document, direct owner only
The script reads ONE JSON document from stdin, which must be a pipe (never a terminal, a file, an environment variable or a CLI flag):
`{"neonOwnerUrl": "<neon owner url>", "supabaseOwnerUrl": "<supabase direct owner url>"}`. `--plan` needs only `neonOwnerUrl` (with `supabaseOwnerUrl` it also prints the expected approvals).

- `neonOwnerUrl` goes through `sessionAffinityUrl` (`src/session-ingest/stdin-credentials.ts`): no Neon pooler host, no port 6543 or `pgbouncer`, an explicit user, a non-empty password and a TCP host. Then the source pins: the owner user, database, the pinned endpoint (compared by digest) and no option beyond `sslmode=require|verify-full` / `channel_binding=require`. An empty password is refused before a client exists, so node-postgres never falls back to `~/.pgpass`; the client gets a discrete config, never a connection string.

- `supabaseOwnerUrl` is the existing direct schema-owner login (`postgres.<project>` through the pinned session pooler). `src/migration/frozen-source-copy-seal.ts` checks it against the fixed schema-release pins before any client exists: host, port 5432, database `postgres`, the owner login, a non-empty password, and nothing beyond `sslmode=require|verify-full` / `channel_binding=require`.
- The client connects with the pinned CA, `rejectUnauthorized` and `servername`, then requires `current_user = session_user =` the owner (inside the seal transaction too).
- There is no `SET ROLE`, no credential minting or refresh, no Supabase CLI temporary login, and no file or environment fallback. The password is never logged or written into the receipt.

The only producer is the protected workflow's operator step. The document is the environment secret `FROZEN_COPY_CREDENTIALS`, piped to stdin; the driver deletes the variable from its own environment at start-up:

```sh
printf '%s' "$FROZEN_COPY_CREDENTIALS" | bun scripts/frozen-source-copy.ts "--$CUSTODY_MODE" --page 5000 --max-pages 0 --budget-ms 19800000
```

The step's sha256 (`FROZEN_COPY_CREDENTIAL_PRODUCER_SHA256`) is bound into the typed approval. Nothing in the repository reads 1Password, and no document or URL is written to disk. Creating the secret is Khaliq's action (1Password is the system of record; for example `op read` of the two items, `jq` into the document, piped to `gh secret set FROZEN_COPY_CREDENTIALS --env frozen-source-custody-production`).

### Modes
- `--plan`: read-only Neon counts per org; with the owner URL also the read-only target binding and the expected `copy` and `readback` approvals (PLAN = this run's id).
- `--copy [--org X] [--concurrency 2] [--page 5000] [--max-page-ms 4000] [--budget-ms N] [--max-pages N] [--receipt PATH]`: resumable, throttled copy of the records and receipts, plus the privacy rows (deletions, resets) and control heads. `--max-pages N` (0 = no limit) stops the run after N pages with `frozen_copy_page_limit`; the checkpoints are kept.
- `--readback [--budget-ms N] [--max-pages N] [--receipt PATH]`: the seal's verification, read-only (every checkpoint complete, every org on both sides, privacy pre-read), stopping before the fence. Status `VERIFIED` with the manifest the seal will rebuild, and the expected `seal` approval carrying it.
- `--seal [--budget-ms N] [--max-pages N] [--receipt PATH]`: requires a complete checkpoint for every org and dataset under this run's binding, verifies every org on both sides, then seals inside the fence. `--org` is refused with `--seal` and `--readback`, so a subset never becomes the global completeness seal.
- `--repair-head`: dry run only, local (see `src/hoopdb/frozen-control-head-repair.ts`). `--execute` is refused (exit 64): no supervised repair action exists, so no head write happens outside the custody workflow.

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
0. Before the lock: a minted supervisor proof for action `seal` (`frozen_copy_supervisor_missing` / `_mismatch`, see "Supervisor (protected workflow)").
1. The sealer takes the privacy-authority advisory key EXCLUSIVELY at session level. Every writer of the fenced tables takes the same key SHARED per statement (guard trigger), so this waits for in-flight writers and blocks new ones; `lock_timeout` refuses (`frozen_copy_fence_busy`). The fenced tables (`FENCED_TABLES`) are the mirror heads, deletions, resets, records, owned sessions (guards of `20261105041000`) and the receipts `ingest_frozen_source_receipts` (guard of `20261105047030_frozen_copy_progress`). Without every guard the seal refuses `frozen_copy_fence_unavailable`, so the receipt parity no longer rests on the copy-ownership try-lock alone.
2. Only then `BEGIN ISOLATION LEVEL SERIALIZABLE`: its snapshot is taken after the lock, so it sees every committed writer and no writer can commit inside it.
3. Inside it: owner identity, guard-trigger presence on every fenced table with exactly this key, authority not `active`.
4. Neon `REPEATABLE READ READ ONLY`: phase `active`, privacy rows and control heads in one snapshot.
5. Supabase privacy rows, control heads and policy in the same transaction. Epoch and privacy baseline are checked here against the Neon descriptor; epoch, privacy baseline and source identity against the pre-read.
6. ANY head difference (Supabase vs Neon, or either side vs the pre-verification read) refuses `frozen_copy_mirror_advanced`; privacy movement refuses `frozen_copy_privacy_advanced`; a parity difference refuses with its own code. The proof's epoch and privacy baseline must equal the live policy, and its approved MANIFEST the manifest built here (`frozen_copy_supervisor_mismatch`).
7. A serialization failure (40001/40P01) refuses `frozen_copy_serialization_failure`, with no retry.
8. Every refusal or error rolls back both transactions, releases the key and exits non-zero.
9. So the seal rows exist only if both sides were equal at one instant at which no guarded writer could move either side.
10. A COMMIT with no server answer is reported as `UNKNOWN_COMMIT` and reconciled by reading the completeness row, never retried blindly.

**Rollback = no seal row.** A refused or aborted seal writes nothing: no org seal and no completeness row. Rerun `--seal` after fixing the cause. An existing completeness row is immutable evidence; a rerun with an identical manifest reports `inserted: false`, and a different manifest refuses `frozen_copy_completeness_conflict`.

### Progress table (owner-only, target Supabase; follow-up migration)
The ONLY copy cursor is `sessions.__96bulls_frozen_copy_progress` in the target. There is no file, artifact, log or receipt cursor. The table and the receipts guard trigger (`ingest_privacy_authority_guard` on `sessions.ingest_frozen_source_receipts`) ship in migration `20261105047030_frozen_copy_progress` (PR #560), not in this PR. The operator refuses with `frozen_copy_progress_table_missing` until the table exists, and with `frozen_copy_progress_table_unsafe` if the table is not owned by the session, RLS is off, anything is granted to another role, or the columns differ. The DDL below is the proposed migration text for a follow-up registry slot (it is executed only in PGlite tests):

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
- **SEALED** requires: every parity MATCH (resets included), the exact binding, policy and counts, a committed completeness seal, no unreconciled unknown commit, and the supervisor proof for `seal`.

### Job receipt (`frozen-source-copy-job-v3`)
Every `--copy`/`--readback`/`--seal` run ends with one sanitized receipt (stdout, and `--receipt PATH` as a new 0600 file) and a non-zero exit unless the status is COPIED, VERIFIED or SEALED. Statuses: `COPIED`, `VERIFIED`, `SEALED`, `REFUSED`, `ABORTED` and `UNKNOWN_COMMIT`, with a `next` action. A pass without its result (seal manifest, readback manifest) or without the supervisor proof for exactly that action is `REFUSED`. It carries:
- `progress{schemaVersion, stateTable, bindingSha, totalOrgs, completedOrgs, dataset, pages, rows, resumeCount, lastCheckpointAt, cursorSha256}`;
- `budget{maxWallMs, elapsedWallMs, deadline, pageSizes, maxPages, concurrency, maxPageMs, statementTimeoutMs, lockTimeoutMs, exceeded}`;
- `abort{requested, observed, boundedReason, requestedAt, completedAt, inFlightResolved, sourceClosed, targetClosed, writesStopped}`;
- `unknownCommit{detected, phase, dataset, checkpointSeq, detectedAt, reconciliation, readbackAt, resolvedAt, noBlindRetry}`, where reconciliation is one of `NOT_REQUIRED|PENDING|COMMITTED|NOT_COMMITTED|REFUSED`;
- `seal` (SEALED), `readback{manifestSha256, orgs}` (VERIFIED);
- `supervisor{repository_id, workflow, environment, run_id, run_attempt, source_sha, actor, triggering_actor, plan_run, approval_sha256, action, ci_conclusion}` when the attestation was proven, else `null`.

There is no credential, URL, org id or raw key in the receipt; the cursor appears only as `cursorSha256`, the approval only as `approval_sha256`.

### Supervisor (protected workflow)
The qualified supervisor is the workflow `.github/workflows/frozen-source-custody.yml`, not a person or a database login (`src/migration/frozen-source-copy-supervisor.ts`). It is `workflow_dispatch` only, with inputs `mode` (`plan|copy|seal|readback`), `sha`, `plan_run` and `approval`, permissions `contents: read, actions: read, deployments: read`, and one concurrency group.

1. **prerequisite-gate** (no environment, no secret; only for repository id `1437714333` on `refs/heads/main`) runs `scripts/frozen-source-custody-github.ts` and outputs `environment=frozen-source-custody-production`.
2. **custody** runs under that environment (its secret `FROZEN_COPY_CREDENTIALS` exists only there), re-runs the gate with `--protected`, then runs the operator step above. The operator re-reads the run through the GitHub API with the job's `GITHUB_TOKEN`; it never trusts a file or output from the previous job.

What is proven, live, before any write (`verifySupervisor`, called in the driver after the target binding is read and before the resume check):
- the Actions context: repository + id, `workflow_dispatch`, `refs/heads/main`, `GITHUB_WORKFLOW_REF` = this workflow on main, `run_attempt` 1, `actor` AND `triggering_actor` = the pinned dispatcher (`SCHEMA_RELEASE.dispatcher`: login, id, type User), mode and sha equal to the inputs;
- the run (API): this workflow's path and repository id, first attempt, pinned actor and triggering actor, head sha = the input sha, run name `frozen-source-custody <mode> <sha>`;
- the sha is the live `main` head;
- the environment: exists with a single custom branch policy `main`, under the same human-approval rule as the guarded release workflows (`requireProtectedEnvironment` / `requireHumanApproval`): with a required-reviewers rule, a User reviewer of the environment approved this run and nobody rejected it; a plan that cannot create that rule falls back to the first-attempt dispatch by the pinned account, which the run checks above already require. A deployment of this sha to the environment has an `in_progress` status linking this run;
- the full push proof of the schema-release gate (`requireGreenPushRuns` with its default set, no override): the latest `ci.yml` push run on `main` at that sha completed/success with jobs `checks` and `session-read-worker / worker`, and the latest `pg-proof.yml` push run on `main` at that sha completed/success with jobs `real PostgreSQL 15`, `real PostgreSQL 16` and `real PostgreSQL 17`; every job of each run attempt completed/success at that sha;
- for `copy`/`readback`/`seal`: the `plan_run` input is a successful `plan` run of this workflow at the same sha, first attempt, by the pinned dispatcher, under 24 hours old;
- the typed `approval` input equals, as one exact string, what this run derives from the live source and target:
  `FROZEN-COPY <ACTION> <sha> SOURCE <source descriptor sha256> TARGET <target descriptor sha256> EPOCH <epoch> PRIVACY <privacy baseline> GENERATION <generation> MANIFEST <manifest sha256|none> OPERATOR <operator sha256> PRODUCER <credential-producer sha256> RECEIPT frozen-source-copy-job-v3 PAGE <n> MAXPAGES <n> BUDGET <ms> PLAN <plan run id>`.
  `--plan` prints the `copy` and `readback` lines; `--readback` prints the `seal` line with the manifest. For `seal` the MANIFEST is enforced inside the fence.

Refusal codes (the receipt names the failed check, never a value): `frozen_copy_supervisor_missing` (no Actions context, token, approval, plan run or proof; GitHub API unreachable), `frozen_copy_supervisor_unqualified` (wrong repository, workflow, event, ref, attempt, actor, environment, approval, deployment, CI or plan run), `frozen_copy_supervisor_stale` (main moved, plan run older than 24 hours), `frozen_copy_supervisor_mismatch` (sha, mode, any approval field, the seal's manifest or binding). COPIED, VERIFIED and SEALED are impossible without the proof: only `verifySupervisor` mints it (module-private brand plus a registry of minted objects), so a hand-built or copied object is rejected. A local run of `--copy`, `--readback` or `--seal` refuses before any connection.

Creating and protecting the `frozen-source-custody-production` environment (required reviewer, branch policy `main`), adding its secret `FROZEN_COPY_CREDENTIALS`, and every dispatch of the workflow are Khaliq's actions only. No agent creates the environment, sets the secret or dispatches the workflow. Until the environment exists every protected job fails closed (`frozen_copy_supervisor_unqualified`, check `environment`).

The deployment check relies on GitHub creating a deployment with an `in_progress` status whose `log_url`/`target_url` names this run for a job that declares `environment:`. Those field semantics are not yet verified against a live run; if they differ, the job refuses (`environment_deployment`), it never passes.

## Ingest shadow phase (PR #558)
The dormant `step-0-shadow` private-ingest phase is delivered by PR #558 (branch `fix/ingest-ci-shadow-20261111`), not by this operator. The copy/seal operator does not select or depend on it.
