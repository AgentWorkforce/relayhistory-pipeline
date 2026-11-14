# Backlog ops workflow (0005 backfill, export and drain)

`.github/workflows/backlog-ops.yml` is the only way the 0005 backlog tools run against production. Production
operations do not run from a laptop or a host. The tools are:

- the fence backfill, `scripts/upload-fence-backfill.ts`;
- the accepted-stage export, `scripts/accepted-stage-export.ts`;
- the S3 drain, `scripts/buffered-drain.ts`.

See [decision 0005](decisions/0005-s3-native-upload-ledger.md), "The buffered backlog". The contract is in
`src/session-ingest/backlog-ops.ts`, the gate in `scripts/backlog-ops-github.ts`, and the credential step in
`scripts/backlog-ops.ts`.

## Modes and order

| Mode | Writes | Binds (typed `confirmation`) |
| --- | --- | --- |
| `backfill-dry` | nothing | none |
| `backfill-execute` | fences, owner stubs, revision shards | `DRY <backfill-dry run>` |
| `export-dry` | nothing | none |
| `export-execute` | accepted sidecars, per-org completion | `DRY <export-dry run> BACKFILL <backfill-execute run>` |
| `drain-dry` | nothing | none |
| `drain-execute` | canonical layout, index, re-arm and drained markers | `DRY <drain-dry run> BACKFILL <backfill-execute run> EXPORT <export-execute run>` |
| `presence-dry` | nothing (dry only; there is no `presence-execute`) | none |

The confirmation is exactly `<MODE uppercase> <sha> DRY <id> [BACKFILL <id>] [EXPORT <id>]`, for example
`DRAIN-EXECUTE <sha> DRY 111 BACKFILL 222 EXPORT 333`.

`presence-dry` is outside the sequence: a diagnostic for one session, described in
[Session presence (presence-dry)](#session-presence-presence-dry).

Run them in this order, all at the same `main` commit:

1. `backfill-dry`
2. `backfill-execute`
3. `export-dry`
4. `export-execute`
5. `drain-dry`
6. `backfill-execute` again
7. `export-execute` again
8. `drain-execute`

The drain needs a backfill rerun immediately before it, because a control made after a backfill is not in the
fence. The gate requires the drain's bound backfill to be under 3 hours old and its export to have been created
after that backfill.

To drain in slices, give `max_objects` and rerun `drain-execute`. Each rerun skips what earlier runs wrote, using
the create-only `drained` markers. The workflow never passes `--checkpoint-file` or `--start-after`.

## Org scope

A run can be limited to some orgs with the environment **secret** `BACKLOG_OPS_ORGS`, set on both
`session-schema-production` (dry runs) and `session-backlog-execute` (executes): comma-separated lowercase org UUIDs,
no spaces, at most 64. Unset or empty runs every org, as before. It is a secret rather than a variable so this public
repository's logs mask the ids; the receipt records only the scope's id (`all`, or `orgs_` and 32 hex of a sha256 of
the sorted list).

- The workflow passes each org to the tool as `--org`. The tools' own rules are unchanged: the drain still writes
  nothing for an org without its fence-authority marker.
- An execute refuses unless its dry run has the same scope. A bound backfill or export must have the same scope or
  be unscoped. A receipt from before scoping counts as unscoped. So change the secret only between complete
  sequences, and set it on both environments to the same value.
- A scoped backfill reads `ingest_lazy_native_slots` in heap block windows (one sequential pass) instead of walking
  the primary key, which fetched every slot at random to test its org. A slot rewritten during the pass can be
  missed, so run a scoped backfill-execute after the scope's HTTP admission stop (decision 0005's cutover order).

## Gates

Every run passes the same gates as the guarded schema release:

- the repository id, `refs/heads/main`, and the exact live main head;
- first attempt only;
- the pinned dispatcher, as both actor and triggering actor;
- the mode's protected environment, with deployments from `main` only: `session-schema-production` for a dry run,
  `session-backlog-execute` for an execute (derived from the mode, never an input);
- green push `ci` and `pg-proof` at the commit;
- inside the protected job, the recorded approval for that environment.

Every execute additionally needs all of the following:

- **Dormant switch:** `BACKLOG_OPS_EXECUTE_ENABLED` in `src/session-ingest/backlog-ops.ts` is `false`, and only a
  reviewed commit flips it. A dry run never needs it.
- **Bound runs:** each one is a successful first attempt of this workflow at the same commit, by the pinned
  dispatcher, of the expected mode, under 24 hours old, with exactly one unexpired receipt artifact.
- **Receipts, checked before any credential is read:**
  - every bound receipt exited 0;
  - the backfill's `sections` are all `complete`. A backfill-execute therefore cannot pass while any user is
    `needs_review`: the backfill leaves that section incomplete until the user is resolved, and every later execute
    stays blocked until then;
  - the export has `failed = 0`, `fence_late = 0` and `orgs.incomplete = 0`.
- **The tools' own refusals:**
  - the export refuses while the v2 claim-sessions reader gate is off (`export_claim_sessions_gate_off`);
  - the drain refuses beside a live lease at `96bulls/.drain/lock.json`, and writes nothing for an org without a
    fence-authority marker.

Dispatches share the concurrency group `guarded-session-schema-production` with every other guarded production
operator (the schema apply, the generation overlap/rebind, the 058000 index prebuild, owner-buffered parity and
admission renewal), so a backfill or export never reads the owner database mid-apply or mid-rebind, and one backlog
operation runs at a time. The cost: a running backlog operation (a drain slice can take up to the 355-minute job limit) holds that
group, so a schema apply or rebind queues behind it. Use `max_objects` to keep drain slices short when one is due.

## Credentials

The **database** login is the environment secret `SUPABASE_MIGRATION_OWNER_URL`, the same one the schema release
uses. It is passed only to the backfill, export and `presence-dry` modes; the drain needs no database. It is checked
against the pinned release connection and handed to the tool in one normal form (`sslmode=verify-full`). The tools
pin TLS to the Supabase CA themselves. The URL reaches the tool only on its stdin pipe, as `{"dbUrl": …}` (for
`presence-dry`, `{"dbUrl": …, "session": …}`). Every tool runs every page in a READ ONLY transaction.

**AWS** access is a GitHub OIDC token, exchanged inside the credential step for a role session through STS
`AssumeRoleWithWebIdentity`, for at most 6 hours; the step refuses before the tool starts unless the session has at
least 5 hours left. Dry modes use the read-only role and executes use the writer role. The session lives only in the
tool's environment.

The credential step is the only step given the database secret or the role session, and the only one that requests
the OIDC token. It is **not** the only step that could request one: GitHub exposes `ACTIONS_ID_TOKEN_REQUEST_*` to
every step of a job with `id-token: write`. So only the operate job has `id-token: write`, its actions are
`actions/checkout` and `actions/download-artifact` (GitHub's own) plus the third-party `oven-sh/setup-bun`, which is
pinned to a full commit SHA (v2.2.0) rather than a mutable tag. The separation that matters is in IAM: the write role trusts only the `session-backlog-execute` environment's subject, which no other job uses.

Both environments' protected variables pin the target (set the same five on each): `BACKLOG_OPS_BUCKET`, `BACKLOG_OPS_REGION`,
`BACKLOG_OPS_KMS_KEY_ARN` (a key ARN, `key/<uuid>` or `key/mrk-<id>`, not an alias), `BACKLOG_OPS_DRY_ROLE_ARN` and `BACKLOG_OPS_EXECUTE_ROLE_ARN`.
They are shape-checked: the key must be in the region, and both roles must be in the key's account and distinct.

Receipts are counts only. Every leaf is a number, a boolean or a short lowercase code; any other string, such as an
id, a key or a URL, refuses the receipt. They are kept for 30 days as the `backlog-ops-receipt` artifact.

## Session presence (presence-dry)

**Purpose.** Support diagnosis of ONE session whose dashboard shows "No conversation recorded yet": it counts how
far the session's rows got through ingest, the owner buffer, custody and publication. It reads the owner database
only, in one `REPEATABLE READ READ ONLY` transaction that always ends in `ROLLBACK`. It writes nothing, reads no S3
object and binds no other run. The tool is `scripts/session-presence.ts` (`src/session-ingest/session-presence.ts`).

**Inputs.** The `presence-dry` mode, the exact `main` sha, no `confirmation`, and no bound run ids. It runs in the dry
environment `session-schema-production` with the dry (read-only) AWS role, which it does not use (it reads no S3).
The **database** login is `SUPABASE_MIGRATION_OWNER_URL`, the migration owner, not a read-only database role. The
run is read-only only because the tool's connection sets `default_transaction_read_only=on` (beside the owner
config's `idle_in_transaction_session_timeout`) and every count runs in a `REPEATABLE READ READ ONLY` transaction
that checks `SHOW transaction_read_only` and always rolls back. All gates above apply unchanged. `BACKLOG_OPS_ORGS`,
if set, scopes every count to those orgs (receipt `scope` `orgs_<n>`, else `all`).

**Scope it where you can.** Unscoped runs (no `BACKLOG_OPS_ORGS`) are allowed, but most counts are then sequential
scans of large tenant-first tables (ownership, claim sessions, lazy slots, revisions, heads, generations and
`session_index`) on the production primary. Set `BACKLOG_OPS_ORGS` to the session's org whenever you know it, so the
counts use their org-leading indexes.

**Time bounds.** Each statement has a 60 s `statement_timeout` and a 5 s `lock_timeout` (a count that waits on DDL
refuses the run with `sqlstate_55p03`). The whole transaction has a 180 s budget (`--total-budget-ms 180000`): no
statement starts with less than 1 s left, and each statement's timeout is lowered to what is left, so the
`REPEATABLE READ` snapshot is held for about 3 minutes at most. Both values are fixed in the workflow's argv
(`BACKLOG_OPS_PRESENCE_TIMEOUT`).

**The session id is a secret.** The owner sets the environment **secret** `SESSION_PRESENCE_SESSION` on
`session-schema-production` immediately before the dispatch and **removes it immediately after** the run. The value
is one lowercase UUID or a `[A-Za-z0-9_-]{8,128}` token. The workflow passes it only to the credential step and only
for `presence-dry`. That step hands it to the tool on its stdin pipe beside the database URL. It never goes into argv,
a log line or the receipt. The tool's own refusals pass through only as `session_presence_*`, `backlog_ops_*` or
`sqlstate_<code>`; any other error is `internal_error`. The refusals are fixed codes and never contain the value:

- `backlog_ops_presence_session_missing`: the secret is unset or empty.
- `backlog_ops_presence_session_invalid`: any other shape.
- `backlog_ops_presence_session_unexpected`: another mode saw the secret non-empty, so remove it after the run.
  This is defense in depth: the workflow expression already blanks the secret for every other mode, so it fires only
  if that expression is changed or bypassed.
- `backlog_ops_presence_dry_only`: the dispatch named `presence-execute`, or an execute path reached the presence
  tool.
- `backlog_ops_bound_presence_receipt`: a presence receipt was bound as a dry, backfill or export run.

**Receipt.** Every value is a count, `null` when that count hit the 60 s statement timeout or the 180 s budget, or a
fixed code. There are no ids, keys, URLs, digests or content. With `BACKLOG_OPS_ORGS` unset, most counts scan their
table (the tables are keyed tenant-first). Each one is bounded by the timeout and the budget.

| Field | Counts |
| --- | --- |
| `ownership` | `ingest_session_ownership` rows naming the session (1 is normal) |
| `claims` | accepted upload claims (`ingest_claim_attempts`) whose records name the session |
| `claim_sessions` | `ingest_claim_sessions` rows (the v2 reader's session index, written only once its policy enables writes) |
| `buffered_stages` | owner-buffered stage rows of those claims that the buffered reader could serve. This is an upper bound, because the reader's runtime gates are not evaluated |
| `outbox`, `outbox_buffered` | publication outbox rows of those batches, in any state and still `buffered` |
| `lazy_slots` | `ingest_lazy_native_slots` currently owned by the session |
| `revisions` | `ingest_record_revisions` of the session |
| `heads`, `published_heads` | `ingest_publication_heads` rows, and the published ones (`live` or carrying a current pointer) |
| `dispositions` | `heads_building`, `heads_live`, `heads_retired`: heads per state (the fixed `state` enum) |
| `published_objects` | live objects of live publication generations |
| `canonical_pointers`, `canonical_pointers_live` | `session_index` pointers the read API looks for first, and the undeleted ones |
| `generation_stable` | whether the publication marker (heads and canonical pointers of the session) was identical when read first and last. This is a snapshot / pooler-consistency check, not a change detector: two reads in one `REPEATABLE READ` transaction see the same snapshot, so they can differ only if the pooler did not hold the transaction on one backend. A publish during the run does not change it |
| `complete`, `incomplete_reason` | `true` and `none`, or `false` with one of the codes below |

**`incomplete_reason` codes.** When more than one applies, the first in this list is reported. The counts are still
emitted.

1. `generation_unobserved`: a marker read timed out, or the budget ran out before the last one.
2. `generation_unstable`: the first and last marker reads differed, so the counts did not come from one snapshot
   (the pooler did not hold the transaction on one backend). Rerun; do not read the counts.
3. `multi_ownership`: more than one ownership row, or the session id appears under more than one org in the tables
   counted. Read the counts with care, because they span every owner. Scope with `BACKLOG_OPS_ORGS` to split them.
4. `count_timeout`: some count hit the 60 s statement timeout and is `null`.
5. `budget_exhausted`: the 180 s budget ran out, so the remaining counts were skipped or cut short and are `null`.
   Scope the run with `BACKLOG_OPS_ORGS`.

**Reading it against "No conversation recorded yet":**

| Shape | Meaning |
| --- | --- |
| everything 0 | Nothing arrived for this session id, or it arrived under another org (check `BACKLOG_OPS_ORGS`) or another id. Look at the uploader. |
| `ownership` 1, `claims` > 0, `buffered_stages` > 0, `published_heads` 0 | An unpublished buffered stage: accepted and buffered, never published. The page can show it only through the owner-buffered reader (flag, allowlist and admission), and after that only through the drain. |
| `buffered_stages` 0, `outbox` > 0, `outbox_buffered` 0, `published_heads` 0 | The buffer left the outbox without a publication. Check the drain and publisher receipts. |
| `published_heads` > 0, `canonical_pointers_live` 0 | Published, but there is no live canonical pointer for the read API. Check the index step, or a tombstone if `canonical_pointers` > 0. |
| `canonical_pointers_live` > 0 | The data is published and indexed. Look at the read path and access (org, workspace, user), not ingest. |
| `heads_retired` > 0 and no live head | The session's publication was retired (deleted or reset). |

## IAM

The provider, both roles, the two key statements and the five variables on `session-schema-production` were applied
by hand with Khaliq's OK on 2026-11-13 at 17:49Z. **The write role's trust and the `session-backlog-execute`
environment are not done yet** (see "Still to apply" below). The two key statements are reverted by the next
`aws-sessions-prod` stack deploy until the backlog-ops key phase (`transcriptStore.backlogOpsRoles`) lands and is
set in the stage inputs; until then, re-add them after any stack deploy.

No existing principal covers the drain's writes: the transcripts key policy grants key use only to named principals,
and the srv-writer and writer policies are prefix-scoped. So the workflow needs, in the sessions-prod account:

1. **An OIDC provider** for `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`.
2. **`96bulls-aws-sessions-prod-backlog-ops-read`**, trusted only for
   `token.actions.githubusercontent.com:sub` =
   `repo:AgentWorkforce@252050005/relayhistory-pipeline@1437714333:environment:session-schema-production`
   (the repository's immutable-id subject, from `gh api repos/AgentWorkforce/relayhistory-pipeline/actions/oidc/customization/sub`
   on 2026-11-13; the name segment follows the repository's current name, so recheck it after a rename) and `aud` =
   `sts.amazonaws.com`. `MaxSessionDuration` is 6 h. It may only `s3:ListBucket` the transcripts bucket,
   `s3:GetObject` under `96bulls/*`, and `kms:Decrypt` on the transcripts key via `s3.us-east-1.amazonaws.com`.
3. **`96bulls-aws-sessions-prod-backlog-ops-write`**, trusted only for
   `repo:AgentWorkforce@252050005/relayhistory-pipeline@1437714333:environment:session-backlog-execute` (and `aud` =
   `sts.amazonaws.com`), never the schema environment's subject. It may do everything the read role does,
   plus `s3:PutObject` under `96bulls/*` with `s3:x-amz-server-side-encryption = aws:kms` and the transcripts key,
   and `kms:GenerateDataKey` via S3. It gets no `DeleteObject`: the bucket policy's `OnlyTheEraserDeletes` already
   denies it, and nothing in the tools deletes.
4. **Two key-policy statements** on the transcripts key, naming the two roles. The key policy delegates no key use
   to IAM:
   - `BacklogOpsReadDecryptsViaS3`: `kms:Decrypt`;
   - `BacklogOpsWriteEncryptsViaS3`: `kms:GenerateDataKey` and `kms:Decrypt`.

   Both are conditioned on `kms:ViaService` = `s3.us-east-1.amazonaws.com`.

### Still to apply (before the first execute)

1. Create the GitHub environment `session-backlog-execute` with deployments from `main` only (one branch policy,
   `main`, type branch), as on `session-schema-production`. The gate refuses an environment without that branch
   policy. Required reviewers are optional: with the pinned dispatcher (`trustedDispatcher`), the gate accepts an
   environment with only the branch policy and no approval, exactly as on `session-schema-production`. The real
   control is the pinned dispatcher identity plus the write role's IAM trust in this environment's subject.
   (Created 2026-11-13 with the `main` branch policy, the secret and the five variables; the write role's trust was
   narrowed to this subject the same day.)
2. Set on it the environment secret `SUPABASE_MIGRATION_OWNER_URL` (the same value as on `session-schema-production`;
   backfill-execute and export-execute need it) and the same five `BACKLOG_OPS_*` variables.
3. Change the write role's trust to the `session-backlog-execute` subject above (it currently trusts the
   `session-schema-production` subject, like the read role).

Dry runs need none of this and can run now.
