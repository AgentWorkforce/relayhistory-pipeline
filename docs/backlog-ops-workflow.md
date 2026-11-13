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

The confirmation is exactly `<MODE uppercase> <sha> DRY <id> [BACKFILL <id>] [EXPORT <id>]`, for example
`DRAIN-EXECUTE <sha> DRY 111 BACKFILL 222 EXPORT 333`.

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
the create-only `drained` markers. The workflow never passes `--checkpoint-file`, `--start-after` or an org.

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
uses. It is passed only to the backfill and export modes; the drain needs no database. It is checked against the
pinned release connection and handed to the tool in one normal form (`sslmode=verify-full`). The tools pin TLS to
the Supabase CA themselves. The URL reaches the tool only as `{"dbUrl": …}` on its
stdin pipe. Both tools run every page in a READ ONLY transaction.

**AWS** access is a GitHub OIDC token, exchanged inside the credential step for a role session through STS
`AssumeRoleWithWebIdentity`, for at most 6 hours; the step refuses before the tool starts unless the session has at
least 5 hours left. Dry modes use the read-only role and executes use the writer role. The session lives only in the
tool's environment.

The credential step is the only step given the database secret or the role session, and the only one that requests
the OIDC token. It is **not** the only step that could request one: GitHub exposes `ACTIONS_ID_TOKEN_REQUEST_*` to
every step of a job with `id-token: write`. So only the operate job has `id-token: write`, every action in it is
first-party (`actions/checkout`, `actions/download-artifact`, `oven-sh/setup-bun`), and the separation that matters
is in IAM: the write role trusts only the `session-backlog-execute` environment's subject, which no other job uses.

Both environments' protected variables pin the target (set the same five on each): `BACKLOG_OPS_BUCKET`, `BACKLOG_OPS_REGION`,
`BACKLOG_OPS_KMS_KEY_ARN` (a key ARN, `key/<uuid>` or `key/mrk-<id>`, not an alias), `BACKLOG_OPS_DRY_ROLE_ARN` and `BACKLOG_OPS_EXECUTE_ROLE_ARN`.
They are shape-checked: the key must be in the region, and both roles must be in the key's account and distinct.

Receipts are counts only. Every leaf is a number, a boolean or a short lowercase code; any other string, such as an
id, a key or a URL, refuses the receipt. They are kept for 30 days as the `backlog-ops-receipt` artifact.

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

1. Create the GitHub environment `session-backlog-execute`: required reviewers as on `session-schema-production`, and
   deployments from `main` only (one branch policy, `main`, type branch). The gate refuses it otherwise.
2. Set on it the environment secret `SUPABASE_MIGRATION_OWNER_URL` (the same value as on `session-schema-production`;
   backfill-execute and export-execute need it) and the same five `BACKLOG_OPS_*` variables.
3. Change the write role's trust to the `session-backlog-execute` subject above (it currently trusts the
   `session-schema-production` subject, like the read role).

Dry runs need none of this and can run now.
