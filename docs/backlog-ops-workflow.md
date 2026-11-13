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
- the protected `session-schema-production` environment, with deployments from `main` only;
- green push `ci` and `pg-proof` at the commit;
- inside the protected job, the recorded environment approval.

Every execute additionally needs all of the following:

- **Dormant switch:** `BACKLOG_OPS_EXECUTE_ENABLED` in `src/session-ingest/backlog-ops.ts` is `false`, and only a
  reviewed commit flips it. A dry run never needs it.
- **Bound runs:** each one is a successful first attempt of this workflow at the same commit, by the pinned
  dispatcher, of the expected mode, under 24 hours old, with exactly one unexpired receipt artifact.
- **Receipts, checked before any credential is read:**
  - every bound receipt exited 0;
  - the backfill's `sections` are all `complete`;
  - the export has `failed = 0`, `fence_late = 0` and `orgs.incomplete = 0`.
- **The tools' own refusals:**
  - the export refuses while the v2 claim-sessions reader gate is off (`export_claim_sessions_gate_off`);
  - the drain refuses beside a live lease at `96bulls/.drain/lock.json`, and writes nothing for an org without a
    fence-authority marker.

One backlog operation runs at a time, enforced by the concurrency group `guarded-backlog-ops-production`.

## Credentials

The **database** login is the environment secret `SUPABASE_MIGRATION_OWNER_URL`, the same one the schema release
uses. It is passed only to the backfill and export modes; the drain needs no database. It is checked against the
pinned release connection, then re-encoded with only `sslmode=verify-full`, which is what the tools' strict parser
accepts. The tools pin TLS to the Supabase CA themselves. The URL reaches the tool only as `{"dbUrl": …}` on its
stdin pipe. Both tools run every page in a READ ONLY transaction.

**AWS** access is a GitHub OIDC token, exchanged inside the one credential step for a role session through STS
`AssumeRoleWithWebIdentity`, for at most 6 hours. Dry modes use the read-only role and executes use the writer
role. The session lives only in the tool's environment.

The environment's protected variables pin the target: `BACKLOG_OPS_BUCKET`, `BACKLOG_OPS_REGION`,
`BACKLOG_OPS_KMS_KEY_ARN` (a key ARN, not an alias), `BACKLOG_OPS_DRY_ROLE_ARN` and `BACKLOG_OPS_EXECUTE_ROLE_ARN`.
They are shape-checked: the key must be in the region, and both roles must be in the key's account and distinct.

Receipts are counts only. Every leaf is a number, a boolean or a short lowercase code; any other string, such as an
id, a key or a URL, refuses the receipt. They are kept for 30 days as the `backlog-ops-receipt` artifact.

## IAM this needs (not applied; needs Khaliq's explicit OK)

The sessions-prod account has **no GitHub OIDC provider**, and no existing principal covers the drain's writes. The
transcripts key policy grants key use only to named principals, and the srv-writer and writer policies are
prefix-scoped. So the workflow needs, in the sessions-prod account:

1. **An OIDC provider** for `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`.
2. **`96bulls-aws-sessions-prod-backlog-ops-read`**, trusted only for
   `token.actions.githubusercontent.com:sub` =
   `repo:AgentWorkforce@252050005/relayhistory-pipeline@1437714333:environment:session-schema-production`
   (the repository's immutable-id subject, from `gh api repos/AgentWorkforce/relayhistory-pipeline/actions/oidc/customization/sub`
   on 2026-11-13; the name segment follows the repository's current name, so recheck it after a rename) and `aud` =
   `sts.amazonaws.com`. `MaxSessionDuration` is 6 h. It may only `s3:ListBucket` the transcripts bucket,
   `s3:GetObject` under `96bulls/*`, and `kms:Decrypt` on the transcripts key via `s3.us-east-1.amazonaws.com`.
3. **`96bulls-aws-sessions-prod-backlog-ops-write`**, with the same trust. It may do everything the read role does,
   plus `s3:PutObject` under `96bulls/*` with `s3:x-amz-server-side-encryption = aws:kms` and the transcripts key,
   and `kms:GenerateDataKey` via S3. It gets no `DeleteObject`: the bucket policy's `OnlyTheEraserDeletes` already
   denies it, and nothing in the tools deletes.
4. **Two key-policy statements** on the transcripts key, naming the two roles. The key policy delegates no key use
   to IAM:
   - `BacklogOpsReadDecryptsViaS3`: `kms:Decrypt`;
   - `BacklogOpsWriteEncryptsViaS3`: `kms:GenerateDataKey` and `kms:Decrypt`.

   Both are conditioned on `kms:ViaService` = `s3.us-east-1.amazonaws.com`.

The key policy and the bucket policy are owned by the `aws-sessions-prod` stack. A hand-added key statement is
reverted by the next stack deploy, and the stack's deploy guard accepts only its listed plan shapes (docs/aws-stage.md).
So the durable form is a stack change with its own guarded phase, like the cross-account worker grants (#608).
That is a follow-up. Until it lands, an operator with Khaliq's OK could create the provider and roles and add the
key statements by hand, re-applying them after any stack deploy.

After the roles exist, set the five protected variables on `session-schema-production`.
