# Legacy evidence export (conversation_turns, session_links, session_outcomes)

Operator: Khaliq only. This runbook does production reads from Neon and the
live HoopDb/Supabase privacy authority (table-read-only, with narrow
authority-registration and readiness RPCs), plus production S3 writes. Do not run any mode against real data or write the production bucket
without Khaliq's explicit words for that exact run. Design and gate status:
[docs/canonical-legacy-source.md](../docs/canonical-legacy-source.md).

The landing branch's `--export` entry point refuses unconditionally with
`legacy_export_requires_activation_heartbeat`. Only the separately reviewed
activation follow-up may replace that refusal with its fresh, eraser-written
heartbeat gate; landing or applying 062030 alone cannot enable exports.

## Rules

- **Credentials only on stdin, one per line:**
  1. Neon URL;
  2. live HoopDb/Supabase privacy-authority URL;
  3. S3 access key id;
  4. S3 secret;
  5. optional session token.

  Never put them in argv, in env or in files. The runner refuses a terminal stdin. Non-secret settings are flags.
- **Neon role.**
  - Use a SELECT-only role (e.g. `relayhistory_readonly`) on a **direct**, not pooler, URL.
  - The runner verifies `default_transaction_read_only=on`, UTC, SELECT and no write privilege on all three evidence tables plus `deleted_upload_sessions` and `conversation_turn_fences`.
  - It refuses anything else, including `neondb_owner`.
- **Privacy role.**
  - Use only the dedicated `bulls96_legacy_export_privacy` direct connection.
    Bootstrap/migration `20261105062030_legacy_export_scope_authority.sql`
    grants it SELECT on `session_erasure_receipts`, `upload_deletions`,
    `ingest_frozen_source_deletions`, `upload_resets`,
    `turn_session_fences`, and `operator_user_erasures`, plus EXECUTE on the
    narrow SECURITY DEFINER authority registration and readiness RPCs. It has no table-write
    privilege and no privilege on `legacy_export_scope_authority`. Promote the
    bare role to LOGIN only through
    `supabase/bootstrap/legacy_export_privacy_role.sql` under a separate apply
    approval; run `supabase/bootstrap/legacy_export_privacy_verify.sql` before
    and after that step, and set its password from 1Password in memory.
  - Migration
    `20261105062010_turns_forward_gates.sql`, which creates
    `turn_session_fences`, must already be landed and applied; a missing
    relation is a hard refusal, never an empty fence set.
    Preserve the registry order `062010` turns gates -> `062020` read-API
    reader kind -> `062030` legacy authority: do not land or apply `062030`
    while either predecessor is absent.
  - Poolers, including port 6543, any other identity, and roles with ledger
    write privileges are refused. With RLS active, migration 062030's reviewed,
    permissive full-read policy must be present and no applicable restrictive
    policy may hide rows; a policy-less SELECT is refused. Creating this
    dedicated role, changing grants, or granting `BYPASSRLS` is an IAM change
    and needs Khaliq's explicit approval; do not make it as part of a run.
  - The exporter holds the operator-erasure advisory locks shared across each
    object write and rechecks authority readiness, session receipts, upload deletions/resets,
    turn-session fences and user erasures before checkpointing. It overrides
    the privacy connection's idle-in-transaction timeout to a 15-minute scope
    bound and aborts the whole run if that connection or transaction is lost.
  - Before the first S3 PUT for a scope, the exporter immutably registers
    `(org, normalized workspace, source, session, legacy user, legacy machine)`.
    A replay must return the same tuple; an owner or machine change refuses.
    This is how the hosted eraser finds the exact legacy copy after any member,
    session, frozen-source or turn-fence deletion without deriving an owner
    from the deleting member.
  - The shared global privacy lock is held for one scope's Neon read and S3
    write. A queued exclusive erase can in turn delay later ingest/export shared
    lock requests. Run only in the approved window, keep scopes bounded, and
    stop if an erase is queued rather than increasing lock timeouts.
- **S3 key.** Use the transcript **writer/exporter** key (the same one as neon-s3-sync), never the eraser key. The store is wrapped write-only.
- **Checkpoint.** It lives at `~/.agent-tmp/legacy-export/state.sqlite` (override with `--state=`). It holds cursors, scope keys, fingerprints and manifest keys; no secrets, no content. Keep it: it makes runs resumable and is the input to `--verify`.
  A checkpoint created before user-scoped legacy machines were introduced is
  refused if it retains a manifest without `user_id`; its exact old object and
  pointer need separately approved cleanup before starting a fresh checkpoint.

The secrets pipe used below: substitute the real 1Password item paths, and add `op read …/session-token` as line 5 if the key is temporary.

```sh
secrets() {
  op read "op://96Bulls-prod/<neon relayhistory_readonly item>/direct url"
  op read "op://96Bulls-prod/<HoopDb privacy readonly item>/direct url"
  op read "op://96Bulls-prod/sessions-prod transcript writer/access-key-id"
  op read "op://96Bulls-prod/sessions-prod transcript writer/secret-access-key"
}
COMMON="--bucket=<transcript bucket> --expect-bucket=<transcript bucket> --region=us-east-1 \
  --sse=aws:kms --kms-key=<bucket key ARN> --key-root=96bulls --grant-id=neon-legacy-export"
```

## 0. Read-only Neon baseline (record the output)

Run these as the read-only role in `psql`:

```sql
BEGIN READ ONLY;
-- the role really is read-only for these tables (all three rows must be f)
SELECT t, has_table_privilege(current_user, 'sessions.'||t, 'INSERT,UPDATE,DELETE,TRUNCATE') AS can_write,
       has_table_privilege(current_user, 'sessions.'||t, 'SELECT') AS can_read
  FROM unnest(ARRAY['conversation_turns','session_links','session_outcomes']) t;
-- totals and freshness per table
SELECT 'conversation_turns' t, count(*), max(ts) FROM sessions.conversation_turns
UNION ALL SELECT 'session_links', count(*), max(coalesce(link_ts,updated_at,created_at)) FROM sessions.session_links
UNION ALL SELECT 'session_outcomes', count(*), max(coalesce(shipped_at,ingested_at,created_at)) FROM sessions.session_outcomes;
-- per-org counts (pick the canary org from here)
SELECT org_id, count(*) AS turns, max(ts) FROM sessions.conversation_turns GROUP BY 1 ORDER BY 2 DESC;
SELECT org_id, count(*) AS links FROM sessions.session_links GROUP BY 1 ORDER BY 2 DESC;
SELECT org_id, count(*) AS outcomes FROM sessions.session_outcomes GROUP BY 1 ORDER BY 2 DESC;
-- scopes per org (what --plan should report as distinct stored groups)
SELECT org_id, count(DISTINCT (workspace_id, source, session_id)) FROM sessions.conversation_turns GROUP BY 1;
-- recent source activity only; confirm the active writer separately
SELECT count(*) FROM sessions.conversation_turns WHERE ts > now() - interval '1 hour';
COMMIT;
```

## 1. Plan (reads only; writes nothing)

Preflight must show the Neon read-only connection and the table-read-only
privacy connection healthy. A scope present in a
frozen-Neon `deleted_upload_sessions` / `conversation_turn_fences` table or a
live session/turn/user erasure ledger is reported fenced and is never exported. A
scope without an unambiguous owner fails; it is never written as `unknown`.

```sh
secrets | bun run migration:legacy-export --plan --org=<canary org>
secrets | bun run migration:legacy-export --plan --all-orgs
```

Each org prints one `org plan` line:
- `groups`, `scopes`, `rows` per table;
- `fenced`: scopes whose session upload was deleted; never exported;
- `unboundTurns`: turns no rule can attribute; Neon-only, listed later.

Compare `rows` with the step 0 per-org counts. They are equal unless `--since`/`--until` are used.

Do not proceed from plan to export until the turns-forward migration above is
present in the live privacy database and the privacy preflight confirms all
six tables are fully visible through their reviewed SELECT policy and the register RPC is callable.

## 2. Export the canary, then all orgs

```sh
secrets | bun run migration:legacy-export --export --org=<canary org> $COMMON
secrets | bun run migration:legacy-export --export --all-orgs $COMMON
```

- **Resumable.** Ctrl-C (or a crash) stops between scopes. A rerun continues
  from the `(table, org, since, until)` cursor. A windowed pilot cannot mark
  the later full run complete. `--max-scopes` counts objects actually exported,
  so fingerprint skips do not livelock a resume.
- **Scoping.**
  - `--since=ISO` / `--until=ISO` restrict discovery to groups with rows in that window. The whole scope is still exported.
  - `--max-scopes=N` limits a trial.
- **After relayhistory goes `TURNS_WRITE_PATH=forward`,** run `--export --all-orgs --refresh $COMMON` once. It rediscovers everything, but re-exports only the scopes that changed.
- `--force` re-exports even unchanged scopes. It is not normally needed.

## 3. Verify (exit 2 on any mismatch)

```sh
secrets | bun run migration:legacy-export --verify $COMMON            # every exported scope in the checkpoint
secrets | bun run migration:legacy-export --verify --org=<org> $COMMON
```

For each exported scope the verifier:
- (a) runs the Neon SQL below and compares the row count and an md5 over the source keys (byte order) with the stored records;
- (b) rebuilds the same lines from Neon, passes them through the same scrub (`scrubRecords`), and compares an order-independent sha256 of the full stored records per table, so a scrubbed secret is not a false mismatch;
- (c) requires exactly one coverage marker.

The report is one JSON line:
`{ok, scopes, mismatched, statuses, unboundTurns, unexplained, privacyFenced}` (ids and
digests, no content). Failed statuses and unbound turns make `ok:false`;
privacy-fenced scopes are reported. Any failed or fenced checkpoint row that
retains a manifest key also makes `ok:false`: it is an object-cleanup
obligation, not an explained skip.

The Neon side of (a) is `NEON_SCOPE_SQL` in `src/migration/legacy-export/runner.ts`. For each table it runs:

```sql
SELECT '<table>' AS table_name, count(*)::text AS n,
  md5(coalesce(string_agg(t.<key>::text, ',' ORDER BY convert_to(t.<key>::text,'UTF8')), '')) AS keys_md5,
  md5(coalesce(string_agg(md5(to_jsonb(t)::text), ',' ORDER BY convert_to(t.<key>::text,'UTF8')), '')) AS rows_md5,
  coalesce(floor(extract(epoch FROM max(<time>))*1000)::bigint::text, '') AS max_ms
FROM sessions.<table> t
WHERE t.org_id=$1 AND t.workspace_id=$2 AND t.source=$3 AND t.session_id=$4   -- turns: the public source predicate
```

The keys are `id` for turns and links and `commit_sha` for outcomes.

## 4. Pointers and read side (read-only Supabase)

The S3-event session-pipeline Lambda indexes every new manifest. Check that each exported scope got its pointer:

```sql
-- as a read-only role on HoopDb/Supabase
-- Reconcile the exact manifest keys from the checkpoint. Legacy machines are
-- user-scoped srv_ ids and cannot be identified by a shared machine constant.
SELECT org_id, workspace_id, harness, session_id, user_id, manifest_key
  FROM sessions.session_index
 WHERE manifest_key IN ('<checkpoint manifest key>', '<repeat as needed>')
  ORDER BY org_id,workspace_id,harness,session_id;
```

The count should equal the number of `exported` scopes in the checkpoint:

```sh
sqlite3 ~/.agent-tmp/legacy-export/state.sqlite "SELECT org, count(*) FROM scopes WHERE status='exported' GROUP BY 1"
```

Then, for a canary session, the read API's `GET /v1/sessions/:id/turns` (PR 3, shadow) should match relayhistory.

## What stays on Neon

- **Unbound turns.**

  ```sh
  sqlite3 ~/.agent-tmp/legacy-export/state.sqlite "SELECT org, count(*), sum(turns) FROM unbound GROUP BY 1"
  ```

  They have no attributable source. relayhistory serves them only to a sourceless `GET /turns` or as part of an attributed scope's replay. The second case is exported.
- **Fenced scopes** are deliberately not exported.
- Do not infer the active writer from this runbook. Confirm
  `TURNS_WRITE_PATH` separately; this exporter never changes it.

## Rollback

There is nothing to roll back in Neon: it was read-only throughout.

Exported objects are not inert: the S3 event indexer may publish pointers
immediately, and canonical reads/output facts consume admitted legacy evidence.
Rollback therefore requires a separately approved erase of the exact
user-scoped `m=srv_…` prefixes plus pointer-tombstone verification. Never run
that erase from this exporter.

The same rule applies when a deletion/reset wins the final privacy recheck:
the object PUT has already completed. The runner aborts, records
`failed:legacy_privacy_race` plus the written manifest key, and verification
stays red. With Khaliq's separate approval, use the production eraser to delete
that exact manifest, its raw chunks, and its indexed pointer, verify fresh
list/read absence, then resume with `--refresh`. The exporter deliberately has
write-only transcript credentials and cannot perform this cleanup itself.
