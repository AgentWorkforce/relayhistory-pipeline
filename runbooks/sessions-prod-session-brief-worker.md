# Session brief Worker (sessions-prod)

Model-written session titles and descriptions for sessions-prod, from the brief engine #127 added
(`src/session-brief`), run as the Cloudflare Worker `96bulls-session-brief` on a cron
(Khaliq, 2026-11-02: a Cloudflare Worker cron; global cap at the code default, 20000 model calls a
UTC day). sessions-prod has no container runtime, so the container service
(`docker/entrypoint.sh SERVICE=session-brief`) never ran there.

- **Stack:** `stacks/cloudflare/session-brief.ts`. Its own Alchemy config is
  `stacks/cloudflare/alchemy.session-brief.run.ts`: stack `96bulls-session-brief`, stage
  `cf-sessions-prod`. Same guards as #154's ingest stage: inputs file, reviewed capture, a bound
  approval and a plan contract. `destroy` is refused.
- **Entry:** `src/session-brief/cloudflare.ts`. It exports `scheduled` only. Every minute it runs
  one `briefTick` (`src/session-brief/worker.ts`, unchanged). The tick prunes stale wakeups, then
  claims at most `SESSION_BRIEF_BATCH` (5) due wakeups under a 300 s lease and briefs each one.
- **No way in:** no route, no custom domain, no workers.dev and no preview URL
  (`assertPrivateBriefWorker`).
- **Not included:** the historical backfill of about 120k sessions. `SESSION_BRIEF_SWEEP_SECONDS`
  stays `0`, so only sessions indexed after arming (step 9) get a brief.

## How it reaches what it needs

| Dependency | Path | Credential |
| --- | --- | --- |
| Supabase `pyamkrhoyszgzirqqffq` | `HYPERDRIVE` binding → the Supavisor **session pooler** `aws-0-us-east-1.pooler.supabase.com:5432` (the direct host is IPv6-only), as `bulls96_runtime`. Caching **off**, `verify-full` against the uploaded Supabase Root 2021 CA. | Lives only in the Hyperdrive config. The Worker gets a local connection string at runtime. |
| Transcripts bucket `96bulls-aws-sessions-prod-transcripts-941433935742` | SigV4 `GET` through the shared Locker store (`buildLockerStore`, static-key branch). | The existing `transcript-reader` IAM user (`ListBucket`; `GetObject` on `96bulls/*/v1/*`; `kms:Decrypt` via S3; explicit deny on put, delete and old versions). Secrets Store `96bulls-session-brief-s3-access-key-id` / `-s3-secret-access-key`. |
| OpenAI Responses API | `fetch`, 20 s timeout per call (engine). | Secrets Store `96bulls-session-brief-openai-api-key`. |

**Why Hyperdrive, not `pg` over TCP with the pinned CA.**
- Both #154 and #155 chose it.
- Workers' `cloudflare:sockets` TLS cannot be given Supabase's private root, so a direct connection
  could not verify the server. Hyperdrive takes the uploaded CA with `verify-full`.
- The password stays in Cloudflare.
- Connections to Supavisor are pooled and capped.

**Caching must stay off.** The tick reads wakeups, briefs and pointers that it then writes.

**Why the Secrets Store, not `wrangler secret put`.** This Alchemy version uploads every version
without `keep_bindings`, so a secret set on the script out of band would be dropped by the next
deploy. #154 hit the same problem.

## Database grants (checked against the code; verify live in step 1)

The Worker connects as `bulls96_runtime`, the #121 pipeline Lambda's runtime role. #127 added the
three brief tables to the exact runtime ACL that the migrator enforces (#116,
`src/hoopdb/migrate-guards.ts`).

| Object | Needed by the Worker | In the runtime ACL |
| --- | --- | --- |
| `sessions.session_brief_wakeups` | `SELECT`, `INSERT` (sweep, off), `UPDATE`, `DELETE` | `SELECT, INSERT, UPDATE, DELETE`, plus the `bulls96_trusted_backend` RLS policy |
| `sessions.session_briefs` | `SELECT … FOR UPDATE`, `INSERT`, `UPDATE` | same |
| `sessions.session_brief_attempts` | `SELECT`, `INSERT … ON CONFLICT UPDATE`, `DELETE` (prune) | same |
| `sessions.session_index` | `SELECT` (live pointers) | `SELECT, INSERT, UPDATE, DELETE` |
| `sessions.session_directory` | `SELECT`, `UPDATE` (title, `short_summary`, `search_text`) | `SELECT, INSERT, UPDATE, DELETE` |
| `sessions.session_directory_search_text(text×8)` | `EXECUTE` | in `RUNTIME_APPLICATION_FUNCTIONS` |
| `sessions.__96bulls_migrations`, `sessions.__96bulls_role_validation` | `SELECT` (the Worker's gate) | granted to the dedicated runtime role by the migrator |
| `pg_advisory_xact_lock`, `hashtextextended`, `gen_random_uuid` | built-ins (`PUBLIC`) | n/a |

`sessions.session_brief(text, text, text)` is the read RPC for `session_directory_search_worker`.
The brief Worker does not call it and must not be granted it. The migration strips `EXECUTE` from
every other grantee on purpose.

**The Worker's gate.** Before claiming anything, the Worker checks two things:
- `upstream:20261102000000_session_briefs.sql` is in the migration ledger;
- the role-validation marker is `pass`.

A migrator sets the marker to `pending` while it runs, and the Worker pauses until the marker is
back to `pass`. The gate is lighter than the container's (no phase 2 re-run, no generation match),
because the Worker carries no migration files. The #121 Lambda has no gate at all.

## Runtime bounds

| Bound | Value | Why |
| --- | --- | --- |
| Raw bytes per session | `SESSION_BRIEF_MAX_SESSION_BYTES` = 16 MiB (code default 256 MiB) | An isolate has 128 MB. The cap is checked against the manifest's chunk sizes before any fetch. A larger session is recorded `failed` / `session_too_large`: no model call, nothing counted against the caps. Without the cap, one huge session would kill the isolate and its whole batch on every reclaim for 72 h. |
| Batch | 5 per invocation | At most 5 model calls of up to 20 s each, plus S3 and SQL, which is far below the 15 min cron wall limit. The 300 s lease outlasts a full batch. |
| CPU | `limits.cpuMs` 60 000 | Most CPU goes to zstd and JSON parsing of at most 5 × 16 MiB. The model call is wall time, not CPU. |
| Overlap | allowed | A tick that overruns the next cron only runs alongside it. Claims are `SKIP LOCKED` and finishes are lease-fenced. The global cap is soft by at most one extra batch. |
| Caps | code defaults | 20 000 model calls/day overall, 5 000 distinct sessions per org per day, 30 per session per day, one per session per 2 min. |

## Prerequisites

- PR 1 (this Worker) merged. Its deploy is step 7.
- PR 2 (`SESSION_BRIEF_ENABLED="true"` on the session pipeline Lambda) stays **unapplied** until
  step 9.
- Prod Supabase at generation `5ed11913…` (#127's tables). This is already the case per
  `docs/activation/supabase-upgrade-eff8b3be-to-5ed11913.md`.
- The Cloudflare account. **Recommended:** Agent Workforce `f7232cb80f6fab86a95426302af243e4`,
  where #155 runs the read Worker. The brief Worker needs no service binding, so any account works:
  `sessionBrief.accountId` decides, and its Hyperdrive config and Secrets Store must be in that
  same account.
  - If this is the account's first Alchemy stage, the first plan also deploys the state-store
    Worker `alchemy-state-store-96bulls`. That deploy is part of this approval.

## 1Password items (vault `96Bulls-prod`; values are never printed)

| Item | Status | Used for |
| --- | --- | --- |
| `openai session-brief sessions-prod`, field `credential` | **Proposed (create it).** No OpenAI item is referenced in this repo. If relayhistory's existing production `OPENAI_API_KEY` item is to be reused, use that item instead and record its name here. | Secrets Store `96bulls-session-brief-openai-api-key` |
| `sessions-prod transcript reader session-brief`, fields `access-key-id` and `secret-access-key` | **Create** in step 3, with the transcript store runbook's recipe. | Secrets Store `96bulls-session-brief-s3-*` |
| The HoopDb runtime role item for `bulls96_runtime`: the same item `runbooks/sessions-prod-session-pipeline.md` reads `hoopdb_url` from (likely `bulls96_runtime - Supabase sessions (pyamkrhoyszgzirqqffq)`; confirm with `op item list --vault 96Bulls-prod`, names only) | Exists. | The Hyperdrive origin (step 2) |

## Deploy (each step is the lead's, in order; cloud-handoff on linuyc, `op` via `ssh kjg-lap`)

**1. Verify the grants (read only).** Connect as `bulls96_runtime` itself through the session
pooler, with the pipeline's URL from 1Password over stdin. Run:
```sql
SELECT t, p, has_table_privilege(current_user, t, p) AS ok
  FROM unnest(ARRAY['sessions.session_brief_wakeups','sessions.session_briefs','sessions.session_brief_attempts']) AS t,
       unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE']) AS p
UNION ALL SELECT 'sessions.session_index', 'SELECT', has_table_privilege(current_user, 'sessions.session_index', 'SELECT')
UNION ALL SELECT 'sessions.session_directory', 'UPDATE', has_table_privilege(current_user, 'sessions.session_directory', 'UPDATE')
UNION ALL SELECT 'session_directory_search_text', 'EXECUTE',
  has_function_privilege(current_user, 'sessions.session_directory_search_text(text,text,text,text,text,text,text,text)', 'EXECUTE')
UNION ALL SELECT 'session_brief (must be false)', 'EXECUTE', has_function_privilege(current_user, 'sessions.session_brief(text,text,text)', 'EXECUTE');
SELECT EXISTS (SELECT 1 FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261102000000_session_briefs.sql') AS ledgered,
       (SELECT status FROM sessions.__96bulls_role_validation WHERE name = 'session_directory_search_worker') AS marker;
```
- **Pass:** every `ok` is true except the `session_brief` row, which must be false; `ledgered` is
  true; `marker` is `pass`.
- **On any miss, stop.** The migrator's `--activation-check` ACL drift preview (#147) shows the
  difference. A miss would also break PR 2, because the indexer's `armWakeup` writes
  `session_brief_wakeups` inside the pointer upsert.

**2. Hyperdrive.** Upload the CA once per account (skip this if #155 already did), then create
the config. The connection URL goes from 1Password to the API on stdin: never argv, never a file.
```sh
bunx wrangler cert upload certificate-authority --ca-cert certs/supabase-prod-ca-2021.crt --name supabase-prod-ca-2021
ssh kjg-lap "zsh -lc 'op read \"op://96Bulls-prod/<bulls96_runtime item>/<url field>\"'" \
  | CA_ID=<certificate id> node -e '
      let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
        const u = new URL(s.trim());
        if (u.hostname !== "aws-0-us-east-1.pooler.supabase.com" || (u.port || "5432") !== "5432") { console.error("not the session pooler"); process.exit(1); }
        process.stdout.write(JSON.stringify({
          name: "96bulls-session-brief-supabase",
          origin: { scheme: "postgres", host: u.hostname, port: 5432, database: u.pathname.slice(1) || "postgres",
                    user: decodeURIComponent(u.username), password: decodeURIComponent(u.password) },
          caching: { disabled: true },
          mtls: { ca_certificate_id: process.env.CA_ID, sslmode: "verify-full" },
          origin_connection_limit: 5,
        }));
      });' \
  | curl -sS -X POST "https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID/hyperdrive/configs" \
      -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" -H "Content-Type: application/json" --data-binary @- \
  | jq '{success, id: .result.id, caching: .result.caching, user: .result.origin.user}'
```
- `caching.disabled` must come back `true`, and `user` must be `bulls96_runtime.pyamkrhoyszgzirqqffq`.
- Note the `id`; it is `hyperdriveId`.
- If the API refuses `origin_connection_limit` on this plan, drop it. The Worker opens at most 2
  connections per invocation.

**3. The transcript-reader key.** Create a dedicated key for this consumer, so CloudTrail
attributes its reads. A user holds at most two keys.
- Check first with `aws iam list-access-keys --user-name 96bulls-aws-sessions-prod-transcript-reader`.
- If two keys already exist, stop and decide which consumer to rotate.
- Then follow `runbooks/sessions-prod-transcript-store.md` "Credentials", with
  `U=96bulls-aws-sessions-prod-transcript-reader` and `T="sessions-prod transcript reader session-brief"`.

**4. Secrets Store.** Find the account's store id (`bunx wrangler secrets-store store list`), then
create the three secrets with scope `workers`. Each value goes from `op read` to the API on stdin:
```sh
put() {  # put <secret name> <op:// reference>
  ssh kjg-lap "zsh -lc 'op read \"$2\"'" \
    | jq -Rs --arg n "$1" '[{name: $n, value: (. | rtrimstr("\n")), scopes: ["workers"], comment: "96bulls-session-brief"}]' \
    | curl -sS -X POST "https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID/secrets_store/stores/$STORE_ID/secrets" \
        -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" -H "Content-Type: application/json" --data-binary @- \
    | jq '{success, names: [.result[]?.name]}'
}
put 96bulls-session-brief-openai-api-key        "op://96Bulls-prod/openai session-brief sessions-prod/credential"
put 96bulls-session-brief-s3-access-key-id      "op://96Bulls-prod/sessions-prod transcript reader session-brief/access-key-id"
put 96bulls-session-brief-s3-secret-access-key  "op://96Bulls-prod/sessions-prod transcript reader session-brief/secret-access-key"
```

**5. Inputs.** Write `.96bulls/cf-sessions-prod/session-brief-inputs.json`. It is gitignored and
holds ids only:
```json
{ "sessionBrief": { "accountId": "<32 hex>", "hyperdriveId": "<step 2>", "secretsStoreId": "<step 4>" } }
```

**6. Capture and review.** From a clean tree at the merged `main`, with `CLOUDFLARE_ACCOUNT_ID` and
`CLOUDFLARE_API_TOKEN` (from 1Password) in the environment:
```sh
bun scripts/cf-session-brief-capture.ts
```
- The script runs `alchemy plan` twice: compact, then `--detailed`.
- It refuses any plan the contract does not accept. The plan must read **session brief rollout**:
  `[session-brief] create`, the only resource, with no `secret_text` and no credential in the
  detailed plan.
- It writes `docs/plans/cf-sessions-prod.session-brief.{plan.txt,plan.detailed.txt,provenance.json}`
  and prints `approval.capture`.

In the detailed plan, check:
- `crons: ["* * * * *"]`, `workersDev: false`, `routes: []`;
- the Secrets Store bindings (store id and secret names only);
- `HYPERDRIVE` by id;
- `SESSION_BRIEF_ENABLED: "true"`, `SESSION_BRIEF_GLOBAL_DAILY_CAP: "20000"`,
  `SESSION_BRIEF_MAX_SESSION_BYTES: "16777216"`.

Commit the three files.

**7. Approve and deploy.** Write `approval` into the inputs: `decision`, `by: "Khaliq Gant"`,
`date`, `quote`, and `capture` from step 6. Then run:
```sh
bunx alchemy deploy --config stacks/cloudflare/alchemy.session-brief.run.ts --stage cf-sessions-prod
```
- The deploy refuses unless the approval binds the committed capture and the plan passes the contract.
- **Check:** `96bulls-session-brief` exists with one cron trigger, no route, no workers.dev and no
  domain. Its settings list `HYPERDRIVE` and three `secrets_store_secret` bindings.

**8. Watch it run dark** (no wakeups are armed yet) for a few minutes. In Workers Logs for
`96bulls-session-brief`, one line per minute should read
`session brief invocation` with `status: "ticked"`.

| Status | Meaning |
| --- | --- |
| `ticked` | Working (with nothing to claim yet). |
| `not_configured` | `reason` names the missing binding (step 2 or 4). |
| `gated` | `schema_pending` / `validation_pending`: the step 1 checks, or a migrator is running. `gate_error:<SQLSTATE>`: Hyperdrive or grants. |
| `model_not_configured` | The OpenAI secret is unset. |

**9. Arm (PR 2).** Merge PR 2, then capture and apply `aws-sessions-prod` per
`runbooks/sessions-prod-session-pipeline.md`. The contract classifies it as **session pipeline
update**: `session-pipeline` adds `SESSION_BRIEF_ENABLED: "true"` to its env, and the five grants
show only the role-ARN cascade.
- From then on, each newly indexed revision arms one debounced wakeup. It is due 10 minutes after
  the last activity, and never later than 60 minutes after the first unbriefed activity.

**10. Verify.** Within about 15 minutes of real uploads:
- Workers Logs show `session brief generated` lines (`model_id`, `tokens_in` / `tokens_out`, ids
  only) and `session brief tick` counts.
- `sessions.session_briefs` gains `ready` rows.
- `session_directory.title` / `short_summary` change for those sessions.
- Run this read-only query:
  ```sql
  SELECT status, error, count(*) FROM sessions.session_briefs WHERE attempted_at > now() - interval '1 hour' GROUP BY 1, 2;
  ```
  `failed` rows should be rare and carry a code. A high `session_too_large` count means the
  16 MiB bound is too low for real sessions; raise it in a reviewed update, never above 32 MiB.
- Spend: `SELECT day, sum(attempts) FROM sessions.session_brief_attempts GROUP BY 1 ORDER BY 1 DESC LIMIT 3;`
  stays under 20 000 a day.

## Order, and what arming alone would do

Deploy the Worker (step 7) **before** arming (step 9). Arming without a Worker is harmless but untidy:
- each indexed revision upserts one row per logical session in `session_brief_wakeups`;
- no model is called and nothing else changes;
- nothing prunes those rows (the Worker's tick does).

When the Worker starts later, it briefs the due rows at no more than 5 per minute, within the caps.
Wakeups with no new activity for 72 h are dropped unbriefed.

## Rollback

| Goal | Action |
| --- | --- |
| Stop model calls at once, keep the Worker | Delete or blank the OpenAI Secrets Store secret, or set it to a value ending in `-disabled`. The tick then prunes only and claims nothing. Takes effect on the next invocation, with no deploy. |
| Turn the Worker off | Set `"enabled": false` in the inputs. Capture again (phase **session brief update**: `SESSION_BRIEF_ENABLED: "false"`), approve, deploy. |
| Stop arming | Revert PR 2 (a session pipeline update). Existing wakeups are briefed or pruned. |
| Bad Worker version | Deploy the previous commit through steps 6 and 7. The contract never accepts a delete, so there is no Alchemy destroy. |

Briefs are additive. The directory keeps the pointer title when no brief exists, and deleting a
session deletes its brief (`session_briefs_forget`).
