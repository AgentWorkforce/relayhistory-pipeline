# Session read API on Cloudflare Workers

The session read Worker is `packages/session-read-worker`. It serves the same `/v1` read contract as the #135 Lambda (`runbooks/sessions-prod-session-read-api.md`), with the same code: `src/session-read-api/{app,auth,cursor,page,queries,liveness}.ts`. Only the adapter is new: `packages/session-read-worker/src/index.ts` replaces `lambda.ts`. No Lambda sits on the request path.

It runs on the Cloudflare account Agent Workforce (`f7232cb80f6fab86a95426302af243e4`) as the Worker `bulls96-session-read-api`, exclusively at the custom domain `https://sessions-read.agentrelay.com`. Cloud reaches it through `SESSIONS_READ_96BULLS_URL` (cloud #5910). Switching Cloud from the Lambda to the Worker is that one variable. `workers.dev` is disabled: a same-account Worker-to-Worker fetch through it can fail with Cloudflare error 1042.

Only the lead deploys, sets secrets and creates the Hyperdrive config. Nothing in this file prints or stores a credential.

## What is the same as the Lambda

Every guarantee in the Lambda runbook holds unchanged, because the code is unchanged:
- tenancy from a verified RelayAuth assertion addressed to this Worker's origin;
- ClickHouse newest-version dedupe before value filters (#242 H1);
- cursors that never silently truncate (#251);
- the history floor (`READ_API_COMPLETE_SINCE`) and the `needs_full` partial-session check;
- erasure checks against `sessions.session_index`.

The same `b96e1:` cursors are issued and accepted. A cursor that the Lambda issued keeps working on the Worker, so a switch mid-pagination is seamless.

`packages/session-read-worker/vitest.config.ts` runs #135's suites unchanged inside workerd, and adds the Worker's full request path. That path covers:
- the deployed entry;
- RS256 assertions checked against a live JWKS;
- ClickHouse as a SELECT-only user;
- Supabase as `bulls96_read_api` through a Hyperdrive binding, with `supabase/bootstrap/session_read_api_role.sql` applied as written.

## Configuration

| what | where | value |
|---|---|---|
| `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Worker secrets (`wrangler secret put`) | 1Password `96Bulls-prod` / `clickhouse relay-search-prod session-read-api`: user `session_read_api`, SELECT on `transcript_events` and `search_ingest_session_state`, profile `search_read_guard` |
| `CLICKHOUSE_DATABASE` | `wrangler.jsonc` var | `relay_search` |
| `SUPABASE` | Hyperdrive binding; id from GitHub env var `SESSION_READ_HYPERDRIVE_ID` | Hyperdrive config over 1Password `96Bulls-prod` / `bulls96_read_api - Supabase sessions (<supabase-project>)`, which uses the session pooler `aws-0-us-east-1`, port 5432. The deploy preflight pins disabled caching, `verify-full`, CA id, project-qualified role and the complete non-secret origin tuple. |
| `READ_API_ORIGIN` | GitHub env var `SESSION_READ_ORIGIN` | `https://sessions-read.agentrelay.com`, exactly as Cloud's `SESSIONS_READ_96BULLS_URL` |
| `READ_API_COMPLETE_SINCE` | GitHub env var `SESSION_READ_COMPLETE_SINCE` | the same value as the Lambda's `sessionReadApi.completeSince` |
| `RELAYAUTH_JWKS_URL`, `RELAYAUTH_ISSUER` | `wrangler.jsonc` vars | `https://api.relayauth.dev/.well-known/jwks.json` and `https://relayauth.dev`, as on the Lambda since #158. Never leave them unset: the verifier's fallback JWKS does not resolve, so every token would be refused with a 401 |
| `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS`, `RELAYCONTROL_INTROSPECT_URL`, `RELAYCONTROL_INTROSPECT_SECRET` | unset (`wrangler.jsonc` var, and a Worker secret for the last) | relaycontrol `sh_at_` tokens, default off; leave unset in prod until the relaycontrol endpoint and the rollout window are agreed. Only exactly `true` enables; the URL must be https; `aud` = `READ_API_ORIGIN`, workspace = the request's single `workspace=` when given (the response's workspace is authoritative). Only `principal_type: "desktop-user"` is admitted. Unavailable introspection answers 503 `relaycontrol_unavailable`, including relaycontrol's 400 (contract drift or a History bug, logged `introspect_bad_request`), 401 (a wrong `RELAYCONTROL_INTROSPECT_SECRET`, logged `introspect_unauthorized`) and 404 (introspection disabled): every `sh_at_` read then 503s, so check the secret and the relaycontrol contract before suspecting tokens. See docs/local-stack.md "Relaycontrol user access tokens" |
| `READ_API_DIRECTORY_WORKSPACES` | GitHub env var `SESSION_READ_DIRECTORY_WORKSPACES` | the P3 canary allowlist (directory list/activity and session timing); unset = every P3 route answers 501 |
| `READ_API_BRIEF_TITLES_WORKSPACES` | GitHub env var `SESSION_READ_BRIEF_TITLES_WORKSPACES` | the F2 brief-titles allowlist (`GET /v1/sessions/titles`, docs/brief-titles.md); separate from the P3 canary; unset = 501 `brief_titles_workspace_not_enabled`, `*` = all |
| `PIPELINE_RAW_S3_BUCKET`, `_REGION`, `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY` (+ optional `_ENDPOINT`, `_SSE`), `PIPELINE_KEY_PREFIX`, `PIPELINE_ORG_FROM_PREFIX` | Worker secrets / vars | session timing's canonical S3 read: a GetObject-only key on the raw bucket, the same key layout the session-pipeline Lambda writes. Unset = timing answers 501 `timing_not_configured` |
| `READ_API_TIMING_ASSUME_NO_TURNS` | GitHub env var `SESSION_READ_TIMING_ASSUME_NO_TURNS` | `true` only for canary workspaces a read-only count showed have no Neon `conversation_turns` (see "Session timing" below); unset = such sessions answer 501 `timing_turns_unknown` |
| `RELAYHISTORY_READ_OWNER_BUFFERED`, `READ_API_OWNER_BUFFERED_WORKSPACES`, `INGEST_S3_BUCKET`, `INGEST_S3_REGION`, binding `HISTORY_CONTROL_HEAD` | rendered only by a `owner_buffered=on` dispatch (`scripts/render-deploy-config.ts`); bucket/region from GitHub env vars `SESSION_READ_INGEST_S3_BUCKET` / `SESSION_READ_INGEST_S3_REGION`; allowlist from the `owner_buffered_workspaces` input | Option C owner-buffered reads, default off. On adds the service binding to `relayhistory-api` entrypoint `SessionReadPrivacyControls` (pinned, not an input), the flag `true`, and the canary allowlist (explicit ids, each also on `SESSION_READ_DIRECTORY_WORKSPACES`; or exactly `*`, see "Owner-buffered all-workspaces mode" below). S3 reads use the `PIPELINE_RAW_S3_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` Worker secrets (the on deploy refuses unless both exist); GetObject + kms:Decrypt on `buffered-batches/` is a separate IAM grant. Flag on without the binding = every request 503. Any later dispatch without inputs deploys it off again. Prerequisites (PR A migration, IAM grant, relayhistory-cloud exporting the entrypoint) are separate approvals |
| `READ_API_OWNER_BUFFERED_EVENTS` | rendered only by a `owner_buffered_events=on` dispatch together with `owner_buffered=on` (`scripts/render-deploy-config.ts`) | Owner-buffered session events (see "Owner-buffered events"); unset = off; any value other than `on` = 503 `backend_unavailable` |
| `READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY` | rendered only by an `owner_buffered_stage_concurrency=<2..32>` dispatch together with `owner_buffered_events=on` (`scripts/render-deploy-config.ts`) | Bounded-concurrency S3 stage fetches for the owner-buffered events read (see "Owner-buffered events latency flags"); unset, empty, `0` or `1` = the serial loop; `2`..`32` = at most N in flight; any other value = 503 `backend_unavailable` |
| `READ_API_OWNER_BUFFERED_PHASE_LOG` | rendered only by an `owner_buffered_phase_log=on` dispatch together with `owner_buffered_events=on` (`scripts/render-deploy-config.ts`) | Per-phase times on the events request log line (see "Owner-buffered events latency flags"); exactly `1` enables, anything else is off |
| `READ_API_OWNER_BUFFERED_INDEX_VERIFY` | rendered only by an `owner_buffered_index_verify=on` dispatch together with `owner_buffered=on` (`scripts/render-deploy-config.ts`) | Owner-buffered reads first prove the out-of-band index `ingest_buffer_outbox_user_buffered_idx` exists and is valid (see "Owner-buffered index verify"); unset = off; any value other than `on` = 503 `backend_unavailable` |
| `READ_API_BUFFERED_SEARCH_GATE`, binding `HISTORY_CONTROL_HEAD` | rendered only by a `buffered_search_gate=on` dispatch (`scripts/render-deploy-config.ts`) | Per-hit privacy gate for buffered-origin search hits (#338), default off; see "Buffered search gate" below |

**Fail-closed defaults:**
- With no origin, every `/v1` request gets a 401.
- With a missing ClickHouse secret, a missing `completeSince` or a missing Hyperdrive binding, every request gets a 503.

Cloud falls back to relayhistory on a 503.

**Per-query bounds:** the Worker adds three ClickHouse settings to #135's own (`max_execution_time`, `max_result_bytes`, `result_overflow_mode`), as relayhistory-cloud's `search-clickhouse.ts` does:
- `cancel_http_readonly_queries_on_client_close=1`
- `max_threads=4`
- `use_skip_indexes=1`

## Owner-buffered all-workspaces mode

Default off; nothing changes unless a dispatch asks for it. Dispatch *deploy session-read-worker* with `owner_buffered=on` and `owner_buffered_workspaces=*`. The render (`packages/session-read-worker/scripts/render-deploy-config.ts`) then sets `READ_API_OWNER_BUFFERED_WORKSPACES=*` next to `RELAYHISTORY_READ_OWNER_BUFFERED=true`, the `HISTORY_CONTROL_HEAD` binding and the S3 vars, exactly as a canary on-render.

The render accepts `*` only when the GitHub env var `SESSION_READ_DIRECTORY_WORKSPACES` is itself exactly `*`. It refuses, before anything deploys:
- `*` while `SESSION_READ_DIRECTORY_WORKSPACES` is a list or unset (list the canary ids instead);
- `*` mixed with ids, or repeated (`*,ws-a`, `*,*`);
- `*` with `owner_buffered=off` or no mode.

An explicit canary list validates exactly as before.

**Privacy invariant.** The mode widens the workspace allowlist only. Every buffered row is still served only to its owner, through the same per-user gate:
- the SQL readers bind the verified user per call and gate on that user's own G2 parity row in `sessions.owner_buffered_parity` (rule 4, mandatory; `sessions.owner_buffered_read_scope`). Users without a G2 row stay hidden;
- the `HISTORY_CONTROL_HEAD` authority gate is observed for exactly that (org, workspace, user).

Tests: `src/session-read-api/__tests__/owner-buffered.routes.test.ts` ("all-workspaces mode does not bypass the per-user privacy gate").

To turn it off, dispatch again without inputs. Prerequisites and approvals are the same as for a canary: the PR A migration, the S3/KMS grant, the binding and the deploy itself each need Khaliq's explicit OK.

## Owner-buffered events

Default off; nothing changes unless a dispatch asks for it. Dispatch *deploy session-read-worker* with `owner_buffered=on` and `owner_buffered_events=on`. The render then also sets `READ_API_OWNER_BUFFERED_EVENTS=on`; `GET /v1/sessions/:id/events?source=...` then serves the caller's own buffered events for a session with no canonical pointer (header `X-Relayhistory-Buffered: 1`). The render refuses `owner_buffered_events=on` without `owner_buffered=on`, any value other than `off`/`on`, and a `wrangler.jsonc` that already sets the var. `off` (or no input) renders byte-identically to a deploy without the input. A Worker holding any other value answers every request 503 `backend_unavailable`.

Clients paging across the backend transition (canonical pointers appearing for a session first served from the buffer, or the reverse) get 409 `cursor_backend_transition` and must restart without a cursor (docs/session-read-api-design.md, "Owner-buffered events").

Turning it on is a production deploy: dispatch is Khaliq's call. To turn it off, dispatch again without the input.

### Owner-buffered events latency flags

Both default off and both need `owner_buffered_events=on` in the same dispatch (the render refuses either alone, any malformed value, and a `wrangler.jsonc` that already sets the var). Off renders byte-identically to a deploy without the input.

- **`owner_buffered_stage_concurrency=<2..32>`** renders `READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY=<n>`. The events read then fetches at most N S3 stage objects at once instead of one after another. Nothing else moves: the control head is observed before the stages SQL, the recheck runs after every fetch, the final observation is still the last I/O before any byte is sent, the fold runs in stage order, and a failing stage fails the request with the same error and status as the serial read (the lowest failing stage wins). A canceled request stops issuing new fetches and fails closed (503 `buffered_fenced`, reason `request_aborted`). The turns read stays serial. The Worker sees a canceled request only with the `enable_request_signal` compatibility flag, which `wrangler.jsonc` does not set today; without it fetches simply run to completion.
- **`owner_buffered_phase_log=on`** renders `READ_API_OWNER_BUFFERED_PHASE_LOG=1`. The events request log line then also carries, in whole milliseconds: `session_start_ms`, `control_head_1_ms`, `stages_sql_ms`, `s3_fetch_ms`, `recheck_ms`, `control_head_2_ms`, `fold_ms`, and the count `s3_stage_count`. A phase never reached has no field. A phase that threw adds `aborted_phase` (that phase), also when the route then answers as without the flag (for example the control head refusing before a candidate). A canceled request logs one line `reason: "request_aborted"` with the same fields and `aborted_phase` (the phase in progress, else `none`), and its request line does not repeat them. The fields are durations and one count only: no content, keys or ids beyond the line's existing `route` and `correlation_id`. `s3_fetch_ms` includes any refresh SQL after a missing stage object; `fold_ms` covers the record parse, the fold, normalize and the projection/scrub. Workerd does not advance timers during pure CPU work, so `fold_ms` (and other CPU-only phases) can read `0`.

Turning either on is a production deploy: dispatch is Khaliq's call. To turn them off, dispatch again without the inputs.

### Owner-buffered index verify

Default off and needs `owner_buffered=on` in the same dispatch (the render refuses it alone, any value other than `off`/`on`, and a `wrangler.jsonc` that already sets the var). Off renders byte-identically to a deploy without the input.

**`owner_buffered_index_verify=on`** renders `READ_API_OWNER_BUFFERED_INDEX_VERIFY=on`. Every owner-buffered read (list, turns, metadata, events) then first checks, through the read role and the buffered statement timeout, that `sessions.ingest_buffer_outbox` has a valid index `ingest_buffer_outbox_user_buffered_idx` (the out-of-band `CREATE INDEX CONCURRENTLY` from migration 20261105043700). A valid answer is cached for the isolate's lifetime; a failure is re-checked after 60 s. Not proven: the list is served canonical-only with `X-Relayhistory-Buffered: fenced` (log `buffered_fenced`), detail routes answer 501 as without owner-buffered reads (log `buffered_unverified`), with `gate` = `index_missing`, `index_invalid` (drop the invalid index before rebuilding it; `IF NOT EXISTS` skips it) or `index_unverified` (the probe failed or timed out). Building or dropping the index is a separate operator apply.

Turning it on is a production deploy: dispatch is Khaliq's call. To turn it off, dispatch again without the input.

## Buffered search gate

Default off; nothing changes unless a dispatch asks for it. Dispatch *deploy session-read-worker* with `buffered_search_gate=on`. The render then sets `READ_API_BUFFERED_SEARCH_GATE=on` and the same pinned `HISTORY_CONTROL_HEAD` binding (`relayhistory-api`, entrypoint `SessionReadPrivacyControls`; not an input). With `owner_buffered=on` as well, the binding is rendered once. The gate needs no workspace list and no S3 vars. The render refuses any value other than `off`/`on` and a `wrangler.jsonc` that already declares `services`, `env` or the flag. `off` (or no input) renders byte-identically to a deploy without the input.

The gate drops every buffered-origin search hit it cannot prove visible (control head for the hit's owner, original admission, G2 parity row, deletions and resets; `src/session-read-api/buffered-search-gate.ts`). Order matters: deploy the gate **before** the buffered search feeder (#339) is enabled, so a buffered row is never served without it. The flag only has an effect once #338 is on `main`.

Turning it on is a production deploy: dispatch is Khaliq's call. To turn it off, dispatch again without the input.

## Why Hyperdrive and not a direct TLS connection

Hyperdrive, with caching disabled and `verify-full` against the Supabase Root 2021 CA, for these reasons:

1. **The certificate is verified.** A direct `cloudflare:sockets` TLS connection from the Worker accepts only publicly trusted CAs. It cannot be given Supabase's private root, which the Lambda pins (`SUPABASE_PROD_CA_2021`). Hyperdrive accepts an uploaded CA (`wrangler cert upload certificate-authority`) with `sslmode=verify-full`, so the chain and the host name are both checked, as they were on the Lambda.
2. **Latency.** A direct connection pays TCP, the TLS handshake and SCRAM authentication to us-east-1 on every request, because isolates do not keep sockets between requests. Hyperdrive keeps warm connections next to the database, so a request pays one round trip to the nearest Hyperdrive.
3. **Pooler capacity.** Supavisor session mode gives each client its own backend session. Hyperdrive caps and reuses the connections the Worker opens instead of opening one per request.
4. **The credential stays out of the Worker.** The role's password lives in the Hyperdrive config, and the Worker only sees a per-request local connection string.

Tenancy is unchanged under pooling: every check is one `BEGIN READ ONLY … COMMIT` transaction that binds the tenant with transaction-local `set_config`. A pooled connection never carries a tenant into the next transaction; `worker.test.ts` asserts this through the binding.

**Caching must stay disabled.** Erasure must be seen at once, and a cached `session_index` read could serve an erased session.

## First deploy and rollout

The ClickHouse user and the Supabase role already exist: the Lambda uses them. Steps 1 to 3 run once.

1. **Check the ClickHouse profile.** In the ClickHouse Cloud SQL console:
   ```sql
   SELECT setting_name, value, min, max, writability FROM system.settings_profile_elements WHERE profile_name = 'search_read_guard';
   ```
   The profile must let the user set `max_execution_time`, `max_result_bytes`, `result_overflow_mode`, `cancel_http_readonly_queries_on_client_close`, `max_threads` (4) and `use_skip_indexes`. It must not be `readonly=1`, because `readonly=2` or grants alone are what allow settings. If it constrains one of them, every Worker read returns 503 with ClickHouse code 452 or 164; Cloud still falls back.
2. **Upload the CA, then create the Hyperdrive config.** The CA is public (`certs/supabase-prod-ca-2021.crt`). From `packages/session-read-worker`, with a Cloudflare token for the account in the environment:
   ```sh
   bunx wrangler cert upload certificate-authority --ca-cert ../../certs/supabase-prod-ca-2021.crt --name supabase-prod-ca-2021
   # Note the certificate id it prints, then create the config.
   # The connection URL goes from 1Password to the API on stdin, never into argv or a file.
   op read "op://96Bulls-prod/bulls96_read_api - Supabase sessions (<supabase-project>)/<connection URL field>" \
     | CA_ID=<certificate id> node -e '
         let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
           const u = new URL(s.trim());
           process.stdout.write(JSON.stringify({
             name: "bulls96-session-read-api-supabase",
             origin: { scheme: "postgres", host: u.hostname, port: Number(u.port || 5432), database: u.pathname.slice(1) || "postgres",
                       user: decodeURIComponent(u.username), password: decodeURIComponent(u.password) },
             caching: { disabled: true },
             mtls: { ca_certificate_id: process.env.CA_ID, sslmode: "verify-full" },
           }));
         });' \
     | curl -sS -X POST "https://api.cloudflare.com/client/v4/accounts/f7232cb80f6fab86a95426302af243e4/hyperdrive/configs" \
         -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" -H "Content-Type: application/json" --data-binary @- \
     | jq '{success, id: .result.id, caching: .result.caching}'
   ```
   `caching.disabled` must come back `true`. If the item name does not resolve in `op read`, use the item's ID.
3. **Create the GitHub environment** `session-read-worker` in AgentWorkforce/sessions-pipeline, with required reviewers. Then set:
   - secret `CLOUDFLARE_API_TOKEN`, scoped to this account with Workers Scripts:Edit and Hyperdrive:Read;
   - variables `SESSION_READ_HYPERDRIVE_ID` and `SESSION_READ_SUPABASE_CA_ID` (both from step 2), `SESSION_READ_COMPLETE_SINCE` (the Lambda's value) and `SESSION_READ_ORIGIN`.

   `SESSION_READ_ORIGIN` must be `https://sessions-read.agentrelay.com`. The matching `custom_domain` route is committed in `wrangler.jsonc`, and the deploy workflow refuses any other origin. Do not enable or use a `workers.dev` origin.
4. **Deploy.** Run *deploy session-read-worker* (`workflow_dispatch`, from `main`). Before deploying, it reads the live Hyperdrive config and refuses any config whose caching is enabled, TLS is not `verify-full`, CA id differs from `SESSION_READ_SUPABASE_CA_ID`, or origin differs from `postgres://bulls96_read_api.<supabase-project>@aws-0-us-east-1.pooler.supabase.com:5432/postgres` (password omitted). The project-qualified user is what binds this shared pooler connection to Supabase project `<supabase-project>`. The check logs field names and booleans only. It then renders the Hyperdrive id, deploys with the origin and `completeSince` vars, checks the secrets by name and smoke-tests the Worker.

   On the very first run, the secret check fails because the Worker had no secrets yet. Until they are set, the Worker answers 503 and is not on any request path.
5. **Set the Worker secrets** from 1Password, through stdin. Then rerun step 4 and confirm it is fully green.
   ```sh
   cd packages/session-read-worker
   for pair in "CLICKHOUSE_URL:<url field>" "CLICKHOUSE_USER:<user field>" "CLICKHOUSE_PASSWORD:<password field>"; do
     op read "op://96Bulls-prod/clickhouse relay-search-prod session-read-api/${pair#*:}" | bunx wrangler secret put "${pair%%:*}" --name bulls96-session-read-api
   done
   ```
6. **Smoke check.** Both of these are also in the workflow:
   - `curl -s -o /dev/null -w '%{http_code}' "$ORIGIN/health"` returns `200`;
   - the same request to `/v1/events?workspace=x&q=y` with no token returns `401`.
7. **Repoint Cloud to the Worker, in shadow first.**
   - Set Cloud's `SESSIONS_READ_96BULLS_URL` to the Worker origin, exactly `SESSION_READ_ORIGIN`. Cloud mints `aud` from it, so a mismatch is a 401 on every read.
   - If routes are already `on` against the Lambda, set them to `shadow` first, so relayhistory serves while the Worker is measured: `SESSIONS_READ_96BULLS_ROUTES=events_search=shadow,session_events=shadow,events_in_session=shadow`.
   - Keep the origin on `https://sessions-read.agentrelay.com`. The custom domain avoids the same-account `workers.dev` hop that can fail with Cloudflare error 1042.
8. **Watch the gates per route** (design doc section 5) in `[sessions-read-shadow]` for at least 30 minutes:
   - zero `crossTenantRows`;
   - 96Bulls error rate at most 1%;
   - p95 at most 1 s;
   - first-page event-id overlap of at least 99%;
   - the erasure check: delete a test session, then confirm a 404 and no search hit.
9. **Turn routes on,** one at a time: `events_search=on`, then the session routes.
10. **Retire the Lambda** after the Worker has served `on` for every route for a full day.
    - The sessions-prod plan contract refuses deletes, so this needs its own reviewed change: a `session read api retire` phase that deletes exactly the three read-API resources, applied by the lead.
    - Until it lands, the Lambda is idle but still deployed.
    - Keep `src/session-read-api/lambda.ts` until then, so the rollback below stays possible.

## Session timing (P3c)

`GET /v1/sessions/:id/timing` is computed from the session's canonical S3 objects, never Neon or ClickHouse:

1. the session's live pointer rows, read as `bulls96_read_api` (tenant-bound RLS) with the object-pointer columns `session_read_api_role.sql` grants;
2. the manifests and chunks they name, digest-checked (`canonical.ts canonicalSnapshot`, the same 16 MiB / 100k-event bounds as `/v1/canonical`);
3. the shared delivery projection (the port of Neon's delivery projection) and the timing port (`timing.ts`), which the parity suite holds equal to relayhistory's `session_timing_compute` v1.

Every case it cannot answer exactly is a 501 Cloud falls back on: `timing_workspace_not_enabled`, `timing_not_configured`, `timing_no_pointers`, `timing_turns_unknown`, `timing_canonical_<reason>` (including `timing_canonical_deadline` after 3 s, below Cloud's 4 s timeout) and `timing_<input>`.

Activation, in order (each step is the lead's):
1. Apply the widened grant: rerun `supabase/bootstrap/session_read_api_role.sql`, then `session_read_api_role_verify.sql`. Rollback: `REVOKE SELECT (canonical_session_id, machine_id, canonical_machine_id, user_id, storage_provider, bucket, manifest_key, content_sha256, raw_format, revision) ON sessions.session_index FROM bulls96_read_api;` and the previous verify file.
2. Give the Worker the raw-store secrets above (a GetObject-only key on the raw bucket prefix).
3. Count the canary workspace's Neon `conversation_turns` read-only. If zero, set `SESSION_READ_TIMING_ASSUME_NO_TURNS=true`; otherwise leave it unset until the legacy turn export (docs/canonical-legacy-source.md) has run.
4. Redeploy, then put the workspace on `SESSION_READ_DIRECTORY_WORKSPACES` (if it is not already).
5. In Cloud, `SESSIONS_READ_96BULLS_ROUTES` adds `session_timing=shadow`; watch `[sessions-read-shadow] route=session_timing` for `equal`, `shadowStatus` and `shadowMs`, then `session_timing=on`.

Expected shadow differences: Neon froze at the cutover, so a session with activity after it is more complete on S3 (`equal: false` there is not a regression). Compare sessions whose last activity predates the cutover.

## Session turns and metadata (P3d)

`GET /v1/sessions/:id/turns` and `/metadata` come from the same canonical S3 objects as timing, never Neon (`session-turns-source.ts`):
- every harness scope holding a pointer for the session id (at most 4; with `source`, only that one), read with `canonicalSnapshot`;
- its `conversation_turns` rows: the legacy export (`legacy_conversation_turn`) merged with live `conversation_turn` records from relayhistory `TURNS_WRITE_PATH=dual|forward` (`canonical-turns.ts`; a live record wins per (workspace, stored source, turn_index));
- de-duplicated by Neon row id, ordered as relayhistory orders them, `ts` as a JS Date serializes.

501s Cloud falls back on: `turns_workspace_not_enabled`, `turns_not_configured`, `org_scope` (no `workspace=`), `turns_no_pointers`, `turns_harness_bound`, `turns_unknown` (a scope without the export's coverage marker), `turns_truncated`, `turns_attribution`, `turns_canonical_<reason>` (including `_deadline` after 3 s). Erased sessions: `turns: []` and metadata 404, as relayhistory after erasure. A session with no turns: metadata 404 (Cloud falls back on any non-2xx in `on` mode).

Activation, in order (each step is the lead's), after the timing steps 1, 2 and 4 above:
1. Run the legacy turns export for the canary workspace (docs/canonical-legacy-source.md) and its verifier; without it every read is `turns_unknown`.
2. In Cloud, `SESSIONS_READ_96BULLS_ROUTES` adds `session_turns=shadow,session_metadata=shadow`; watch `[sessions-read-shadow] route=session_turns` / `session_metadata` for `equal`, then `on`. Only callers that send `workspace=` reach 96Bulls (Cloud's dashboard replay does not yet).
3. Expected shadow differences: a turn re-POSTed after `TURNS_WRITE_PATH=forward` carries the new request's row id (Neon keeps the first id); under `dual` ids match.

Read-only checks (Neon, any time):
```sql
-- turns the canary workspace holds, and how many sessions
SELECT count(*) AS turns, count(DISTINCT session_id) AS sessions, max(ts) AS newest
  FROM sessions.conversation_turns WHERE org_id = :'org' AND workspace_id = :'ws';
-- one session's rows in relayhistory's order (compare with the Worker's answer)
SELECT id, source, turn_index, role, ts FROM sessions.conversation_turns
 WHERE org_id = :'org' AND workspace_id = :'ws' AND session_id = :'session'
 ORDER BY turn_index, workspace_id COLLATE "C", source COLLATE "C", id;
```

## Rollback

- **Fastest:** set the route back to `shadow` or `off` in Cloud. This needs no Worker change.
- **Back to the Lambda,** while it still exists: set `SESSIONS_READ_96BULLS_URL` back to the Lambda's Function URL. Cursors are compatible both ways.
- **Bad Worker version:** `bunx wrangler rollback --name bulls96-session-read-api` restores the previous version.
