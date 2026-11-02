# Session read API on sessions-prod

The session read API is the Lambda in `stacks/aws/session-read-api.ts`, with code in `src/session-read-api/`. It serves relayhistory's `/v1` event reads from ClickHouse `transcript_events`, and checks erasure and the history floor in Supabase. Cloud calls it per route behind a flag (`SESSIONS_READ_96BULLS_*`) and falls back to relayhistory on any 501, 5xx or timeout.

Only the lead applies this, after independent review. Nothing in this file prints or stores a credential.

## What it serves (phase A)

| route | callers (Cloud) |
|---|---|
| `GET /v1/sessions/:id/events` | MCP `get_shared_session`, `list_team_activity`, Skip session events |
| `GET /v1/events?session=…` | MCP `get_shared_session_context` |
| `GET /v1/events?q=…` | MCP `search_shared_sessions` |

These requests get `501 unsupported_by_backend` with a `reason`, and Cloud then falls back:
- every other route;
- `includeMessageId`, `taskRef`, `tag`, `agent`, `noProject` or `projectContains`;
- org-wide reads (no `workspace`);
- ascending search;
- a search limit above 100;
- a relayhistory cursor;
- history older than `completeSince` (first page only, see below);
- a session whose latest `search_ingest_session_state` row has `needs_full = 1` (`session_partial`), and every workspace search while any session of the org has one (first page only, see below).

**Only a first page is ever a 501.** Once a cursor has been issued, Cloud cannot fall back without restarting the caller's pagination, so:
- The `needs_full` check runs when pagination starts. A cursor continues the read that check admitted.
- A search cursor page that reaches `completeSince` is a terminal `200`, flagged `X-Sessions-Read-Continuation: history_floor`, with `nextCursor: null` and `continuation: { backend: "relayhistory", reason: "history_floor", until }`. `events` holds every match from the floor up. `until` is one microsecond below the floor. Cloud fetches the rest of that page from relayhistory with the same parameters, `until`, and no cursor, appends it, and returns relayhistory's cursor, so the caller's pagination continues there without an error, a gap or a repeat.

## Guarantees and their tests

Each guarantee below has a mutation in `scripts/aws-hardening-mutations.ts` (`--only "read api"`, 26 of 26 caught). Reverting the control turns the named test red.

**Tenancy:**
- Org and workspace come only from the verified RelayAuth assertion. The token must be:
  - minted for this exact origin;
  - carry scope `rth:read`;
  - name its tenancy as `org` / `wks` / `sponsorId`, with no alternative claim names.
- A `workspace` parameter must equal the token's workspace.
- Every ClickHouse row is rechecked in code.
- Every Supabase row is fenced by an RLS policy bound to the tenant this transaction set.

**Newest version first:** value predicates (`q`, project) are re-applied to the newest version of each event, so a redacted version never surfaces through its stale predecessor (relayhistory-cloud #242 H1).

**No silent truncation:**
- A page that the byte budget (2 MiB) cuts, or whose candidate scan filled `limit`, carries a cursor.
- Pages are full while rows remain.

**Partial sessions:** `needs_full = 1` (a partial or capped pipeline pass, search-index.ts `recordState`) is read from the latest state row per session (`argMax(needs_full, synced_at)`). Such a session is never served as a complete transcript; a search starts at relayhistory while any session of the org is partial, because the state table has no workspace column.

**Erasure:**
- The pipeline's lightweight DELETE masks rows at once.
- A session whose pointer rows are all tombstoned is dropped from search and reads as empty, even before the pipeline runs.

## First apply (rollout)

1. **ClickHouse user.** In ClickHouse Cloud, create a user that has only `SELECT` on `relay_search.transcript_events` and `relay_search.search_ingest_session_state` (the `needs_full` check). Store it in 1Password. Without the second grant every first page is a 503, which Cloud falls back from.
2. **Supabase role.** As `postgres`, after the HoopDb migrations:
   ```sh
   psql -v ON_ERROR_STOP=1 -X -d "$HOOPDB" -f supabase/bootstrap/session_read_api_role.sql
   psql -X -d "$HOOPDB" -c '\password bulls96_read_api'
   psql -v ON_ERROR_STOP=1 -X -d "$HOOPDB" -f supabase/bootstrap/session_read_api_role_verify.sql   # NOTICE: PASS
   ```
3. **Inputs.** In `.96bulls/aws-sessions-prod/aws-inputs.json`, add:
   ```json
   "sessionReadApi": { "completeSince": "<backfill start, ISO UTC>" }
   ```
   Use the real backfill start (the search-index window start), not a guess. Use `"epoch"` only once full-history backfill is proven complete.

   **Account below the default Lambda quota.** A new account's concurrency limit is 10, and AWS keeps 10 unreserved, so the default reservation (10) is refused (`decreases account's UnreservedConcurrentExecution below its minimum value of [10]`). Only while that quota increase is pending, add `"reservedConcurrency": "none"`: the function then reserves nothing and shares the account pool with the session pipeline. `session-read-api-throttles-alarm` (`Throttles`, Sum > 0 over 5 minutes) shows when that pool is exhausted. Remove `"none"` once the quota is raised: the re-plan is a `session read api update` that sets the reservation to 10.
4. **Plan.** Capture the plan and record approval as for every sessions-prod change. The contract accepts only the phase `session read api rollout`: the read API's four resources (function, secret, errors and throttles alarms) are created, or the rest of them after a partial apply, and every other resource is `noop`. After the sessions-prod first apply that created only the errors alarm, the plan is `session-read-api`, `session-read-api-secret` and `session-read-api-throttles-alarm` create, `session-read-api-errors-alarm` noop.
5. **Apply.** Then set the secret value from 1Password:
   ```json
   { "clickhouse": { "url": "https://…:8443", "user": "…", "password": "…", "database": "relay_search" },
     "supabase_url": "postgresql://bulls96_read_api:…@…:5432/postgres" }
   ```
6. **Bind the origin.** Read the Function URL from the apply output, then:
   - set `"origin": "https://<id>.lambda-url.us-east-1.on.aws"` in the inputs;
   - capture again; the contract phase must be `session read api update`;
   - apply.

   Until this step, every `/v1` request returns 401.

## Checks after apply

- `curl -s -o /dev/null -w '%{http_code}' "$URL/health"` returns `200`.
- `curl -s -o /dev/null -w '%{http_code}' "$URL/v1/events?workspace=x&q=y"` returns `401`, because no token was sent.
- Turn on Cloud shadow mode for one route. The shadow log line `[sessions-read-shadow]` should show `crossTenantRows: false` and statuses for both backends.

## Rollback

- **Cloud:** set the route's flag back to `off`. This needs no AWS change.
- **Service:** if the service itself must stop, set reserved concurrency to 0 via a reviewed update, or remove `sessionReadApi` from the inputs. Removing it shows up as deletes, which the contract refuses, so it is a deliberate, separately approved change.
