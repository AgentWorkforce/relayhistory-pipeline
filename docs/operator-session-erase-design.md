# Operator session-erase front door (design, HELD)

## 1. Status and gates

- **HELD. Source-only design. Default-off.** Requested by layered-control (relay 245052487018340352) as a draft; Khaliq
  has NOT ordered it. Control acts only if Khaliq orders it after PR #338. Nothing here is approved to deploy, and
  **there is no canary until the full privacy gate exists.**
- **Flag:** `OPERATOR_SESSION_ERASE_ENABLED`. It is unset by default, and only the literal `"1"` turns it on (the
  repo's pattern for new behaviour, e.g. `SESSION_DESKTOP_CATALOG_ENABLED` in `src/session-pipeline/lambda.ts`
  l.27, l.141). While it is unset, the new route answers 503 `operator_erase_disabled` and does nothing else. This
  behaviour has its own flag. No existing flag, including `INGEST_PRIVACY_AUTHORITY` or `SEARCH_INDEX_ENABLED`,
  implies it.
- **Ordering:** PR #338 ("feat(read-api): per-hit R2 privacy gate for buffered search hits (default off)") must land
  first. Its gate is part of what makes a read-API 404 mean "unreadable everywhere the read API serves from" (section 4.6).
- **What this doc authorizes:** nothing. Any deploy, flag flip, migration apply, operator step, IAM or credential
  change needs Khaliq's separate, explicit OK in his own words (AGENTS.md). The standing code-integration order covers
  merging a future implementation PR only. It does not cover turning that PR on.
- **Peers:** control (AgentWorkforce/relay-control, owned by layered-control) is the only intended caller. The control
  half of the contract (section 5) is theirs, and this doc only states what the pipeline needs from it.

## 2. Scope and non-goals

**In scope:** one operator route that lets control perform the **existing** per-user reset (every owned session of
`(org, ws, user)`) for a workspace owner/admin. It reports the fenced count and a bounded session list, and gives
control a way to observe completion.

**Non-goals (v1):**

1. **No new erasure semantics.** The route drives the existing privacy-authority reset. It does not replace it
   (`runbooks/c3-supabase-privacy-authority.md`, "What changes" and "Reset coverage"). Whatever a reset fences, this
   fences. Whatever a reset leaves (section 4.6), this leaves.
2. **No workspace-scope request at the pipeline.** Control expands a workspace target into per-user resets (section 5).
3. **No per-session operator delete.** Selection (`enabled:false`) stays the user's own path.
4. **No synchronous byte deletion.** 404 means unreadable, not "bytes gone" (section 4.6).
5. **No Neon path.** The route works only under `INGEST_PRIVACY_AUTHORITY=supabase`. Under `neon` the Worker refuses to
   start with the flag set (section 4.1).
6. **No end-user caller.** End users keep relayhistory's `POST /v1/delivery/sessions/reset`.
7. **No undo.** A reset is one-way, as today.

## 3. Existing contract (what this builds on)

| Piece | Where | What it does |
|---|---|---|
| Front door today | relayhistory-api `POST /v1/delivery/sessions/{selection,reset}` (separate repo) with `DELIVERY_CUSTODY_SOURCE_MODE=supabase-authority` | authority begin, ordered fence with the ingest-built frozen witness, authority complete (runbook "What changes", table row "relayhistory-api") |
| Authority routes | ingest Worker `POST /v1/custody/authority/begin` / `complete`, `src/session-ingest/app.ts` l.620-655 | open only when `privacyAuthority === "supabase"`, otherwise 503 `privacy_authority_unavailable`. Tenant comes **only** from the signed identity, never the body |
| Ordered fence | ingest Worker `POST /v1/delivery/sessions/reset`, `app.ts` l.529-551 | `parseReset` (l.315-321) requires exactly `{account_id, request_id, confirmation:"DELETE ALL SESSIONS"}`. With a signed `control` it runs `applyOrderedControl` after building the frozen witness |
| Caller auth | `signedBindingIdentity`, `app.ts` l.55-108 | HMAC-SHA256 over version, time, method, path, identity header and body hash, with a 60 s window. Today the only signer is relayhistory's Worker, over a service binding. The ingest Worker has no public route (`app.ts` l.19-24) |
| Authority SQL | `supabase/migrations/20261105041000_ingest_privacy_authority.sql`: `ingest_privacy_authority_apply` l.369-438, `_begin` l.445-507, `_complete` l.510-550 | reset = fence every owned session of `(org, ws, user)` (owned copy UNION frozen records), record `(request_id, count)` in `ingest_frozen_source_resets`. A repeated `request_id` returns the recorded count (l.412-414). Complete moves the user's buffered stages to the head of the GC queue (l.544-546) |
| Refusals | same file l.474-475 (not active), l.488-490 (control pending), l.476 and l.486 (epoch) | all `55000`. The routes map every ledger error to 503 `delivery_unavailable` (`app.ts` l.256-268, `deliver.ts` l.154-159) |
| Read-side erasure | `docs/session-read-api-design.md` l.95-98, `src/session-read-api/liveness.ts` l.1-15, l.54-60 | a session whose `session_index` rows **all** carry `deleted_at` is 404 and is dropped from search. A session with **no** pointer row is not treated as erased |
| Pointer tombstones on reset | `20261104060000_ingest_canonical_publication.sql` l.450-478 (`ingest_publication_retire_fence`) | a new `upload_resets` row retires the user's publication heads and tombstones their `session_index` and directory rows |
| Byte removal | runbook "Reset coverage" rows "S3 copies" and "Buffered/staged copies"; `docs/session-ingest-design.md` l.113, l.142, l.215; `docs/buffered-upload-gc.md` | the eraser (`S3X_PRIVACY_AUTHORITY=supabase`) erases S3 copies. The buffered GC redacts staged copies. `upload_erasure_rearm` re-queues a session that a reset fenced mid-commit |

**Replay is not detectable today.** A repeated `request_id` after the first reset completed bumps the head version and
calls apply again. Apply finds the recorded row and returns its count, and begin returns `apply: true` (migration l.494-506).
That is the same answer as a fresh reset. Section 4.4 closes this gap.

## 4. Proposed route

### 4.1 Host and flag interaction

- **Host: the ingest Worker** (`src/session-ingest/`), not the read API. The ingest already holds the only role that may
  call `ingest_privacy_authority_{begin,complete}` (migration l.551-552). The read API role is read-only by
  construction (`liveness.ts` l.3-10), and widening it would break that boundary.
- **Path:** `POST /v1/operator/sessions/erase`.
- **Startup checks** follow the `worker.ts` l.233-235 pattern. With `OPERATOR_SESSION_ERASE_ENABLED=1` and
  `INGEST_PRIVACY_AUTHORITY` not `supabase`, the Worker throws at startup. The operator verifier key must also be configured
  (section 4.2).
- **Reachability is an open question (Q1).** The ingest Worker has no route, custom domain or workers.dev URL today.
  Control would need either a service binding (same account) or a dedicated authenticated ingress. This doc does not
  choose, because the repo does not say where control runs.
- **Orchestration.** The route runs, in-Worker, the same three steps relayhistory-api runs today. It uses the existing
  ledger methods (`ledger.ts` l.190-194): `privacyAuthorityBegin` → `frozenWitness` + `applyOrderedControl`
  (the reset fence) → `privacyAuthorityComplete`. It adds no SQL path for the fence itself.

### 4.2 Caller principal

- **Principal:** a dedicated control service principal with **its own signing key**, separate from relayhistory's ingest
  signing key, and its own scope (`rth:operator-erase`). The key is stored in the platform secret store, never on
  disk (AGENTS.md).
- **Verification** uses the `signedBindingIdentity` scheme (HMAC over method, path, identity and body hash, 60 s
  window, timing-safe compare) against the **operator** key only. A request signed with relayhistory's key is
  refused with 403 `wrong_principal`, and so is an end-user bearer.
- **Tenant source.** The tenant `(org, ws, user)` comes from the signed identity header that control builds per request.
  The body echoes it, and any mismatch is 400 `tenant_mismatch`. This matches the existing rule "tenancy only from the
  signed identity" (`app.ts` l.22-24, l.500).
- **Admin identity.** The workspace admin's identity is **not** a pipeline principal. Control proves admin and membership
  (section 6), and the pipeline records control's `actor_ref` as an opaque audit field only.

### 4.3 Request and response

Request (exact keys, refused otherwise, with `identifier` rules from `app.ts` l.296-299):

```json
{
  "request_id": "ctl-erase-<control job id>-<user ordinal>",
  "tenant": { "orgId": "...", "workspaceId": "...", "userId": "..." },
  "confirmation": "DELETE ALL SESSIONS",
  "actor_ref": "<opaque control audit id, 1..128 chars>"
}
```

200 response:

```json
{
  "request_id": "...",
  "count": 42,
  "replayed": false,
  "sessions": [{ "source": "claude", "session_id": "..." }],
  "sessions_truncated": false,
  "control": { "epoch": "...", "version": "...", "digest": "...", "kind": "reset", "outcome": "applied" },
  "correlationId": "..."
}
```

- **`count`** is the authority's recorded reset count: distinct `(source, session_id)` of the owned copy (migration
  l.422-423). It is unchanged on replay.
- **`sessions`** holds the fenced `(source, session_id)` set (owned copy UNION frozen records, migration l.418-420),
  sorted, capped at **1000**. When it is capped, `sessions_truncated: true` and control must use the listing check in
  section 4.5. This needs a future SQL change (section 4.4). The set is recorded once with the reset, so a replay returns
  the same list.
- **`control`** is the ordered-fence ack (`control-protocol.ts` l.4-5), passed through for diagnosis.

### 4.4 Idempotency and replay

- **Who mints `request_id`.** Control mints it once per (job, user). It is stored on control's job row and in the admin
  audit entry, and the same id is resent on every retry.
- **Semantics.** These are exactly the authority's reset replay semantics (runbook "What changes" l.21-25 and the "Reset
  coverage" row `session_resets`): a repeated `request_id` for the same `(org, ws, user)` returns the recorded
  `(request_id, count)` and fences nothing new. A reset with a **new** `request_id` that happens after uploads resumed
  is a new reset, as today.
- **Making replay detectable (future migration, slot from the registry).**
  - Extend `ingest_frozen_source_resets` with the capped fenced-session list.
  - Have `ingest_privacy_authority_begin` return `replayed: true` when the `FOUND` branch at migration l.413-414 is taken.
  - Until that ships, the route **must not** claim detection. In that state it returns `replayed: null`, and control
    treats a replay as "same count, same job".
- **Per-session erase ships separately.** The per-session erase (`sessions:[{source,session_id}]`) ships as a separate,
  held, default-off PR. It covers ClickHouse rows, S3 copies, buffered/staged copies, search docs, directory rows and
  PR evidence, under the same fence and witness as reset, consistent with the R2 v5.3 privacy gate.
- **Concurrency.**
  - **Same user, same `request_id`, in flight:** a second call while the head is pending with the same digest gets the
    pending head back (`apply:false`, migration l.488-493). The route re-drives fence and complete, which is safe because
    both are idempotent on `{version, digest}`.
  - **Same user, different `request_id`, while pending:** 409 `control_pending`.
- **Same `request_id` reused for a different tenant:** this is a distinct row (the key is `(org, ws, user, request_id)`),
  not a collision. Control must still never do it. The route logs it, and Q4 asks whether to refuse it outright.

### 4.5 Completion observation (control polls; nothing is pushed)

Control's job stays `pending` until the read API answers **404** for every targeted session.

1. **Sessions listed** (`sessions_truncated: false`): for each `(source, session_id)`, control polls
   `GET /v1/sessions/{session_id}/events?source={source}&limit=1` on the read API for that workspace. A 404 means
   erased (`liveness.ts` `ERASED_SQL`). 200, 501 and 5xx mean keep polling, with backoff, for up to a control-side
   deadline. After that the job goes to `stalled` and needs an operator. It is never marked done.
2. **Truncated, or as a cross-check**: `GET /v1/sessions?user={userId}&limit=...` (the directory list,
   `src/session-read-api/app.ts` l.716-776; `user` accepts up to 20 values, l.225) must page to **empty**.
   Caveats:
   - The directory is canary-gated (`directoryFor`, l.619-623). Off the canary it answers 501, which is "not observable",
     never "done".
   - It needs a fresh read after the reset's pointer tombstones (section 3).
3. **What control does not know** is the session ids. That is why the reset response carries them. v1 does not add a new
   pipeline "is user X empty" endpoint, because the two read-API calls above are enough once the read principal (Q2)
   exists.

**Gap (Q2):** the read API accepts only an end-user `rth:read` token for the workspace (`app.ts` l.114, l.419-420).
Control has no such principal for another user's sessions. Polling needs either a read-only control service principal
on the read API (with its own flag and review) or a narrow status route. This doc does not resolve it.

### 4.6 What 404 does and does not mean

**404 means "unreadable through the read API", not "bytes gone".** Lag and residue that remain after the 404:

- **S3 copies** (exports, live `srv_` objects): removed by the eraser in `supabase` mode from the union of
  `ingest_frozen_source_deletions` and `upload_deletions`, on its own sweep cadence (runbook "Reset coverage", "S3
  copies"; step 8).
- **Buffered/staged copies:** removed by the buffered GC, which the authority complete moves to the head of its queue.
  The runbook lists a healthy, armed GC as an activation blocker (runbook "Reset coverage", "Buffered/staged copies";
  `docs/buffered-upload-gc.md`).
- **Staged live batches:** erased after the 48 h redrive window (`docs/session-ingest-design.md` l.113). A reset that lands
  mid-commit is re-armed through `upload_erasure_rearm` (l.142, l.215).
- **Neon-era data:** Neon transcript data of reset sessions stays in Neon until Neon `sessions` is dropped (runbook "Known
  gaps" l.150, precondition 4).
- **Sessions with no pointer row** (backfilled history) are never 404 by the liveness rule (`liveness.ts` l.12-14).
  Control would wait on them forever, so they must be excluded or handled (Q3).
- **Buffered search hits:** covered only once PR #338's gate is live. That is why #338 lands first.

So control should have two states: `unreadable` (all 404) and `erasure_pending_bytes`. This doc does not give a "bytes
gone" signal, and the response must not imply one.

### 4.7 Errors (static codes; body shape `{ error: { code, message }, correlationId }`, `app.ts` l.214-216)

| Status | Code | When |
|---|---|---|
| 400 | `invalid_request` | not JSON, extra/missing keys, bad identifier, wrong confirmation |
| 400 | `tenant_mismatch` | body tenant differs from signed identity |
| 401 | `missing_authorization` / `invalid_token` | unsigned, stale or altered (existing verifier codes) |
| 403 | `wrong_principal` | signed with any key but the operator key, or missing `rth:operator-erase` |
| 404 | `not_found` | org, ws or user unknown to the authority **for this tenant**. Same body whether the workspace exists in another org or not at all (no cross-workspace existence leak) |
| 409 | `authority_not_active` | authority row not `active` for the current epoch (migration l.474-475, l.527-528) |
| 409 | `control_pending` | a different control is pending for this user (migration l.488-490) |
| 503 | `operator_erase_disabled` | flag unset |
| 503 | `delivery_unavailable` | any other ledger/fence failure (existing `failure()`). Retry with the same `request_id` |

**Two implementation notes for the future PR:**

- **Distinct SQLSTATEs.** The 409 cases share SQLSTATE `55000` with other refusals. Mapping by message text is fragile, so
  the future migration should raise distinct SQLSTATEs or return a structured refusal.
- **Defining "unknown user".** The authority has no "user exists" table. "Unknown user" can only mean "no owned/frozen
  rows and no prior reset". Answering 404 there versus 200 `count:0` is Q5. The recommendation is 200 `count:0`: it is
  idempotent, it does not reveal existence, and control has already checked membership.

## 5. Workspace expansion (control side) and why the pipeline stays per-user

- **Expansion.** Control expands a workspace target into one reset per current member. Each reset gets its own
  `request_id`, and all of them hang off one control job.
- **Job states.** The job is `unreadable` only when every per-user reset reached `unreadable`. Any 4xx or terminal error
  marks the job `partial`. Control does not roll back the other users, because resets are one-way.
- **The pipeline does not accept a workspace-scope request in v1.**
  - The authority's owned set, reset record, control head and pending rule are all keyed per `(org, ws, user)`
    (migration l.412-436, l.478-485). A workspace request would need a new multi-head transaction or a fan-out inside
    the pipeline, with its own partial-failure model.
  - Both duplicate what control already does with an audit trail.
- **Gap: former members (Q6).** Expanding over *current* members misses sessions owned by users who have left the
  workspace. The authority holds those rows, but control's membership list does not. Closing this needs either a
  control-side record of past members or a later, separately reviewed pipeline listing of distinct owners in `(org, ws)`.

## 6. Security review notes

- **Trust boundaries.**
  - Admin to control: `cli:auth`, plus a membership re-check before every pipeline call. Members get 403 and foreign
    workspaces get 404 at control.
  - Control to pipeline: the operator HMAC key and scope.
  - Pipeline to authority: the ingest DB role, with SECURITY DEFINER functions that take the tenant from their argument.
- **What control proves:** the admin's identity, role and membership, and that the target user belongs to the workspace.
  **What the pipeline re-checks:**
  - signature, window and key identity;
  - scope;
  - body tenant equals signed tenant;
  - flag and authority mode;
  - authority active (in SQL).
  The pipeline cannot re-check membership, because it has no membership data. That is a stated reliance on control
  (Q7: should the signed identity carry a control-issued membership attestation with an expiry?).
- **Cross-workspace isolation.**
  - Every authority statement binds `(org, ws, user)` from the signed tenant.
  - 404 and `count:0` bodies are identical for every unknown tenant.
  - Responses list only that tenant's sessions, and logs never include session ids.
- **Replay of a captured request** within 60 s yields the same idempotent reset and nothing more, which is the existing
  property of the scheme (`app.ts` l.61-67).
- **Audit fields** (one log line per request, no content):
  - `route: "operator.sessions.erase"`, `outcome`;
  - `org_id`, `workspace_id`, `request_id`, `actor_ref`;
  - `count`, `replayed`, `correlationId`.

  The durable audit stays control's job row. Q8 asks whether the pipeline should also persist `actor_ref` beside the
  reset row.
- **Bounds.**
  - Body at most 4 KiB.
  - One user per request.
  - `sessions` capped at 1000.
  - A per-principal rate limit: a proposed 10 requests per second and 1000 per hour, configurable, with 429 and
    `Retry-After` (reusing `DeliveryBusyError`, `app.ts` l.258).
  - At most one in-flight control per user, enforced by the pending head.
- **Fail-closed paths.** Flag off, authority not active, or a missing key all refuse before any write. A crash between
  begin and complete leaves a pending head. The same `request_id` resumes it, and new uploads for that user stay fenced
  by the pending rule.

## 7. Open questions (Khaliq / lead)

Q1, Q2, Q7 and Q8 are decided below (direction from relay-control draft PR #48, section 4). Q3-Q6 and Q9 stay open.

### Q1. Reachability: how does control reach the ingest Worker, and should the route live there at all?

**Decided: a dedicated private ingress on the existing ingest Worker.**

- **Host.** The route stays on the ingest Worker (section 4.1). There is no new service, and it does not move to
  relayhistory-api.
- **Ingress.** It is a dedicated private ingress that carries an operator credential (section 4.2). It is not reachable
  through the public upload path, and the Worker's existing routes gain no new exposure.
- **Flag.** The default-off flag is proposed as `OPERATOR_ERASE_ENABLED`. Section 1 names it
  `OPERATOR_SESSION_ERASE_ENABLED`, and the implementation PR fixes the final name. Either way the behaviour keeps
  its own flag, and only the literal `"1"` turns it on.

### Q2. Read principal: how does control poll the read API for another user's sessions?

**Decided: a control-class read principal, verified through the existing relaycontrol token introspection.**

- **Verification.** It uses the same introspection path as the `sh_at_` acceptance in #313. The pipeline mints nothing,
  adds no JWKS and runs no second verifier.
- **Token class.** The class is NEW. It needs pipeline-lead's ruling before implementation, and it needs its own flag
  and review on the read API (section 4.5).

### Q7. Membership attestation: should control sign one, or should the pipeline rely on control's check alone?

**Decided: a control-signed short-lived attestation, verified through the same introspection.**

- **Flow.** Relay-control issues the attestation and the operator presents it. The pipeline verifies it through the
  same introspection as Q2.
- **No local evaluation.** The pipeline never evaluates membership itself (section 6).
- **Bounds.** Expiry is bounded, and the proposal is 5 minutes. The attestation is bound to the request's org and
  workspace, and a mismatch is refused.

### Q8. Audit: should the pipeline persist `actor_ref` durably, or log it only?

**Decided: persist it durably, beside `request_id`.**

- **Migration.** `actor_ref` is stored in the same migration that extends `ingest_frozen_source_resets` for replay
  detectability (section 4.4). The slot is assigned at implementation, by the registry keeper when the implementation PR
  opens.
- **Emission.** `ingest_privacy_authority_begin` emits the capped session list and `replayed: true` from that same
  row.

### Still open

3. Pointer-less sessions. What does completion mean for backfilled sessions that have no pointer row?
4. Should a `request_id` reused across different tenants be refused (409) instead of being logged?
5. Unknown user. Should the pipeline answer 200 `count:0` (recommended) or 404?
6. Former members. Who closes the gap for users who have left the workspace (section 5)?
9. Who owns the future migration (replay flag, capped session list, distinct SQLSTATEs), and which registry slot does
   it take?

## 8. Test plan (the future implementation PR must ship this; no code now)

**Unit (Hono app, fake ledger):**

- Flag unset: 503 `operator_erase_disabled`, and the ledger is never called.
- Flag set with authority `neon`: startup throws. Missing operator key: startup throws.
- Auth and principal:
  - unsigned, stale and tampered requests get 401;
  - a request signed with relayhistory's key gets 403;
  - a missing scope gets 403.
- Request validation:
  - body/identity tenant mismatch gets 400;
  - extra keys or the wrong confirmation get 400.
- Error mapping and isolation:
  - `authority_not_active` and `control_pending` map to 409;
  - other ledger errors map to 503;
  - unknown tenant bodies are byte-identical across orgs.
- Response shape:
  - the cap and `sessions_truncated` are correct;
  - logs carry no session ids and no content.

**PGlite (authority SQL, extending `src/session-ingest/__tests__/privacy-authority.pglite.test.ts`):**

- Reset, then the same `request_id`: same count and same list, with `replayed: true`. A new `request_id` gets a fresh
  count.
- A concurrent second `request_id` while pending gets `control_pending`.
- A crash after begin resumes on retry.
- A reset of user A in ws1 leaves user A in ws2 and user B in ws1 untouched, including analysis and digest purges.
- `session_index` rows are tombstoned through the retire trigger, and `ERASED_SQL` reports the sessions erased.
- An authority that is not active refuses before any write.

**Real-PG proof job (PostgreSQL 16+, synthetic tenants only, never production):**

- The full begin → fence → complete sequence through the route, with grants checked so only the ingest role can call it.
- Read-API 404 observed for every listed session.
- The eraser dry run lists the sessions, and the buffered GC queue head holds the user's stages.

**Not covered here:** live end-to-end proof. It needs the privacy gate (#338 and the runbook's activation blockers)
and Khaliq's explicit OK.
