# Operator per-session erase: Control-to-ingest contract

Status: **implemented for synchronous canonical self-host, default off.** Khaliq ordered
per-session hard-delete implementation on 2026-11-11 (via infra-chief). The automatic
runtime, activation prerequisites, supported scope and explicit refusals are documented
in [operator-session-erase-runtime.md](operator-session-erase-runtime.md). Production
migration application, deployment, credentials, IAM and every flag activation still
need his separate explicit OK (AGENTS.md). Hosted/unbound runtimes retain the 501 stub.

This is the internal contract between **relay-control** ("Control") and **session-ingest** ("ingest") for
the hard delete of one session. It is a sibling of `docs/operator-session-erase-design.md` (the per-user
reset, HELD), not a dependency of it. Code: `src/session-ingest/session-erase.ts`.

## 1. Route and erase key

```
POST /internal/v1/session-erase/organizations/{orgId}/workspaces/{workspaceId}/sources/{source}/sessions/{sessionId}
GET  (same path)
```

- **The erase key is `(orgId, workspaceId, source, sessionId)`.** A session id is unique only per
  source, and one logical session can have several users, origins and machine pointers. The erase covers
  all of them.
- Each segment is percent-encoded by Control and decoded by ingest. A decoded id must be 1-512 UTF-8
  bytes, well-formed, and contain no control characters. Otherwise the answer is 400 `invalid_request`.
- **Control validates that the workspace belongs to the org, and that the caller may erase it,** before
  calling. Ingest never calls Control and never reads or changes membership.
- Host: session-ingest. It owns the only write role, `bulls96_session_ingest`. The hosted Worker and the
  self-host `SERVICE=session-ingest` register the same route, but only self-host is reachable in v1 (next
  paragraph).
- Internal only. The self-host edge must never route `/internal/*` (`docs/selfhost-composition.md`
  section 2, rule 6). The self-host wrapper forwards the route to the app **without** relaycontrol
  introspection, because the app checks its own bearer. It forwards only GET and POST; any other method is
  answered 405 (`Allow: GET, POST`) by the wrapper and never reaches the app.

**Ingress in v1: self-host only.** The hosted Worker is **not reachable** for this route in v1. It has no
URL, route or custom domain; its only ingress is relayhistory's `DELIVERY_INGEST` service binding, and
relayhistory must **not** forward `/internal/*` over that binding (a tenant-facing service must never proxy
to a route whose only guard is the bearer). Hosted stays 501 until a reviewed ingress decision names the
mechanism (for example a dedicated Control service binding, or an exact-path allowlist that strips tenant
identity headers) without breaking the stack's "only way in is the service binding" invariant. Before the
flag may be set on the hosted Worker, the Cloudflare stack (`stacks/cloudflare/session-ingest.ts`) must add
`secretsStoreSecret` bindings for `SESSION_ERASE_API_SECRET` and `SESSION_ERASE_API_SECRET_PREVIOUS`, named
to match `SECRET_NAME` so `assertNoSecretLiterals` covers them. Self-host is the only v1 ingress.

## 2. Authentication

- `Authorization: Bearer <SESSION_ERASE_API_SECRET>`. This secret is separate from every read or
  introspection secret, and it is never reused for anything else.
- `SESSION_ERASE_API_SECRET_PREVIOUS` is accepted during a rotation. This is pipeline-side only: Control
  always sends the current secret. Rotation order: set `_PREVIOUS` = old and the secret = new on ingest,
  switch Control, then remove `_PREVIOUS`.
- Each secret must be at least 32 characters and contain no whitespace at all (the bearer is a single
  token compared exactly), and `_PREVIOUS` must differ from the current secret. An invalid secret or flag
  value is a configuration error, handled per runtime:
  - **Self-host:** the process refuses to start (a deploy-time failure). The error names the variable,
    never the value.
  - **Hosted Worker:** the erase config is isolated. Its secrets are runtime-mutable Secrets Store values,
    so an invalid value must never take down delivery or the queue consumer. The Worker logs only
    `error_code: "session_erase_config_invalid"` (never the value), disables erase with reason
    `config_invalid`, and the erase route fails closed: **503** `{"error":"unavailable","feature":"session_erase"}`
    with `Retry-After`, before any auth or body read. Every other route keeps working.
- The compare is constant time (SHA-256 digests, `secretMatches` from `self-issued.ts`). A missing,
  malformed or wrong bearer is **401 `invalid_token`** with `WWW-Authenticate: Bearer`.
- Auth runs before the path shape is checked, so once the feature is enabled an unauthenticated caller
  learns nothing about the path shape. The 501 discloses only that the feature is off.
- The secret comes from the secret store (Worker Secrets Store binding, or a self-host Secret injected as
  env). It is never written to disk, logged or echoed.

## 3. Flag and gating

`SESSION_ERASE_API_ENABLED` must be exactly `1`. Unset, empty or `0` is off; anything else (`true`
included) is a configuration error (section 2). The secret is read and validated only while the flag is on,
so a dormant feature cannot take ingest down.

| Flag | Secret | Answer |
| --- | --- | --- |
| off (default) | any | **501**, nothing read, no auth |
| on | unset | **501**, nothing read, no auth |
| on | set | auth and validation, then the self-host service; unbound/hosted stays **501** |
| invalid value, or on with an invalid secret (hosted Worker only) | any | **503** `unavailable` with `Retry-After`, nothing read, no auth |

The exact 501 body, in every 501 case above:

```json
{"error":"not_implemented","feature":"session_erase"}
```

Nothing else changes with the flag off: no other route, middleware or log line is affected.

## 4. Request

POST body, JSON, at most **8 KiB** (413 `body_too_large` when declared or
streamed above it):

```json
{ "requestId": "<stable id from Control>", "actor": { "kind": "user" | "bootstrap", "id": "<actor id>" } }
```

- Exactly these keys, at both levels. Unknown keys, a wrong type, an empty value, or invalid UTF-8 or
  JSON give 400 `invalid_request`.
- `requestId` and `actor.id` are 1-128 UTF-8 bytes with no control characters. Control should send a
  UUID for `requestId`, and it must reuse it on every retry of the same erase.
- A request authorised by Control's bootstrap bearer (no user) carries the actor
  `{"kind": "bootstrap", "id": "bootstrap"}`; `"bootstrap"` is a valid actor id.
- GET has no body.

## 5. Responses

| Status | When | Body |
| --- | --- | --- |
| 200 | POST or GET, receipt `status: "completed"` | receipt |
| 202 | POST, receipt `status: "in_progress"` | receipt |
| 200 | POST or GET, receipt `status: "failed"` (a later POST retries it) | receipt |
| 400 | malformed path id or POST body | `invalid_request` |
| 401 | missing or wrong bearer | `invalid_token` |
| 404 | GET, no erase recorded for the key | `erasure_not_found` |
| 404 | the path is not the route's shape | `not_found` |
| 405 | any other method (`Allow: GET, POST`) | `method_not_allowed` |
| 413 | POST body above 8 KiB | `body_too_large` |
| 501 | flag off, secret unset, or hosted/unbound runtime | `not_implemented` |
| 503 | erase config invalid on the hosted Worker (section 2), a store is unavailable, or the owner cap is exceeded (section 7); `Retry-After` set | `unavailable` / `too_many_owners` / `unsupported_storage` |

- Every response from the erase route itself is flat: `{"error": "<code>", "feature": "session_erase"}`,
  including 404 `erasure_not_found` (agreed with the relay-control owner). The correlation id is in the
  `X-Correlation-Id` response header.
- **Exceptions** (answers that do not come from the erase route, in ingest's envelope shape
  `{"error": {"code", "message"}, "correlationId"}`):
  - the self-host wrapper's generic 5xx (500 `internal_error`, 503 `not_configured`) and the hosted
    Worker's 503 `not_configured`. Control maps any generic 5xx to its retryable 503
    `session_erasure_unavailable` (501 stays 501; only non-5xx surprises become 502);
  - the self-host wrapper's 405 for a method other than GET or POST (`Allow: GET, POST`);
  - a path whose decoded form holds a control character (`%0A`) matches no route and gets the app's
    generic 404 envelope. Control rejects such ids with 400 before calling, so this is defence in depth
    only and never reaches the pipeline from Control.
- **There is no 404 `session_not_found`.** A key with no pointer, ownership, directory or buffered row
  still gets the session fence and a receipt with zero counts. A 404 would leave an unfenced race with an
  in-flight first upload.
- Every 5xx is retryable; Control retries with the same `requestId`.

## 6. Receipt

Ids and counts only. No transcript content, title, summary, path or secret. At most **16 KiB**.

```json
{
  "orgId": "...", "workspaceId": "...", "source": "...", "sessionId": "...",
  "requestId": "...",
  "status": "in_progress",
  "requestedAt": "2026-01-01T00:00:00.000Z",
  "completedAt": null,
  "unreadable": true,  "unreadableAt": "2026-01-01T00:00:00.100Z",
  "bytesErased": false, "bytesErasedAt": null,
  "uploadsFenced": true,
  "legacyScope": "excluded",
  "artifacts": { "transcriptObjects": { "count": 3, "state": "pending" }, "...": {} },
  "failure": null
}
```

- `orgId`, `workspaceId`, `source` and `sessionId` echo the requested erase key exactly (the decoded path
  values), including the requested `workspaceId` and `source`.
- `status`: `in_progress | completed | failed`.
- `unreadable` / `unreadableAt`: the session is permanently fenced and its exact authenticated
  read routes return 404. Set when the fence transaction commits; physical object and search
  cleanup continues asynchronously. Direct object access requires its existing credentials until verified deletion.
- `bytesErased` / `bytesErasedAt`: every store's delete has been **verified** (not merely issued).
- **`status: "completed"` if and only if `unreadable` and `bytesErased` are both true**, and then
  `completedAt` is set. Otherwise the status is `in_progress` (or `failed`).
- `uploadsFenced`: true only after the fence transaction commits. From then on no claim, commit or index
  write can recreate the session (once lane B's M2 is enforced).
- `legacyScope`: always `"excluded"` in v1 (section 9).
- `failure`: `null`, or `{ "step": "<artifact key or fence>", "code": "<static code>", "retryable": true }`.
  `failure.step` and `failure.code` each match `^[A-Za-z][A-Za-z0-9_.-]{0,63}$`.

`artifacts` has exactly these keys. Each value is `{ "count": <integer >= 0>, "state": "done" | "pending" | "not_applicable" }`.
Counts preserve recorded evidence across retries; external-store counts are conservative observed
counts, not an exact billing inventory after an ambiguous provider response. Completion is determined
by fresh absence verification, never by a count alone.

| Key | Store |
| --- | --- |
| `transcriptObjects` | transcript objects under the session prefix, live pointer JSON, replay and projection pages |
| `stagedObjects` | staged and buffered batch objects (outside the session prefix) |
| `bufferedBatches` | buffered batches naming the session |
| `bufferedRows` | buffered ClickHouse rows (`origin='buffered'`) |
| `pointerRows` | `session_index` pointers (every user, origin and machine) |
| `ledgerRows` | ownership, revision and publication rows |
| `directoryRows` | `session_directory` and its companion tables |
| `catalogRows` | desktop catalog projections |
| `largeReadJobs` | large-read jobs and scope errors |
| `searchDocuments` | search index documents (Postgres or OpenSearch) |
| `briefDocuments` | brief search documents |
| `summaries` | short summary and history summaries |
| `briefs` | session briefs, wakeups and attempts |
| `replayPublications` | replay publication rows |
| `analysisPublications` | analysis publication rows and payload objects |
| `historyDerivedObjects` | history-derived objects |

Fence records (session fence, `upload_deletions`, acquisition owners, the receipt itself) are kept by
design and are not artifacts.

A `completed` receipt carries **exactly these 16 artifact keys, none of them `pending`**.

**Control's checks:** a receipt with `status: "completed"` and any `pending` key is invalid; Control
treats it as a **502** from ingest and keeps polling. The same applies to `completed` with
`unreadable` or `bytesErased` false, or with an unknown or missing artifact key.

## 7. Idempotency and concurrency

- Control sends a stable `requestId`. A repeat POST for the key returns the existing receipt (200 or 202).
- **A different `requestId` never starts a second erase** while one exists for the key; it gets the
  existing receipt.
- A `failed` receipt is retryable: the next POST starts a new attempt under the same receipt.
- The fence transaction locks every owning user's fence in user-id order, before the ownership read.
- **Owner cap.** The number of owning users per session is unbounded. Above a fixed cap, the POST answers
  503 `too_many_owners` with `Retry-After`, and nothing is fenced. The cap is set in lane B and raised only
  by a reviewed change.
- Completion is asynchronous. S3 and ClickHouse deletes are eventually consistent, and staged objects sit
  outside the session prefix and wait for the staged window, so **a 202 can last up to about 48 h**.
  Control polls GET; ingest never pushes.
- Membership is never touched, and ingest never calls Control.

## 8. Plan

Lanes (supersede the six-PR split in the research plan):

| Lane | Content | Flag |
| --- | --- | --- |
| **A** | this contract and the 501 stub route (this PR) | `SESSION_ERASE_API_ENABLED` |
| **B** | SQL. **M1, slot 050000:** `session_erasure_receipts`, `session_erasure_events`, `session_erasure_fences`, and `ingest_operator_erase_session` (granted to the ingest role only), plus the GET read function. **M2, slot 050010:** the session-fence check in claim, commit and index | `SESSION_ERASE_FENCE_ENFORCED` (M2) |
| **C** | **self-host S3 byte eraser:** deletes the per-session prefix and the staged objects outside it, verifies the delete (lists every version), reports counts | `SESSION_ERASE_BYTES_ENABLED` |
| **D** | orchestrator and per-store steps (search, buffered rows, staged objects, transcript bytes, analysis, large-read, verification of trigger-driven stores) | `SESSION_ERASE_BYTES_ENABLED` |

**Prefix safety (lanes C and D).** A decoded id may contain `/` or be `..`-shaped. Byte deletion derives
object keys and prefixes only through the same encoder as writes (`objectKey`), terminates every prefix
with `/`, and never deletes by a prefix built from a raw decoded id. This keeps erasing `abc` from touching
`abc2`, and a `../`-shaped id from escaping the session prefix.

Flags, each default off:

- `SESSION_ERASE_API_ENABLED`: the route (exactly `1`).
- `SESSION_ERASE_FENCE_ENFORCED`: claim, commit and index consult the session fence.
- `SESSION_ERASE_BYTES_ENABLED`: byte deletion steps, hosted and self-host.

Self-host API or bytes activation also requires fence enforcement; enforcement refuses async/drain
admission. Deploy fence-aware ingest, consumers and readers consistently before API activation.
SQL fence enforcement remains permanent for recorded erasures even if API/byte flags are disabled.

Every new behaviour needs its own default-off flag: some stack defaults (for example the search index)
are already live, so the stack default is not dormancy.

## 9. Risks

- **M2 is on the hot upload path.** A regression blocks every upload. It ships behind
  `SESSION_ERASE_FENCE_ENFORCED` with function-boundary lane tests.
- **Self-host byte erasure.** There was no self-host byte eraser; it is now in v1 scope as lane C. Until
  it ships, a self-host receipt cannot reach `completed`; it stays `in_progress` with `unreadable: true`.
- **Completion is async** (up to about 48 h). Control must present `unreadable` and `completed` as
  different states.
- **Owner count is unbounded.** Capped with a retryable 503 (section 7).
- **Configuration error blast radius.** The hosted secrets are Secrets Store values another team can
  rotate without a deploy, so an invalid flag value or secret is isolated: the Worker logs
  `session_erase_config_invalid` (code only), the erase route answers 503 `unavailable` with `Retry-After`,
  and delivery and the queue consumer are unaffected. Self-host refuses to start, a deploy-time failure.
  Keep the flag unset until the secret is provisioned.
- **Out of v1 (`legacyScope: "excluded"`):** Neon-era data and legacy ClickHouse rows.

## 10. Questions

Decided:

- Path carries workspace and source (section 1).
- `completed` means unreadable **and** bytes verified erased (section 6).
- Neon-era and legacy ClickHouse data are out of v1 (`legacyScope: "excluded"`).
- A pointer-less session gets a fence-only receipt with zero counts, never a 404.
- Self-host byte erasure is in v1 (lane C).
- Error body shape: the erase route stays flat `{error, feature}`, 404 `erasure_not_found` included; the
  exceptions are listed in section 5 (agreed with the relay-control owner).
- The bootstrap bearer's actor is `{kind: "bootstrap", id: "bootstrap"}` (section 4).
- Hosted ingress: none in v1; self-host only (section 1).

Open, with lead-desktop:

1. How `actor.kind = "bootstrap"` maps to actor persistence (design doc Q8).
2. Ordering with the held per-user reset (#349) and the buffered-erasure sweeper (#383).
3. The owner cap value.
4. Whether keeping the acquisition-owner fence row counts as residue.
