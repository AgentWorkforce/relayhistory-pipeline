# Operator per-session erase: Control-to-ingest contract

Status: **contract, default off.** PR 1 (lane A) ships this document and a stub route that always answers
501. Nothing is erased until lanes B-D land, and each of their default-off flags is turned on with
Khaliq's explicit OK. Applying migrations, deploying and provisioning the secret also each need his OK
(AGENTS.md).

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
  self-host `SERVICE=session-ingest` serve the same route.
- Internal only. The self-host edge must never route `/internal/*` (`docs/selfhost-composition.md`
  section 2, rule 6). The self-host wrapper forwards the route to the app **without** relaycontrol
  introspection, because the app checks its own bearer.

## 2. Authentication

- `Authorization: Bearer <SESSION_ERASE_API_SECRET>`. This secret is separate from every read or
  introspection secret, and it is never reused for anything else.
- `SESSION_ERASE_API_SECRET_PREVIOUS` is accepted during a rotation. This is pipeline-side only: Control
  always sends the current secret. Rotation order: set `_PREVIOUS` = old and the secret = new on ingest,
  switch Control, then remove `_PREVIOUS`.
- Each secret must be at least 32 characters with no leading or trailing whitespace, and `_PREVIOUS`
  must differ from the current secret. Otherwise the configuration fails at startup (Worker: every
  request is 503; self-host: the process refuses to start). The error names the variable, never the value.
- The compare is constant time (SHA-256 digests, `secretMatches` from `self-issued.ts`). A missing,
  malformed or wrong bearer is **401 `invalid_token`** with `WWW-Authenticate: Bearer`.
- Auth runs before the path shape is checked, so an unauthenticated caller learns nothing about the route.
- The secret comes from the secret store (Worker Secrets Store binding, or a self-host Secret injected as
  env). It is never written to disk, logged or echoed.

## 3. Flag and gating

`SESSION_ERASE_API_ENABLED` must be exactly `1`. Unset, empty or `0` is off; anything else (`true`
included) is a configuration error. The secret is read and validated only while the flag is on, so a
dormant feature cannot take ingest down.

| Flag | Secret | Answer |
| --- | --- | --- |
| off (default) | any | **501**, nothing read, no auth |
| on | unset | **501**, nothing read, no auth |
| on | set | auth, then validation, then (PR 1) **501** |

The exact 501 body, in every case above:

```json
{"error":"not_implemented","feature":"session_erase"}
```

Nothing else changes with the flag off: no other route, middleware or log line is affected.

## 4. Request

POST body, `Content-Type: application/json`, at most **8 KiB** (413 `body_too_large` when declared or
streamed above it):

```json
{ "requestId": "<stable id from Control>", "actor": { "kind": "user" | "bootstrap", "id": "<actor id>" } }
```

- Exactly these keys, at both levels. Unknown keys, a wrong type, an empty value, or invalid UTF-8 or
  JSON give 400 `invalid_request`.
- `requestId` and `actor.id` are 1-128 UTF-8 bytes with no control characters. Control should send a
  UUID for `requestId`, and it must reuse it on every retry of the same erase.
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
| 501 | flag off, secret unset, or (PR 1) not implemented yet | `not_implemented` |
| 503 | a store is unavailable, or the owner cap is exceeded (section 7); `Retry-After` set | `unavailable` / `too_many_owners` |

- Every error body is `{"error": "<code>", "feature": "session_erase"}`. The correlation id is in the
  `X-Correlation-Id` response header.
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

- `status`: `in_progress | completed | failed`.
- `unreadable` / `unreadableAt`: the session is fenced and hidden in every store. Set as soon as the
  fence transaction commits.
- `bytesErased` / `bytesErasedAt`: every store's delete has been **verified** (not merely issued).
- **`status: "completed"` if and only if `unreadable` and `bytesErased` are both true**, and then
  `completedAt` is set. Otherwise the status is `in_progress` (or `failed`).
- `uploadsFenced`: true only after the fence transaction commits. From then on no claim, commit or index
  write can recreate the session (once lane B's M2 is enforced).
- `legacyScope`: always `"excluded"` in v1 (section 9).
- `failure`: `null`, or `{ "step": "<artifact key or fence>", "code": "<static code>", "retryable": true }`.

`artifacts` has exactly these keys. Each value is `{ "count": <integer >= 0>, "state": "done" | "pending" | "not_applicable" }`.

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

Flags, each default off:

- `SESSION_ERASE_API_ENABLED`: the route (exactly `1`).
- `SESSION_ERASE_FENCE_ENFORCED`: claim, commit and index consult the session fence.
- `SESSION_ERASE_BYTES_ENABLED`: byte deletion steps, hosted and self-host.

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
- **Configuration error blast radius.** An invalid flag value or a short secret with the flag on makes the
  Worker refuse every request (503), which is the repo's strict-flag convention. Keep the flag unset until
  the secret is provisioned.
- **Out of v1 (`legacyScope: "excluded"`):** Neon-era data and legacy ClickHouse rows.

## 10. Questions

Decided:

- Path carries workspace and source (section 1).
- `completed` means unreadable **and** bytes verified erased (section 6).
- Neon-era and legacy ClickHouse data are out of v1 (`legacyScope: "excluded"`).
- A pointer-less session gets a fence-only receipt with zero counts, never a 404.
- Self-host byte erasure is in v1 (lane C).

Open, with lead-desktop:

1. Error body shape: this route uses `{error, feature}`; the rest of ingest uses
   `{error: {code, message}, correlationId}`. Control to confirm.
2. How `actor.kind = "bootstrap"` maps to actor persistence (design doc Q8).
3. Ordering with the held per-user reset (#349) and the buffered-erasure sweeper (#383).
4. The owner cap value.
5. Whether keeping the acquisition-owner fence row counts as residue.
