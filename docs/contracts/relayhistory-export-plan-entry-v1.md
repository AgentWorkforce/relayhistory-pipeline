# Contract: RelayHistory export plan, plan entry and upload identity, v1

Status: **binding proposal, code only** (helm-chart-chief contract decision, E3). It is shared by:

- **relayauth #110**, the issuer: it stores the plan, derives the upload identity, and signs it.
- **The #40 courier**: `derivePlanContract`, the mint request's `upload`.
- **Both ingests**, which recompute the identity from the actual request:
  - relayhistory-cloud #228;
  - this repo's `src/alleyoop/manifests.ts` plus `packages/pipeline`.

Test vectors: [`relayhistory-export-plan-entry-v1.vectors.json`](relayhistory-export-plan-entry-v1.vectors.json). Every implementation must reproduce every value. They were cross-checked three ways:

- the reference implementation, `packages/pipeline/src/lib/pipeline/migration-assertions.ts`, byte-identical in relayhistory-cloud;
- the #40 courier's `derivePlanContract` at `2f17febc`;
- an independent Python implementation.

## Canonical form

- **Ids:** `org_id`, `workspace_id` and `source_user_id` carry no `relay_` prefix. Signed claims (`org`, `wks`, `sponsorId`) are normalized by stripping one leading `relay_` before any comparison. An upload identity whose ids are not canonical is a malformed token, and both "alias" vectors must be refused.
- **JSON:**
  - object keys are sorted with `Array.prototype.sort` (UTF-16 code unit order; every key here is ASCII);
  - `undefined` members are dropped, and there is no whitespace;
  - every other value is written as `JSON.stringify` writes it.

## Plan entry: one manifest upload

| field | meaning |
|---|---|
| `grant_id` | the migration grant |
| `key` | the v1 manifest key without any deployment prefix: `v1/ws=…/m=…/h=…/d=YYYY/MM/DD/s=…/manifest.json` (the work key's object) |
| `org_id`, `workspace_id`, `source_user_id` | the grant pair, in canonical form |
| `session` | `{machine_id, harness, session_id, started_at}`; `started_at` is the manifest's `session.started_at`, and its date must be the key's `d=` |
| `state` | `open` or `closed` (`raw.state`) |
| `chunks` | `[{seq, epoch, start_offset, bytes, sha256, records}]`, where `sha256` is the SHA-256 of the chunk's **source** bytes and `records` defaults to 0 |
| `content_sha256` | the manifest v1 `content_sha256` hash chain over `chunks` |
| `raw_bytes` | the sum of `chunks[].bytes` |

- `entry_sha256` = lowercase hex `SHA-256(canonical(entry))`.
- `plan_digest`, the grant's `planDigest`, = lowercase hex `SHA-256(canonical({v: 1, grant_id, exported_at, manifests: entries sorted by key}))`.
- This is the hash of the canonical **stored** plan.
- Two rules for the issuer:
  - It stores the plan when it creates the grant, and it checks `plan_digest` against the stored plan.
  - It mints an assertion only for an entry of that plan whose `(source_user_id, workspace_id)` is the requested pair.

## Upload identity: signed in `meta.upload`

The issuer derives this from the stored plan; it is never taken from client input:

```
{ grant_id, plan_digest, key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at},
  content_sha256, entry_sha256 }
```

It binds:

- the grant and its plan;
- the org and workspace;
- the source user;
- the machine and session key;
- the manifest (work) key;
- the content and source-chunk digest, through `content_sha256` and `entry_sha256`.

## What the ingest recomputes (never an echoed field)

| request | recomputed from the request | must equal |
|---|---|---|
| any | normalized signed `org`, `wks`, `sponsorId`; signed `meta.grantId`, `meta.planDigest` | `upload.org_id`, `workspace_id`, `source_user_id`, `grant_id`, `plan_digest` |
| `POST /v1/pipeline/chunks` | key, machine, harness, session id and date of the request's session | `upload.key`, `upload.session.*`, and the date of `started_at` |
| `GET /v1/pipeline/manifests/status` | the same, from `?key=` | the same |
| `POST /v1/pipeline/manifests` | the plan entry derived from the key and the validated manifest (session, state, chunk list, content recomputed over the chunks) | `upload.session`, `content_sha256`, `entry_sha256` |

Any difference is **403 `migration_upload_forbidden`**. A fresh machine key, a fresh session key, other content, or another user's upload cannot match.

## Other rules

- **Chunks:**
  - The ledger allows one chunk per `(jti, seq)`. An identical repeat is idempotent; any other chunk at that seq is 409 `migration_chunk_conflict`.
  - There are at most 256 chunks per assertion (403 `migration_chunk_limit`). 256 chunks of at most 4 MiB is 1 GiB, which is more than a courier can send in an assertion's 65 seconds; mint again for the rest of the session.
- **Expiry:** an assertion lives at most 60 s, with 5 s of skew. When `exp + 5 <= now`, the response is **401 `migration_assertion_expired`**: re-mint. Every other 401 (`invalid_token`, including a bad signature, and `invalid_claims`) is terminal.
- **Replay:** 409 `migration_assertion_replayed` means mint a fresh assertion; an identical retry is idempotent. 503 `migration_ledger_unavailable` fails closed.
- **Retention:**
  - There is no cron. Each ledger insert deletes at most 100 rows whose `expires_at` (`exp + 5 s`) passed more than 5 minutes ago.
  - An expired token is refused before the ledger is read, so a purged `jti` or nonce is never accepted again.

## Known limit

- v1 entries cover a session's whole chunk list. A straddling session (O4: a `backfill` epoch on a live manifest) needs an entry over the backfill epoch only.
- Until then such a session does not match, and it is refused.
