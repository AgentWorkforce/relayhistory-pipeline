# Contract: RelayHistory export plan entry, v1

Status: **proposal, code only** (E3). It binds three implementations, and each one must pass the test vectors in
[`relayhistory-export-plan-entry-v1.vectors.json`](relayhistory-export-plan-entry-v1.vectors.json):

- **RelayAuth #110** signs the entry digest into the migration assertion.
- **The #40 exporter** builds the plan and the uploads.
- **The ingests** derive the entry from the actual upload and compare it with the signed digest: relayhistory-cloud #228 (the Worker) and this repo's `src/alleyoop/manifests.ts`.

The reference implementation is `packages/pipeline/src/lib/pipeline/migration-assertions.ts`. It is byte-identical to the relayhistory-cloud copy.

## Plan entry

One entry is one upload: one session manifest. Every field is a JSON string unless marked as an integer.

| field | value | the ingest derives it from |
|---|---|---|
| `v` | integer `1` | constant |
| `grant_id` | the migration grant id | signed `meta.grantId` |
| `org_id` | the grant's org | signed `org` |
| `workspace_id` | the grant pair's workspace | signed `wks` (which must equal `meta.workspaceId`) |
| `source_user_id` | the grant pair's user | signed `sponsorId` (which must equal `meta.sourceUserId`) |
| `key` | v1 manifest key, **without** any deployment prefix: `v1/ws=…/m=…/h=…/d=YYYY/MM/DD/s=…/manifest.json` | the upload's key (commit) or the chunk/status request's session |
| `machine_id`, `harness`, `session_id` | the session identity | the key |
| `started_date` | `YYYY-MM-DD` (the key's `d=`) | the key |
| `content_sha256` | the manifest v1 `content_sha256` hash chain over the ordered chunk list `(epoch, start_offset, bytes, sha256)` | **recomputed** from the manifest's chunk list. It must also equal the declared `raw.content_sha256` |
| `chunk_count` | integer, number of chunks | the chunk list (it must equal `raw.chunk_count`) |
| `raw_bytes` | integer, sum of the chunks' source `bytes` | the chunk list |

The ingest never compares an echoed field. A manifest extension or a request field that only repeats the signed value proves nothing. The upload is accepted only when the ingest's own derivation hashes to the signed digest. This binds the key, the machine, the session, the content, the user and the workspace. A fresh key, another machine, other content, or another user's session cannot match.

## Canonical serialization

- JSON, with object keys sorted by Unicode code point and no whitespace.
- Strings are serialized as ECMAScript `JSON.stringify` does. Integers are safe and decimal.
- The entry uses no floats, `null` values, or absent optional fields.

## Digests

- `entry_digest` = lowercase hex `SHA-256(UTF-8("relayhistory.export.plan-entry.v1\n" + canonical_json(entry)))`.
- `plan_digest`, the grant's `planDigest`, = lowercase hex `SHA-256(UTF-8("relayhistory.export.plan.v1\n" + join("\n", sorted unique entry_digests)))`.

## The assertion (relayauth #110 amendment)

- The migration assertion carries one entry's digest as **`meta.planEntryDigest`**. This is a new meta field that this contract requires; the #110 amendment must add it.
- RelayAuth should mint only for an entry of the grant's plan whose `(source_user_id, workspace_id)` is the requested pair.
- `meta.planDigest` stays the grant's plan digest.

## Requests

- **Manifest commit (`POST /v1/pipeline/manifests`):** the entry is derived from the key and the manifest alone.
- **Chunks (`POST /v1/pipeline/chunks`) and status (`GET /v1/pipeline/manifests/status`):** these requests carry the planned content, which they cannot derive, in `X-Relay-Migration-Plan-Entry`. Its value is base64url (no padding) of `canonical_json({chunk_count, content_sha256, raw_bytes})`. Everything else comes from the request's session and the signed claims.
- **Chunk limits:** a chunk needs `seq < chunk_count` and `start_offset + bytes <= raw_bytes`. The ledger keeps one chunk per `(jti, seq)`. An identical repeat is idempotent; a different chunk at the same `seq` is 409 `migration_chunk_conflict`.

## Error codes (both ingests)

| status | code | meaning |
|---|---|---|
| 401 | `invalid_claims` / `invalid_token` | malformed, expired (5 s skew) or disabled migration token; a non-migration token on the dedicated path |
| 403 | `migration_assertion_route` | the route is not chunks, the commit or GET status |
| 403 | `migration_workspace_forbidden` | the signed pair is not the grant pair, or the target workspace or user is different |
| 403 | `migration_grant_forbidden` | the grant or its plan is not accepted by this ingest (`RELAYHISTORY_MIGRATION_GRANTS`), or the ledger's grant does not agree |
| 403 | `migration_plan_mismatch` | the derived entry is not the signed `meta.planEntryDigest` (or the chunk is outside it) |
| 409 | `migration_chunk_conflict` | another chunk is already at this seq under this assertion |
| 409 | `migration_assertion_replayed` | the `jti` or nonce is already consumed. Mint a fresh assertion; an identical retry is idempotent |
| 503 | `migration_ledger_unavailable` | there is no ledger, or the ledger failed (fail closed, nothing consumed) |

## Known limit

- The v1 entry covers a session's whole chunk list. A straddling session (O4: a `backfill` epoch appended to a live manifest) has live chunks that the plan cannot know.
- Those sessions need a v2 entry over the backfill epoch's chunks only. They are not admitted today: the derived content would not match.
