# Contract: relayauth issuer changes for plan entry v2 (backfill epochs)

Status: **draft for contract review.** This is the spec the relayauth #111 implementation follows. Contract v1 and its vectors are unchanged.

- **Data contract:** `relayhistory-export-plan-entry-v2.md`, plus its vectors (96BullSessions #79).
- **Ingest side:** #79 for 96BullSessions, and relayhistory-cloud #234 (stacked on #228). The Worker refuses v2.

## 1. Plan shape

The grant-creation request's `plan` gains an optional list:

```
plan: { exported_at, manifests: [v1 entry inputs...], backfills?: [v2 entry inputs...] }
```

### v2 entry input

The input has exactly these keys and nothing else:

```
{ v: 2, kind: "backfill", key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at},
  format: "ndjson",
  chunks: [{start_offset, bytes, sha256, records}] }
```

### What the issuer checks

Checks are made per entry, and any failure gives 400 `invalid_plan`:

- **Ids:** they are canonical, using `canonicalMigrationId`, as for v1. `org_id` must be the grant's org.
- **Key:** it matches the v1 manifest key shape. Its `ws=`, `m=`, `h=`, `s=` and `d=` segments must equal `workspace_id` and the session's machine id, harness, session id and the date of `started_at`.
- **Format:** `format` is `"ndjson"`.
- **Chunks:** there are 1 to 4096 of them, and they count toward the 50,000 total. Each chunk is `{start_offset, bytes ≥ 1, sha256 (64 lowercase hex), records}`:
  - **`records` is mandatory** and must be an integer of 0 or more;
  - `start_offset` runs contiguously from 0 (each chunk starts where the previous one ended);
  - no other key may appear.
- **Derived fields:** the issuer adds `grant_id`, computes `backfill_sha256` (the manifest v1 content hash chain over the chunks with every epoch set to 0) and computes `raw_bytes` as the sum of `bytes`. **A client-supplied `backfill_sha256` or `raw_bytes` is refused.**
- **Keys:** every key is unique across `manifests` and `backfills` together. A session is either a cold export or a backfill, never both.
- **Allowed pairs:** they come from both lists.

### The stored v2 entry

```
{ v: 2, kind: "backfill", grant_id, key, org_id, workspace_id, source_user_id,
  session, format: "ndjson", chunks, backfill_sha256, raw_bytes }
```

- `entry_sha256` = lowercase hex `SHA-256(canonicalMigrationJson(entry))`.

## 2. Plan digest

- **With no backfills (or an empty `backfills`):** unchanged. It is v1's `SHA-256(canonical({v: 1, grant_id, exported_at, manifests}))`, and every existing grant and vector keeps its digest.
- **With at least one backfill:** `SHA-256(canonical({v: 2, grant_id, exported_at, manifests, backfills}))`. Both lists are sorted by key, and `manifests` holds the v1 entries exactly as before.
- **Stored plans:** `verifyStoredMigrationGrantPlan` recomputes the digest for both shapes. The v2 vectors' `plan` block is the test vector.

## 3. Mint requests

`POST /v1/tokens/migration-assertion` keeps the same caller, key scope, `grantId`, `sourceUserId`, `workspaceId` and `nonce` as today. `upload` takes one of three forms, and any other form gets 400 `invalid_upload_selector`:

| `upload` | refers to | signed `meta.upload` |
|---|---|---|
| `{key}` | a `manifests` entry | the v1 identity, unchanged |
| `{key, kind: "backfill_base_read"}` | a `backfills` entry | the **base-read capability** (§4) |
| `{key, base: {revision, version, owner, writer, chunk_count, epoch, content_sha256}}` | a `backfills` entry | the v2 commit identity (§5) |

### Rules for every form

- **Lookup:** the key is looked up in the grant's **stored** plan, in the list the form names. A key that is not there gets 403 `migration_upload_not_allowed`.
- **Pair:** the entry's `(source_user_id, workspace_id)` must equal the request's canonical pair, else 403 `migration_pair_not_allowed`.
- **Live checks:** the active-human and revocation checks are the same as v1.
- **Tenancy comes only from the stored entry and the grant:** `org_id`, `workspace_id`, `source_user_id`, `session`, `grant_id`, `plan_digest`, `entry_sha256` and `backfill_sha256`. A client may not supply any tenant or identity field, and a request that does gets 400 `invalid_upload_selector`.
- **No listing:** there is no listing or search endpoint, and the issuer never reveals which keys a plan contains beyond the yes or no of a mint.
- **Lifetime:** at most 60 s, as v1. The claims are otherwise v1's: `sub agent_relayhistory_migration`, `meta.tokenClass migration_assertion`, `org`, `wks`, `sponsorId`, `sponsorChain`, `scopes ["rth:sync"]`, `aud`, `jti`, and a `nonce` in meta.

## 4. The base-read capability: `meta.upload` for the form `{key, kind: "backfill_base_read"}`

```
{ v: 2, kind: "backfill_base_read",
  grant_id, plan_digest, key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at},
  entry_sha256 }
```

- **What it binds:** exactly one org, one source user, one workspace, one manifest key and session, one grant and one plan. It carries no base, and it is not a commit identity.
- **Where it works:** the ingest admits it on **one route only**, `GET /v1/pipeline/manifests/base?key=<the signed unprefixed key>`.
  - The server applies the deployment prefix itself; no caller-supplied prefix, tenant or machine is used.
  - The response is exactly `{base: {revision, version, owner, writer, chunk_count, epoch, content_sha256}}` for the stored live manifest.
  - Error responses:
    - 404 when there is no manifest;
    - 409 `migration_base_unusable` when the owner is `migration:*`, a backfill epoch already exists, or the format is not ndjson;
    - 403 for any other route.
- **Single use:** the ingest consumes the capability (jti and nonce) and appends an audit row with result `base_read`. A replay gets 409 `migration_assertion_replayed`.

## 5. The v2 commit identity: `meta.upload` for the form `{key, base}`

```
{ v: 2, kind: "backfill",
  grant_id, plan_digest, key, org_id, workspace_id, source_user_id,
  session: {machine_id, harness, session_id, started_at},
  backfill_sha256, entry_sha256,
  base: {revision, version, owner, writer, chunk_count, epoch, content_sha256} }
```

- **Where `base` comes from:** the courier copies it verbatim from the base read.
- **Issuer validation of `base`:** the shape only:
  - exactly these 7 keys;
  - `revision` is an integer of 1 or more;
  - `chunk_count` and `epoch` are integers of 0 or more;
  - `version`, `owner` and `writer` are non-empty strings of at most 512 characters;
  - `owner` does not start with `migration:`;
  - `content_sha256` is 64 lowercase hex.

  Any failure gives 400 `invalid_upload_selector`.
- **The issuer does not and cannot vouch for the base.** The ingest compares it exactly with storage at commit, and a stale base fails (403 `migration_upload_forbidden`, or 409 on the compare-and-swap version).
- **Every other field** is derived from the stored entry, never from the request.
- **Field order is irrelevant;** canonical JSON sorts keys. No other key may appear, and the ingest refuses extra keys.

## 6. Audit (issuer)

- Every mint appends the existing assertion-issuance audit record, extended with three fields:
  - `upload_kind`: `v1`, `backfill_base_read` or `backfill`;
  - the key's SHA-256;
  - `entry_sha256`.
- It carries no token, nonce or base content beyond `base.revision`.
- Refusals (wrong pair, not in plan, malformed base) are audited with their code.

## 7. Verifier (`packages/sdk/typescript/src/verify.ts`)

`isMigrationUploadIdentity` accepts exactly three shapes:

| shape | keys | key rules |
|---|---|---|
| v1 | the 9 keys (no `v`) | unchanged |
| v2 base-read | `v`, `kind`, `grant_id`, `plan_digest`, `key`, `org_id`, `workspace_id`, `source_user_id`, `session`, `entry_sha256` | `kind` is `"backfill_base_read"` |
| v2 commit | the base-read keys plus `backfill_sha256` and `base` | `kind` is `"backfill"`, and `base` has exactly the 7 keys |

In all three shapes the ids are canonical and the digests are 64 lowercase hex.

## 8. Test vectors the issuer must reproduce

From `relayhistory-export-plan-entry-v2.vectors.json` (96BullSessions #79):

- **The backfill entry:** `backfill_entry`, `backfill_entry_canonical_json`, `backfill_entry_sha256`, and the backfill digest.
- **The plan:** `plan.canonical_json` and `plan.plan_digest`. This plan is the v1 vectors' two manifests plus one backfill.
- **The base-read capability:** `base_read_identity`, for the form `{key, kind: "backfill_base_read"}`.
- **The commit identity:** `upload_identity`, for the form `{key, base}`, using the vectors' `base`.
- **Negative cases:** a missing `records`; non-contiguous offsets; a supplied `backfill_sha256`; a duplicate key across the two lists; a base whose owner starts with `migration:`; an extra identity key.
