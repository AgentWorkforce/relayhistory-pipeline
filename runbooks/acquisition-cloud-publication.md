# Acquisition: Q3 Cloud publication receiver (HELD, default off)

Operator runbook for `POST /v1/acquisition/cloud/batches`, the History side of the private
Cloud publication port (design PR #314 `docs/factory-acquisition-owner-design.md` §9). Code:
`src/acquisition/cloud-batches.ts` (receiver), `src/acquisition/app.ts` (route, gate,
authentication), `src/acquisition/config.ts` (`cloudPublicationConfig`). Tests:
`src/acquisition/__tests__/cloud-batches.test.ts`.

**Status: HELD.** The change is source-only and **default off**. This runbook grants no
permission to deploy, enable the flag, expose the ops listener, create or rotate credentials,
or apply any migration. Each of those needs Khaliq's separate, explicit OK. No migration is
part of this change.

## What it does

Cloud sends one prepared delivery batch for one acquired `droid` session, wrapped with the
original signed install envelope and the session's grant secret G. History authenticates the
operator, verifies the install against the pinned **Cloud** issuer keys, checks that the batch
is bound to that install, and forwards the exact decoded batch bytes to the private Ingest
`POST /v1/delivery/batches` with freshly signed internal headers. Ingest's 200
`DeliveryReceipt` is returned byte for byte. Final owner/epoch/grant admission happens in
Ingest's SQL claim (§5), not here.

It is mounted on the **ops listener only** (`ACQUISITION_OPS_HOST`:`ACQUISITION_OPS_PORT`,
loopback by default). It is not on the native (Desktop) listener (`404` there) and is not
added to the Desktop-facing upload ingress allowlist.

## Flag and configuration

| Variable | Meaning |
|---|---|
| `ACQUISITION_ENABLED` | Master flag (must be `true` for anything below to be read). |
| `ACQUISITION_CLOUD_PUBLICATION_ENABLED` | This route's own flag, default `false`. Strict `true`/`false`/unset; any other value fails startup. Independent of `ACQUISITION_OPS_ENABLED`. |
| `ACQUISITION_CLOUD_INGEST_ORIGIN` | Private Ingest origin, exactly `scheme://host[:port]` (no path, no trailing slash, lowercase). https, loopback http, or in-cluster http with `ALLOW_IN_CLUSTER_HTTP=true` (`src/shared/destination-url.ts`). |
| `ACQUISITION_CLOUD_INGEST_SIGNING_KEY` | The existing session-ingest edge HMAC key (secret; 1Password, never on disk). Signs the internal hop only. |
| `ACQUISITION_PINNED_KEYS` | Must contain at least one `cloud`-class key. Only cloud-class keys verify here. |
| `ACQUISITION_ORIGIN`, `RELAYCONTROL_INTROSPECT_URL`, `RELAYCONTROL_INTROSPECT_SECRET` | Shared with the rest of the service (operator introspection). |

States:

- Flag unset/`false` (default): every request answers `503 acquisition_disabled`,
  `Retry-After: 60`. No introspection, no upstream call.
- Flag `true`, any receiver variable missing or invalid: startup does **not** fail; it logs
  `acquisition cloud publication unconfigured` with the missing variable **names** (never
  values) in `reason`, and every request answers `503 acquisition_cloud_unconfigured`,
  `Retry-After: 5`. No introspection, no upstream call, no fallback publisher, provider key or
  generic user token.
- Flag `true`, config complete: the receiver runs.

Rollback is setting `ACQUISITION_CLOUD_PUBLICATION_ENABLED=false` (or unset) and restarting.
The receiver holds no state.

## Request

```
POST /v1/acquisition/cloud/batches
Authorization: Bearer sh_wk_...        (per-workspace relaycontrol operator token)
Content-Type: application/json         (no Content-Encoding other than identity)

{"v":1,"install":{"payload":{...A + evidenceSha256 + requestSha256},"signature":"<86-char base64url>"},"batchBodyBase64url":"<unpadded base64url>"}
```

Decoded `batchBodyBase64url` is exactly
`{"protocolVersion":1,"batch":{...},"acquisition":[{"source":"droid","sessionId":SID,"grantId":A.operationUuid,"ownerEpoch":"<A.expectedOwnerEpoch+1>","grant":"rhg_<64 hex>"}]}`.

Order of checks (a refusal stops the request; nothing before step 9 calls Ingest):

1. Flag / config gate (503).
2. Content-Type and Content-Encoding (415); declared `Content-Length` over 2,100,000 (413),
   before authentication.
3. Bearer: `sh_wk_` only, introspected (#313 wire) with `aud = ACQUISITION_ORIGIN`;
   `principal_type` `service`, scope `acquisition:operate`, a workspace present. Org/workspace
   come only from introspection.
4. Streamed body over 2,100,000 bytes (413, stream cancelled).
5. Outer JSON: strict UTF-8, exactly `{v, install, batchBodyBase64url}`, `v === 1`.
6. A's org/workspace equal the operator's (403 `target_mismatch`); install verified with the
   Cloud registry only (rebuild A, Ed25519 S, both digests; 403 `not_authorized` or 400).
7. Only now decode the batch: canonical unpadded base64url (no padding, no `+`/`/`, no
   whitespace, zero trailing bits), non-empty, at most 1,500,000 bytes, strict UTF-8, JSON.
8. Shape and binding (below).
9. Forward and map the answer.

Binding rules (403 unless noted, all before any upstream call):

| Rule | Code |
|---|---|
| request keys exactly `protocolVersion:1`, `batch`, `acquisition`; one acquisition entry with exactly `source, sessionId, grantId, ownerEpoch, grant`, well-formed | 400 `invalid_request` |
| 0 records / more than 500 records | 400 / 413 |
| A.source and entry source are `droid`; entry sessionId = A.sessionId; grantId = A.operationUuid | `target_mismatch` |
| ownerEpoch = A.expectedOwnerEpoch + 1, BigInt arithmetic (exact above 2^53) | `target_mismatch` |
| §3 grant hash of G equals A.grantSha256 (constant-time) | `not_authorized` |
| batch `origin_id` and `instance_id` = A.ownerOriginId | `target_mismatch` |
| `account_id` = `deliveryAccount({orgId, workspaceId})` of the operator | `target_mismatch` |
| every record: `source` droid, `session_id` = SID, `origin_id` = A.ownerOriginId | `target_mismatch` |
| revision ids are strings and unique | 400 `invalid_request` |

Everything else about the records is the ordinary delivery validator's job in Ingest.

## Forwarding

- Destination: `<ACQUISITION_CLOUD_INGEST_ORIGIN>/v1/delivery/batches` only; redirects are
  never followed; bounded by a 30 s timeout; the response is read up to 1 MiB.
- Headers sent: `content-type`, `accept`, `x-relay-ingest-identity`,
  `x-relay-ingest-signature`, `x-correlation-id`. Identity is
  `{userId: A.ownerUserId, orgId, workspaceId, scopes:["rth:sync"]}`, built from the verified A
  and the operator scope, and signed with the existing `signRequest` edge signer. Caller
  `Authorization`, identity, signature, cookie or forwarding headers are never forwarded.
- Body: the exact decoded bytes (never re-serialized).

## Refusal matrix

| Situation | HTTP | Body / headers | Upstream call |
|---|---|---|---|
| master or cloud flag off | 503 | `acquisition_disabled`, `Retry-After: 60` | no |
| flag on, config incomplete | 503 | `acquisition_cloud_unconfigured`, `Retry-After: 5` | no |
| wrong Content-Type / Content-Encoding | 415 | `unsupported_media_type` | no |
| declared or streamed outer body over 2,100,000 | 413 | `payload_too_large` | no |
| missing / wrong-class / refused bearer, wrong principal type or scope | 403 | `not_authorized` | no |
| introspection unavailable | 503 | `introspection_unavailable` | no |
| bad outer JSON / keys / base64url / UTF-8 / shape | 400 | `invalid_request` | no |
| decoded batch over 1,500,000 or more than 500 records | 413 | `payload_too_large` | no |
| operator/A workspace or signed tuple mismatch | 403 | `target_mismatch` | no |
| wrong issuer class, unknown key, bad signature or digest, wrong G | 403 | `not_authorized` | no |
| Ingest 200 with a valid receipt for this batch | 200 | Ingest bytes verbatim | yes |
| Ingest 400/403/409 (incl. `acquisition_refused`)/413/422/429/5xx | same | Ingest bytes verbatim; numeric `Retry-After` forwarded | yes |
| Ingest 401 (internal hop signature refused: our config) | 503 | `ingest_unavailable` | yes |
| transport failure, timeout, oversized response | 503 | `ingest_unavailable` | yes |
| Ingest 1xx/2xx other than a valid 200 receipt, or 3xx | 503 | `invalid_upstream_receipt` | yes |

Every 503 from Ingest's side is an **unknown outcome**: Cloud retries the exact saved
envelope; it never mints a new batch, grant or assertion. A byte-identical retry is forwarded
again; Ingest's claim identity (tenant, signed origin, `batch_id`) is the idempotency key.

A valid receipt is JSON whose `batchId` equals the batch's `batch_id` and whose
`acceptedRevisionIds` is a duplicate-free subset of the batch's revision ids. Cloud still
owns the exact receipt-set acknowledgement (§9.3).

## Logs

One line per request, message `acquisition request`, allowlisted fields only:
`route` (`cloud POST batches`), `correlation_id`, `kind` (`cloud`), `org_id`, `workspace_id`,
`status`, `outcome`, `error_code` (static code; for a forwarded Ingest error, its static code
only), `reason` (`upstream_<status>`), `count` (records), `duration_ms`, and `error_name` for
an unexpected 503. G, the signed envelope, the bearer, the signing key and body bytes are
never logged or echoed.

## Known gaps / prerequisites before enablement

- Ingest must accept the top-level `acquisition` sibling (the ingest-side admission lane,
  §9.2 / §5.6). Until it lands, Ingest refuses the forwarded request with its ordinary 400,
  which this route forwards verbatim. That is expected while HELD.
- The #313 introspection wire carries no `purpose` field, so the §9.1 purpose
  `acquisition-operator` cannot be checked here; the token class, principal type, scope,
  audience and workspace are checked.
- `acquisition:operate` registration (layered-control) and `ACQUISITION_ORIGIN` in
  `SELFHOST_INTROSPECT_AUDIENCES` (see `docs/acquisition-exchange.md`).
- Exposing the ops listener to Cloud egress is a NetworkPolicy/deploy decision, not approved.
- Out of scope for this component: §8 real-Postgres admission/revocation ordering, sender-side
  501-record splitting, Cloud outbox immutability and grant pinning, and receipt-set
  acknowledgement in Cloud.
