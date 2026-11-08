# Acquisition exchange (`SERVICE=acquisition`)

Status: **dormant**. Default off, no deployment, no migration applied. Implements the
History side of the acquisition exchange contract v1 (native issuer + operations port) on
top of the design, PR #314 `docs/factory-acquisition-owner-design.md` (§3, §3.1, §4.1, §4.2,
§4.7, §5.6, §6, §8, §9).
The A/S codec and verifier are the peer module `src/session-ingest/acquisition-assertion.ts`
(#322, `docs/acquisition-assertion.md`); the four SQL functions come from the SQL lane
(`feat/ingest-acquisition-owner-20261107`).

Code: `src/acquisition/` (`app.ts` routes and auth, `config.ts` env, `installer.ts` SQL
adapter, error table and ops handler, `native-issuer.ts` P3/P3a, `intent-store.ts` durable
intents, `key-registry.ts` pinned keys and signer, `verifier.ts` acquisition token verifier,
`main.ts` listeners and reconciler).
Proposed DDL: `supabase/acquisition-intents.proposed.sql` (not a migration; never applied by
this branch). It follows the babysitter migration conventions (role guard, REVOKE from every
known role, column-scoped grants) and carries a **DEVIATION** block from design §6 for the SQL
lane to rule on: the installer role gets column-scoped table DML and RLS is not enabled; the
preferred end state is SECURITY DEFINER functions over an RLS-enabled table with no policies.

## Cloud reachability (pinned for phase 2)

This answers the Cloud phase-2 publisher. It changes no deployed name or source pin.

1. **Transport.** Authenticated HTTPS to `ACQUISITION_ORIGIN` on the acquisition service's
   private ingress only. `SERVICE=acquisition` is a Bun/Node container. It exposes **no**
   Worker service binding and **no** named entrypoint class. `RELAYHISTORY_READS` does not
   reach it and is not an acquisition path. These routes are not on the Desktop-facing upload
   ingress. Exposing the private ingress to Cloud Worker egress is a separate deploy decision.
   It is **not approved** and needs Khaliq's explicit OK. Until then the routes are not
   reachable from Cloud.
2. **Routes, audience and issuer.**
   - Operations port (contract P2), implemented here:
     - `POST /v1/acquisition/ops/install`
     - `POST /v1/acquisition/ops/revoke`
     - `POST /v1/acquisition/ops/rebind`: always 403 for cloud keys, because the codec treats
       rebind as native-only.
     - `POST /v1/acquisition/ops/status`
   - Q3 publication port: `POST /v1/acquisition/cloud/batches`.
     - It is part of contract v1, specified in PR #314 §9 ("Private Cloud publication port (Q3,
       implementation contract v1)", head d72a664bab17): a three-field wrapper with a strict
       ordinary durable receipt, scoped separately from P2.
     - It is **not implemented on this branch**. Its handler, ingress binding and tests land in
       a follow-up source PR on top of this service. Until then it answers `404 not_found` here.
   - Auth for both:
     - `Authorization: Bearer <token>`, introspected by the deployment's relaycontrol, which is
       the token issuer.
     - `aud` = the actual `ACQUISITION_ORIGIN`, compared by normalized URL origin.
     - `principal_type` = `service`, scope `acquisition:operate`. §9.1 describes a per-workspace
       `sh_wk_` operator token, and org/workspace come from introspection only.
     - Envelopes must be signed by a `cloud`-class key in `ACQUISITION_PINNED_KEYS`.
3. **Canonical path and fallback.**
   - The canonical phase-2 path is authenticated HTTPS to `ACQUISITION_ORIGIN` +
     `/v1/acquisition/ops/*`, plus `/v1/acquisition/cloud/batches` once the follow-up lands.
   - There is no alternate transport, no service binding, no legacy publisher, no provider key,
     no generic user token, no ingest-HMAC or `rth:sync` path, and no fallback.
   - A missing receiver or missing configuration is refused, never retried elsewhere.

### Verifier in use

Both groups authenticate through the purpose-specific acquisition verifier
`src/acquisition/verifier.ts` (`createAcquisitionVerifier`). It keeps the #313 wire contract
(endpoint, request body, service secret, response shape, size cap, outage mapping) by reusing
the unchanged exports of `packages/pipeline/src/auth/relaycontrol.ts`, and differs only in
policy:

| Route group | Token prefix | principal_type | Scope |
|---|---|---|---|
| native | `sh_at_` | `desktop-user` | `rth:acquire` |
| ops (and the future Q3 route) | `sh_wk_` (§9.1 per-workspace service token) | `service` | `acquisition:operate` |

- A token with the other group's prefix is refused before introspection.
- The real `principal_type` is returned; an unknown or absent one is refused (fail closed).
- Scopes are returned raw (no allowlist); the route check asks for exactly its one scope.
- `aud` = `ACQUISITION_ORIGIN`, compared by normalized origin. No cache: every request
  introspects, bounded by the timeout.

The shared #313 client is unchanged and is no longer used by this service.

## Process model

One container, two listeners, so the private operations port is never reachable through
the Desktop ingress:

| Listener | Env | Routes | Exposure |
|---|---|---|---|
| native | `ACQUISITION_PORT` (0.0.0.0:8080) | `/v1/acquisition/native/*`, `/healthz` | Desktop ingress (same as uploads) |
| ops | `ACQUISITION_OPS_HOST`:`ACQUISITION_OPS_PORT` (127.0.0.1:8082) | `/v1/acquisition/ops/*`, `/healthz` | loopback only by default |

The native listener binds `0.0.0.0`. The ops listener binds `ACQUISITION_OPS_HOST`, default
`127.0.0.1`, and its port defaults to **8082** because 8081 is the VPC worker health port
(`stacks/aws/proof-addons.ts:229-230`). **Exposing the ops listener beyond loopback (setting
`ACQUISITION_OPS_HOST` to a pod or VPC address) needs a NetworkPolicy change** that admits
only the Cloud caller; it is never routed through the Desktop ingress.

Every route on a listener answers `503 {"error":{"code":"acquisition_disabled"}}` (with
`Retry-After: 60`) unless `ACQUISITION_ENABLED=true` and the listener's group flag is true.
`/healthz` always answers `{ok, enabled, group}`.

## Environment

All configuration is environment-only; nothing is read from disk. With
`ACQUISITION_ENABLED` unset or `false`, only the listener variables are read
(`ACQUISITION_PORT`, `ACQUISITION_OPS_PORT`, `ACQUISITION_OPS_HOST`), because the listeners
still bind to answer `503 acquisition_disabled` and `/healthz`; an invalid value fails startup
even when disabled. No other variable is read. Booleans are strict
(`true` / `false` / unset). Secrets are read once at startup and never logged.

| Variable | Required when | Meaning |
|---|---|---|
| `ACQUISITION_ENABLED` | - | Master flag, default `false`. |
| `ACQUISITION_NATIVE_ENABLED` | - | Native exchange group, default `false`. |
| `ACQUISITION_OPS_ENABLED` | - | Operations port group, default `false`. |
| `ACQUISITION_PORT` / `ACQUISITION_OPS_PORT` | always | Listener ports, default 8080 / 8082 (8081 is the worker health port), must differ. |
| `ACQUISITION_OPS_HOST` | always | Ops bind address (IP literal), default `127.0.0.1`. Anything else needs a NetworkPolicy change. |
| `ACQUISITION_ORIGIN` | enabled | This service's public origin (URL, origin only). Sent as `aud` to relaycontrol and compared by normalized origin. |
| `RELAYCONTROL_INTROSPECT_URL` | enabled | #313 introspection endpoint; checked against the destination policy. |
| `RELAYCONTROL_INTROSPECT_SECRET` | enabled | #313 service secret (secret; 1Password). |
| `ACQUISITION_PINNED_KEYS` | enabled | JSON array of `{issuerKeyId, issuerClass:"cloud"\|"native", publicKeyBase64}` (raw 32-byte Ed25519). Ids are unique and one public key may not be pinned under two ids (same or different class); startup fails otherwise. The ops port uses only `cloud` keys. |
| `ACQUISITION_DATABASE_URL` | enabled | Postgres URL for role `bulls96_acquisition_installer` (secret). |
| `ACQUISITION_ISSUER_KEY_ID` | native | The native issuer's key id; must be a `native` entry in `ACQUISITION_PINNED_KEYS` whose public key matches the private key (checked at startup). |
| `ACQUISITION_ISSUER_PRIVATE_KEY` | native | Ed25519 private key: PKCS8 PEM or base64 32-byte seed (secret). |
| `ACQUISITION_INTENT_KEY` | native | base64 of 32 bytes; AES-256-GCM key for G at rest (secret). |
| `DESTINATION_*` | as the shared policy | `src/shared/destination-url.ts` policy for the introspection URL. |

## Authentication

Bearer only, introspected through `verifier.ts` (see "Verifier in use") with
`aud = ACQUISITION_ORIGIN`. Identity (org, workspace, user) comes **only** from the
introspection answer; no client header is forwarded. Introspection unavailable or timed out:
`503 introspection_unavailable` (fail closed).

| Group | principal_type | scope | Refusals |
|---|---|---|---|
| native | `desktop-user` | `rth:acquire` | missing bearer, wrong prefix, or rejected by relaycontrol: 401 `unauthorized` (sign-in needed); valid bearer but other principal type, missing scope, workspace or user: 403 `not_authorized` |
| ops | `service` (per-workspace service token) | `acquisition:operate` | **every** authentication refusal is 403 `not_authorized`, a missing bearer included (contract P2) |

Ops tokens are per-workspace: an envelope or status body whose `orgId`/`workspaceId` differ
from the introspected ones is `403 {"error":{"code":"target_mismatch"}}` before any
verification or SQL.

### Pending registration (layered-control) and relaycontrol prerequisites

Both scopes are new and **pending registration by layered-control**: `rth:acquire`
(desktop-user) and `acquisition:operate` (service principal). Until then, and until the
prerequisites below land (after layered-control's relaycontrol #20 / #22), every
acquisition request fails closed:

1. `rth:acquire` must be included in Desktop-issued session scopes. Sessions issued before
   that change do not carry it: a control token refresh keeps the scopes stored with the
   session, so only the next Desktop sign-in issues a token with the new scope (see "401 vs
   403").
2. The acquisition origin (`ACQUISITION_ORIGIN`) must be listed in
   `SELFHOST_INTROSPECT_AUDIENCES` on each deployment.

This service no longer depends on the shared #313 client's scope allowlist or principal
filter (see "Verifier in use"); the remaining prerequisites are only the two above and the
scope registrations.

## Routes

Bodies are JSON (`Content-Type: application/json`, max 64 KiB; 415 / 413 / 400 otherwise).
Every bigint-valued result field (`revision`, `ownerEpoch`, `tombstoneRevision`, ...) is a
decimal string, exact beyond 2^53. Every response carries `X-Correlation-Id` and
`Cache-Control: no-store`.

### Operations port (P2)

| Route | Body | 200 |
|---|---|---|
| `POST /v1/acquisition/ops/install` | `{payload:{A fields + evidenceSha256 + requestSha256}, signature}` | `{result:<SQL result>}` |
| `POST /v1/acquisition/ops/revoke` | same, revoke fields | `{result}` |
| `POST /v1/acquisition/ops/rebind` | same, rebind fields | `{result}` (see note) |
| `POST /v1/acquisition/ops/status` | `{orgId, workspaceId, source, sessionIds[<=500]}` | `{result:{sessions:[{sessionId,state,revision,tombstoneRevision,ownerEpoch,ownerKind,grantId}]}}` |

The envelope is verified by the peer verifier against the cloud-class pinned keys. Note:
the peer codec requires rebinds to be native-issued, so `ops/rebind` cannot succeed with a
cloud key (403); rebind is effectively native-only.

### Native exchange (P3 + P3a)

| Route | Body / query | 200 |
|---|---|---|
| `POST /v1/acquisition/native/installations` | `{requestUuid, originId, label?, installationProof}` | `{installationId, originId, issuerKeyId}` |
| `POST /v1/acquisition/native/claims` | `{operationUuid, installationId, source, sessionId, originId}` | `{outcome:"installed"\|"replay", grantId, ownerEpoch, revision, expiresAt:null, grant}` |
| `POST /v1/acquisition/native/revocations` | `{operationUuid, installationId, source, sessionId, targetGrantId}` | `{outcome, revision, ownerEpoch, state}` |
| `POST /v1/acquisition/native/rebinds` | `{operationUuid, installationId, source, sessionId, grantId, originId, grant}` | `{outcome:"rebound"\|"replay", revision, ownerEpoch}` |
| `GET /v1/acquisition/native/status?installationId=&source=&sessionIds=a&sessionIds=b` | - | `{result:{sessions:[{..., grantId, mine}]}}` |

Every native route except `installations` needs `X-Acquisition-Installation-Proof`. The
full wire contract, with every refusal, is in "Native wire contract" below.

**Installation proof (P3a).** `installationProof` is base64url (unpadded) of 32 CSPRNG bytes
that Desktop keeps in its secure store. The issuer stores only
`SHA-256("96bulls:acquisition-installation-proof-v1" US orgId US workspaceId US userId US proofBytes)`.
Same `requestUuid` + same proof: the same `installationId` (a lost response is recoverable).
Same `requestUuid` + different proof (or another user): `409 installation_proof_mismatch`
(`retryable:false`). Missing or malformed proof: `400 invalid_request`. On the other routes
the hash is recomputed from the introspected identity and compared in constant time with the
named installation; unknown, retired, other-principal or mismatched: `403
installation_not_proven`, before any SQL or intent read. Rebind additionally requires the
session's G (§4.7). Another installation of the same user never sees another installation's
G, never replays its claims, and status shows `mine:false` with `grantId:null` for grants it
did not install. The proof is never echoed, stored or logged.

### Error table (both groups)

| Cause | HTTP | Body |
|---|---|---|
| malformed body, 22023 | 400 | `{error:{code}}` (`invalid_request`, `request_digest_mismatch`, ...) |
| unknown key, bad signature, wrong issuer class, 42501 | 403 | `{error:{code}}` (`not_authorized`, `not_owner`, `target_mismatch`, ...) |
| 55000, 40001 | 409 | ops `{error:{code, retryable}}`; native `{error:{code:"acquisition_refused", reason, retryable}}` |
| explicit revoke of a grant that is not live | 409 | native `{error:{code:"acquisition_refused", reason:"grant_not_installed", retryable:false}}` |
| native intent leased by another sender, or failed and not yet due (stalled included) | 503 | `{error:{code:"acquisition_unavailable", retryable:true}}` |
| request target over 8 KiB | 414 | `{error:{code:"request_uri_too_long"}}` |
| database / transport failure | 503 | `{error:{code:"acquisition_unavailable", retryable:true}}` |
| introspection unavailable | 503 | `{error:{code:"introspection_unavailable"}}` |
| `ACQUISITION_ENABLED` or the group flag off | 503 | `{error:{code:"acquisition_disabled"}}`, `Retry-After: 60` (other 503s: `Retry-After: 5`) |

Only `session_busy` and transport-class failures are `retryable:true`; `revision_cas_lost`,
`stale_revision`, `predecessor_mismatch`, `operation_conflict`, `origin_cas_lost` and every
55000 code are `retryable:false` (re-read status, build a new operation). The stable code is
the message after `acquisition: `; a message outside the contract is a defect (500).

## Native wire contract

Native listener only (`ACQUISITION_PORT`). Native status is **GET**; every cloud ops route
(including `ops/status`) is **POST**. All routes need `Authorization: Bearer sh_at_...`.

| Verb and path | Headers | Body / query | 200 body |
|---|---|---|---|
| `POST /v1/acquisition/native/installations` | `Authorization`, `Content-Type: application/json` | `{requestUuid, originId, installationProof, label?}` | `{installationId, originId, issuerKeyId}` |
| `POST /v1/acquisition/native/claims` | `Authorization`, `Content-Type`, `x-acquisition-installation-proof` | `{operationUuid, installationId, source, sessionId, originId}` | `{outcome:"installed"\|"replay", grantId, ownerEpoch, revision, expiresAt:null, grant}` |
| `POST /v1/acquisition/native/revocations` | `Authorization`, `Content-Type`, `x-acquisition-installation-proof` | `{operationUuid, installationId, source, sessionId, targetGrantId}` | `{outcome, revision, ownerEpoch, state}` (`outcome:"replay"` on a retry) |
| `POST /v1/acquisition/native/rebinds` | `Authorization`, `Content-Type`, `x-acquisition-installation-proof` | `{operationUuid, installationId, source, sessionId, grantId, originId, grant}` | `{outcome:"rebound"\|"replay", revision, ownerEpoch}` |
| `GET /v1/acquisition/native/status` | `Authorization`, `x-acquisition-installation-proof` | query `installationId`, `source`, and one `sessionIds` (or `sessionId`) parameter **per id**, 1-500 distinct ids | `{result:{sessions:[{sessionId, state, revision, tombstoneRevision, ownerEpoch, ownerKind, grantId, mine}]}}` |

Session ids are opaque and are never split on commas: `?sessionIds=a&sessionIds=b`, not
`?sessionIds=a,b`, which is the single id `a,b`.

Refusals. The first six rows are checked in this order before any route logic; the rest follow
the route (for POST, JSON parsing precedes the proof check, field validation follows it). Every body is `{"error":{...}}`; every response carries
`X-Correlation-Id` and `Cache-Control: no-store`.

| Status | Code | When | Routes |
|---|---|---|---|
| 503 | `acquisition_disabled` | `ACQUISITION_ENABLED` or `ACQUISITION_NATIVE_ENABLED` off; `Retry-After: 60` | all |
| 414 | `request_uri_too_long` | request target (path + query) over 8 KiB | all |
| 404 | `not_found` | unknown verb or path | - |
| 401 | `unauthorized` | missing bearer, not `sh_at_`, or rejected by relaycontrol: **sign-in needed** | all |
| 503 | `introspection_unavailable` | relaycontrol unreachable or timed out; `Retry-After: 5` | all |
| 403 | `not_authorized` | **valid bearer**, but not `desktop-user`, no `rth:acquire`, or no workspace/user | all |
| 415 | `unsupported_media_type` | body route without `Content-Type: application/json` | POST |
| 413 | `payload_too_large` | body over 64 KiB (declared or streamed) | POST |
| 400 | `invalid_request` | malformed JSON, missing/extra field, bad UUID or identifier, missing or malformed `installationProof` (installations), bad `grant`, epoch at the bigint limit, 0 or over 500 or duplicate status ids | all |
| 409 | `installation_proof_mismatch` (`retryable:false`) | same `requestUuid`, different proof or user | installations |
| 403 | `installation_not_proven` | proof header missing, or it does not match the named installation, or that installation is unknown, retired or another principal's | all but installations |
| 403 | `not_authorized` | revoke/rebind of a grant this installation did not install, or a rebind without the session's G | revocations, rebinds |
| 403 | `target_mismatch` | revoke names a session or source other than the grant's | revocations |
| 403 | SQL 42501 code (`not_owner`, ...) | the SQL owner check refused | claims, revocations, rebinds |
| 409 | `acquisition_refused` + `reason`, `retryable` | SQL 55000/40001 (`session_busy` retryable; others not), `operation_conflict`, `grant_not_installed` | claims, revocations, rebinds |
| 503 | `acquisition_unavailable` (`retryable:true`) | database or transport failure (the intent stays pending), or a replay of a failed intent before its backoff elapses (not sent); `Retry-After: 5` | all but installations |
| 500 | `internal_error` | a SQL message outside the contract (a defect) | claims, revocations, rebinds |

**401 vs 403.** 401 means the bearer is missing or not a valid relaycontrol token, so Desktop
should sign in again. 403 means the bearer is valid but the request is not allowed. Two kinds
of 403 need different handling:

- **Sign-in remediation: `403 not_authorized` from the bearer check** (the `not_authorized` row
  above with "valid bearer"). The usual cause is a Desktop token issued before `rth:acquire`
  was added to Desktop session scopes. A control token refresh does not help, because it keeps
  the scopes stored with the session; the next Desktop sign-in issues a token with the new
  scope. Desktop does **one** normal setup re-auth (the ordinary sign-in flow, user visible)
  and retries once; if the retry is still refused it holds (no acquisition, reports the
  state) and does not loop. There is no silent scope elevation: the service never widens a
  token, and Desktop never requests the scope outside that normal sign-in.
- **Proof and ownership 403s** (`installation_not_proven`, the route-level `not_authorized` for
  a grant this installation did not install, `target_mismatch`, SQL 42501 codes such as
  `not_owner`): signing in again does **not** fix these. They mean the installation proof,
  the installation's state (unknown, retired, another principal's) or grant ownership is
  wrong; Desktop re-registers its installation or stops presenting the grant, and does not
  trigger a re-auth for them.

(Ops differs: every ops authentication refusal is 403, a missing bearer included.)

**Retry-After.** `503 acquisition_disabled` carries `Retry-After: 60`; every other 503 carries
`Retry-After: 5`. 413 and 415 apply only to request bodies (the POST routes).

## Durable state and retries

The native issuer persists each operation as an intent **before** calling SQL:
`{A, S, evidenceSha256, requestSha256, G encrypted}`. A retry (same `operationUuid`, or the
reconciler) resends the stored A/S byte-for-byte; nothing is ever re-signed. G is returned
only on `installed` or on an exact replay by the same installation, and is stored only as
AES-256-GCM ciphertext with AAD `domain US operationUuid US installationId`.

## Durable cleanup (reserved revokes) and the reconciler

When an install is **applied**, the **same intent-store transaction** that records the result
stores a **reserved, pre-signed revoke** of that grant: `targetGrantId` = the grant,
`targetOwnerEpoch` = the `ownerEpoch` the install result reports, and `revision` = the result's
revision + 1 (§4.2; SQL takes `greatest(revision, p.revision)`). The design fixes the epoch:
the claim sends `expectedOwnerEpoch` = the current epoch from status and the install lands at
exactly `expectedOwnerEpoch + 1` (tests use that contract). The reserved revoke's epoch is
therefore deterministic, but the issuer still signs from the epoch the result reports, never
from an assumed value, so a result that disagrees is not silently re-derived. If the result
carries no `ownerEpoch`, or signing fails, **no** revoke is pre-signed and the install records
`reservation = "no_epoch"` (or `"sign_failed"`).
Activation of a reserved revoke needs no network and no private key; it is a local row update:

- explicit permanent exclude (`POST .../revocations`): activated and sent immediately;
- session membership removal (`NativeIssuer.removeSession`);
- installation retirement (`NativeIssuer.retireInstallation`, which retires the installation,
  cancels its pending installs and activates every reserved revoke of it in one transaction).

**Cancellation ordering.** An install whose outcome is not yet known (in flight, response
lost, stalled) has no reserved revoke yet, and its SQL may have committed. Removal and
retirement therefore record the cancel on the pending install itself (`cancel_reason`). The
store serializes this with the install's apply on the install row: the apply updates that row
first and reads `cancel_reason`, so whichever commits first, the revoke is inserted already
**pending** (active, with that reason) instead of `reserved`, and the reconciler sends it. A
pending install is still resent after a cancel, because the SQL is idempotent by
`operationUuid` and a resend of the same bytes replays the outcome. A claim whose installation
is retired while the claim awaits status is refused when its intent is created
(`createInstallIntent` takes `FOR SHARE` on the installation row and rechecks `retired_at`):
`403 installation_not_proven`, nothing sent.

An explicit revoke of an install **without** a reservation (`no_epoch`, `sign_failed`, or a
stalled install whose grant may have landed) reads the authority status row and signs a revoke
against its current `ownerEpoch`/`revision`, only if the row's `grantId` is that install;
otherwise `409 grant_not_installed` (retry after status shows the grant). For a stalled
install, that refusal also records the exclude as its cancel, so a later apply creates its
revoke active. An **applied** install with `no_epoch`/`sign_failed` has no revoke row:
removal and retirement cannot clean it up by themselves, and after retirement the explicit
revoke is refused by the proof check (known limitation; a SQL install result always carries
`ownerEpoch`).

A reconciler (`main.ts`, every 30 s while native is enabled) resends **every** due pending
intent, installs and rebinds as well as revokes, byte-for-byte. Each failed send increments
`attempts` and sets `next_attempt_at` with capped exponential backoff
(`min(30 s * 2^(attempts-1), 1 h)`). A failed send is **never terminal**, because its SQL may
have committed (a lost response): dropping the intent would drop its encrypted G (no recovery
by replay) and its key retention. After 8 attempts (`MAX_SEND_ATTEMPTS`) the intent is
**stalled**: still pending, still resent hourly, still holding its issuer key; the reconciler
counts it and `main.ts` logs `acquisition reconcile stalled` at warn (alert on it). An
explicit revoke of a stalled install may proceed (above); a rebind of its grant still answers
`409 session_busy`, and a replay of the claim sends it when due (else `503`). Due intents are taken in `next_attempt_at, created_at` order, so a
run of failing intents backs off and cannot starve later ones.

**Send lease.** Every send (request path or reconciler) first takes the intent's lease: a
single-statement compare-and-set that sets `lease_until` = now + 5 min only if the intent is
pending, unleased (or its lease expired) and due (`attempts` = 0 or `next_attempt_at` passed).
The outcome (applied, refused, retry) clears it. So concurrent client replays and reconcilers
never send one intent at once, even at `attempts` = 0: a replay that does not get the lease
answers `503 acquisition_unavailable` (retryable) without spending an attempt, and the
reconciler skips it. A sender that dies holding the lease delays the intent by at most the
lease; a send that outlives its lease can at worst overlap one resend of the same bytes, which
the SQL's `operationUuid` idempotency absorbs. (The lease and the due check use the database
clock in Postgres; small skew against the app clock only shifts timing.) A revoke whose
installation is still selected for the session is deferred by 30 s, not resent. A refused
install voids its reserved revoke. Desktop's reversible exclude remains a local hold (stop
presenting the grant), not a server revoke (contract P3, F5).

## Key rotation and retention (§9)

Rotating an issuer key never drops the old public key from `ACQUISITION_PINNED_KEYS` while
anything can still present an assertion signed by it: an outstanding publication outbox
entry, a receipt replay, a reserved control revoke, or a pending intent; not merely until
the control intents are applied. Rotation is additive: pin the new key, switch
`ACQUISITION_ISSUER_KEY_ID` / `ACQUISITION_ISSUER_PRIVATE_KEY`, and remove the old entry only
after nothing references it.

Enforced here: with native enabled, startup reads the distinct `issuerKeyId` (last A field)
of every `reserved` or `pending` intent (stalled ones included) and refuses to start (before
listening) if any is missing from the pinned set or pinned under another class
(`assertKeysRetained`). Applied, refused and void intents no longer hold a key. Cloud-issued outbox and receipt-replay state
lives outside this service (Cloud side and the SQL lane), so this service cannot check the
cloud-class set; operators must keep those keys pinned until the owner confirms the outbox
and replay window is drained.

## Logging

Only allowlisted fields go through `src/shared/log.ts`: route, correlation id, status,
outcome, error code, retryable, duration, org/workspace ids. Never the bearer, the
introspection secret, the installation proof, G, the intent key or any private key.
