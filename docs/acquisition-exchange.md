# Acquisition exchange (`SERVICE=acquisition`)

Status: **dormant**. Default off, no deployment, no migration applied. Implements the
History side of the acquisition exchange contract v1 (native issuer + operations port) on
top of `docs/factory-acquisition-owner-design.md` (§3, §3.1, §4.1, §4.2, §4.7, §5.6, §6, §8).
The A/S codec and verifier are the peer module `src/session-ingest/acquisition-assertion.ts`
(#322, `docs/acquisition-assertion.md`); the four SQL functions come from the SQL lane
(`feat/ingest-acquisition-owner-20261107`).

Code: `src/acquisition/` (`app.ts` routes and auth, `config.ts` env, `installer.ts` SQL
adapter, error table and ops handler, `native-issuer.ts` P3/P3a, `intent-store.ts` durable
intents, `key-registry.ts` pinned keys and signer, `main.ts` listeners and reconciler).
Proposed DDL: `supabase/acquisition-intents.proposed.sql` (not a migration).

## Cloud reachability (pinned for phase 2, as implemented on this branch)

Answers the Cloud phase-2 publisher's question. This states what the receiver on this
branch exposes; it changes no deployed name or source pin.

1. **Transport.** The receiver is authenticated HTTP(S) only. `SERVICE=acquisition` is a
   Bun/Node container, not a Cloudflare Worker: it exposes **no** Worker service binding and
   **no** named entrypoint class. The existing `RELAYHISTORY_READS` binding does not reach it
   and is not an acquisition path. The ops listener (`ACQUISITION_OPS_PORT`) is intended for
   private networking only (contract P1). Making it reachable from Cloud Worker egress needs an
   ingress decision that this branch does not make (a deployment change, so it needs Khaliq's
   explicit OK). Until then the receiver is not reachable from the Cloud Worker at all.
2. **Routes, audience and issuer.** These are the only Cloud-facing routes:
   `POST /v1/acquisition/ops/install`, `POST /v1/acquisition/ops/revoke`,
   `POST /v1/acquisition/ops/rebind` (always 403 for cloud keys, because rebind is native-only
   in the codec) and `POST /v1/acquisition/ops/status`. `Authorization: Bearer <token>` is
   introspected by the deployment's relaycontrol (#313 client). The token issuer is that
   relaycontrol. The expected `aud` is `ACQUISITION_ORIGIN`, compared by normalized URL origin
   (one origin per deployment, shared with the native group). `principal_type` must be
   `service` (a per-workspace service token), with scope `acquisition:operate`. Envelope
   signatures must come from a `cloud`-class key in `ACQUISITION_PINNED_KEYS`.
   **`/v1/acquisition/cloud/batches` does not exist** on this branch and is not in contract v1
   (P5: anything not listed is not part of v1). A Q3 publication route needs a contract
   addition from the History owner before anyone wires it.
3. **Canonical path and fallback.** For phase 2 the canonical path is authenticated HTTPS to
   `ACQUISITION_ORIGIN` + `/v1/acquisition/ops/*` as above. There is no alternate transport,
   no service binding, no ingest-HMAC or `rth:sync` path and no fallback. Anything else must be
   refused, not tried.

## Process model

One container, two listeners, so the private operations port is never reachable through
the Desktop ingress:

| Listener | Env | Routes | Exposure |
|---|---|---|---|
| native | `ACQUISITION_PORT` (8080) | `/v1/acquisition/native/*`, `/healthz` | Desktop ingress (same as uploads) |
| ops | `ACQUISITION_OPS_PORT` (8081) | `/v1/acquisition/ops/*`, `/healthz` | private network only |

Every route on a listener answers `503 {"error":{"code":"acquisition_disabled"}}` (with
`Retry-After: 60`) unless `ACQUISITION_ENABLED=true` and the listener's group flag is true.
`/healthz` always answers `{ok, enabled, group}`.

## Environment

All configuration is environment-only; nothing is read from disk. With
`ACQUISITION_ENABLED` unset or `false`, no other variable is read. Booleans are strict
(`true` / `false` / unset). Secrets are read once at startup and never logged.

| Variable | Required when | Meaning |
|---|---|---|
| `ACQUISITION_ENABLED` | - | Master flag, default `false`. |
| `ACQUISITION_NATIVE_ENABLED` | - | Native exchange group, default `false`. |
| `ACQUISITION_OPS_ENABLED` | - | Operations port group, default `false`. |
| `ACQUISITION_PORT` / `ACQUISITION_OPS_PORT` | - | Listener ports, default 8080 / 8081, must differ. |
| `ACQUISITION_ORIGIN` | enabled | This service's public origin (URL, origin only). Sent as `aud` to relaycontrol and compared by normalized origin, exactly as the session-ingest edge does. |
| `RELAYCONTROL_INTROSPECT_URL` | enabled | #313 introspection endpoint; checked against the destination policy. |
| `RELAYCONTROL_INTROSPECT_SECRET` | enabled | #313 service secret (secret; 1Password). |
| `ACQUISITION_PINNED_KEYS` | enabled | JSON array of `{issuerKeyId, issuerClass:"cloud"\|"native", publicKeyBase64}` (raw 32-byte Ed25519). The ops port uses only `cloud` keys. |
| `ACQUISITION_DATABASE_URL` | enabled | Postgres URL for role `bulls96_acquisition_installer` (secret). |
| `ACQUISITION_ISSUER_KEY_ID` | native | The native issuer's key id; must be a `native` entry in `ACQUISITION_PINNED_KEYS` whose public key matches the private key (checked at startup). |
| `ACQUISITION_ISSUER_PRIVATE_KEY` | native | Ed25519 private key: PKCS8 PEM or base64 32-byte seed (secret). |
| `ACQUISITION_INTENT_KEY` | native | base64 of 32 bytes; AES-256-GCM key for G at rest (secret). |
| `DESTINATION_*` | as the shared policy | `src/shared/destination-url.ts` policy for the introspection URL. |

## Authentication

Bearer only, introspected through the shared #313 client
(`packages/pipeline/src/auth/relaycontrol.ts`) with `aud = ACQUISITION_ORIGIN`. Identity
(org, workspace, user) comes **only** from the introspection answer; no client header is
forwarded. Introspection unavailable or timed out: `503 introspection_unavailable` (fail
closed).

| Group | principal_type | scope | Refusals |
|---|---|---|---|
| native | `desktop-user` | `rth:acquire` | no bearer / not a relaycontrol token / rejected: 401 `unauthorized`; other principal type, missing scope or workspace: 403 `not_authorized` |
| ops | `service` (per-workspace service token) | `acquisition:operate` | no bearer: 401; rejected, other principal type, missing scope: 403 `not_authorized` |

Ops tokens are per-workspace: an envelope or status body whose `orgId`/`workspaceId` differ
from the introspected ones is `403 {"error":{"code":"target_mismatch"}}` before any
verification or SQL.

### Pending registration (layered-control) and relaycontrol prerequisites

Both scopes are new and **pending registration by layered-control**: `rth:acquire`
(desktop-user) and `acquisition:operate` (service principal). Until then, and until the
prerequisites below land (after layered-control's relaycontrol #20 / #22), every
acquisition request fails closed:

1. `rth:acquire` must be included in Desktop-issued session scopes. Sessions issued before
   that change do not carry it and must be re-issued.
2. The acquisition origin (`ACQUISITION_ORIGIN`) must be listed in
   `SELFHOST_INTROSPECT_AUDIENCES` on each deployment.

In addition, the shared #313 client in this repo currently passes through only the scopes
in `RELAYCONTROL_SCOPE_ALLOWLIST` (`rth:read`, `rth:sync`) and admits only
`principal_type: "desktop-user"`. So today native requests get 403 (scope filtered) and ops
requests get 403 (service principal rejected). Its owner must extend the allowlist with
`rth:acquire` / `acquisition:operate` and admit `service` principals for the ops audience;
`relaycontrolAuthenticator` in `app.ts` is the single adapter to update.

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
| `GET /v1/acquisition/native/status?installationId=&source=&sessionIds=a,b` | - | `{result:{sessions:[{..., grantId, mine}]}}` |

Every native route except `installations` needs `X-Acquisition-Installation-Proof`.

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
| database / transport failure | 503 | `{error:{code:"acquisition_unavailable", retryable:true}}` |
| introspection unavailable | 503 | `{error:{code:"introspection_unavailable"}}` |

Only `session_busy` and transport-class failures are `retryable:true`; `revision_cas_lost`,
`stale_revision`, `predecessor_mismatch`, `operation_conflict`, `origin_cas_lost` and every
55000 code are `retryable:false` (re-read status, build a new operation). The stable code is
the message after `acquisition: `; a message outside the contract is a defect (500).

## Durable state and retries

The native issuer persists each operation as an intent **before** calling SQL:
`{A, S, evidenceSha256, requestSha256, G encrypted}`. A retry (same `operationUuid`, or the
reconciler) resends the stored A/S byte-for-byte; nothing is ever re-signed. G is returned
only on `installed` or on an exact replay by the same installation, and is stored only as
AES-256-GCM ciphertext with AAD `domain US operationUuid US installationId`.

## Durable cleanup (reserved revokes)

When the native issuer issues an install, the **same intent-store transaction** stores a
**reserved, pre-signed revoke** of that grant (`targetGrantId` = the grant, revision and
epoch placeholders per §4.2; SQL takes `greatest(revision, p.revision)`). Activation needs
no network and no private key; it is a local row update:

- explicit permanent exclude (`POST .../revocations`): activated and sent immediately;
- session membership removal (`NativeIssuer.removeSession`);
- installation retirement (`NativeIssuer.retireInstallation`, which activates every reserved
  revoke of the installation in one transaction).

A reconciler (`main.ts`, every 30 s while native is enabled) sends **pending revokes only**,
oldest first, and never while the installation is still selected for that session. A
refused install voids its reserved revoke. Desktop's reversible exclude remains a local hold
(stop presenting the grant), not a server revoke (contract P3, F5).

## Key rotation and retention (§9)

Rotating an issuer key never drops the old public key from `ACQUISITION_PINNED_KEYS` while
anything can still present an assertion signed by it: an outstanding publication outbox
entry, a receipt replay, a reserved control revoke, or a pending intent; not merely until
the control intents are applied. Rotation is additive: pin the new key, switch
`ACQUISITION_ISSUER_KEY_ID` / `ACQUISITION_ISSUER_PRIVATE_KEY`, and remove the old entry only
after nothing references it.

Enforced here: with native enabled, startup reads the distinct `issuerKeyId` (last A field)
of every `reserved` or `pending` intent and refuses to start (before listening) if any is
missing from the pinned set or pinned under another class (`assertKeysRetained`). Applied,
refused and void intents no longer hold a key. Cloud-issued outbox and receipt-replay state
lives outside this service (Cloud side and the SQL lane), so this service cannot check the
cloud-class set; operators must keep those keys pinned until the owner confirms the outbox
and replay window is drained.

## Logging

Only allowlisted fields go through `src/shared/log.ts`: route, correlation id, status,
outcome, error code, retryable, duration, org/workspace ids. Never the bearer, the
introspection secret, the installation proof, G, the intent key or any private key.
